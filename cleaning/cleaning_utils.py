"""
Utility functions for data operations.
"""

from io import BytesIO


def _is_delta_table(minio_client, bucket_name: str, prefix: str) -> bool:
    """
    Return True if MinIO contains a ``_delta_log/`` directory under *prefix*.
    Delta tables always have this transaction-log directory; raw Parquet dirs
    produced by ``df.write.parquet()`` do not.
    """
    delta_log_prefix = prefix.rstrip("/") + "/_delta_log/"
    try:
        objs = list(
            minio_client.list_objects(bucket_name, prefix=delta_log_prefix, recursive=False)
        )
        return bool(objs)
    except Exception:
        return False


def get_file_paths_from_minio(minio_client, bucket_name, table_names, folder="mapped"):
    """
    Get list of file paths / prefixes that exist in MinIO for given tables.

    Checks three layouts (priority order):
    1. ``mapped/{table}/`` with ``_delta_log/`` — Delta table (db/api/batch mode).
    2. ``mapped/{table}/`` with ``.parquet`` files — plain partitioned Parquet.
    3. ``mapped/{table}.csv`` — flat CSV (legacy).

    Returns a list of paths that exist.
    """
    existing_files = []

    for table_name in table_names:
        parquet_prefix = f"{folder}/{table_name}/"
        csv_path       = f"{folder}/{table_name}.csv"

        # 1. Delta table?
        if _is_delta_table(minio_client, bucket_name, parquet_prefix):
            existing_files.append(parquet_prefix)
            continue

        # 2. Plain Parquet directory?
        try:
            objs = list(
                minio_client.list_objects(bucket_name, prefix=parquet_prefix, recursive=True)
            )
            if any(o.object_name.endswith(".parquet") for o in objs):
                existing_files.append(parquet_prefix)
                continue
        except Exception:
            pass

        # 3. Flat CSV (legacy batch path).
        try:
            minio_client.stat_object(bucket_name, csv_path)
            existing_files.append(csv_path)
        except Exception:
            pass  # table not yet available in any format

    return existing_files


def load_data_from_minio(spark, minio_client, bucket_name, table_names, folder="mapped"):
    """
    Load data from MinIO from the specified directory.

    Storage layout priority (highest → lowest):

    1. **Delta table** – ``{folder}/{table}/_delta_log/`` exists.
       Read with ``spark.read.format("delta").load(s3_path)``.
    2. **Parquet directory** – streaming path, ``{folder}/{table}/`` contains ``.parquet`` files.
       Read with ``spark.read.parquet(s3_path)``.
    3. **Flat CSV** – batch fallback, ``{folder}/{table}.csv``.
       Read with ``spark.read.csv(s3_path)``.

    Args:
        spark (SparkSession): Active Spark session
        minio_client (Minio): MinIO client instance
        bucket_name (str): MinIO bucket name
        table_names (list): List of table names to load
        folder (str): Folder in MinIO (default: "mapped")

    Returns:
        tuple: (dict of table_name → DataFrame, dict of table_name → path)
    """
    dataframes = {}
    file_paths  = {}

    _META_COLS = ("_batch_id", "_ingested_at")

    for table_name in table_names:
        parquet_prefix = f"{folder}/{table_name}/"
        parquet_s3     = f"s3a://{bucket_name}/{folder}/{table_name}"
        csv_path       = f"{folder}/{table_name}.csv"
        csv_s3         = f"s3a://{bucket_name}/{csv_path}"

        # ── 1. Delta table (CDC / streaming with MERGE) ─────────────────────
        if _is_delta_table(minio_client, bucket_name, parquet_prefix):
            try:
                df = spark.read.format("delta").load(parquet_s3)
                for col in _META_COLS:
                    if col in df.columns:
                        df = df.drop(col)
                dataframes[table_name] = df
                file_paths[table_name] = parquet_prefix
                print(f"Loaded {table_name} ({df.count()} rows) from Delta {parquet_prefix}")
                continue
            except Exception as e:
                print(f"Delta read failed for {table_name}: {e}, falling back to Parquet")

        # ── 2. Plain Parquet directory (streaming append / partitioned) ──────
        try:
            parquet_objs = list(
                minio_client.list_objects(bucket_name, prefix=parquet_prefix, recursive=True)
            )
            has_parquet = any(o.object_name.endswith(".parquet") for o in parquet_objs)
            if has_parquet:
                df = spark.read.parquet(parquet_s3)
                for col in _META_COLS:
                    if col in df.columns:
                        df = df.drop(col)
                dataframes[table_name] = df
                file_paths[table_name] = parquet_prefix
                print(f"Loaded {table_name} ({df.count()} rows) from Parquet {parquet_prefix}")
                continue
        except Exception:
            pass  # no parquet, try csv

        # ── 3. Flat CSV (legacy batch path) ──────────────────────────────────
        try:
            minio_client.stat_object(bucket_name, csv_path)
            df = (
                spark.read.option("header", "true")
                .option("inferSchema", "true")
                .csv(csv_s3)
            )
            dataframes[table_name] = df
            file_paths[table_name] = csv_path
            print(f"Loaded {table_name} ({df.count()} rows) from CSV {csv_path}")
        except Exception as e:
            print(f"Could not load {table_name}: {e}")

    return dataframes, file_paths


def save_data_to_minio(dataframes, minio_client, bucket_name):
    """
    Save cleaned DataFrames to MinIO as CSV files in the cleaned/ directory.

    Args:
        dataframes (dict): Dictionary of table names to DataFrames
        minio_client (Minio): MinIO client instance
        bucket_name (str): MinIO bucket name
    """
    for table, df in dataframes.items():
        pdf = df.toPandas()
        csv_buffer = BytesIO()
        pdf.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        file_name = f"cleaned/{table}.csv"

        try:
            minio_client.put_object(
                bucket_name,
                file_name,
                csv_buffer,
                length=len(csv_buffer.getvalue()),
                content_type="text/csv",
            )
            print(f"✅ Saved {file_name} ({len(pdf)} rows)")
        except Exception as e:
            print(f"❌ Failed to save {file_name}: {str(e)}")
        finally:
            csv_buffer.close()


def display_summary(dataframes):
    """
    Display summary statistics for all DataFrames.

    Args:
        dataframes (dict): Dictionary of table names to DataFrames
    """
    print("\n" + "=" * 60)
    print("📊 DATA SUMMARY")
    print("=" * 60)

    for table_name, df in dataframes.items():
        row_count = df.count()
        col_count = len(df.columns)
        print(f"\n{table_name}:")
        print(f"  Rows: {row_count}")
        print(f"  Columns: {col_count}")

    print("=" * 60)
