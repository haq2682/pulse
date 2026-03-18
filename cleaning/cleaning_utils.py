"""
Utility functions for data operations.
"""




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


def load_data_from_minio(spark, minio_client, bucket_name, table_names, folder="mapped",
                         new_batch_ids_by_table=None):
    """
    Load data from MinIO from the specified directory.

    Storage layout priority (highest → lowest):

    1. **Delta table** – ``{folder}/{table}/_delta_log/`` exists.
       In incremental mode: filters rows to only those whose ``_batch_id`` column
       matches one of the new batch IDs, so only the fresh rows are processed.
    2. **Parquet directory** – streaming append path, ``{folder}/{table}/`` contains
       ``_batch_id=N/`` partition subdirectories.
       In incremental mode: reads only the new ``_batch_id=N/`` subdirectory paths.
    3. **Flat CSV** – batch fallback, ``{folder}/{table}.csv``.  Always full read.

    Args:
        spark (SparkSession): Active Spark session
        minio_client (Minio): MinIO client instance
        bucket_name (str): MinIO bucket name
        table_names (list): List of table names to load
        folder (str): Folder in MinIO (default: "mapped")
        new_batch_ids_by_table (dict | None): Maps table_name → set of new integer
            batch IDs to read.  When None (full mode) the entire table is loaded.

    Returns:
        tuple: (dict of table_name → DataFrame, dict of table_name → path)
    """
    import pyspark.sql.functions as F

    dataframes = {}
    file_paths  = {}

    _META_COLS = ("_batch_id", "_ingested_at")

    for table_name in table_names:
        parquet_prefix = f"{folder}/{table_name}/"
        parquet_s3     = f"s3a://{bucket_name}/{folder}/{table_name}"
        csv_path       = f"{folder}/{table_name}.csv"
        csv_s3         = f"s3a://{bucket_name}/{csv_path}"

        # Batch IDs we care about for this table (None = full read)
        new_ids = (new_batch_ids_by_table or {}).get(table_name)  # set[int] or None

        # ── 1. Delta table (CDC / streaming with MERGE) ─────────────────────
        if _is_delta_table(minio_client, bucket_name, parquet_prefix):
            try:
                df = spark.read.format("delta").load(parquet_s3)
                if new_ids is not None and "_batch_id" in df.columns:
                    df = df.filter(F.col("_batch_id").isin(list(new_ids)))
                    print(f"Loaded {table_name} from Delta (batch_ids={sorted(new_ids)})")
                else:
                    print(f"Loaded {table_name} from Delta {parquet_prefix} (full)")
                for meta_col in _META_COLS:
                    if meta_col in df.columns:
                        df = df.drop(meta_col)
                dataframes[table_name] = df
                file_paths[table_name] = parquet_prefix
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
                if new_ids is not None:
                    # Read only the specific _batch_id=N/ subdirectories
                    partition_s3_paths = [
                        f"s3a://{bucket_name}/{folder}/{table_name}/_batch_id={bid}"
                        for bid in new_ids
                    ]
                    df = spark.read.parquet(*partition_s3_paths)
                    print(f"Loaded {table_name} from Parquet partitions (batch_ids={sorted(new_ids)})")
                else:
                    df = spark.read.option("mergeSchema", "true").parquet(parquet_s3)
                    print(f"Loaded {table_name} from Parquet {parquet_prefix} (full)")
                for meta_col in _META_COLS:
                    if meta_col in df.columns:
                        df = df.drop(meta_col)
                dataframes[table_name] = df
                file_paths[table_name] = parquet_prefix
                continue
        except Exception as parquet_exc:
            # If Parquet files exist but Spark couldn't read them, log and skip
            # rather than silently falling through to a CSV that doesn't exist.
            # This surfaces real errors (corrupt files, schema problems, S3A
            # connectivity) instead of hiding them behind a misleading NoSuchKey.
            try:
                parquet_objs_check = list(
                    minio_client.list_objects(bucket_name, prefix=parquet_prefix, recursive=True)
                )
                if any(o.object_name.endswith(".parquet") for o in parquet_objs_check):
                    print(f"⚠️  Parquet files exist for {table_name} but could not be read: {parquet_exc}")
                    print(f"   Skipping {table_name} — fix the files or rerun the mapping pipeline.")
                    continue  # do NOT fall through to CSV
            except Exception:
                pass  # listing failed too; fall through to CSV as last resort

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
            print(f"Loaded {table_name} from CSV {csv_path}")
        except Exception as e:
            print(f"Could not load {table_name}: {e}")

    return dataframes, file_paths


# Primary key used for Delta MERGE when saving cleaned data incrementally.
# Must stay in sync with the canonical schema.
_CLEANED_PRIMARY_KEYS = {
    "customers":            "customer_id",
    "orders":               "order_id",
    "order_items":          "order_item_id",
    "products":             "product_id",
    "suppliers":            "supplier_id",
    "payments":             "payment_id",
    "marketing_campaigns":  "campaign_id",
    "shopping_cart":        "cart_id",
    "cart_items":           "cart_item_id",
    "inventory":            "inventory_id",
    "reviews":              "review_id",
    "wishlist":             "wishlist_id",
    "addresses":            "address_id",
    "customer_sessions":    "session_id",
    "categories":           "category_id",
}


def save_data_to_minio(dataframes, minio_client, bucket_name, incremental=False):
    """
    Save cleaned DataFrames to MinIO under the cleaned/ prefix.

    Full mode  (``incremental=False``):
        Writes each table as a Delta table with ``mode("overwrite")``.
        This is used for the first run or when ``--force-full`` is passed.

        Incremental mode (``incremental=True``):
                - If a primary key is configured and the target Delta table exists:
                    perform Delta MERGE (upsert).
                - If no primary key is configured but the target Delta table exists:
                    append new rows.
                - If the target Delta table does not yet exist: create it.

    Using Delta instead of plain Parquet lets the transformation loader read
    a schema-consistent, deduplicated view of the cleaned data without
    ``inferSchema`` scans.

    Args:
        dataframes (dict): Dictionary of table names to DataFrames
        minio_client (Minio): MinIO client instance (kept for API compatibility)
        bucket_name (str): MinIO bucket name
        incremental (bool): Whether to MERGE instead of overwrite
    """
    from delta.tables import DeltaTable

    for table, df in dataframes.items():
        s3_path = f"s3a://{bucket_name}/cleaned/{table}"
        pk = _CLEANED_PRIMARY_KEYS.get(table)

        try:
            delta_exists = DeltaTable.isDeltaTable(df.sparkSession, s3_path)

            if incremental and pk and delta_exists:
                # Upsert: update existing rows by PK, insert new ones
                cleaned_delta = DeltaTable.forPath(df.sparkSession, s3_path)
                (
                    cleaned_delta.alias("existing")
                    .merge(
                        df.alias("new"),
                        f"existing.`{pk}` = new.`{pk}`",
                    )
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute()
                )
                print(f"✅ Merged cleaned/{table} (incremental upsert on {pk})")
            elif incremental and not pk and delta_exists:
                # No PK configured: append only new microbatch rows, keep history intact
                df.write.format("delta").mode("append").save(s3_path)
                print(f"✅ Appended cleaned/{table} (incremental append; no PK configured)")
            else:
                # Create/refresh table in full mode, or create table on first incremental write
                df.coalesce(1).write.format("delta").mode("overwrite").save(s3_path)
                print(f"✅ Saved cleaned/{table} as Delta (overwrite)")
        except Exception as e:
            print(f"❌ Failed to save cleaned/{table}: {str(e)}")


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
