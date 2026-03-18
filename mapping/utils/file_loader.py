import pandas as pd
from io import BytesIO
import os
import re
import json
import traceback
from utils.helpers import normalize_name


def load_all_files_from_minio(minio_client, bucket_name, spark):
    """
    Load all supported files from the 'ingested' folder in a MinIO bucket directly into
    Spark DataFrames.

    Two storage layouts are handled:

    1. **Chunked Parquet directory** (``initial_load.py`` / DB-API mode):
       ``ingested/{canonical_table}/chunk_NNNN.parquet``
       The entire directory is read in one ``spark.read.parquet()`` call so Spark
       handles all chunks as a single scan without building a deep union lineage
       plan.  That plan explosion was causing TaskSetManager task failures for
       large tables when chunks were read individually through pandas and unioned.

    2. **Flat file** (user-uploaded batch mode):
       ``ingested/orders.csv``, ``ingested/data.xlsx``, ``ingested/records.json``,
       ``ingested/data.parquet`` (single file, no sub-directory).
       These are read via the MinIO SDK → pandas → ``createDataFrame`` path, which
       is fine for the small files users typically upload.

    Args:
        minio_client: MinIO client instance
        bucket_name:  Name of the MinIO bucket
        spark:        Active SparkSession

    Returns:
        dict: ``{canonical_table_name: Spark DataFrame}``
    """
    import os as _os

    dataframes   = {}
    SUPPORTED    = {".csv", ".xlsx", ".xls", ".parquet", ".json"}

    all_objects  = list(minio_client.list_objects(bucket_name, prefix="ingested/", recursive=True))
    print("Listing available files in the ingested folder...")

    # ── Pass 1: classify each object ─────────────────────────────────────────
    # chunked_dirs : set of prefixes like "ingested/orders/"
    # flat_objects  : list of objects that are direct children of ingested/
    chunked_dirs  = set()
    flat_objects  = []

    for obj in all_objects:
        path = obj.object_name                     # e.g. "ingested/orders/chunk_0000.parquet"
        rel  = path[len("ingested/"):]             # e.g. "orders/chunk_0000.parquet"
        parts = rel.split("/")

        if len(parts) == 2 and any(parts[1].endswith(ext) for ext in SUPPORTED):
            # Exactly one sub-directory level → chunked layout
            if parts[1].endswith(".parquet"):
                chunked_dirs.add(f"ingested/{parts[0]}/")
            else:
                # Non-parquet file inside a sub-directory (unusual but possible)
                flat_objects.append(obj)
        elif len(parts) == 1 and any(parts[0].endswith(ext) for ext in SUPPORTED):
            # Direct child of ingested/ → flat file
            flat_objects.append(obj)
        # Deeper nesting or unsupported extension → skip silently

    # ── Pass 2a: read chunked parquet dirs via spark.read.parquet() ──────────
    # Reading via Spark's native parquet reader (not pandas) avoids the
    # union-lineage explosion that caused TaskSetManager task failures when large
    # tables (8+ chunk files × 200k rows each) were unioned in the driver.
    minio_ep = _os.getenv("MINIO_ENDPOINT", "minio:9000")
    if "://" in minio_ep:
        minio_ep = minio_ep.split("://", 1)[1]

    for dir_prefix in sorted(chunked_dirs):
        table_dir = dir_prefix[len("ingested/"):].rstrip("/")   # e.g. "orders"
        norm_name = normalize_name(table_dir)
        if norm_name is None:
            print(f"  ⚠️  No canonical match for ingested dir '{table_dir}', skipping.")
            continue

        # Read the whole directory with a single Spark scan; mergeSchema handles
        # slight column differences across chunks (e.g. MongoDB flexible schema).
        s3_path = f"s3a://{bucket_name}/ingested/{table_dir}"
        print(f"  📂 {dir_prefix} → {norm_name} (spark.read.parquet)", flush=True)
        try:
            df = (
                spark.read
                .option("mergeSchema", "true")
                .parquet(s3_path)
            )
            # Cast every column to string for consistency with the batch-upload path.
            from pyspark.sql import functions as _F
            from pyspark.sql.types import StringType as _ST
            for col_name in df.columns:
                df = df.withColumn(col_name, _F.col(col_name).cast(_ST()))
            row_count = df.count()
            dataframes[norm_name] = df
            print(f"  ✅ Loaded {norm_name} ({row_count:,} rows from {dir_prefix})", flush=True)
        except Exception as exc:
            print(f"  ❌ Could not read {dir_prefix}: {exc}", flush=True)
            traceback.print_exc()

    # ── Pass 2b: read flat files via MinIO SDK → pandas → Spark ─────────────
    # This is the original batch-upload path and remains unchanged so that
    # user-uploaded CSV / Excel / JSON / single-Parquet files still work.
    for obj in flat_objects:
        file_name = obj.object_name
        print(f"Processing file: {file_name}")
        try:
            base_name  = _os.path.splitext(_os.path.basename(file_name))[0]
            clean_name = re.sub(r"[^0-9a-zA-Z_]+", "_", base_name)

            result = load_file_from_minio(minio_client, bucket_name, file_name, spark)

            if isinstance(result, dict):
                # Multi-table file (Excel sheets / JSON multi-table)
                for tname, sdf in result.items():
                    dataframes[tname] = sdf
                    print(f"✅ Successfully loaded {file_name}:{tname}")
            elif result is not None:
                norm_name = normalize_name(clean_name)
                if norm_name is None:
                    print(f"No match found for {base_name}, skipping.")
                    continue
                if norm_name in dataframes:
                    # Flat file duplicates a chunked table (shouldn't happen in normal
                    # usage, but handle gracefully with a union)
                    dataframes[norm_name] = dataframes[norm_name].union(result)
                    print(f"  ➕ Merged {file_name} into {norm_name}")
                else:
                    dataframes[norm_name] = result
                    print(f"✅ Successfully loaded {file_name} as {norm_name}")
        except Exception as exc:
            print(f"Error processing {file_name}: {exc}")
            traceback.print_exc()

        except Exception as e:
            print(f"Error processing {file_name}: {str(e)}")
            traceback.print_exc()

    print(f"Loaded {len(dataframes)} dataframes: {', '.join(dataframes.keys())}")
    return dataframes


def load_file_from_minio(minio_client, bucket_name, file_name, spark):
    """
    Load a file from MinIO and convert to Spark DataFrame(s).
    
    Handles:
    - CSV: Single table
    - Excel: Multiple sheets (each sheet becomes a separate table)
    - Parquet: Single table
    - JSON: Can be single table or multi-table structure
    
    Args:
        minio_client: MinIO client instance
        bucket_name: Name of the bucket
        file_name: Path to the file in MinIO
        spark: SparkSession instance
        
    Returns:
        Single Spark DataFrame or dict of {table_name: Spark DataFrame} for multi-table files
    """
    
    obj = minio_client.get_object(bucket_name, file_name)
    data = obj.read()
    obj.close()
    obj.release_conn()
    
    print(f"File: {file_name}")
    print(f"File size: {len(data)} bytes")
    print(f"First few bytes: {data[:10]}")

    # Handle different file types
    if file_name.endswith(".csv"):
        pdf = pd.read_csv(BytesIO(data), dtype=str)
        spark_df = spark.createDataFrame(pdf)
        spark_df.cache()
        spark_df.count()
        return spark_df
        
    elif file_name.endswith(".xlsx") or file_name.endswith(".xls"):
        # Excel files can have multiple sheets - handle each sheet as a potential table
        excel_file = BytesIO(data)
        excel_file.seek(0)  # Ensure we're at the start of the stream
        xl_file = pd.ExcelFile(excel_file, engine='openpyxl')
        sheet_names = xl_file.sheet_names
        
        if len(sheet_names) == 1:
            # Single sheet - return as single DataFrame
            pdf = pd.read_excel(xl_file, sheet_name=0, dtype=str)
            spark_df = spark.createDataFrame(pdf)
            spark_df.cache()
            spark_df.count()
            return spark_df
        else:
            # Multiple sheets - return dict of DataFrames
            result = {}
            for sheet_name in sheet_names:
                pdf = pd.read_excel(xl_file, sheet_name=sheet_name, dtype=str)
                if not pdf.empty:  # Skip empty sheets
                    spark_df = spark.createDataFrame(pdf)
                    spark_df.cache()
                    spark_df.count()
                    # Normalize sheet name to match table naming
                    normalized_name = normalize_name(sheet_name)
                    if normalized_name:
                        result[normalized_name] = spark_df
                    else:
                        # If normalization fails, use sheet name directly
                        result[f"sheet_{sheet_name}"] = spark_df
            return result if result else None
            
    elif file_name.endswith(".parquet"):
        pdf = pd.read_parquet(BytesIO(data))
        pdf = pdf.astype(str)
        spark_df = spark.createDataFrame(pdf)
        spark_df.cache()
        spark_df.count()
        return spark_df
        
    elif file_name.endswith(".json"):
        # Try to parse JSON to detect structure
        json_data = json.loads(data.decode('utf-8'))
        
        # Check if it's the API format with "tables" key
        if isinstance(json_data, dict) and "tables" in json_data:
            # Multi-table format: {"tables": [{"table_name": "...", "data": [...]}]}
            result = {}
            for table_obj in json_data["tables"]:
                table_name = table_obj.get("table_name")
                table_data = table_obj.get("data", [])
                if table_name and table_data:
                    pdf = pd.DataFrame(table_data)
                    if not pdf.empty:
                        pdf = pdf.astype(str)
                        spark_df = spark.createDataFrame(pdf)
                        spark_df.cache()
                        spark_df.count()
                        # Normalize table name
                        normalized_name = normalize_name(table_name)
                        if normalized_name:
                            result[normalized_name] = spark_df
                        else:
                            result[table_name] = spark_df
            return result if result else None
            
        elif isinstance(json_data, dict) and not any(k in json_data for k in ["tables", "data"]):
            # Check if it's a nested structure with table keys
            # Example: {"customers": [{...}], "orders": [{...}]}
            result = {}
            has_valid_tables = False
            for key, value in json_data.items():
                # Check if value is a non-empty list with dict elements
                if isinstance(value, list) and len(value) > 0:
                    # Verify all elements are dictionaries
                    if all(isinstance(item, dict) for item in value):
                        # This looks like a table
                        pdf = pd.DataFrame(value)
                        if not pdf.empty:
                            pdf = pdf.astype(str)
                            spark_df = spark.createDataFrame(pdf)
                            spark_df.cache()
                            spark_df.count()
                            # Normalize table name
                            normalized_name = normalize_name(key)
                            if normalized_name:
                                result[normalized_name] = spark_df
                            else:
                                result[key] = spark_df
                            has_valid_tables = True
            
            if has_valid_tables:
                return result
            # Otherwise fall through to default JSON handling
        
        # Default: treat as single table with array of records
        pdf = pd.read_json(BytesIO(data), dtype=str)
        spark_df = spark.createDataFrame(pdf)
        spark_df.cache()
        spark_df.count()
        return spark_df
        
    else:
        raise ValueError(f"Unsupported file format: {file_name}")
