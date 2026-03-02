#!/usr/bin/env python3
"""
Apply manual mappings to already-mapped files in MinIO.
This script reads files from the mapped-temp folder, applies manual column mappings,
and moves the results to the mapped folder.
"""

import sys
import os
import json
import pandas as pd
from io import BytesIO, StringIO
from minio import Minio
from dotenv import load_dotenv, find_dotenv

# Add current directory to Python path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.helpers import parse_minio_endpoint

load_dotenv(find_dotenv())

# Initialize MinIO client
minio_endpoint = parse_minio_endpoint(os.getenv("MINIO_ENDPOINT", "localhost:9000"))
minio_client = Minio(
    minio_endpoint,
    access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    secure=False,
)


def load_ingested_files_cache(bucket_name: str):
    """
    Load all data files from the ingested folder and cache them.
    Supports CSV, JSON, Excel (.xlsx, .xls), and Parquet files.
    For Excel files, each sheet is cached separately.
    
    Args:
        bucket_name: Name of the MinIO bucket
    
    Returns:
        dict: Dictionary mapping table_name -> DataFrame for all ingested files
              For Excel files, table names follow format: filename_sheetname
    """
    from io import BytesIO  # Add this import if not already present
    
    ingested_cache = {}
    ingested_folder = "ingested/"
    
    # Supported file extensions
    supported_extensions = ['.csv', '.json', '.xlsx', '.xls', '.parquet']
    
    try:
        objects = list(minio_client.list_objects(bucket_name, prefix=ingested_folder, recursive=False))
        
        # Filter for supported file types
        data_objects = [obj for obj in objects 
                       if any(obj.object_name.endswith(ext) for ext in supported_extensions)]
        
        for obj in data_objects:
            filename = obj.object_name.replace(ingested_folder, '')
            
            # Determine file extension and base name
            file_ext = None
            for ext in supported_extensions:
                if filename.endswith(ext):
                    file_ext = ext
                    base_name = filename.replace(ext, '')
                    break
            
            try:
                response = minio_client.get_object(bucket_name, obj.object_name)
                file_data = response.read()
                response.close()
                response.release_conn()
                
                # Handle different file types
                if file_ext == '.csv':
                    df = pd.read_csv(StringIO(file_data.decode("utf-8")))
                    ingested_cache[base_name] = df
                    print(f"  Cached {base_name} from ingested/ ({len(df)} rows)")
                
                elif file_ext == '.json':
                    df = pd.read_json(StringIO(file_data.decode("utf-8")))
                    ingested_cache[base_name] = df
                    print(f"  Cached {base_name} from ingested/ ({len(df)} rows)")
                
                elif file_ext == '.parquet':
                    df = pd.read_parquet(BytesIO(file_data))
                    ingested_cache[base_name] = df
                    print(f"  Cached {base_name} from ingested/ ({len(df)} rows)")
                
                elif file_ext in ['.xlsx', '.xls']:
                    # Read all sheets from Excel file
                    excel_file = pd.ExcelFile(BytesIO(file_data), engine='openpyxl')
                    for sheet_name in excel_file.sheet_names:
                        df = pd.read_excel(excel_file, sheet_name=sheet_name)
                        # Create table name as filename_sheetname
                        table_name = f"{base_name}_{sheet_name}"
                        ingested_cache[table_name] = df
                        print(f"  Cached {table_name} from ingested/ ({len(df)} rows)")
                
            except Exception as e:
                print(f"  ⚠️  Warning: Could not load {obj.object_name}: {e}")
                continue
                
    except Exception as e:
        print(f"  ⚠️  Warning: Could not list ingested files: {e}")
    
    return ingested_cache


def find_column_in_ingested(column_name: str, ingested_cache: dict):
    """
    Search for a column across all ingested files.
    
    Args:
        column_name: Name of the column to find
        ingested_cache: Dictionary of ingested DataFrames
    
    Returns:
        tuple: (table_name, DataFrame) if found, else (None, None)
    """
    for table_name, df in ingested_cache.items():
        if column_name in df.columns:
            return table_name, df
    return None, None


def _detect_streaming_parquet_tables(bucket_name: str, temp_folder: str) -> dict:
    """
    Detect tables stored as partitioned Parquet under mapped-temp/ (db/api streaming mode).

    Layout written by _write_spark_native:
        mapped-temp/{table_name}/_batch_id=0/part-*.parquet

    Returns a dict mapping table_name -> list of MinIO object keys for that table's
    Parquet files.  Returns an empty dict if no Parquet files are found.
    """
    try:
        all_objects = list(
            minio_client.list_objects(bucket_name, prefix=temp_folder, recursive=True)
        )
    except Exception as e:
        print(f"  ⚠️  Could not list {temp_folder}: {e}")
        return {}

    tables = {}
    for obj in all_objects:
        if not obj.object_name.endswith(".parquet"):
            continue
        # Strip "mapped-temp/" prefix → "table_name/_batch_id=0/part-xxxx.parquet"
        rel = obj.object_name[len(temp_folder):]
        # First segment is the table name
        parts = rel.split("/", 1)
        if not parts:
            continue
        table_name = parts[0]
        tables.setdefault(table_name, []).append(obj.object_name)

    return tables


def _apply_mappings_to_df(df: pd.DataFrame, table_name: str, manual_mappings: dict,
                           ingested_cache: dict, updated_results: dict) -> pd.DataFrame:
    """
    Apply manual column renames (and cross-table lookups) to a DataFrame in-place.
    Shared by both CSV and Parquet paths.
    """
    if table_name not in manual_mappings:
        return df

    table_map = manual_mappings[table_name]
    print(f"  📝 Applying manual mappings for {table_name}...")

    for canonical_col, source_col in table_map.items():
        if source_col in df.columns:
            df.rename(columns={source_col: canonical_col}, inplace=True)
            print(f"     ✅ Mapped {source_col} → {canonical_col}")
        else:
            # Try cross-table lookup from ingested files
            print(f"     🔍 '{source_col}' not found in {table_name} — searching ingested files...")
            source_table, source_df = find_column_in_ingested(source_col, ingested_cache)

            if source_table is not None:
                min_len = min(len(df), len(source_df))
                df[canonical_col] = pd.NA
                df.loc[:min_len - 1, canonical_col] = source_df[source_col].iloc[:min_len].values
                print(
                    f"     ✅ Mapped {source_col} (from {source_table}) → {canonical_col} in {table_name}"
                )
            else:
                warning_msg = (
                    f"Source column '{source_col}' not found in {table_name} "
                    "or any ingested files"
                )
                print(f"     ⚠️  {warning_msg}")
                updated_results["failed_mappings"].append({
                    "table": table_name,
                    "canonical_column": canonical_col,
                    "source_column": source_col,
                    "error": warning_msg,
                    "stage": "column_mapping",
                })

    return df


def apply_manual_mappings_to_files(bucket_name: str, manual_mappings: dict):
    """
    Apply manual mappings to files in the mapped-temp folder.

    Supports two storage layouts written by the mapping pipeline:

    **Batch mode (CSV)**
        ``mapped-temp/{table_name}.csv``  →  ``mapped/{table_name}.csv``

    **Streaming mode — db / api (Parquet)**
        ``mapped-temp/{table_name}/_batch_id=0/part-*.parquet``
        →  ``mapped/{table_name}/_batch_id=0/part-0.parquet``

    Args:
        bucket_name: Name of the MinIO bucket
        manual_mappings: Dictionary of manual column mappings
                        Format: {table_name: {canonical_col: source_col}}

    Returns:
        dict: Updated mapping results with missing_cols and extra_cols
    """
    print(f"\n{'='*60}")
    print(f"Applying Manual Mappings to Bucket: {bucket_name}")
    print(f"{'='*60}\n")

    if not minio_client.bucket_exists(bucket_name):
        raise ValueError(f"Bucket '{bucket_name}' does not exist")

    updated_results = {
        "missing_cols": [],
        "extra_cols": [],
        "failed_mappings": []
    }

    # Load ingested files for cross-table column lookup (batch mode uses these;
    # streaming mode may not have an ingested/ folder — that's fine).
    print("📂 Loading ingested files for cross-table column lookup...")
    ingested_cache = load_ingested_files_cache(bucket_name)
    print(f"   Cached {len(ingested_cache)} ingested files\n")

    temp_folder = "mapped-temp/"

    # ── Detect storage format ─────────────────────────────────────────────────
    try:
        top_objects = list(
            minio_client.list_objects(bucket_name, prefix=temp_folder, recursive=False)
        )
    except Exception as e:
        raise ValueError(f"Failed to list objects in {temp_folder}: {e}")

    csv_objects = [obj for obj in top_objects if obj.object_name.endswith(".csv")]

    # Streaming mode: look for Parquet files one level deeper
    streaming_tables = {}
    if not csv_objects:
        streaming_tables = _detect_streaming_parquet_tables(bucket_name, temp_folder)

    if not csv_objects and not streaming_tables:
        raise ValueError(
            f"No mapped files found in {bucket_name}/{temp_folder}. "
            "Initial mapping may not have completed or files may have already been moved."
        )
    
    successfully_processed_files = []   # object keys to delete after success
    failed_tables = []

    try:
        # ── BATCH MODE: flat CSV files ────────────────────────────────────────
        if csv_objects:
            print(f"📋 Detected batch mode ({len(csv_objects)} CSV files)\n")
            for obj in csv_objects:
                table_name = obj.object_name.replace(temp_folder, "").replace(".csv", "")
                print(f"Processing table: {table_name}")

                try:
                    response = minio_client.get_object(bucket_name, obj.object_name)
                    csv_data = response.read().decode("utf-8")
                    df = pd.read_csv(StringIO(csv_data))
                    response.close()
                    response.release_conn()
                    print(f"  Loaded {len(df)} rows from {obj.object_name}")
                except Exception as e:
                    error_msg = f"Error loading {obj.object_name}: {e}"
                    print(f"  ⚠️  {error_msg}")
                    failed_tables.append(table_name)
                    updated_results["failed_mappings"].append(
                        {"table": table_name, "error": error_msg, "stage": "loading"}
                    )
                    continue

                df = _apply_mappings_to_df(df, table_name, manual_mappings,
                                            ingested_cache, updated_results)

                try:
                    mapped_file_name = f"mapped/{table_name}.csv"
                    csv_buffer = BytesIO()
                    df.to_csv(csv_buffer, index=False)
                    csv_buffer.seek(0)
                    minio_client.put_object(
                        bucket_name,
                        mapped_file_name,
                        csv_buffer,
                        length=len(csv_buffer.getvalue()),
                        content_type="text/csv",
                    )
                    print(f"  ✅ Saved to {mapped_file_name} ({len(df)} rows)")
                    csv_buffer.close()
                    successfully_processed_files.append(obj.object_name)
                except Exception as e:
                    error_msg = f"Error saving {mapped_file_name}: {e}"
                    print(f"  ⚠️  {error_msg}")
                    failed_tables.append(table_name)
                    updated_results["failed_mappings"].append(
                        {"table": table_name, "error": error_msg, "stage": "saving"}
                    )

        # ── STREAMING MODE: partitioned Parquet directories ───────────────────
        else:
            print(f"🌊 Detected streaming mode ({len(streaming_tables)} Parquet tables)\n")
            for table_name, parquet_keys in streaming_tables.items():
                print(f"Processing table: {table_name} ({len(parquet_keys)} parquet file(s))")

                # Read all parquet files for this table and concatenate
                frames = []
                load_failed = False
                for key in parquet_keys:
                    try:
                        response = minio_client.get_object(bucket_name, key)
                        data = response.read()
                        response.close()
                        response.release_conn()
                        frames.append(pd.read_parquet(BytesIO(data)))
                    except Exception as e:
                        error_msg = f"Error loading {key}: {e}"
                        print(f"  ⚠️  {error_msg}")
                        updated_results["failed_mappings"].append(
                            {"table": table_name, "error": error_msg, "stage": "loading"}
                        )
                        load_failed = True
                        break

                if load_failed or not frames:
                    failed_tables.append(table_name)
                    continue

                df = pd.concat(frames, ignore_index=True)
                print(f"  Loaded {len(df)} rows ({len(frames)} file(s))")

                df = _apply_mappings_to_df(df, table_name, manual_mappings,
                                            ingested_cache, updated_results)

                # Preserve Spark-compatible column types so schema-merge succeeds when
                # the next CDC micro-batch is appended by Spark to the same directory.
                # Spark writes _ingested_at via current_timestamp() → TimestampType
                # (pyarrow: timestamp[us, tz=UTC]).  Pandas may drop the timezone when
                # round-tripping through read_parquet / to_parquet, so we restore it
                # explicitly here to keep the file schema identical to what Spark writes.
                if "_ingested_at" in df.columns:
                    df["_ingested_at"] = pd.to_datetime(df["_ingested_at"], utc=True)

                # Write to mapped/{table_name}/_batch_id=0/part-0.parquet
                # This matches the layout expected by cleaning_utils / Spark downstream.
                try:
                    mapped_file_name = f"mapped/{table_name}/_batch_id=0/part-0.parquet"
                    parquet_buffer = BytesIO()
                    df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
                    parquet_buffer.seek(0)
                    parquet_bytes = parquet_buffer.getvalue()
                    minio_client.put_object(
                        bucket_name,
                        mapped_file_name,
                        BytesIO(parquet_bytes),
                        length=len(parquet_bytes),
                        content_type="application/octet-stream",
                    )
                    print(f"  ✅ Saved to {mapped_file_name} ({len(df)} rows)")
                    successfully_processed_files.extend(parquet_keys)
                except Exception as e:
                    error_msg = f"Error saving parquet for {table_name}: {e}"
                    print(f"  ⚠️  {error_msg}")
                    failed_tables.append(table_name)
                    updated_results["failed_mappings"].append(
                        {"table": table_name, "error": error_msg, "stage": "saving"}
                    )

    finally:
        # Remove ONLY successfully migrated source files from mapped-temp/
        print(f"\n🧹 Cleaning up temporary files...")
        if successfully_processed_files:
            print(f"  Removing {len(successfully_processed_files)} source file(s)...")
            for file_path in successfully_processed_files:
                try:
                    minio_client.remove_object(bucket_name, file_path)
                    print(f"  ✅ Removed {file_path}")
                except Exception as e:
                    print(f"  ⚠️  Error removing {file_path}: {e}")

        if failed_tables:
            print(f"\n  ⚠️  {len(failed_tables)} table(s) had errors and were NOT moved:")
            for t in failed_tables:
                print(f"     - {t}")
            print(f"  These files remain in {bucket_name}/mapped-temp/ for manual review")
    
    print(f"\n{'='*60}")
    if not failed_tables:
        print(f"✅ Manual Mappings Applied Successfully")
        print(f"   Processed {len(successfully_processed_files)} file(s) across tables")
        print(f"   Files moved from {bucket_name}/mapped-temp/ to {bucket_name}/mapped/")
    else:
        print(f"⚠️  Manual Mappings Completed with Warnings")
        print(f"   Successfully processed: {len(streaming_tables) + len(csv_objects) - len(failed_tables)} table(s)")
        print(f"   Failed: {len(failed_tables)} table(s) (remain in mapped-temp)")
    print(f"{'='*60}\n")
    
    return updated_results


def load_mapping_results_from_files(bucket_name: str, manual_mappings: dict):
    """
    Load existing mapping results and update them based on manual mappings.
    
    Args:
        bucket_name: Name of the MinIO bucket
        manual_mappings: Dictionary of manual column mappings
    
    Returns:
        dict: Updated mapping results
    """
    import redis
    
    try:
        redis_client = redis.Redis(
            host=os.getenv("REDIS_HOST", "redis"),
            port=6379,
            decode_responses=True
        )
        
        # Get existing mapping results from Redis
        mapping_results_str = redis_client.get(f"mapping_results:{bucket_name}")
        if mapping_results_str:
            mapping_results = json.loads(mapping_results_str)
        else:
            mapping_results = {"missing_cols": [], "extra_cols": []}
        
        # Update missing_cols based on manual mappings
        updated_missing_cols = []
        for missing_item in mapping_results.get("missing_cols", []):
            table = missing_item.get("table")
            column = missing_item.get("column")
            
            # Check if this column was manually mapped
            if table in manual_mappings and column in manual_mappings[table]:
                # Column was mapped, don't include it in missing anymore
                print(f"  ✅ Removing {column} from missing columns for {table}")
            else:
                # Column still missing
                updated_missing_cols.append(missing_item)
        
        mapping_results["missing_cols"] = updated_missing_cols
        
        # Save updated results back to Redis
        redis_client.setex(
            f"mapping_results:{bucket_name}",
            86400,  # 24 hours
            json.dumps(mapping_results)
        )
        
        # Clear the streaming temp folder flag so subsequent batches go to mapped/
        redis_client.delete(f"streaming_use_temp:{bucket_name}")
        print(f"   Cleared streaming temp folder flag")
        
        print(f"\n✅ Updated mapping results in Redis")
        print(f"   Remaining missing columns: {len(updated_missing_cols)}")
        
        return mapping_results
        
    except Exception as e:
        print(f"⚠️  Error updating mapping results in Redis: {e}")
        return {"missing_cols": [], "extra_cols": []}


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Apply manual mappings to mapped files")
    parser.add_argument("--bucket-name", required=True, help="MinIO bucket name")
    parser.add_argument("--manual-mappings", required=True, help="JSON string of manual mappings")
    
    args = parser.parse_args()
    
    manual_mappings = json.loads(args.manual_mappings)
    
    # Apply manual mappings to files
    apply_manual_mappings_to_files(args.bucket_name, manual_mappings)
    
    # Update mapping results in Redis
    load_mapping_results_from_files(args.bucket_name, manual_mappings)
