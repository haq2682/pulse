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


def apply_manual_mappings_to_files(bucket_name: str, manual_mappings: dict):
    """
    Apply manual mappings to files in the mapped-temp folder.
    
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
        "failed_mappings": []  # Track mappings that failed to apply
    }
    
    # Load all ingested files into cache for cross-table column lookup
    print(f"📂 Loading ingested files for cross-table column lookup...")
    ingested_cache = load_ingested_files_cache(bucket_name)
    print(f"   Cached {len(ingested_cache)} ingested files\n")
    
    # List all files in mapped-temp folder
    temp_folder = "mapped-temp/"
    try:
        objects = list(minio_client.list_objects(bucket_name, prefix=temp_folder, recursive=False))
    except Exception as e:
        raise ValueError(f"Failed to list objects in {temp_folder}: {e}")
    
    # Filter to CSV files only
    csv_objects = [obj for obj in objects if obj.object_name.endswith('.csv')]
    
    if not csv_objects:
        raise ValueError(f"No CSV files found in {bucket_name}/{temp_folder}. Initial mapping may not have completed or files may have already been moved.")
    
    # Track successfully processed files for cleanup
    successfully_processed_files = []
    failed_files = []
    
    # Use try-finally to ensure cleanup happens even if there's an error
    try:
        for obj in csv_objects:
            # Extract table name from file path (e.g., "mapped-temp/customers.csv" -> "customers")
            table_name = obj.object_name.replace(temp_folder, '').replace('.csv', '')
            
            print(f"Processing table: {table_name}")
            
            # Load the CSV file from MinIO
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
                failed_files.append(obj.object_name)
                updated_results["failed_mappings"].append({
                    "table": table_name,
                    "error": error_msg,
                    "stage": "loading"
                })
                continue
            
            # Get current columns
            current_columns = set(df.columns)
            
            # Apply manual mappings for this table if provided
            if table_name in manual_mappings:
                print(f"  📝 Applying manual mappings for {table_name}...")
                table_mappings = manual_mappings[table_name]
                
                for canonical_col, source_col in table_mappings.items():
                    if source_col in df.columns:
                        # Rename the column
                        df.rename(columns={source_col: canonical_col}, inplace=True)
                        print(f"     ✅ Mapped {source_col} → {canonical_col}")
                        
                        # Update column sets
                        current_columns.discard(source_col)
                        current_columns.add(canonical_col)
                    else:
                        # Source column not found in mapped-temp file
                        # Search for it across ALL ingested files
                        print(f"     🔍 Searching for '{source_col}' across ingested files...")
                        source_table, source_df = find_column_in_ingested(source_col, ingested_cache)
                        
                        if source_table is not None:
                            # Found the source column in another ingested file
                            print(f"     ✅ Found '{source_col}' in ingested/{source_table}.csv")
                            
                            # Add the column data from source to target dataframe
                            # Handle case where dataframes have different lengths
                            if len(source_df) != len(df):
                                print(f"     ⚠️  Row count mismatch: {table_name} has {len(df)} rows, {source_table} has {len(source_df)} rows")
                                print(f"        Using first {min(len(df), len(source_df))} rows for mapping")
                            
                            # Copy column data (truncate or pad as necessary)
                            min_len = min(len(df), len(source_df))
                            df[canonical_col] = pd.NA  # Initialize with pd.NA for better null handling
                            df.loc[:min_len-1, canonical_col] = source_df[source_col].iloc[:min_len].values
                            
                            # Warn if there are unmapped rows
                            if len(df) > min_len:
                                print(f"        ⚠️  {len(df) - min_len} rows in {table_name} will have {canonical_col}=NA due to insufficient source data")
                            
                            print(f"     ✅ Mapped {source_col} (from {source_table}) → {canonical_col} (in {table_name})")
                            current_columns.add(canonical_col)
                        else:
                            # Column not found anywhere
                            warning_msg = f"Source column '{source_col}' not found in mapped-temp/{table_name}.csv or any ingested files"
                            print(f"     ⚠️  {warning_msg}")
                            updated_results["failed_mappings"].append({
                                "table": table_name,
                                "canonical_column": canonical_col,
                                "source_column": source_col,
                                "error": warning_msg,
                                "stage": "column_mapping"
                            })
            
            # Save the updated dataframe to the mapped folder
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
                
                # Track successfully processed file for cleanup
                successfully_processed_files.append(obj.object_name)
            except Exception as e:
                error_msg = f"Error saving {mapped_file_name}: {e}"
                print(f"  ⚠️  {error_msg}")
                failed_files.append(obj.object_name)
                updated_results["failed_mappings"].append({
                    "table": table_name,
                    "error": error_msg,
                    "stage": "saving"
                })
                # Don't add to successfully_processed_files since save failed
    
    finally:
        # Clean up: Remove ONLY successfully processed files from mapped-temp folder
        print(f"\n🧹 Cleaning up temporary files...")
        
        if successfully_processed_files:
            print(f"  Removing {len(successfully_processed_files)} successfully processed files...")
            for file_path in successfully_processed_files:
                try:
                    minio_client.remove_object(bucket_name, file_path)
                    print(f"  ✅ Removed {file_path}")
                except Exception as e:
                    print(f"  ⚠️  Error removing {file_path}: {e}")
        
        if failed_files:
            print(f"\n  ⚠️  {len(failed_files)} files had errors and were NOT moved:")
            for file_path in failed_files:
                print(f"     - {file_path}")
            print(f"  These files remain in {bucket_name}/mapped-temp/ for manual review")
    
    print(f"\n{'='*60}")
    if len(failed_files) == 0:
        print(f"✅ Manual Mappings Applied Successfully")
        print(f"   Processed {len(successfully_processed_files)} tables")
        print(f"   Files moved from {bucket_name}/mapped-temp/ to {bucket_name}/mapped/")
    else:
        print(f"⚠️  Manual Mappings Completed with Warnings")
        print(f"   Successfully processed: {len(successfully_processed_files)} tables")
        print(f"   Failed: {len(failed_files)} tables (remain in mapped-temp)")
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
