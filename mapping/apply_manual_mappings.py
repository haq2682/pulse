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
                        # Track failed mapping
                        warning_msg = f"Source column '{source_col}' not found in table '{table_name}'"
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
