"""
Main data cleaning pipeline for e-commerce data.

This script orchestrates the complete data cleaning process including:
- Loading data from MinIO
- Schema casting
- Merging related tables
- Handling duplicates and null values
- Removing outliers
- Validating dates and timestamps
- Detecting and cleaning gibberish patterns
- Advanced text cleaning and validation
- Saving cleaned data back to MinIO
- Incremental processing support to avoid reprocessing files
"""

import argparse
from cleaning_config import create_spark_session, create_minio_client, get_bucket_name
from schema import cast_dataframes
from merge import merge_tables
from data_cleaning import (
    check_duplicates,
    drop_duplicates,
    drop_null_keys,
    check_nulls,
    fill_null_values,
    impute_all_numeric,
    clean_text_columns,
    clean_numeric_strings,
    clean_whitespace_issues,
    clean_mixed_scripts,
    validate_all_cleaned_data,
)
from standardization import (
    remove_all_outliers,
    normalize_dates_and_timestamps,
    validate_dates_and_timestamps,
    detect_gibberish_patterns,
    convert_currency_columns,
)
from cleaning_utils import (
    load_data_from_minio,
    save_data_to_minio,
    display_summary,
    get_file_paths_from_minio
)
from incremental_cleaner import IncrementalCleaner
from pyspark.sql.functions import regexp_extract, col


def main(bucket_name=None, incremental=True, force_full=False):
    """
    Main function to execute the data cleaning pipeline.
    
    Args:
        bucket_name: MinIO bucket name (business_id). If None, uses default from config.
        incremental: If True, only process new files (default: True)
        force_full: If True, reset state and reprocess all files (default: False)
    """
    print("=" * 60)
    print("🚀 STARTING DATA CLEANING PIPELINE")
    if incremental and not force_full:
        print("   Mode: INCREMENTAL (processing only new files)")
    else:
        print("   Mode: FULL (processing all files)")
    print("=" * 60)

    # 1. Initialize Spark and MinIO
    print("\n📌 Step 1: Initializing Spark and MinIO...")
    spark = create_spark_session()
    minio_client = create_minio_client()
    
    # Use provided bucket_name or fall back to default
    if bucket_name is None:
        bucket_name = get_bucket_name()
    
    print(f"✅ Initialization complete - Using bucket: {bucket_name}")

    # 1a. Initialize incremental cleaner if in incremental mode
    cleaner = None
    if incremental:
        print("\n📌 Step 1a: Initializing incremental cleaner...")
        cleaner = IncrementalCleaner()
        
        if force_full:
            print("⚠️  Force full mode: Resetting state table...")
            cleaner.reset_state()
        
        # Show state summary
        summary = cleaner.get_state_summary()
        if summary.get('total_files', 0) > 0:
            print(f"   Previously processed: {summary['total_files']} files")
            print(f"   Last processed: {summary.get('last_processed', 'N/A')}")

    # 2. Load data from MinIO
    print("\n📌 Step 2: Loading data from MinIO...")
    table_names = [
        "addresses",
        "categories",
        "customer_sessions",
        "customers",
        "inventory",
        "marketing_campaigns",
        "order_items",
        "orders",
        "payments",
        "products",
        "reviews",
        "shopping_cart",
        "cart_items",
        "suppliers",
        "wishlist",
    ]

    # Get available file paths
    all_file_paths = get_file_paths_from_minio(minio_client, bucket_name, table_names, folder="mapped")
    print(f"   Found {len(all_file_paths)} files in MinIO")
    
    # Filter to unprocessed files if in incremental mode
    if incremental and cleaner:
        file_paths_to_process = cleaner.get_unprocessed_files(all_file_paths)
        if not file_paths_to_process:
            print("\n✅ No new files to process. Cleaning pipeline complete!")
            spark.stop()
            return
    else:
        file_paths_to_process = all_file_paths
    
    # Extract table names from file paths
    tables_to_process = [fp.split('/')[-1].replace('.csv', '') for fp in file_paths_to_process]
    print(f"   Processing {len(tables_to_process)} tables: {', '.join(tables_to_process)}")

    dataframes, processed_file_paths = load_data_from_minio(
        spark, minio_client, bucket_name, tables_to_process, folder="mapped"
    )
    # print(f"✅ Loaded {len(dataframes)} tables")

    # 2a. Clean ID columns with regex
    print("\n🔌 Step 2a: Cleaning ID columns with regex extraction...")
    for table in dataframes.keys():
        df = dataframes[table]
        for column in df.columns:
            if column.endswith("_id") and not column.startswith("session_id"):
                # Extract only numeric part of IDs, set non-numeric to NULL
                df = df.withColumn(
                    column,
                    when(
                        regexp_extract(col(column), r"(\d+)", 1) == "",
                        None,
                    ).otherwise(regexp_extract(col(column), r"(\d+)", 1)),
                )
        dataframes[table] = df
    print("✅ ID columns cleaned")

    # 3. Cast data types
    print("\n📌 Step 3: Casting DataFrames to correct data types...")
    dataframes = cast_dataframes(dataframes)

    # 4. Merge related tables
    print("\n📌 Step 4: Merging related tables...")
    dataframes = merge_tables(dataframes, spark)

    # 5. Handle duplicates
    print("\n📌 Step 5: Checking for duplicates...")
    check_duplicates(dataframes)
    print("\n📌 Step 5a: Removing duplicates...")
    dataframes = drop_duplicates(dataframes)
    print("✅ Duplicates removed")

    # 6. Drop null primary/foreign keys
    print("\n📌 Step 6: Dropping rows with null keys...")
    dataframes = drop_null_keys(dataframes)
    print("✅ Null keys handled")

    # 7. Check null values
    print("\n📌 Step 7: Checking null values...")
    check_nulls(dataframes)

    # 8. Fill null values in non-numeric columns
    print("\n📌 Step 8: Filling null values in non-numeric columns...")
    dataframes = fill_null_values(dataframes)
    print("✅ Non-numeric nulls filled")

    # 9. Impute numeric null values
    print("\n📌 Step 9: Imputing numeric null values...")
    dataframes = impute_all_numeric(dataframes)
    print("✅ Numeric values imputed")

    # 10. Check nulls after imputation
    print("\n📌 Step 10: Final null check...")
    check_nulls(dataframes)

    # 11. Remove outliers
    print("\n📌 Step 11: Removing outliers...")
    dataframes = remove_all_outliers(dataframes)
    print("✅ Outliers removed")

    # 12. Validate dates and timestamps
    print("\n📌 Step 12: Normalizing & Validating dates and timestamps...")
    dataframes = normalize_dates_and_timestamps(dataframes)
    dataframes = validate_dates_and_timestamps(dataframes)

    # 13. Detect and clean gibberish patterns
    print("\n📌 Step 13: Detecting and cleaning gibberish patterns...")
    dataframes = detect_gibberish_patterns(dataframes)

    # 14. Clean text columns for gibberish using linguistic analysis
    print("\n📌 Step 14: Cleaning text columns with linguistic analysis...")
    dataframes = clean_text_columns(dataframes)
    # 15. Clean numeric strings (IDs, status codes, validation)
    print("\n📌 Step 15: Cleaning numeric strings...")
    dataframes = clean_numeric_strings(dataframes)

    # 16. Clean whitespace and formatting issues
    print("\n📌 Step 16: Cleaning whitespace and formatting...")
    dataframes = clean_whitespace_issues(dataframes)
    # 17. Clean mixed scripts and non-ASCII characters
    print("\n📌 Step 17: Cleaning mixed scripts and non-ASCII characters...")
    dataframes = clean_mixed_scripts(dataframes)
    # 18. Final data validation
    print("\n📌 Step 18: Running final data validation...")
    dataframes = validate_all_cleaned_data(dataframes)

    # 19. Convert currency columns
    print("\n📌 Step 19: Converting currency columns...")
    dataframes = convert_currency_columns(dataframes, bucket_name)

    # 20. Display summary
    print("\n📌 Step 20: Generating summary...")
    display_summary(dataframes)

    # 21. Save cleaned data
    print("\n📌 Step 21: Saving cleaned data to MinIO...")
    save_data_to_minio(dataframes, minio_client, bucket_name)

    # 21a. Mark files as processed if in incremental mode
    if incremental and cleaner:
        print("\n📌 Step 21a: Marking files as processed...")
        file_records = {}
        for table_name, file_path in processed_file_paths.items():
            if table_name in dataframes:
                df = dataframes[table_name]
                record_count = df.count()
                file_records[file_path] = {
                    'record_count': record_count,
                    'file_size': None,  # Could calculate if needed
                    'checksum': None    # Could calculate if needed
                }
        cleaner.mark_multiple_processed(file_records)

    # 22. Stop Spark session
    print("\n📌 Step 22: Stopping Spark session...")
    spark.stop()
    print("✅ Spark session stopped")

    print("\n" + "=" * 60)
    print("🎉 DATA CLEANING PIPELINE COMPLETED SUCCESSFULLY!")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Data cleaning pipeline for e-commerce data')
    parser.add_argument('--bucket-name', type=str, help='MinIO bucket name (business_id)')
    parser.add_argument('--full', action='store_true', 
                       help='Run full cleaning (process all files, not just new ones)')
    parser.add_argument('--force-full', action='store_true',
                       help='Reset state and reprocess all files')
    args = parser.parse_args()
    
    incremental = not args.full
    main(bucket_name=args.bucket_name, incremental=incremental, force_full=args.force_full)
