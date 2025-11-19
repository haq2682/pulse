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
- Saving cleaned data back to MinIO
"""

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
)
from standardization import (
    remove_all_outliers,
    validate_dates_and_timestamps,
    detect_gibberish_patterns,
)
from cleaning_utils import load_data_from_minio, save_data_to_minio, display_summary


def main():
    """
    Main function to execute the data cleaning pipeline.
    """
    print("=" * 60)
    print("🚀 STARTING DATA CLEANING PIPELINE")
    print("=" * 60)

    # 1. Initialize Spark and MinIO
    print("\n📌 Step 1: Initializing Spark and MinIO...")
    spark = create_spark_session()
    minio_client = create_minio_client()
    bucket_name = get_bucket_name()
    print("✅ Initialization complete")

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
        "suppliers",
        "wishlist",
    ]

    dataframes = load_data_from_minio(spark, minio_client, bucket_name, table_names)
    print(f"✅ Loaded {len(dataframes)} tables")

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
    print("\n📌 Step 12: Validating dates and timestamps...")
    dataframes = validate_dates_and_timestamps(dataframes)

    # 13. Detect and clean gibberish patterns
    print("\n📌 Step 13: Detecting and cleaning gibberish patterns...")
    dataframes = detect_gibberish_patterns(dataframes)

    # 14. Display summary
    print("\n📌 Step 14: Generating summary...")
    display_summary(dataframes)

    # 15. Save cleaned data
    print("\n📌 Step 15: Saving cleaned data to MinIO...")
    save_data_to_minio(dataframes, minio_client, bucket_name)

    # 16. Stop Spark session
    print("\n📌 Step 16: Stopping Spark session...")
    spark.stop()
    print("✅ Spark session stopped")

    print("\n" + "=" * 60)
    print("🎉 DATA CLEANING PIPELINE COMPLETED SUCCESSFULLY!")
    print("=" * 60)


if __name__ == "__main__":
    main()
