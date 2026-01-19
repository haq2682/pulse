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
)
from cleaning_utils import load_data_from_minio, save_data_to_minio, display_summary
from pyspark.sql.functions import regexp_extract, col, when


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
        "cart_items",
        "suppliers",
        "wishlist",
    ]

    dataframes = load_data_from_minio(spark, minio_client, bucket_name, table_names)
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
    # 19. Display summary
    print("\n📌 Step 19: Generating summary...")
    display_summary(dataframes)
    # 20. Save cleaned data
    print("\n📌 Step 20: Saving cleaned data to MinIO...")
    save_data_to_minio(dataframes, minio_client, bucket_name)

    # 21. Stop Spark session
    print("\n📌 Step 21: Stopping Spark session...")
    spark.stop()
    print("✅ Spark session stopped")

    print("\n" + "=" * 60)
    print("🎉 DATA CLEANING PIPELINE COMPLETED SUCCESSFULLY!")
    print("=" * 60)


if __name__ == "__main__":
    main()
