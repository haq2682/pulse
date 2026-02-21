import os
import pyspark.sql.functions as F
from pyspark.sql import Window
from minio import Minio


def parse_minio_endpoint(endpoint_url):
    """
    Parse MinIO endpoint URL and strip protocol prefix and path if present.
    
    The MinIO Python client expects endpoint in format 'hostname:port' without
    protocol prefix or path components. This function handles various formats:
    - With protocol: 'http://localhost:9000' -> 'localhost:9000'
    - With protocol and path: 'http://localhost:9000/path' -> 'localhost:9000'
    - Without protocol: 'localhost:9000' -> 'localhost:9000'
    - Without protocol but with path: 'localhost:9000/path' -> 'localhost:9000'
    
    Args:
        endpoint_url: MinIO endpoint URL (e.g., 'localhost:9000' or 'http://localhost:9000/path')
        
    Returns:
        str: Endpoint in 'hostname:port' format
        
    Raises:
        ValueError: If endpoint is empty or invalid after parsing
    """
    if not endpoint_url:
        raise ValueError("MINIO_ENDPOINT cannot be empty")
    
    # Strip protocol prefix if present
    if "://" in endpoint_url:
        endpoint_url = endpoint_url.split("://", 1)[1]
    
    # Strip any path component (everything after the first /)
    if "/" in endpoint_url:
        endpoint_url = endpoint_url.split("/", 1)[0]
    
    # Validate that we have a non-empty endpoint after parsing
    endpoint_url = endpoint_url.strip()
    if not endpoint_url:
        raise ValueError("MINIO_ENDPOINT is invalid after removing protocol and path")
    
    return endpoint_url


def get_minio_client():
    """Create and return a MinIO client instance."""
    # Parse MINIO_ENDPOINT to strip protocol prefix if present
    minio_endpoint = parse_minio_endpoint(os.getenv("MINIO_ENDPOINT", "localhost:9000"))
    
    return Minio(
        minio_endpoint,
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False,
    )




def get_agg_tables(spark, db_config=None, bucket_name=None):
    """
    Load aggregated tables from MinIO transformed/ directory.
    
    Args:
        spark: SparkSession
        db_config: Deprecated parameter kept for backward compatibility
        bucket_name: MinIO bucket name (business_id). If None, uses default from env.
        
    Returns:
        dict: Dictionary of table names to Spark DataFrames
    """
    try:
        minio_client = get_minio_client()
        
        # Use provided bucket_name or fall back to env/default
        if bucket_name is None:
            bucket_name = os.getenv("MINIO_BUCKET", "pulse-bucket-1")
        
        print(f"Loading aggregated tables from MinIO bucket: {bucket_name}")
        print(f"Directory: transformed/")
        
        # List all CSV files in the transformed/ directory
        objects = minio_client.list_objects(bucket_name, prefix="transformed/", recursive=True)
        
        spark_dfs = {}
        tables_found = []
        
        for obj in objects:
            if not obj.object_name.endswith(".parquet"):
                continue
                
            # Extract table name from path: transformed/{table_name}.parquet
            table_name = obj.object_name.replace("transformed/", "").replace(".parquet", "")
            tables_found.append(table_name)
        
        print(f"Found tables: {tables_found}")
        
        for table_name in tables_found:
            try:
                file_path = f"transformed/{table_name}.parquet"
                print(f"Loading table: {table_name}...")
                
                df = (
                    spark.read
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .parquet(f"s3a://{bucket_name}/{file_path}")
                )
                
                spark_dfs[table_name] = df
                print(f"  ✅ Loaded {table_name}: {df.count()} rows")
                
            except Exception as e:
                print(f"  ❌ Error loading {table_name}: {e}")
        
        return spark_dfs

    except Exception as e:
        print(f"Error loading tables from MinIO: {e}")
        import traceback
        traceback.print_exc()
        return {}


def is_column_all_null_or_zero(df, col_name):
    if df is None: 
        return True                    

    if col_name not in df.columns:
        return True                   

    col_type = dict(df.dtypes)[col_name]
    non_null_count = df.agg(
        F.count(F.col(col_name)).alias("non_null_count")
    ).collect()[0]["non_null_count"]
    
    if non_null_count == 0:
        return True                   

    if col_type in ("int", "bigint", "double", "float", "decimal", "smallint", "tinyint"):
        non_zero_non_null_count = df.agg(
            F.sum(
                F.when(
                    (F.col(col_name).isNotNull()) & (F.col(col_name) != 0), 1
                ).otherwise(0)
            ).alias("non_zero_non_null_count")
        ).collect()[0]["non_zero_non_null_count"]

        if non_zero_non_null_count == 0:
            return True                 

    return False

def add_time_grain(df, date_col, grain="day"):
    """
    Adds time grain columns safely. 
    Standardizes 'day' by removing time components.
    Standardizes 'week' and 'month' by using the start date of that period.
    """
    
    # 1. Day: Ensure we remove time components (hh:mm:ss)
    if grain == "day":
        return df.withColumn("grain_date", F.to_date(F.col(date_col)))

    # 2. Week: Use date_trunc to get the Monday of that week
    # This solves the year-crossover issue perfectly.
    elif grain == "week":
        # Returns a DATE column representing the start of the week
        # We also extract year/week for your group_cols logic
        return df.withColumn("grain_date", F.to_date(F.date_trunc("week", F.col(date_col)))) \
                 .withColumn("grain_year", F.year(F.col(date_col))) \
                 .withColumn("grain_week", F.weekofyear(F.col(date_col)))

    # 3. Month: Truncate to the 1st of the month
    elif grain == "month":
        return df.withColumn("grain_date", F.to_date(F.trunc(F.col(date_col), "month"))) \
                 .withColumn("grain_year", F.year(F.col(date_col))) \
                 .withColumn("grain_month", F.month(F.col(date_col)))

    else:
        raise ValueError("grain must be 'day', 'week', or 'month'")


def check_null_dataframes(dataframe):
    empty_dataframes = []
    all_null_dataframes = []
    has_null_values = []

    for key, df in dataframe.items():
        if df is None:
            empty_dataframes.append(key)
            print(f"dataframe['{key}'] is None")
        else: 
            row_count = df.count()
            
            if row_count == 0:
                empty_dataframes.append(key)
                print(f"dataframe['{key}'] has 0 rows")
            else:
                null_count = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0]
                
                if all(null_count[c] == row_count for c in df.columns):
                    all_null_dataframes.append(key)
                    print(f"dataframe['{key}'] has all NULL values in every column for all {row_count} rows")
                elif any(null_count[c] > 0 for c in df.columns):
                    null_cols = [c for c in df.columns if null_count[c] > 0]
                    has_null_values.append((key, null_cols))
                    print(f"dataframe['{key}'] has some NULL values in columns: {null_cols}")
                else:
                    print(f"dataframe['{key}'] has no NULL values ({row_count} rows)")

    print("\n" + "="*80)
    print("SUMMARY REPORT")
    print("="*80)
    print(f"\n❌ Empty/None DataFrames ({len(empty_dataframes)}):")
    for key in empty_dataframes:
        print(f"   - {key}")
    print(f"\n⚠️  All-NULL DataFrames ({len(all_null_dataframes)}):")
    for key in all_null_dataframes: 
        print(f"   - {key}")
    print(f"\n⚡ DataFrames with Some NULL values ({len(has_null_values)}):")
    for key, cols in has_null_values: 
        print(f"   - {key}:  {cols}")
