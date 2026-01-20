"""
Utility functions for data loading and analytical calculations.
"""
import psycopg2
import pyspark.sql.functions as F
from pyspark.sql import Window

def get_agg_tables(spark, db_config):
    """
    Connects to Postgres, finds all tables starting with 'agg_', 
    and loads them into Spark DataFrames.

    Args:
        spark (SparkSession): Active Spark session
        db_config (dict): Database configuration dictionary

    Returns:
        dict: Dictionary of table names to Spark DataFrames
    """
    try:
        print(f"Connecting to Postgres at {db_config['host']}:{db_config['port']}...")
        conn = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['database'],
            user=db_config['user'],
            password=db_config['password']
        )
        cursor = conn.cursor()

        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name LIKE 'agg_%'
        """)
        
        tables = [row[0] for row in cursor.fetchall()]
        print(f"Found tables: {tables}")

        spark_dfs = {}
        
        connection_properties = {
            "user": db_config['user'],
            "password": db_config['password'],
            "driver": db_config['driver']
        }

        for table in tables:
            print(f"Processing table: {table}...")
            df = spark.read.jdbc(
                url=db_config['url'], 
                table=f'"{table}"', 
                properties=connection_properties
            )
            spark_dfs[table] = df

        cursor.close()
        conn.close()
        return spark_dfs

    except Exception as e:
        print(f"Error loading tables: {e}")
        import traceback
        traceback.print_exc()
        return {}

def is_column_all_null_or_zero(df, col_name):
    """
    Checks if a column in a dataframe is entirely Null or Zero.
    """
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
    Adds time grain columns (day, week, or month) to a DataFrame.
    """
    if grain == "day":
        return df.withColumn("grain_date", F.col(date_col))
    elif grain == "week":
        return df.withColumn("grain_year", F.year(date_col)) \
                 .withColumn("grain_week", F.weekofyear(date_col))
    elif grain == "month":
        return df.withColumn("grain_year", F.year(date_col)) \
                 .withColumn("grain_month", F.month(date_col))
    else:
        raise ValueError("grain must be 'day', 'week', or 'month'")



def check_null_dataframes(dataframe):
    # Initialize tracking lists
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
                
                # Check if all columns are completely null
                if all(null_count[c] == row_count for c in df.columns):
                    all_null_dataframes.append(key)
                    print(f"dataframe['{key}'] has all NULL values in every column for all {row_count} rows")
                # Check if some columns have nulls
                elif any(null_count[c] > 0 for c in df.columns):
                    null_cols = [c for c in df.columns if null_count[c] > 0]
                    has_null_values.append((key, null_cols))
                    print(f"dataframe['{key}'] has some NULL values in columns: {null_cols}")
                else:
                    print(f"dataframe['{key}'] has no NULL values ({row_count} rows)")

        # Summary report
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
        print(f"   - {key}: {cols}")
        