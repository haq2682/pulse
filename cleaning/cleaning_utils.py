"""
Utility functions for data operations.
"""

from io import BytesIO


def load_data_from_minio(spark, minio_client, bucket_name, table_names):
    """
    Load data from MinIO from the mapped/ directory.

    Args:
        spark (SparkSession): Active Spark session
        minio_client (Minio): MinIO client instance
        bucket_name (str): MinIO bucket name
        table_names (list): List of table names to load

    Returns:
        dict: Dictionary of table names to DataFrames
    """
    dataframes = {}
    
    for table_name in table_names:
        file_path = f"mapped/{table_name}.csv"
        try:
            # Check if the file exists
            minio_client.stat_object(bucket_name, file_path)
            
            df = (
                spark.read.option("header", "true")
                .option("inferSchema", "true")
                .csv(f"s3a://{bucket_name}/{file_path}")
            )
            dataframes[table_name] = df
            print(f"Loaded {table_name} with {df.count()} rows from {file_path}")
        except Exception as e:
            print(f"Could not load {table_name}: {e}")
    
    return dataframes


def save_data_to_minio(dataframes, minio_client, bucket_name):
    """
    Save cleaned DataFrames to MinIO as CSV files in the cleaned/ directory.

    Args:
        dataframes (dict): Dictionary of table names to DataFrames
        minio_client (Minio): MinIO client instance
        bucket_name (str): MinIO bucket name
    """
    for table, df in dataframes.items():
        pdf = df.toPandas()
        csv_buffer = BytesIO()
        pdf.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        file_name = f"cleaned/{table}.csv"

        try:
            minio_client.put_object(
                bucket_name,
                file_name,
                csv_buffer,
                length=len(csv_buffer.getvalue()),
                content_type="text/csv",
            )
            print(f"✅ Saved {file_name} ({len(pdf)} rows)")
        except Exception as e:
            print(f"❌ Failed to save {file_name}: {str(e)}")
        finally:
            csv_buffer.close()


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
