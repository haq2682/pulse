"""
Utility functions for data operations.
"""

from io import BytesIO


def load_data_from_minio(spark, minio_client, bucket_name, table_names):
    """
    Load data from MinIO using pure MinIO client (bypasses Spark S3A issues).
    Best for debugging or when Spark read hangs.

    Args:
        spark (SparkSession): Active Spark session
        minio_client (Minio): MinIO client instance
        bucket_name (str): MinIO bucket name
        table_names (list): Optional list of table names to filter

    Returns:
        dict: Dictionary of table names to DataFrames
    """
    import pandas as pd
    from io import BytesIO
    import time
    
    print(f"📂 Scanning bucket '{bucket_name}' for mapped files...")
    
    try:
        objects = list(minio_client.list_objects(bucket_name, prefix="mapped_", recursive=True))
        print(f"✅ Found {len(objects)} files")
    except Exception as e:
        print(f"❌ Error accessing MinIO: {e}")
        return {}
    
    dataframes = {}
    
    for idx, obj in enumerate(objects, 1):
        object_name = obj.object_name.replace("mapped_", "").replace(".csv", "")
        
        # Filter if needed
        if table_names and object_name not in table_names:
            continue
        
        print(f"\n[{idx}/{len(objects)}] 📥 Downloading: {obj.object_name} ({obj.size / 1024:.2f} KB)")
        
        start_time = time.time()
        
        try:
            # Download from MinIO
            response = minio_client.get_object(bucket_name, obj.object_name)
            data = response.read()
            response.close()
            response.release_conn()
            
            # Parse CSV with pandas
            df_pandas = pd.read_csv(BytesIO(data))
            
            # Convert to Spark DataFrame
            df_spark = spark.createDataFrame(df_pandas)
            
            load_time = time.time() - start_time
            
            dataframes[object_name] = df_spark
            
            print(f"   ✅ Loaded: {len(df_pandas):,} rows × {len(df_pandas.columns)} cols in {load_time:.2f}s")
            print(f"   📊 Schema: {list(df_pandas.columns)[:5]}")
            
        except Exception as e:
            print(f"   ❌ Error loading {obj.object_name}: {e}")
            continue
    
    print(f"\n{'='*60}")
    print(f"✅ Total tables loaded: {len(dataframes)}")
    for name, df in dataframes.items():
        print(f"   • {name}: {df.count():,} rows")
    print(f"{'='*60}\n")
    
    return dataframes


def save_data_to_minio(dataframes, minio_client, bucket_name):
    """
    Save cleaned DataFrames back to MinIO as CSV files.

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
        file_name = "cleaned_" + table + ".csv"

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
