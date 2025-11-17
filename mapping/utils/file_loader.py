import pandas as pd
from io import BytesIO
import os
import re
from utils.helpers import normalize_name


def load_all_files_from_minio(minio_client, bucket_name, spark):
    """
    Load all supported files from a MinIO bucket directly into Spark DataFrames.

    Args:
        minio_client: MinIO client instance
        bucket_name: Name of the bucket
        spark: SparkSession instance

    Returns:
        Dictionary of {df_name: Spark DataFrame}
    """
    dataframes = {}
    objects = minio_client.list_objects(bucket_name, recursive=True)
    print("Listing available files in the bucket...")

    for obj in objects:
        file_name = obj.object_name
        print(f"Processing file: {file_name}")

        if not any(file_name.endswith(ext) for ext in [".csv", ".xlsx", ".parquet", ".json"]):
            print(f"Skipping {file_name} - unsupported format")
            continue

        try:
            base_name = os.path.splitext(os.path.basename(file_name))[0]
            clean_name = re.sub(r"[^0-9a-zA-Z_]+", "_", base_name)
            norm_name = normalize_name(clean_name)
            if norm_name is None:
                print(f"No match found for {base_name}, skipping.")
                continue

            spark_df = load_file_from_minio(minio_client, bucket_name, file_name, spark)
            dataframes[norm_name] = spark_df
            print(f"✅ Successfully loaded {file_name} as {norm_name}")

        except Exception as e:
            print(f"Error processing {file_name}: {str(e)}")

    print(f"Loaded {len(dataframes)} dataframes: {', '.join(dataframes.keys())}")
    return dataframes


def load_file_from_minio(minio_client, bucket_name, file_name, spark):
    """
    Load a single file from MinIO and convert it to a Spark DataFrame in-memory.

    Args:
        minio_client: MinIO client instance
        bucket_name: Name of the bucket
        file_name: Name of the file
        spark: SparkSession instance

    Returns:
        Spark DataFrame
    """
    obj = minio_client.get_object(bucket_name, file_name)
    data = obj.read()
    obj.close()
    obj.release_conn()

    # Read into Pandas
    if file_name.endswith(".csv"):
        pdf = pd.read_csv(BytesIO(data))
    elif file_name.endswith(".xlsx"):
        pdf = pd.read_excel(BytesIO(data))
    elif file_name.endswith(".parquet"):
        pdf = pd.read_parquet(BytesIO(data))
    elif file_name.endswith(".json"):
        pdf = pd.read_json(BytesIO(data))
    else:
        raise ValueError(f"Unsupported file format: {file_name}")

    # Convert Pandas DataFrame directly to Spark DataFrame
    spark_df = spark.createDataFrame(pdf).cache()
    _ = spark_df.count()  # Force Spark to load the data
    return spark_df
