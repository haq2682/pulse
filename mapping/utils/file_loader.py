import os
import tempfile
import pandas as pd
from io import BytesIO
import re
import uuid
from utils.helpers import normalize_name


def load_all_files_from_minio(minio_client, bucket_name, spark):
    """
    Load all supported files from MinIO bucket.

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

        if not (
            file_name.endswith(".csv")
            or file_name.endswith(".xlsx")
            or file_name.endswith(".parquet")
            or file_name.endswith(".json")
        ):
            print(f"Skipping {file_name} - unsupported format")
            continue

        try:
            base_name = os.path.splitext(os.path.basename(file_name))[0]
            clean_name = re.sub(r"[^0-9a-zA-Z_]+", "_", base_name)
            norm_name = normalize_name(clean_name)
            if norm_name is None:
                print(f"No match found for {base_name}, skipping.")
                continue
            df = load_file_from_minio(minio_client, bucket_name, file_name, spark)
            df_name = f"{norm_name}"
            dataframes[df_name] = df
            print(f"✅ Successfully loaded {file_name} as {df_name}")
        except Exception as e:
            print(f"Error processing {file_name}: {str(e)}")

    print(f"Loaded {len(dataframes)} dataframes: {', '.join(dataframes.keys())}")
    return dataframes


def load_file_from_minio(minio_client, bucket_name, file_name, spark):
    """
    Load a single file from MinIO and convert to Spark DataFrame.

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

    temp_csv = os.path.join(
        tempfile.gettempdir(),
        f"temp_{uuid.uuid4().hex}_{os.path.basename(file_name)}.csv",
    )
    pdf.to_csv(temp_csv, index=False)

    spark_df = spark.read.csv(temp_csv, header=True, inferSchema=True).cache()
    _ = spark_df.count()

    try:
        os.remove(temp_csv)
    except Exception:
        pass

    return spark_df
