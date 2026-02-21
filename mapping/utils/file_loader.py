import pandas as pd
from io import BytesIO
import os
import re
from utils.helpers import normalize_name


def load_all_files_from_minio(minio_client, bucket_name, spark):
    """
    Load all supported files from the 'ingested' folder in a MinIO bucket directly into Spark DataFrames.

    Args:
        minio_client: MinIO client instance
        bucket_name: Name of the bucket
        spark: SparkSession instance

    Returns:
        Dictionary of {df_name: Spark DataFrame}
    """
    dataframes = {}
    objects = minio_client.list_objects(bucket_name, prefix="ingested/", recursive=True)
    print("Listing available files in the ingested folder...")

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
    Load a file from MinIO and convert to Spark DataFrame(s).
    
    Handles:
    - CSV: Single table
    - Excel: Multiple sheets (each sheet becomes a separate table)
    - Parquet: Single table
    - JSON: Can be single table or multi-table structure
    
    Args:
        minio_client: MinIO client instance
        bucket_name: Name of the bucket
        file_name: Path to the file in MinIO
        spark: SparkSession instance
        
    Returns:
        Single Spark DataFrame or dict of {table_name: Spark DataFrame} for multi-table files
    """
    
    obj = minio_client.get_object(bucket_name, file_name)
    data = obj.read()
    obj.close()
    obj.release_conn()
    
    print(f"File: {file_name}")
    print(f"File size: {len(data)} bytes")
    print(f"First few bytes: {data[:10]}")

    # Read into Pandas, forcing all columns to string
    if file_name.endswith(".csv"):
        pdf = pd.read_csv(BytesIO(data), dtype=str)
        spark_df = spark.createDataFrame(pdf)
        spark_df.cache()
        spark_df.count()
        return spark_df
        
    elif file_name.endswith(".xlsx") or file_name.endswith(".xls"):
        # Excel files can have multiple sheets - handle each sheet as a potential table
        excel_file = BytesIO(data)
        excel_file.seek(0)  # Ensure we're at the start of the stream
        xl_file = pd.ExcelFile(excel_file, engine='openpyxl')
        sheet_names = xl_file.sheet_names
        
        if len(sheet_names) == 1:
            # Single sheet - return as single DataFrame
            pdf = pd.read_excel(xl_file, sheet_name=0, dtype=str)
            spark_df = spark.createDataFrame(pdf)
            spark_df.cache()
            spark_df.count()
            return spark_df
        else:
            # Multiple sheets - return dict of DataFrames
            result = {}
            for sheet_name in sheet_names:
                pdf = pd.read_excel(xl_file, sheet_name=sheet_name, dtype=str)
                if not pdf.empty:  # Skip empty sheets
                    spark_df = spark.createDataFrame(pdf)
                    spark_df.cache()
                    spark_df.count()
                    # Normalize sheet name to match table naming
                    normalized_name = normalize_name(sheet_name)
                    if normalized_name:
                        result[normalized_name] = spark_df
                    else:
                        # If normalization fails, use sheet name directly
                        result[f"sheet_{sheet_name}"] = spark_df
            return result if result else None
            
    elif file_name.endswith(".parquet"):
        pdf = pd.read_parquet(BytesIO(data))
        pdf = pdf.astype(str)  # parquet often safe but mixed columns can still break
    elif file_name.endswith(".json"):
        pdf = pd.read_json(BytesIO(data), dtype=str)
    else:
        raise ValueError(f"Unsupported file format: {file_name}")

    spark_df = spark.createDataFrame(pdf)
    spark_df.cache()
    spark_df.count()
    return spark_df
