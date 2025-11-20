def load_data_from_minio(spark, minio_client, bucket_name):
    objects = minio_client.list_objects(bucket_name, prefix="cleaned_", recursive=True)
    dataframes = {}
    for obj in objects:
        df = spark.read.csv(
            f"s3a://{bucket_name}/{obj.object_name}", header=True, inferSchema=True
        )
        object_name = obj.object_name.replace("cleaned_", "").replace(".csv", "")
        dataframes[object_name] = df
    return dataframes
