def load_data_from_minio(spark, minio_client, bucket_name):
    """
    Load data from MinIO from the cleaned/ directory.
    
    Args:
        spark: SparkSession
        minio_client: MinIO client instance
        bucket_name: Name of the bucket
        
    Returns:
        dict: Dictionary of table names to DataFrames
    """
    objects = minio_client.list_objects(bucket_name, prefix="cleaned/", recursive=True)
    dataframes = {}

    # Define primary keys for each table
    primary_keys = {
        "products": ["product_id"],
        "customers": ["customer_id"],
        "orders": ["order_id"],
        "order_items": ["order_item_id"],
        "suppliers": ["supplier_id"],
        "inventory": ["inventory_id"],
        "customer_sessions": ["session_id"],
        "wishlist": ["wishlist_id"],
        "shopping_cart": ["cart_id"],
        "cart_items": ["cart_item_id"],
        "reviews": ["review_id"],
        "payments": ["payment_id"],
        "marketing_campaigns": ["campaign_id"],
    }

    for obj in objects:
        if not obj.object_name.endswith(".csv"):
            continue
        df = spark.read.csv(
            f"s3a://{bucket_name}/{obj.object_name}", header=True, inferSchema=True
        )
        # Extract table name from path: cleaned/{table_name}.csv
        object_name = obj.object_name.replace("cleaned/", "").replace(".csv", "")

        # Deduplicate based on primary key if available
        if object_name in primary_keys:
            df = df.dropDuplicates(primary_keys[object_name])

        dataframes[object_name] = df
        print(f"Loaded {object_name} with {df.count()} rows from {obj.object_name}")

    return dataframes
