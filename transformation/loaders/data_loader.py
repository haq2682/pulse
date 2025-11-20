def load_data_from_minio(spark, minio_client, bucket_name):
    objects = minio_client.list_objects(bucket_name, prefix="cleaned_", recursive=True)
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
        "reviews": ["review_id"],
        "payments": ["payment_id"],
        "marketing_campaigns": ["campaign_id"],
    }

    for obj in objects:
        df = spark.read.csv(
            f"s3a://{bucket_name}/{obj.object_name}", header=True, inferSchema=True
        )
        object_name = obj.object_name.replace("cleaned_", "").replace(".csv", "")

        # Deduplicate based on primary key if available
        if object_name in primary_keys:
            df = df.dropDuplicates(primary_keys[object_name])

        dataframes[object_name] = df

    return dataframes
