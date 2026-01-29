from pyspark.sql.functions import col, when

def transform_products(dataframes):
    # Skip transformation if products doesn't exist or is empty
    if "products" not in dataframes or dataframes["products"] is None or dataframes["products"].rdd.isEmpty():
        print("⚠️ Skipping transform_products: 'products' dataframe not found or empty")
        return
    
    # Add profit_margin column safely
    df = dataframes["products"]
    dataframes["products"] = df.withColumn(
        "profit_margin",
        when((col("sell_price").isNotNull()) & (col("cost_price").isNotNull()),
            col("sell_price") - col("cost_price")
        ).otherwise(None)
    )

