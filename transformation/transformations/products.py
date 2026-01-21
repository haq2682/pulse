from pyspark.sql.functions import col, when


def transform_products(dataframes):
    # Skip transformation if products doesn't exist
    if "products" not in dataframes or dataframes["products"] is None or dataframes["products"].count() == 0:
        print("⚠️ Skipping transform_products: 'products' dataframe not found or empty")
        return
    
    dataframes["products"] = dataframes["products"].withColumns(
        {
            "profit_margin": when(
                col("cost_price").isNotNull()
                & col("sell_price").isNotNull()
                & (col("sell_price") > 0),
                col("sell_price") - col("cost_price"),
            ),
        }
    )
