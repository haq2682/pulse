from pyspark.sql.functions import col, when


def transform_products(dataframes):
    dataframes["products"] = dataframes["products"].withColumns(
        {
            "profit_margin": when(
                col("cost_price").isNotNull()
                & col("sell_price").isNotNull()
                & (col("cost_price") != 0)
                & (col("sell_price") != 0),
                col("sell_price") - col("cost_price"),
            ),
        }
    )
