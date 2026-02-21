from pyspark.sql.functions import (
    sum as spark_sum,
    avg as spark_avg,
    to_date,
    col,
    when,
    lit,
)

def transform_inventory(dataframes):
    # Skip transformation if required dataframes don't exist
    required_dataframes = ["order_items", "inventory", "orders"]
    for df_name in required_dataframes:
        if df_name not in dataframes or dataframes[df_name] is None or dataframes[df_name].count() == 0:
            print(f"⚠️ Skipping transform_inventory: '{df_name}' dataframe not found or empty")
            return
    
    total_sold_df = (
        dataframes["order_items"]
        .groupBy("product_id")
        .agg(spark_sum("quantity").alias("total_sold"))
    )
    avg_inventory_df = (
        dataframes["inventory"]
        .groupBy("product_id")
        .agg(spark_avg("stock_quantity").alias("avg_inventory"))
    )
    avg_daily_units_sold = (
        dataframes["order_items"]
        .join(
            dataframes["orders"].select("order_id", "order_placed_at"),
            "order_id",
            "inner",
        )
        .groupBy(to_date("order_placed_at").alias("order_date"))
        .agg(spark_sum("quantity").alias("daily_units"))
        .select(spark_avg("daily_units").alias("avg_daily_units"))
        .collect()[0]["avg_daily_units"]
    )
    if avg_daily_units_sold is None or avg_daily_units_sold == 0:
        avg_daily_units_sold = 1  # Avoid division by zero
    dataframes["inventory"] = (
        dataframes["inventory"]
        .join(total_sold_df, "product_id", "left")
        .join(avg_inventory_df, "product_id", "left")
        .withColumns(
            {
                "storage_cost_per_unit": when(
                    col("storage_cost").isNotNull()
                    & col("stock_quantity").isNotNull()
                    & (col("stock_quantity") != 0)
                    & (col("storage_cost") != 0),
                    col("storage_cost") / col("stock_quantity"),
                ),
                "stock_status": when(
                    col("stock_quantity").isNotNull(),
                    when(col("stock_quantity") == 0, "Out of Stock")
                    .when(
                        col("minimum_stock_level").isNotNull()
                        & (col("stock_quantity") < col("minimum_stock_level")),
                        "Low Stock",
                    )
                    .when(
                        col("reserved_quantity").isNotNull()
                        & (col("stock_quantity") <= col("reserved_quantity")),
                        "Low Stock",
                    )
                    .when(
                        col("minimum_stock_level").isNotNull()
                        & (col("stock_quantity") <= col("minimum_stock_level") * 3),
                        "In Stock",
                    )
                    .when(col("stock_quantity") <= 50, "In Stock")
                    .otherwise("High Stock"),
                ),
                "available_stock": when(
                    col("stock_quantity").isNotNull()
                    & col("reserved_quantity").isNotNull()
                    & (col("stock_quantity") >= col("reserved_quantity")),
                    col("stock_quantity") - col("reserved_quantity"),
                ),
                "stock_coverage_days": when(
                    col("stock_quantity").isNotNull()
                    & (lit(avg_daily_units_sold) > lit(0)),
                    col("stock_quantity") / lit(avg_daily_units_sold),
                ),
                "reorder_point_breach": when(
                    col("stock_quantity").isNotNull()
                    & col("minimum_stock_level").isNotNull(),
                    when(
                        col("stock_quantity") < col("minimum_stock_level"), 1
                    ).otherwise(0),
                ),
                "stock_turnover_ratio": when(
                    col("avg_inventory").isNotNull()
                    & (col("avg_inventory") != 0)
                    & col("total_sold").isNotNull(),
                    col("total_sold") / col("avg_inventory"),
                ),
            }
        )
    )
