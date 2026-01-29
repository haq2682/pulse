from pyspark.sql.functions import (
    col,
    coalesce,
    year,
    month,
    quarter,
    dayofweek,
    weekofyear,
    dayofmonth,
    unix_timestamp,
    floor,
    datediff,
    greatest,
    lit,
    when,
    countDistinct,
    to_timestamp,
    sum as spark_sum,
    avg as spark_avg,
    max as spark_max,
)


def transform_orders(dataframes):
    # Skip transformation if required dataframes don't exist
    required_dataframes = ["orders", "order_items", "products"]
    for df_name in required_dataframes:
        if df_name not in dataframes or dataframes[df_name] is None or dataframes[df_name].count() == 0:
            print(f"⚠️ Skipping transform_orders: '{df_name}' dataframe not found or empty")
            return
    
    orders = dataframes["orders"]
    order_items = dataframes["order_items"]
    products = dataframes["products"]

    # -------------------------
    # Join order_items with products to get cost_price
    # -------------------------
    order_items = order_items.join(
        products.select("product_id", "cost_price"),
        on="product_id",
        how="left"
    )

    # -------------------------
    # Calculate line_total and item_cogs
    # -------------------------
    order_items = order_items.withColumn(
        "line_total",
        greatest(
            (col("product_price") * col("quantity")) - when(col("discount_amount").isNotNull(), col("discount_amount")).otherwise(lit(0)),
            lit(0)
        )
    ).withColumn(
        "item_cogs",
        when(
            col("cost_price").isNotNull() & col("quantity").isNotNull(),
            col("cost_price") * col("quantity")
        ).otherwise(lit(0))
    )

    # -------------------------
    # Aggregate order items
    # -------------------------
    order_metrics = (
        order_items
        .groupBy("order_id")
        .agg(
            # Gross revenue (before discounts)
            coalesce(spark_sum(col("product_price") * col("quantity")), lit(0)).alias("gross_product_total"),
            # Net revenue (after discounts)
            coalesce(spark_sum("line_total"), lit(0)).alias("total_product_price"),
            # Cost of goods sold
            coalesce(spark_sum("item_cogs"), lit(0)).alias("total_cogs"),
            coalesce(spark_sum("quantity"), lit(0)).alias("total_quantity"),
            spark_avg("product_price").alias("avg_product_price"),
            coalesce(spark_max("discount_amount"), lit(0)).alias("max_item_discount"),
            countDistinct("product_id").alias("unique_products_ordered"),
            coalesce(spark_sum("discount_amount"), lit(0)).alias("total_discount_from_items")
        )
    )

    orders = orders.join(order_metrics, "order_id", "inner")

    # -------------------------
    # Timestamp normalization
    # -------------------------
    placed_ts = to_timestamp(col("order_placed_at"))
    shipped_ts = to_timestamp(col("order_shipped_at"))
    delivered_ts = to_timestamp(col("order_delivered_at"))

    processing_seconds = unix_timestamp(shipped_ts) - unix_timestamp(placed_ts)
    delivery_seconds = unix_timestamp(delivered_ts) - unix_timestamp(shipped_ts)

    # -------------------------
    # Enriched columns
    # -------------------------
    orders = orders.withColumns({

        # ---- Date dimensions (placed)
        "order_placed_year": year(col("order_placed_at")),
        "order_placed_month": month(col("order_placed_at")),
        "order_placed_quarter": quarter(col("order_placed_at")),
        "order_placed_day_of_week": dayofweek(col("order_placed_at")),
        "order_placed_week_of_year": weekofyear(col("order_placed_at")),
        "order_placed_day_of_month": dayofmonth(col("order_placed_at")),

        # ---- Date dimensions (shipped)
        "order_shipped_year": year(col("order_shipped_at")),
        "order_shipped_month": month(col("order_shipped_at")),
        "order_shipped_quarter": quarter(col("order_shipped_at")),
        "order_shipped_day_of_week": dayofweek(col("order_shipped_at")),
        "order_shipped_week_of_year": weekofyear(col("order_shipped_at")),
        "order_shipped_day_of_month": dayofmonth(col("order_shipped_at")),

        # ---- Date dimensions (delivered)
        "order_delivered_year": year(col("order_delivered_at")),
        "order_delivered_month": month(col("order_delivered_at")),
        "order_delivered_quarter": quarter(col("order_delivered_at")),
        "order_delivered_day_of_week": dayofweek(col("order_delivered_at")),
        "order_delivered_week_of_year": weekofyear(col("order_delivered_at")),
        "order_delivered_day_of_month": dayofmonth(col("order_delivered_at")),

        # ---- Processing time
        "order_processing_seconds_diff": greatest(processing_seconds, lit(0)),
        "order_processing_minutes_diff": greatest(floor(processing_seconds / 60), lit(0)),
        "order_processing_hours_diff": greatest(floor(processing_seconds / 3600), lit(0)),
        "order_processing_days_diff": greatest(datediff(shipped_ts, placed_ts), lit(0)),
        "order_processing_weeks_diff": greatest(datediff(shipped_ts, placed_ts) / 7, lit(0)),
        "order_processing_months_diff": greatest(datediff(shipped_ts, placed_ts) / 30, lit(0)),
        "order_processing_years_diff": greatest(datediff(shipped_ts, placed_ts) / 365, lit(0)),

        # ---- Delivery time
        "delivery_seconds_diff": greatest(delivery_seconds, lit(0)),
        "delivery_minutes_diff": greatest(floor(delivery_seconds / 60), lit(0)),
        "delivery_hours_diff": greatest(floor(delivery_seconds / 3600), lit(0)),
        "delivery_days_diff": greatest(datediff(delivered_ts, shipped_ts), lit(0)),
        "delivery_weeks_diff": greatest(datediff(delivered_ts, shipped_ts) / 7, lit(0)),
        "delivery_months_diff": greatest(datediff(delivered_ts, shipped_ts) / 30, lit(0)),
        "delivery_years_diff": greatest(datediff(delivered_ts, shipped_ts) / 365, lit(0)),

        # ---- Fulfillment totals
        "total_order_fulfillment_time_seconds": 
            greatest(processing_seconds, lit(0)) + greatest(delivery_seconds, lit(0)),

        "total_order_fulfillment_time_minutes":
            greatest(floor(processing_seconds / 60), lit(0)) +
            greatest(floor(delivery_seconds / 60), lit(0)),

        "total_order_fulfillment_time_hours": 
            greatest(floor(processing_seconds / 3600), lit(0)) +
            greatest(floor(delivery_seconds / 3600), lit(0)),

        "total_order_fulfillment_time_days":
            greatest(datediff(shipped_ts, placed_ts), lit(0)) +
            greatest(datediff(delivered_ts, shipped_ts), lit(0)),

        "total_order_fulfillment_time_weeks":
            greatest(datediff(shipped_ts, placed_ts) / 7, lit(0)) +
            greatest(datediff(delivered_ts, shipped_ts) / 7, lit(0)),

        "total_order_fulfillment_time_months":
            greatest(datediff(shipped_ts, placed_ts) / 30, lit(0)) +
            greatest(datediff(delivered_ts, shipped_ts) / 30, lit(0)),

        "total_order_fulfillment_time_years":
            greatest(datediff(shipped_ts, placed_ts) / 365, lit(0)) +
            greatest(datediff(delivered_ts, shipped_ts) / 365, lit(0)),

        # ---- Financial metrics (using COGS from products table)
        # order_profit = Revenue (after discounts) - Cost of Goods Sold
        "order_profit": col("total_product_price") - col("total_cogs"),

        # net_revenue = Revenue after discounts minus shipping
        "net_revenue": col("total_product_price") - col("shipping_cost"),

        # net_profit = Revenue - COGS - Shipping
        "net_profit": col("total_product_price") - col("total_cogs") - col("shipping_cost"),

        # ---- Ratios
        "discount_percentage": when(
            col("gross_product_total") > 0,
            (col("total_discount_from_items") / col("gross_product_total")) * 100
        ),

        "average_item_value": when(
            col("total_quantity") > 0,
            col("total_product_price") / col("total_quantity")
        ),

        "cost_per_item": when(
            col("total_quantity") > 0,
            col("total_cogs") / col("total_quantity")
        ),

        # ---- Categoricals
        "order_size_category": when(
            col("total_quantity") < 3, "Small"
        ).when(
            col("total_quantity") < 7, "Medium"
        ).otherwise("Large"),

        "season": when(
            col("order_placed_month").isin(12, 1, 2), "Winter"
        ).when(
            col("order_placed_month").isin(3, 4, 5), "Spring"
        ).when(
            col("order_placed_month").isin(6, 7, 8), "Summer"
        ).otherwise("Fall"),
    })

    # Drop intermediate columns before saving
    orders = orders.drop("gross_product_total", "total_cogs")

    dataframes["orders"] = orders.dropDuplicates(["order_id"])
    return dataframes