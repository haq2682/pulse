from pyspark.sql.functions import (
    col,
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
    orders = dataframes["orders"]
    order_items = dataframes["order_items"]

    # -------------------------
    # Aggregate order items
    # -------------------------
    order_metrics = (
        order_items
        .groupBy("order_id")
        .agg(
            spark_sum(col("product_cost") * col("quantity")).alias("total_product_cost"),
            spark_sum("quantity").alias("total_quantity"),
            spark_avg("product_cost").alias("avg_product_cost"),
            spark_max("discount_amount").alias("max_item_discount"),
            countDistinct("product_id").alias("unique_products_ordered"),
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
            col("order_processing_seconds_diff") + col("delivery_seconds_diff"),

        "total_order_fulfillment_time_minutes":
            col("order_processing_minutes_diff") + col("delivery_minutes_diff"),

        "total_order_fulfillment_time_hours":
            col("order_processing_hours_diff") + col("delivery_hours_diff"),

        "total_order_fulfillment_time_days":
            col("order_processing_days_diff") + col("delivery_days_diff"),

        "total_order_fulfillment_time_weeks":
            col("order_processing_weeks_diff") + col("delivery_weeks_diff"),

        "total_order_fulfillment_time_months":
            col("order_processing_months_diff") + col("delivery_months_diff"),

        "total_order_fulfillment_time_years":
            col("order_processing_years_diff") + col("delivery_years_diff"),

        # ---- Financial metrics (simplified & correct)
        "order_profit": col("subtotal") - col("total_product_cost"),
        "net_revenue": col("total_amount") - col("total_discount") - col("shipping_cost"),
        "net_profit": col("order_profit") - col("shipping_cost"),

        # ---- Ratios (division guarded)
        "discount_percentage": when(
            col("subtotal") > 0,
            (col("total_discount") / col("subtotal")) * 100
        ),

        "average_item_value": when(
            col("total_quantity") > 0,
            col("subtotal") / col("total_quantity")
        ),

        "cost_per_item": when(
            col("total_quantity") > 0,
            col("total_product_cost") / col("total_quantity")
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

    dataframes["orders"] = orders.dropDuplicates(["order_id"])
    return dataframes
