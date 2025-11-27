from pyspark.sql import Window

from pyspark.sql.functions import (
    col,
    countDistinct,
    count,
    sum as spark_sum,
    avg as spark_avg,
    min as spark_min,
    max as spark_max,
    when,
    lit,
    lower,
    lag,
    datediff,
    current_date,
    coalesce,
    expr,
)

def aggregate_customers(dataframes):
    orders_with_items = (
        dataframes["orders"]
        .join(dataframes["order_items"], "order_id", "inner")
        .select(
            "order_id",
            "customer_id",
            "order_status",
            "total_amount",
            "total_discount",
            "order_placed_at",
            "quantity",
        )
    )
    window_spec = Window.partitionBy("customer_id").orderBy("order_placed_at")
    orders_with_lag = orders_with_items.withColumn(
        "prev_order_date", lag("order_placed_at", 1).over(window_spec)
    ).withColumn(
        "days_since_prev_order",
        when(
            col("prev_order_date").isNotNull(),
            datediff(col("order_placed_at"), col("prev_order_date")),
        ),
    )
    customer_order_agg = orders_with_lag.groupBy("customer_id").agg(
        # Total orders
        countDistinct("order_id").alias("total_orders"),
        # Revenue metrics
        spark_sum(
            when(
                col("total_amount").isNotNull() & (col("total_amount") != 0),
                col("total_amount"),
            )
        ).alias("total_revenue"),
        spark_avg(
            when(
                col("total_amount").isNotNull() & (col("total_amount") != 0),
                col("total_amount"),
            )
        ).alias("avg_order_value"),
        # Items purchased
        spark_sum(
            when(col("quantity").isNotNull() & (col("quantity") > 0), col("quantity"))
        ).alias("total_items_purchased"),
        spark_avg(
            when(col("quantity").isNotNull() & (col("quantity") > 0), col("quantity"))
        ).alias("avg_items_per_order"),
        # Discount metrics
        spark_sum(
            when(
                col("total_discount").isNotNull() & (col("total_discount") > 0),
                col("total_discount"),
            )
        ).alias("total_discount_received"),
        spark_avg(
            when(
                col("total_discount").isNotNull() & (col("total_discount") > 0),
                col("total_discount"),
            )
        ).alias("avg_discount_per_order"),
        # Order dates
        spark_min(
            when(col("order_placed_at").isNotNull(), col("order_placed_at"))
        ).alias("first_order_date"),
        spark_max(
            when(col("order_placed_at").isNotNull(), col("order_placed_at"))
        ).alias("last_order_date"),
        # Days between orders
        spark_avg(
            when(
                col("days_since_prev_order").isNotNull()
                & (col("days_since_prev_order") > 0),
                col("days_since_prev_order"),
            )
        ).alias("avg_days_between_orders"),
        # Cancelled orders
        spark_sum(
            when(
                col("order_status").isNotNull()
                & (lower(col("order_status")) == "cancelled"),
                lit(1),
            ).otherwise(lit(0))
        ).alias("total_cancelled_orders"),
    )
    customer_review_agg = (
        dataframes["reviews"]
        .filter(col("customer_id").isNotNull())
        .groupBy("customer_id")
        .agg(
            count("review_id").alias("total_reviews_written"),
            spark_avg(
                when(col("rating").isNotNull() & (col("rating") > 0), col("rating"))
            ).alias("avg_review_rating"),
        )
    )
    customer_session_agg = (
        dataframes["customer_sessions"]
        .filter(col("customer_id").isNotNull())
        .groupBy("customer_id")
        .agg(
            countDistinct("session_id").alias("total_sessions"),
            spark_avg(
                when(
                    col("session_duration_minutes").isNotNull()
                    & (col("session_duration_minutes") > 0),
                    col("session_duration_minutes"),
                )
            ).alias("avg_session_duration"),
            spark_sum(
                when(
                    col("pages_viewed").isNotNull() & (col("pages_viewed") > 0),
                    col("pages_viewed"),
                )
            ).alias("total_pages_viewed"),
            spark_sum(
                when(
                    col("products_viewed").isNotNull() & (col("products_viewed") > 0),
                    col("products_viewed"),
                )
            ).alias("total_products_viewed"),
            # Conversion rate: conversions / total sessions
            (
                spark_sum(when(col("conversion_flag") == 1, lit(1)).otherwise(lit(0)))
                / count("session_id")
            ).alias("session_conversion_rate"),
            # Cart abandonment rate
            (
                spark_sum(
                    when(col("cart_abandonment_flag") == 1, lit(1)).otherwise(lit(0))
                )
                / count("session_id")
            ).alias("cart_abandonment_rate"),
            # Preferred device (most common)
            expr("first(device_type)").alias("preferred_device_type"),
            # Preferred referrer source
            expr("first(referrer_source)").alias("preferred_referrer_source"),
        )
    )
    customer_wishlist_agg = (
        dataframes["wishlist"]
        .filter(col("customer_id").isNotNull())
        .groupBy("customer_id")
        .agg(
            countDistinct("wishlist_id").alias("wishlist_items_count"),
            # Wishlist conversion rate
            (
                spark_sum(
                    when(col("purchased_date").isNotNull(), lit(1)).otherwise(lit(0))
                )
                / count("wishlist_id")
            ).alias("wishlist_conversion_rate"),
        )
    )
    customer_payment_agg = (
        dataframes["payments"]
        .join(
            dataframes["orders"].select("order_id", "customer_id"), "order_id", "inner"
        )
        .filter(col("customer_id").isNotNull() & col("payment_method").isNotNull())
        .groupBy("customer_id")
        .agg(expr("first(payment_method)").alias("preferred_payment_method"))
    )
    dataframes["customers"] = (
        dataframes["customers"]
        .join(customer_order_agg, "customer_id", "left")
        .join(customer_review_agg, "customer_id", "left")
        .join(customer_session_agg, "customer_id", "left")
        .join(customer_wishlist_agg, "customer_id", "left")
        .join(customer_payment_agg, "customer_id", "left")
    )
    dataframes["customers"] = dataframes["customers"].withColumns(
        {
            # Customer lifetime value (already calculated as total_revenue)
            "customer_lifetime_value": coalesce(col("total_revenue"), lit(0)),
            # Days since last purchase
            "days_since_last_purchase": when(
                col("last_order_date").isNotNull(),
                datediff(current_date(), col("last_order_date")),
            ),
            # Is repeat customer
            "is_repeat_customer": when(
                col("total_orders").isNotNull() & (col("total_orders") > 1), lit(1)
            ).otherwise(lit(0)),
            # Cancellation rate
            "cancellation_rate": when(
                col("total_orders").isNotNull()
                & (col("total_orders") > 0)
                & col("total_cancelled_orders").isNotNull(),
                (col("total_cancelled_orders") / col("total_orders")) * 100,
            ),
            # Customer activity score (weighted: recency=40%, frequency=40%, monetary=20%)
            "customer_activity_score": when(
                col("days_since_last_purchase").isNotNull()
                & col("total_orders").isNotNull()
                & col("total_revenue").isNotNull(),
                (
                    # Recency score (inverse - lower days is better)
                    (lit(1) / (col("days_since_last_purchase") + lit(1))) * lit(0.4)
                    +
                    # Frequency score
                    (col("total_orders") / lit(100)) * lit(0.4)
                    +
                    # Monetary score
                    (col("total_revenue") / lit(10000)) * lit(0.2)
                )
                * lit(100),
            ),
            # Handle null values with defaults
            "total_orders": coalesce(col("total_orders"), lit(0)),
            "total_revenue": coalesce(col("total_revenue"), lit(0.0)),
            "total_items_purchased": coalesce(col("total_items_purchased"), lit(0)),
            "total_reviews_written": coalesce(col("total_reviews_written"), lit(0)),
            "total_sessions": coalesce(col("total_sessions"), lit(0)),
            "wishlist_items_count": coalesce(col("wishlist_items_count"), lit(0)),
        }
    )
