from pyspark.sql.functions import *
from pyspark.sql.window import Window

def transform_customers(dataframes):
    customers_with_orders = dataframes["customers"] \
        .drop("order_recency_days", "order_frequency", "order_total_spent") \
        .join(dataframes["orders"].select("customer_id", "order_placed_at", "order_id", "total_amount").groupBy("customer_id").agg(
            datediff(current_date(), max("order_placed_at")).alias("order_recency_days"),
            count("order_id").alias("order_frequency"),
            sum("total_amount").alias("order_total_spent"),
        ), "customer_id", "left")

    dataframes["customers"] = customers_with_orders.withColumns(
        {
            "customer_age": when(col("date_of_birth").isNotNull(), floor(datediff(current_date(), col("date_of_birth")) / 365)),
            "customer_tenure_days": when(col("account_created_at").isNotNull(), datediff(current_date(), to_date(col("account_created_at")))),
            "days_since_last_login": when(col("last_login_date").isNotNull(), datediff(current_date(), to_date(col("last_login_date")))),
            "customer_age_group": when(
                col("customer_age").isNotNull(),
                when(col("customer_age") < 18, "Under 18")
                .when((col("customer_age") >= 18) & (col("customer_age") < 25), "18-24")
                .when((col("customer_age") >= 25) & (col("customer_age") < 35), "25-34")
                .when((col("customer_age") >= 35) & (col("customer_age") < 45), "35-44")
                .when((col("customer_age") >= 45) & (col("customer_age") < 55), "45-54")
                .when((col("customer_age") >= 55) & (col("customer_age") < 65), "55-64")
                .otherwise("65 and over")
            ),
            "customer_activity_status": when(
                col("days_since_last_login").isNotNull(),
                when(col("days_since_last_login") <= 30, "Active")
                .when((col("days_since_last_login") > 30) & (col("days_since_last_login") <= 90), "Inactive")
                .otherwise("Dormant")
            ),
            "customer_segment": when(
                col("order_recency_days").isNotNull(),
                when((col("order_frequency") >= 10) & (col("order_total_spent") >= 1000) & (col("order_recency_days") <= 30), "High Value")
                .when((col("order_frequency") >= 5) & (col("order_total_spent") >= 500) & (col("order_recency_days") <= 60), "Medium Value")
                .when((col("order_frequency") >= 2) & (col("order_total_spent") >= 100) & (col("order_recency_days") <= 90), "Low Value")
                .when((col("order_frequency") == 1) & (col("order_recency_days") <= 30), "New")
                .when((col("order_recency_days") > 90) & (col("order_recency_days") <= 180), "At Risk")
                .when(col("order_recency_days") > 180, "Lost")
                .otherwise("Uncategorized")
            ),
            "customer_lifetime_value": (
                (col("order_total_spent") / when(col("order_frequency") > 0, col("order_frequency")).otherwise(lit(1)))
                *
                ((col("order_frequency") / when(col("customer_tenure_days") > 0, col("customer_tenure_days")).otherwise(lit(365))) * 365)
                *
                lit(3.0)
            )
        }
    )

def add_customer_aggregations(dataframes):
    orders_with_items = dataframes["orders"] \
        .join(dataframes["order_items"], "order_id", "inner") \
        .select(
            "order_id",
            "customer_id",
            "order_status",
            "total_amount",
            "total_discount",
            "order_placed_at",
            "quantity"
        )

    window_spec = Window.partitionBy("customer_id").orderBy("order_placed_at")
    orders_with_lag = orders_with_items \
        .withColumn("prev_order_date", lag("order_placed_at", 1).over(window_spec)) \
        .withColumn(
            "days_since_prev_order",
            when(
                col("prev_order_date").isNotNull(),
                datediff(col("order_placed_at"), col("prev_order_date"))
            )
        )

    customer_order_agg = orders_with_lag.groupBy("customer_id").agg(
        countDistinct("order_id").alias("total_orders"),
        sum(when(col("total_amount").isNotNull() & (col("total_amount") != 0), col("total_amount"))).alias("total_revenue"),
        avg(when(col("total_amount").isNotNull() & (col("total_amount") != 0), col("total_amount"))).alias("avg_order_value"),
        sum(when(col("quantity").isNotNull() & (col("quantity") > 0), col("quantity"))).alias("total_items_purchased"),
        avg(when(col("quantity").isNotNull() & (col("quantity") > 0), col("quantity"))).alias("avg_items_per_order"),
        sum(when(col("total_discount").isNotNull() & (col("total_discount") > 0), col("total_discount"))).alias("total_discount_received"),
        avg(when(col("total_discount").isNotNull() & (col("total_discount") > 0), col("total_discount"))).alias("avg_discount_per_order"),
        min(when(col("order_placed_at").isNotNull(), col("order_placed_at"))).alias("first_order_date"),
        max(when(col("order_placed_at").isNotNull(), col("order_placed_at"))).alias("last_order_date"),
        avg(when(col("days_since_prev_order").isNotNull() & (col("days_since_prev_order") > 0), col("days_since_prev_order"))).alias("avg_days_between_orders"),
        sum(when(col("order_status").isNotNull() & (lower(col("order_status")) == "cancelled"), lit(1)).otherwise(lit(0))).alias("total_cancelled_orders")
    )

    customer_review_agg = dataframes["reviews"] \
        .filter(col("customer_id").isNotNull()) \
        .groupBy("customer_id").agg(
            count("review_id").alias("total_reviews_written"),
            avg(when(col("rating").isNotNull() & (col("rating") > 0), col("rating"))).alias("avg_review_rating")
        )

    customer_session_agg = dataframes["customer_sessions"] \
        .filter(col("customer_id").isNotNull()) \
        .groupBy("customer_id").agg(
            countDistinct("session_id").alias("total_sessions"),
            avg(when(col("session_duration_minutes").isNotNull() & (col("session_duration_minutes") > 0), col("session_duration_minutes"))).alias("avg_session_duration"),
            sum(when(col("pages_viewed").isNotNull() & (col("pages_viewed") > 0), col("pages_viewed"))).alias("total_pages_viewed"),
            sum(when(col("products_viewed").isNotNull() & (col("products_viewed") > 0), col("products_viewed"))).alias("total_products_viewed"),
            (sum(when(col("conversion_flag") == 1, lit(1)).otherwise(lit(0))) / count("session_id")).alias("session_conversion_rate"),
            (sum(when(col("cart_abandonment_flag") == 1, lit(1)).otherwise(lit(0))) / count("session_id")).alias("cart_abandonment_rate"),
            expr("first(device_type)").alias("preferred_device_type"),
            expr("first(referrer_source)").alias("preferred_referrer_source")
        )

    customer_wishlist_agg = dataframes["wishlist"] \
        .filter(col("customer_id").isNotNull()) \
        .groupBy("customer_id").agg(
            countDistinct("wishlist_id").alias("wishlist_items_count"),
            (sum(when(col("purchased_date").isNotNull(), lit(1)).otherwise(lit(0))) / count("wishlist_id")).alias("wishlist_conversion_rate")
        )

    customer_payment_agg = dataframes["payments"] \
        .join(dataframes["orders"].select("order_id", "customer_id"), "order_id", "inner") \
        .filter(col("customer_id").isNotNull() & col("payment_method").isNotNull()) \
        .groupBy("customer_id").agg(
            expr("first(payment_method)").alias("preferred_payment_method")
        )

    dataframes["customers"] = dataframes["customers"] \
        .join(customer_order_agg, "customer_id", "left") \
        .join(customer_review_agg, "customer_id", "left") \
        .join(customer_session_agg, "customer_id", "left") \
        .join(customer_wishlist_agg, "customer_id", "left") \
        .join(customer_payment_agg, "customer_id", "left")

    dataframes["customers"] = dataframes["customers"].withColumns({
        "customer_lifetime_value": coalesce(col("total_revenue"), lit(0)),
        "days_since_last_purchase": when(col("last_order_date").isNotNull(), datediff(current_date(), col("last_order_date"))),
        "is_repeat_customer": when(col("total_orders").isNotNull() & (col("total_orders") > 1), lit(1)).otherwise(lit(0)),
        "cancellation_rate": when(col("total_orders").isNotNull() & (col("total_orders") > 0) & col("total_cancelled_orders").isNotNull(), (col("total_cancelled_orders") / col("total_orders")) * 100),
        "customer_activity_score": when(
            col("days_since_last_purchase").isNotNull() &
            col("total_orders").isNotNull() &
            col("total_revenue").isNotNull(),
            ((lit(1) / (col("days_since_last_purchase") + lit(1))) * lit(0.4) +
            (col("total_orders") / lit(100)) * lit(0.4) +
            (col("total_revenue") / lit(10000)) * lit(0.2)) * lit(100)
        ),
        "total_orders": coalesce(col("total_orders"), lit(0)),
        "total_revenue": coalesce(col("total_revenue"), lit(0.0)),
        "total_items_purchased": coalesce(col("total_items_purchased"), lit(0)),
        "total_reviews_written": coalesce(col("total_reviews_written"), lit(0)),
        "total_sessions": coalesce(col("total_sessions"), lit(0)),
        "wishlist_items_count": coalesce(col("wishlist_items_count"), lit(0)),
    })
