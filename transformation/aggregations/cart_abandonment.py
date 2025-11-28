from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    avg as spark_avg,
    min as spark_min,
    max as spark_max,
    when,
    expr,
    datediff,
    current_date,
    lower,
    coalesce,
    lit,
)


def cart_abandonment_aggregations(dataframes):
    cart_with_products = (
        dataframes["shopping_cart"]
        .join(
            dataframes["products"].select("product_id", "category"),
            "product_id",
            "left",
        )
        .join(
            dataframes["customer_sessions"].select(
                "session_id", "device_type", "conversion_flag"
            ),
            "session_id",
            "left",
        )
    )
    cart_agg = (
        cart_with_products.filter(col("cart_id").isNotNull())
        .groupBy("cart_id")
        .agg(
            # Cart total value
            spark_sum(
                when(
                    col("unit_price").isNotNull()
                    & col("quantity").isNotNull()
                    & (col("unit_price") > 0)
                    & (col("quantity") > 0),
                    col("unit_price") * col("quantity"),
                )
            ).alias("cart_total_value"),
            # Cart items count
            count("product_id").alias("cart_items_count"),
            # Average item price
            spark_avg(
                when(
                    col("unit_price").isNotNull() & (col("unit_price") > 0),
                    col("unit_price"),
                )
            ).alias("cart_avg_item_price"),
            # Device used
            expr("first(device_type)").alias("device_used"),
            # Primary category (most frequent)
            expr("first(category)").alias("abandoned_cart_category"),
            # Session conversion flag
            expr("first(conversion_flag)").alias("session_converted"),
            # Earliest added date
            spark_min("added_date").alias("first_added_date"),
            # Latest added date
            spark_max("added_date").alias("last_added_date"),
            # Customer ID
            expr("first(customer_id)").alias("customer_id"),
            # Session ID
            expr("first(session_id)").alias("session_id"),
        )
    )
    cart_full = (
        dataframes["shopping_cart"]
        .select("cart_id", "cart_status", "added_date")
        .groupBy("cart_id")
        .agg(
            expr("first(cart_status)").alias("cart_status"),
            spark_min("added_date").alias("cart_added_date"),
        )
        .join(cart_agg, "cart_id", "left")
    )
    cart_full = cart_full.withColumns(
        {
            # Time in cart (days since first added)
            "time_in_cart_days": when(
                col("first_added_date").isNotNull(),
                datediff(current_date(), col("first_added_date")),
            ),
            # Time in cart (hours)
            "time_in_cart_hours": when(
                col("first_added_date").isNotNull(),
                datediff(current_date(), col("first_added_date")) * 24,
            ),
            # Cart status classification
            "cart_status_derived": when(
                lower(col("cart_status")).isin("purchased", "completed", "ordered"),
                "Purchased",
            )
            .when(lower(col("cart_status")).isin("active", "in progress"), "Active")
            .otherwise("Abandoned"),
            # Handle nulls for aggregated columns
            "cart_total_value": coalesce(col("cart_total_value"), lit(0.0)),
            "cart_items_count": coalesce(col("cart_items_count"), lit(0)),
            "cart_avg_item_price": coalesce(col("cart_avg_item_price"), lit(0.0)),
        }
    )
    cart_full = cart_full.withColumns(
        {
            # Cart abandonment reason (derived from patterns)
            "cart_abandonment_reason": when(
                col("cart_status_derived") == "Purchased", "N/A - Purchased"
            )
            .when(col("cart_status_derived") == "Active", "N/A - Active")
            .when(col("cart_total_value") > 500, "High Value - Price Concern")
            .when(col("cart_items_count") > 10, "Too Many Items")
            .when(col("time_in_cart_days") > 7, "Long Time - Lost Interest")
            .when(col("time_in_cart_days") <= 1, "Quick Abandon - Browsing")
            .when(col("device_used") == "Mobile", "Mobile - Checkout Friction")
            .otherwise("Unknown"),
            # Cart value tier
            "cart_value_tier": when(col("cart_total_value") >= 1000, "High Value")
            .when(col("cart_total_value") >= 500, "Medium Value")
            .when(col("cart_total_value") >= 100, "Low Value")
            .otherwise("Very Low Value"),
            # Cart size category
            "cart_size_category": when(col("cart_items_count") >= 10, "Large")
            .when(col("cart_items_count") >= 5, "Medium")
            .when(col("cart_items_count") >= 2, "Small")
            .otherwise("Single Item"),
            # Abandonment risk score
            "abandonment_risk_score": when(
                col("cart_status_derived") != "Purchased",
                (
                    # Time weight (40%)
                    (col("time_in_cart_days") / lit(30)) * lit(0.4)
                    +
                    # Value weight (30%)
                    (col("cart_total_value") / lit(1000)) * lit(0.3)
                    +
                    # Items weight (30%)
                    (col("cart_items_count") / lit(10)) * lit(0.3)
                )
                * lit(100),
            ).otherwise(lit(0.0)),
            # Recovery potential score
            "recovery_potential_score": when(
                col("cart_status_derived") == "Abandoned",
                when(
                    (col("cart_total_value") > 200) & (col("time_in_cart_days") <= 3),
                    lit(100),
                )
                .when(
                    (col("cart_total_value") > 100) & (col("time_in_cart_days") <= 7),
                    lit(75),
                )
                .when(col("time_in_cart_days") <= 14, lit(50))
                .otherwise(lit(25)),
            ).otherwise(lit(0)),
        }
    )
    dataframes["cart_abandonment_analysis"] = cart_full
    customer_cart_behavior = (
        cart_full.filter(col("customer_id").isNotNull())
        .groupBy("customer_id")
        .agg(
            # Total carts created
            count("cart_id").alias("total_carts_created"),
            # Abandoned carts
            spark_sum(
                when(col("cart_status_derived") == "Abandoned", lit(1)).otherwise(
                    lit(0)
                )
            ).alias("total_abandoned_carts"),
            # Purchased carts
            spark_sum(
                when(col("cart_status_derived") == "Purchased", lit(1)).otherwise(
                    lit(0)
                )
            ).alias("total_purchased_carts"),
            # Total abandoned value
            spark_sum(
                when(col("cart_status_derived") == "Abandoned", col("cart_total_value"))
            ).alias("total_abandoned_value"),
            # Average time in cart
            spark_avg("time_in_cart_days").alias("avg_time_in_cart_days"),
        )
        .withColumns(
            {
                # Customer abandonment rate
                "customer_abandonment_rate": when(
                    col("total_carts_created") > 0,
                    (col("total_abandoned_carts") / col("total_carts_created")) * 100,
                ),
                # Customer purchase rate
                "customer_purchase_rate": when(
                    col("total_carts_created") > 0,
                    (col("total_purchased_carts") / col("total_carts_created")) * 100,
                ),
            }
        )
    )
    dataframes["customers"] = (
        dataframes["customers"]
        .drop(
            "total_carts_created",
            "total_abandoned_carts",
            "total_abandoned_value",
            "avg_time_in_cart_days",
            "customer_abandonment_rate",
            "customer_purchase_rate",
        )
        .join(customer_cart_behavior, "customer_id", "left")
        .withColumns(
            {
                "total_carts_created": coalesce(col("total_carts_created"), lit(0)),
                "total_abandoned_carts": coalesce(col("total_abandoned_carts"), lit(0)),
                "total_abandoned_value": coalesce(
                    col("total_abandoned_value"), lit(0.0)
                ),
            }
        )
    )
