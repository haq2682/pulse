from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum as spark_sum,
    when,
    coalesce,
    expr,
    lit,
    to_date,
)


def session_aggregations(dataframes):
    # Skip aggregation if required dataframes don't exist
    required_dataframes = ["shopping_cart", "cart_items", "customer_sessions", "orders"]
    for df_name in required_dataframes:
        if df_name not in dataframes or dataframes[df_name] is None or dataframes[df_name].count() == 0:
            print(f"⚠️ Skipping session_aggregations: '{df_name}' dataframe not found or empty")
            return
    
    # Join shopping_cart with cart_items to get item-level details
    cart_with_items = dataframes["shopping_cart"].join(
        dataframes["cart_items"], "cart_id", "left"
    )

    session_cart_agg = (
        cart_with_items.filter(col("session_id").isNotNull())
        .groupBy("session_id")
        .agg(
            # Items added to cart (count of cart_item_id)
            count("cart_item_id").alias("items_added_to_cart"),
            # Cart value (unit_price * quantity from cart_items)
            spark_sum(
                when(
                    col("unit_price").isNotNull()
                    & col("quantity").isNotNull()
                    & (col("unit_price") > 0)
                    & (col("quantity") > 0),
                    col("unit_price") * col("quantity"),
                )
            ).alias("cart_value"),
        )
    )
    session_orders = (
        dataframes["customer_sessions"]
        .join(
            dataframes["orders"].select("customer_id", "order_id", "order_placed_at"),
            "customer_id",
            "left",
        )
        .filter(
            col("order_placed_at").isNotNull()
            & (
                # Match orders placed during the session window
                (
                    (col("order_placed_at") >= col("session_start"))
                    & (col("order_placed_at") <= col("session_end"))
                )
                |
                # Also match orders placed on the same calendar day as session
                # (accounts for sessions/orders generated independently)
                (to_date(col("order_placed_at")) == to_date(col("session_start")))
            )
        )
        .groupBy("session_id")
        .agg(countDistinct("order_id").alias("orders_from_session"))
    )
    dataframes["customer_sessions"] = (
        dataframes["customer_sessions"]
        .join(session_cart_agg, "session_id", "left")
        .join(session_orders, "session_id", "left")
    )
    dataframes["customer_sessions"] = (
        dataframes["customer_sessions"]
        .withColumns(
            {
                # Session duration (already calculated, ensure exists)
                "session_duration_minutes": coalesce(
                    col("session_duration_minutes"), lit(0)
                ),
                # Total pages viewed (already exists)
                "total_pages_viewed": coalesce(col("pages_viewed"), lit(0)),
                # Total products viewed (already exists)
                "total_products_viewed": coalesce(col("products_viewed"), lit(0)),
                # Items added to cart (from join)
                "items_added_to_cart": coalesce(col("items_added_to_cart"), lit(0)),
                # Cart value (from join)
                "cart_value": coalesce(col("cart_value"), lit(0.0)),
                # Converted flag (1 if order placed, 0 otherwise)
                "converted": when(
                    col("orders_from_session").isNotNull()
                    & (col("orders_from_session") > 0),
                    lit(1),
                ).otherwise(lit(0)),
                # Abandoned flag (already exists as cart_abandonment_flag)
                "abandoned": coalesce(col("cart_abandonment_flag"), lit(0)),
                # Device type (already exists)
                "device_type": col("device_type"),
                # Referrer source (already exists)
                "referrer_source": col("referrer_source"),
                # Pages per minute (already calculated, ensure exists)
                "pages_per_minute": when(
                    col("session_duration_minutes").isNotNull()
                    & (col("session_duration_minutes") > 0)
                    & col("pages_viewed").isNotNull(),
                    col("pages_viewed") / col("session_duration_minutes"),
                ).otherwise(lit(0.0)),
                # Additional useful metrics
                "products_per_page": when(
                    col("pages_viewed").isNotNull()
                    & (col("pages_viewed") > 0)
                    & col("products_viewed").isNotNull(),
                    col("products_viewed") / col("pages_viewed"),
                ).otherwise(lit(0.0)),
                "cart_add_rate": when(
                    col("products_viewed").isNotNull()
                    & (col("products_viewed") > 0)
                    & col("items_added_to_cart").isNotNull(),
                    (col("items_added_to_cart") / col("products_viewed")) * 100,
                ).otherwise(lit(0.0)),
                "avg_cart_item_value": when(
                    col("cart_value").isNotNull()
                    & col("items_added_to_cart").isNotNull()
                    & (col("items_added_to_cart") > 0),
                    col("cart_value") / col("items_added_to_cart"),
                ).otherwise(lit(0.0)),
                "session_engagement_score": when(
                    col("session_duration_minutes").isNotNull()
                    & col("products_viewed").isNotNull()
                    & col("items_added_to_cart").isNotNull(),
                    (
                        # Duration weight (30%)
                        (col("session_duration_minutes") / lit(60)) * lit(0.3)
                        +
                        # Product views weight (40%)
                        (col("products_viewed") / lit(10)) * lit(0.4)
                        +
                        # Cart additions weight (30%)
                        (col("items_added_to_cart") / lit(5)) * lit(0.3)
                    )
                    * lit(100),
                ).otherwise(lit(0.0)),
                "session_type": when(col("converted") == 1, "Converted")
                .when(col("abandoned") == 1, "Abandoned")
                .when(col("items_added_to_cart") > 0, "Cart Activity")
                .when(col("products_viewed") > 0, "Browsing")
                .otherwise("Bounce"),
            }
        )
        .dropDuplicates(["session_id"])
    )
