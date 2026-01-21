from pyspark.sql.functions import (
    col,
    current_timestamp,
    expr,
    greatest,
    lit,
    unix_timestamp,
    when,
)


def transform_carts(dataframes):
    # Skip transformation if required dataframes don't exist
    required_dataframes = ["cart_items", "shopping_cart", "customer_sessions", "orders"]
    for df_name in required_dataframes:
        if df_name not in dataframes or dataframes[df_name] is None or dataframes[df_name].count() == 0:
            print(f"⚠️ Skipping transform_carts: '{df_name}' dataframe not found or empty")
            return
    
    # Transform cart_items: calculate cart_age_time based on added_at
    dataframes["cart_items"] = dataframes["cart_items"].withColumn(
        "cart_age_time",
        when(
            col("added_at").isNotNull(),
            greatest(
                unix_timestamp(current_timestamp())
                - unix_timestamp(col("added_at")),
                lit(0),
            ),
        ),
    )

    # Determine cart abandonment flag by joining shopping_cart with sessions and orders
    # A cart is considered "Abandoned" if there was activity but no order was placed
    cart_abandonment_df = (
        dataframes["shopping_cart"]
        .join(dataframes["customer_sessions"], "session_id", "left")
        .join(dataframes["orders"], "customer_id", "left")
        .select(
            col("cart_id"),
            when(
                col("order_id").isNotNull(),
                lit("Converted"),  # Cart led to a purchase
            )
            .when(
                (col("products_viewed") > 0) & (col("order_id").isNull()),
                lit("Abandoned"),  # Products viewed but no order - abandoned
            )
            .otherwise(lit("Active"))  # Still active, not abandoned yet
            .alias("cart_abandonment_flag"),
        )
        .dropDuplicates(["cart_id"])
    )

    dataframes["shopping_cart"] = (
        dataframes["shopping_cart"]
        .join(cart_abandonment_df, "cart_id", "left")
        .dropDuplicates(["cart_id"])
    )
