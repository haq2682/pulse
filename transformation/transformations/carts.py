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
    cart_abandonment_df = (
        dataframes["shopping_cart"]
        .join(dataframes["customer_sessions"], "session_id", "left")
        .join(dataframes["orders"], "customer_id", "left")
        .select(
            col("cart_id"),
            when(
                (col("products_viewed") > 0)
                & (col("order_id").isNull())
                & (col("cart_id").isNotNull()),
                lit("Active"),
            )
            .otherwise(lit("Inactive"))
            .alias("cart_abandonment_flag"),
        )
        .dropDuplicates(["cart_id"])
    )

    dataframes["shopping_cart"] = (
        dataframes["shopping_cart"]
        .join(cart_abandonment_df, "cart_id", "left")
        .dropDuplicates(["cart_id"])
    )
