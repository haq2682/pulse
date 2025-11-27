import pyspark
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    coalesce,
    datediff,
    expr,
    lit,
    to_date,
    when,
    sum as spark_sum,
)


def transform_carts(dataframes):
    dataframes["shopping_cart"] = dataframes["shopping_cart"].withColumn(
        "cart_age_time",
        when(
            col("added_date").isNotNull(),
            greatest(
                unix_timestamp(current_date())
                - unix_timestamp(to_timestamp(col("added_date"))),
                lit(0),
            ),
        ),
    )

    dataframes["shopping_cart"] = (
        dataframes["shopping_cart"]
        .join(
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
            .dropDuplicates(["cart_id"]),
            "cart_id",
            "left",
        )
        .dropDuplicates(["cart_id"])
    )
