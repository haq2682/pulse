from pyspark.sql.functions import (
    col,
    count,
    current_date,
    datediff,
    floor,
    lit,
    to_date,
    when,
    max as spark_max,
    sum as spark_sum,
)


def transform_customers(dataframes):
    customers_with_orders = (
        dataframes["customers"]
        .drop("order_recency_days", "order_frequency", "order_total_spent")
        .join(
            dataframes["orders"]
            .select("customer_id", "order_placed_at", "order_id", "total_amount")
            .groupBy("customer_id")
            .agg(
                datediff(current_date(), spark_max("order_placed_at")).alias(
                    "order_recency_days"
                ),
                count("order_id").alias("order_frequency"),
                spark_sum("total_amount").alias("order_total_spent"),
            ),
            "customer_id",
            "left",
        )
    )

    dataframes["customers"] = customers_with_orders.withColumns(
        {
            "customer_age": when(
                col("date_of_birth").isNotNull(),
                floor(datediff(current_date(), col("date_of_birth")) / 365),
            ),
            "customer_tenure_days": when(
                col("account_created_at").isNotNull(),
                datediff(current_date(), to_date(col("account_created_at"))),
            ),
            "days_since_last_login": when(
                col("last_login_date").isNotNull(),
                datediff(current_date(), to_date(col("last_login_date"))),
            ),
            "customer_age_group": when(
                col("customer_age").isNotNull(),
                when(col("customer_age") < 18, "Under 18")
                .when((col("customer_age") >= 18) & (col("customer_age") < 25), "18-24")
                .when((col("customer_age") >= 25) & (col("customer_age") < 35), "25-34")
                .when((col("customer_age") >= 35) & (col("customer_age") < 45), "35-44")
                .when((col("customer_age") >= 45) & (col("customer_age") < 55), "45-54")
                .when((col("customer_age") >= 55) & (col("customer_age") < 65), "55-64")
                .otherwise("65 and over"),
            ),
            "customer_activity_status": when(
                col("days_since_last_login").isNotNull(),
                when(col("days_since_last_login") <= 30, "Active")
                .when(
                    (col("days_since_last_login") > 30)
                    & (col("days_since_last_login") <= 90),
                    "Inactive",
                )
                .otherwise("Dormant"),
            ),
            "customer_segment": when(
                col("order_recency_days").isNotNull(),
                when(
                    (col("order_frequency") >= 10)
                    & (col("order_total_spent") >= 1000)
                    & (col("order_recency_days") <= 30),
                    "High Value",
                )
                .when(
                    (col("order_frequency") >= 5)
                    & (col("order_total_spent") >= 500)
                    & (col("order_recency_days") <= 60),
                    "Medium Value",
                )
                .when(
                    (col("order_frequency") >= 2)
                    & (col("order_total_spent") >= 100)
                    & (col("order_recency_days") <= 90),
                    "Low Value",
                )
                .when(
                    (col("order_frequency") == 1) & (col("order_recency_days") <= 30),
                    "New",
                )
                .when(
                    (col("order_recency_days") > 90)
                    & (col("order_recency_days") <= 180),
                    "At Risk",
                )
                .when(col("order_recency_days") > 180, "Lost")
                .otherwise("Uncategorized"),
            ),
            "customer_lifetime_value": (
                (
                    col("order_total_spent")
                    / when(
                        col("order_frequency") > 0, col("order_frequency")
                    ).otherwise(lit(1))
                )
                * (
                    (
                        col("order_frequency")
                        / when(
                            col("customer_tenure_days") > 0, col("customer_tenure_days")
                        ).otherwise(lit(365))
                    )
                    * 365
                )
                * lit(3.0)  # Expected lifespan in years
            ),
        }
    )
