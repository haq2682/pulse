from pyspark.sql.functions import (
    col,
    countDistinct,
    datediff,
    lit,
    when,
    expr,
    coalesce,
    count,
    max as spark_max,
    sum as spark_sum,
    avg as spark_avg,
)


def rfm_segmentation(dataframes):
    rfm_metrics = (
        dataframes["orders"]
        .filter(col("customer_id").isNotNull() & col("order_placed_at").isNotNull())
        .groupBy("customer_id")
        .agg(
            # Recency: Days since last order
            datediff(lit("2025-11-14"), spark_max("order_placed_at")).alias(
                "days_since_last_order"
            ),
            # Frequency: Total number of orders
            countDistinct("order_id").alias("total_orders_rfm"),
            # Monetary: Total revenue
            spark_sum(
                when(
                    col("total_amount").isNotNull() & (col("total_amount") > 0),
                    col("total_amount"),
                )
            ).alias("total_revenue_rfm"),
        )
    )
    recency_percentiles = rfm_metrics.approxQuantile(
        "days_since_last_order", [0.2, 0.4, 0.6, 0.8], 0.01
    )
    frequency_percentiles = rfm_metrics.approxQuantile(
        "total_orders_rfm", [0.2, 0.4, 0.6, 0.8], 0.01
    )
    monetary_percentiles = rfm_metrics.approxQuantile(
        "total_revenue_rfm", [0.2, 0.4, 0.6, 0.8], 0.01
    )
    # Check if percentiles are valid before using them
    if (
        len(recency_percentiles) == 4
        and len(frequency_percentiles) == 4
        and len(monetary_percentiles) == 4
    ):
        rfm_scored = rfm_metrics.withColumns(
            {
                # Recency Score (1-5, lower days = higher score)
                "recency_score": when(
                    col("days_since_last_order") <= recency_percentiles[0], lit(5)
                )
                .when(col("days_since_last_order") <= recency_percentiles[1], lit(4))
                .when(col("days_since_last_order") <= recency_percentiles[2], lit(3))
                .when(col("days_since_last_order") <= recency_percentiles[3], lit(2))
                .otherwise(lit(1)),
                # Frequency Score (1-5, higher orders = higher score)
                "frequency_score": when(
                    col("total_orders_rfm") >= frequency_percentiles[3], lit(5)
                )
                .when(col("total_orders_rfm") >= frequency_percentiles[2], lit(4))
                .when(col("total_orders_rfm") >= frequency_percentiles[1], lit(3))
                .when(col("total_orders_rfm") >= frequency_percentiles[0], lit(2))
                .otherwise(lit(1)),
                # Monetary Score (1-5, higher revenue = higher score)
                "monetary_score": when(
                    col("total_revenue_rfm") >= monetary_percentiles[3], lit(5)
                )
                .when(col("total_revenue_rfm") >= monetary_percentiles[2], lit(4))
                .when(col("total_revenue_rfm") >= monetary_percentiles[1], lit(3))
                .when(col("total_revenue_rfm") >= monetary_percentiles[0], lit(2))
                .otherwise(lit(1)),
            }
        )
    else:
        # If percentiles calculation failed, assign default scores
        rfm_scored = rfm_metrics.withColumns(
            {
                "recency_score": lit(3),
                "frequency_score": lit(3),
                "monetary_score": lit(3),
            }
        )
    rfm_scored = rfm_scored.withColumn(
        "rfm_segment", expr("concat(recency_score, frequency_score, monetary_score)")
    )
    rfm_scored = rfm_scored.withColumn(
        "customer_segment_label",
        when(
            (col("recency_score") >= 4)
            & (col("frequency_score") >= 4)
            & (col("monetary_score") >= 4),
            "Champions",
        )
        .when(
            (col("recency_score") >= 3)
            & (col("frequency_score") >= 3)
            & (col("monetary_score") >= 3),
            "Loyal Customers",
        )
        .when(
            (col("recency_score") >= 4)
            & (col("frequency_score") <= 2)
            & (col("monetary_score") <= 2),
            "Promising",
        )
        .when(
            (col("recency_score") >= 3)
            & (col("frequency_score") <= 2)
            & (col("monetary_score") <= 2),
            "Potential Loyalists",
        )
        .when(
            (col("recency_score") >= 4)
            & (col("frequency_score") >= 3)
            & (col("monetary_score") <= 2),
            "New Customers",
        )
        .when(
            (col("recency_score") >= 3)
            & (col("frequency_score") >= 1)
            & (col("monetary_score") >= 3),
            "Need Attention",
        )
        .when(
            (col("recency_score") <= 2)
            & (col("frequency_score") >= 3)
            & (col("monetary_score") >= 3),
            "At Risk",
        )
        .when(
            (col("recency_score") <= 2)
            & (col("frequency_score") <= 2)
            & (col("monetary_score") >= 3),
            "Cant Lose Them",
        )
        .when(
            (col("recency_score") <= 1)
            & (col("frequency_score") >= 2)
            & (col("monetary_score") >= 2),
            "Hibernating",
        )
        .when(
            (col("recency_score") <= 2)
            & (col("frequency_score") <= 2)
            & (col("monetary_score") <= 2),
            "Lost",
        )
        .otherwise("Others"),
    )
    rfm_scored = rfm_scored.withColumns(
        {
            # Overall RFM score (average of three scores)
            "rfm_overall_score": (
                col("recency_score") + col("frequency_score") + col("monetary_score")
            )
            / lit(3),
            # RFM category (Simple classification)
            "rfm_category": when(
                (col("recency_score") + col("frequency_score") + col("monetary_score"))
                >= 12,
                "High Value",
            )
            .when(
                (col("recency_score") + col("frequency_score") + col("monetary_score"))
                >= 9,
                "Medium Value",
            )
            .otherwise("Low Value"),
            # Engagement level
            "engagement_level": when(col("recency_score") >= 4, "Highly Engaged")
            .when(col("recency_score") >= 3, "Moderately Engaged")
            .otherwise("Low Engagement"),
            # Purchase behavior
            "purchase_behavior": when(col("frequency_score") >= 4, "Frequent Buyer")
            .when(col("frequency_score") >= 2, "Occasional Buyer")
            .otherwise("Rare Buyer"),
            # Spending pattern
            "spending_pattern": when(col("monetary_score") >= 4, "High Spender")
            .when(col("monetary_score") >= 2, "Average Spender")
            .otherwise("Low Spender"),
            # Risk flag
            "churn_risk": when(
                col("customer_segment_label").isin(
                    "At Risk", "Cant Lose Them", "Hibernating", "Lost"
                ),
                "High Risk",
            )
            .when(col("customer_segment_label") == "Need Attention", "Medium Risk")
            .otherwise("Low Risk"),
            # Handle nulls
            "days_since_last_order": coalesce(col("days_since_last_order"), lit(999)),
            "total_orders_rfm": coalesce(col("total_orders_rfm"), lit(0)),
            "total_revenue_rfm": coalesce(col("total_revenue_rfm"), lit(0.0)),
        }
    )
    dataframes["rfm_segmentation"] = rfm_scored
    dataframes["customers"] = (
        dataframes["customers"]
        .join(
            rfm_scored.select(
                col("customer_id").alias("rfm_customer_id"),
                col("recency_score").alias("rfm_recency_score"),
                col("frequency_score").alias("rfm_frequency_score"),
                col("monetary_score").alias("rfm_monetary_score"),
                col("rfm_segment").alias("rfm_segment_code"),
                col("customer_segment_label").alias("rfm_customer_segment_label"),
                col("rfm_overall_score").alias("rfm_overall_score_value"),
                col("rfm_category").alias("rfm_category_tier"),
                col("churn_risk").alias("rfm_churn_risk"),
            ),
            dataframes["customers"]["customer_id"] == col("rfm_customer_id"),
            "left",
        )
        .drop("rfm_customer_id")
        .withColumnRenamed("rfm_recency_score", "recency_score")
        .withColumnRenamed("rfm_frequency_score", "frequency_score")
        .withColumnRenamed("rfm_monetary_score", "monetary_score")
        .withColumnRenamed("rfm_segment_code", "rfm_segment")
        .withColumnRenamed("rfm_customer_segment_label", "customer_segment_label")
        .withColumnRenamed("rfm_overall_score_value", "rfm_overall_score")
        .withColumnRenamed("rfm_category_tier", "rfm_category")
        .withColumnRenamed("rfm_churn_risk", "churn_risk")
    ).dropDuplicates(["customer_id"])
    rfm_segment_summary = (
        rfm_scored.groupBy("customer_segment_label")
        .agg(
            count("customer_id").alias("customer_count"),
            spark_avg("total_revenue_rfm").alias("avg_revenue"),
            spark_avg("total_orders_rfm").alias("avg_orders"),
            spark_avg("days_since_last_order").alias("avg_days_since_order"),
            spark_avg("rfm_overall_score").alias("avg_rfm_score"),
        )
        .orderBy(col("customer_count").desc())
    )

    dataframes["rfm_segment_summary"] = rfm_segment_summary
