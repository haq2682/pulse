from pyspark.sql.types import DoubleType

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


def transform_campaigns(dataframes):
    # Skip transformation if marketing_campaigns doesn't exist
    if "marketing_campaigns" not in dataframes or dataframes["marketing_campaigns"] is None or dataframes["marketing_campaigns"].count() == 0:
        print("⚠️ Skipping transform_campaigns: 'marketing_campaigns' dataframe not found or empty")
        return
    
    # Skip if required dependencies don't exist
    required_dataframes = ["customer_sessions", "orders"]
    for df_name in required_dataframes:
        if df_name not in dataframes or dataframes[df_name] is None:
            print(f"⚠️ Skipping transform_campaigns: '{df_name}' dataframe not found")
            return
    
    dataframes["marketing_campaigns"] = (
        dataframes["marketing_campaigns"]
        .join(
            dataframes["customer_sessions"]
            .join(
                dataframes["marketing_campaigns"],
                (col("session_start") >= col("start_date"))
                & (col("session_start") <= col("end_date")),
                "cross",
            )
            .select(
                col("session_id"),
                col("customer_id"),
                col("conversion_flag"),
                col("campaign_id"),
                col("start_date").alias("campaign_start"),
                col("end_date").alias("campaign_end"),
            )
            .filter((col("conversion_flag") == lit(True)) | (col("conversion_flag") == "true"))
            .join(
                dataframes["orders"].filter(
                    (col("order_status") != "cancelled")
                    & (col("order_status") != "refunded")
                ),
                "customer_id",
                "inner",
            )
            # Add time constraint: orders must be placed during campaign or within 7 days after
            .filter(
                (col("order_placed_at") >= col("campaign_start"))
                & (col("order_placed_at") <= expr("date_add(campaign_end, 7)"))
            )
            # Prevent double counting - each order attributed once per campaign
            .dropDuplicates(["order_id", "campaign_id"])
            .groupBy("campaign_id")
            .agg(
                spark_sum(col("total_amount").cast(DoubleType())).alias(
                    "campaign_revenue"
                )
            ),
            "campaign_id",
            "left",  # LEFT JOIN to keep all campaigns even without revenue
        )
        .withColumn("revenue_generated", coalesce(col("campaign_revenue"), lit(0)))
        .drop("campaign_revenue")
        .withColumns(
            {
                "campaign_duration_days": when(
                    col("start_date").isNotNull() & col("end_date").isNotNull(),
                    datediff(to_date(col("end_date")), to_date(col("start_date"))),
                ),
                "campaign_roi": when(
                    col("spent_amount") > 0,
                    (
                        (col("revenue_generated") - col("spent_amount"))
                        / col("spent_amount")
                    )
                    * 100,
                ).otherwise(None),
                "click_through_rate": when(
                    (col("impressions") > 0)
                    & col("clicks").isNotNull()
                    & col("impressions").isNotNull(),
                    (col("clicks") / col("impressions")) * 100,
                ).otherwise(None),
                "conversion_rate": when(
                    (col("impressions") > 0)
                    & col("conversions").isNotNull()
                    & col("impressions").isNotNull(),
                    (col("conversions") / col("impressions")) * 100,
                ).otherwise(None),
                "cost_per_conversion": when(
                    (col("conversions") > 0) & col("spent_amount").isNotNull(),
                    col("spent_amount") / col("conversions"),
                ).otherwise(None),
                "cost_per_click": when(
                    (col("clicks") > 0) & col("spent_amount").isNotNull(),
                    col("spent_amount") / col("clicks"),
                ).otherwise(None),
                "campaign_efficiency_score": when(
                    col("conversions").isNotNull()
                    & col("budget").isNotNull()
                    & (col("budget") != 0),
                    (col("conversions") * 100) / col("budget"),
                ).otherwise(None),
            }
        )
    )
