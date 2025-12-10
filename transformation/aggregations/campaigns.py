from pyspark.sql.functions import (
    col,
    expr,
    sum as spark_sum,
    when,
    countDistinct,
    coalesce,
    lit,
    datediff,
    current_date,
)

def aggregate_campaigns(dataframes):

    campaign_revenue = (
        dataframes["customer_sessions"]
        .join(
            dataframes["marketing_campaigns"],
            (col("session_start") >= col("start_date"))
            & (col("session_start") <= col("end_date")),
            "cross",
        )
        .filter(col("conversion_flag") == 1)
        .join(
            dataframes["orders"].filter(
                (col("order_status") != "cancelled")
                & (col("order_status") != "refunded")
            ),
            "customer_id",
            "inner",
        )
        .filter(
            (col("order_placed_at") >= col("start_date"))
            & (col("order_placed_at") <= expr("date_add(end_date, 7)"))
        )
        .dropDuplicates(["order_id", "campaign_id"])
        .groupBy("campaign_id")
        .agg(
            spark_sum(
                when(
                    col("total_amount").isNotNull() & (col("total_amount") > 0),
                    col("total_amount"),
                )
            ).alias("revenue_generated"),
            countDistinct("order_id").alias("orders_from_campaign"),
        )
    )

    campaign_revenue = campaign_revenue.select(
        "campaign_id",
        col("revenue_generated").alias("agg_revenue_generated"),
        col("orders_from_campaign").alias("agg_orders_from_campaign"),
    )

    mc = dataframes["marketing_campaigns"]

    cols_to_drop = [c for c in mc.columns if "revenue_generated" in c]
    if cols_to_drop:
        mc = mc.drop(*cols_to_drop)

    dataframes["marketing_campaigns"] = (
        mc.join(campaign_revenue, "campaign_id", "left")
        .withColumn(
            "revenue_generated",
            coalesce(col("agg_revenue_generated"), lit(0.0)),
        )
        .withColumn(
            "orders_from_campaign",
            coalesce(col("agg_orders_from_campaign"), lit(0)),
        )
        .drop("agg_revenue_generated", "agg_orders_from_campaign")
    )

    dataframes["marketing_campaigns"] = dataframes["marketing_campaigns"].withColumns(
        {
            # Total impressions (already exists, ensure not null)
            "total_impressions": coalesce(col("impressions"), lit(0)),
            # Total clicks (already exists, ensure not null)
            "total_clicks": coalesce(col("clicks"), lit(0)),
            # Total conversions (already exists, ensure not null)
            "total_conversions": coalesce(col("conversions"), lit(0)),
            # Total budget (already exists)
            "total_budget": coalesce(col("budget"), lit(0.0)),
            # Total spent (already exists)
            "total_spent": coalesce(col("spent_amount"), lit(0.0)),
            # Budget utilization rate
            "budget_utilization_rate": when(
                col("budget").isNotNull()
                & col("spent_amount").isNotNull()
                & (col("budget") > 0),
                (col("spent_amount") / col("budget")) * 100,
            ),
            # Click-through rate (CTR)
            "ctr": when(
                col("impressions").isNotNull()
                & col("clicks").isNotNull()
                & (col("impressions") > 0),
                (col("clicks") / col("impressions")) * 100,
            ),
            # Conversion rate
            "conversion_rate": when(
                col("clicks").isNotNull()
                & col("conversions").isNotNull()
                & (col("clicks") > 0),
                (col("conversions") / col("clicks")) * 100,
            ),
            # Cost per click (CPC)
            "cost_per_click": when(
                col("spent_amount").isNotNull()
                & col("clicks").isNotNull()
                & (col("clicks") > 0),
                col("spent_amount") / col("clicks"),
            ),
            # Cost per conversion (CPA)
            "cost_per_conversion": when(
                col("conversions").isNotNull()
                & col("spent_amount").isNotNull()
                & (col("conversions") > 0),
                col("spent_amount") / col("conversions"),
            ),
            # Return on Investment (ROI)
            "roi": when(
                (col("revenue_generated") > 0)
                & col("spent_amount").isNotNull()
                & (col("spent_amount") > 0),
                ((col("revenue_generated") - col("spent_amount")) / col("spent_amount"))
                * 100,
            ),
            # Return on Ad Spend (ROAS)
            "roas": when(
                (col("revenue_generated") > 0)
                & col("spent_amount").isNotNull()
                & (col("spent_amount") > 0),
                col("revenue_generated") / col("spent_amount"),
            ),
            # Average order value from campaign
            "avg_order_value": when(
                (col("revenue_generated") > 0)
                & col("conversions").isNotNull()
                & (col("conversions") > 0),
                col("revenue_generated") / col("conversions"),
            ),
            # Days active (campaign duration)
            "days_active": when(
                col("start_date").isNotNull() & col("end_date").isNotNull(),
                datediff(col("end_date"), col("start_date")) + lit(1),
            ),
            # Revenue per impression
            "revenue_per_impression": when(
                (col("revenue_generated") > 0)
                & col("impressions").isNotNull()
                & (col("impressions") > 0),
                col("revenue_generated") / col("impressions"),
            ),
            # Revenue per click
            "revenue_per_click": when(
                (col("revenue_generated") > 0)
                & col("clicks").isNotNull()
                & (col("clicks") > 0),
                col("revenue_generated") / col("clicks"),
            ),
            # Profit from campaign
            "campaign_profit": when(
                col("spent_amount").isNotNull(),
                col("revenue_generated") - col("spent_amount"),
            ),
            # Cost efficiency ratio
            "cost_efficiency_ratio": when(
                (col("revenue_generated") > 0) & col("spent_amount").isNotNull(),
                (col("spent_amount") / col("revenue_generated")) * 100,
            ),
            # Engagement rate
            "engagement_rate": when(
                col("impressions").isNotNull()
                & col("clicks").isNotNull()
                & col("conversions").isNotNull()
                & (col("impressions") > 0),
                ((col("clicks") + col("conversions")) / col("impressions")) * 100,
            ),
            # Campaign status based on dates
            "campaign_status_derived": when(
                col("start_date").isNotNull() & col("end_date").isNotNull(),
                when(current_date() < col("start_date"), "Scheduled")
                .when(
                    (current_date() >= col("start_date"))
                    & (current_date() <= col("end_date")),
                    "Active",
                )
                .otherwise("Completed"),
            ).otherwise(col("campaign_status")),
            # Days until end
            "days_until_end": when(
                col("end_date").isNotNull() & (col("end_date") >= current_date()),
                datediff(col("end_date"), current_date()),
            ),
            # Performance tier
            "performance_tier": when(
                col("roi").isNotNull(),
                when(col("roi") >= 200, "Excellent")
                .when(col("roi") >= 100, "Good")
                .when(col("roi") >= 0, "Break-even")
                .otherwise("Poor"),
            ),
            # Budget status
            "budget_status": when(
                col("budget_utilization_rate").isNotNull(),
                when(col("budget_utilization_rate") >= 100, "Over Budget")
                .when(col("budget_utilization_rate") >= 80, "Near Budget")
                .when(col("budget_utilization_rate") >= 50, "On Track")
                .otherwise("Under Budget"),
            ),
        }
    )

    dataframes["marketing_campaigns"] = (
        dataframes["marketing_campaigns"]
        .withColumn(
            "campaign_efficiency_score",
            when(
                col("roi").isNotNull()
                & col("ctr").isNotNull()
                & col("conversion_rate").isNotNull(),
                (
                    (col("roi") / lit(100)) * lit(0.5)
                    + (col("ctr") / lit(10)) * lit(0.25)
                    + (col("conversion_rate") / lit(5)) * lit(0.25)
                )
                * lit(100),
            ),
        )
        .dropDuplicates(["campaign_id"])
    )
