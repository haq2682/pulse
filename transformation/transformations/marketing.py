from pyspark.sql.functions import *

def transform_marketing(dataframes):
    dataframes["marketing_campaigns"] = dataframes["marketing_campaigns"] \
        .join(
            dataframes["customer_sessions"]
            .join(
                dataframes["marketing_campaigns"],
                (col("session_start") >= col("start_date")) & 
                (col("session_start") <= col("end_date")),
                "cross"
            )
            .select(
                col("session_id"),
                col("customer_id"),
                col("conversion_flag"),
                col("campaign_id"),
                col("start_date").alias("campaign_start"),
                col("end_date").alias("campaign_end")
            )
            .filter(col("conversion_flag") == "true")
            .join(
                dataframes["orders"].filter(
                    (col("order_status") != "cancelled") & 
                    (col("order_status") != "refunded")
                ),
                "customer_id",
                "inner"
            )
            .filter(
                (col("order_placed_at") >= col("campaign_start")) &
                (col("order_placed_at") <= expr("date_add(campaign_end, 7)"))
            )
            .dropDuplicates(["order_id", "campaign_id"])
            .groupBy("campaign_id")
            .agg(sum(col("total_amount").cast("double")).alias("campaign_revenue")),
            "campaign_id",
            "left"
        ) \
        .withColumn("revenue_generated", coalesce(col("campaign_revenue"), lit(0))) \
        .drop("campaign_revenue") \
        .withColumns(
            {
                "campaign_duration_days": when(
                    col("start_date").isNotNull() & col("end_date").isNotNull(),
                    datediff(to_date(col("end_date")), to_date(col("start_date")))
                ),
                "campaign_roi": when(
                    col("spent_amount") > 0,
                    ((col("revenue_generated") - col("spent_amount")) / col("spent_amount")) * 100
                ).otherwise(None),
                "click_through_rate": when(
                    (col("impressions") > 0) & col("clicks").isNotNull() & col("impressions").isNotNull(),
                    (col("clicks") / col("impressions")) * 100
                ).otherwise(None),
                "conversion_rate": when(
                    (col("impressions") > 0) & col("conversions").isNotNull() & col("impressions").isNotNull(),
                    (col("conversions") / col("impressions")) * 100
                ).otherwise(None),
                "cost_per_conversion": when(
                    (col("conversions") > 0) & col("spent_amount").isNotNull(),
                    col("spent_amount") / col("conversions")
                ).otherwise(None),
                "cost_per_click": when(
                    (col("clicks") > 0) & col("spent_amount").isNotNull(),
                    col("spent_amount") / col("clicks")
                ).otherwise(None),
                "campaign_efficiency_score": when(
                    col("conversions").isNotNull() & col("budget").isNotNull() & (col("budget") != 0),
                    (col("conversions") * 100) / col("budget")
                ).otherwise(None)
            }
        )

def add_marketing_metrics(dataframes):
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
                (col("order_status") != "cancelled") & (col("order_status") != "refunded")
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
            sum(when(col("total_amount").isNotNull() & (col("total_amount") > 0), col("total_amount"))).alias("agg_revenue_generated"),
            countDistinct("order_id").alias("agg_orders_from_campaign"),
        )
    )

    mc = dataframes["marketing_campaigns"]
    cols_to_drop = [c for c in mc.columns if "revenue_generated" in c]
    if cols_to_drop:
        mc = mc.drop(*cols_to_drop)

    dataframes["marketing_campaigns"] = (
        mc.join(campaign_revenue, "campaign_id", "left")
          .withColumn("revenue_generated", coalesce(col("agg_revenue_generated"), lit(0.0)))
          .withColumn("orders_from_campaign", coalesce(col("agg_orders_from_campaign"), lit(0)))
          .drop("agg_revenue_generated", "agg_orders_from_campaign")
    )

    dataframes["marketing_campaigns"] = dataframes["marketing_campaigns"].withColumns({
        "total_impressions": coalesce(col("impressions"), lit(0)),
        "total_clicks": coalesce(col("clicks"), lit(0)),
        "total_conversions": coalesce(col("conversions"), lit(0)),
        "total_budget": coalesce(col("budget"), lit(0.0)),
        "total_spent": coalesce(col("spent_amount"), lit(0.0)),
        "budget_utilization_rate": when(col("budget").isNotNull() & col("spent_amount").isNotNull() & (col("budget") > 0), (col("spent_amount") / col("budget")) * 100),
        "ctr": when(col("impressions").isNotNull() & col("clicks").isNotNull() & (col("impressions") > 0), (col("clicks") / col("impressions")) * 100),
        "conversion_rate": when(col("clicks").isNotNull() & col("conversions").isNotNull() & (col("clicks") > 0), (col("conversions") / col("clicks")) * 100),
        "cost_per_click": when(col("spent_amount").isNotNull() & col("clicks").isNotNull() & (col("clicks") > 0), col("spent_amount") / col("clicks")),
        "cost_per_conversion": when(col("conversions").isNotNull() & col("spent_amount").isNotNull() & (col("conversions") > 0), col("spent_amount") / col("conversions")),
        "roi": when((col("revenue_generated") > 0) & col("spent_amount").isNotNull() & (col("spent_amount") > 0), ((col("revenue_generated") - col("spent_amount")) / col("spent_amount")) * 100),
        "roas": when((col("revenue_generated") > 0) & col("spent_amount").isNotNull() & (col("spent_amount") > 0), col("revenue_generated") / col("spent_amount")),
        "avg_order_value": when((col("revenue_generated") > 0) & col("conversions").isNotNull() & (col("conversions") > 0), col("revenue_generated") / col("conversions")),
        "days_active": when(col("start_date").isNotNull() & col("end_date").isNotNull(), datediff(col("end_date"), col("start_date")) + lit(1)),
        "revenue_per_impression": when((col("revenue_generated") > 0) & col("impressions").isNotNull() & (col("impressions") > 0), col("revenue_generated") / col("impressions")),
        "revenue_per_click": when((col("revenue_generated") > 0) & col("clicks").isNotNull() & (col("clicks") > 0), col("revenue_generated") / col("clicks")),
        "campaign_profit": when(col("spent_amount").isNotNull(), col("revenue_generated") - col("spent_amount")),
        "cost_efficiency_ratio": when((col("revenue_generated") > 0) & col("spent_amount").isNotNull(), (col("spent_amount") / col("revenue_generated")) * 100),
        "engagement_rate": when(col("impressions").isNotNull() & col("clicks").isNotNull() & col("conversions").isNotNull() & (col("impressions") > 0), ((col("clicks") + col("conversions")) / col("impressions")) * 100),
        "campaign_status_derived": when(col("start_date").isNotNull() & col("end_date").isNotNull(), when(current_date() < col("start_date"), "Scheduled").when((current_date() >= col("start_date")) & (current_date() <= col("end_date")), "Active").otherwise("Completed")).otherwise(col("campaign_status")),
        "days_until_end": when(col("end_date").isNotNull() & (col("end_date") >= current_date()), datediff(col("end_date"), current_date())),
        "performance_tier": when(col("roi").isNotNull(), when(col("roi") >= 200, "Excellent").when(col("roi") >= 100, "Good").when(col("roi") >= 0, "Break-even").otherwise("Poor")),
        "budget_status": when(col("budget_utilization_rate").isNotNull(), when(col("budget_utilization_rate") >= 100, "Over Budget").when(col("budget_utilization_rate") >= 80, "Near Budget").when(col("budget_utilization_rate") >= 50, "On Track").otherwise("Under Budget")),
    })

    dataframes["marketing_campaigns"] = dataframes["marketing_campaigns"].withColumn(
        "campaign_efficiency_score",
        when(col("roi").isNotNull() & col("ctr").isNotNull() & col("conversion_rate").isNotNull(),
            ((col("roi") / lit(100)) * lit(0.5) + (col("ctr") / lit(10)) * lit(0.25) + (col("conversion_rate") / lit(5)) * lit(0.25)) * lit(100)),
    )
