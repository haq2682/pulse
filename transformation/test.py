placed_timestamp = to_timestamp(col("order_placed_at"))
shipped_timestamp = to_timestamp(col("order_shipped_at"))
delivered_timestamp = to_timestamp(col("order_delivered_at"))

order_metrics = (
    dataframes["order_items"]
    .groupBy("order_id")
    .agg(
        spark_sum("product_cost").alias("total_product_cost"),
        spark_sum("quantity").alias("total_quantity"),
        spark_avg("product_cost").alias("avg_product_cost"),
        spark_max("discount_amount").alias("max_item_discount"),
        countDistinct("product_id").alias("unique_products_ordered"),
    )
)

dataframes["orders"] = (
    dataframes["orders"]
    .join(
        order_metrics,
        "order_id",
        "inner",
    )
    .withColumns(
        {
            "order_placed_year": when(
                col("order_placed_at").isNotNull(), year(col("order_placed_at"))
            ),
            "order_placed_month": when(
                col("order_placed_at").isNotNull(), month(col("order_placed_at"))
            ),
            "order_placed_quarter": when(
                col("order_placed_at").isNotNull(), quarter(col("order_placed_at"))
            ),
            "order_placed_day_of_week": when(
                col("order_placed_at").isNotNull(), dayofweek(col("order_placed_at"))
            ),
            "order_placed_week_of_year": when(
                col("order_placed_at").isNotNull(), weekofyear(col("order_placed_at"))
            ),
            "order_placed_day_of_month": when(
                col("order_placed_at").isNotNull(), dayofmonth(col("order_placed_at"))
            ),
            "order_shipped_year": when(
                col("order_shipped_at").isNotNull(), year(col("order_shipped_at"))
            ),
            "order_shipped_month": when(
                col("order_shipped_at").isNotNull(), month(col("order_shipped_at"))
            ),
            "order_shipped_quarter": when(
                col("order_shipped_at").isNotNull(), quarter(col("order_shipped_at"))
            ),
            "order_shipped_day_of_week": when(
                col("order_shipped_at").isNotNull(), dayofweek(col("order_shipped_at"))
            ),
            "order_shipped_week_of_year": when(
                col("order_shipped_at").isNotNull(), weekofyear(col("order_shipped_at"))
            ),
            "order_shipped_day_of_month": when(
                col("order_shipped_at").isNotNull(), dayofmonth(col("order_shipped_at"))
            ),
            "order_delivered_year": when(
                col("order_delivered_at").isNotNull(), year(col("order_delivered_at"))
            ),
            "order_delivered_month": when(
                col("order_delivered_at").isNotNull(), month(col("order_delivered_at"))
            ),
            "order_delivered_quarter": when(
                col("order_delivered_at").isNotNull(),
                quarter(col("order_delivered_at")),
            ),
            "order_delivered_day_of_week": when(
                col("order_delivered_at").isNotNull(),
                dayofweek(col("order_delivered_at")),
            ),
            "order_delivered_week_of_year": when(
                col("order_delivered_at").isNotNull(),
                weekofyear(col("order_delivered_at")),
            ),
            "order_delivered_day_of_month": when(
                col("order_delivered_at").isNotNull(),
                dayofmonth(col("order_delivered_at")),
            ),
            "order_processing_seconds_diff": when(
                placed_timestamp.isNotNull() & shipped_timestamp.isNotNull(),
                greatest(
                    unix_timestamp(shipped_timestamp)
                    - unix_timestamp(placed_timestamp),
                    lit(0),
                ),
            ),
            "order_processing_minutes_diff": when(
                placed_timestamp.isNotNull() & shipped_timestamp.isNotNull(),
                greatest(
                    floor(
                        (
                            unix_timestamp(shipped_timestamp)
                            - unix_timestamp(placed_timestamp)
                        )
                        / 60
                    ),
                    lit(0),
                ),
            ),
            "order_processing_hours_diff": when(
                placed_timestamp.isNotNull() & shipped_timestamp.isNotNull(),
                greatest(
                    floor(
                        (
                            unix_timestamp(shipped_timestamp)
                            - unix_timestamp(placed_timestamp)
                        )
                        / 3600
                    ),
                    lit(0),
                ),
            ),
            "order_processing_days_diff": when(
                placed_timestamp.isNotNull() & shipped_timestamp.isNotNull(),
                greatest(datediff(shipped_timestamp, placed_timestamp), lit(0)),
            ),
            "order_processing_weeks_diff": when(
                placed_timestamp.isNotNull() & shipped_timestamp.isNotNull(),
                greatest(datediff(shipped_timestamp, placed_timestamp) / 7, lit(0)),
            ),
            "order_processing_months_diff": when(
                placed_timestamp.isNotNull() & shipped_timestamp.isNotNull(),
                greatest(datediff(shipped_timestamp, placed_timestamp) / 30, lit(0)),
            ),
            "order_processing_years_diff": when(
                placed_timestamp.isNotNull() & shipped_timestamp.isNotNull(),
                greatest(datediff(shipped_timestamp, placed_timestamp) / 365, lit(0)),
            ),
            "delivery_seconds_diff": when(
                shipped_timestamp.isNotNull() & delivered_timestamp.isNotNull(),
                greatest(
                    unix_timestamp(delivered_timestamp)
                    - unix_timestamp(shipped_timestamp),
                    lit(0),
                ),
            ),
            "delivery_minutes_diff": when(
                shipped_timestamp.isNotNull() & delivered_timestamp.isNotNull(),
                greatest(
                    floor(
                        (
                            unix_timestamp(delivered_timestamp)
                            - unix_timestamp(shipped_timestamp)
                        )
                        / 60
                    ),
                    lit(0),
                ),
            ),
            "delivery_hours_diff": when(
                shipped_timestamp.isNotNull() & delivered_timestamp.isNotNull(),
                greatest(
                    floor(
                        (
                            unix_timestamp(delivered_timestamp)
                            - unix_timestamp(shipped_timestamp)
                        )
                        / 3600
                    ),
                    lit(0),
                ),
            ),
            "delivery_days_diff": when(
                shipped_timestamp.isNotNull() & delivered_timestamp.isNotNull(),
                greatest(datediff(delivered_timestamp, shipped_timestamp), lit(0)),
            ),
            "delivery_weeks_diff": when(
                shipped_timestamp.isNotNull() & delivered_timestamp.isNotNull(),
                greatest(datediff(delivered_timestamp, shipped_timestamp) / 7, lit(0)),
            ),
            "delivery_months_diff": when(
                shipped_timestamp.isNotNull() & delivered_timestamp.isNotNull(),
                greatest(datediff(delivered_timestamp, shipped_timestamp) / 30, lit(0)),
            ),
            "delivery_years_diff": when(
                shipped_timestamp.isNotNull() & delivered_timestamp.isNotNull(),
                greatest(
                    datediff(delivered_timestamp, shipped_timestamp) / 365, lit(0)
                ),
            ),
            "total_order_fulfillment_time_seconds": when(
                col("order_processing_seconds_diff").isNotNull()
                & col("delivery_seconds_diff").isNotNull(),
                col("order_processing_seconds_diff") + col("delivery_seconds_diff"),
            ),
            "total_order_fulfillment_time_minutes": when(
                col("order_processing_minutes_diff").isNotNull()
                & col("delivery_minutes_diff").isNotNull(),
                col("order_processing_minutes_diff") + col("delivery_minutes_diff"),
            ),
            "total_order_fulfillment_time_hours": when(
                col("order_processing_hours_diff").isNotNull()
                & col("delivery_hours_diff").isNotNull(),
                col("order_processing_hours_diff") + col("delivery_hours_diff"),
            ),
            "total_order_fulfillment_time_days": when(
                col("order_processing_days_diff").isNotNull()
                & col("delivery_days_diff").isNotNull(),
                col("order_processing_days_diff") + col("delivery_days_diff"),
            ),
            "total_order_fulfillment_time_weeks": when(
                col("order_processing_weeks_diff").isNotNull()
                & col("delivery_weeks_diff").isNotNull(),
                col("order_processing_weeks_diff") + col("delivery_weeks_diff"),
            ),
            "total_order_fulfillment_time_months": when(
                col("order_processing_months_diff").isNotNull()
                & col("delivery_months_diff").isNotNull(),
                col("order_processing_months_diff") + col("delivery_months_diff"),
            ),
            "total_order_fulfillment_time_years": when(
                col("order_processing_years_diff").isNotNull()
                & col("delivery_years_diff").isNotNull(),
                col("order_processing_years_diff") + col("delivery_years_diff"),
            ),
            "order_profit": when(
                col("subtotal").isNotNull()
                & col("total_product_cost").isNotNull()
                & col("total_quantity").isNotNull()
                & (col("subtotal") != 0)
                & (col("total_quantity") != 0)
                & (col("total_product_cost") != 0),
                col("subtotal") - (col("total_product_cost") * col("total_quantity")),
            ),
            "net_revenue": when(
                col("total_amount").isNotNull()
                & col("total_discount").isNotNull()
                & col("shipping_cost").isNotNull()
                & (col("total_amount") != 0)
                & (col("shipping_cost") != 0)
                & (col("total_discount") != 0),
                col("total_amount") - col("total_discount") - col("shipping_cost"),
            ),
            "net_profit": when(
                col("order_profit").isNotNull()
                & col("shipping_cost").isNotNull()
                & (col("order_profit") != 0)
                & (col("shipping_cost") != 0),
                col("order_profit") - col("shipping_cost"),
            ),
            "discount_percentage": when(
                col("subtotal").isNotNull()
                & col("total_discount").isNotNull()
                & (col("subtotal") != 0)
                & (col("total_discount") != 0),
                (col("total_discount") / col("subtotal")) * 100,
            ),
            "average_item_value": when(
                col("subtotal").isNotNull()
                & col("total_quantity").isNotNull()
                & (col("subtotal") != 0)
                & (col("total_quantity") != 0),
                col("subtotal") / col("total_quantity"),
            ),
            "cost_per_item": when(
                col("total_product_cost").isNotNull()
                & col("total_quantity").isNotNull()
                & (col("total_product_cost") != 0)
                & (col("total_quantity") != 0),
                col("total_product_cost") / col("total_quantity"),
            ),
            "order_size_category": when(
                col("total_quantity").isNotNull() & (col("total_quantity") != 0),
                when(col("total_quantity") < 3, "Small")
                .when(
                    (col("total_quantity") >= 3) & (col("total_quantity") < 7), "Medium"
                )
                .otherwise("Large"),
            ),
            "season": when(
                col("order_placed_month").isNotNull(),
                when(col("order_placed_month").isin(12, 1, 2), "Winter")
                .when(col("order_placed_month").isin(3, 4, 5), "Spring")
                .when(col("order_placed_month").isin(6, 7, 8), "Summer")
                .otherwise("Fall"),
            ),
        }
    )
)
