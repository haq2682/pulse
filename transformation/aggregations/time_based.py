from pyspark.sql.window import Window

from pyspark.sql.functions import (
    col,
    to_date,
    year,
    month,
    weekofyear,
    dayofweek,
    expr,
    when,
    lit,
    countDistinct,
    count,
    coalesce,
    lag,
    sum as spark_sum,
    avg as spark_avg,
    min as spark_min,
)


def time_based_aggregations(dataframes):
    orders_with_time = (
        dataframes["orders"]
        .filter(col("order_placed_at").isNotNull())
        .withColumn("order_date", to_date(col("order_placed_at")))
        .withColumn("order_year", year(col("order_placed_at")))
        .withColumn("order_month", month(col("order_placed_at")))
        .withColumn("order_week", weekofyear(col("order_placed_at")))
        .withColumn("order_day_of_week", dayofweek(col("order_placed_at")))
        .withColumn(
            "year_month", expr("concat(order_year, '-', lpad(order_month, 2, '0'))")
        )
        .withColumn(
            "year_week", expr("concat(order_year, '-W', lpad(order_week, 2, '0'))")
        )
    )
    orders_with_items_time = orders_with_time.join(
        dataframes["order_items"].select("order_id", "quantity"), "order_id", "left"
    )
    customer_first_order = (
        dataframes["orders"]
        .filter(col("order_placed_at").isNotNull())
        .groupBy("customer_id")
        .agg(spark_min("order_placed_at").alias("first_order_date"))
    )
    orders_with_customer_type = (
        orders_with_time.join(customer_first_order, "customer_id", "left")
        .withColumn(
            "is_new_customer",
            when(
                col("first_order_date").isNotNull()
                & (to_date(col("order_placed_at")) == to_date(col("first_order_date"))),
                lit(1),
            ).otherwise(lit(0)),
        )
        .withColumn(
            "is_returning_customer",
            when(col("is_new_customer") == 0, lit(1)).otherwise(lit(0)),
        )
    )
    daily_agg = orders_with_customer_type.groupBy(
        "order_date", "order_year", "order_month"
    ).agg(
        # Total revenue
        spark_sum(
            when(
                col("total_amount").isNotNull() & (col("total_amount") > 0),
                col("total_amount"),
            )
        ).alias("total_revenue"),
        # Total orders
        countDistinct("order_id").alias("total_orders"),
        # Total customers
        countDistinct("customer_id").alias("total_customers"),
        # New customers
        spark_sum("is_new_customer").alias("new_customers"),
        # Returning customers
        spark_sum("is_returning_customer").alias("returning_customers"),
        # Average order value
        spark_avg(
            when(
                col("total_amount").isNotNull() & (col("total_amount") > 0),
                col("total_amount"),
            )
        ).alias("avg_order_value"),
    )
    daily_units = orders_with_items_time.groupBy("order_date").agg(
        spark_sum(
            when(col("quantity").isNotNull() & (col("quantity") > 0), col("quantity"))
        ).alias("total_units_sold")
    )
    daily_agg = daily_agg.join(daily_units, "order_date", "left")
    daily_sessions = (
        dataframes["customer_sessions"]
        .filter(col("session_start").isNotNull())
        .withColumn("session_date", to_date(col("session_start")))
        .groupBy("session_date")
        .agg(
            count("session_id").alias("total_sessions"),
            spark_sum(
                when(col("conversion_flag") == 1, lit(1)).otherwise(lit(0))
            ).alias("total_conversions"),
        )
    )
    daily_agg = daily_agg.join(
        daily_sessions, daily_agg.order_date == daily_sessions.session_date, "left"
    ).drop("session_date")
    window_daily = Window.orderBy("order_date")
    daily_agg = daily_agg.withColumns(
        {
            # Session to order rate
            "session_to_order_rate": when(
                col("total_sessions").isNotNull() & (col("total_sessions") > 0),
                (col("total_orders") / col("total_sessions")) * 100,
            ),
            # Previous day metrics
            "prev_day_revenue": lag("total_revenue", 1).over(window_daily),
            "prev_day_customers": lag("total_customers", 1).over(window_daily),
            # Daily growth rate
            "revenue_growth_rate": when(
                col("prev_day_revenue").isNotNull() & (col("prev_day_revenue") > 0),
                (
                    (col("total_revenue") - col("prev_day_revenue"))
                    / col("prev_day_revenue")
                )
                * 100,
            ),
            # Customer retention rate
            "customer_retention_rate": when(
                col("prev_day_customers").isNotNull() & (col("prev_day_customers") > 0),
                (col("returning_customers") / col("prev_day_customers")) * 100,
            ),
            # Handle nulls
            "total_revenue": coalesce(col("total_revenue"), lit(0.0)),
            "total_orders": coalesce(col("total_orders"), lit(0)),
            "total_customers": coalesce(col("total_customers"), lit(0)),
            "new_customers": coalesce(col("new_customers"), lit(0)),
            "returning_customers": coalesce(col("returning_customers"), lit(0)),
            "total_units_sold": coalesce(col("total_units_sold"), lit(0)),
            "total_sessions": coalesce(col("total_sessions"), lit(0)),
        }
    )

    dataframes["daily_aggregations"] = daily_agg
    weekly_agg = orders_with_customer_type.groupBy(
        "year_week", "order_year", "order_week"
    ).agg(
        spark_sum(
            when(
                col("total_amount").isNotNull() & (col("total_amount") > 0),
                col("total_amount"),
            )
        ).alias("total_revenue"),
        countDistinct("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("total_customers"),
        spark_sum("is_new_customer").alias("new_customers"),
        spark_sum("is_returning_customer").alias("returning_customers"),
        spark_avg(
            when(
                col("total_amount").isNotNull() & (col("total_amount") > 0),
                col("total_amount"),
            )
        ).alias("avg_order_value"),
    )
    weekly_units = orders_with_items_time.groupBy("year_week").agg(
        spark_sum(
            when(col("quantity").isNotNull() & (col("quantity") > 0), col("quantity"))
        ).alias("total_units_sold")
    )
    weekly_agg = weekly_agg.join(weekly_units, "year_week", "left")
    weekly_sessions = (
        dataframes["customer_sessions"]
        .filter(col("session_start").isNotNull())
        .withColumn("session_year", year(col("session_start")))
        .withColumn("session_week", weekofyear(col("session_start")))
        .withColumn(
            "session_year_week",
            expr("concat(session_year, '-W', lpad(session_week, 2, '0'))"),
        )
        .groupBy("session_year_week")
        .agg(
            count("session_id").alias("total_sessions"),
            spark_sum(
                when(col("conversion_flag") == 1, lit(1)).otherwise(lit(0))
            ).alias("total_conversions"),
        )
    )
    weekly_agg = weekly_agg.join(
        weekly_sessions,
        weekly_agg.year_week == weekly_sessions.session_year_week,
        "left",
    ).drop("session_year_week")
    window_weekly = Window.orderBy("order_year", "order_week")
    weekly_agg = weekly_agg.withColumns(
        {
            "session_to_order_rate": when(
                col("total_sessions").isNotNull() & (col("total_sessions") > 0),
                (col("total_orders") / col("total_sessions")) * 100,
            ),
            "prev_week_revenue": lag("total_revenue", 1).over(window_weekly),
            "prev_week_customers": lag("total_customers", 1).over(window_weekly),
            "revenue_growth_rate": when(
                col("prev_week_revenue").isNotNull() & (col("prev_week_revenue") > 0),
                (
                    (col("total_revenue") - col("prev_week_revenue"))
                    / col("prev_week_revenue")
                )
                * 100,
            ),
            "customer_retention_rate": when(
                col("prev_week_customers").isNotNull()
                & (col("prev_week_customers") > 0),
                (col("returning_customers") / col("prev_week_customers")) * 100,
            ),
            "total_revenue": coalesce(col("total_revenue"), lit(0.0)),
            "total_orders": coalesce(col("total_orders"), lit(0)),
            "total_customers": coalesce(col("total_customers"), lit(0)),
            "new_customers": coalesce(col("new_customers"), lit(0)),
            "returning_customers": coalesce(col("returning_customers"), lit(0)),
            "total_units_sold": coalesce(col("total_units_sold"), lit(0)),
            "total_sessions": coalesce(col("total_sessions"), lit(0)),
        }
    )

    dataframes["weekly_aggregations"] = weekly_agg

    monthly_agg = orders_with_customer_type.groupBy(
        "year_month", "order_year", "order_month"
    ).agg(
        spark_sum(
            when(
                col("total_amount").isNotNull() & (col("total_amount") > 0),
                col("total_amount"),
            )
        ).alias("total_revenue"),
        countDistinct("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("total_customers"),
        spark_sum("is_new_customer").alias("new_customers"),
        spark_sum("is_returning_customer").alias("returning_customers"),
        spark_avg(
            when(
                col("total_amount").isNotNull() & (col("total_amount") > 0),
                col("total_amount"),
            )
        ).alias("avg_order_value"),
    )
    monthly_units = orders_with_items_time.groupBy("year_month").agg(
        spark_sum(
            when(col("quantity").isNotNull() & (col("quantity") > 0), col("quantity"))
        ).alias("total_units_sold")
    )
    monthly_agg = monthly_agg.join(monthly_units, "year_month", "left")
    monthly_sessions = (
        dataframes["customer_sessions"]
        .filter(col("session_start").isNotNull())
        .withColumn("session_year", year(col("session_start")))
        .withColumn("session_month", month(col("session_start")))
        .withColumn(
            "session_year_month",
            expr("concat(session_year, '-', lpad(session_month, 2, '0'))"),
        )
        .groupBy("session_year_month")
        .agg(
            count("session_id").alias("total_sessions"),
            spark_sum(
                when(col("conversion_flag") == 1, lit(1)).otherwise(lit(0))
            ).alias("total_conversions"),
        )
    )
    monthly_agg = monthly_agg.join(
        monthly_sessions,
        monthly_agg.year_month == monthly_sessions.session_year_month,
        "left",
    ).drop("session_year_month")
    window_monthly = Window.orderBy("order_year", "order_month")
    monthly_agg = monthly_agg.withColumns(
        {
            "session_to_order_rate": when(
                col("total_sessions").isNotNull() & (col("total_sessions") > 0),
                (col("total_orders") / col("total_sessions")) * 100,
            ),
            "prev_month_revenue": lag("total_revenue", 1).over(window_monthly),
            "prev_month_customers": lag("total_customers", 1).over(window_monthly),
            "revenue_growth_rate": when(
                col("prev_month_revenue").isNotNull() & (col("prev_month_revenue") > 0),
                (
                    (col("total_revenue") - col("prev_month_revenue"))
                    / col("prev_month_revenue")
                )
                * 100,
            ),
            "customer_retention_rate": when(
                col("prev_month_customers").isNotNull()
                & (col("prev_month_customers") > 0),
                (col("returning_customers") / col("prev_month_customers")) * 100,
            ),
            "churn_rate": when(
                col("prev_month_customers").isNotNull()
                & col("total_customers").isNotNull()
                & (col("prev_month_customers") > 0),
                (
                    (col("prev_month_customers") - col("returning_customers"))
                    / col("prev_month_customers")
                )
                * 100,
            ),
            "total_revenue": coalesce(col("total_revenue"), lit(0.0)),
            "total_orders": coalesce(col("total_orders"), lit(0)),
            "total_customers": coalesce(col("total_customers"), lit(0)),
            "new_customers": coalesce(col("new_customers"), lit(0)),
            "returning_customers": coalesce(col("returning_customers"), lit(0)),
            "total_units_sold": coalesce(col("total_units_sold"), lit(0)),
            "total_sessions": coalesce(col("total_sessions"), lit(0)),
        }
    )

    dataframes["monthly_aggregations"] = monthly_agg
