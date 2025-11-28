from pyspark.sql.window import Window

from pyspark.sql.functions import (
    col,
    countDistinct,
    sum as spark_sum,
    avg as spark_avg,
    when,
    coalesce,
    lit,
    row_number,
    broadcast,
)

def geographic_aggregations(dataframes):
    orders_with_geography = dataframes["orders"].join(
        dataframes["customers"].select(
            "customer_id", "city", "state_province", "country"
        ),
        "customer_id",
        "left",
    )
    orders_with_category = orders_with_geography.join(
        dataframes["order_items"].select("order_id", "product_id"), "order_id", "left"
    ).join(
        dataframes["products"].select("product_id", "category"), "product_id", "left"
    )
    country_agg = (
        orders_with_geography.filter(col("country").isNotNull())
        .groupBy("country")
        .agg(
            # Total customers
            countDistinct("customer_id").alias("total_customers"),
            # Total orders
            countDistinct("order_id").alias("total_orders"),
            # Total revenue
            spark_sum(
                when(
                    col("total_amount").isNotNull() & (col("total_amount") > 0),
                    col("total_amount"),
                )
            ).alias("total_revenue"),
            # Average order value
            spark_avg(
                when(
                    col("total_amount").isNotNull() & (col("total_amount") > 0),
                    col("total_amount"),
                )
            ).alias("avg_order_value"),
        )
    )
    country_clv = (
        dataframes["customers"]
        .filter(col("country").isNotNull())
        .groupBy("country")
        .agg(
            spark_avg(
                when(
                    col("customer_lifetime_value").isNotNull(),
                    col("customer_lifetime_value"),
                )
            ).alias("avg_customer_lifetime_value")
        )
    )
    country_agg = country_agg.join(country_clv, "country", "left")
    country_suppliers = (
        dataframes["suppliers"]
        .filter(col("country").isNotNull())
        .groupBy("country")
        .agg(countDistinct("supplier_id").alias("total_suppliers"))
    )
    country_agg = country_agg.join(country_suppliers, "country", "left")
    country_categories = (
        orders_with_category.filter(
            col("country").isNotNull() & col("category").isNotNull()
        )
        .groupBy("country", "category")
        .agg(
            spark_sum(
                when(
                    col("total_amount").isNotNull() & (col("total_amount") > 0),
                    col("total_amount"),
                )
            ).alias("category_revenue")
        )
    )
    window_country_category = Window.partitionBy("country").orderBy(
        col("category_revenue").desc()
    )
    country_top_category = (
        country_categories.withColumn(
            "rank", row_number().over(window_country_category)
        )
        .filter(col("rank") == 1)
        .select("country", col("category").alias("preferred_category"))
    )
    country_agg = country_agg.join(country_top_category, "country", "left")
    country_agg = country_agg.withColumns(
        {
            "revenue_per_customer": when(
                col("total_revenue").isNotNull()
                & col("total_customers").isNotNull()
                & (col("total_customers") > 0),
                col("total_revenue") / col("total_customers"),
            ),
            "orders_per_customer": when(
                col("total_orders").isNotNull()
                & col("total_customers").isNotNull()
                & (col("total_customers") > 0),
                col("total_orders") / col("total_customers"),
            ),
            "total_customers": coalesce(col("total_customers"), lit(0)),
            "total_orders": coalesce(col("total_orders"), lit(0)),
            "total_revenue": coalesce(col("total_revenue"), lit(0.0)),
            "total_suppliers": coalesce(col("total_suppliers"), lit(0)),
        }
    )
    dataframes["country_aggregations"] = country_agg
    state_agg = (
        orders_with_geography.filter(
            col("state_province").isNotNull() & col("country").isNotNull()
        )
        .groupBy("country", "state_province")
        .agg(
            countDistinct("customer_id").alias("total_customers"),
            countDistinct("order_id").alias("total_orders"),
            spark_sum(
                when(
                    col("total_amount").isNotNull() & (col("total_amount") > 0),
                    col("total_amount"),
                )
            ).alias("total_revenue"),
            spark_avg(
                when(
                    col("total_amount").isNotNull() & (col("total_amount") > 0),
                    col("total_amount"),
                )
            ).alias("avg_order_value"),
        )
    )
    state_clv = (
        dataframes["customers"]
        .filter(col("state_province").isNotNull() & col("country").isNotNull())
        .groupBy("country", "state_province")
        .agg(
            spark_avg(
                when(
                    col("customer_lifetime_value").isNotNull(),
                    col("customer_lifetime_value"),
                )
            ).alias("avg_customer_lifetime_value")
        )
    )
    state_agg = state_agg.join(state_clv, ["country", "state_province"], "left")
    state_suppliers = (
        dataframes["suppliers"]
        .filter(col("state").isNotNull() & col("country").isNotNull())
        .groupBy(col("country"), col("state").alias("state_province"))
        .agg(countDistinct("supplier_id").alias("total_suppliers"))
    )

    state_agg = state_agg.join(state_suppliers, ["country", "state_province"], "left")
    state_categories = (
        orders_with_category.filter(
            col("state_province").isNotNull()
            & col("country").isNotNull()
            & col("category").isNotNull()
        )
        .groupBy("country", "state_province", "category")
        .agg(
            spark_sum(
                when(
                    col("total_amount").isNotNull() & (col("total_amount") > 0),
                    col("total_amount"),
                )
            ).alias("category_revenue")
        )
    )
    window_state_category = Window.partitionBy("country", "state_province").orderBy(
        col("category_revenue").desc()
    )
    state_top_category = (
        state_categories.withColumn("rank", row_number().over(window_state_category))
        .filter(col("rank") == 1)
        .select(
            "country", "state_province", col("category").alias("preferred_category")
        )
    )

    state_agg = state_agg.join(
        state_top_category, ["country", "state_province"], "left"
    )
    state_agg = state_agg.withColumns(
        {
            "revenue_per_customer": when(
                col("total_revenue").isNotNull()
                & col("total_customers").isNotNull()
                & (col("total_customers") > 0),
                col("total_revenue") / col("total_customers"),
            ),
            "orders_per_customer": when(
                col("total_orders").isNotNull()
                & col("total_customers").isNotNull()
                & (col("total_customers") > 0),
                col("total_orders") / col("total_customers"),
            ),
            "total_customers": coalesce(col("total_customers"), lit(0)),
            "total_orders": coalesce(col("total_orders"), lit(0)),
            "total_revenue": coalesce(col("total_revenue"), lit(0.0)),
            "total_suppliers": coalesce(col("total_suppliers"), lit(0)),
        }
    )
    dataframes["state_aggregations"] = state_agg
    city_agg = (
        orders_with_geography.filter(
            col("city").isNotNull()
            & col("state_province").isNotNull()
            & col("country").isNotNull()
        )
        .groupBy("country", "state_province", "city")
        .agg(
            countDistinct("customer_id").alias("total_customers"),
            countDistinct("order_id").alias("total_orders"),
            spark_sum(
                when(
                    col("total_amount").isNotNull() & (col("total_amount") > 0),
                    col("total_amount"),
                )
            ).alias("total_revenue"),
            spark_avg(
                when(
                    col("total_amount").isNotNull() & (col("total_amount") > 0),
                    col("total_amount"),
                )
            ).alias("avg_order_value"),
        )
    )
    city_clv = (
        dataframes["customers"]
        .filter(
            col("city").isNotNull()
            & col("state_province").isNotNull()
            & col("country").isNotNull()
        )
        .groupBy("country", "state_province", "city")
        .agg(
            spark_avg(
                when(
                    col("customer_lifetime_value").isNotNull(),
                    col("customer_lifetime_value"),
                )
            ).alias("avg_customer_lifetime_value")
        )
    )
    city_agg = city_agg.join(city_clv, ["country", "state_province", "city"], "left")
    city_suppliers = (
        dataframes["suppliers"]
        .filter(
            col("city").isNotNull()
            & col("state").isNotNull()
            & col("country").isNotNull()
        )
        .groupBy(col("country"), col("state").alias("state_province"), col("city"))
        .agg(countDistinct("supplier_id").alias("total_suppliers"))
    )
    city_agg = city_agg.join(
        city_suppliers, ["country", "state_province", "city"], "left"
    )
    city_categories = (
        orders_with_category.filter(
            col("city").isNotNull()
            & col("state_province").isNotNull()
            & col("country").isNotNull()
            & col("category").isNotNull()
        )
        .groupBy("country", "state_province", "city", "category")
        .agg(
            spark_sum(
                when(
                    col("total_amount").isNotNull() & (col("total_amount") > 0),
                    col("total_amount"),
                )
            ).alias("category_revenue")
        )
    )
    city_categories.persist()

    window_city_category = Window.partitionBy(
        "country", "state_province", "city"
    ).orderBy(col("category_revenue").desc())

    # Use broadcast hint for smaller dataframe and repartition for better parallelism
    city_top_category = (
        city_categories.withColumn("rank", row_number().over(window_city_category))
        .filter(col("rank") == 1)
        .select(
            "country",
            "state_province",
            "city",
            col("category").alias("preferred_category"),
        )
        .repartition(200)  # Adjust based on your cluster size
    )

    # Perform the join with broadcast hint if city_top_category is small
    city_agg = city_agg.join(
        broadcast(city_top_category), ["country", "state_province", "city"], "left"
    )

    # Unpersist after use to free memory
    city_categories.unpersist()
    city_agg = city_agg.withColumns(
        {
            "revenue_per_customer": when(
                col("total_revenue").isNotNull()
                & col("total_customers").isNotNull()
                & (col("total_customers") > 0),
                col("total_revenue") / col("total_customers"),
            ),
            "orders_per_customer": when(
                col("total_orders").isNotNull()
                & col("total_customers").isNotNull()
                & (col("total_customers") > 0),
                col("total_orders") / col("total_customers"),
            ),
            "customer_density": col(
                "total_customers"
            ),  # Density = customers in that city
            "total_customers": coalesce(col("total_customers"), lit(0)),
            "total_orders": coalesce(col("total_orders"), lit(0)),
            "total_revenue": coalesce(col("total_revenue"), lit(0.0)),
            "total_suppliers": coalesce(col("total_suppliers"), lit(0)),
        }
    )

    dataframes["city_aggregations"] = city_agg
