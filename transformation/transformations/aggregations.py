from pyspark.sql.functions import *
from pyspark.sql.window import Window


def create_time_aggregations(dataframes):
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

    customer_first_order = (
        dataframes["orders"]
        .filter(col("order_placed_at").isNotNull())
        .groupBy("customer_id")
        .agg(min("order_placed_at").alias("first_order_date"))
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
        sum(
            when(
                col("total_amount").isNotNull() & (col("total_amount") > 0),
                col("total_amount"),
            )
        ).alias("total_revenue"),
        countDistinct("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("total_customers"),
        sum("is_new_customer").alias("new_customers"),
        sum("is_returning_customer").alias("returning_customers"),
        avg(
            when(
                col("total_amount").isNotNull() & (col("total_amount") > 0),
                col("total_amount"),
            )
        ).alias("avg_order_value"),
    )
    dataframes["daily_aggregations"] = daily_agg

    weekly_agg = orders_with_customer_type.groupBy(
        "year_week", "order_year", "order_week"
    ).agg(
        sum(
            when(
                col("total_amount").isNotNull() & (col("total_amount") > 0),
                col("total_amount"),
            )
        ).alias("total_revenue"),
        countDistinct("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("total_customers"),
        sum("is_new_customer").alias("new_customers"),
        sum("is_returning_customer").alias("returning_customers"),
        avg(
            when(
                col("total_amount").isNotNull() & (col("total_amount") > 0),
                col("total_amount"),
            )
        ).alias("avg_order_value"),
    )
    dataframes["weekly_aggregations"] = weekly_agg

    monthly_agg = orders_with_customer_type.groupBy(
        "year_month", "order_year", "order_month"
    ).agg(
        sum(
            when(
                col("total_amount").isNotNull() & (col("total_amount") > 0),
                col("total_amount"),
            )
        ).alias("total_revenue"),
        countDistinct("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("total_customers"),
        sum("is_new_customer").alias("new_customers"),
        sum("is_returning_customer").alias("returning_customers"),
        avg(
            when(
                col("total_amount").isNotNull() & (col("total_amount") > 0),
                col("total_amount"),
            )
        ).alias("avg_order_value"),
    )
    dataframes["monthly_aggregations"] = monthly_agg


def create_geography_aggregations(dataframes):
    orders_with_geography = dataframes["orders"].join(
        dataframes["customers"].select(
            "customer_id", "city", "state_province", "country"
        ),
        "customer_id",
        "left",
    )

    country_agg = (
        orders_with_geography.filter(col("country").isNotNull())
        .groupBy("country")
        .agg(
            countDistinct("customer_id").alias("total_customers"),
            countDistinct("order_id").alias("total_orders"),
            sum(
                when(
                    col("total_amount").isNotNull() & (col("total_amount") > 0),
                    col("total_amount"),
                )
            ).alias("total_revenue"),
            avg(
                when(
                    col("total_amount").isNotNull() & (col("total_amount") > 0),
                    col("total_amount"),
                )
            ).alias("avg_order_value"),
        )
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
            sum(
                when(
                    col("total_amount").isNotNull() & (col("total_amount") > 0),
                    col("total_amount"),
                )
            ).alias("total_revenue"),
            avg(
                when(
                    col("total_amount").isNotNull() & (col("total_amount") > 0),
                    col("total_amount"),
                )
            ).alias("avg_order_value"),
        )
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
            sum(
                when(
                    col("total_amount").isNotNull() & (col("total_amount") > 0),
                    col("total_amount"),
                )
            ).alias("total_revenue"),
            avg(
                when(
                    col("total_amount").isNotNull() & (col("total_amount") > 0),
                    col("total_amount"),
                )
            ).alias("avg_order_value"),
        )
    )
    dataframes["city_aggregations"] = city_agg


def create_categories(dataframes):
    order_items_with_category = (
        dataframes["order_items"]
        .join(
            dataframes["products"].select("product_id", "category", "sell_price"),
            "product_id",
            "left",
        )
        .join(
            dataframes["orders"].select("order_id", "customer_id", "order_placed_at"),
            "order_id",
            "inner",
        )
        .select(
            "order_item_id",
            "order_id",
            "product_id",
            "category",
            "quantity",
            "product_cost",
            "sell_price",
            "customer_id",
            "order_placed_at",
        )
    )

    category_agg = (
        order_items_with_category.filter(col("category").isNotNull())
        .groupBy("category")
        .agg(
            countDistinct(when(col("product_id").isNotNull(), col("product_id"))).alias(
                "total_products_in_category"
            ),
            sum(
                when(
                    col("quantity").isNotNull()
                    & col("sell_price").isNotNull()
                    & (col("quantity") > 0)
                    & (col("sell_price") > 0),
                    col("quantity") * col("sell_price"),
                )
            ).alias("total_revenue"),
            sum(
                when(
                    col("quantity").isNotNull() & (col("quantity") > 0), col("quantity")
                )
            ).alias("total_units_sold"),
            countDistinct(when(col("order_id").isNotNull(), col("order_id"))).alias(
                "total_orders"
            ),
            countDistinct(
                when(col("customer_id").isNotNull(), col("customer_id"))
            ).alias("unique_customers"),
        )
    )

    categories_df = (
        dataframes["products"]
        .select("category")
        .filter(col("category").isNotNull())
        .distinct()
        .join(category_agg, "category", "left")
    )
    dataframes["categories"] = categories_df


def create_cart_analysis(dataframes):
    cart_with_products = (
        dataframes["shopping_cart"]
        .join(
            dataframes["products"].select("product_id", "category"),
            "product_id",
            "left",
        )
        .join(
            dataframes["customer_sessions"].select(
                "session_id", "device_type", "conversion_flag"
            ),
            "session_id",
            "left",
        )
    )

    cart_agg = (
        cart_with_products.filter(col("cart_id").isNotNull())
        .groupBy("cart_id")
        .agg(
            sum(
                when(
                    col("unit_price").isNotNull()
                    & col("quantity").isNotNull()
                    & (col("unit_price") > 0)
                    & (col("quantity") > 0),
                    col("unit_price") * col("quantity"),
                )
            ).alias("cart_total_value"),
            count("product_id").alias("cart_items_count"),
            avg(
                when(
                    col("unit_price").isNotNull() & (col("unit_price") > 0),
                    col("unit_price"),
                )
            ).alias("cart_avg_item_price"),
        )
    )

    cart_full = (
        dataframes["shopping_cart"]
        .select("cart_id", "cart_status", "added_date", "customer_id")
        .groupBy("cart_id")
        .agg(
            expr("first(cart_status)").alias("cart_status"),
            min("added_date").alias("cart_added_date"),
            expr("first(customer_id)").alias("customer_id"),
        )
        .join(cart_agg, "cart_id", "left")
    )
    dataframes["cart_abandonment_analysis"] = cart_full


def create_rfm_segmentation(dataframes):
    rfm_metrics = (
        dataframes["orders"]
        .filter(col("customer_id").isNotNull() & col("order_placed_at").isNotNull())
        .groupBy("customer_id")
        .agg(
            datediff(lit("2025-11-14"), max("order_placed_at")).alias(
                "days_since_last_order"
            ),
            countDistinct("order_id").alias("total_orders_rfm"),
            sum(
                when(
                    col("total_amount").isNotNull() & (col("total_amount") > 0),
                    col("total_amount"),
                )
            ).alias("total_revenue_rfm"),
        )
    )

    rfm_scored = rfm_metrics.withColumns(
        {"recency_score": lit(3), "frequency_score": lit(3), "monetary_score": lit(3)}
    )
    rfm_scored = rfm_scored.withColumn(
        "rfm_segment", expr("concat(recency_score, frequency_score, monetary_score)")
    )
    rfm_scored = rfm_scored.withColumn("customer_segment_label", lit("Others"))
    dataframes["rfm_segmentation"] = rfm_scored


def create_product_affinity(dataframes):
    order_products = (
        dataframes["order_items"]
        .filter(col("order_id").isNotNull() & col("product_id").isNotNull())
        .select("order_id", "product_id")
        .distinct()
    )
    product_pairs = (
        order_products.alias("a")
        .join(
            order_products.alias("b"),
            (col("a.order_id") == col("b.order_id"))
            & (col("a.product_id") < col("b.product_id")),
            "inner",
        )
        .select(
            col("a.product_id").alias("product_a_id"),
            col("b.product_id").alias("product_b_id"),
            col("a.order_id"),
        )
    )
    product_affinity = product_pairs.groupBy("product_a_id", "product_b_id").agg(
        countDistinct("order_id").alias("co_occurrence_count")
    )
    dataframes["product_affinity"] = product_affinity
    dataframes["top_product_pairs"] = product_affinity.orderBy(
        col("co_occurrence_count").desc()
    ).limit(100)

    # FIX: Create proper product_recommendations with array converted to string
    product_recommendations = product_affinity.groupBy("product_a_id").agg(
        count("product_b_id").alias("recommendation_count"),
        collect_list("product_b_id").alias("recommended_products_array"),
        avg("co_occurrence_count").alias("avg_affinity_score"),
    )

    # Convert array to PostgreSQL-compatible format (cast to string and PostgreSQL will handle it)
    product_recommendations = product_recommendations.withColumn(
        "recommended_products",
        concat(lit("{"), concat_ws(",", col("recommended_products_array")), lit("}")),
    ).drop("recommended_products_array")

    # Join with products to get product names
    if "products" in dataframes:
        product_recommendations = product_recommendations.join(
            dataframes["products"].select(
                col("product_id").alias("product_a_id"),
                col("product_name").alias("product_a_name"),
            ),
            "product_a_id",
            "left",
        )

    dataframes["product_recommendations"] = product_recommendations

    # Create category_affinity
    if "products" in dataframes:
        category_affinity = (
            product_affinity.join(
                dataframes["products"].select(
                    col("product_id").alias("product_a_id"),
                    col("category").alias("product_a_category"),
                ),
                "product_a_id",
                "left",
            )
            .join(
                dataframes["products"].select(
                    col("product_id").alias("product_b_id"),
                    col("category").alias("product_b_category"),
                ),
                "product_b_id",
                "left",
            )
            .filter(
                col("product_a_category").isNotNull()
                & col("product_b_category").isNotNull()
            )
            .groupBy("product_a_category", "product_b_category")
            .agg(
                count("*").alias("pair_count"),
                sum("co_occurrence_count").alias("total_co_occurrences"),
            )
        )
        dataframes["category_affinity"] = category_affinity.limit(100)
    else:
        dataframes["category_affinity"] = product_affinity.limit(100)


def create_inventory_health(dataframes):
    product_inventory_health = (
        dataframes["inventory"]
        .filter(col("product_id").isNotNull())
        .groupBy("product_id")
        .agg(
            expr("first(supplier_id)").alias("supplier_id"),  # Take first supplier
            sum("stock_quantity").alias(
                "current_stock"
            ),  # Sum stock across all locations/suppliers
        )
    )

    dataframes["product_inventory_health"] = product_inventory_health

    supplier_inventory_health = (
        dataframes["inventory"]
        .filter(col("supplier_id").isNotNull())
        .groupBy("supplier_id")
        .agg(countDistinct("product_id").alias("total_products"))
    )

    dataframes["supplier_inventory_health"] = supplier_inventory_health


def create_global_aggregations(dataframes, spark):
    global_metrics = {}
    global_metrics["total_revenue_all_time"] = (
        dataframes["orders"]
        .filter(col("total_amount").isNotNull() & (col("total_amount") > 0))
        .agg(sum("total_amount").alias("value"))
        .collect()[0]["value"]
        or 0.0
    )
    global_metrics["total_orders_all_time"] = dataframes["orders"].count()
    global_metrics["total_customers_all_time"] = dataframes["customers"].count()

    global_aggregations_data = [(k, float(v)) for k, v in global_metrics.items()]
    global_aggregations_df = spark.createDataFrame(
        global_aggregations_data, ["metric_name", "metric_value"]
    ).withColumn("calculated_at", lit("2025-11-14 16:34:39"))
    dataframes["global_aggregations"] = global_aggregations_df
