from pyspark.sql.window import Window

from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum as spark_sum,
    avg as spark_avg,
    when,
    year,
    month,
    expr,
    lag,
    coalesce,
    lit,
    greatest,
)

def aggregate_categories(dataframes):
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
            # Total products in category
            countDistinct(when(col("product_id").isNotNull(), col("product_id"))).alias(
                "total_products_in_category"
            ),
            # Total revenue (quantity * sell_price)
            spark_sum(
                when(
                    col("quantity").isNotNull()
                    & col("sell_price").isNotNull()
                    & (col("quantity") > 0)
                    & (col("sell_price") > 0),
                    col("quantity") * col("sell_price"),
                )
            ).alias("total_revenue"),
            # Total units sold
            spark_sum(
                when(
                    col("quantity").isNotNull() & (col("quantity") > 0), col("quantity")
                )
            ).alias("total_units_sold"),
            # Total orders
            countDistinct(when(col("order_id").isNotNull(), col("order_id"))).alias(
                "total_orders"
            ),
            # Unique customers
            countDistinct(
                when(col("customer_id").isNotNull(), col("customer_id"))
            ).alias("unique_customers"),
            # Average items per order
            spark_avg(
                when(
                    col("quantity").isNotNull() & (col("quantity") > 0), col("quantity")
                )
            ).alias("avg_items_per_order"),
        )
    )
    category_price_agg = (
        dataframes["products"]
        .filter(col("category").isNotNull())
        .groupBy("category")
        .agg(
            # Average product price in category
            spark_avg(
                when(
                    col("sell_price").isNotNull() & (col("sell_price") > 0),
                    col("sell_price"),
                )
            ).alias("avg_product_price")
        )
    )
    category_rating_agg = (
        dataframes["reviews"]
        .join(
            dataframes["products"].select("product_id", "category"),
            "product_id",
            "inner",
        )
        .filter(col("category").isNotNull())
        .groupBy("category")
        .agg(
            # Average rating
            spark_avg(
                when(col("rating").isNotNull() & (col("rating") > 0), col("rating"))
            ).alias("avg_rating"),
            # Total reviews
            count("review_id").alias("total_reviews"),
        )
    )
    total_revenue_all = (
        order_items_with_category.filter(
            col("quantity").isNotNull()
            & col("sell_price").isNotNull()
            & (col("quantity") > 0)
            & (col("sell_price") > 0)
        )
        .agg(
            spark_sum(col("quantity") * col("sell_price")).alias("grand_total_revenue")
        )
        .collect()[0]["grand_total_revenue"]
    )
    order_items_with_month = (
        order_items_with_category.filter(
            col("order_placed_at").isNotNull() & col("category").isNotNull()
        )
        .withColumn("order_year", year(col("order_placed_at")))
        .withColumn("order_month", month(col("order_placed_at")))
        .withColumn(
            "year_month", expr("concat(order_year, '-', lpad(order_month, 2, '0'))")
        )
    )
    category_monthly_revenue = order_items_with_month.groupBy(
        "category", "year_month", "order_year", "order_month"
    ).agg(
        spark_sum(
            when(
                col("quantity").isNotNull()
                & col("sell_price").isNotNull()
                & (col("quantity") > 0)
                & (col("sell_price") > 0),
                col("quantity") * col("sell_price"),
            )
        ).alias("monthly_revenue")
    )
    window_growth = Window.partitionBy("category").orderBy("order_year", "order_month")

    category_growth = category_monthly_revenue.withColumn(
        "prev_month_revenue", lag("monthly_revenue", 1).over(window_growth)
    ).withColumn(
        "month_over_month_growth",
        when(
            col("prev_month_revenue").isNotNull() & (col("prev_month_revenue") > 0),
            (
                (col("monthly_revenue") - col("prev_month_revenue"))
                / col("prev_month_revenue")
            )
            * 100,
        ),
    )

    # Average growth rate per category
    category_growth_agg = (
        category_growth.filter(col("month_over_month_growth").isNotNull())
        .groupBy("category")
        .agg(spark_avg("month_over_month_growth").alias("avg_category_growth_rate"))
    )
    order_items_with_season = (
        order_items_with_category.filter(
            col("order_placed_at").isNotNull() & col("category").isNotNull()
        )
        .withColumn("order_month", month(col("order_placed_at")))
        .withColumn(
            "season",
            when(col("order_month").isin(12, 1, 2), "Winter")
            .when(col("order_month").isin(3, 4, 5), "Spring")
            .when(col("order_month").isin(6, 7, 8), "Summer")
            .otherwise("Fall"),
        )
    )
    category_seasonal_revenue = order_items_with_season.groupBy(
        "category", "season"
    ).agg(
        spark_sum(
            when(
                col("quantity").isNotNull()
                & col("sell_price").isNotNull()
                & (col("quantity") > 0)
                & (col("sell_price") > 0),
                col("quantity") * col("sell_price"),
            )
        ).alias("seasonal_revenue")
    )
    category_avg_seasonal = category_seasonal_revenue.groupBy("category").agg(
        spark_avg("seasonal_revenue").alias("avg_seasonal_revenue")
    )
    category_seasonal_with_avg = category_seasonal_revenue.join(
        category_avg_seasonal, "category", "inner"
    ).withColumn(
        "seasonal_index",
        when(
            col("avg_seasonal_revenue").isNotNull() & (col("avg_seasonal_revenue") > 0),
            (col("seasonal_revenue") / col("avg_seasonal_revenue")) * 100,
        ),
    )
    category_seasonal_pivot = (
        category_seasonal_with_avg.groupBy("category")
        .pivot("season")
        .agg(expr("first(seasonal_index)"))
        .withColumnRenamed("Winter", "seasonal_index_winter")
        .withColumnRenamed("Spring", "seasonal_index_spring")
        .withColumnRenamed("Summer", "seasonal_index_summer")
        .withColumnRenamed("Fall", "seasonal_index_fall")
    )
    categories_df = (
        dataframes["products"]
        .select("category")
        .filter(col("category").isNotNull())
        .distinct()
    )
    categories_df = (
        categories_df.join(category_agg, "category", "left")
        .join(category_price_agg, "category", "left")
        .join(category_rating_agg, "category", "left")
        .join(category_growth_agg, "category", "left")
        .join(category_seasonal_pivot, "category", "left")
    )

    available_columns = categories_df.columns

    # Define the transformations dictionary
    transformations = {
        # Revenue share percentage
        "revenue_share_percentage": when(
            col("total_revenue").isNotNull()
            & (col("total_revenue") > 0)
            & (lit(total_revenue_all) > 0),
            (col("total_revenue") / lit(total_revenue_all)) * 100,
        ),
        # Average order value for category
        "avg_order_value": when(
            col("total_revenue").isNotNull()
            & col("total_orders").isNotNull()
            & (col("total_orders") > 0),
            col("total_revenue") / col("total_orders"),
        ),
        # Revenue per customer
        "revenue_per_customer": when(
            col("total_revenue").isNotNull()
            & col("unique_customers").isNotNull()
            & (col("unique_customers") > 0),
            col("total_revenue") / col("unique_customers"),
        ),
        # Average units per customer
        "avg_units_per_customer": when(
            col("total_units_sold").isNotNull()
            & col("unique_customers").isNotNull()
            & (col("unique_customers") > 0),
            col("total_units_sold") / col("unique_customers"),
        ),
        # Category popularity score (normalized)
        "category_popularity_score": when(
            col("total_orders").isNotNull()
            & col("unique_customers").isNotNull()
            & col("avg_rating").isNotNull(),
            (
                (col("total_orders") / lit(1000)) * lit(0.4)
                + (col("unique_customers") / lit(500)) * lit(0.3)
                + (col("avg_rating") / lit(5)) * lit(0.3)
            )
            * lit(100),
        ),
        # Product diversity index
        "product_diversity_index": when(
            col("total_products_in_category").isNotNull()
            & col("total_units_sold").isNotNull()
            & (col("total_products_in_category") > 0),
            col("total_units_sold") / col("total_products_in_category"),
        ),
        # Orders per product
        "avg_orders_per_product": when(
            col("total_orders").isNotNull()
            & col("total_products_in_category").isNotNull()
            & (col("total_products_in_category") > 0),
            col("total_orders") / col("total_products_in_category"),
        ),
        # Handle null values with defaults
        "total_products_in_category": coalesce(
            col("total_products_in_category"), lit(0)
        ),
        "total_revenue": coalesce(col("total_revenue"), lit(0.0)),
        "total_units_sold": coalesce(col("total_units_sold"), lit(0)),
        "total_orders": coalesce(col("total_orders"), lit(0)),
        "unique_customers": coalesce(col("unique_customers"), lit(0)),
        "total_reviews": coalesce(col("total_reviews"), lit(0)),
        "avg_rating": coalesce(col("avg_rating"), lit(0.0)),
        "avg_category_growth_rate": coalesce(col("avg_category_growth_rate"), lit(0.0)),
    }

    # Only add peak_season if all seasonal columns exist
    seasonal_columns = [
        "seasonal_index_winter",
        "seasonal_index_spring",
        "seasonal_index_summer",
        "seasonal_index_fall",
    ]

    if all(col_name in available_columns for col_name in seasonal_columns):
        transformations["peak_season"] = greatest(
            coalesce(col("seasonal_index_winter"), lit(0)),
            coalesce(col("seasonal_index_spring"), lit(0)),
            coalesce(col("seasonal_index_summer"), lit(0)),
            coalesce(col("seasonal_index_fall"), lit(0)),
        )
    else:
        # Set to null if seasonal data not available
        transformations["peak_season"] = lit(None).cast("double")

    # Apply all transformations
    categories_df = categories_df.withColumns(transformations)
    dataframes["categories"] = categories_df

    # Optional: Add category rankings
    categories_with_rank = (
        categories_df.withColumn(
            "revenue_rank", expr("row_number() OVER (ORDER BY total_revenue DESC)")
        )
        .withColumn("rating_rank", expr("row_number() OVER (ORDER BY avg_rating DESC)"))
        .withColumn(
            "growth_rank",
            expr("row_number() OVER (ORDER BY avg_category_growth_rate DESC)"),
        )
    )

    dataframes["categories"] = categories_with_rank

    # Also add category performance back to products for easy reference
    dataframes["products"] = (
        dataframes["products"]
        .drop("category_performance_tier")
        .join(
            categories_df.select(
                "category",
                "revenue_share_percentage",
                "avg_category_growth_rate",
                col("avg_rating").alias("category_avg_rating"),
            ),
            "category",
            "left",
        )
        .withColumn(
            "category_performance_tier",
            when(
                col("revenue_share_percentage").isNotNull(),
                when(col("revenue_share_percentage") >= 20, "Top Tier")
                .when(col("revenue_share_percentage") >= 10, "Mid Tier")
                .when(col("revenue_share_percentage") >= 5, "Growing")
                .otherwise("Niche"),
            ),
        )
    )
