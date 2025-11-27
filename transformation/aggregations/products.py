from pyspark.sql.window import Window

from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    avg as spark_avg,
    count,
    countDistinct,
    when,
    lit,
    expr,
    lower,
    coalesce,
    current_date,
    datediff,
    lag,
)


def aggregate_products(dataframes):
    order_items_enhanced = (
        dataframes["order_items"]
        .join(
            dataframes["products"].select("product_id", "sell_price", "cost_price"),
            "product_id",
            "left",
        )
        .select(
            "order_item_id",
            "order_id",
            "product_id",
            "quantity",
            "discount_amount",
            "product_cost",
            "sell_price",
            "cost_price",
        )
    )
    product_sales_agg = (
        order_items_enhanced.filter(col("product_id").isNotNull())
        .join(
            dataframes["orders"].select("order_id", "customer_id"), "order_id", "inner"
        )
        .groupBy("product_id")
        .agg(
            # Total units sold
            spark_sum(
                when(
                    col("quantity").isNotNull() & (col("quantity") > 0), col("quantity")
                )
            ).alias("total_units_sold"),
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
            # Total profit ((sell_price - cost_price) * quantity)
            spark_sum(
                when(
                    col("quantity").isNotNull()
                    & col("sell_price").isNotNull()
                    & col("cost_price").isNotNull()
                    & (col("quantity") > 0)
                    & (col("sell_price") > 0)
                    & (col("cost_price") >= 0),
                    (col("sell_price") - col("cost_price")) * col("quantity"),
                )
            ).alias("total_profit"),
            # Average profit margin
            spark_avg(
                when(
                    col("sell_price").isNotNull()
                    & col("cost_price").isNotNull()
                    & (col("sell_price") > 0)
                    & (col("cost_price") >= 0),
                    col("sell_price") - col("cost_price"),
                )
            ).alias("avg_profit_margin"),
            # Total orders
            countDistinct(when(col("order_id").isNotNull(), col("order_id"))).alias(
                "total_orders"
            ),
            # Unique customers
            countDistinct(
                when(col("customer_id").isNotNull(), col("customer_id"))
            ).alias("unique_customers"),
            # Average quantity per order
            spark_avg(
                when(
                    col("quantity").isNotNull() & (col("quantity") > 0), col("quantity")
                )
            ).alias("avg_quantity_per_order"),
            # Average discount amount
            spark_avg(
                when(
                    col("discount_amount").isNotNull() & (col("discount_amount") > 0),
                    col("discount_amount"),
                )
            ).alias("avg_discount_amount"),
        )
    )
    product_review_agg = (
        dataframes["reviews"]
        .filter(col("product_id").isNotNull())
        .groupBy("product_id")
        .agg(
            # Total reviews
            count("review_id").alias("total_reviews"),
            # Average rating
            spark_avg(
                when(col("rating").isNotNull() & (col("rating") > 0), col("rating"))
            ).alias("avg_rating"),
            # Rating standard deviation
            expr(
                "stddev(CASE WHEN rating IS NOT NULL AND rating > 0 THEN rating ELSE NULL END)"
            ).alias("rating_std_dev"),
            # Positive review rate (rating >= 4)
            (
                spark_sum(
                    when(
                        col("rating").isNotNull() & (col("rating") >= 4), lit(1)
                    ).otherwise(lit(0))
                )
                / count("review_id")
            ).alias("positive_review_rate"),
        )
    )
    product_wishlist_agg = (
        dataframes["wishlist"]
        .filter(col("product_id").isNotNull())
        .groupBy("product_id")
        .agg(
            # Total wishlist adds
            count("wishlist_id").alias("total_wishlist_adds"),
            # Wishlist to purchase rate
            (
                spark_sum(
                    when(col("purchased_date").isNotNull(), lit(1)).otherwise(lit(0))
                )
                / count("wishlist_id")
            ).alias("wishlist_to_purchase_rate"),
        )
    )
    product_cart_agg = (
        dataframes["shopping_cart"]
        .filter(col("product_id").isNotNull())
        .groupBy("product_id")
        .agg(
            # Total cart adds
            count("cart_id").alias("total_cart_adds"),
            # Cart to purchase rate (cart_status = 'purchased' or similar indicator)
            (
                spark_sum(
                    when(
                        col("cart_status").isNotNull()
                        & (
                            lower(col("cart_status")).isin(
                                "purchased", "completed", "ordered"
                            )
                        ),
                        lit(1),
                    ).otherwise(lit(0))
                )
                / count("cart_id")
            ).alias("cart_to_purchase_rate"),
        )
    )
    product_view_agg = (
        dataframes["customer_sessions"]
        .filter(col("products_viewed").isNotNull() & (col("products_viewed") > 0))
        .groupBy()
        .agg(spark_sum("products_viewed").alias("total_views_all_products"))
        .collect()[0]["total_views_all_products"]
    )
    product_inventory_agg = (
        dataframes["inventory"]
        .filter(col("product_id").isNotNull())
        .groupBy("product_id")
        .agg(
            # Current stock level (latest/last value)
            expr("last(stock_quantity)").alias("current_stock_level"),
            # Stockout days calculation
            spark_sum(
                when(
                    col("stock_quantity").isNotNull() & (col("stock_quantity") == 0),
                    lit(1),
                ).otherwise(lit(0))
            ).alias("stockout_occurrences"),
        )
    )
    inventory_restock = (
        dataframes["inventory"]
        .filter(col("product_id").isNotNull() & col("last_restocked_date").isNotNull())
        .select("product_id", "last_restocked_date")
        .distinct()
    )

    window_restock = Window.partitionBy("product_id").orderBy("last_restocked_date")

    inventory_with_prev = inventory_restock.withColumn(
        "prev_restock_date", lag("last_restocked_date", 1).over(window_restock)
    ).withColumn(
        "days_between_restocks",
        when(
            col("prev_restock_date").isNotNull(),
            datediff(col("last_restocked_date"), col("prev_restock_date")),
        ),
    )

    product_restock_agg = (
        inventory_with_prev.filter(col("days_between_restocks").isNotNull())
        .groupBy("product_id")
        .agg(spark_avg("days_between_restocks").alias("avg_restock_frequency"))
    )
    dataframes["products"] = (
        dataframes["products"]
        .join(product_sales_agg, "product_id", "left")
        .join(product_review_agg, "product_id", "left")
        .join(product_wishlist_agg, "product_id", "left")
        .join(product_cart_agg, "product_id", "left")
        .join(product_inventory_agg, "product_id", "left")
        .join(product_restock_agg, "product_id", "left")
    )
    dataframes["products"] = dataframes["products"].withColumns(
        {
            # View to purchase rate (if view data is available per product)
            # Note: Adjust if you have product-specific view counts
            "view_to_purchase_rate": when(
                col("total_units_sold").isNotNull() & (col("total_units_sold") > 0),
                col("total_units_sold")
                / lit(100),  # Placeholder - adjust with actual view data
            ),
            # Revenue per view (if view data is available)
            "revenue_per_view": when(
                col("total_revenue").isNotNull() & (col("total_revenue") > 0),
                col("total_revenue")
                / lit(100),  # Placeholder - adjust with actual view data
            ),
            # Days since launch
            "days_since_launch": when(
                col("launch_date").isNotNull(),
                datediff(current_date(), col("launch_date")),
            ),
            # Stockout days (approximation based on occurrences)
            "stockout_days": coalesce(col("stockout_occurrences"), lit(0)),
            # Product performance score (weighted metric)
            "product_performance_score": when(
                col("total_units_sold").isNotNull()
                & col("avg_rating").isNotNull()
                & col("total_profit").isNotNull(),
                (
                    # Sales volume (40%)
                    (col("total_units_sold") / lit(1000)) * lit(0.4)
                    +
                    # Rating (30%)
                    (col("avg_rating") / lit(5)) * lit(0.3)
                    +
                    # Profitability (30%)
                    (col("total_profit") / lit(10000)) * lit(0.3)
                )
                * lit(100),
            ),
            # Inventory turnover rate
            "inventory_turnover_rate": when(
                col("current_stock_level").isNotNull()
                & (col("current_stock_level") > 0)
                & col("total_units_sold").isNotNull(),
                col("total_units_sold") / col("current_stock_level"),
            ),
            # Average order value for this product
            "avg_order_value_product": when(
                col("total_revenue").isNotNull()
                & col("total_orders").isNotNull()
                & (col("total_orders") > 0),
                col("total_revenue") / col("total_orders"),
            ),
            # Customer penetration rate
            "customer_penetration": when(
                col("unique_customers").isNotNull() & (col("unique_customers") > 0),
                col("unique_customers")
                / lit(2000),  # Adjust denominator to total customer count
            ),
            # Handle null values with defaults
            "total_units_sold": coalesce(col("total_units_sold"), lit(0)),
            "total_revenue": coalesce(col("total_revenue"), lit(0.0)),
            "total_profit": coalesce(col("total_profit"), lit(0.0)),
            "total_orders": coalesce(col("total_orders"), lit(0)),
            "unique_customers": coalesce(col("unique_customers"), lit(0)),
            "total_reviews": coalesce(col("total_reviews"), lit(0)),
            "avg_rating": coalesce(col("avg_rating"), lit(0.0)),
            "total_wishlist_adds": coalesce(col("total_wishlist_adds"), lit(0)),
            "total_cart_adds": coalesce(col("total_cart_adds"), lit(0)),
            "current_stock_level": coalesce(col("current_stock_level"), lit(0)),
            "stockout_occurrences": coalesce(col("stockout_occurrences"), lit(0)),
        }
    )
    product_category_performance = (
        dataframes["products"]
        .filter(col("category").isNotNull())
        .groupBy("category")
        .agg(
            spark_sum("total_revenue").alias("category_total_revenue"),
            # spark_avg("avg_rating").alias("category_avg_rating"),
            count("product_id").alias("products_in_category"),
        )
    )

    dataframes["products"] = (
        dataframes["products"]
        .join(product_category_performance, "category", "left")
        .withColumn(
            "product_category_revenue_share",
            when(
                col("total_revenue").isNotNull()
                & col("category_total_revenue").isNotNull()
                & (col("category_total_revenue") > 0),
                (col("total_revenue") / col("category_total_revenue")) * 100,
            ),
        )
    )
