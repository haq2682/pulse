from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    avg as spark_avg,
    count,
    countDistinct,
    when,
    lit,
    trim,
    expr,
    lower,
    coalesce,
    current_date,
    datediff,
    lag,
    first,
    broadcast,
)

STATUS_MAPPING = {
    # Purchased / Completed
    "purchased": "purchased",
    "completed": "purchased",
    "ordered": "purchased",
    "paid": "purchased",
    "checked_out": "purchased",
    "fulfilled": "purchased",
    "shipped": "purchased",
    "delivered": "purchased",
    "closed": "purchased",
    "converted": "purchased",
    
    # Active / In progress
    "active": "active",
    "in_progress": "active",
    "open": "active",
    "pending": "active",
    "awaiting_payment": "active",
    "processing": "active",
    "in_checkout": "active",
    "review": "active",
    
    # Abandoned / Canceled
    "abandoned": "abandoned",
    "canceled": "abandoned",
    "cancelled": "abandoned",
    "expired": "abandoned",
    "failed": "abandoned",
    "returned": "abandoned",
    "rejected": "abandoned",
    
    # Deleted / Removed
    "deleted": "deleted",
    "removed": "deleted",
    "voided": "deleted",
    
    # Unknown / Misc
    "unknown": "unknown",
    "error": "unknown",
    "other": "unknown",
    "draft": "unknown",
    "inactive": "unknown",
}


def normalize_cart_status(spark, df, mapping):
    mapping_data = [(k, v) for k, v in mapping.items()]
    mapping_df = spark.createDataFrame(mapping_data, ["raw_status", "normalized_status"])
    
    # Trim and lower
    df_with_lower = df.withColumn("cart_status_lower", lower(trim(col("cart_status"))))
    
    result = (
        df_with_lower
        .join(
            broadcast(mapping_df),
            df_with_lower["cart_status_lower"] == mapping_df["raw_status"],
            "left"
        )
        .withColumn(
            "cart_status_normalized",
            coalesce(col("normalized_status"), lit("unknown"))
        )
        .drop("cart_status_lower", "raw_status", "normalized_status")
    )
    
    # Debug print: what statuses did we find?
    result.groupBy("cart_status", "cart_status_normalized").count().show(100, truncate=False)
    return result


def _is_dataframe_empty(df):
    """
    Check if a DataFrame is empty using a cheaper method than .count().
    
    Args:
        df: DataFrame to check
    
    Returns:
        True if empty, False otherwise
    """
    return df.head(1) is None or len(df.head(1)) == 0


def aggregate_products(spark, dataframes):
    """
    Aggregate product metrics from various source dataframes.
    
    Args:
        spark: SparkSession instance (required for broadcast operations)
        dataframes: Dictionary of DataFrames containing order_items, products, orders, etc.
    
    Returns:
        None (modifies dataframes["products"] in place)
    """
    # Skip aggregation if required dataframes don't exist
    required_dataframes = ["order_items", "products", "orders", "cart_items", "shopping_cart"]
    for df_name in required_dataframes:
        if df_name not in dataframes or dataframes[df_name] is None:
            print(f"⚠️ Skipping aggregate_products: '{df_name}' dataframe not found")
            return
        # Use cheaper emptiness check instead of .count() == 0
        if _is_dataframe_empty(dataframes[df_name]):
            print(f"⚠️ Skipping aggregate_products: '{df_name}' dataframe is empty")
            return
    
    # =========================================================================
    # SALES AGGREGATION
    # =========================================================================
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
            "product_price",
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
                ).otherwise(lit(0))
            ).alias("total_units_sold"),
            # Total revenue (quantity * sell_price)
            spark_sum(
                when(
                    col("quantity").isNotNull()
                    & col("sell_price").isNotNull()
                    & (col("quantity") > 0)
                    & (col("sell_price") > 0),
                    col("quantity") * col("sell_price"),
                ).otherwise(lit(0))
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
                ).otherwise(lit(0))
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
    
    # =========================================================================
    # REVIEW AGGREGATION
    # =========================================================================
    product_review_agg = None
    if "reviews" in dataframes and dataframes["reviews"] is not None:
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
    
    # =========================================================================
    # WISHLIST AGGREGATION
    # =========================================================================
    product_wishlist_agg = None
    if "wishlist" in dataframes and dataframes["wishlist"] is not None:
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
    
    # =========================================================================
    # CART AGGREGATION
    # =========================================================================
    cart_df = normalize_cart_status(spark, dataframes["shopping_cart"], STATUS_MAPPING)

    product_cart_agg = (
        dataframes["cart_items"]
        .join(cart_df.select("cart_id", "cart_status_normalized"), "cart_id", "left")
        .filter(col("product_id").isNotNull())
        .groupBy("product_id")
        .agg(
            count("cart_item_id").alias("total_cart_adds"),
            (
                spark_sum(
                    when(col("cart_status_normalized") == "purchased", lit(1)).otherwise(lit(0))
                )
                / count("cart_item_id")
            ).alias("cart_to_purchase_rate")
        )
    )
    
    # =========================================================================
    # VIEW AGGREGATION
    # Get total views and total customers for dynamic calculations
    # =========================================================================
    total_views_all_products = None
    if "customer_sessions" in dataframes and dataframes["customer_sessions"] is not None:
        # Compute total views as a DataFrame to avoid expensive .collect()
        total_views_df = (
            dataframes["customer_sessions"]
            .filter(col("products_viewed").isNotNull() & (col("products_viewed") > 0))
            .agg(spark_sum("products_viewed").alias("total_views_all_products"))
        )
        # Cache this small result for reuse
        total_views_all_products = total_views_df.first()["total_views_all_products"]
        if total_views_all_products is None or total_views_all_products == 0:
            total_views_all_products = 1  # Avoid division by zero
    else:
        total_views_all_products = 1  # Default fallback
    
    # Get total unique customers for customer penetration calculation
    total_customers = (
        dataframes["orders"]
        .select("customer_id")
        .filter(col("customer_id").isNotNull())
        .distinct()
        .count()
    )
    if total_customers == 0:
        total_customers = 1  # Avoid division by zero
    
    # =========================================================================
    # INVENTORY AGGREGATION
    # Use window function to get the latest stock quantity per product
    # =========================================================================
    product_inventory_agg = None
    if "inventory" in dataframes and dataframes["inventory"] is not None:
        # Window to get the latest inventory record per product
        # Assumes there's a date column; adjust 'last_restocked_date' if needed
        inventory_window = Window.partitionBy("product_id").orderBy(
            col("last_restocked_date").desc_nulls_last()
        )
        
        product_inventory_agg = (
            dataframes["inventory"]
            .filter(col("product_id").isNotNull())
            .withColumn("row_num", F.row_number().over(inventory_window))
            .filter(col("row_num") == 1)
            .select("product_id", col("stock_quantity").alias("current_stock_level"))
        )
        
        # Stockout occurrences (count of records where stock was 0)
        stockout_agg = (
            dataframes["inventory"]
            .filter(col("product_id").isNotNull())
            .groupBy("product_id")
            .agg(
                spark_sum(
                    when(
                        col("stock_quantity").isNotNull() & (col("stock_quantity") == 0),
                        lit(1),
                    ).otherwise(lit(0))
                ).alias("stockout_occurrences"),
            )
        )
        
        product_inventory_agg = product_inventory_agg.join(
            stockout_agg, "product_id", "left"
        )
    
    # =========================================================================
    # RESTOCK FREQUENCY AGGREGATION
    # =========================================================================
    product_restock_agg = None
    if "inventory" in dataframes and dataframes["inventory"] is not None:
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
            ).otherwise(lit(None)),
        )

        product_restock_agg = (
            inventory_with_prev.filter(col("days_between_restocks").isNotNull())
            .groupBy("product_id")
            .agg(spark_avg("days_between_restocks").alias("avg_restock_frequency"))
        )
    
    # =========================================================================
    # JOIN ALL AGGREGATIONS TO PRODUCTS
    # =========================================================================
    products_df = dataframes["products"]
    
    products_df = products_df.join(product_sales_agg, "product_id", "left")
    
    if product_review_agg is not None:
        products_df = products_df.join(product_review_agg, "product_id", "left")
    
    if product_wishlist_agg is not None:
        products_df = products_df.join(product_wishlist_agg, "product_id", "left")
    
    products_df = products_df.join(product_cart_agg, "product_id", "left")
    
    if product_inventory_agg is not None:
        products_df = products_df.join(product_inventory_agg, "product_id", "left")
    
    if product_restock_agg is not None:
        products_df = products_df.join(product_restock_agg, "product_id", "left")
    
    # ============================================================================
    # DERIVED COLUMNS
    # Using chained withColumn calls instead of invalid withColumns
    # ============================================================================
    
    # View to purchase rate (using dynamic total views)
    products_df = products_df.withColumn(
        "view_to_purchase_rate",
        when(
            col("total_units_sold").isNotNull() & (col("total_units_sold") > 0),
            col("total_units_sold") / lit(total_views_all_products),
        ).otherwise(lit(0.0))
    )
    
    # Revenue per view (using dynamic total views)
    products_df = products_df.withColumn(
        "revenue_per_view",
        when(
            col("total_revenue").isNotNull() & (col("total_revenue") > 0),
            col("total_revenue") / lit(total_views_all_products),
        ).otherwise(lit(0.0))
    )
    
    # Days since launch
    products_df = products_df.withColumn(
        "days_since_launch",
        when(
            col("launch_date").isNotNull(),
            datediff(current_date(), col("launch_date")),
        ).otherwise(lit(None))
    )
    
    # Stockout days (approximation based on occurrences)
    products_df = products_df.withColumn(
        "stockout_days",
        coalesce(col("stockout_occurrences"), lit(0))
    )
    
    # Product performance score (weighted metric)
    # Note: Scaling factors (1000, 10000) should be adjusted based on your data distribution
    # - 1000: Expected max/typical units sold for normalization
    # - 10000: Expected max/typical profit for normalization
    products_df = products_df.withColumn(
        "product_performance_score",
        when(
            col("total_units_sold").isNotNull()
            & col("avg_rating").isNotNull()
            & col("total_profit").isNotNull(),
            (
                # Sales volume weight: 40%
                (col("total_units_sold") / lit(1000)) * lit(0.4)
                +
                # Rating weight: 30% (normalized to 0-1 scale from 0-5)
                (col("avg_rating") / lit(5)) * lit(0.3)
                +
                # Profitability weight: 30%
                (col("total_profit") / lit(10000)) * lit(0.3)
            )
            * lit(100),
        ).otherwise(lit(0.0))
    )
    
    # Inventory turnover rate
    products_df = products_df.withColumn(
        "inventory_turnover_rate",
        when(
            col("current_stock_level").isNotNull()
            & (col("current_stock_level") > 0)
            & col("total_units_sold").isNotNull(),
            col("total_units_sold") / col("current_stock_level"),
        ).otherwise(lit(0.0))
    )
    
    # Average order value for this product
    products_df = products_df.withColumn(
        "avg_order_value_product",
        when(
            col("total_revenue").isNotNull()
            & col("total_orders").isNotNull()
            & (col("total_orders") > 0),
            col("total_revenue") / col("total_orders"),
        ).otherwise(lit(0.0))
    )
    
    # Customer penetration rate (using dynamic total customer count)
    products_df = products_df.withColumn(
        "customer_penetration",
        when(
            col("unique_customers").isNotNull() & (col("unique_customers") > 0),
            col("unique_customers") / lit(total_customers),
        ).otherwise(lit(0.0))
    )
    
    # =========================================================================
    # HANDLE NULL VALUES WITH DEFAULTS
    # =========================================================================
    null_defaults = {
        "total_units_sold": lit(0),
        "total_revenue": lit(0.0),
        "total_profit": lit(0.0),
        "total_orders": lit(0),
        "unique_customers": lit(0),
        "total_reviews": lit(0),
        "avg_rating": lit(0.0),
        "total_wishlist_adds": lit(0),
        "total_cart_adds": lit(0),
        "current_stock_level": lit(0),
        "stockout_occurrences": lit(0),
    }
    
    for col_name, default_val in null_defaults.items():
        # Only apply coalesce if the column exists
        if col_name in products_df.columns:
            products_df = products_df.withColumn(
                col_name, coalesce(col(col_name), default_val)
            )
    
    # =========================================================================
    # CATEGORY PERFORMANCE
    # =========================================================================
    product_category_performance = (
        products_df
        .filter(col("category").isNotNull())
        .groupBy("category")
        .agg(
            spark_sum("total_revenue").alias("category_total_revenue"),
            count("product_id").alias("products_in_category"),
        )
    )

    products_df = (
        products_df
        .join(product_category_performance, "category", "left")
        .withColumn(
            "product_category_revenue_share",
            when(
                col("total_revenue").isNotNull()
                & col("category_total_revenue").isNotNull()
                & (col("category_total_revenue") > 0),
                (col("total_revenue") / col("category_total_revenue")) * 100,
            ).otherwise(lit(0.0)),
        )
    )
    
    # Update the dataframes dictionary
    dataframes["products"] = products_df