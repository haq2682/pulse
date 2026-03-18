from pyspark.sql import SparkSession
from pyspark import StorageLevel

from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    avg as spark_avg,
    count,
    countDistinct,
    current_timestamp,
    datediff,
    lower,
    lit,
    to_date,
    when,
)


def _df(dataframes, name):
    """Return the DataFrame or None if absent/empty (without triggering a count)."""
    return dataframes.get(name)


def global_aggregations(spark, dataframes):
    # Skip aggregation if required dataframes don't exist.
    # Use isEmpty() (no shuffle, no full scan) instead of .count() == 0.
    required_dataframes = ["orders", "customers", "products"]
    for df_name in required_dataframes:
        df = _df(dataframes, df_name)
        if df is None or df.rdd.isEmpty():
            print(f"⚠️ Skipping global_aggregations: '{df_name}' dataframe not found or empty")
            return

    global_metrics = {}

    # ------------------------------------------------------------------
    # ORDERS — one combined aggregation pass (avoids 5+ separate actions)
    # ------------------------------------------------------------------
    orders = (
        dataframes["orders"]
        .select(
            "customer_id",
            "total_amount",
            "order_shipped_at",
            "order_delivered_at",
            "order_placed_at",
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    orders_agg = (
        orders
        .agg(
            count("*").alias("total_orders"),
            countDistinct("customer_id").alias("active_customers"),
            spark_avg(
                when(col("total_amount").isNotNull() & (col("total_amount") > 0),
                     col("total_amount"))
            ).alias("avg_order_value"),
            spark_sum(
                when(col("total_amount").isNotNull() & (col("total_amount") > 0),
                     col("total_amount"))
            ).alias("total_revenue"),
        )
        .collect()[0]
    )

    global_metrics["total_orders_all_time"]  = int(orders_agg["total_orders"] or 0)
    global_metrics["total_revenue_all_time"] = float(orders_agg["total_revenue"] or 0.0)
    global_metrics["avg_order_value_global"] = float(orders_agg["avg_order_value"] or 0.0)
    global_metrics["total_active_customers"] = int(orders_agg["active_customers"] or 0)

    # Delivery & processing time — needs withColumn, keep as a single action each
    global_metrics["avg_delivery_time_days"] = (
        orders
        .filter(col("order_shipped_at").isNotNull() & col("order_delivered_at").isNotNull())
        .withColumn("delivery_time", datediff(col("order_delivered_at"), col("order_shipped_at")))
        .filter(col("delivery_time") >= 0)
        .agg(spark_avg("delivery_time").alias("v"))
        .collect()[0]["v"] or 0.0
    )

    global_metrics["avg_order_processing_days"] = (
        orders
        .filter(col("order_placed_at").isNotNull() & col("order_shipped_at").isNotNull())
        .withColumn("processing_time", datediff(col("order_shipped_at"), to_date(col("order_placed_at"))))
        .filter(col("processing_time") >= 0)
        .agg(spark_avg("processing_time").alias("v"))
        .collect()[0]["v"] or 0.0
    )

    orders.unpersist(blocking=False)

    # ------------------------------------------------------------------
    # CUSTOMERS — one combined pass
    # ------------------------------------------------------------------
    customers = (
        dataframes["customers"]
        .select("customer_lifetime_value")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    cust_agg = (
        customers
        .agg(
            count("*").alias("total_customers"),
            spark_avg(
                when(col("customer_lifetime_value").isNotNull(),
                     col("customer_lifetime_value"))
            ).alias("avg_clv"),
        )
        .collect()[0]
    )

    global_metrics["total_customers_all_time"]            = int(cust_agg["total_customers"] or 0)
    global_metrics["avg_customer_lifetime_value_global"]  = float(cust_agg["avg_clv"] or 0.0)

    customers.unpersist(blocking=False)

    global_metrics["customer_activation_rate"] = (
        (global_metrics["total_active_customers"] / global_metrics["total_customers_all_time"] * 100)
        if global_metrics["total_customers_all_time"] > 0 else 0.0
    )

    # ------------------------------------------------------------------
    # PRODUCTS — one combined pass
    # ------------------------------------------------------------------
    products = (
        dataframes["products"]
        .select("product_id", "category", "sell_price", "supplier_id", "cost_price")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    prod_agg = (
        products
        .agg(
            count("*").alias("total_products"),
            countDistinct(when(col("category").isNotNull(), col("category"))).alias("total_categories"),
            spark_avg(
                when(col("sell_price").isNotNull() & (col("sell_price") > 0),
                     col("sell_price"))
            ).alias("avg_price"),
        )
        .collect()[0]
    )

    global_metrics["total_products_catalog"] = int(prod_agg["total_products"] or 0)
    global_metrics["total_categories"]       = int(prod_agg["total_categories"] or 0)
    global_metrics["avg_product_price"]      = float(prod_agg["avg_price"] or 0.0)

    global_metrics["avg_products_per_supplier"] = (
        products
        .filter(col("supplier_id").isNotNull())
        .groupBy("supplier_id")
        .agg(count("product_id").alias("product_count"))
        .agg(spark_avg("product_count").alias("v"))
        .collect()[0]["v"] or 0.0
    )

    # ------------------------------------------------------------------
    # ORDER_ITEMS — one combined pass
    # ------------------------------------------------------------------
    order_items = _df(dataframes, "order_items")
    if order_items is not None:
        order_items = (
            order_items
            .select("order_id", "quantity")
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        oi_agg = (
            order_items
            .filter(col("quantity").isNotNull() & (col("quantity") > 0))
            .agg(spark_sum("quantity").alias("total_units"))
            .collect()[0]
        )
        global_metrics["total_units_sold_all_time"] = int(oi_agg["total_units"] or 0)

        global_metrics["avg_items_per_order"] = (
            order_items
            .filter(col("quantity").isNotNull() & (col("quantity") > 0))
            .groupBy("order_id")
            .agg(spark_sum("quantity").alias("order_qty"))
            .agg(spark_avg("order_qty").alias("v"))
            .collect()[0]["v"] or 0.0
        )
        order_items.unpersist(blocking=False)
    else:
        global_metrics["total_units_sold_all_time"] = 0
        global_metrics["avg_items_per_order"]       = 0.0

    # ------------------------------------------------------------------
    # REVIEWS — one combined pass
    # ------------------------------------------------------------------
    reviews = _df(dataframes, "reviews")
    if reviews is not None:
        rev_agg = (
            reviews
            .agg(
                count("*").alias("total_reviews"),
                spark_avg(
                    when(col("rating").isNotNull() & (col("rating") > 0), col("rating"))
                ).alias("avg_rating"),
            )
            .collect()[0]
        )
        global_metrics["total_reviews_all_time"]       = int(rev_agg["total_reviews"] or 0)
        global_metrics["overall_customer_satisfaction"] = float(rev_agg["avg_rating"] or 0.0)
    else:
        global_metrics["total_reviews_all_time"]       = 0
        global_metrics["overall_customer_satisfaction"] = 0.0

    global_metrics["review_participation_rate"] = (
        (global_metrics["total_reviews_all_time"] / global_metrics["total_orders_all_time"] * 100)
        if global_metrics["total_orders_all_time"] > 0 else 0.0
    )

    # ------------------------------------------------------------------
    # CUSTOMER SESSIONS — one combined pass
    # ------------------------------------------------------------------
    sessions = _df(dataframes, "customer_sessions")
    if sessions is not None:
        sess_agg = (
            sessions
            .agg(
                count("*").alias("total_sessions"),
                spark_sum(when(col("conversion_flag") == 1, lit(1)).otherwise(lit(0))).alias("converted"),
            )
            .collect()[0]
        )
        total_sessions_val   = int(sess_agg["total_sessions"] or 0)
        converted_sessions   = int(sess_agg["converted"] or 0)
        global_metrics["overall_conversion_rate"] = (
            (converted_sessions / total_sessions_val * 100) if total_sessions_val > 0 else 0.0
        )
    else:
        global_metrics["overall_conversion_rate"] = 0.0

    # ------------------------------------------------------------------
    # SHOPPING CART — one combined pass
    # ------------------------------------------------------------------
    cart = _df(dataframes, "shopping_cart")
    if cart is not None:
        cart_agg = (
            cart
            .select("cart_id", "cart_status")
            .agg(
                countDistinct("cart_id").alias("total_carts"),
                countDistinct(
                    when(
                        col("cart_status").isNotNull()
                        & ~lower(col("cart_status")).isin("purchased", "completed", "ordered"),
                        col("cart_id"),
                    )
                ).alias("abandoned_carts"),
            )
            .collect()[0]
        )
        total_carts_val   = int(cart_agg["total_carts"] or 0)
        abandoned_carts   = int(cart_agg["abandoned_carts"] or 0)
        global_metrics["overall_cart_abandonment_rate"] = (
            (abandoned_carts / total_carts_val * 100) if total_carts_val > 0 else 0.0
        )
    else:
        global_metrics["overall_cart_abandonment_rate"] = 0.0

    # ------------------------------------------------------------------
    # SUPPLIERS
    # ------------------------------------------------------------------
    suppliers = _df(dataframes, "suppliers")
    global_metrics["total_suppliers"] = (
        suppliers.agg(count("*").alias("n")).collect()[0]["n"] if suppliers is not None else 0
    )

    # ------------------------------------------------------------------
    # MARKETING CAMPAIGNS — one combined pass
    # ------------------------------------------------------------------
    campaigns = _df(dataframes, "marketing_campaigns")
    if campaigns is not None:
        camp_agg = (
            campaigns
            .agg(
                count("*").alias("total_campaigns"),
                spark_avg(when(col("roi").isNotNull(), col("roi"))).alias("avg_roi"),
            )
            .collect()[0]
        )
        global_metrics["total_marketing_campaigns"] = int(camp_agg["total_campaigns"] or 0)
        global_metrics["avg_campaign_roi"]          = float(camp_agg["avg_roi"] or 0.0)
    else:
        global_metrics["total_marketing_campaigns"] = 0
        global_metrics["avg_campaign_roi"]          = 0.0

    # ------------------------------------------------------------------
    # INVENTORY — join with products cache already released; re-read narrow slice
    # ------------------------------------------------------------------
    inventory = _df(dataframes, "inventory")
    if inventory is not None:
        inv_value = (
            inventory
            .select("product_id", "stock_quantity")
            .join(
                products.select(
                    col("product_id").alias("inv_prod_id"), "cost_price"
                ),
                inventory["product_id"] == col("inv_prod_id"),
                "left",
            )
            .filter(
                col("stock_quantity").isNotNull()
                & col("cost_price").isNotNull()
                & (col("stock_quantity") > 0)
                & (col("cost_price") > 0)
            )
            .withColumn("inventory_value", col("stock_quantity") * col("cost_price"))
            .agg(spark_sum("inventory_value").alias("v"))
            .collect()[0]["v"]
        )
        global_metrics["total_inventory_value"] = float(inv_value or 0.0)
    else:
        global_metrics["total_inventory_value"] = 0.0

    products.unpersist(blocking=False)

    # Create DataFrame from global metrics
    global_aggregations_data = [(k, float(v)) for k, v in global_metrics.items()]
    global_aggregations_df = spark.createDataFrame(
        global_aggregations_data, ["metric_name", "metric_value"]
    ).withColumn("calculated_at", current_timestamp().cast("string"))

    dataframes["global_aggregations"] = global_aggregations_df

    # Create a summary view with formatted metrics
    print("\n" + "=" * 80)
    print("GLOBAL BUSINESS METRICS SUMMARY")
    print("=" * 80)
    print(f"\nRevenue & Orders:")
    print(
        f"  Total Revenue (All Time):           ${global_metrics['total_revenue_all_time']:,.2f}"
    )
    print(
        f"  Total Orders (All Time):            {global_metrics['total_orders_all_time']:,}"
    )
    print(
        f"  Average Order Value:                ${global_metrics['avg_order_value_global']:,.2f}"
    )
    print(
        f"  Total Units Sold:                   {global_metrics['total_units_sold_all_time']:,}"
    )

    print(f"\nCustomers:")
    print(
        f"  Total Customers:                    {global_metrics['total_customers_all_time']:,}"
    )
    print(
        f"  Active Customers:                   {global_metrics['total_active_customers']:,}"
    )
    print(
        f"  Customer Activation Rate:           {global_metrics['customer_activation_rate']:.2f}%"
    )
    print(
        f"  Avg Customer Lifetime Value:        ${global_metrics['avg_customer_lifetime_value_global']:,.2f}"
    )

    print(f"\nConversion & Engagement:")
    print(
        f"  Overall Conversion Rate:            {global_metrics['overall_conversion_rate']:.2f}%"
    )
    print(
        f"  Cart Abandonment Rate:              {global_metrics['overall_cart_abandonment_rate']:.2f}%"
    )
    print(
        f"  Customer Satisfaction (Avg Rating): {global_metrics['overall_customer_satisfaction']:.2f}/5.0"
    )
    print(
        f"  Review Participation Rate:          {global_metrics['review_participation_rate']:.2f}%"
    )

    print(f"\nOperations:")
    print(
        f"  Avg Order Processing Time:          {global_metrics['avg_order_processing_days']:.1f} days"
    )
    print(
        f"  Avg Delivery Time:                  {global_metrics['avg_delivery_time_days']:.1f} days"
    )
    print(
        f"  Avg Items per Order:                {global_metrics['avg_items_per_order']:.1f}"
    )

    print(f"\nCatalog & Inventory:")
    print(
        f"  Total Products in Catalog:          {global_metrics['total_products_catalog']:,}"
    )
    print(
        f"  Total Categories:                   {global_metrics['total_categories']:,}"
    )
    print(
        f"  Average Product Price:              ${global_metrics['avg_product_price']:,.2f}"
    )
    print(
        f"  Total Inventory Value:              ${global_metrics['total_inventory_value']:,.2f}"
    )

    print(f"\nSuppliers & Marketing:")
    print(
        f"  Total Suppliers:                    {global_metrics['total_suppliers']:,}"
    )
    print(
        f"  Avg Products per Supplier:          {global_metrics['avg_products_per_supplier']:.1f}"
    )
    print(
        f"  Total Marketing Campaigns:          {global_metrics['total_marketing_campaigns']:,}"
    )
    print(
        f"  Average Campaign ROI:               {global_metrics['avg_campaign_roi']:.2f}%"
    )

    print("\n" + "=" * 80)
    print("Global aggregations completed!")
    print(f"Metrics stored in: dataframes['global_aggregations']")
    print("=" * 80 + "\n")

    # Show the dataframe
    dataframes["global_aggregations"].show(50, False)
