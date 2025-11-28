from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    avg as spark_avg,
    count,
    datediff,
    lower,
    lit,
    to_date,
)


def global_aggregations(spark, dataframes):
    # Calculate global metrics across entire dataset
    global_metrics = {}

    # Total revenue all time
    global_metrics["total_revenue_all_time"] = (
        dataframes["orders"]
        .filter(col("total_amount").isNotNull() & (col("total_amount") > 0))
        .agg(spark_sum("total_amount").alias("value"))
        .collect()[0]["value"]
        or 0.0
    )

    # Total orders all time
    global_metrics["total_orders_all_time"] = dataframes["orders"].count()

    # Total customers all time
    global_metrics["total_customers_all_time"] = dataframes["customers"].count()

    # Average order value global
    global_metrics["avg_order_value_global"] = (
        dataframes["orders"]
        .filter(col("total_amount").isNotNull() & (col("total_amount") > 0))
        .agg(spark_avg("total_amount").alias("value"))
        .collect()[0]["value"]
        or 0.0
    )

    # Total products in catalog
    global_metrics["total_products_catalog"] = dataframes["products"].count()

    # Average customer lifetime value global
    global_metrics["avg_customer_lifetime_value_global"] = (
        dataframes["customers"]
        .filter(col("customer_lifetime_value").isNotNull())
        .agg(spark_avg("customer_lifetime_value").alias("value"))
        .collect()[0]["value"]
        or 0.0
    )

    # Overall conversion rate (sessions with orders / total sessions)
    total_sessions = dataframes["customer_sessions"].count()
    converted_sessions = (
        dataframes["customer_sessions"].filter(col("conversion_flag") == 1).count()
    )

    global_metrics["overall_conversion_rate"] = (
        (converted_sessions / total_sessions * 100) if total_sessions > 0 else 0.0
    )

    # Overall cart abandonment rate
    total_carts = dataframes["shopping_cart"].select("cart_id").distinct().count()
    abandoned_carts = (
        dataframes["shopping_cart"]
        .filter(
            col("cart_status").isNotNull()
            & ~lower(col("cart_status")).isin("purchased", "completed", "ordered")
        )
        .select("cart_id")
        .distinct()
        .count()
    )

    global_metrics["overall_cart_abandonment_rate"] = (
        (abandoned_carts / total_carts * 100) if total_carts > 0 else 0.0
    )

    # Average delivery time in days
    global_metrics["avg_delivery_time_days"] = (
        dataframes["orders"]
        .filter(
            col("order_shipped_at").isNotNull() & col("order_delivered_at").isNotNull()
        )
        .withColumn(
            "delivery_time",
            datediff(col("order_delivered_at"), col("order_shipped_at")),
        )
        .filter(col("delivery_time") >= 0)
        .agg(spark_avg("delivery_time").alias("value"))
        .collect()[0]["value"]
        or 0.0
    )

    # Overall customer satisfaction (average rating)
    global_metrics["overall_customer_satisfaction"] = (
        dataframes["reviews"]
        .filter(col("rating").isNotNull() & (col("rating") > 0))
        .agg(spark_avg("rating").alias("value"))
        .collect()[0]["value"]
        or 0.0
    )

    # Additional useful global metrics
    # Total units sold all time
    global_metrics["total_units_sold_all_time"] = (
        dataframes["order_items"]
        .filter(col("quantity").isNotNull() & (col("quantity") > 0))
        .agg(spark_sum("quantity").alias("value"))
        .collect()[0]["value"]
        or 0
    )

    # Total active customers (with at least one order)
    global_metrics["total_active_customers"] = (
        dataframes["orders"].select("customer_id").distinct().count()
    )

    # Customer activation rate
    global_metrics["customer_activation_rate"] = (
        (
            global_metrics["total_active_customers"]
            / global_metrics["total_customers_all_time"]
            * 100
        )
        if global_metrics["total_customers_all_time"] > 0
        else 0.0
    )

    # Average items per order
    global_metrics["avg_items_per_order"] = (
        dataframes["order_items"]
        .filter(col("quantity").isNotNull() & (col("quantity") > 0))
        .groupBy("order_id")
        .agg(spark_sum("quantity").alias("order_items"))
        .agg(spark_avg("order_items").alias("value"))
        .collect()[0]["value"]
        or 0.0
    )

    # Total reviews submitted
    global_metrics["total_reviews_all_time"] = dataframes["reviews"].count()

    # Review participation rate
    global_metrics["review_participation_rate"] = (
        (
            global_metrics["total_reviews_all_time"]
            / global_metrics["total_orders_all_time"]
            * 100
        )
        if global_metrics["total_orders_all_time"] > 0
        else 0.0
    )

    # Total suppliers
    global_metrics["total_suppliers"] = dataframes["suppliers"].count()

    # Average products per supplier
    global_metrics["avg_products_per_supplier"] = (
        dataframes["products"]
        .filter(col("supplier_id").isNotNull())
        .groupBy("supplier_id")
        .agg(count("product_id").alias("product_count"))
        .agg(spark_avg("product_count").alias("value"))
        .collect()[0]["value"]
        or 0.0
    )

    # Total categories
    global_metrics["total_categories"] = (
        dataframes["products"]
        .select("category")
        .filter(col("category").isNotNull())
        .distinct()
        .count()
    )

    # Average price point
    global_metrics["avg_product_price"] = (
        dataframes["products"]
        .filter(col("sell_price").isNotNull() & (col("sell_price") > 0))
        .agg(spark_avg("sell_price").alias("value"))
        .collect()[0]["value"]
        or 0.0
    )

    # Total marketing campaigns
    global_metrics["total_marketing_campaigns"] = dataframes[
        "marketing_campaigns"
    ].count()

    # Average campaign ROI
    global_metrics["avg_campaign_roi"] = (
        dataframes["marketing_campaigns"]
        .filter(col("roi").isNotNull())
        .agg(spark_avg("roi").alias("value"))
        .collect()[0]["value"]
        or 0.0
    )

    # Total inventory value
    global_metrics["total_inventory_value"] = (
        dataframes["inventory"]
        .join(
            dataframes["products"].select(
                col("product_id").alias("inv_prod_id"), "cost_price"
            ),
            dataframes["inventory"]["product_id"] == col("inv_prod_id"),
            "left",
        )
        .filter(
            col("stock_quantity").isNotNull()
            & col("cost_price").isNotNull()
            & (col("stock_quantity") > 0)
            & (col("cost_price") > 0)
        )
        .withColumn("inventory_value", col("stock_quantity") * col("cost_price"))
        .agg(spark_sum("inventory_value").alias("value"))
        .collect()[0]["value"]
        or 0.0
    )

    # Average order processing time (days)
    global_metrics["avg_order_processing_days"] = (
        dataframes["orders"]
        .filter(
            col("order_placed_at").isNotNull() & col("order_shipped_at").isNotNull()
        )
        .withColumn(
            "processing_time",
            datediff(col("order_shipped_at"), to_date(col("order_placed_at"))),
        )
        .filter(col("processing_time") >= 0)
        .agg(spark_avg("processing_time").alias("value"))
        .collect()[0]["value"]
        or 0.0
    )

    # Create DataFrame from global metrics
    global_aggregations_data = [(k, float(v)) for k, v in global_metrics.items()]
    global_aggregations_df = spark.createDataFrame(
        global_aggregations_data, ["metric_name", "metric_value"]
    ).withColumn("calculated_at", lit("2025-11-14 16:34:39"))

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
