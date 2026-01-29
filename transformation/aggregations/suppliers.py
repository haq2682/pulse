from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, countDistinct, sum as spark_sum, avg as spark_avg, count,
    when, lag, datediff, current_date, lit, coalesce
)

def aggregate_suppliers(dataframes):
    if "suppliers" not in dataframes or dataframes["suppliers"] is None or dataframes["suppliers"].count() == 0:
        return
    
    suppliers_df = dataframes["suppliers"]
    
    # Check if products table has supplier_id
    has_supplier_id_in_products = False
    if "products" in dataframes and dataframes["products"] is not None:
        has_supplier_id_in_products = dataframes["products"].filter(col("supplier_id").isNotNull()).count() > 0
    
    # Fallback: Check if inventory table has supplier_id
    has_supplier_id_in_inventory = False
    if not has_supplier_id_in_products and "inventory" in dataframes and dataframes["inventory"] is not None:
        inventory_df = dataframes["inventory"]
        if "supplier_id" in inventory_df.columns and "product_id" in inventory_df.columns:
            has_supplier_id_in_inventory = inventory_df.filter(col("supplier_id").isNotNull()).count() > 0
    
    has_supplier_id = has_supplier_id_in_products or has_supplier_id_in_inventory
    
    products_with_supplier = None
    if has_supplier_id_in_products:
        products_with_supplier = dataframes["products"].filter(col("supplier_id").isNotNull())
    elif has_supplier_id_in_inventory:
        # Create product-to-supplier mapping from inventory
        product_supplier_mapping = (
            dataframes["inventory"]
            .filter(col("supplier_id").isNotNull())
            .select("product_id", "supplier_id")
            .dropDuplicates(["product_id"])
        )
        # Enrich products with supplier_id from inventory
        products_with_supplier = (
            dataframes["products"]
            .drop("supplier_id")  # Drop null supplier_id column if exists
            .join(product_supplier_mapping, "product_id", "inner")
        )
    
    # Calculate total_products_supplied as all products a supplier sells (not just those appearing in order_items)
    total_products_supplied_df = None
    if has_supplier_id:
        total_products_supplied_df = (
            products_with_supplier
            .groupBy("supplier_id")
            .agg(countDistinct("product_id").alias("total_products_supplied"))
        )
        suppliers_df = suppliers_df.join(total_products_supplied_df, "supplier_id", "left")
    elif "inventory" in dataframes and dataframes["inventory"] is not None:
        inventory_df = dataframes["inventory"]
        if "supplier_id" in inventory_df.columns and "product_id" in inventory_df.columns:
            total_products_supplied_df = (
                inventory_df
                .groupBy("supplier_id")
                .agg(countDistinct("product_id").alias("total_products_supplied"))
            )
            suppliers_df = suppliers_df.join(total_products_supplied_df, "supplier_id", "left")

    # Sales and orders aggregation (without total_products_supplied)
    if has_supplier_id and "order_items" in dataframes and "orders" in dataframes:
        try:
            order_metrics = (
                dataframes["order_items"]
                .join(products_with_supplier.select("product_id", "supplier_id", "sell_price", "cost_price"), "product_id")
                .join(dataframes["orders"].select("order_id", "order_status"), "order_id")
                .withColumn("revenue", when((col("quantity") > 0) & (col("sell_price") > 0), col("quantity") * col("sell_price")))
                .withColumn("profit", col("sell_price") - col("cost_price"))
                .groupBy("supplier_id")
                .agg(
                    spark_sum(when(col("quantity") > 0, col("quantity"))).alias("total_units_sold"),
                    countDistinct("order_id").alias("total_orders_fulfilled"),
                    spark_sum(col("revenue")).alias("total_revenue_generated"),
                    spark_avg(when((col("sell_price") > 0) & (col("cost_price") >= 0), col("profit"))).alias("avg_profit_margin")
                )
            )
            suppliers_df = suppliers_df.join(order_metrics, "supplier_id", "left")
        except Exception as e:
            print(f"⚠️ Sales aggregation failed: {e}")
    
    # Reviews aggregation
    if has_supplier_id and "reviews" in dataframes:
        try:
            review_metrics = (
                dataframes["reviews"]
                .join(products_with_supplier.select("product_id", "supplier_id"), "product_id")
                .groupBy("supplier_id")
                .agg(
                    count("review_id").alias("total_reviews"),
                    spark_avg(when(col("rating") > 0, col("rating"))).alias("avg_product_rating")
                )
            )
            suppliers_df = suppliers_df.join(review_metrics, "supplier_id", "left")
        except Exception as e:
            print(f"⚠️ Review aggregation failed: {e}")
    
    # Inventory aggregation
    if has_supplier_id and "inventory" in dataframes:
        try:
            # Avoid ambiguous supplier_id by renaming before join
            products_with_supplier_inventory = products_with_supplier.select(
                col("product_id"),
                col("supplier_id").alias("supplier_id_products"),
                col("cost_price")
            )
            inventory_metrics = (
                dataframes["inventory"]
                .join(products_with_supplier_inventory, "product_id")
                .withColumn("stock_value", col("stock_quantity") * col("cost_price"))
                .groupBy("supplier_id_products")
                .agg(
                    spark_sum(when((col("stock_quantity") > 0) & (col("cost_price") > 0), col("stock_value"))).alias("total_stock_value"),
                    spark_avg(when(col("stock_quantity") >= 0, col("stock_quantity"))).alias("avg_stock_quantity")
                )
                .withColumnRenamed("supplier_id_products", "supplier_id")
            )
            suppliers_df = suppliers_df.join(inventory_metrics, "supplier_id", "left")
        except Exception as e:
            print(f"⚠️ Inventory aggregation failed: {e}")
    else:
        print("Skipping total_stock_value and avg_stock_quantity aggregation due to missing supplier_id in products or inventory.")
    
    # Restock lead time
    if has_supplier_id and "inventory" in dataframes:
        try:
            # Avoid ambiguous supplier_id by renaming before join
            products_with_supplier_restock = products_with_supplier.select(
                col("product_id"),
                col("supplier_id").alias("supplier_id_products")
            )
            restock_data = (
                dataframes["inventory"]
                .filter(col("last_restocked_date").isNotNull())
                .join(products_with_supplier_restock, "product_id")
                .select(col("supplier_id_products").alias("supplier_id"), "product_id", "last_restocked_date")
            )
            
            if restock_data.count() > 0:
                window_spec = Window.partitionBy("supplier_id", "product_id").orderBy("last_restocked_date")
                restock_metrics = (
                    restock_data
                    .withColumn("prev_date", lag("last_restocked_date").over(window_spec))
                    .withColumn("lead_time", datediff(col("last_restocked_date"), col("prev_date")))
                    .filter(col("lead_time").isNotNull())
                    .groupBy("supplier_id")
                    .agg(spark_avg("lead_time").alias("avg_restock_lead_time"))
                )
                suppliers_df = suppliers_df.join(restock_metrics, "supplier_id", "left")
        except Exception as e:
            print(f"⚠️ Restock aggregation failed: {e}")
    else:
        print("Skipping avg_restock_lead_time aggregation due to missing supplier_id in products or inventory.")
    
    # Derived columns
    derived = {}
    
    if "contract_end_date" in suppliers_df.columns:
        derived["contract_status_flag"] = when(
            col("contract_end_date").isNotNull(),
            when(col("contract_end_date") >= current_date(), "Active").otherwise("Expired")
        ).otherwise("Unknown")
        derived["days_until_contract_expiry"] = when(col("contract_end_date").isNotNull(), datediff(col("contract_end_date"), current_date()))
    
    if "contract_start_date" in suppliers_df.columns and "contract_end_date" in suppliers_df.columns:
        derived["contract_duration_days"] = when(
            col("contract_start_date").isNotNull() & col("contract_end_date").isNotNull(),
            datediff(col("contract_end_date"), col("contract_start_date"))
        )
    
    if "total_revenue_generated" in suppliers_df.columns and "total_products_supplied" in suppliers_df.columns:
        derived["revenue_per_product"] = when(
            col("total_revenue_generated").isNotNull() & (col("total_products_supplied") > 0),
            col("total_revenue_generated") / col("total_products_supplied")
        )
    
    if "total_revenue_generated" in suppliers_df.columns and "total_orders_fulfilled" in suppliers_df.columns:
        derived["avg_order_value"] = when(
            col("total_revenue_generated").isNotNull() & (col("total_orders_fulfilled") > 0),
            col("total_revenue_generated") / col("total_orders_fulfilled")
        )
    
    if "total_units_sold" in suppliers_df.columns and "total_products_supplied" in suppliers_df.columns:
        derived["avg_units_per_product"] = when(
            col("total_units_sold").isNotNull() & (col("total_products_supplied") > 0),
            col("total_units_sold") / col("total_products_supplied")
        )
    
    if "total_revenue_generated" in suppliers_df.columns and "supplier_rating" in suppliers_df.columns and "avg_product_rating" in suppliers_df.columns:
        derived["supplier_performance_score"] = when(
            col("supplier_rating").isNotNull() & col("avg_product_rating").isNotNull() & col("total_revenue_generated").isNotNull(),
            ((col("supplier_rating") / 5) * 0.4 + (col("avg_product_rating") / 5) * 0.3 + (col("total_revenue_generated") / 100000) * 0.3) * 100
        )
    
    if "total_stock_value" in suppliers_df.columns and "total_revenue_generated" in suppliers_df.columns:
        derived["stock_efficiency_ratio"] = when(
            (col("total_stock_value") > 0) & col("total_revenue_generated").isNotNull(),
            (col("total_revenue_generated") / col("total_stock_value")) * 100
        )
    
    if "supplier_rating" in suppliers_df.columns:
        derived["supplier_reliability_score"] = when(
            col("supplier_rating").isNotNull() & (col("is_verified") == lit(True)) & (col("is_preferred") == lit(True)),
            col("supplier_rating") * 1.2
        ).when(
            col("supplier_rating").isNotNull() & (col("is_verified") == lit(True)),
            col("supplier_rating") * 1.1
        ).otherwise(col("supplier_rating"))
    
    for col_name in ["total_products_supplied", "total_units_sold", "total_orders_fulfilled", "total_reviews"]:
        if col_name in suppliers_df.columns:
            derived[col_name] = coalesce(col(col_name), lit(0))
    
    for col_name in ["total_revenue_generated", "avg_product_rating", "total_stock_value"]:
        if col_name in suppliers_df.columns:
            derived[col_name] = coalesce(col(col_name), lit(0.0))
    
    if derived:
        suppliers_df = suppliers_df.withColumns(derived)
    
    dataframes["suppliers"] = suppliers_df