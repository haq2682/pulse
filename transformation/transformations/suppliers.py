from pyspark.sql.functions import *

def transform_suppliers(dataframes):
    products_with_supplier = dataframes["products"].select("product_id", "supplier_id", "cost_price", "sell_price")
    order_items_with_supplier = dataframes["order_items"].join(products_with_supplier, "product_id", "left").join(dataframes["orders"].select("order_id", "order_status"), "order_id", "inner")

    supplier_sales_agg = order_items_with_supplier.filter(col("supplier_id").isNotNull()).groupBy("supplier_id").agg(
        countDistinct(when(col("product_id").isNotNull(), col("product_id"))).alias("total_products_supplied"),
        sum(when(col("quantity").isNotNull() & col("sell_price").isNotNull() & (col("quantity") > 0) & (col("sell_price") > 0), col("quantity") * col("sell_price"))).alias("total_revenue_generated"),
        sum(when(col("quantity").isNotNull() & (col("quantity") > 0), col("quantity"))).alias("total_units_sold"),
        countDistinct(when(col("order_id").isNotNull(), col("order_id"))).alias("total_orders_fulfilled"),
        avg(when(col("sell_price").isNotNull() & col("cost_price").isNotNull() & (col("sell_price") > 0) & (col("cost_price") >= 0), col("sell_price") - col("cost_price"))).alias("avg_profit_margin"),
    )

    supplier_rating_agg = dataframes["reviews"].join(dataframes["products"].select("product_id", "supplier_id"), "product_id", "inner").filter(col("supplier_id").isNotNull()).groupBy("supplier_id").agg(
        avg(when(col("rating").isNotNull() & (col("rating") > 0), col("rating"))).alias("avg_product_rating"),
        count("review_id").alias("total_reviews"),
    )

    dataframes["suppliers"] = dataframes["suppliers"].join(supplier_sales_agg, "supplier_id", "left").join(supplier_rating_agg, "supplier_id", "left")
