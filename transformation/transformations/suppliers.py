from pyspark.sql.functions import *


def transform_suppliers(dataframes):
    products_with_supplier = dataframes["products"].select(
        "product_id", "supplier_id", "cost_price", "sell_price"
    )

    # FIX: Join order_items with products to get sell_price, then aggregate
    order_items_with_price = dataframes["order_items"].join(
        dataframes["products"].select("product_id", "sell_price"), "product_id", "left"
    )

    order_items_agg = order_items_with_price.groupBy("product_id").agg(
        sum(col("quantity") * coalesce(col("sell_price"), lit(0))).alias(
            "product_revenue"
        ),
        sum("quantity").alias("units_sold"),
        countDistinct("order_id").alias("orders_count"),
    )

    supplier_sales_agg = (
        products_with_supplier.join(order_items_agg, "product_id", "left")
        .filter(col("supplier_id").isNotNull())
        .groupBy("supplier_id")
        .agg(
            countDistinct("product_id").alias("total_products_supplied"),
            sum(when(col("product_revenue").isNotNull(), col("product_revenue"))).alias(
                "total_revenue_generated"
            ),
            sum(when(col("units_sold").isNotNull(), col("units_sold"))).alias(
                "total_units_sold"
            ),
            sum(when(col("orders_count").isNotNull(), col("orders_count"))).alias(
                "total_orders_fulfilled"
            ),
            avg(
                when(
                    col("sell_price").isNotNull()
                    & col("cost_price").isNotNull()
                    & (col("sell_price") > 0)
                    & (col("cost_price") >= 0),
                    col("sell_price") - col("cost_price"),
                )
            ).alias("avg_profit_margin"),
        )
    )

    supplier_rating_agg = (
        dataframes["reviews"]
        .join(
            dataframes["products"].select("product_id", "supplier_id"),
            "product_id",
            "inner",
        )
        .filter(col("supplier_id").isNotNull())
        .groupBy("supplier_id")
        .agg(
            avg(
                when(col("rating").isNotNull() & (col("rating") > 0), col("rating"))
            ).alias("avg_product_rating"),
            count("review_id").alias("total_reviews"),
        )
    )

    dataframes["suppliers"] = (
        dataframes["suppliers"]
        .join(supplier_sales_agg, "supplier_id", "left")
        .join(supplier_rating_agg, "supplier_id", "left")
    )
