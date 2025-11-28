from pyspark.sql.window import Window

from pyspark.sql.functions import (
    col,
    countDistinct,
    sum as spark_sum,
    avg as spark_avg,
    count,
    when,
    lag,
    datediff,
    current_date,
    lit,
    coalesce,
)

def aggregate_suppliers(dataframes):
    products_with_supplier = dataframes["products"].select(
        "product_id", "supplier_id", "cost_price", "sell_price"
    )

    order_items_with_supplier = (
        dataframes["order_items"]
        .join(products_with_supplier, "product_id", "left")
        .join(
            dataframes["orders"].select("order_id", "order_status"), "order_id", "inner"
        )
    )
    supplier_sales_agg = (
        order_items_with_supplier.filter(col("supplier_id").isNotNull())
        .groupBy("supplier_id")
        .agg(
            # Total products supplied
            countDistinct(when(col("product_id").isNotNull(), col("product_id"))).alias(
                "total_products_supplied"
            ),
            # Total revenue generated (quantity * sell_price)
            spark_sum(
                when(
                    col("quantity").isNotNull()
                    & col("sell_price").isNotNull()
                    & (col("quantity") > 0)
                    & (col("sell_price") > 0),
                    col("quantity") * col("sell_price"),
                )
            ).alias("total_revenue_generated"),
            # Total units sold
            spark_sum(
                when(
                    col("quantity").isNotNull() & (col("quantity") > 0), col("quantity")
                )
            ).alias("total_units_sold"),
            # Total orders fulfilled
            countDistinct(when(col("order_id").isNotNull(), col("order_id"))).alias(
                "total_orders_fulfilled"
            ),
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
            # Average product rating
            spark_avg(
                when(col("rating").isNotNull() & (col("rating") > 0), col("rating"))
            ).alias("avg_product_rating"),
            # Total reviews
            count("review_id").alias("total_reviews"),
        )
    )
    supplier_inventory_agg = (
        dataframes["inventory"]
        .join(
            dataframes["products"].select(
                "product_id", col("supplier_id").alias("prod_supplier_id"), "cost_price"
            ),
            "product_id",
            "left",
        )
        .filter(col("prod_supplier_id").isNotNull())
        .groupBy("prod_supplier_id")
        .agg(
            # Total stock value (stock_quantity * cost_price)
            spark_sum(
                when(
                    col("stock_quantity").isNotNull()
                    & col("cost_price").isNotNull()
                    & (col("stock_quantity") > 0)
                    & (col("cost_price") > 0),
                    col("stock_quantity") * col("cost_price"),
                )
            ).alias("total_stock_value"),
            # Average stock quantity
            spark_avg(
                when(
                    col("stock_quantity").isNotNull() & (col("stock_quantity") >= 0),
                    col("stock_quantity"),
                )
            ).alias("avg_stock_quantity"),
        )
        .withColumnRenamed("prod_supplier_id", "supplier_id")
    )
    inventory_restock_supplier = (
        dataframes["inventory"]
        .select("product_id", "last_restocked_date")
        .join(
            dataframes["products"].select("product_id", "supplier_id"),
            "product_id",
            "left",
        )
        .filter(col("supplier_id").isNotNull() & col("last_restocked_date").isNotNull())
        .select("supplier_id", "product_id", "last_restocked_date")
    )
    window_supplier_restock = Window.partitionBy("supplier_id", "product_id").orderBy(
        "last_restocked_date"
    )

    supplier_restock_lead = (
        inventory_restock_supplier.withColumn(
            "prev_restock_date",
            lag("last_restocked_date", 1).over(window_supplier_restock),
        )
        .withColumn(
            "restock_lead_time",
            when(
                col("prev_restock_date").isNotNull(),
                datediff(col("last_restocked_date"), col("prev_restock_date")),
            ),
        )
        .filter(col("restock_lead_time").isNotNull())
        .groupBy("supplier_id")
        .agg(spark_avg("restock_lead_time").alias("avg_restock_lead_time"))
    )
    dataframes["suppliers"] = (
        dataframes["suppliers"]
        .join(supplier_sales_agg, "supplier_id", "left")
        .join(supplier_rating_agg, "supplier_id", "left")
        .join(supplier_inventory_agg, "supplier_id", "left")
        .join(supplier_restock_lead, "supplier_id", "left")
    )
    dataframes["suppliers"] = dataframes["suppliers"].withColumns(
        {
            # Contract status flag
            "contract_status_flag": when(
                col("contract_end_date").isNotNull(),
                when(col("contract_end_date") >= current_date(), "Active").otherwise(
                    "Expired"
                ),
            ).otherwise("Unknown"),
            # Days until contract expiry
            "days_until_contract_expiry": when(
                col("contract_end_date").isNotNull(),
                datediff(col("contract_end_date"), current_date()),
            ),
            # Contract duration in days
            "contract_duration_days": when(
                col("contract_start_date").isNotNull()
                & col("contract_end_date").isNotNull(),
                datediff(col("contract_end_date"), col("contract_start_date")),
            ),
            # Revenue per product
            "revenue_per_product": when(
                col("total_revenue_generated").isNotNull()
                & col("total_products_supplied").isNotNull()
                & (col("total_products_supplied") > 0),
                col("total_revenue_generated") / col("total_products_supplied"),
            ),
            # Average order value
            "avg_order_value": when(
                col("total_revenue_generated").isNotNull()
                & col("total_orders_fulfilled").isNotNull()
                & (col("total_orders_fulfilled") > 0),
                col("total_revenue_generated") / col("total_orders_fulfilled"),
            ),
            # Units per product
            "avg_units_per_product": when(
                col("total_units_sold").isNotNull()
                & col("total_products_supplied").isNotNull()
                & (col("total_products_supplied") > 0),
                col("total_units_sold") / col("total_products_supplied"),
            ),
            # Supplier performance score
            "supplier_performance_score": when(
                col("supplier_rating").isNotNull()
                & col("avg_product_rating").isNotNull()
                & col("total_revenue_generated").isNotNull(),
                (
                    # Supplier rating (40%)
                    (col("supplier_rating") / lit(5)) * lit(0.4)
                    +
                    # Product rating (30%)
                    (col("avg_product_rating") / lit(5)) * lit(0.3)
                    +
                    # Revenue contribution (30%)
                    (col("total_revenue_generated") / lit(100000)) * lit(0.3)
                )
                * lit(100),
            ),
            # Stock efficiency ratio
            "stock_efficiency_ratio": when(
                col("total_stock_value").isNotNull()
                & col("total_revenue_generated").isNotNull()
                & (col("total_stock_value") > 0),
                (col("total_revenue_generated") / col("total_stock_value")) * 100,
            ),
            # Supplier reliability score (based on ratings and status)
            "supplier_reliability_score": when(
                col("supplier_rating").isNotNull()
                & (col("is_verified") == True)
                & (col("is_preferred") == True),
                col("supplier_rating") * lit(1.2),
            )
            .when(
                col("supplier_rating").isNotNull() & (col("is_verified") == True),
                col("supplier_rating") * lit(1.1),
            )
            .otherwise(col("supplier_rating")),
            # Handle null values with defaults
            "total_products_supplied": coalesce(col("total_products_supplied"), lit(0)),
            "total_revenue_generated": coalesce(
                col("total_revenue_generated"), lit(0.0)
            ),
            "total_units_sold": coalesce(col("total_units_sold"), lit(0)),
            "total_orders_fulfilled": coalesce(col("total_orders_fulfilled"), lit(0)),
            "total_reviews": coalesce(col("total_reviews"), lit(0)),
            "avg_product_rating": coalesce(col("avg_product_rating"), lit(0.0)),
            "total_stock_value": coalesce(col("total_stock_value"), lit(0.0)),
        }
    )
