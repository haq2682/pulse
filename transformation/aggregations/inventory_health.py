from pyspark.sql.functions import (
    col,
    when,
    lit,
    coalesce,
    to_date,
    datediff,
    current_date,
    countDistinct,
    sum as spark_sum,
    avg as spark_avg,
    max as spark_max,
    expr,
)

def inventory_health_aggregations(dataframes):
    product_daily_sales = (
        dataframes["orders"]
        .filter(col("order_placed_at").isNotNull())
        .join(dataframes["order_items"], "order_id", "inner")
        .withColumn("order_date", to_date(col("order_placed_at")))
        .groupBy("product_id", "order_date")
        .agg(
            spark_sum(
                when(
                    col("quantity").isNotNull() & (col("quantity") > 0), col("quantity")
                )
            ).alias("daily_quantity")
        )
        .groupBy("product_id")
        .agg(spark_avg("daily_quantity").alias("avg_daily_sales"))
    )
    inventory_enhanced = (
        dataframes["inventory"]
        .join(
            dataframes["products"].select(
                col("product_id").alias("prod_product_id"),
                col("supplier_id").alias("prod_supplier_id"),
                col("cost_price").alias("prod_cost_price"),
            ),
            dataframes["inventory"]["product_id"] == col("prod_product_id"),
            "left",
        )
        .drop("prod_product_id")
        .withColumn(
            "supplier_id", coalesce(col("prod_supplier_id"), col("supplier_id"))
        )
        .withColumn("cost_price", coalesce(col("prod_cost_price"), lit(0.0)))
        .drop("prod_supplier_id", "prod_cost_price")
        .join(product_daily_sales, "product_id", "left")
    )
    product_inventory_health = (
        inventory_enhanced.filter(col("product_id").isNotNull())
        .groupBy("product_id")
        .agg(
            # Current stock (latest value)
            expr("last(stock_quantity)").alias("current_stock"),
            # Available stock (latest)
            expr("last(stock_quantity - reserved_quantity)").alias("available_stock"),
            # Average stock quantity
            spark_avg(
                when(
                    col("stock_quantity").isNotNull() & (col("stock_quantity") >= 0),
                    col("stock_quantity"),
                )
            ).alias("avg_stock_quantity"),
            # Reorder point breach count
            spark_sum(
                when(
                    col("stock_quantity").isNotNull()
                    & col("minimum_stock_level").isNotNull()
                    & (col("stock_quantity") < col("minimum_stock_level")),
                    lit(1),
                ).otherwise(lit(0))
            ).alias("reorder_point_breach_count"),
            # Stockout frequency
            spark_sum(
                when(
                    col("stock_quantity").isNotNull() & (col("stock_quantity") == 0),
                    lit(1),
                ).otherwise(lit(0))
            ).alias("stockout_frequency"),
            # Storage cost per unit (latest)
            expr("last(storage_cost / NULLIF(stock_quantity, 0))").alias(
                "storage_cost_per_unit"
            ),
            # Last restock date
            spark_max("last_restocked_date").alias("last_restock_date"),
            # Latest reserved quantity
            expr("last(reserved_quantity)").alias("reserved_quantity"),
            # Latest minimum stock level
            expr("last(minimum_stock_level)").alias("minimum_stock_level"),
            # Latest storage cost
            expr("last(storage_cost)").alias("storage_cost"),
            # Supplier ID (latest)
            expr("last(supplier_id)").alias("supplier_id"),
            # Cost price (latest)
            expr("last(cost_price)").alias("cost_price"),
            # Average daily sales
            expr("last(avg_daily_sales)").alias("avg_daily_sales"),
        )
    )
    product_cogs = (
        dataframes["order_items"]
        .filter(col("product_id").isNotNull())
        .groupBy("product_id")
        .agg(
            spark_sum(
                when(
                    col("quantity").isNotNull()
                    & col("product_cost").isNotNull()
                    & (col("quantity") > 0)
                    & (col("product_cost") > 0),
                    col("quantity") * col("product_cost"),
                )
            ).alias("total_cogs")
        )
    )
    product_inventory_health = product_inventory_health.join(
        product_cogs, "product_id", "left"
    )
    product_inventory_health = product_inventory_health.withColumns(
        {
            # Stock status
            "stock_status": when(col("current_stock") == 0, "Out of Stock")
            .when(col("current_stock") < col("minimum_stock_level"), "Low Stock")
            .when(col("current_stock") > (col("minimum_stock_level") * 3), "Overstock")
            .otherwise("Adequate"),
            # Days of supply
            "days_of_supply": when(
                col("current_stock").isNotNull()
                & col("avg_daily_sales").isNotNull()
                & (col("avg_daily_sales") > 0),
                col("current_stock") / col("avg_daily_sales"),
            ),
            # Inventory turnover ratio
            "inventory_turnover_ratio": when(
                col("total_cogs").isNotNull()
                & col("avg_stock_quantity").isNotNull()
                & (col("avg_stock_quantity") > 0),
                col("total_cogs") / col("avg_stock_quantity"),
            ),
            # Days since restock
            "days_since_restock": when(
                col("last_restock_date").isNotNull(),
                datediff(current_date(), col("last_restock_date")),
            ),
            # Stock health score
            "stock_health_score": when(
                col("current_stock").isNotNull()
                & col("minimum_stock_level").isNotNull(),
                when(col("stock_status") == "Adequate", lit(100))
                .when(col("stock_status") == "Overstock", lit(75))
                .when(col("stock_status") == "Low Stock", lit(50))
                .when(col("stock_status") == "Out of Stock", lit(0)),
            ),
            # Reorder urgency
            "reorder_urgency": when(col("stock_status") == "Out of Stock", "Critical")
            .when(
                (col("stock_status") == "Low Stock") & (col("days_of_supply") < 7),
                "High",
            )
            .when(
                (col("stock_status") == "Low Stock") & (col("days_of_supply") < 14),
                "Medium",
            )
            .otherwise("Low"),
            # Handle nulls
            "current_stock": coalesce(col("current_stock"), lit(0)),
            "available_stock": coalesce(col("available_stock"), lit(0)),
            "reorder_point_breach_count": coalesce(
                col("reorder_point_breach_count"), lit(0)
            ),
            "stockout_frequency": coalesce(col("stockout_frequency"), lit(0)),
            "storage_cost_per_unit": coalesce(col("storage_cost_per_unit"), lit(0.0)),
        }
    )
    dataframes["product_inventory_health"] = product_inventory_health
    supplier_inventory_health = (
        inventory_enhanced.filter(col("supplier_id").isNotNull())
        .groupBy("supplier_id")
        .agg(
            # Total products
            countDistinct("product_id").alias("total_products"),
            # Total current stock
            spark_sum(
                when(
                    col("stock_quantity").isNotNull() & (col("stock_quantity") >= 0),
                    col("stock_quantity"),
                )
            ).alias("total_current_stock"),
            # Total available stock
            spark_sum(
                when(
                    col("stock_quantity").isNotNull()
                    & col("reserved_quantity").isNotNull(),
                    col("stock_quantity") - col("reserved_quantity"),
                )
            ).alias("total_available_stock"),
            # Reorder point breaches
            spark_sum(
                when(
                    col("stock_quantity").isNotNull()
                    & col("minimum_stock_level").isNotNull()
                    & (col("stock_quantity") < col("minimum_stock_level")),
                    lit(1),
                ).otherwise(lit(0))
            ).alias("total_reorder_breaches"),
            # Stockout count
            spark_sum(
                when(
                    col("stock_quantity").isNotNull() & (col("stock_quantity") == 0),
                    lit(1),
                ).otherwise(lit(0))
            ).alias("total_stockouts"),
            # Total storage cost
            spark_sum(
                when(
                    col("storage_cost").isNotNull() & (col("storage_cost") > 0),
                    col("storage_cost"),
                )
            ).alias("total_storage_cost"),
            # Average stock quantity
            spark_avg(
                when(
                    col("stock_quantity").isNotNull() & (col("stock_quantity") >= 0),
                    col("stock_quantity"),
                )
            ).alias("avg_stock_per_product"),
            # Latest restock date
            spark_max("last_restocked_date").alias("last_restock_date"),
        )
    )
    supplier_inventory_health = supplier_inventory_health.withColumns(
        {
            # Stockout rate
            "stockout_rate": when(
                col("total_products") > 0,
                (col("total_stockouts") / col("total_products")) * 100,
            ),
            # Breach rate
            "breach_rate": when(
                col("total_products") > 0,
                (col("total_reorder_breaches") / col("total_products")) * 100,
            ),
            # Average storage cost per unit
            "avg_storage_cost_per_unit": when(
                col("total_current_stock") > 0,
                col("total_storage_cost") / col("total_current_stock"),
            ),
            # Days since last restock
            "days_since_last_restock": when(
                col("last_restock_date").isNotNull(),
                datediff(current_date(), col("last_restock_date")),
            ),
            # Supplier inventory health score
            "supplier_inventory_health_score": when(
                col("stockout_rate").isNotNull() & col("breach_rate").isNotNull(),
                (
                    # Low stockout rate (50%)
                    (lit(100) - col("stockout_rate")) * lit(0.5)
                    +
                    # Low breach rate (50%)
                    (lit(100) - col("breach_rate")) * lit(0.5)
                ),
            ),
            # Handle nulls
            "total_products": coalesce(col("total_products"), lit(0)),
            "total_current_stock": coalesce(col("total_current_stock"), lit(0)),
            "total_available_stock": coalesce(col("total_available_stock"), lit(0)),
            "total_reorder_breaches": coalesce(col("total_reorder_breaches"), lit(0)),
            "total_stockouts": coalesce(col("total_stockouts"), lit(0)),
            "total_storage_cost": coalesce(col("total_storage_cost"), lit(0.0)),
        }
    )
    dataframes["supplier_inventory_health"] = supplier_inventory_health
    dataframes["products"] = (
        dataframes["products"]
        .join(
            product_inventory_health.select(
                col("product_id").alias("inv_product_id"),
                col("current_stock").alias("inv_current_stock"),
                col("stock_status").alias("inv_stock_status"),
                col("days_of_supply").alias("inv_days_of_supply"),
                col("reorder_urgency").alias("inv_reorder_urgency"),
            ),
            dataframes["products"]["product_id"] == col("inv_product_id"),
            "left",
        )
        .drop(
            "inv_product_id",
            "current_stock",
            "stock_status",
            "days_of_supply",
            "reorder_urgency",
        )
        .withColumnRenamed("inv_current_stock", "current_stock")
        .withColumnRenamed("inv_stock_status", "stock_status")
        .withColumnRenamed("inv_days_of_supply", "days_of_supply")
        .withColumnRenamed("inv_reorder_urgency", "reorder_urgency")
    ).dropDuplicates(["product_id"])
    dataframes["suppliers"] = (
        dataframes["suppliers"]
        .join(
            supplier_inventory_health.select(
                col("supplier_id").alias("inv_supplier_id"),
                col("total_stockouts").alias("inv_total_stockouts"),
                col("stockout_rate").alias("inv_stockout_rate"),
                col("supplier_inventory_health_score").alias(
                    "inv_supplier_health_score"
                ),
            ),
            dataframes["suppliers"]["supplier_id"] == col("inv_supplier_id"),
            "left",
        )
        .drop(
            "inv_supplier_id",
            "total_stockouts",
            "stockout_rate",
            "supplier_inventory_health_score",
        )
        .withColumnRenamed("inv_total_stockouts", "total_stockouts")
        .withColumnRenamed("inv_stockout_rate", "stockout_rate")
        .withColumnRenamed(
            "inv_supplier_health_score", "supplier_inventory_health_score"
        )
    ).dropDuplicates(["supplier_id"])
