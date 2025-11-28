from pyspark.sql.window import Window

from pyspark.sql.functions import (
    col,
    countDistinct,
    coalesce,
    lit,
    when,
    row_number,
    expr,
    avg as spark_avg,
    count,
    sum as spark_sum,
)

def product_affinity(dataframes):
    order_products = (
        dataframes["order_items"]
        .filter(col("order_id").isNotNull() & col("product_id").isNotNull())
        .select("order_id", "product_id")
        .distinct()
    )
    product_pairs = (
        order_products.alias("a")
        .join(
            order_products.alias("b"),
            (col("a.order_id") == col("b.order_id"))
            & (
                col("a.product_id") < col("b.product_id")
            ),  # Avoid duplicates and self-pairs
            "inner",
        )
        .select(
            col("a.product_id").alias("product_a_id"),
            col("b.product_id").alias("product_b_id"),
            col("a.order_id"),
        )
    )
    product_affinity = product_pairs.groupBy("product_a_id", "product_b_id").agg(
        countDistinct("order_id").alias("co_occurrence_count")
    )
    total_orders = dataframes["orders"].count()
    product_occurrence = order_products.groupBy("product_id").agg(
        countDistinct("order_id").alias("product_order_count")
    )
    product_affinity = product_affinity.join(
        product_occurrence.select(
            col("product_id").alias("prod_a_id"),
            col("product_order_count").alias("product_a_count"),
        ),
        col("product_a_id") == col("prod_a_id"),
        "left",
    ).drop("prod_a_id")
    product_affinity = product_affinity.join(
        product_occurrence.select(
            col("product_id").alias("prod_b_id"),
            col("product_order_count").alias("product_b_count"),
        ),
        col("product_b_id") == col("prod_b_id"),
        "left",
    ).drop("prod_b_id")
    product_affinity = product_affinity.withColumns(
        {
            # Handle nulls first
            "co_occurrence_count": coalesce(col("co_occurrence_count"), lit(0)),
            "product_a_count": coalesce(col("product_a_count"), lit(0)),
            "product_b_count": coalesce(col("product_b_count"), lit(0)),
        }
    )
    product_affinity = product_affinity.withColumns(
        {
            # Support: P(A and B) = co-occurrence / total_orders
            "support": when(
                (col("co_occurrence_count") > 0) & (lit(total_orders) > 0),
                (col("co_occurrence_count") / lit(total_orders)) * 100,
            ).otherwise(lit(0.0)),
            # Confidence: P(B|A) = co-occurrence / product_a_count
            "confidence_a_to_b": when(
                (col("co_occurrence_count") > 0) & (col("product_a_count") > 0),
                (col("co_occurrence_count") / col("product_a_count")) * 100,
            ).otherwise(lit(0.0)),
            # Confidence: P(A|B) = co-occurrence / product_b_count
            "confidence_b_to_a": when(
                (col("co_occurrence_count") > 0) & (col("product_b_count") > 0),
                (col("co_occurrence_count") / col("product_b_count")) * 100,
            ).otherwise(lit(0.0)),
            # P(B) = product_b_count / total_orders
            "prob_b": when(
                (col("product_b_count") > 0) & (lit(total_orders) > 0),
                col("product_b_count") / lit(total_orders),
            ).otherwise(lit(0.0)),
            # P(A) = product_a_count / total_orders
            "prob_a": when(
                (col("product_a_count") > 0) & (lit(total_orders) > 0),
                col("product_a_count") / lit(total_orders),
            ).otherwise(lit(0.0)),
        }
    )
    product_affinity = product_affinity.withColumns(
        {
            # Lift: confidence / P(B)
            "lift_a_to_b": when(
                (col("confidence_a_to_b") > 0) & (col("prob_b") > 0),
                (col("confidence_a_to_b") / 100) / col("prob_b"),
            ).otherwise(lit(0.0)),
            # Lift: confidence / P(A)
            "lift_b_to_a": when(
                (col("confidence_b_to_a") > 0) & (col("prob_a") > 0),
                (col("confidence_b_to_a") / 100) / col("prob_a"),
            ).otherwise(lit(0.0)),
        }
    )
    product_affinity = product_affinity.withColumn(
        "avg_lift", (col("lift_a_to_b") + col("lift_b_to_a")) / lit(2)
    )
    product_affinity = (
        product_affinity.join(
            dataframes["products"].select(
                col("product_id").alias("prod_a_join_id"),
                col("product_name").alias("product_a_name"),
                col("category").alias("product_a_category"),
            ),
            col("product_a_id") == col("prod_a_join_id"),
            "left",
        )
        .drop("prod_a_join_id")
        .join(
            dataframes["products"].select(
                col("product_id").alias("prod_b_join_id"),
                col("product_name").alias("product_b_name"),
                col("category").alias("product_b_category"),
            ),
            col("product_b_id") == col("prod_b_join_id"),
            "left",
        )
        .drop("prod_b_join_id")
    )
    product_affinity = product_affinity.withColumns(
        {
            # Affinity strength based on lift
            "affinity_strength": when(col("avg_lift") >= 3.0, "Very Strong")
            .when(col("avg_lift") >= 2.0, "Strong")
            .when(col("avg_lift") >= 1.5, "Moderate")
            .when(col("avg_lift") >= 1.0, "Weak")
            .otherwise("No Affinity"),
            # Cross-category indicator
            "is_cross_category": when(
                col("product_a_category") != col("product_b_category"), lit(True)
            ).otherwise(lit(False)),
            # Affinity score (composite metric)
            "affinity_score": (
                # Support weight (30%)
                (col("support") / 10) * lit(0.3)
                +
                # Lift weight (50%)
                col("avg_lift") * lit(0.5)
                +
                # Frequency weight (20%)
                (col("co_occurrence_count") / 100) * lit(0.2)
            )
            * lit(10),
        }
    )
    product_affinity_filtered = product_affinity.filter(
        (col("co_occurrence_count") >= 3)  # At least 3 co-occurrences
        & (col("avg_lift") >= 1.0)  # Lift greater than random
    )
    dataframes["product_affinity"] = product_affinity_filtered
    top_product_pairs = product_affinity_filtered.orderBy(
        col("affinity_score").desc()
    ).limit(100)
    dataframes["top_product_pairs"] = top_product_pairs
    window_product_a = Window.partitionBy("product_a_id").orderBy(
        col("affinity_score").desc()
    )
    product_recommendations = (
        product_affinity_filtered.withColumn(
            "rank", row_number().over(window_product_a)
        )
        .filter(col("rank") <= 5)
        .groupBy("product_a_id", "product_a_name")
        .agg(
            expr("collect_list(product_b_name)").alias("recommended_products"),
            spark_avg("affinity_score").alias("avg_affinity_score"),
            count("product_b_id").alias("recommendation_count"),
        )
    )

    dataframes["product_recommendations"] = product_recommendations
    category_affinity = (
        product_affinity_filtered.groupBy("product_a_category", "product_b_category")
        .agg(
            count("*").alias("pair_count"),
            spark_avg("avg_lift").alias("avg_lift_between_categories"),
            spark_avg("support").alias("avg_support"),
            spark_sum("co_occurrence_count").alias("total_co_occurrences"),
        )
        .filter(col("product_a_category") != col("product_b_category"))
        .orderBy(col("avg_lift_between_categories").desc())
    )
    dataframes["category_affinity"] = category_affinity
