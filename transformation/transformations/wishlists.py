from pyspark.sql.functions import col, when, greatest, unix_timestamp, to_timestamp, lit


def transform_wishlists(dataframes):
    dataframes["wishlist"] = (
        dataframes["wishlist"]
        .withColumns(
            {
                "wishlist_to_purchase_time": when(
                    col("added_date").isNotNull() & col("purchased_date").isNotNull(),
                    greatest(
                        unix_timestamp(to_timestamp(col("purchased_date")))
                        - unix_timestamp(to_timestamp(col("added_date"))),
                        lit(0),
                    ),
                ),
            }
        )
        .dropDuplicates(["wishlist_id"])
    )
