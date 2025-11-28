from pyspark.sql.functions import (
    col,
    when,
    greatest,
    unix_timestamp,
    to_timestamp,
    lit,
    floor,
)


def transform_customer_sessions(dataframes):
    dataframes["customer_sessions"] = dataframes["customer_sessions"].withColumns(
        {
            "session_duration_seconds": when(
                col("session_start").isNotNull() & col("session_end").isNotNull(),
                greatest(
                    unix_timestamp(to_timestamp(col("session_end")))
                    - unix_timestamp(to_timestamp(col("session_start"))),
                    lit(0),
                ),
            ),
            "session_duration_minutes": when(
                col("session_start").isNotNull() & col("session_end").isNotNull(),
                greatest(
                    floor(
                        (
                            unix_timestamp(to_timestamp(col("session_end")))
                            - unix_timestamp(to_timestamp(col("session_start")))
                        )
                        / 60
                    ),
                    lit(0),
                ),
            ),
            "session_duration_hours": when(
                col("session_start").isNotNull() & col("session_end").isNotNull(),
                greatest(
                    floor(
                        (
                            unix_timestamp(to_timestamp(col("session_end")))
                            - unix_timestamp(to_timestamp(col("session_start")))
                        )
                        / 3600
                    ),
                    lit(0),
                ),
            ),
            "pages_per_minute": when(
                col("session_duration_minutes").isNotNull()
                & (col("session_duration_minutes") != 0)
                & col("pages_viewed").isNotNull()
                & (col("pages_viewed") != 0),
                col("pages_viewed") / col("session_duration_minutes"),
            ),
            "products_per_page": when(
                col("pages_viewed").isNotNull()
                & (col("pages_viewed") != 0)
                & col("products_viewed").isNotNull()
                & (col("products_viewed") != 0),
                col("products_viewed") / col("pages_viewed"),
            ),
            "conversion_flag": when(
                col("conversion_flag").isNotNull(),
                when(
                    (col("conversion_flag") == "true")
                    | (col("conversion_flag") == "True"),
                    1,
                ).otherwise(0),
            ),
            "cart_abandonment_flag": when(
                col("cart_abandonment_flag").isNotNull(),
                when(
                    (col("cart_abandonment_flag") == "true")
                    | (col("cart_abandonment_flag") == "True"),
                    1,
                ).otherwise(0),
            ),
        }
    )
