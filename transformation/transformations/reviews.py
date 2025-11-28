from pyspark.sql.functions import col, when


def transform_reviews(dataframes):
    dataframes["reviews"] = (
        dataframes["reviews"]
        .withColumns(
            {
                "review_sentiment": when(
                    col("rating").isNotNull() & (col("rating") != 0),
                    when(col("rating") >= 4, "Positive")
                    .when(col("rating") == 3, "Neutral")
                    .otherwise("Negative"),
                )
            }
        )
        .dropDuplicates(["review_id"])
    )
