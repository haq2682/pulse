from rapidfuzz import fuzz, process
from pyspark.sql.functions import lit


def rapidfuzz_column_mapping(df, missing_cols, extra_cols, mapped_cols, threshold=85):
    """
    Map columns using RapidFuzz's string matching algorithms.

    Args:
        df: Spark DataFrame
        missing_cols: List of missing columns
        extra_cols: List of extra columns
        mapped_cols: Dictionary of mapped columns
        threshold: Minimum similarity score (0-100)

    Returns:
        Tuple of (df, missing_cols, extra_cols, mapped_cols)
    """
    for missing_col in missing_cols[:]:
        match = process.extractOne(
            missing_col,
            extra_cols,
            scorer=fuzz.ratio,
            score_cutoff=threshold,
        )
        if match:
            best_match, score = match[0], match[1]
            print(f"RapidFuzz Mapping: {best_match} -> {missing_col}: {score:.2f}")
            mapped_cols[missing_col] = best_match
            missing_cols.remove(missing_col)
            extra_cols.remove(best_match)

    for new_col, old_col in mapped_cols.items():
        df = df.withColumnRenamed(old_col, new_col)

    df = df.drop(*extra_cols)
    for col in missing_cols:
        df = df.withColumn(col, lit(None))

    return df, missing_cols, extra_cols, mapped_cols
