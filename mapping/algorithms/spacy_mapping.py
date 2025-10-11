import spacy
import re
from pyspark.sql.functions import lit


def preprocess_column_name(column):
    """
    Preprocess column name for spaCy.

    Args:
        column: Column name to preprocess

    Returns:
        List of lowercase tokens
    """
    words = "".join(c if c.isalnum() else " " for c in column).split()
    result = []
    for word in words:
        result.extend(filter(None, re.split("([A-Z][a-z]*)", word)))
    return [w.lower() for w in result if w]


def spacy_column_mapping(df, missing_cols, extra_cols, mapped_cols, threshold=0.87):
    """
    Map columns using spaCy word embeddings.

    Args:
        df: Spark DataFrame
        missing_cols: List of missing columns
        extra_cols: List of extra columns
        mapped_cols: Dictionary of mapped columns
        threshold: Minimum similarity score (0-1)

    Returns:
        Tuple of (df, missing_cols, extra_cols, mapped_cols)
    """
    nlp = spacy.load("en_core_web_md")

    for missing_col in missing_cols[:]:
        best_match = None
        best_score = threshold
        missing_doc = nlp(" ".join(preprocess_column_name(missing_col)))

        for extra_col in extra_cols[:]:
            extra_doc = nlp(" ".join(preprocess_column_name(extra_col)))
            similarity = missing_doc.similarity(extra_doc)

            if similarity > best_score:
                best_score = similarity
                best_match = extra_col

        if best_match:
            print(f"spaCy Mapping: {best_match} -> {missing_col}: {best_score:.2f}")
            mapped_cols[missing_col] = best_match
            missing_cols.remove(missing_col)
            extra_cols.remove(best_match)

    for new_col, old_col in mapped_cols.items():
        df = df.withColumnRenamed(old_col, new_col)
    df = df.drop(*extra_cols)
    for col in missing_cols:
        df = df.withColumn(col, lit(None))
    return df, missing_cols, extra_cols, mapped_cols
