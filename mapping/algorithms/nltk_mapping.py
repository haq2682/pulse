import re
import nltk
from nltk.tokenize import word_tokenize
from nltk.metrics.distance import edit_distance
from difflib import SequenceMatcher
from pyspark.sql.functions import lit

# Download required NLTK data
nltk.download("punkt", quiet=True)
nltk.download("stopwords", quiet=True)
nltk.download("wordnet", quiet=True)
nltk.download("averaged_perceptron_tagger", quiet=True)
nltk.download("punkt_tab", quiet=True)


def preprocess_column_name(column):
    """
    Preprocess column name for comparison.

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


def jaccard_similarity(source_column, target_column):
    """
    Calculate Jaccard similarity between two column names.

    Args:
        source_column: Source column name
        target_column: Target column name

    Returns:
        Similarity score (0-1)
    """
    source_col = preprocess_column_name(source_column)
    target_col = preprocess_column_name(target_column)
    intersection = len(set(source_col).intersection(set(target_col)))
    union = len(set(source_col).union(set(target_col)))
    jaccard_similarity = intersection / union if union > 0 else 0
    return jaccard_similarity


def sequence_matching(source_column, target_column):
    """
    Calculate sequence similarity between two column names.

    Args:
        source_column: Source column name
        target_column: Target column name

    Returns:
        Similarity score (0-1)
    """
    source_col = preprocess_column_name(source_column)
    target_col = preprocess_column_name(target_column)
    return SequenceMatcher(None, source_col, target_col).ratio()


def editing_distance(source_column, target_column):
    """
    Calculate normalized edit distance between two column names.

    Args:
        source_column: Source column name
        target_column: Target column name

    Returns:
        Similarity score (0-1)
    """
    source_col = preprocess_column_name(source_column)
    target_col = preprocess_column_name(target_column)
    max_len = max(len(source_col), len(target_col))
    return 1 - (edit_distance(source_col, target_col) / max_len)


def mapping_with_nltk(df, missing_cols, extra_cols, mapped_cols, threshold=0.87):
    """
    Map columns using combined NLTK-based similarity metrics.

    Args:
        df: Spark DataFrame
        missing_cols: List of missing columns
        extra_cols: List of extra columns
        mapped_cols: Dictionary of mapped columns
        threshold: Minimum similarity score (0-1)

    Returns:
        Tuple of (df, missing_cols, extra_cols, mapped_cols)
    """
    for missing_col in missing_cols[:]:
        best_match = None
        best_score = threshold
        for extra_col in extra_cols[:]:
            final = (
                0.4 * jaccard_similarity(missing_col, extra_col)
                + 0.3 * sequence_matching(missing_col, extra_col)
                + 0.3 * editing_distance(missing_col, extra_col)
            )
            if final > best_score:
                best_score = final
                best_match = extra_col

        if best_match and best_score > threshold:
            print(f"NLTK Mapping: {best_match} -> {missing_col}: {best_score:.2f}")
            mapped_cols[missing_col] = best_match
            missing_cols.remove(missing_col)
            extra_cols.remove(best_match)

    for new_col, old_col in mapped_cols.items():
        df = df.withColumnRenamed(old_col, new_col)
    df = df.drop(*extra_cols)
    for col in missing_cols:
        df = df.withColumn(col, lit(None))
    return df, missing_cols, extra_cols, mapped_cols
