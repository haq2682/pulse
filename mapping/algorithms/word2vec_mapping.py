import numpy as np
from gensim.models import KeyedVectors
from gensim.models import Word2Vec
import re
import uuid
from pyspark.sql.functions import lit


def preprocess_column_name(column):
    """
    Preprocess column name for Word2Vec.

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


def load_word2vec_model(df, extra_df):
    """
    Load or train Word2Vec model.

    Args:
        df: Primary DataFrame
        extra_df: Extra DataFrame with additional columns

    Returns:
        Word2Vec model
    """
    all_columns = list(set(df.columns + extra_df.columns))

    try:
        model = KeyedVectors.load_word2vec_format(
            "GoogleNews-vectors-negative300.bin", binary=True
        )
    except:
        sentences = [preprocess_column_name(col) for col in all_columns]
        model = Word2Vec(sentences, vector_size=100, window=5, min_count=1)
        model = model.wv
    return model


def calculate_word2vec_similarity(col1, col2, model):
    """
    Calculate Word2Vec similarity between two columns.

    Args:
        col1: First column name
        col2: Second column name
        model: Word2Vec model

    Returns:
        Similarity score (0-1)
    """
    words1 = preprocess_column_name(col1)
    words2 = preprocess_column_name(col2)

    if not words1 or not words2:
        return 0.0
    vec1 = []
    vec2 = []

    for word in words1:
        try:
            vec1.append(model[word])
        except KeyError:
            continue

    for word in words2:
        try:
            vec2.append(model[word])
        except KeyError:
            continue

    if not vec1 or not vec2:
        return 0.0

    vec1_avg = np.mean(vec1, axis=0)
    vec2_avg = np.mean(vec2, axis=0)

    similarity = np.dot(vec1_avg, vec2_avg) / (
        np.linalg.norm(vec1_avg) * np.linalg.norm(vec2_avg)
    )
    return float(similarity)


def word2vec_column_mapping(
    df, extra_df, missing_cols, extra_cols, mapped_cols, threshold=0.87
):
    """
    Map columns using Word2Vec embeddings.

    Args:
        df: Spark DataFrame
        extra_df: Extra DataFrame with additional columns
        missing_cols: List of missing columns
        extra_cols: List of extra columns
        mapped_cols: Dictionary of mapped columns
        threshold: Minimum similarity score (0-1)

    Returns:
        Tuple of (df, missing_cols, extra_cols, mapped_cols)
    """
    model = load_word2vec_model(df, extra_df)

    for missing_col in missing_cols[:]:
        best_match = None
        best_score = threshold

        for extra_col in extra_cols[:]:
            similarity = calculate_word2vec_similarity(missing_col, extra_col, model)
            if similarity > best_score:
                best_score = similarity
                best_match = extra_col

        if best_match:
            print(f"Word2Vec Mapping: {best_match} -> {missing_col}: {best_score:.2f}")
            mapped_cols[missing_col] = best_match
            missing_cols.remove(missing_col)
            extra_cols.remove(best_match)

    for new_col, old_col in mapped_cols.items():
        df = df.withColumnRenamed(old_col, new_col)
    df = df.drop(*extra_cols)
    for col in missing_cols:
        df = df.withColumn(col, lit(None))
    return df, missing_cols, extra_cols, mapped_cols
