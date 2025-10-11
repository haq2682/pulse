from nltk.corpus import wordnet
from pyspark.sql.functions import lit
import re
from nltk.tokenize import word_tokenize


def preprocess_column_name(column):
    """
    Preprocess column name for semantic analysis.

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


def get_wordnet_synsets(word):
    """
    Get WordNet synsets for a word.

    Args:
        word: Word to look up

    Returns:
        List of synsets
    """
    return wordnet.synsets(word)


def calculate_semantic_similarity(missing_col, extra_col):
    """
    Calculate semantic similarity using WordNet.

    Args:
        missing_col: Missing column name
        extra_col: Extra column name

    Returns:
        Similarity score (0-1)
    """
    missing_tokens = preprocess_column_name(missing_col)
    extra_tokens = preprocess_column_name(extra_col)
    if not missing_tokens or not extra_tokens:
        return 0.0
    max_similarities = []
    for token1 in missing_tokens:
        synsets1 = get_wordnet_synsets(token1)
        if not synsets1:
            continue
        token_similarities = []
        for token2 in extra_tokens:
            synsets2 = get_wordnet_synsets(token2)
            if not synsets2:
                continue
            similarities = [
                s1.path_similarity(s2)
                for s1 in synsets1
                for s2 in synsets2
                if s1.path_similarity(s2) is not None
            ]
            if similarities:
                token_similarities.append(max(similarities))
        if token_similarities:
            max_similarities.append(max(token_similarities))
    return sum(max_similarities) / len(max_similarities) if max_similarities else 0.0


def semantic_column_mapping(df, missing_cols, extra_cols, mapped_cols, threshold=0.6):
    """
    Map columns using WordNet semantic similarity.

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
            similarity = calculate_semantic_similarity(missing_col, extra_col)
            if similarity > best_score:
                best_score = similarity
                best_match = extra_col
        if best_match:
            print(f"Wordnet Mapping: {best_match} -> {missing_col}: {best_score:.2f}")
            mapped_cols[missing_col] = best_match
            missing_cols.remove(missing_col)
            extra_cols.remove(best_match)

    for new_col, old_col in mapped_cols.items():
        df = df.withColumnRenamed(old_col, new_col)
    df = df.drop(*extra_cols)
    for col in missing_cols:
        df = df.withColumn(col, lit(None))
    return df, missing_cols, extra_cols, mapped_cols
