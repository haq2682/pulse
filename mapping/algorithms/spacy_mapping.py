import logging
import spacy
import re
from pyspark.sql.functions import lit


logger = logging.getLogger(__name__)
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

# Module-level singleton, loaded once per process instead of once per call
# (spacy_column_mapping runs once per table - customers, orders, addresses,
# etc. - and previously reloaded the full model from disk every single time).
# disable=[...] drops the tagger/parser/ner/lemmatizer/attribute_ruler
# components - the only thing this file ever calls is doc.similarity(),
# which only needs the tokenizer and word vectors. Those disabled
# components are the majority of en_core_web_md's memory footprint and were
# never used - verified live: the mapping subprocess (shares pulse-api's
# container memory cgroup with uvicorn - see deployment.yaml's resources
# comment) was OOM-killed by the kernel loading the full pipeline on top of
# whatever the earlier algorithms in the same run (nltk, wordnet, roberta)
# had already left resident.
_NLP = spacy.load(
    "en_core_web_md",
    disable=["tagger", "parser", "ner", "lemmatizer", "attribute_ruler"],
)


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
    nlp = _NLP

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
            logger.info(
                "spaCy mapping | source=%s target=%s score=%.2f%%",
                best_match,
                missing_col,
                best_score * 100,
            )
            mapped_cols[missing_col] = best_match
            missing_cols.remove(missing_col)
            extra_cols.remove(best_match)

    for new_col, old_col in mapped_cols.items():
        df = df.withColumnRenamed(old_col, new_col)
    df = df.drop(*extra_cols)
    for col in missing_cols:
        df = df.withColumn(col, lit(None).cast("string"))
    return df, missing_cols, extra_cols, mapped_cols
