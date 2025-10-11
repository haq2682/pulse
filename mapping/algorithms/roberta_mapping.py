from sentence_transformers import SentenceTransformer, util
import torch
from pyspark.sql.functions import lit

# Load model globally to avoid reloading
roberta = SentenceTransformer("all-MiniLM-L12-v2")


def roberta_similarity(df, missing_cols, extra_cols, mapped_cols, threshold=0.87):
    """
    Map columns using RoBERTa (BERT) embeddings.

    Args:
        df: Spark DataFrame
        missing_cols: List of missing columns
        extra_cols: List of extra columns
        mapped_cols: Dictionary of mapped columns
        threshold: Minimum similarity score (0-1)

    Returns:
        Tuple of (df, missing_cols, extra_cols, mapped_cols)
    """
    missing_embeddings = roberta.encode(missing_cols, convert_to_tensor=True)
    extra_embeddings = roberta.encode(extra_cols, convert_to_tensor=True)

    for i, missing_col in enumerate(missing_cols):
        sims = util.cos_sim(missing_embeddings[i], extra_embeddings)[0]
        best_score, best_idx = torch.max(sims, dim=0)
        best_score = best_score.item()
        best_match = extra_cols[best_idx]

        if best_score >= threshold:
            print(
                f"BERT Mapping: {best_match} -> {missing_col} (score={best_score:.2f})"
            )
            mapped_cols[missing_col] = best_match
            missing_cols.remove(missing_col)
            extra_cols.remove(best_match)

    for schema_col, df_col in mapped_cols.items():
        df = df.withColumnRenamed(df_col, schema_col)
    df = df.drop(*extra_cols)
    for col in missing_cols:
        df = df.withColumn(col, lit(None))
    return df, missing_cols, extra_cols, mapped_cols
