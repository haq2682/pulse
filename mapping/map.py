import os
import sys
import pandas as pd
import findspark

findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import List as mapping_list
import psycopg2
from io import BytesIO
from minio import Minio
from dotenv import load_dotenv, find_dotenv

from algorithms.rapidfuzz_mapping import rapidfuzz_column_mapping
from algorithms.nltk_mapping import mapping_with_nltk
from algorithms.wordnet_mapping import semantic_column_mapping
from algorithms.spacy_mapping import spacy_column_mapping
from algorithms.word2vec_mapping import word2vec_column_mapping
from algorithms.roberta_mapping import roberta_similarity
from algorithms.gpt_mapping import gpt_schema_mapping
from utils.file_loader import load_all_files_from_minio, load_file_from_minio
from utils.helpers import (
    normalize_name,
    get_table_name,
    safe_serialize,
    make_json_safe,
    detect_table,
    split_unified_dataframe,
)

load_dotenv(find_dotenv())

minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False,
)

bucket_name = "pulse-bucket-1"

spark = (
    SparkSession.builder.appName("NormalizeData")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("inferSchema", "true")
    .config("mergeSchema", "true")
    .getOrCreate()
)

df_to_table = {
    "customers_df": "customers",
    "addresses_df": "addresses",
    "products_df": "products",
    "inventories_df": "inventory",
    "orders_df": "orders",
    "reviews_df": "reviews",
    "categories_df": "categories",
    "wishlists_df": "wishlist",
    "payments_df": "payments",
    "order_items_df": "order_items",
    "shopping_carts_df": "shopping_cart",
    "customer_sessions_df": "customer_sessions",
    "marketing_campaigns_df": "marketing_campaigns",
    "suppliers_df": "suppliers",
}


def normalize_dataframe(df, column_variants, mapped_cols):
    """
    Normalize dataframe columns using predefined variants.

    Args:
        df: Spark DataFrame to normalize
        column_variants: Dictionary mapping standard columns to variants
        mapped_cols: Dictionary to track column mappings

    Returns:
        Tuple of (normalized_df, extra_df, extra_cols, missing_cols, mapped_cols)
    """
    variant_to_standard = {
        v.lower(): std_col
        for std_col, variants in column_variants.items()
        for v in variants
    }

    new_columns = []
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in variant_to_standard:
            std_col = variant_to_standard[col_lower]
            new_columns.append(std_col)
            mapped_cols[std_col] = col
        else:
            new_columns.append(col)

    for old_col, new_col in zip(df.columns, new_columns):
        df = df.withColumnRenamed(old_col, new_col)

    missing_cols = []
    for std_col in column_variants.keys():
        if std_col not in df.columns:
            df = df.withColumn(std_col, lit(None))
            missing_cols.append(std_col)

    schema_cols = list(column_variants.keys())
    extra_cols = [c for c in df.columns if c not in schema_cols]

    new_df = df.select(schema_cols)
    df_extra = df.select(schema_cols + extra_cols)

    return new_df, df_extra, extra_cols, missing_cols, mapped_cols


def mapping(df, column_variants, mapped):
    """
    Apply multiple mapping algorithms in sequence to normalize DataFrame columns.

    Args:
        df: Spark DataFrame to map
        column_variants: Dictionary of column variants
        mapped: Initial mapping dictionary

    Returns:
        Tuple of (normalized_df, extra_df, extra_cols, missing_cols, mapped_cols)
    """
    new_df, extra_df, extra_cols, missing_cols, mapped_cols = normalize_dataframe(
        df, column_variants, mapped
    )

    if missing_cols:
        print("\nAfter Initial Normalization:")
        print(f"Missing columns: {missing_cols}")
        print(new_df.columns)
        print("Implementing RapidFuzz Mapping...")
        new_df, missing_cols, extra_cols, mapped_cols = rapidfuzz_column_mapping(
            df, missing_cols, extra_cols, mapped_cols, threshold=70
        )

    if missing_cols:
        print("\nAfter RapidFuzz Mapping:")
        print(f"Missing columns: {missing_cols}")
        print(new_df.columns)
        print("Implementing NLTK Combination Mapping...")
        new_df, missing_cols, extra_cols, mapped_cols = mapping_with_nltk(
            df, missing_cols, extra_cols, mapped_cols, threshold=0.7
        )

    if missing_cols:
        print("\nAfter NLTK Combination Mapping:")
        print(f"Missing columns: {missing_cols}")
        print(new_df.columns)
        print("Implementing WordNet Semantic Mapping...")
        new_df, missing_cols, extra_cols, mapped_cols = semantic_column_mapping(
            df, missing_cols, extra_cols, mapped_cols, threshold=0.7
        )

    if missing_cols:
        print("\nAfter WordNet Semantic Mapping:")
        print(f"Missing columns: {missing_cols}")
        print(new_df.columns)
        print("Implementing spaCy Mapping...")
        new_df, missing_cols, extra_cols, mapped_cols = spacy_column_mapping(
            df, missing_cols, extra_cols, mapped_cols, threshold=0.7
        )

    if missing_cols:
        print("\nAfter spaCy Mapping:")
        print(f"Missing columns: {missing_cols}")

        print("Implementing Word2Vec Mapping...")
        new_df, missing_cols, extra_cols, mapped_cols = word2vec_column_mapping(
            df, extra_df, missing_cols, extra_cols, mapped_cols
        )

    if missing_cols:
        print("\nAfter Word2Vec Mapping:")
        print(f"Missing columns: {missing_cols}")
        print(new_df.columns)
        print("Implementing BERT Mapping...")
        new_df, missing_cols, extra_cols, mapped_cols = roberta_similarity(
            df, missing_cols, extra_cols, mapped_cols, threshold=0.7
        )

    if missing_cols:
        print("\nAfter roBERTa Mapping:")
        print(f"Missing columns: {missing_cols}")
        print(new_df.columns)
        print("Implementing GPT Mapping...")
        new_df, missing_cols, extra_cols, mapped_cols = gpt_schema_mapping(
            df, missing_cols, extra_cols, mapped_cols
        )

    return new_df, extra_df, extra_cols, missing_cols, mapped_cols


def process_all_dataframes(all_dataframes, columns_info, mapping_list, mode="batch"):
    """
    Unified processor for schema mapping.

    Supports:
    1. Single unified file (all tables in one DataFrame).
    2. Multiple files (one or more tables per file).
    3. Streaming (micro-batches, API, DB source).

    Parameters
    ----------
    all_dataframes : dict
        Dict of {df_name: dataframe} from files, unified table, or stream.
    columns_info : list
        List of (table_name, column_name, data_type) from PostgreSQL schema.
    mapping_list : object
        Object containing mapping_dict_<table> for each table.
    mode : str
        "batch" for files (single or multiple), "stream" for streaming.

    Returns
    -------
    results : dict
        Dict with results per canonical table.
    """

    df_to_mapping_dict = {
        "customer_df": mapping_list.mapping_dict_customers,
        "product_df": mapping_list.mapping_dict_products,
        "inventory_df": mapping_list.mapping_dict_inventory,
        "orders_df": mapping_list.mapping_dict_orders,
        "reviews_df": mapping_list.mapping_dict_reviews,
        "wishlist_df": mapping_list.mapping_dict_wishlist,
        "payments_df": mapping_list.mapping_dict_payments,
        "order_items_df": mapping_list.mapping_dict_order_items,
        "shopping_cart_df": mapping_list.mapping_dict_shopping_cart,
        "customer_sessions_df": mapping_list.mapping_dict_customer_sessions,
        "marketing_campaigns_df": mapping_list.mapping_dict_marketing_campaigns,
        "suppliers_df": mapping_list.mapping_dict_suppliers,
    }

    results = {}

    # Iterate over incoming dataframes
    for df_name, df in all_dataframes.items():
        print(f"\n{'='*50}")
        print(f"Incoming dataframe: {df_name}")

        if mode == "stream":
            detected_table = detect_table(df, columns_info)
            if detected_table:
                split_dfs = {detected_table: df}
            else:
                print(f"⚠️ Could not detect schema for streaming dataframe {df_name}")
                continue
        else:
            if df_name in df_to_table:
                split_dfs = {df_to_table[df_name]: df}
            else:
                split_dfs = split_unified_dataframe(df, columns_info)

        for table_name, sub_df in split_dfs.items():
            mapping_dict = getattr(mapping_list, f"mapping_dict_{table_name}", None)
            if not mapping_dict:
                print(f"⚠️ No mapping dict found for {table_name}. Skipping.")
                continue

            mapped = {col: "" for t, col, _ in columns_info if t == table_name}
            print(f"Processing → {table_name} with {len(mapped)} canonical columns")

            final_df, extra_df, extra_cols, missing_cols, mapped_cols = mapping(
                sub_df, mapping_dict, mapped
            )

            # Sanitize lists before putting them into results
            results[f"{df_name}__{table_name}"] = {
                "table_name": table_name,
                "final_df": final_df,  # keep Spark DF
                "extra_df": extra_df,  # keep Spark DF
                "extra_cols": [safe_serialize(c) for c in extra_cols],
                "missing_cols": [safe_serialize(c) for c in missing_cols],
                "mapped_cols": [safe_serialize(c) for c in mapped_cols],
            }

            print(f"✅ Completed {df_name} → {table_name}")
            print(f"   Missing cols: {missing_cols}")
            print(f"   Extra cols: {extra_cols}")
            print("   Preview:")
            final_df.show(3)

    return results


def save_dataframes_to_minio(results, client, bucket_name):
    """
    Save processed DataFrames to MinIO bucket.

    Args:
        results: Dictionary of processed results
        client: MinIO client instance
        bucket_name: Name of the bucket to save to
    """
    client = Minio(
        "localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False,
    )

    # bucket_name = "mapped"

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Created bucket: {bucket_name}")
    else:
        print(f"Bucket already exists: {bucket_name}")

    for result_key, result_data in results.items():
        table_name = result_data["table_name"]
        final_df = result_data["final_df"]

        print(f"Saving {table_name} to MinIO...")

        # Convert Spark DataFrame to Pandas
        pdf = final_df.toPandas()

        # Save as CSV to MinIO
        csv_buffer = BytesIO()
        pdf.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        # Upload to MinIO
        file_name = "mapped_" + table_name + ".csv"
        minio_client.put_object(
            bucket_name,
            file_name,
            csv_buffer,
            length=len(csv_buffer.getvalue()),
            content_type="text/csv",
        )

        print(f"✅ Saved {file_name} ({len(pdf)} rows)")
        csv_buffer.close()


if __name__ == "__main__":
    all_dataframes = load_all_files_from_minio(minio_client, bucket_name, spark)

    conn = psycopg2.connect(
        host="localhost",
        database=os.getenv("POSTGRES_DATABASE_NAME"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )

    cur = conn.cursor()
    cur.execute(
        """
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
    """
    )

    columns_info = cur.fetchall()
    cur.close()
    conn.close()

    results = process_all_dataframes(all_dataframes, columns_info, mapping_list)
    save_dataframes_to_minio(results, minio_client, bucket_name)

    print("\n" + "=" * 50)
    print("Processing complete!")
    print(f"Total tables processed: {len(results)}")
    spark.stop()
