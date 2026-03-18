import os
import sys
import re

# ---------------------------------------------------------------------------
# JAR paths — set BEFORE pyspark is imported so the JVM gateway starts with
# the correct driver classpath (Delta Lake + S3A).
# ---------------------------------------------------------------------------
_JARS_DIR = "/app/jars"
_MAP_JARS = [
    f"{_JARS_DIR}/hadoop-aws-3.3.4.jar",
    f"{_JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar",
    f"{_JARS_DIR}/delta-spark_2.12-3.0.0.jar",
    f"{_JARS_DIR}/delta-storage-3.0.0.jar",
]
_MAP_CP = ":".join(_MAP_JARS)

if "PYSPARK_SUBMIT_ARGS" not in os.environ:
    # Only the driver-side classpath.  If running in cluster mode the executor
    # (spark-master-py310 image) already has these JARs at
    # /opt/spark/external-jars/.  Passing --jars here would re-upload them,
    # producing duplicate class definitions and ClassCastException on Scala
    # collection types at runtime.
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f"--driver-class-path {_MAP_CP} pyspark-shell"
    )

# findspark is only needed when Spark is installed as a standalone binary
# (SPARK_HOME is set). pip-installed PySpark bundles py4j itself.
if os.environ.get("SPARK_HOME"):
    import findspark
    findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
import List as mapping_list
import pandas as pd
from io import BytesIO, StringIO
from minio import Minio
from dotenv import load_dotenv, find_dotenv
from algorithms.rapidfuzz_mapping import rapidfuzz_column_mapping
from algorithms.nltk_mapping import mapping_with_nltk
from algorithms.wordnet_mapping import semantic_column_mapping
from algorithms.spacy_mapping import spacy_column_mapping
from algorithms.word2vec_mapping import word2vec_column_mapping
from algorithms.roberta_mapping import roberta_similarity
from algorithms.gpt_mapping import gpt_schema_mapping
from utils.file_loader import load_all_files_from_minio
from utils.helpers import (
    safe_serialize,
    detect_table,
    split_unified_dataframe,
    parse_minio_endpoint,
)
from utils.table_mapper import map_table_name
import argparse

load_dotenv(find_dotenv())

# Hardcoded columns_info from canonical_schema.sql
# Format: List of (table_name, column_name, data_type) tuples
COLUMNS_INFO = [
    # addresses table
    ("addresses", "address_id", "character varying"),
    ("addresses", "city", "character varying"),
    ("addresses", "state_province", "character varying"),
    ("addresses", "postal_code", "character varying"),
    ("addresses", "country", "character varying"),
    # customers table
    ("customers", "customer_id", "character varying"),
    ("customers", "gender", "character varying"),
    ("customers", "date_of_birth", "date"),
    ("customers", "account_status", "character varying"),
    ("customers", "address_id", "character varying"),
    ("customers", "city", "character varying"),
    ("customers", "state_province", "character varying"),
    ("customers", "postal_code", "character varying"),
    ("customers", "country", "character varying"),
    ("customers", "account_created_at", "timestamp without time zone"),
    ("customers", "last_login_date", "timestamp without time zone"),
    ("customers", "is_active", "boolean"),
    # suppliers table
    ("suppliers", "supplier_id", "character varying"),
    ("suppliers", "supplier_rating", "numeric"),
    ("suppliers", "supplier_status", "character varying"),
    ("suppliers", "is_preferred", "boolean"),
    ("suppliers", "is_verified", "boolean"),
    ("suppliers", "contract_start_date", "date"),
    ("suppliers", "contract_end_date", "date"),
    ("suppliers", "city", "character varying"),
    ("suppliers", "state", "character varying"),
    ("suppliers", "zip_code", "character varying"),
    ("suppliers", "country", "character varying"),
    # categories table
    ("categories", "category_id", "character varying"),
    ("categories", "category_name", "character varying"),
    ("categories", "sub_category", "character varying"),
    # products table
    ("products", "product_id", "character varying"),
    ("products", "product_name", "character varying"),
    ("products", "sku", "character varying"),
    ("products", "category_id", "character varying"),
    ("products", "category", "character varying"),
    ("products", "sub_category", "character varying"),
    ("products", "brand", "character varying"),
    ("products", "supplier_id", "character varying"),
    ("products", "cost_price", "numeric"),
    ("products", "sell_price", "numeric"),
    ("products", "launch_date", "date"),
    ("products", "weight", "numeric"),
    ("products", "dimensions", "character varying"),
    ("products", "color", "character varying"),
    ("products", "size", "character varying"),
    ("products", "material", "character varying"),
    # inventory table
    ("inventory", "inventory_id", "character varying"),
    ("inventory", "product_id", "character varying"),
    ("inventory", "supplier_id", "character varying"),
    ("inventory", "stock_quantity", "integer"),
    ("inventory", "reserved_quantity", "integer"),
    ("inventory", "minimum_stock_level", "integer"),
    ("inventory", "last_restocked_date", "timestamp without time zone"),
    ("inventory", "storage_cost", "numeric"),
    ("inventory", "stock_status", "character varying"),
    # wishlist table
    ("wishlist", "wishlist_id", "character varying"),
    ("wishlist", "customer_id", "character varying"),
    ("wishlist", "product_id", "character varying"),
    ("wishlist", "added_date", "timestamp without time zone"),
    ("wishlist", "purchased_date", "timestamp without time zone"),
    ("wishlist", "removed_date", "timestamp without time zone"),
    # shopping_cart table
    ("shopping_cart", "cart_id", "character varying"),
    ("shopping_cart", "customer_id", "character varying"),
    ("shopping_cart", "session_id", "character varying"),
    ("shopping_cart", "cart_status", "character varying"),
    ("shopping_cart", "created_at", "timestamp without time zone"),
    ("shopping_cart", "updated_at", "timestamp without time zone"),
    # cart_items table
    ("cart_items", "cart_item_id", "bigint"),
    ("cart_items", "cart_id", "character varying"),
    ("cart_items", "product_id", "character varying"),
    ("cart_items", "quantity", "integer"),
    ("cart_items", "unit_price", "numeric"),
    ("cart_items", "total_price", "numeric"),
    ("cart_items", "added_at", "timestamp without time zone"),
    ("cart_items", "updated_at", "timestamp without time zone"),
    ("cart_items", "item_status", "character varying"),
    # orders table
    ("orders", "order_id", "character varying"),
    ("orders", "customer_id", "character varying"),
    ("orders", "order_status", "character varying"),
    ("orders", "subtotal", "numeric"),
    ("orders", "tax_amount", "numeric"),
    ("orders", "shipping_cost", "numeric"),
    ("orders", "total_discount", "numeric"),
    ("orders", "total_amount", "numeric"),
    ("orders", "currency", "character varying"),
    ("orders", "order_placed_at", "timestamp without time zone"),
    ("orders", "order_shipped_at", "timestamp without time zone"),
    ("orders", "order_delivered_at", "timestamp without time zone"),
    # order_items table
    ("order_items", "order_item_id", "character varying"),
    ("order_items", "order_id", "character varying"),
    ("order_items", "product_id", "character varying"),
    ("order_items", "quantity", "integer"),
    ("order_items", "discount_amount", "numeric"),
    ("order_items", "product_price", "numeric"),
    # payments table
    ("payments", "payment_id", "character varying"),
    ("payments", "order_id", "character varying"),
    ("payments", "payment_method", "character varying"),
    ("payments", "payment_provider", "character varying"),
    ("payments", "payment_status", "character varying"),
    ("payments", "transaction_id", "character varying"),
    ("payments", "processing_fee", "numeric"),
    ("payments", "refund_amount", "numeric"),
    ("payments", "refund_date", "timestamp without time zone"),
    ("payments", "payment_date", "timestamp without time zone"),
    # reviews table
    ("reviews", "review_id", "character varying"),
    ("reviews", "product_id", "character varying"),
    ("reviews", "customer_id", "character varying"),
    ("reviews", "rating", "integer"),
    ("reviews", "review_title", "character varying"),
    ("reviews", "review_desc", "text"),
    ("reviews", "review_date", "timestamp without time zone"),
    # marketing_campaigns table
    ("marketing_campaigns", "campaign_id", "character varying"),
    ("marketing_campaigns", "campaign_name", "character varying"),
    ("marketing_campaigns", "campaign_type", "character varying"),
    ("marketing_campaigns", "start_date", "date"),
    ("marketing_campaigns", "end_date", "date"),
    ("marketing_campaigns", "budget", "numeric"),
    ("marketing_campaigns", "spent_amount", "numeric"),
    ("marketing_campaigns", "impressions", "integer"),
    ("marketing_campaigns", "clicks", "integer"),
    ("marketing_campaigns", "conversions", "integer"),
    ("marketing_campaigns", "target_audience", "text"),
    ("marketing_campaigns", "campaign_status", "character varying"),
    # customer_sessions table
    ("customer_sessions", "session_id", "character varying"),
    ("customer_sessions", "customer_id", "character varying"),
    ("customer_sessions", "session_start", "timestamp without time zone"),
    ("customer_sessions", "session_end", "timestamp without time zone"),
    ("customer_sessions", "device_type", "character varying"),
    ("customer_sessions", "referrer_source", "character varying"),
    ("customer_sessions", "pages_viewed", "integer"),
    ("customer_sessions", "products_viewed", "integer"),
    ("customer_sessions", "conversion_flag", "boolean"),
    ("customer_sessions", "cart_abandonment_flag", "boolean"),
]

# Parse MINIO_ENDPOINT to strip protocol prefix if present
minio_endpoint = parse_minio_endpoint(os.getenv("MINIO_ENDPOINT", "localhost:9000"))

minio_client = Minio(
    minio_endpoint,
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False,
)


# Resolve the Spark master: honour SPARK_SERVER first (legacy name), then
# SPARK_MASTER_URL (set in docker-compose for the api/python containers),
# then fall back to local[*] for plain-Python / unit-test use.
_spark_master = (
    os.getenv("SPARK_SERVER")
    or os.getenv("SPARK_MASTER_URL")
    or "local[*]"
)

spark = (
    SparkSession.builder.appName("NormalizeData")
    .master(_spark_master)
    # Dynamic allocation requires an external shuffle service on standalone
    # clusters which is not configured here.  Disable it so executor count
    # stays predictable; the caller (create_spark_session in spark_streaming)
    # manages its own session and overrides this via getOrCreate anyway.
    .config("spark.dynamicAllocation.enabled", "false")
    # Use pre-downloaded local JARs — no Maven/internet access needed.
    # Do NOT set spark.jars to local driver paths — that uploads them to the
    # executor and creates duplicate class definitions alongside the copies
    # already present in /opt/spark/external-jars/ (SPARK_EXTRA_CLASSPATH in
    # the spark-master-py310 image), causing ClassCastException at runtime.
    .config("spark.driver.extraClassPath", _MAP_CP)
    # In cluster mode the executor has these JARs at /opt/spark/external-jars/.
    # In local[*] mode this path is ignored (same JVM as driver).
    .config("spark.executor.extraClassPath", "/opt/spark/external-jars/*")
    .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("inferSchema", "true")
    .config("mergeSchema", "true")
    # Delta Lake extensions — enables Delta format reads/writes and MERGE SQL.
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension",
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )    # Write timestamps as MICROS (INT64) — universally readable by all Spark
    # versions and downstream tools.  The default NANOS type causes
    # "Illegal Parquet type: INT64 (TIMESTAMP(NANOS,true))" in older readers.
    .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df_to_table = {
    "customers_df": {
        "table": "customers",
        "aliases": ["customer", "customers", "client", "clients", "customer_data"]
    },
    "addresses_df": {
        "table": "addresses",
        "aliases": ["address", "addresses", "location", "locations"]
    },
    "products_df": {
        "table": "products",
        "aliases": ["product", "products", "item", "items", "product_catalog"]
    },
    "inventories_df": {
        "table": "inventory",
        "aliases": ["inventory", "inventories", "stock", "stocks"]
    },
    "orders_df": {
        "table": "orders",
        "aliases": ["order", "orders", "purchase", "purchases"]
    },
    "order_items_df": {
        "table": "order_items",
        "aliases": ["order_items", "orderitems", "order_line", "order_lines"]
    },
    "shopping_carts_df": {
        "table": "shopping_cart",
        "aliases": [
            "shopping_cart",
            "shoppingcart",
            "cart",
            "carts",
            "cart_data",
            "cart_dataset",
            "carts_dataset"
        ]
    },
    "cart_items_df": {
        "table": "cart_items",
        "aliases": ["cart_items", "cartitems", "cart_line", "cart_lines"]
    },
    "payments_df": {
        "table": "payments",
        "aliases": ["payment", "payments", "transactions", "payment_data"]
    },
    "reviews_df": {
        "table": "reviews",
        "aliases": ["review", "reviews", "ratings", "feedback"]
    },
    "categories_df": {
        "table": "categories",
        "aliases": ["category", "categories", "product_categories"]
    },
    "wishlists_df": {
        "table": "wishlist",
        "aliases": ["wishlist", "wishlists", "favorites", "favourites"]
    },
    "customer_sessions_df": {
        "table": "customer_sessions",
        "aliases": ["session", "sessions", "customer_sessions", "user_sessions"]
    },
    "marketing_campaigns_df": {
        "table": "marketing_campaigns",
        "aliases": ["campaign", "campaigns", "marketing", "marketing_data"]
    },
    "suppliers_df": {
        "table": "suppliers",
        "aliases": ["supplier", "suppliers", "vendor", "vendors"]
    },
}


def resolve_table_splits(df_name, df, columns_info, mode):
    """
    Resolve table splits with fast-path optimization and fuzzy matching.

    Fast path: Check df_to_table first (0.1ms) - works for 95% of streaming cases
    Fuzzy match: Try to map table name using fuzzy matching and synonyms
    Fallback: Use detect_table (50ms) or split_unified_dataframe for unknown names

    Args:
        df_name: Name of the DataFrame
        df: Spark DataFrame
        columns_info: List of (table, column, type) tuples
        mode: "batch" or "stream"

    Returns:
        dict: {table_name: dataframe}
    """
    # Fast path: Known DataFrame name
    if df_name in df_to_table:
        table_name = df_to_table[df_name]["table"]  # Extract the table name from the dict
        return {table_name: df}

    # Try fuzzy matching on the dataframe name
    # Extract potential table name from df_name (e.g., "orders.csv" -> "orders", "customer_data_df" -> "customer_data")
    potential_table_name = df_name.lower().replace('.csv', '').replace('.json', '').replace('_df', '').replace('_data', '').replace('_dataset', '').strip()
    
    canonical_table = map_table_name(potential_table_name, threshold=85)
    if canonical_table:
        print(f"  ✅ Mapped file '{df_name}' to canonical table '{canonical_table}'")
        return {canonical_table: df}
    
    print(f"  ⚠️  Could not map '{df_name}' to any canonical table, trying content-based detection...")

    # Fallback: Unknown name - use detection/splitting
    if mode == "stream":
        detected_table = detect_table(df, columns_info)
        if detected_table:
            # Try fuzzy matching on detected table
            canonical_table = map_table_name(detected_table, threshold=80)
            if canonical_table:
                return {canonical_table: df}
            return {detected_table: df}
        return {}
    else:
        return split_unified_dataframe(df, columns_info)


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
    def _dedupe_dataframe_columns(input_df):
        cols = list(input_df.columns)
        seen = {}
        renamed = []
        changed = False
        for name in cols:
            count = seen.get(name, 0)
            if count == 0:
                renamed.append(name)
            else:
                renamed.append(f"{name}__dup{count}")
                changed = True
            seen[name] = count + 1

        if changed:
            print(f"  ℹ️  Detected duplicate columns; deduplicated names: {renamed}")
            return input_df.toDF(*renamed)
        return input_df

    # Guard against upstream duplicate columns before any rename/select.
    df = _dedupe_dataframe_columns(df)

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

    # Renaming variants can create collisions (e.g., user_id -> customer_id while
    # customer_id already exists). Deduplicate again before downstream selects.
    df = _dedupe_dataframe_columns(df)

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
            df, missing_cols, extra_cols, mapped_cols, threshold=87
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
            df, missing_cols, extra_cols, mapped_cols, threshold=0.87
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
            df, missing_cols, extra_cols, mapped_cols, threshold=0.87
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


_QUALIFIED_REF_RE = re.compile(r'^\s*"(?P<table>[^"]+)"\s*\.\s*"(?P<column>[^"]+)"\s*$')
_UNQUOTED_REF_RE = re.compile(r'^\s*(?P<table>[A-Za-z_][\w$]*)\s*\.\s*(?P<column>[A-Za-z_][\w$]*)\s*$')


def _parse_source_reference(raw_value, default_table: str = None):
    if raw_value is None:
        return None, None, ""

    raw_str = str(raw_value).strip()
    if not raw_str:
        return None, None, ""

    match = _QUALIFIED_REF_RE.match(raw_str) or _UNQUOTED_REF_RE.match(raw_str)
    if match:
        source_table = match.group("table").strip().strip('"')
        source_col = match.group("column").strip().strip('"')
    else:
        source_table = str(default_table or "").strip().strip('"') if default_table else None
        source_col = raw_str.strip().strip('"')

    if not source_col:
        return None, None, ""
    source_table = source_table or str(default_table or "").strip().strip('"')
    qualified = f'"{source_table}"."{source_col}"' if source_table else source_col
    return source_table, source_col, qualified


def process_all_dataframes(all_dataframes, columns_info, mapping_list, mode="batch", manual_mappings=None):
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
    manual_mappings : dict, optional
        Dictionary of manual column mappings provided by user.
        Format: {table_name: {canonical_col: source_col}}

    Returns
    -------
    results : dict
        Dict with results per canonical table.
    """

    # df_to_mapping_dict = {
    #     "customer_df": mapping_list.mapping_dict_customers,
    #     "product_df": mapping_list.mapping_dict_products,
    #     "inventory_df": mapping_list.mapping_dict_inventory,
    #     "orders_df": mapping_list.mapping_dict_orders,
    #     "reviews_df": mapping_list.mapping_dict_reviews,
    #     "wishlist_df": mapping_list.mapping_dict_wishlist,
    #     "payments_df": mapping_list.mapping_dict_payments,
    #     "order_items_df": mapping_list.mapping_dict_order_items,
    #     "shopping_cart_df": mapping_list.mapping_dict_shopping_cart,
    #     "customer_sessions_df": mapping_list.mapping_dict_customer_sessions,
    #     "marketing_campaigns_df": mapping_list.mapping_dict_marketing_campaigns,
    #     "suppliers_df": mapping_list.mapping_dict_suppliers,
    # }

    results = {}

    resolved_entries = []
    source_table_frames = {}

    def _common_join_keys(left_df, right_df):
        left_cols = set(left_df.columns)
        right_cols = set(right_df.columns)
        common = [
            key for key in (left_cols & right_cols)
            if str(key).endswith("_id") and str(key) != "_ingested_at"
        ]
        return sorted(common)

    def _left_join_with_projected_source(dst_df, src_df, join_keys, source_col, src_alias):
        """
        Left-join source value into destination while avoiding ambiguous columns.
        Keeps only destination columns plus one projected source alias.
        """
        dst_alias = "dst"
        src_alias_name = "src"

        src_projection = (
            src_df
            .select(*join_keys, col(source_col).alias(src_alias))
            .dropDuplicates(join_keys)
            .alias(src_alias_name)
        )
        dst_projection = dst_df.alias(dst_alias)

        join_condition = None
        for key in join_keys:
            predicate = col(f"{dst_alias}.`{key}`") == col(f"{src_alias_name}.`{key}`")
            join_condition = predicate if join_condition is None else (join_condition & predicate)

        joined = dst_projection.join(src_projection, on=join_condition, how="left")
        return joined.select(
            col(f"{dst_alias}.*"),
            col(f"{src_alias_name}.`{src_alias}`").alias(src_alias),
        )

    def _resolve_table_manual_mappings(table_name: str, canonical_cols: set):
        """Return canonical -> source reference metadata with robust key/orientation handling."""
        if not isinstance(manual_mappings, dict):
            return {}

        table_map = manual_mappings.get(table_name)
        if not isinstance(table_map, dict):
            for key, value in manual_mappings.items():
                if not isinstance(value, dict):
                    continue
                canonical_key = map_table_name(str(key), threshold=80)
                if canonical_key == table_name:
                    table_map = value
                    break

        if not isinstance(table_map, dict):
            return {}

        resolved = {}
        for left, right in table_map.items():
            l = str(left).strip() if left is not None else ""
            r = str(right).strip() if right is not None else ""
            if not l or not r:
                continue

            # Preferred orientation in onboarding: canonical -> source.
            if l in canonical_cols:
                source_table, source_col, qualified_ref = _parse_source_reference(r, default_table=table_name)
                if source_col:
                    resolved[l] = {
                        "source_table": source_table,
                        "source_col": source_col,
                        "qualified_ref": qualified_ref,
                    }
            # Backward compatibility: source -> canonical.
            elif r in canonical_cols:
                source_table, source_col, qualified_ref = _parse_source_reference(l, default_table=table_name)
                if source_col:
                    resolved[r] = {
                        "source_table": source_table,
                        "source_col": source_col,
                        "qualified_ref": qualified_ref,
                    }

        return resolved

    # Pass 1: resolve incoming dataframes to canonical-table entries so explicit
    # source-table references can reuse other tables within the same micro-batch.
    for df_name, df in all_dataframes.items():
        print(f"\n{'='*50}")
        print(f"Incoming dataframe: {df_name}")

        # Phase 6 optimization: unified resolution with fast-path
        split_dfs = resolve_table_splits(df_name, df, columns_info, mode)

        if not split_dfs:
            print(f"⚠️ Could not resolve table for {df_name}")
            continue

        for table_name, sub_df in split_dfs.items():
            # Check if table_name is in canonical schema
            canonical_table = map_table_name(table_name, threshold=80)
            if canonical_table:
                table_to_use = canonical_table
            else:
                # If no canonical mapping found, skip this table
                print(f"⚠️  Table '{table_name}' not found in canonical schema. Skipping...")
                continue

            resolved_entries.append((df_name, table_to_use, sub_df))
            if table_to_use not in source_table_frames:
                source_table_frames[table_to_use] = sub_df

    for df_name, table_to_use, sub_df in resolved_entries:
            
        mapping_dict = getattr(mapping_list, f"mapping_dict_{table_to_use}", None)
        if not mapping_dict:
            print(f"⚠️  No mapping dict found for canonical table '{table_to_use}'. Skipping...")
            continue

        mapped = {col: "" for t, col, _ in columns_info if t == table_to_use}
        print(f"Processing → {table_to_use} with {len(mapped)} canonical columns")

        canonical_cols = set(mapped.keys())
        table_manual_mappings = _resolve_table_manual_mappings(table_to_use, canonical_cols)

        # Pre-apply manual mappings directly on incoming dataframe when source
        # columns exist. This ensures the algorithm chain starts with user-
        # confirmed mappings and avoids unnecessary fuzzy remapping.
        if table_manual_mappings:
            pre_applied = 0
            for canonical_col, source_meta in table_manual_mappings.items():
                source_table = source_meta.get("source_table") or table_to_use
                source_col = source_meta.get("source_col")
                if not source_col:
                    continue

                # Pre-apply only same-table mappings; cross-table mappings are
                # applied later only for unresolved canonical fields to avoid
                # expensive joins on every mapping candidate in streaming mode.
                if source_table != table_to_use:
                    continue

                if source_col in sub_df.columns and canonical_col not in sub_df.columns:
                    sub_df = sub_df.withColumn(canonical_col, col(source_col))
                    # Avoid duplicate canonical creation during normalization when
                    # source variant would also be renamed to the same canonical name.
                    if source_col != canonical_col and source_col in sub_df.columns:
                        sub_df = sub_df.drop(source_col)
                    pre_applied += 1
            if pre_applied > 0:
                print(f"\n📝 Pre-applied {pre_applied} manual mapping(s) for {table_to_use} before auto-mapping...")

        final_df, extra_df, extra_cols, missing_cols, mapped_cols = mapping(
            sub_df, mapping_dict, mapped
        )

        # Apply manual mappings if provided
        if table_manual_mappings:
            print(f"\n📝 Applying manual mappings for {table_to_use}...")
            source_df_projection_cache = {}

            for canonical_col, source_meta in table_manual_mappings.items():
                if canonical_col not in missing_cols:
                    continue

                source_table = source_meta.get("source_table") or table_to_use
                source_col = source_meta.get("source_col")
                qualified_ref = source_meta.get("qualified_ref") or source_col
                if not source_col:
                    continue

                source_df = sub_df if source_table == table_to_use else source_table_frames.get(source_table)

                if source_col in final_df.columns:
                    try:
                        if source_col != canonical_col:
                            final_df = final_df.withColumn(canonical_col, col(source_col))

                        if canonical_col in missing_cols:
                            missing_cols.remove(canonical_col)
                        if source_col in extra_cols:
                            extra_cols.remove(source_col)
                        mapped_cols[canonical_col] = qualified_ref

                        print(f"   ✅ Mapped {qualified_ref} → {canonical_col}")
                    except Exception as map_error:
                        print(f"   ⚠️  Could not map {qualified_ref} → {canonical_col}: {map_error}")
                elif source_df is not None and source_col in source_df.columns:
                    try:
                        join_keys = _common_join_keys(final_df, source_df)
                        if not join_keys:
                            print(
                                f"   ⚠️  Invalid cross-table mapping {qualified_ref} → {canonical_col}: "
                                f"no valid join keys between '{table_to_use}' and '{source_table}'"
                            )
                            continue

                        cache_key = (source_table or table_to_use, source_col, tuple(join_keys))
                        if cache_key not in source_df_projection_cache:
                            source_df_projection_cache[cache_key] = source_df

                        src_alias = f"__src__{canonical_col}"
                        final_df = _left_join_with_projected_source(
                            dst_df=final_df,
                            src_df=source_df_projection_cache[cache_key],
                            join_keys=join_keys,
                            source_col=source_col,
                            src_alias=src_alias,
                        ).withColumn(canonical_col, col(src_alias)).drop(src_alias)

                        if canonical_col in missing_cols:
                            missing_cols.remove(canonical_col)
                        if source_col in extra_cols:
                            extra_cols.remove(source_col)
                        mapped_cols[canonical_col] = qualified_ref
                        print(f"   ✅ Mapped {qualified_ref} → {canonical_col} (key-based cross-table join: {join_keys})")
                    except Exception as map_error:
                        print(f"   ⚠️  Could not map {qualified_ref} → {canonical_col} via key-based cross-table join: {map_error}")
                else:
                    print(f"   ℹ️  Skipping {qualified_ref} → {canonical_col} (source not found)")

            print(f"   After manual mapping: {len(missing_cols)} missing columns")

        # Sanitize lists before putting them into results
        results[f"{df_name}__{table_to_use}"] = {
            "table_name": table_to_use,
            "final_df": final_df,  # keep Spark DF
            "extra_df": extra_df,  # keep Spark DF
            "extra_cols": [safe_serialize(c) for c in extra_cols],
            "missing_cols": [safe_serialize(c) for c in missing_cols],
            "mapped_cols": {
                safe_serialize(k): safe_serialize(v)
                for k, v in mapped_cols.items()
                if v not in (None, "")
            },
        }

        print(f"✅ Completed {df_name} → {table_to_use}")
        print(f"   Missing cols: {missing_cols}")
        print(f"   Extra cols: {extra_cols}")
        print("   Preview:")
        final_df.show(3)

    return results


def save_dataframes_to_minio(results, client, bucket_name, operation=None, primary_key_col=None, folder="mapped"):
    """
    Save processed DataFrames to MinIO as Delta tables (all three modes).

    Replaces the old toPandas() → CSV → MinIO-SDK path with direct Spark
    Delta writes via S3A so that:
    - No driver OOM from toPandas() on large snapshots.
    - Append-only writes (insert / snapshot) land as partitioned Delta tables.
    - CDC update/delete use DeltaTable.merge() for correct ACID upserts.
    - The cleaning pipeline can read all three modes with a single
      ``spark.read.format("delta")`` call via the updated cleaning_utils.py.

    Args:
        results:          Dictionary of processed results from process_all_dataframes.
        client:           MinIO client (used only for bucket-existence check).
        bucket_name:      Target MinIO bucket.
        operation:        CDC op: 'c'/'r' (insert/snapshot), 'u' (update), 'd' (delete).
        primary_key_col:  Override primary-key column name; auto-detected when None.
        folder:           Sub-prefix inside the bucket (default: ``"mapped"``).
    """
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Created bucket: {bucket_name}")
    else:
        print(f"Bucket already exists: {bucket_name}")

    # Primary key mapping for each table
    table_primary_keys = {
        "addresses":           "address_id",
        "customers":           "customer_id",
        "suppliers":           "supplier_id",
        "categories":          "category_id",
        "products":            "product_id",
        "inventory":           "inventory_id",
        "wishlist":            "wishlist_id",
        "shopping_cart":       "cart_id",
        "cart_items":          "cart_item_id",
        "orders":              "order_id",
        "order_items":         "order_item_id",
        "payments":            "payment_id",
        "reviews":             "review_id",
        "marketing_campaigns": "campaign_id",
        "customer_sessions":   "session_id",
    }

    from pyspark.sql import functions as F

    for result_key, result_data in results.items():
        table_name = result_data["table_name"]
        final_df   = result_data["final_df"]
        pk_col     = primary_key_col or table_primary_keys.get(table_name)
        _sess      = final_df.sparkSession

        print(f"Saving {table_name} to MinIO...")

        df_out  = final_df.withColumn("_ingested_at", F.current_timestamp())
        s3_path = f"s3a://{bucket_name}/{folder}/{table_name}"

        # ── CDC update / delete — use Delta MERGE ────────────────────────────
        if operation in ("u", "update", "d", "delete") and pk_col:
            try:
                from delta.tables import DeltaTable
                if DeltaTable.isDeltaTable(_sess, s3_path):
                    dt = DeltaTable.forPath(_sess, s3_path)
                    if operation in ("d", "delete"):
                        (
                            dt.alias("t")
                            .merge(df_out.alias("s"), f"t.`{pk_col}` = s.`{pk_col}`")
                            .whenMatchedDelete()
                            .execute()
                        )
                        print(f"✅ Delta DELETE  {table_name} → {s3_path}")
                    else:
                        (
                            dt.alias("t")
                            .merge(df_out.alias("s"), f"t.`{pk_col}` = s.`{pk_col}`")
                            .whenMatchedUpdateAll()
                            .whenNotMatchedInsertAll()
                            .execute()
                        )
                        print(f"✅ Delta MERGE   {table_name} → {s3_path}")
                    continue
            except ImportError:
                print(f"  ⚠️  delta-spark not available; falling back to Parquet for {table_name}")

        # ── Default: Delta append (insert, snapshot, or fallback) ─────────────
        try:
            df_out.write.format("delta").mode("append").save(s3_path)
            print(f"✅ Delta APPEND  {table_name} → {s3_path}")
        except Exception:  # delta not on classpath in this session
            df_out.write.mode("append").parquet(s3_path)
            print(f"✅ Parquet APPEND {table_name} → {s3_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Normalize and map data files from MinIO")
    parser.add_argument("--bucket-name", type=str, default="pulse-bucket-1",
                        help="MinIO bucket name (business_id)")
    args = parser.parse_args()
    bucket_name = args.bucket_name

    all_dataframes = load_all_files_from_minio(minio_client, bucket_name, spark)

    # Use hardcoded columns_info from canonical schema
    columns_info = COLUMNS_INFO

    results = process_all_dataframes(all_dataframes, columns_info, mapping_list)
    save_dataframes_to_minio(results, minio_client, bucket_name)

    print("\n" + "=" * 50)
    print("Processing complete!")
    print(f"Total tables processed: {len(results)}")
    spark.stop()
