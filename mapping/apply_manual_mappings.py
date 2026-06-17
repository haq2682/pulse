#!/usr/bin/env python3
"""
Apply manual mappings to already-mapped files in MinIO.
This script reads files from the mapped-temp folder, applies manual column mappings,
and moves the results to the mapped folder.
"""

import sys
import os
import json
import re
import pandas as pd
from io import BytesIO, StringIO
from minio import Minio
from dotenv import load_dotenv, find_dotenv

# Add current directory to Python path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.helpers import parse_minio_endpoint
from utils.table_mapper import map_table_name

load_dotenv(find_dotenv())


def _resolve_canonical_table_name(raw_name: str) -> str:
    """
    Resolve a raw ingested filename/sheet stem (e.g. "campaigns_dataset") to its
    canonical schema table name (e.g. "marketing_campaigns"), mirroring the
    resolution ``utils/file_loader.py`` (via ``normalize_name``) already applies
    when the Spark mapping pipeline first loads ``ingested/``.

    Without this, the ingested cache here would be keyed by the raw upload
    filename instead of the canonical table name used everywhere else
    (manual_mappings, table_name params, etc.), making same-table manual
    mappings indistinguishable from cross-table ones whenever the uploaded
    file isn't already named exactly like its canonical table.

    Returns None if no canonical match is found — callers decide their own
    fallback (e.g. keep the raw name) so the file is still cached either way.
    """
    cleaned = re.sub(r"(dataset|data|table|export)$", "", raw_name.lower())
    cleaned = re.sub(r"[^0-9a-zA-Z_]+", "_", cleaned).strip("_")
    return map_table_name(cleaned, threshold=80)

# Initialize MinIO client
minio_endpoint = parse_minio_endpoint(os.getenv("MINIO_ENDPOINT", "localhost:9000"))
minio_client = Minio(
    minio_endpoint,
    access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    secure=False,
)


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
    qualified_ref = f'"{source_table}"."{source_col}"' if source_table else source_col
    return source_table, source_col, qualified_ref


def load_ingested_files_cache(bucket_name: str, required_tables=None):
    """
    Load all data files from the ingested folder and cache them.
    Supports CSV, JSON, Excel (.xlsx, .xls), and Parquet files.
    For Excel files, each sheet is cached separately.
    
    Args:
        bucket_name: Name of the MinIO bucket
    
    Returns:
        dict: Dictionary mapping table_name -> DataFrame for all ingested files
              For Excel files, table names follow format: filename_sheetname
    """
    from io import BytesIO  # Add this import if not already present
    
    ingested_cache = {}
    ingested_folder = "ingested/"

    # Supported file extensions
    supported_extensions = ['.csv', '.json', '.xlsx', '.xls', '.parquet']

    try:
        # recursive=True so we find chunks written as
        # ingested/{table}/chunk_NNNN.parquet by initial_load.py
        objects = list(minio_client.list_objects(bucket_name, prefix=ingested_folder, recursive=True))

        # Filter for supported file types
        data_objects = [
            obj for obj in objects
            if any(obj.object_name.endswith(ext) for ext in supported_extensions)
        ]

        # Accumulate parquet chunks by their parent directory name so that
        # ingested/cart_items/chunk_0000.parquet + chunk_0001.parquet → "cart_items" entry
        chunk_frames: dict = {}   # table_name -> list[pd.DataFrame]

        for obj in data_objects:
            # path relative to "ingested/" e.g. "cart_items/chunk_0000.parquet"
            rel_path = obj.object_name[len(ingested_folder):]
            parts = rel_path.split("/")

            # Determine the logical table name:
            # • Flat file (old upload layout): ingested/orders.csv  → parts = ["orders.csv"]
            #   → base_name = "orders"
            # • Chunked layout (initial_load.py): ingested/cart_items/chunk_0000.parquet
            #   → parts = ["cart_items", "chunk_0000.parquet"]
            #   → table_name = "cart_items"  (parent directory)
            is_chunked = len(parts) == 2  # exactly one subdirectory level
            filename = parts[-1]

            file_ext = None
            for ext in supported_extensions:
                if filename.endswith(ext):
                    file_ext = ext
                    break
            if file_ext is None:
                continue

            if is_chunked and file_ext == '.parquet':
                table_name = parts[0]   # parent directory — already canonical (written by initial_load.py)
            else:
                # Flat file (user-uploaded batch mode): the filename stem is
                # whatever the user named their file (e.g. "campaigns_dataset"),
                # not necessarily the canonical table name. Resolve it the same
                # way utils/file_loader.py does for the Spark mapping pipeline,
                # so this cache is keyed consistently with manual_mappings'
                # table names (e.g. "marketing_campaigns") — otherwise same-table
                # lookups below would be mistaken for cross-table ones.
                raw_stem = filename[: -len(file_ext)]
                table_name = _resolve_canonical_table_name(raw_stem) or raw_stem

            if required_tables and table_name not in required_tables:
                continue

            try:
                response = minio_client.get_object(bucket_name, obj.object_name)
                file_data = response.read()
                response.close()
                response.release_conn()

                if file_ext == '.csv':
                    df = pd.read_csv(StringIO(file_data.decode("utf-8")))
                    ingested_cache[table_name] = df
                    print(f"  Cached {table_name} from ingested/ ({len(df)} rows)")

                elif file_ext == '.json':
                    df = pd.read_json(StringIO(file_data.decode("utf-8")))
                    ingested_cache[table_name] = df
                    print(f"  Cached {table_name} from ingested/ ({len(df)} rows)")

                elif file_ext == '.parquet':
                    df = pd.read_parquet(BytesIO(file_data))
                    # Accumulate chunks; we concat at the end to avoid O(n²) copies
                    chunk_frames.setdefault(table_name, []).append(df)

                elif file_ext in ['.xlsx', '.xls']:
                    excel_file = pd.ExcelFile(BytesIO(file_data), engine='openpyxl')
                    for sheet_name in excel_file.sheet_names:
                        df = pd.read_excel(excel_file, sheet_name=sheet_name)
                        # Resolve from the sheet name itself (mirrors file_loader.py,
                        # which treats each sheet as its own table), falling back to
                        # the combined name if no canonical match is found.
                        tname = _resolve_canonical_table_name(sheet_name) or f"{table_name}_{sheet_name}"
                        ingested_cache[tname] = df
                        print(f"  Cached {tname} from ingested/ ({len(df)} rows)")

            except Exception as e:
                print(f"  ⚠️  Warning: Could not load {obj.object_name}: {e}")
                continue

        # Concat all parquet chunks per table
        for table_name, frames in chunk_frames.items():
            combined = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
            ingested_cache[table_name] = combined
            print(f"  Cached {table_name} from ingested/ ({len(combined)} rows, {len(frames)} chunk(s))")

    except Exception as e:
        print(f"  ⚠️  Warning: Could not list ingested files: {e}")

    return ingested_cache


def find_column_in_ingested(column_name: str, ingested_cache: dict, preferred_table: str = None):
    """
    Search for a column across all ingested files.
    
    Args:
        column_name: Name of the column to find
        ingested_cache: Dictionary of ingested DataFrames
    
    Returns:
        tuple: (table_name, DataFrame) if found, else (None, None)
    """
    if preferred_table and preferred_table in ingested_cache:
        preferred_df = ingested_cache[preferred_table]
        if column_name in preferred_df.columns:
            return preferred_table, preferred_df

    for table_name, df in ingested_cache.items():
        if table_name == preferred_table:
            continue
        if column_name in df.columns:
            return table_name, df
    return None, None


def _detect_streaming_parquet_tables(bucket_name: str, temp_folder: str) -> tuple:
    """
    Detect tables stored as partitioned Parquet under mapped-temp/ (db/api streaming mode).

    Layout written by _write_spark_native:
        mapped-temp/{table_name}/_batch_id=0/part-*.parquet
        mapped-temp/{table_name}/_extra_cols/part-*.parquet   (optional companion)

    Returns a 2-tuple:
        tables      : dict  table_name -> list of main Parquet object keys
        extra_tables: dict  table_name -> list of _extra_cols Parquet object keys
    Returns ({}, {}) if no Parquet files are found.
    """
    try:
        all_objects = list(
            minio_client.list_objects(bucket_name, prefix=temp_folder, recursive=True)
        )
    except Exception as e:
        print(f"  ⚠️  Could not list {temp_folder}: {e}")
        return {}, {}

    tables = {}
    extra_tables = {}
    for obj in all_objects:
        if not obj.object_name.endswith(".parquet"):
            continue
        # Strip "mapped-temp/" prefix → "table_name/...rest..."
        rel = obj.object_name[len(temp_folder):]
        parts = rel.split("/", 1)
        if not parts:
            continue
        table_name = parts[0]
        rest = parts[1] if len(parts) > 1 else ""
        if rest.startswith("_extra_cols/"):
            extra_tables.setdefault(table_name, []).append(obj.object_name)
        else:
            tables.setdefault(table_name, []).append(obj.object_name)

    return tables, extra_tables


def _common_join_keys(left_df: pd.DataFrame, right_df: pd.DataFrame) -> list[str]:
    common = [
        c for c in left_df.columns
        if c in right_df.columns and c.endswith("_id") and c != "_ingested_at"
    ]
    return sorted(common)


# Mirrors map.py's table_primary_keys / cleaning_utils.py's _CLEANED_PRIMARY_KEYS.
_TABLE_PRIMARY_KEYS = {
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


def _raw_primary_key_column(table_name: str, df: pd.DataFrame, auto_mappings: dict) -> tuple:
    """
    Resolve (canonical_pk_col, raw_pk_col) for *table_name* using the table's
    canonical primary key plus the auto-mapping record of which raw ingested
    column it was renamed from. Returns (None, None) if any link is missing
    (no canonical PK defined, no auto-mapping recorded, or the canonical PK
    isn't actually present/populated in *df*).

    Used to recover a same-table manual mapping via a real value-based join
    instead of assuming the ingested file and the mapped-temp file share row
    order/count — the only assumption that's safe is that BOTH files share the
    same logical row identity (the primary key), regardless of how many
    chunk files or Spark partitions either side was read through.
    """
    pk_col = _TABLE_PRIMARY_KEYS.get(table_name)
    if not pk_col or pk_col not in df.columns or df[pk_col].isna().all():
        return None, None

    table_auto = (auto_mappings or {}).get(table_name) or {}
    pk_source_ref = table_auto.get(pk_col)
    if not pk_source_ref:
        return None, None

    _, raw_pk_col, _ = _parse_source_reference(pk_source_ref, default_table=table_name)
    return (pk_col, raw_pk_col) if raw_pk_col else (None, None)


def _apply_mappings_to_df(df: pd.DataFrame, table_name: str, manual_mappings: dict,
                           ingested_cache: dict, updated_results: dict,
                           is_streaming: bool = False,
                           extra_source_df: "pd.DataFrame | None" = None,
                           auto_mappings: dict = None) -> pd.DataFrame:
    """
    Apply manual column renames (and cross-table lookups) to a DataFrame in-place.
    Shared by both CSV and Parquet paths.

    is_streaming : bool
        Set to True only for genuine continuous CDC streaming batches (Parquet
        under ``_batch_id=N/``), where a later micro-batch will still arrive and
        can fill in a column left null here. False for CSV and for the flat
        batch/db/api initial-load Delta/Parquet layout, where this is the only
        batch and missing columns must be resolved now. In all cases the
        DataFrame already has CANONICAL column names (produced by the mapping
        pipeline); missing/null canonical columns appear as all-null placeholders.

    extra_source_df : pd.DataFrame | None
        The companion ``_extra_cols/`` Parquet written by ``_write_spark_native``
        alongside the main ``_batch_id=0/`` Parquet.  It contains every column
        from the original micro-batch (canonical + extra source columns) so we
        can back-fill a null canonical slot from the raw source column the user
        identified in the manual mappings UI.  Only present in streaming mode.

    auto_mappings : dict | None
        The {table: {canonical_col: qualified_source_ref}} record of how the
        *automatic* mapping pass renamed columns (saved to
        ``onboarding.auto_mappings``). Used to look up the raw ingested name of
        a table's own primary key, so a same-table manual mapping can be
        recovered with a real value-based join instead of assuming the
        mapped-temp file and the ingested/ file share row order — which may not
        hold for db/api initial loads whose ``ingested/`` data spans multiple
        chunk files read through multiple Spark partitions.
    """
    if table_name not in manual_mappings:
        return df

    table_map = manual_mappings[table_name]
    print(f"  📝 Applying manual mappings for {table_name}...")

    for canonical_col, source_ref in table_map.items():
        source_table, source_col, qualified_ref = _parse_source_reference(source_ref, default_table=table_name)
        if not source_col:
            continue

        if source_col in df.columns:
            # Happy path: source column is directly present — rename it.
            df.rename(columns={source_col: canonical_col}, inplace=True)
            print(f"     ✅ Mapped {qualified_ref} → {canonical_col}")
        elif f"__src__{source_col}" in df.columns:
            # _write_spark_native embedded extra source columns under a __src__
            # prefix in mapped-temp Parquet so we can back-fill them here.
            df.rename(columns={f"__src__{source_col}": canonical_col}, inplace=True)
            print(f"     ✅ Mapped {qualified_ref} → {canonical_col} (recovered from __src__ helper column)")
        elif extra_source_df is not None and source_col in extra_source_df.columns:
            # Source column is present in the _extra_cols companion Parquet that
            # _write_spark_native wrote alongside the main batch Parquet.
            # Use a primary-key join so row ordering doesn't matter.
            # If no suitable key exists, treat as invalid mapping.
            id_cols = [
                c for c in extra_source_df.columns
                if c.endswith("_id") and c in df.columns
                and not extra_source_df[c].isna().all()
            ]
            if id_cols:
                key_col = id_cols[0]
                extra_subset = (
                    extra_source_df[[key_col, source_col]]
                    .drop_duplicates(subset=[key_col])
                    .copy()
                )
                extra_subset.rename(columns={source_col: canonical_col}, inplace=True)
                # Drop the all-null placeholder so the merged column fills it.
                df.drop(columns=[canonical_col], inplace=True, errors="ignore")
                df = pd.merge(df, extra_subset, on=key_col, how="left")
                print(
                    f"     ✅ Mapped {qualified_ref} → {canonical_col} "
                    f"(from _extra_cols companion, joined on '{key_col}')"
                )
            else:
                warning_msg = (
                    f"No valid join key to map '{qualified_ref}' into '{table_name}.{canonical_col}' "
                    "from _extra_cols companion"
                )
                print(f"     ⚠️  {warning_msg}")
                updated_results["failed_mappings"].append({
                    "table": table_name,
                    "canonical_column": canonical_col,
                    "source_column": qualified_ref,
                    "error": warning_msg,
                    "stage": "join_validation",
                })
        elif is_streaming and canonical_col in df.columns:
            # Streaming-mode fallback: canonical column exists but is all-null
            # and the source column was not found in the companion Parquet either
            # (e.g. the source column name had no match in extra_df for that batch).
            # The continuous Airflow streaming job will populate it from batch 1+.
            print(
                f"     ℹ️  '{source_col}' not found in first-batch Parquet for {table_name} "
                f"(canonical column '{canonical_col}' exists as null placeholder). "
                f"Will be populated from source column '{source_col}' by the "
                f"continuous streaming job for all subsequent batches."
            )
        else:
            # Column not present in the mapped-temp file (extra/raw columns are
            # dropped before mapped-temp is written) — recover it from ingested/.
            print(f"     🔍 '{source_col}' not found in {table_name} — searching ingested files...")

            # Prefer the table the user explicitly selected. Only fall back to a
            # fuzzy search across every cached ingested table (which may land on
            # a different table than `table_name`) when that direct lookup misses
            # — e.g. legacy/unqualified manual mappings with no source table.
            direct_df = ingested_cache.get(source_table) if source_table else None
            if direct_df is not None and source_col in direct_df.columns:
                found_table, source_df = source_table, direct_df
            else:
                found_table, source_df = find_column_in_ingested(
                    source_col, ingested_cache, preferred_table=source_table
                )

            if found_table is not None and found_table == table_name:
                # Same-table recovery. Prefer a real value-based join on the
                # table's own primary key (resolved via auto_mappings) — this is
                # correct regardless of row order or how many chunk files /
                # Spark partitions either side was read through (relevant for
                # db/api initial loads, whose ingested/ data can span multiple
                # chunk_NNNN.parquet files). Only fall back to positional
                # alignment — valid because mapping never filters or reorders
                # rows, only renames/drops columns — when no usable primary-key
                # link is available (e.g. single-file batch CSV uploads, where
                # row order is trivially preserved anyway).
                canonical_pk, raw_pk = _raw_primary_key_column(table_name, df, auto_mappings)
                if canonical_pk and raw_pk and raw_pk in source_df.columns:
                    left_key = df[canonical_pk].astype(str)
                    right_key = source_df[raw_pk].astype(str)
                    subset = (
                        source_df.assign(**{f"__pk_{canonical_pk}": right_key})[[f"__pk_{canonical_pk}", source_col]]
                        .drop_duplicates(subset=[f"__pk_{canonical_pk}"])
                        .rename(columns={source_col: canonical_col})
                    )
                    df = df.assign(**{f"__pk_{canonical_pk}": left_key})
                    df.drop(columns=[canonical_col], inplace=True, errors="ignore")
                    df = pd.merge(df, subset, on=f"__pk_{canonical_pk}", how="left").drop(columns=[f"__pk_{canonical_pk}"])
                    print(
                        f"     ✅ Mapped {qualified_ref} → {canonical_col} "
                        f"(recovered from ingested/, joined on primary key '{canonical_pk}')"
                    )
                elif len(source_df) == len(df):
                    df[canonical_col] = source_df[source_col].to_numpy()
                    print(
                        f"     ✅ Mapped {qualified_ref} → {canonical_col} "
                        f"(recovered from ingested/, same-table positional fill — "
                        f"no primary-key link available for a value-based join)"
                    )
                else:
                    warning_msg = (
                        f"Row count mismatch between mapped-temp ({len(df)} rows) and "
                        f"ingested/ ({len(source_df)} rows) for '{table_name}', and no "
                        f"primary-key link available — cannot safely recover '{qualified_ref}'"
                    )
                    print(f"     ⚠️  {warning_msg}")
                    updated_results["failed_mappings"].append({
                        "table": table_name,
                        "canonical_column": canonical_col,
                        "source_column": qualified_ref,
                        "error": warning_msg,
                        "stage": "row_count_mismatch",
                    })
            elif found_table is not None:
                # Genuine cross-table mapping — needs a relational join.
                join_keys = _common_join_keys(df, source_df)
                if join_keys:
                    source_subset = (
                        source_df[join_keys + [source_col]]
                        .drop_duplicates(subset=join_keys)
                        .copy()
                    )
                    source_subset.rename(columns={source_col: canonical_col}, inplace=True)
                    df.drop(columns=[canonical_col], inplace=True, errors="ignore")
                    df = pd.merge(df, source_subset, on=join_keys, how="left")
                    print(
                        f"     ✅ Mapped {qualified_ref} (from {found_table}) → {canonical_col} "
                        f"in {table_name} via key join {join_keys}"
                    )
                else:
                    warning_msg = (
                        f"No valid join key between '{table_name}' and '{found_table}' for '{qualified_ref}'"
                    )
                    print(f"     ⚠️  {warning_msg}")
                    updated_results["failed_mappings"].append({
                        "table": table_name,
                        "canonical_column": canonical_col,
                        "source_column": qualified_ref,
                        "error": warning_msg,
                        "stage": "join_validation",
                    })
            else:
                warning_msg = (
                    f"Source column '{qualified_ref}' not found in {table_name} "
                    "or any ingested files"
                )
                print(f"     ⚠️  {warning_msg}")
                updated_results["failed_mappings"].append({
                    "table": table_name,
                    "canonical_column": canonical_col,
                    "source_column": qualified_ref,
                    "error": warning_msg,
                    "stage": "column_mapping",
                })

    return df


def apply_manual_mappings_to_files(bucket_name: str, manual_mappings: dict, auto_mappings: dict = None):
    """
    Apply manual mappings to files in the mapped-temp folder.

    Supports the storage layouts written by the mapping pipeline:

    **Legacy batch mode (CSV)**
        ``mapped-temp/{table_name}.csv``  →  ``mapped/{table_name}.csv``

    **Batch / db / api initial load (flat Delta/Parquet)**
        ``mapped-temp/{table_name}/part-*.parquet``  (written by
        ``save_dataframes_to_minio``; no ``_batch_id=`` partitioning — this is
        the only batch, so missing columns must be resolved now, not deferred)
        →  ``mapped/{table_name}/_batch_id=0/part-0.parquet``

    **Continuous CDC streaming — db / api (partitioned Parquet)**
        ``mapped-temp/{table_name}/_batch_id=0/part-*.parquet``
        →  ``mapped/{table_name}/_batch_id=0/part-0.parquet``

    Args:
        bucket_name: Name of the MinIO bucket
        manual_mappings: Dictionary of manual column mappings
                        Format: {table_name: {canonical_col: source_col}}

    Returns:
        dict: Updated mapping results with missing_cols and extra_cols
    """
    print(f"\n{'='*60}")
    print(f"Applying Manual Mappings to Bucket: {bucket_name}")
    print(f"{'='*60}\n")

    if not minio_client.bucket_exists(bucket_name):
        raise ValueError(f"Bucket '{bucket_name}' does not exist")

    updated_results = {
        "missing_cols": [],
        "extra_cols": [],
        "failed_mappings": []
    }

    required_source_tables = set()
    for target_table, table_map in (manual_mappings or {}).items():
        if not isinstance(table_map, dict):
            continue
        target = str(target_table or "").strip()
        if not target:
            continue
        for _, source_ref in table_map.items():
            source_table, source_col, _ = _parse_source_reference(source_ref, default_table=target)
            # Include same-table sources too: extra/raw columns are dropped from
            # mapped-temp, so even a same-table mapping must be recovered from
            # ingested/. Without this, a mix of same-table and cross-table
            # mappings across tables would cause `required_tables` to only
            # cover the cross-table sources, filtering the same-table sources
            # out of the ingested cache entirely.
            if source_table and source_col:
                required_source_tables.add(source_table)

    # Load ingested files for column lookup (batch mode uses these to recover
    # both same-table and cross-table manual mappings; streaming mode may not
    # have an ingested/ folder — that's fine).
    print("📂 Loading ingested files for column lookup...")
    ingested_cache = load_ingested_files_cache(
        bucket_name,
        required_tables=required_source_tables if required_source_tables else None,
    )
    print(f"   Cached {len(ingested_cache)} ingested files\n")

    temp_folder = "mapped-temp/"

    # ── Detect storage format ─────────────────────────────────────────────────
    try:
        top_objects = list(
            minio_client.list_objects(bucket_name, prefix=temp_folder, recursive=False)
        )
    except Exception as e:
        raise ValueError(f"Failed to list objects in {temp_folder}: {e}")

    csv_objects = [obj for obj in top_objects if obj.object_name.endswith(".csv")]

    # Parquet mode: look for Parquet files one level deeper. This covers two
    # distinct layouts that both lack top-level .csv files:
    #   • Genuine CDC streaming  — mapped-temp/{table}/_batch_id=N/part-*.parquet
    #     (written by _write_spark_native; a later micro-batch will still arrive).
    #   • Batch / db / api initial-load — mapped-temp/{table}/part-*.parquet
    #     (flat Delta/Parquet table written by save_dataframes_to_minio; this is
    #     the ONLY batch — there is no later run that will fill in null columns).
    # These must be told apart: only the former should defer an already-present
    # null canonical column to "a later batch" instead of resolving it now.
    streaming_tables = {}
    streaming_extra_tables = {}
    if not csv_objects:
        streaming_tables, streaming_extra_tables = _detect_streaming_parquet_tables(bucket_name, temp_folder)

    is_streaming_layout = any(
        "_batch_id=" in key
        for keys in streaming_tables.values()
        for key in keys
    )

    if not csv_objects and not streaming_tables:
        raise ValueError(
            f"No mapped files found in {bucket_name}/{temp_folder}. "
            "Initial mapping may not have completed or files may have already been moved."
        )
    
    successfully_processed_files = []   # object keys to delete after success
    failed_tables = []

    try:
        # ── BATCH MODE: flat CSV files ────────────────────────────────────────
        if csv_objects:
            print(f"📋 Detected batch mode ({len(csv_objects)} CSV files)\n")
            for obj in csv_objects:
                table_name = obj.object_name.replace(temp_folder, "").replace(".csv", "")
                print(f"Processing table: {table_name}")

                try:
                    response = minio_client.get_object(bucket_name, obj.object_name)
                    csv_data = response.read().decode("utf-8")
                    df = pd.read_csv(StringIO(csv_data))
                    response.close()
                    response.release_conn()
                    print(f"  Loaded {len(df)} rows from {obj.object_name}")
                except Exception as e:
                    error_msg = f"Error loading {obj.object_name}: {e}"
                    print(f"  ⚠️  {error_msg}")
                    failed_tables.append(table_name)
                    updated_results["failed_mappings"].append(
                        {"table": table_name, "error": error_msg, "stage": "loading"}
                    )
                    continue

                df = _apply_mappings_to_df(df, table_name, manual_mappings,
                                            ingested_cache, updated_results,
                                            is_streaming=False,
                                            auto_mappings=auto_mappings)

                try:
                    mapped_file_name = f"mapped/{table_name}.csv"
                    csv_buffer = BytesIO()
                    df.to_csv(csv_buffer, index=False)
                    csv_buffer.seek(0)
                    minio_client.put_object(
                        bucket_name,
                        mapped_file_name,
                        csv_buffer,
                        length=len(csv_buffer.getvalue()),
                        content_type="text/csv",
                    )
                    print(f"  ✅ Saved to {mapped_file_name} ({len(df)} rows)")
                    csv_buffer.close()
                    successfully_processed_files.append(obj.object_name)
                except Exception as e:
                    error_msg = f"Error saving {mapped_file_name}: {e}"
                    print(f"  ⚠️  {error_msg}")
                    failed_tables.append(table_name)
                    updated_results["failed_mappings"].append(
                        {"table": table_name, "error": error_msg, "stage": "saving"}
                    )

        # ── PARQUET MODE: streaming (_batch_id=N/) or flat batch/db/api Delta ──
        else:
            layout_desc = "streaming" if is_streaming_layout else "batch/db/api (flat Delta/Parquet)"
            print(f"🌊 Detected {layout_desc} mode ({len(streaming_tables)} Parquet tables)\n")
            for table_name, parquet_keys in streaming_tables.items():
                print(f"Processing table: {table_name} ({len(parquet_keys)} parquet file(s))")

                # Read all parquet files for this table and concatenate
                frames = []
                load_failed = False
                for key in parquet_keys:
                    try:
                        response = minio_client.get_object(bucket_name, key)
                        data = response.read()
                        response.close()
                        response.release_conn()
                        frames.append(pd.read_parquet(BytesIO(data)))
                    except Exception as e:
                        error_msg = f"Error loading {key}: {e}"
                        print(f"  ⚠️  {error_msg}")
                        updated_results["failed_mappings"].append(
                            {"table": table_name, "error": error_msg, "stage": "loading"}
                        )
                        load_failed = True
                        break

                if load_failed or not frames:
                    failed_tables.append(table_name)
                    continue

                df = pd.concat(frames, ignore_index=True)
                print(f"  Loaded {len(df)} rows ({len(frames)} file(s))")

                # Load the _extra_cols/ companion Parquet (written by _write_spark_native
                # to preserve raw source columns that ended up in extra_df).
                extra_source_df = None
                extra_keys = streaming_extra_tables.get(table_name, [])
                if extra_keys:
                    extra_frames = []
                    for ekey in extra_keys:
                        try:
                            resp = minio_client.get_object(bucket_name, ekey)
                            edata = resp.read()
                            resp.close()
                            resp.release_conn()
                            extra_frames.append(pd.read_parquet(BytesIO(edata)))
                        except Exception as _ee:
                            print(f"  ⚠️  Could not load extra_cols file {ekey}: {_ee}")
                    if extra_frames:
                        extra_source_df = pd.concat(extra_frames, ignore_index=True)
                        print(f"  📊 Loaded {len(extra_source_df)} rows of extra source columns: "
                              f"{list(extra_source_df.columns)}")

                df = _apply_mappings_to_df(df, table_name, manual_mappings,
                                            ingested_cache, updated_results,
                                            is_streaming=is_streaming_layout,
                                            extra_source_df=extra_source_df,
                                            auto_mappings=auto_mappings)

                # Strip any legacy __src__* helper columns that may exist in older
                # mapped-temp files (deprecated approach, kept for compatibility).
                src_helper_cols = [c for c in df.columns if c.startswith("__src__")]
                if src_helper_cols:
                    df.drop(columns=src_helper_cols, inplace=True)
                    print(f"  🧹 Stripped {len(src_helper_cols)} legacy helper column(s)")

                # Preserve Spark-compatible column types so schema-merge succeeds when
                # the next CDC micro-batch is appended by Spark to the same directory.
                # Spark writes _ingested_at via current_timestamp() → TimestampType
                # (pyarrow: timestamp[us, tz=UTC]).  Pandas may drop the timezone when
                # round-tripping through read_parquet / to_parquet, so we restore it
                # explicitly here to keep the file schema identical to what Spark writes.
                if "_ingested_at" in df.columns:
                    df["_ingested_at"] = pd.to_datetime(df["_ingested_at"], utc=True)

                # Write to mapped/{table_name}/_batch_id=0/part-0.parquet
                # This matches the layout expected by cleaning_utils / Spark downstream.
                try:
                    mapped_file_name = f"mapped/{table_name}/_batch_id=0/part-0.parquet"
                    parquet_buffer = BytesIO()
                    df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
                    parquet_buffer.seek(0)
                    parquet_bytes = parquet_buffer.getvalue()
                    minio_client.put_object(
                        bucket_name,
                        mapped_file_name,
                        BytesIO(parquet_bytes),
                        length=len(parquet_bytes),
                        content_type="application/octet-stream",
                    )
                    print(f"  ✅ Saved to {mapped_file_name} ({len(df)} rows)")
                    # Schedule both main table files and _extra_cols files for cleanup.
                    successfully_processed_files.extend(parquet_keys)
                    successfully_processed_files.extend(extra_keys)
                except Exception as e:
                    error_msg = f"Error saving parquet for {table_name}: {e}"
                    print(f"  ⚠️  {error_msg}")
                    failed_tables.append(table_name)
                    updated_results["failed_mappings"].append(
                        {"table": table_name, "error": error_msg, "stage": "saving"}
                    )

    finally:
        # Remove ONLY successfully migrated source files from mapped-temp/
        print(f"\n🧹 Cleaning up temporary files...")
        if successfully_processed_files:
            print(f"  Removing {len(successfully_processed_files)} source file(s)...")
            for file_path in successfully_processed_files:
                try:
                    minio_client.remove_object(bucket_name, file_path)
                    print(f"  ✅ Removed {file_path}")
                except Exception as e:
                    print(f"  ⚠️  Error removing {file_path}: {e}")

        if failed_tables:
            print(f"\n  ⚠️  {len(failed_tables)} table(s) had errors and were NOT moved:")
            for t in failed_tables:
                print(f"     - {t}")
            print(f"  These files remain in {bucket_name}/mapped-temp/ for manual review")
    
    print(f"\n{'='*60}")
    if not failed_tables:
        print(f"✅ Manual Mappings Applied Successfully")
        print(f"   Processed {len(successfully_processed_files)} file(s) across tables")
        print(f"   Files moved from {bucket_name}/mapped-temp/ to {bucket_name}/mapped/")
    else:
        print(f"⚠️  Manual Mappings Completed with Warnings")
        print(f"   Successfully processed: {len(streaming_tables) + len(csv_objects) - len(failed_tables)} table(s)")
        print(f"   Failed: {len(failed_tables)} table(s) (remain in mapped-temp)")
    print(f"{'='*60}\n")
    
    return updated_results


def load_mapping_results_from_files(bucket_name: str, manual_mappings: dict):
    """
    Load existing mapping results and update them based on manual mappings.
    
    Args:
        bucket_name: Name of the MinIO bucket
        manual_mappings: Dictionary of manual column mappings
    
    Returns:
        dict: Updated mapping results
    """
    import redis
    
    try:
        redis_client = redis.Redis(
            host=os.getenv("REDIS_HOST", "redis"),
            port=6379,
            decode_responses=True
        )
        
        # Get existing mapping results from Redis
        mapping_results_str = redis_client.get(f"mapping_results:{bucket_name}")
        if mapping_results_str:
            mapping_results = json.loads(mapping_results_str)
        else:
            mapping_results = {"missing_cols": [], "extra_cols": []}
        
        # Update missing_cols based on manual mappings
        updated_missing_cols = []
        for missing_item in mapping_results.get("missing_cols", []):
            table = missing_item.get("table")
            column = missing_item.get("column")
            
            # Check if this column was manually mapped
            if table in manual_mappings and column in manual_mappings[table]:
                # Column was mapped, don't include it in missing anymore
                print(f"  ✅ Removing {column} from missing columns for {table}")
            else:
                # Column still missing
                updated_missing_cols.append(missing_item)
        
        mapping_results["missing_cols"] = updated_missing_cols
        
        # Save updated results back to Redis
        redis_client.setex(
            f"mapping_results:{bucket_name}",
            86400,  # 24 hours
            json.dumps(mapping_results)
        )
        
        # Clear the streaming temp folder flag so subsequent batches go to mapped/
        redis_client.delete(f"streaming_use_temp:{bucket_name}")
        print(f"   Cleared streaming temp folder flag")
        
        print(f"\n✅ Updated mapping results in Redis")
        print(f"   Remaining missing columns: {len(updated_missing_cols)}")
        
        return mapping_results
        
    except Exception as e:
        print(f"⚠️  Error updating mapping results in Redis: {e}")
        return {"missing_cols": [], "extra_cols": []}


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Apply manual mappings to mapped files")
    parser.add_argument("--bucket-name", required=True, help="MinIO bucket name")
    parser.add_argument("--manual-mappings", required=True, help="JSON string of manual mappings")
    parser.add_argument(
        "--auto-mappings", default="{}",
        help="JSON string of the automatic mapping pass's column renames "
             "(onboarding.auto_mappings) — used to recover same-table manual "
             "mappings via a primary-key join instead of row-position alignment.",
    )

    args = parser.parse_args()

    manual_mappings = json.loads(args.manual_mappings)
    auto_mappings = json.loads(args.auto_mappings)

    # Apply manual mappings to files
    apply_manual_mappings_to_files(args.bucket_name, manual_mappings, auto_mappings=auto_mappings)
    
    # Update mapping results in Redis
    load_mapping_results_from_files(args.bucket_name, manual_mappings)
