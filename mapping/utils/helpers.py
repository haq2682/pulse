import os
import datetime
from difflib import get_close_matches


def normalize_name(name):
    """
    Normalize dataframe name to match df_to_table keys.

    Args:
        name: Name to normalize

    Returns:
        Normalized name or None if no match
    """
    from map import df_to_table

    name = name.lower().replace("_df", "")
    name = os.path.splitext(name)[0]

    possible_keys = list(df_to_table.keys())
    close = get_close_matches(name, possible_keys, n=1, cutoff=0.6)
    if close:
        return close[0]

    return None


def get_table_name(df_name):
    """
    Get canonical table name from dataframe name.

    Args:
        df_name: Dataframe name

    Returns:
        Table name or None
    """
    from map import df_to_table

    norm = normalize_name(df_name)
    return df_to_table.get(norm, None)


def safe_serialize(obj):
    """
    Convert objects to JSON-safe types.

    Args:
        obj: Object to serialize

    Returns:
        JSON-safe representation
    """
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    if isinstance(obj, set):
        return list(obj)
    return str(obj)


def make_json_safe(data):
    """
    Recursively convert all values in a dict or list to JSON-safe types.

    Args:
        data: Data structure to convert

    Returns:
        JSON-safe data structure
    """
    if isinstance(data, dict):
        return {k: make_json_safe(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [make_json_safe(v) for v in data]
    else:
        return safe_serialize(data)


def detect_table(df, columns_info, threshold=0.5):
    """
    Detect which table a dataframe corresponds to based on column overlap.

    Args:
        df: Spark DataFrame
        columns_info: List of (table, column, type) tuples
        threshold: Minimum overlap ratio

    Returns:
        Table name or None
    """
    df_cols = set(df.columns)
    table_scores = {}

    for table, col, _ in columns_info:
        if table not in table_scores:
            table_scores[table] = 0
        if col in df_cols:
            table_scores[table] += 1

    best_table = max(table_scores, key=table_scores.get)
    overlap_ratio = table_scores[best_table] / max(1, len(df_cols))

    if overlap_ratio >= threshold:
        return best_table
    return None


def split_unified_dataframe(df, columns_info):
    """
    Splits a unified dataframe into per-table sub-dataframes
    based on the strict rule that columns are named `table__column`.

    Args:
        df: Spark DataFrame with unified schema
        columns_info: List of (table, column, type) tuples

    Returns:
        Dictionary of {table_name: dataframe}
    """
    # Build canonical column set per table for validation
    canonical_schema = {
        table: {col for t, col, _ in columns_info if t == table}
        for table, _, _ in columns_info
    }

    table_to_cols = {}

    for col in df.columns:
        if "__" not in col:
            raise ValueError(
                f"❌ Invalid column '{col}'. All columns must follow 'table__column' format."
            )

        table, column = col.split("__", 1)

        if table not in canonical_schema:
            raise ValueError(f"❌ Unknown table '{table}' in column '{col}'.")

        if column not in canonical_schema[table]:
            raise ValueError(
                f"❌ Unknown column '{column}' for table '{table}' (from '{col}')."
            )

        table_to_cols.setdefault(table, []).append((col, column))

    # Build sub-dataframes
    split_dfs = {}
    for table, col_pairs in table_to_cols.items():
        renamed = [df[c].alias(new_c) for c, new_c in col_pairs]
        split_dfs[table] = df.select(*renamed)

    return split_dfs
