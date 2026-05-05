"""
Data cleaning module for handling duplicates, null values, and basic data quality.
"""

from pyspark.ml.feature import Imputer
import re
import pyspark.sql.functions as F
from pyspark.sql.functions import col, when, length, trim, regexp_replace, udf
from pyspark.sql.types import (
    BooleanType,
    StringType,
    IntegerType,
    LongType,
    ShortType,
    FloatType,
    DoubleType,
    DecimalType,
)


def _non_null_counts(df, columns):
    """Return a ``{column: non_null_row_count}`` mapping for the given columns."""
    if not columns:
        return {}

    counts_row = df.agg(
        *[F.count(F.when(F.col(c).isNotNull(), 1)).alias(c) for c in columns]
    ).collect()[0]
    return {c: counts_row[c] for c in columns}


def _safe_fill_defaults(df, defaults, table_name):
    """
    Filter ``defaults`` to only columns that exist and contain at least one non-null value.

    Spark ``fillna`` raises an AnalysisException if the mapping contains a column that is
    not present in the DataFrame. Columns that are entirely NULL are skipped as well so
    the pipeline can continue without inventing values for empty fields.
    """
    existing_defaults = {c: v for c, v in defaults.items() if c in df.columns}
    missing_defaults = [c for c in defaults if c not in df.columns]

    if missing_defaults:
        print(
            f"⚠️ Skipping missing columns in '{table_name}' null fill: {missing_defaults}"
        )

    if not existing_defaults:
        print(f"⚠️ No matching nullable columns found in '{table_name}' for fillna")
        return {}

    counts = _non_null_counts(df, list(existing_defaults.keys()))
    usable_defaults = {}
    all_null_columns = []

    for col_name, default_value in existing_defaults.items():
        if counts.get(col_name, 0) == 0:
            all_null_columns.append(col_name)
            continue
        usable_defaults[col_name] = default_value

    if all_null_columns:
        print(
            f"⚠️ Skipping all-NULL columns in '{table_name}' null fill: {all_null_columns}"
        )

    if not usable_defaults:
        print(f"⚠️ No eligible non-null columns found in '{table_name}' for fillna")

    return usable_defaults


def check_duplicates(dataframes):
    """
    Check for duplicate rows in all DataFrames.
    Shows actual number of duplicate row groups, not just row count difference.

    Args:
        dataframes (dict): Dictionary of table names to DataFrames
    """
    for table in dataframes.keys():
        # Skip if dataframe is None
        if dataframes[table] is None:
            print(f"⚠️ Skipping duplicate check for '{table}': dataframe is None")
            continue
        
        # Group by all columns and count occurrences
        dup_rows = (
            dataframes[table]
            .groupBy(*dataframes[table].columns)
            .count()
            .filter("count > 1")
        )
        duplicate_count = dup_rows.count()
        print(f"The number of duplicate rows in {table} is: {duplicate_count}")


def drop_duplicates(dataframes):
    """
    Remove duplicate rows from all DataFrames.

    Args:
        dataframes (dict): Dictionary of table names to DataFrames

    Returns:
        dict: Updated dictionary with duplicates removed
    """
    for table in dataframes.keys():
        # Skip if dataframe is None
        if dataframes[table] is None:
            print(f"⚠️ Skipping duplicate removal for '{table}': dataframe is None")
            continue
        
        dataframes[table] = dataframes[table].dropDuplicates()
    return dataframes


def drop_null_rows(dataframes, table, col_name):
    """
    Drop rows where a specific column is NULL.

    Args:
        dataframes (dict): Dictionary of table names to DataFrames
        table (str): Table name
        col_name (str): Column name

    Returns:
        dict: Updated dictionary
    """
    if table not in dataframes:
        print(f"⚠️ Table '{table}' not found in dataframes")
        return dataframes
    
    if dataframes[table] is None:
        print(f"⚠️ Table '{table}' is None, skipping null row removal")
        return dataframes
    
    df = dataframes[table]
    if col_name in df.columns:
        before = df.count()
        cleaned = df.filter(F.col(col_name).isNotNull())
        dataframes[table] = cleaned
        after = cleaned.count()
        print(
            f"Removed {before - after} rows from '{table}' where '{col_name}' is NULL"
        )
    else:
        print(f"Column '{col_name}' not found in '{table}'")

    return dataframes


def drop_null_keys(dataframes):
    """
    Drop rows ONLY where the PRIMARY KEY for that specific table is NULL.
    Foreign keys (e.g., customer_id in the orders table) are ignored.

    Args:
        dataframes (dict): Dictionary of table names to DataFrames

    Returns:
        dict: Updated dictionary
    """
    
    # Map each table to its specific Primary Key
    # This ensures we don't drop an Order just because it has no Customer ID yet
    primary_key_map = {
        "customers": "customer_id",
        "orders": "order_id",
        "order_items": "order_item_id",
        "products": "product_id",
        "suppliers": "supplier_id",
        "payments": "payment_id",
        "marketing_campaigns": "campaign_id",
        "shopping_cart": "cart_id",
        "cart_items": "cart_item_id",
        "inventory": "inventory_id",
        "reviews": "review_id",
        "wishlist": "wishlist_id",
        "addresses": "address_id",
        "customer_sessions": "session_id",
    }

    for table in dataframes.keys():
        # Skip if dataframe is None
        if dataframes[table] is None:
            print(f"⚠️ Skipping null key removal for '{table}': dataframe is None")
            continue
        
        # 1. Check if we have a defined Primary Key for this table
        if table in primary_key_map:
            pk_column = primary_key_map[table]
            
            # 2. Only drop rows if the PK column actually exists in the dataframe
            if pk_column in dataframes[table].columns:
                print(f"🔒 Checking Primary Key '{pk_column}' for table '{table}'...")
                dataframes = drop_null_rows(dataframes, table, pk_column)
            else:
                print(f"⚠️ Primary Key '{pk_column}' defined for '{table}' but column not found.")
        else:
            # Optional: Print if a table has no PK defined in our map
            pass

    return dataframes

def check_nulls(dataframes):
    """
    Check for NULL values in all columns of all DataFrames.

    Args:
        dataframes (dict): Dictionary of table names to DataFrames
    """
    for table_name, df in dataframes.items():
        # Skip if dataframe is None
        if df is None:
            print(f"⚠️ Skipping null check for '{table_name}': dataframe is None")
            continue
        
        null_counts = df.select(
            [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]
        )
        null_counts.show()


def fill_null_values(dataframes):
    """
    Fill NULL values in non-numeric columns with appropriate defaults.

    Args:
        dataframes (dict): Dictionary of table names to DataFrames

    Returns:
        dict: Updated dictionary with filled values
    """
    if "customers" in dataframes.keys() and dataframes["customers"] is not None:
        defaults = {
            "gender": "",
            "account_status": "",
            "city": "",
            "state_province": "",
            "postal_code": "00000",
            "country": "",
            "date_of_birth": "1900-01-01",
            "account_created_at": "1900-01-01",
            "last_login_date": "1900-01-01",
            "is_active": "false",
        }
        safe_defaults = _safe_fill_defaults(dataframes["customers"], defaults, "customers")
        if safe_defaults:
            dataframes["customers"] = dataframes["customers"].fillna(safe_defaults)
    else:
        print("⚠️ Customers DataFrame is missing or None.")

    if "suppliers" in dataframes.keys() and dataframes["suppliers"] is not None:
        defaults = {
            "supplier_rating": 0.0,
            "supplier_status": "",
            "is_preferred":  "false",
            "is_verified": "false",
            "contract_start_date": "1900-01-01",
            "contract_end_date": "1900-01-01",
            "city": "",
            "state":  "",
            "zip_code": "00000",
            "country": "",
        }
        safe_defaults = _safe_fill_defaults(dataframes["suppliers"], defaults, "suppliers")
        if safe_defaults:
            dataframes["suppliers"] = dataframes["suppliers"].fillna(safe_defaults)
    else:
        print("⚠️ Suppliers DataFrame is missing or None.")

    if "products" in dataframes.keys() and dataframes["products"] is not None:
        defaults = {
            "product_name": "",
            "sku": "",
            "category": "",
            "sub_category": "",
            "brand": "",
            "launch_date": "1900-01-01",
            "weight": "0.0",
            "dimensions": "",
            "color": "",
            "size": "",
            "material": "",
        }
        safe_defaults = _safe_fill_defaults(dataframes["products"], defaults, "products")
        if safe_defaults:
            dataframes["products"] = dataframes["products"].fillna(safe_defaults)
    else:
        print("⚠️ Products DataFrame is missing or None.")

    if "inventory" in dataframes.keys() and dataframes["inventory"] is not None:
        defaults = {"last_restocked_date": "1900-01-01"}
        safe_defaults = _safe_fill_defaults(dataframes["inventory"], defaults, "inventory")
        if safe_defaults:
            dataframes["inventory"] = dataframes["inventory"].fillna(safe_defaults)
    else:
        print("⚠️ Inventory DataFrame is missing or None.")

    if "shopping_cart" in dataframes.keys() and dataframes["shopping_cart"] is not None:
        defaults = {
            "cart_status": "",
            "created_at": "1900-01-01",
            "updated_at": "1900-01-01",
        }
        safe_defaults = _safe_fill_defaults(dataframes["shopping_cart"], defaults, "shopping_cart")
        if safe_defaults:
            dataframes["shopping_cart"] = dataframes["shopping_cart"].fillna(safe_defaults)
    else:
        print("⚠️ Shopping_cart DataFrame is missing or None.")

    if "cart_items" in dataframes.keys() and dataframes["cart_items"] is not None:
        defaults = {
            "item_status": "",
            "added_at": "1900-01-01",
            "updated_at": "1900-01-01",
        }
        safe_defaults = _safe_fill_defaults(dataframes["cart_items"], defaults, "cart_items")
        if safe_defaults:
            dataframes["cart_items"] = dataframes["cart_items"].fillna(safe_defaults)
    else:
        print("⚠️ Cart_items DataFrame is missing or None.")

    if "orders" in dataframes.keys() and dataframes["orders"] is not None:
        defaults = {
            "order_status": "",
            "currency": "",
            "order_placed_at": "1900-01-01",
        }
        safe_defaults = _safe_fill_defaults(dataframes["orders"], defaults, "orders")
        if safe_defaults:
            dataframes["orders"] = dataframes["orders"].fillna(safe_defaults)
    else:
        print("⚠️ Orders DataFrame is missing or None.")

    if "payments" in dataframes.keys() and dataframes["payments"] is not None:
        defaults = {
            "payment_method": "",
            "payment_provider": "",
            "payment_status": "",
            "transaction_id": "",
            "payment_date": "1900-01-01",
        }
        safe_defaults = _safe_fill_defaults(dataframes["payments"], defaults, "payments")
        if safe_defaults:
            dataframes["payments"] = dataframes["payments"].fillna(safe_defaults)
    else:
        print("⚠️ Payments DataFrame is missing or None.")

    if "reviews" in dataframes.keys() and dataframes["reviews"] is not None:
        defaults = {
            "review_title": "",
            "review_desc": "",
            "review_date": "1900-01-01",
        }
        safe_defaults = _safe_fill_defaults(dataframes["reviews"], defaults, "reviews")
        if safe_defaults:
            dataframes["reviews"] = dataframes["reviews"].fillna(safe_defaults)
    else:
        print("⚠️ Reviews DataFrame is missing or None.")

    if "marketing_campaigns" in dataframes.keys() and dataframes["marketing_campaigns"] is not None:
        defaults = {
            "campaign_name": "",
            "campaign_type": "",
            "start_date": "1900-01-01",
            "target_audience": "",
            "campaign_status": "",
        }
        safe_defaults = _safe_fill_defaults(
            dataframes["marketing_campaigns"], defaults, "marketing_campaigns"
        )
        if safe_defaults:
            dataframes["marketing_campaigns"] = dataframes["marketing_campaigns"].fillna(safe_defaults)
    else:
        print("⚠️ Marketing_campaigns DataFrame is missing or None.")

    if "customer_sessions" in dataframes.keys() and dataframes["customer_sessions"] is not None:
        defaults = {
            "session_start":  "1900-01-01",
            "session_end": "1900-01-01",
            "device_type": "",
            "referrer_source": "",
            "conversion_flag": "false",
            "cart_abandonment_flag": "false",
        }
        safe_defaults = _safe_fill_defaults(
            dataframes["customer_sessions"], defaults, "customer_sessions"
        )
        if safe_defaults:
            dataframes["customer_sessions"] = dataframes["customer_sessions"].fillna(safe_defaults)
    else:
        print("⚠️ Customer_sessions DataFrame is missing or None.")

    return dataframes


def impute_missing_values(dataframes, table, numeric_cols):
    """
    Impute missing numeric values using median strategy.
    Skips columns that are missing or entirely NULL.

    Args:
        dataframes (dict): Dictionary of table names to DataFrames
        table (str): Table name
        numeric_cols (list): List of numeric column names

    Returns:
        dict: Updated dictionary
    """
    # Skip if dataframe doesn't exist or is None
    if table not in dataframes or dataframes[table] is None:
        print(f"⚠️ Skipping imputation for '{table}': dataframe not found or is None")
        return dataframes
    
    df = dataframes[table]
    # Single Spark action: count total rows + non-null counts for all numeric cols at once
    agg_result = df.agg(
        F.count("*").alias("__total__"),
        *[F.count(F.col(c)).alias(c) for c in numeric_cols]
    ).collect()[0]
    total_rows = agg_result["__total__"]
    print(f"Total rows: {total_rows}")
    non_null_counts = agg_result

    valid_cols = []
    all_null_cols = []

    for col_name in numeric_cols:
        non_null_count = non_null_counts[col_name]

        if non_null_count == 0:
            all_null_cols.append(col_name)
        elif non_null_count < total_rows:
            valid_cols.append(col_name)
            null_count = total_rows - non_null_count
            print(
                f"✅ {col_name}: {non_null_count} non-null, {null_count} null - will impute"
            )

    if all_null_cols:
        print(f"\nAll-NULL columns skipped: {all_null_cols}")
    print(f"Valid columns for imputation: {valid_cols}")

    # Impute valid columns with median
    if valid_cols:
        imputer = Imputer(
            inputCols=valid_cols, outputCols=valid_cols, strategy="median"
        )

        model = imputer.fit(df)
        df_imputed = model.transform(df)
        dataframes[table] = df_imputed
        print(f"✅ Successfully imputed columns with median: {valid_cols}")
    else:
        print("⚠️ No valid columns found for imputation")

    print("\n" + "=" * 50)
    print(f"🔍 Final check for NULL values in {table}:")
    print("=" * 50)

    return dataframes


def impute_all_numeric(dataframes):
    """
    Impute all numeric columns across all DataFrames.

    Args:
        dataframes (dict): Dictionary of table names to DataFrames

    Returns:
        dict:  Updated dictionary
    """
    all_ids = [
        "session_id",
        "customer_id",
        "address_id",
        "product_id",
        "supplier_id",
        "order_id",
        "order_item_id",
        "payment_id",
        "campaign_id",
        "cart_id",
        "cart_item_id",
        "inventory_id",
        "review_id",
        "wishlist_id",
    ]

    for table in dataframes.keys():
        # Skip if dataframe is None
        if dataframes[table] is None:
            print(f"⚠️ Skipping imputation for '{table}': dataframe is None")
            continue
        
        numeric_cols = [
            field.name
            for field in dataframes[table].schema.fields
            if isinstance(
                field.dataType,
                (IntegerType, LongType, FloatType, DoubleType, DecimalType),
            )
        ]
        numeric_cols = [col for col in numeric_cols if col not in all_ids]

        if numeric_cols:
            print(f"\nImputing missing values for table:  {table}")
            dataframes = impute_missing_values(dataframes, table, numeric_cols)
        else:
            print(f"\nNo numeric columns found in table: {table}, skipping imputation.")

    return dataframes


def clean_text_columns(dataframes):
    """
    Clean gibberish from text columns across all tables using linguistic analysis.

    Args:
        dataframes (dict): Dictionary of table names to DataFrames

    Returns:
        dict: Updated dictionary with cleaned text
    """

    def is_gibberish_text(text):
        """Detect gibberish strings based on character ratios and patterns."""
        if not text or len(str(text)) < 3:
            return False

        text = str(text).strip()
        text_lower = text.lower()

        # Skip numeric values
        if re.match(r'^[\d\.\,\s]+$', text):
            return False

        # Skip single word abbreviations (S, M, L, XL, XXL, etc.)
        if len(text) <= 4 and text.isupper():
            return False

        # Skip common multi-word phrases with spaces
        if ' ' in text and len(text.split()) >= 2:
            words = text.split()
            valid_words = 0
            for word in words: 
                if len(word) >= 2:
                    vowels = len(re.findall(r"[aeiou]", word.lower()))
                    if vowels > 0:
                        valid_words += 1
            if valid_words >= len(words) * 0.7:
                return False

        # Skip short strings (likely valid abbreviations or codes)
        if len(text) <= 6: 
            return False

        vowels = len(re.findall(r"[aeiou]", text_lower))
        vowel_ratio = vowels / len(text)

        # More lenient vowel ratio check
        if vowel_ratio < 0.10 or vowel_ratio > 0.75:
            return True

        # Check for excessive consonant clusters (6+ in a row)
        if re.search(r"[bcdfghjklmnpqrstvwxyz]{6,}", text_lower):
            return True

        # Check for repeating patterns (same char 5+ times)
        if re.search(r"(.)\1{4,}", text_lower):
            return True

        return False

    is_gibberish_udf = udf(is_gibberish_text, BooleanType())

    print("\n" + "=" * 60)
    print("📝 CLEANING TEXT COLUMNS FOR GIBBERISH")
    print("=" * 60)

    # Patterns to identify columns that should NOT be cleaned
    skip_patterns = [
        "id",
        "key",
        "sku",
        "code",
        "zip",
        "postal",
        "dimension",
        "transaction",
    ]

    for table_name, df in dataframes.items():
        print(f"\n🔍 Checking {table_name}...")

        # Get string columns, excluding IDs and codes
        string_cols = [
            field.name
            for field in df.schema.fields
            if isinstance(field.dataType, StringType)
            and not any(pattern in field.name.lower() for pattern in skip_patterns)
        ]

        if not string_cols:
            print(f"  ℹ️ No eligible string columns in {table_name}, skipping text cleaning")
            continue

        counts = _non_null_counts(df, string_cols)
        string_cols = [col_name for col_name in string_cols if counts.get(col_name, 0) > 0]

        if not string_cols:
            print(f"  ℹ️ All eligible string columns in {table_name} are NULL, skipping")
            continue

        df = df.cache()  # cache before UDF loop to prevent full re-evaluation per column
        for col_name in string_cols:
            df = df.withColumn(
                col_name,
                when(is_gibberish_udf(col(col_name)), F.lit(None)).otherwise(
                    col(col_name)
                ),
            )

        dataframes[table_name] = df

    print("\n" + "=" * 60)
    print("✅ TEXT CLEANING COMPLETED")
    print("=" * 60)

    return dataframes


def clean_numeric_strings(dataframes):
    """
    Clean string columns with numeric formatting issues.

    Operations performed:
    1.Remove leading zeros from ID columns
    2.Validate integer columns for non-numeric values
    3.Validate decimal/float columns
    4.Convert numeric status codes to text (1→"Active", 0→"Inactive", 2→"Pending")
    5.Clean transaction IDs and SKUs (remove special characters)
    6.Trim numeric string columns

    Args:
        dataframes (dict): Dictionary of table names to DataFrames

    Returns:
        dict: Updated dictionary with cleaned numeric strings
    """
    print("\n" + "=" * 60)
    print("🔢 CLEANING NUMERIC STRING COLUMNS")
    print("=" * 60)

    # 1.Clean all ID columns - remove leading zeros
    for table_name, df in dataframes.items():
        id_columns = [col_name for col_name in df.columns if col_name.endswith("_id")]

        for col_name in id_columns:
            # Remove leading zeros
            df = df.withColumn(
                col_name, regexp_replace(col(col_name), r"^0+(?=\\d)", "")
            )

        if id_columns:
            dataframes[table_name] = df
            print(f"✅ Cleaned {len(id_columns)} ID columns in {table_name}")

    # 2.Clean all integer columns that might have text values
    for table_name, df in dataframes.items():
        integer_cols = [
            field.name
            for field in df.schema.fields
            if isinstance(field.dataType, (IntegerType, LongType, ShortType))
        ]

        for col_name in integer_cols:
            # Skip ID columns (already handled)
            if col_name.endswith("_id"):
                continue

            # Always apply without count guard
            default_value = 1 if "quantity" in col_name.lower() else 0

            df = df.withColumn(
                col_name,
                when(
                    col(col_name).cast("string").rlike("^-?[0-9]+$"),
                    col(col_name),
                ).otherwise(default_value),
            )

        dataframes[table_name] = df

    # 3.Clean all decimal/float columns
    for table_name, df in dataframes.items():
        decimal_cols = [
            field.name
            for field in df.schema.fields
            if isinstance(field.dataType, (FloatType, DoubleType, DecimalType))
        ]

        for col_name in decimal_cols:
            # Always apply without count guard
            df = df.withColumn(
                col_name,
                when(
                    col(col_name).cast("string").rlike("^-?[0-9]+(\\.[0-9]+)?$"),
                    col(col_name),
                ).otherwise(0.0),
            )

        dataframes[table_name] = df

    # 4.Handle columns with numeric status codes (convert to text)
    for table_name, df in dataframes.items():
        status_columns = [
            col_name for col_name in df.columns if col_name.endswith("_status")
        ]

        for status_col in status_columns: 
            # Check if status column has numeric values like "0", "1"
            # Always apply without count guard
            df = df.withColumn(
                status_col,
                when(col(status_col) == "1", "Active")
                .when(col(status_col) == "0", "Inactive")
                .when(col(status_col) == "2", "Pending")
                .otherwise(col(status_col)),
            )

        dataframes[table_name] = df

    # 5.Clean transaction/reference IDs - remove special characters
    for table_name, df in dataframes.items():
        transaction_cols = [
            col_name
            for col_name in df.columns
            if "transaction" in col_name.lower() or col_name == "sku"
        ]

        for col_name in transaction_cols: 
            df = df.withColumn(
                col_name, regexp_replace(col(col_name), "[^a-zA-Z0-9-]", "")
            )

        if transaction_cols:
            dataframes[table_name] = df
            print(f"✅ Cleaned transaction IDs in {table_name}")

    # 6.Trim all numeric string columns
    for table_name, df in dataframes.items():
        string_cols = [
            field.name
            for field in df.schema.fields
            if isinstance(field.dataType, StringType)
        ]

        for col_name in string_cols:
            # Check if column looks numeric
            sample_value = df.select(col_name).filter(col(col_name).isNotNull()).first()
            if sample_value and sample_value[0]: 
                if re.match(r"^[\d\s.,-]+$", str(sample_value[0])):
                    # Trim whitespace from numeric strings
                    df = df.withColumn(col_name, trim(col(col_name)))

        dataframes[table_name] = df

    print("=" * 60)
    print("✅ NUMERIC STRING CLEANUP COMPLETED")
    print("=" * 60)

    return dataframes


def clean_whitespace_issues(dataframes):
    """
    Remove excessive whitespace and formatting issues from all string columns.

    Operations performed:
    1.Trim leading/trailing whitespace
    2.Replace multiple spaces with single space
    3.Remove trailing special characters (*)
    4.Remove leading special characters (*)
    5.Clean up excessive quotes
    6.Remove trailing quotes

    Args:
        dataframes (dict): Dictionary of table names to DataFrames

    Returns: 
        dict: Updated dictionary with cleaned whitespace
    """
    print("\n" + "=" * 60)
    print("🧹 CLEANING WHITESPACE AND FORMATTING")
    print("=" * 60)

    for table_name, df in dataframes.items():
        # Get all string columns
        string_cols = [
            field.name
            for field in df.schema.fields
            if isinstance(field.dataType, StringType)
        ]

        if not string_cols:
            continue

        counts = _non_null_counts(df, string_cols)
        string_cols = [col_name for col_name in string_cols if counts.get(col_name, 0) > 0]

        if not string_cols:
            print(f"ℹ️ All string columns in {table_name} are NULL, skipping whitespace cleanup")
            continue

        for col_name in string_cols: 
            # 1.Trim leading/trailing whitespace
            df = df.withColumn(col_name, trim(col(col_name)))

            # 2.Replace multiple spaces with single space
            df = df.withColumn(col_name, regexp_replace(col(col_name), "\\s+", " "))

            # 3.Remove trailing special characters
            df = df.withColumn(col_name, regexp_replace(col(col_name), "[*]+$", ""))

            # 4.Remove leading special characters
            df = df.withColumn(col_name, regexp_replace(col(col_name), "^[*]+", ""))

            # 5.Clean up excessive quotes
            df = df.withColumn(col_name, regexp_replace(col(col_name), '"{2,}', '"'))

            # 6.Remove trailing quotes
            df = df.withColumn(col_name, regexp_replace(col(col_name), '"+$', ""))

        dataframes[table_name] = df
        print(f"✅ Cleaned {len(string_cols)} string columns in {table_name}")

    print("=" * 60)
    print("✅ WHITESPACE CLEANUP COMPLETED")
    print("=" * 60)

    return dataframes


def clean_mixed_scripts(dataframes):
    """
    Remove non-ASCII characters from all text columns with context-aware replacements.

    Detects non-ASCII characters and replaces them with appropriate defaults based on
    column type (e.g., "Unknown" for names, "No Title" for titles, etc.)

    Args:
        dataframes (dict): Dictionary of table names to DataFrames

    Returns: 
        dict: Updated dictionary with ASCII-only text
    """
    print("\n" + "=" * 60)
    print("🌐 CLEANING MIXED SCRIPTS AND NON-ASCII CHARACTERS")
    print("=" * 60)

    for table_name, df in dataframes.items():
        # Get all string columns
        string_cols = [
            field.name
            for field in df.schema.fields
            if isinstance(field.dataType, StringType)
        ]

        if not string_cols:
            continue

        counts = _non_null_counts(df, string_cols)
        string_cols = [col_name for col_name in string_cols if counts.get(col_name, 0) > 0]

        if not string_cols:
            print(f"ℹ️ All string columns in {table_name} are NULL, skipping mixed-script cleanup")
            continue

        # Skip ID columns and codes
        text_cols = [
            col_name
            for col_name in string_cols
            if not col_name.endswith("_id") and "code" not in col_name.lower()
        ]

        if not text_cols:
            continue

        print(f"\n  🔧 Processing {table_name}...")

        for col_name in text_cols:
            # Determine replacement value based on column type
            replacement_value = "Unknown"
            if "name" in col_name.lower():
                replacement_value = "Unknown"
            elif "title" in col_name.lower():
                replacement_value = "No Title"
            elif "desc" in col_name.lower():
                replacement_value = "No description"
            elif "city" in col_name.lower():
                replacement_value = "Unknown"
            elif "state" in col_name.lower() or "province" in col_name.lower():
                replacement_value = "Unknown"
            elif "country" in col_name.lower():
                replacement_value = "Unknown"

            # Replace non-ASCII characters (no count guard needed)
            df = df.withColumn(
                col_name,
                when(
                    col(col_name).rlike(".*[^\\x00-\\x7f].*"), replacement_value
                ).otherwise(col(col_name)),
            )

        dataframes[table_name] = df

    print("\n" + "=" * 60)
    print("✅ MIXED SCRIPTS CLEANUP COMPLETED")
    print("=" * 60)

    return dataframes


def validate_all_cleaned_data(dataframes):
    """
    Final validation of all cleaned data - quality assurance checks.

    Checks for:
    - Excessive special characters (2+ in a row)
    - Excessive whitespace (3+ spaces)
    - Non-ASCII characters in important columns

    Args:
        dataframes (dict): Dictionary of table names to DataFrames

    Returns:
        dict: Same dictionary (validation only, no modifications)
    """
    print("\n" + "=" * 60)
    print("🔍 FINAL DATA VALIDATION")
    print("=" * 60)

    issues_found = False

    # Check for gibberish patterns in all text columns
    for table_name, df in dataframes.items():
        string_cols = [
            field.name
            for field in df.schema.fields
            if isinstance(field.dataType, StringType)
        ]

        if not string_cols:
            continue

        counts = _non_null_counts(df, string_cols)
        string_cols = [col_name for col_name in string_cols if counts.get(col_name, 0) > 0]

        if not string_cols:
            continue

        for col_name in string_cols: 
            # Check for excessive special characters
            special_char_count = df.filter(
                col(col_name).rlike(".*[*@#$%^&]{2,}.*")
            ).count()

            if special_char_count > 0:
                print(
                    f"⚠️  {table_name}.{col_name}:  {special_char_count} rows with multiple special characters"
                )
                issues_found = True

            # Check for excessive whitespace
            whitespace_count = df.filter(col(col_name).rlike(".*\\s{3,}.*")).count()

            if whitespace_count > 0:
                print(
                    f"⚠️  {table_name}.{col_name}: {whitespace_count} rows with excessive whitespace"
                )
                issues_found = True

            # Check for non-ASCII in important columns
            if col_name in [
                "city",
                "country",
                "state",
                "state_province",
                "product_name",
                "brand",
            ]:
                non_ascii_count = df.filter(
                    col(col_name).rlike(".*[^\\x00-\\x7f].*")
                ).count()

                if non_ascii_count > 0:
                    print(
                        f"⚠️  {table_name}.{col_name}: {non_ascii_count} rows with non-ASCII characters"
                    )
                    issues_found = True

    if not issues_found:
        print("✅ All data passed validation checks!")
    else:
        print("\n⚠️  Some issues found - review the warnings above")

    print("=" * 60)

    return dataframes