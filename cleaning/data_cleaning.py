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
        "wishlists": "wishlist_id",
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
        dataframes["customers"] = dataframes["customers"].fillna(
            {
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
        )
    else:
        print("⚠️ Customers DataFrame is missing or None.")

    if "suppliers" in dataframes.keys() and dataframes["suppliers"] is not None:
        dataframes["suppliers"] = dataframes["suppliers"].fillna(
            {
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
        )
    else:
        print("⚠️ Suppliers DataFrame is missing or None.")

    if "products" in dataframes.keys() and dataframes["products"] is not None:
        dataframes["products"] = dataframes["products"].fillna(
            {
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
        )
    else:
        print("⚠️ Products DataFrame is missing or None.")

    if "inventory" in dataframes.keys() and dataframes["inventory"] is not None:
        dataframes["inventory"] = dataframes["inventory"].fillna(
            {"last_restocked_date": "1900-01-01"}
        )
    else:
        print("⚠️ Inventory DataFrame is missing or None.")

    if "shopping_cart" in dataframes.keys() and dataframes["shopping_cart"] is not None:
        dataframes["shopping_cart"] = dataframes["shopping_cart"].fillna(
            {
                "cart_status": "",
                "created_at": "1900-01-01",
                "updated_at": "1900-01-01",
            }
        )
    else:
        print("⚠️ Shopping_cart DataFrame is missing or None.")

    if "cart_items" in dataframes.keys() and dataframes["cart_items"] is not None:
        dataframes["cart_items"] = dataframes["cart_items"].fillna(
            {
                "item_status": "",
                "added_at": "1900-01-01",
                "updated_at": "1900-01-01",
            }
        )
    else:
        print("⚠️ Cart_items DataFrame is missing or None.")

    if "orders" in dataframes.keys() and dataframes["orders"] is not None:
        dataframes["orders"] = dataframes["orders"].fillna(
            {
                "order_status": "",
                "currency": "",
                "order_placed_at": "1900-01-01",
            }
        )
    else:
        print("⚠️ Orders DataFrame is missing or None.")

    if "payments" in dataframes.keys() and dataframes["payments"] is not None:
        dataframes["payments"] = dataframes["payments"].fillna(
            {
                "payment_method": "",
                "payment_provider": "",
                "payment_status": "",
                "transaction_id": "",
                "payment_date": "1900-01-01",
            }
        )
    else:
        print("⚠️ Payments DataFrame is missing or None.")

    if "reviews" in dataframes.keys() and dataframes["reviews"] is not None:
        dataframes["reviews"] = dataframes["reviews"].fillna(
            {
                "review_title": "",
                "review_desc": "",
                "review_date": "1900-01-01",
            }
        )
    else:
        print("⚠️ Reviews DataFrame is missing or None.")

    if "marketing_campaigns" in dataframes.keys() and dataframes["marketing_campaigns"] is not None:
        dataframes["marketing_campaigns"] = dataframes["marketing_campaigns"].fillna(
            {
                "campaign_name": "",
                "campaign_type": "",
                "start_date": "1900-01-01",
                "target_audience": "",
                "campaign_status": "",
            }
        )
    else:
        print("⚠️ Marketing_campaigns DataFrame is missing or None.")

    if "customer_sessions" in dataframes.keys() and dataframes["customer_sessions"] is not None:
        dataframes["customer_sessions"] = dataframes["customer_sessions"].fillna(
            {
                "session_start":  "1900-01-01",
                "session_end": "1900-01-01",
                "device_type": "",
                "referrer_source": "",
                "conversion_flag": "false",
                "cart_abandonment_flag": "false",
            }
        )
    else:
        print("⚠️ Customer_sessions DataFrame is missing or None.")

    return dataframes


def impute_missing_values(dataframes, table, numeric_cols):
    """
    Impute missing numeric values using median strategy.
    Handles all-NULL columns by filling with 0 first.

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
    total_rows = df.count()
    print(f"Total rows: {total_rows}")

    # Get non-null counts for all columns at once
    non_null_counts = df.select(
        [F.count(F.col(c)).alias(c) for c in numeric_cols]
    ).collect()[0]

    valid_cols = []
    all_null_cols = []

    for col_name in numeric_cols:
        non_null_count = non_null_counts[col_name]

        if non_null_count == 0:
            all_null_cols.append(col_name)
            print(f"🚫 {col_name}:  ALL NULL - will fill with 0")
        elif non_null_count < total_rows:
            valid_cols.append(col_name)
            null_count = total_rows - non_null_count
            print(
                f"✅ {col_name}: {non_null_count} non-null, {null_count} null - will impute"
            )

    print(f"\nAll-NULL columns: {all_null_cols}")
    print(f"Valid columns for imputation: {valid_cols}")

    # Fill all-NULL columns with 0
    if all_null_cols:
        fill_dict = {col: 0 for col in all_null_cols}
        df = df.fillna(fill_dict)
        dataframes[table] = df
        print(f"✅ Filled all-NULL columns with 0:  {all_null_cols}")

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

        for col_name in string_cols: 
            gibberish_count = df.filter(is_gibberish_udf(col(col_name))).count()

            if gibberish_count > 0:
                df = df.withColumn(
                    col_name,
                    when(is_gibberish_udf(col(col_name)), F.lit(None)).otherwise(
                        col(col_name)
                    ),
                )
                print(f"  ✅ Fixed {gibberish_count} gibberish values in {col_name}")

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

            # Check if there are non-numeric values
            non_numeric_count = df.filter(
                ~col(col_name).cast("string").rlike("^-?[0-9]+$")
            ).count()

            if non_numeric_count > 0:
                # Determine default value based on column name
                default_value = 1 if "quantity" in col_name.lower() else 0

                df = df.withColumn(
                    col_name,
                    when(
                        col(col_name).cast("string").rlike("^-?[0-9]+$"),
                        col(col_name),
                    ).otherwise(default_value),
                )
                print(
                    f"  ✅ Cleaned {non_numeric_count} non-numeric values in {table_name}.{col_name}"
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
            # Check for non-numeric text values
            non_numeric_count = df.filter(
                ~col(col_name).cast("string").rlike("^-?[0-9]+(\\.[0-9]+)?$")
            ).count()

            if non_numeric_count > 0:
                df = df.withColumn(
                    col_name,
                    when(
                        col(col_name).cast("string").rlike("^-?[0-9]+(\\.[0-9]+)?$"),
                        col(col_name),
                    ).otherwise(0.0),
                )
                print(
                    f"  ✅ Cleaned {non_numeric_count} non-numeric values in {table_name}.{col_name}"
                )

        dataframes[table_name] = df

    # 4.Handle columns with numeric status codes (convert to text)
    for table_name, df in dataframes.items():
        status_columns = [
            col_name for col_name in df.columns if col_name.endswith("_status")
        ]

        for status_col in status_columns: 
            # Check if status column has numeric values like "0", "1"
            numeric_status_count = df.filter(col(status_col).rlike("^[0-9]$")).count()

            if numeric_status_count > 0:
                # Convert common numeric codes to text
                df = df.withColumn(
                    status_col,
                    when(col(status_col) == "1", "Active")
                    .when(col(status_col) == "0", "Inactive")
                    .when(col(status_col) == "2", "Pending")
                    .otherwise(col(status_col)),
                )
                print(
                    f"  ✅ Converted {numeric_status_count} numeric status codes in {table_name}.{status_col}"
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
            # Count non-ASCII before cleaning
            non_ascii_count = df.filter(
                col(col_name).rlike(".*[^\\x00-\\x7f].*")
            ).count()

            if non_ascii_count > 0:
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

                # Replace non-ASCII characters
                df = df.withColumn(
                    col_name,
                    when(
                        col(col_name).rlike(".*[^\\x00-\\x7f].*"), replacement_value
                    ).otherwise(col(col_name)),
                )

                print(
                    f"    ✅ Cleaned {non_ascii_count} rows with non-ASCII in {col_name}"
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