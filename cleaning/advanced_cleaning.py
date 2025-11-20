"""
Advanced data cleaning module for text quality, formatting, and validation.

This module provides sophisticated cleaning operations including:
- Text gibberish detection using linguistic analysis
- Numeric string validation and formatting
- Whitespace and formatting cleanup
- Mixed scripts and non-ASCII character handling
- Final data quality validation
"""

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


def is_gibberish_text(text):
    """
    UDF to detect gibberish strings based on character ratios and patterns.
    
    Checks:
    - Vowel ratio (English text typically has 30-40% vowels)
    - Excessive consonant clusters (4+ in a row)
    - Character repetition patterns (same char 4+ times)
    
    Args:
        text: Input text string
        
    Returns:
        bool: True if text appears to be gibberish, False otherwise
    """
    if not text or text in ["Unknown", "NULL", None]:
        return False

    text = str(text).lower()

    # Skip very short text
    if len(text) < 3:
        return False

    vowels = len(re.findall(r'[aeiou]', text))

    if len(text) > 3:
        vowel_ratio = vowels / len(text)
        # English text typically has 30-40% vowels
        if vowel_ratio < 0.15 or vowel_ratio > 0.7:
            return True

        # Check for excessive consonant clusters (4+ in a row)
        if re.search(r'[bcdfghjklmnpqrstvwxyz]{4,}', text):
            return True

        # Check for repeating patterns (same char 4+ times)
        if re.search(r'(.)\1{3,}', text):
            return True

    return False


# Register UDF
is_gibberish_udf = udf(is_gibberish_text, BooleanType())


def clean_text_columns(dataframes):
    """
    Clean gibberish from text columns across all tables using linguistic analysis.
    
    Uses a UDF to detect gibberish based on:
    - Vowel-to-consonant ratios
    - Excessive consonant clusters
    - Character repetition patterns
    
    Args:
        dataframes (dict): Dictionary of table names to DataFrames
        
    Returns:
        dict: Updated dictionary with cleaned text
    """
    print("\n" + "=" * 60)
    print("📝 CLEANING TEXT COLUMNS FOR GIBBERISH")
    print("=" * 60)

    for table_name, df in dataframes.items():
        print(f"\n🔍 Checking {table_name}...")

        # Get all string columns except special ones
        string_cols = [
            field.name
            for field in df.schema.fields
            if isinstance(field.dataType, StringType)
            and field.name
            not in ["sku", "zip_code", "postal_code", "dimensions", "transaction_id"]
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
    1. Remove leading zeros from ID columns
    2. Validate integer columns for non-numeric values
    3. Validate decimal/float columns
    4. Convert numeric status codes to text (1→"Active", 0→"Inactive", 2→"Pending")
    5. Clean transaction IDs and SKUs (remove special characters)
    6. Trim numeric string columns
    
    Args:
        dataframes (dict): Dictionary of table names to DataFrames
        
    Returns:
        dict: Updated dictionary with cleaned numeric strings
    """
    print("\n" + "=" * 60)
    print("🔢 CLEANING NUMERIC STRING COLUMNS")
    print("=" * 60)

    # 1. Clean all ID columns - remove leading zeros
    for table_name, df in dataframes.items():
        id_columns = [col_name for col_name in df.columns if col_name.endswith("_id")]

        for col_name in id_columns:
            # Remove leading zeros
            df = df.withColumn(
                col_name, regexp_replace(col(col_name), "^0+(?=\\d)", "")
            )

        if id_columns:
            dataframes[table_name] = df
            print(f"✅ Cleaned {len(id_columns)} ID columns in {table_name}")

    # 2. Clean all integer columns that might have text values
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

    # 3. Clean all decimal/float columns
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

    # 4. Handle columns with numeric status codes (convert to text)
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

    # 5. Clean transaction/reference IDs - remove special characters
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

    # 6. Trim all numeric string columns
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
    1. Trim leading/trailing whitespace
    2. Replace multiple spaces with single space
    3. Remove trailing special characters (*)
    4. Remove leading special characters (*)
    5. Clean up excessive quotes
    6. Remove trailing quotes
    
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
            # 1. Trim leading/trailing whitespace
            df = df.withColumn(col_name, trim(col(col_name)))

            # 2. Replace multiple spaces with single space
            df = df.withColumn(col_name, regexp_replace(col(col_name), "\\s+", " "))

            # 3. Remove trailing special characters
            df = df.withColumn(col_name, regexp_replace(col(col_name), "[*]+$", ""))

            # 4. Remove leading special characters
            df = df.withColumn(col_name, regexp_replace(col(col_name), "^[*]+", ""))

            # 5. Clean up excessive quotes
            df = df.withColumn(col_name, regexp_replace(col(col_name), '"{2,}', '"'))

            # 6. Remove trailing quotes
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
                    f"⚠️  {table_name}.{col_name}: {special_char_count} rows with multiple special characters"
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
