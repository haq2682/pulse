"""
Data standardization module for outlier removal and date validation.
"""

import pyspark.sql.functions as F
from pyspark.sql.functions import (
    current_date,
    current_timestamp,
    when,
    col,
    length,
    trim,
)
from pyspark.sql.types import (
    DateType,
    TimestampType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType,
)


def remove_outliers(dataframes, table_name, columns):
    """
    Remove outliers from specified columns using IQR method (1% and 99% quantiles).

    Args:
        dataframes (dict): Dictionary of table names to DataFrames
        table_name (str): Name of the table
        columns (list): List of column names to process

    Returns:
        dict: Updated dictionary
    """
    if table_name not in dataframes:
        print(f"Table {table_name} not found")
        return dataframes

    df = dataframes[table_name]
    result_df = df

    for column in columns:
        if column not in df.columns:
            print(f"Column {column} not found in {table_name}")
            continue

        print(f"\nProcessing outliers for {column} in {table_name}...")
        quantiles = result_df.approxQuantile(column, [0.01, 0.99], 0.0)
        low_cutoff, high_cutoff = quantiles[0], quantiles[1]

        print(f"  {column} - Low cutoff: {low_cutoff}, High cutoff: {high_cutoff}")
        if low_cutoff < 0:
            low_cutoff = 0
            print(f"  Adjusted Low cutoff for {column} to 0 since it was negative.")

        before_count = result_df.count()
        result_df = result_df.filter(
            (F.col(column) >= low_cutoff) & (F.col(column) <= high_cutoff)
        )
        after_count = result_df.count()

        removed = before_count - after_count
        print(f"  Removed {removed} outlier rows based on {column}")

    dataframes[table_name] = result_df
    print(f"\n✅ Completed outlier removal for {table_name}")

    return dataframes


def remove_all_outliers(dataframes):
    """
    Remove outliers from all numeric columns across all DataFrames.

    Args:
        dataframes (dict): Dictionary of table names to DataFrames

    Returns:
        dict: Updated dictionary
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
        "review_id",
        "wishlist_id",
    ]

    for table in dataframes.keys():
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
            print(f"\nRemoving outliers for table: {table}")
            dataframes = remove_outliers(dataframes, table, numeric_cols)
        else:
            print(
                f"\nNo numeric columns found in table: {table}, skipping outlier removal."
            )

    return dataframes


def validate_dates_and_timestamps(dataframes):
    """
    Validate dates and timestamps, replacing future dates with current date/timestamp.

    Args:
        dataframes (dict): Dictionary of table names to DataFrames

    Returns:
        dict: Updated dictionary
    """
    print("🕒 Validating dates and timestamps...")

    for table_name, df in dataframes.items():
        print(f"\n📅 Processing {table_name}...")

        # Get all date and timestamp columns
        date_timestamp_cols = []
        for field in df.schema.fields:
            if isinstance(field.dataType, (DateType, TimestampType)):
                date_timestamp_cols.append((field.name, field.dataType))

        if not date_timestamp_cols:
            print(f"  ✅ No date/timestamp columns found in {table_name}")
            continue

        result_df = df

        for col_name, col_type in date_timestamp_cols:
            print(f"  🔍 Checking {col_name} ({col_type})...")

            if isinstance(col_type, DateType):
                # Check for future dates
                future_count = result_df.filter(col(col_name) > current_date()).count()

                if future_count > 0:
                    print(f"    ⚠️ Found {future_count} future dates in {col_name}")
                    result_df = result_df.withColumn(
                        col_name,
                        when(col(col_name) > current_date(), current_date()).otherwise(
                            col(col_name)
                        ),
                    )
                    print(f"    ✅ Updated {future_count} future dates to current date")
                else:
                    print(f"    ✅ No future dates found in {col_name}")

            elif isinstance(col_type, TimestampType):
                # Check for future timestamps
                future_count = result_df.filter(
                    col(col_name) > current_timestamp()
                ).count()

                if future_count > 0:
                    print(f"    ⚠️ Found {future_count} future timestamps in {col_name}")
                    result_df = result_df.withColumn(
                        col_name,
                        when(
                            col(col_name) > current_timestamp(), current_timestamp()
                        ).otherwise(col(col_name)),
                    )
                    print(
                        f"    ✅ Updated {future_count} future timestamps to current timestamp"
                    )
                else:
                    print(f"    ✅ No future timestamps found in {col_name}")

        dataframes[table_name] = result_df

    print("\n🎉 Date and timestamp validation completed!")
    return dataframes


def detect_gibberish_patterns(dataframes):
    """
    Cleans specific columns with known gibberish or invalid data patterns
    (like postal codes, dimensions, or status columns) across all tables.
    Invalid values are replaced with lit(NULL) (represented by F.lit(None)).
    """

    print("\n" + "=" * 60)
    print("🔍 DETECTING AND CLEANING GIBBERISH PATTERNS (Specific Columns)")
    print("=" * 60)

    # 1. Clean postal/zip code columns - Relaxed for international formats (Issue 2)
    # Replaced 'Unknown' with lit(NULL) (Issue 1)
    postal_columns = {"customers": "postal_code", "suppliers": "zip_code"}

    for table, col_name in postal_columns.items():
        if table in dataframes:
            df = dataframes[table]
            if col_name in df.columns:
                df = df.withColumn(
                    col_name,
                    when(
                        # Check for characters outside of letters, numbers, space, and hyphen
                        (col(col_name).rlike(r"[^a-zA-Z0-9 -]"))
                        # Check for excessive length (permissive for international codes)
                        | (length(trim(col(col_name))) > 15)
                        # Check for repeating patterns (e.g., AAAA, 1111)
                        | (col(col_name).rlike(r"(.)\1{3,}")),
                        F.lit(None),  # Replace with lit(NULL)
                    ).otherwise(trim(col(col_name))),
                )
                dataframes[table] = df
                print(
                    f"✅ Cleaned {col_name} in {table} (using flexible international check)"
                )

    # 2. Clean dimensions column in products - Updated for 2D/3D and 'x'/'*' separators (Issue 3)
    # Replaced 'Unknown' with lit(NULL) (Issue 1)
    if "products" in dataframes:
        df = dataframes["products"]
        if "dimensions" in df.columns:
            # Regex accepts:
            # - Numeric/decimal values ([\d\.])
            # - Separators 'x', 'X', or '*' ([xX*])
            # - Optional second segment for 3D/3-part dimensions ((?:[xX*][\d\.]+)?\s*$)
            dimension_pattern = r"^\s*[\d\.]+[xX*][\d\.]+(?:[xX*][\d\.]+)?\s*$"

            df = df.withColumn(
                "dimensions",
                when(
                    # Only keep values that match the expected dimension format
                    col("dimensions").rlike(dimension_pattern),
                    trim(col("dimensions")),
                ).otherwise(
                    F.lit(None)
                ),  # Replace with lit(NULL)
            )
            dataframes["products"] = df
            print(
                "✅ Cleaned dimensions in products (Updated to accept 2D/3D and '*/x')"
            )

    # 3. Clean state/province columns - Generic check for gibberish/non-alpha characters
    for table, col_name in {
        "customers": "state_province",
        "suppliers": "state",
    }.items():
        if table in dataframes:
            df = dataframes[table]
            if col_name in df.columns:
                df = df.withColumn(
                    col_name,
                    when(
                        col(col_name).rlike(r".*[*@#$%^&].*"),
                        F.lit(None),  # Replace with lit(NULL)
                    ).otherwise(col(col_name)),
                )
                dataframes[table] = df
                print(f"✅ Cleaned {col_name} in {table} (special chars check)")

    # 4. Clean city columns - Generic check for gibberish/numbers in city names
    for table in dataframes.keys():
        df = dataframes[table]
        if "city" in df.columns:
            df = df.withColumn(
                "city",
                when(
                    col("city").rlike(r".*[*@#$%^&0-9].*"),  # Special chars or numbers
                    F.lit(None),  # Replace with lit(NULL)
                ).otherwise(col("city")),
            )
            dataframes[table] = df
            print(f"✅ Cleaned city in {table} (special chars/numbers check)")

    # 5. Clean country columns - Generic check for gibberish/numbers in country names
    for table in dataframes.keys():
        df = dataframes[table]
        if "country" in df.columns:
            df = df.withColumn(
                "country",
                when(
                    col("country").rlike(
                        r".*[*@#$%^&0-9].*"
                    ),  # Special chars or numbers
                    F.lit(None),  # Replace with lit(NULL)
                ).otherwise(col("country")),
            )
            dataframes[table] = df
            print(f"✅ Cleaned country in {table} (special chars/numbers check)")

    # 6. Validate status-type columns (e.g., order_status, account_status)
    status_columns = {
        "customers": ["account_status"],
        "orders": ["order_status", "delivery_status"],
        "payments": ["payment_status"],
        "suppliers": ["supplier_status"],
        "shopping_cart": ["cart_status"],
        "marketing_campaigns": ["campaign_status"],
    }

    valid_statuses = [
        "Active",
        "Inactive",
        "Pending",
        "Shipped",
        "Delivered",
        "Cancelled",
        "Completed",
        "Failed",
        "Success",
        "Open",
        "Closed",
    ]

    for table, cols in status_columns.items():
        if table in dataframes:
            df = dataframes[table]
            for col_name in cols:
                if col_name in df.columns:
                    df = df.withColumn(
                        col_name,
                        when(
                            (col(col_name).isNull())
                            | (col(col_name).isin(valid_statuses)),
                            col(col_name),
                        ).otherwise(
                            F.lit(None)
                        ),  # Replace invalid status with lit(NULL)
                    )
                    dataframes[table] = df
                    print(f"✅ Cleaned {col_name} in {table} (status check)")

    # 7. Validate gender column
    if "customers" in dataframes and "gender" in dataframes["customers"].columns:
        df = dataframes["customers"]
        valid_genders = ["Male", "Female", "Other", "Prefer Not to Say", "X"]
        df = df.withColumn(
            "gender",
            when(
                (col("gender").isNull()) | (col("gender").isin(valid_genders)),
                col("gender"),
            ).otherwise(
                F.lit(None)
            ),  # Replace invalid gender with lit(NULL)
        )
        dataframes["customers"] = df
        print("✅ Cleaned gender in customers (gender check)")

    print("=" * 60)
    print("✅ PATTERN DETECTION COMPLETED")
    print("=" * 60)

    return dataframes
