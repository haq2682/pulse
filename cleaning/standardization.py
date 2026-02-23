"""
Data standardization module for outlier removal and date validation.
"""

import pyspark.sql.functions as F
from pyspark.sql.functions import (
    current_date,
    current_timestamp,
    to_date,
    to_timestamp,
    coalesce,
    regexp_replace,
    from_utc_timestamp,
    when,
    col,
    length,
    trim,
    upper,
    initcap,
    lower,
    lit,
    udf,
)
from pyspark.sql.types import (
    DateType,
    StringType,
    TimestampType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType,
)
import re
from currency_converter import CurrencyConverter


def remove_outliers(dataframes, table_name, columns):
    """
    Remove outliers using a flexible Quantile method.
    
    Adjustments made:
    1. Changed quantiles from 5%/95% to 0.1%/99.9% to keep more data.
    2. Added a "Safety Floor" for quantity columns to prevent cutting valid small numbers (like 7).
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
        
        # --- FIX 1: Relaxed Quantiles ---
        # Changed from [0.05, 0.95] to [0.001, 0.999]
        # This keeps the top 0.1% and bottom 0.1% only, preserving legitimate bulk orders.
        quantiles = result_df.approxQuantile(column, [0.001, 0.999], 0.0)
        
        if len(quantiles) < 2:
            print(f"Not enough data to compute outliers for column {column}")
            continue
            
        low_cutoff, high_cutoff = quantiles[0], quantiles[1]

        # --- FIX 2: Safety Floor for Quantity ---
        # If the column represents quantity, DO NOT allow the cutoff to be less than 20.
        # This fixes your issue where "7" was being removed.
        if "quantity" in column.lower() or "qty" in column.lower():
            if high_cutoff < 20:
                print(f"  ⚠️  Calculated high cutoff ({high_cutoff}) is too strict for {column}.")
                high_cutoff = 20
                print(f"  🔧 Adjusted high cutoff to {high_cutoff} to preserve valid orders (e.g. 7, 10, 15).")

        # --- FIX 3: Negative Price Check ---
        if low_cutoff < 0:
            low_cutoff = 0
            print(f"  Adjusted Low cutoff for {column} to 0 since it was negative.")

        print(f"  {column} - Keeping data between: {low_cutoff} and {high_cutoff}")

        before_count = result_df.count()
        
        # Apply the filter
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
        "cart_item_id",
        "inventory_id",
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

def normalize_dates_and_timestamps(
    dataframes,
    timestamp_formats=None,
    date_formats=None,
    sample_size=50,
    default_timezone="UTC",
):
    """
    Optimized normalization with: 
    - Automatic timezone inference per column
    - Safe multi-format parsing
    - Designed for very wide tables

    Args:
        dataframes (dict): table_name -> DataFrame
        timestamp_formats (list): timestamp patterns
        date_formats (list): date patterns
        sample_size (int): rows to sample per column
        default_timezone (str): assumed TZ if none present

    Returns:
        dict: Updated dictionary of DataFrames
    """

    print("⚡ Normalizing dates & timestamps (optimized)...")

    timestamp_formats = timestamp_formats or [
        "yyyy-MM-dd HH: mm:ss",
        "yyyy-MM-dd HH:mm:ss.SSS",
        "yyyy-MM-dd'T'HH:mm:ss",
        "yyyy-MM-dd'T'HH:mm:ss.SSS",
        "yyyy-MM-dd'T'HH:mm: ssX",
        "yyyy-MM-dd'T'HH:mm:ss.SSSX",
        "yyyy/MM/dd HH:mm: ss",
        "MM/dd/yyyy HH:mm:ss",
    ]

    date_formats = date_formats or [
        "yyyy-MM-dd",
        "yyyy/MM/dd",
        "MM/dd/yyyy",
        "dd-MM-yyyy",
    ]

    date_regex = re.compile(r"\d{4}[-/]\d{2}[-/]\d{2}")
    timestamp_regex = re.compile(r"\d{2}:\d{2}:\d{2}")
    tz_regex = re.compile(r"(Z|[+-]\d{2}:?\d{2})$")

    for table_name, df in dataframes.items():
        print(f"\n📄 Processing {table_name}...")

        for field in df.schema.fields:
            col_name = field.name
            col_type = field.dataType

            if not isinstance(col_type, StringType):
                continue

            sample = (
                df.select(col_name)
                .where(col(col_name).isNotNull())
                .limit(sample_size)
                .rdd.map(lambda r: r[0])
                .collect()
            )

            if not sample:
                continue

            looks_like_ts = any(timestamp_regex.search(str(v)) for v in sample)
            looks_like_date = any(date_regex.search(str(v)) for v in sample)

            has_explicit_tz = any(tz_regex.search(str(v)) for v in sample)

            df = df.withColumn(
                col_name,
                regexp_replace(
                    col(col_name),
                    r"([+-]\d{2})(\d{2})$",
                    r"\1:\2",
                ),
            )

            if looks_like_ts:
                inferred_tz = "embedded" if has_explicit_tz else default_timezone
                print(
                    f"  🕒 {col_name}: timestamp "
                    f"(timezone={'detected' if has_explicit_tz else default_timezone})"
                )

                parsed_ts = coalesce(
                    *[to_timestamp(col(col_name), f) for f in timestamp_formats]
                )

                if has_explicit_tz:
                    df = df.withColumn(col_name, parsed_ts)
                else: 
                    df = df.withColumn(
                        col_name,
                        from_utc_timestamp(parsed_ts, default_timezone),
                    )

            elif looks_like_date:
                print(f"  📅 {col_name}: date")

                parsed_date = coalesce(
                    *[to_date(col(col_name), f) for f in date_formats]
                )

                df = df.withColumn(col_name, parsed_date)

        dataframes[table_name] = df

    print("\n🎉 Optimized date & timestamp normalization completed!")
    return dataframes

def validate_dates_and_timestamps(dataframes):
    """
    Validate dates and timestamps, replacing future dates with current date/timestamp.

    Args:
        dataframes (dict): Dictionary of table names to DataFrames

    Returns:
        dict: Updated dictionary
    """
    date_cols_not_to_check = [
        "start_date",
        "end_date",
        "launch_date",
        "contract_start_date",
        "contract_end_date",
    ]
    print("🕒 Validating dates and timestamps...")

    for table_name, df in dataframes.items():
        print(f"\n📅 Processing {table_name}...")

        date_timestamp_cols = []
        for field in df.schema.fields:
            if isinstance(field.dataType, (DateType, TimestampType)):
                date_timestamp_cols.append((field.name, field.dataType))

        if not date_timestamp_cols:
            print(f"  ✅ No date/timestamp columns found in {table_name}")
            continue

        result_df = df

        for col_name, col_type in date_timestamp_cols:
            if col_name in date_cols_not_to_check:
                print(f"  🔍 Skipping {col_name} ({col_type}) as it's in the exclusion list.")
                continue
            
            print(f"  🔍 Checking {col_name} ({col_type})...")

            if isinstance(col_type, DateType):
                future_count = result_df.filter(col(col_name) > current_date()).count()

                if future_count > 0:
                    print(f"    ⚠�� Found {future_count} future dates in {col_name}")
                    result_df = result_df.withColumn(
                        col_name,
                        when(col(col_name) > current_date(), current_date()).otherwise(
                            col(col_name)
                        ),
                    )
                    print(f"✅ Updated {future_count} future dates to current date")
                else: 
                    print(f"✅ No future dates found in {col_name}")

            elif isinstance(col_type, TimestampType):
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


def is_likely_gibberish(text):
    """
    Detect if a string is gibberish using pattern analysis.
    Returns True if gibberish, False if likely valid.
    """
    if not text or len(text) < 2:
        return False
    
    text_lower = text.lower()
    
    # Check for excessive special characters (more than 30% of string)
    special_chars = len(re.findall(r"[^a-zA-Z0-9\s]", text))
    if special_chars / len(text) > 0.3:
        return True
    
    # Check for random character patterns
    if re.search(r"[*@#$%^&]{2,}", text):
        return True
    
    # Check vowel ratio (English text has 30-40% vowels typically)
    vowels = len(re.findall(r"[aeiou]", text_lower))
    if len(text) > 4: 
        vowel_ratio = vowels / len(text)
        if vowel_ratio < 0.10 or vowel_ratio > 0.75:
            return True
    
    # Check for excessive consonant clusters (6+ consonants in a row)
    if re.search(r"[bcdfghjklmnpqrstvwxyz]{6,}", text_lower):
        return True
    
    # Check for character repetition (5+ same chars)
    if re.search(r"(.)\1{4,}", text_lower):
        return True
    
    return False


def detect_gibberish_patterns(dataframes):
    """
    Data-driven approach to clean status columns: 
    1.Normalizes values (trim, extract from brackets, standardize case)
    2.Automatically learns valid values from the data
    3.Only removes actual gibberish patterns
    4.Works across different datasets without hardcoding
    """

    print("\n" + "=" * 60)
    print("🔍 DETECTING AND CLEANING GIBBERISH PATTERNS (Data-Driven)")
    print("=" * 60)

    postal_columns = {"customers": "postal_code", "suppliers": "zip_code"}

    for table, col_name in postal_columns.items():
        if table in dataframes: 
            df = dataframes[table]
            if col_name in df.columns:
                df = df.withColumn(
                    col_name,
                    when(
                        (col(col_name).rlike(r"[^a-zA-Z0-9-]"))
                        | (length(trim(col(col_name))) > 15)
                        | (col(col_name).rlike(r"(.)\1{3,}")),
                        F.lit(None),
                    ).otherwise(trim(col(col_name))),
                )
                dataframes[table] = df
                print(
                    f"✅ Cleaned {col_name} in {table} (using flexible international check)"
                )

    if "products" in dataframes:
        df = dataframes["products"]
        if "dimensions" in df.columns:
            dimension_pattern = r"^\s*[\d\.]+[xX*][\d\.]+(?:[xX*][\d\.]+)?\s*$"

            df = df.withColumn(
                "dimensions",
                when(
                    col("dimensions").rlike(dimension_pattern),
                    trim(col("dimensions")),
                ).otherwise(
                    F.lit(None)
                ),
            )
            dataframes["products"] = df
            print(
                "✅ Cleaned dimensions in products (Updated to accept 2D/3D and '*/x')"
            )

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
                        F.lit(None),
                    ).otherwise(col(col_name)),
                )
                dataframes[table] = df
                print(f"✅ Cleaned {col_name} in {table} (special chars check)")

    for table in dataframes.keys():
        df = dataframes[table]
        if "city" in df.columns:
            df = df.withColumn(
                "city",
                when(
                    col("city").rlike(r".*[*@#$%^&0-9].*"),
                    F.lit(None),
                ).otherwise(col("city")),
            )
            dataframes[table] = df
            print(f"✅ Cleaned city in {table} (special chars/numbers check)")

    for table in dataframes.keys():
        df = dataframes[table]
        if "country" in df.columns:
            df = df.withColumn(
                "country",
                when(
                    col("country").rlike(r".*[*@#$%^&0-9].*"),
                    F.lit(None),
                ).otherwise(col("country")),
            )
            dataframes[table] = df
            print(f"✅ Cleaned country in {table} (special chars/numbers check)")

    # Data-driven status field cleaning
    status_field_patterns = ["_status", "gender"]
    
    for table_name, df in dataframes.items():
        for field in df.schema.fields:
            field_name = field.name
            
            # Check if this is a status-type field
            if not any(pattern in field_name.lower() for pattern in status_field_patterns):
                continue
            
            if not isinstance(field.dataType, StringType):
                continue
            
            print(f"\n🔍 Processing status field: {table_name}.{field_name}")
            
            # Step 1: Normalize the field (trim, extract from brackets)
            df = df.withColumn(field_name + "_raw", col(field_name))
            df = df.withColumn(field_name, trim(col(field_name)))
            
            # Extract content from parentheses or brackets
            extracted_from_parens = regexp_replace(col(field_name), r"^.*\(([^)]+)\).*$", r"$1")
            extracted_from_brackets = regexp_replace(col(field_name), r"^.*\[([^\]]+)\].*$", r"$1")
            text_without_parens = regexp_replace(col(field_name), r"\s*\([^)]*\)\s*", "")
            text_without_brackets = regexp_replace(col(field_name), r"\s*\[[^\]]*\]\s*", "")
            
            # Try extracted content first, then fall back to cleaned text
            df = df.withColumn(
                field_name + "_normalized",
                when(
                    length(trim(extracted_from_parens)) > 0,
                    when(
                        col(field_name).rlike(r".*\(.*\).*"),
                        trim(extracted_from_parens)
                    ).otherwise(trim(text_without_parens))
                ).otherwise(
                    when(
                        col(field_name).rlike(r".*\[.*\].*"),
                        trim(extracted_from_brackets)
                    ).otherwise(trim(text_without_brackets))
                )
            )
            
            df = df.withColumn(
                field_name + "_normalized",
                trim(regexp_replace(col(field_name + "_normalized"), r"\s*[\(\)\[\]]\s*", ""))
            )
            
            # Step 2: Collect unique normalized values and analyze
            unique_values = (
                df.select(field_name + "_normalized")
                .filter(col(field_name + "_normalized").isNotNull())
                .distinct()
                .collect()
            )
            
            unique_values = [row[0] for row in unique_values if row[0]]
            
            # Step 3: Filter out gibberish using pattern detection
            valid_values = []
            gibberish_values = []
            
            for value in unique_values:
                if is_likely_gibberish(value):
                    gibberish_values.append(value)
                else:
                    valid_values.append(value)
            
            print(f"  📊 Found {len(unique_values)} unique values")
            print(f"  ✅ Valid values: {len(valid_values)}")
            print(f"  🗑️  Gibberish values: {len(gibberish_values)}")
            
            if valid_values:
                print(f"  📋 Sample valid values: {valid_values[: 10]}")
            
            # Step 4: Apply validation - keep only non-gibberish values
            valid_values_upper = [v.upper() for v in valid_values]
            
            df = df.withColumn(
                field_name,
                when(
                    col(field_name + "_normalized").isNull(),
                    F.lit(None)
                ).when(
                    upper(col(field_name + "_normalized")).isin(valid_values_upper),
                    initcap(col(field_name + "_normalized"))
                ).otherwise(
                    F.lit(None)
                ),
            )

            # Clean up temporary columns
            df = df.drop(field_name + "_raw", field_name + "_normalized")

            dataframes[table_name] = df
            print(f"✅ Cleaned and normalized {field_name} in {table_name}")

    print("=" * 60)
    print("✅ PATTERN DETECTION COMPLETED")
    print("=" * 60)


def convert_currency_columns(dataframes, bucket_name):
    """
    Convert price columns from source currency to target currency.

    This function:
    1. Identifies the source currency for each order (from orders table)
    2. Fetches target currency from PostgreSQL (based on business_id)
    3. Converts all price-related columns to target currency
    4. Handles missing currency values gracefully by skipping conversion
    5. Preserves original currency column for tracking

    Args:
        dataframes: Dictionary of DataFrames
        bucket_name: Business ID (used to fetch target currency)

    Returns:
        dict: Updated dataframes with converted prices
    """
    print("\n💱 Converting currencies...")

    try:
        # Initialize currency converter with business_id
        converter = CurrencyConverter(bucket_name)
        target_currency = converter.get_target_currency()
        print(f"   Target currency: {target_currency}")

        # Check if orders table exists and has currency column
        if 'orders' not in dataframes:
            print(f"   ⚠️  Orders table not found, skipping currency conversion")
            return dataframes

        orders_df = dataframes['orders']
        if 'currency' not in orders_df.columns:
            print(f"   ⚠️  Currency column not found in orders table, skipping conversion")
            return dataframes

        # Collect unique currencies and their exchange rates
        unique_currencies = orders_df.select('currency').filter(col('currency').isNotNull()).distinct().collect()

        if not unique_currencies:
            print(f"   ⚠️  No currency values found in orders, skipping conversion")
            return dataframes

        currency_rates = {}
        all_same_as_target = True

        for row in unique_currencies:
            source_currency = row['currency']
            if source_currency.upper() != target_currency.upper():
                all_same_as_target = False
                rate = converter.get_exchange_rate(source_currency)
                if rate is not None:
                    currency_rates[source_currency] = rate
                    print(f"   Exchange rate: 1 {source_currency} = {rate:.4f} {target_currency}")
                else:
                    print(f"   ⚠️  Failed to fetch exchange rate for {source_currency}")
                    currency_rates[source_currency] = None
            else:
                currency_rates[source_currency] = 1.0

        # If all currencies are the same as target, skip conversion
        if all_same_as_target:
            print(f"   All currencies match target ({target_currency}), skipping conversion")
            return dataframes

        # Create a currency mapping for orders
        # Add a temp column with exchange rates to orders table

        # Build a case statement for currency conversion
        rate_expr = when(col('currency').isNull(), lit(None))
        for curr, rate in currency_rates.items():
            if rate is not None:
                rate_expr = rate_expr.when(col('currency') == curr, lit(rate))
            else:
                rate_expr = rate_expr.when(col('currency') == curr, lit(None))
        rate_expr = rate_expr.otherwise(lit(None))

        # Add exchange_rate column to orders
        orders_with_rate = orders_df.withColumn('_exchange_rate', rate_expr)

        # Convert price columns in orders table
        converted_count = 0
        order_price_columns = ['subtotal', 'tax_amount', 'shipping_cost', 'total_discount', 'total_amount']

        for price_col in order_price_columns:
            if price_col in orders_with_rate.columns:
                # Convert only where exchange_rate is not null
                orders_with_rate = orders_with_rate.withColumn(
                    price_col,
                    when(col('_exchange_rate').isNotNull(),
                         col(price_col) * col('_exchange_rate')
                    ).otherwise(col(price_col))
                )
                converted_count += 1

        # Drop the temporary exchange_rate column before saving
        orders_with_rate = orders_with_rate.drop('_exchange_rate')
        dataframes['orders'] = orders_with_rate

        # Now handle order_items and payments which are linked to orders
        # We need to join with orders to get the exchange rate
        if 'order_items' in dataframes and 'order_id' in orders_df.columns:
            order_items_df = dataframes['order_items']
            if 'order_id' in order_items_df.columns:
                # Create a mapping of order_id to exchange_rate
                order_rates = orders_df.select('order_id', 'currency').withColumn('_exchange_rate', rate_expr)

                # Join order_items with order rates
                order_items_with_rate = order_items_df.join(
                    order_rates.select('order_id', '_exchange_rate'),
                    'order_id',
                    'left'
                )

                # Convert price columns
                item_price_columns = ['discount_amount', 'product_price']
                for price_col in item_price_columns:
                    if price_col in order_items_with_rate.columns:
                        order_items_with_rate = order_items_with_rate.withColumn(
                            price_col,
                            when(col('_exchange_rate').isNotNull(),
                                 col(price_col) * col('_exchange_rate')
                            ).otherwise(col(price_col))
                        )
                        converted_count += 1

                # Drop temporary column
                order_items_with_rate = order_items_with_rate.drop('_exchange_rate')
                dataframes['order_items'] = order_items_with_rate

        if 'payments' in dataframes and 'order_id' in orders_df.columns:
            payments_df = dataframes['payments']
            if 'order_id' in payments_df.columns:
                # Create a mapping of order_id to exchange_rate
                order_rates = orders_df.select('order_id', 'currency').withColumn('_exchange_rate', rate_expr)

                # Join payments with order rates
                payments_with_rate = payments_df.join(
                    order_rates.select('order_id', '_exchange_rate'),
                    'order_id',
                    'left'
                )

                # Convert price columns
                payment_price_columns = ['processing_fee', 'refund_amount']
                for price_col in payment_price_columns:
                    if price_col in payments_with_rate.columns:
                        payments_with_rate = payments_with_rate.withColumn(
                            price_col,
                            when(col('_exchange_rate').isNotNull(),
                                 col(price_col) * col('_exchange_rate')
                            ).otherwise(col(price_col))
                        )
                        converted_count += 1

                # Drop temporary column
                payments_with_rate = payments_with_rate.drop('_exchange_rate')
                dataframes['payments'] = payments_with_rate

        # Handle products, inventory, marketing_campaigns, cart_items
        # These don't have direct order linkage, so we'll use a default conversion if all source currencies are the same
        # Otherwise, we skip conversion for these tables

        if len(currency_rates) == 1:
            # Single source currency, convert these tables
            source_currency = list(currency_rates.keys())[0]
            rate = currency_rates[source_currency]

            if rate is not None and rate != 1.0:
                # Define tables and their price columns for non-order tables
                other_tables_map = {
                    'products': ['cost_price', 'sell_price'],
                    'inventory': ['storage_cost'],
                    'marketing_campaigns': ['budget', 'spent_amount'],
                    'cart_items': ['unit_price', 'total_price']
                }

                # Create UDF for currency conversion
                def convert_price(price):
                    if price is None:
                        return None
                    return float(price) * rate

                convert_udf = udf(convert_price, DoubleType())

                for table_name, price_columns in other_tables_map.items():
                    if table_name in dataframes:
                        df = dataframes[table_name]

                        for price_col in price_columns:
                            if price_col in df.columns:
                                df = df.withColumn(price_col, convert_udf(col(price_col)))
                                converted_count += 1

                        dataframes[table_name] = df
        else:
            print(f"   ℹ️  Multiple source currencies detected, skipping conversion for non-order tables")

        # Note: We do NOT update the currency column in orders - it's preserved as-is
        print(f"   ✅ Converted {converted_count} price columns to {target_currency}")
        print(f"   ℹ️  Original currency values preserved in orders table")

    except Exception as e:
        print(f"   ⚠️  Error during currency conversion: {e}")
        import traceback
        traceback.print_exc()
        print(f"   Continuing without currency conversion...")

    return dataframes