"""
Data cleaning module for handling duplicates, null values, and basic data quality.
"""
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, LongType, FloatType, DoubleType, DecimalType
from pyspark.ml.feature import Imputer


def check_duplicates(dataframes):
    """
    Check for duplicate rows in all DataFrames.
    Shows actual number of duplicate row groups, not just row count difference.
    
    Args:
        dataframes (dict): Dictionary of table names to DataFrames
    """
    for table in dataframes.keys():
        # Group by all columns and count occurrences
        dup_rows = dataframes[table].groupBy(*dataframes[table].columns).count().filter("count > 1")
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
    if table in dataframes:
        df = dataframes[table]
        if col_name in df.columns:
            before = df.count()
            cleaned = df.filter(F.col(col_name).isNotNull())
            dataframes[table] = cleaned
            after = cleaned.count()
            print(f"Removed {before - after} rows from '{table}' where '{col_name}' is NULL")
        else:
            print(f"Column '{col_name}' not found in '{table}'")
    else:
        print(f"Table '{table}' not found in dataframes")
    
    return dataframes


def drop_null_keys(dataframes):
    """
    Drop rows where primary keys or foreign keys are NULL.
    
    Args:
        dataframes (dict): Dictionary of table names to DataFrames
        
    Returns:
        dict: Updated dictionary
    """
    all_ids = [
        "session_id", "customer_id", "address_id", "product_id", 
        "supplier_id", "order_id", "order_item_id", "payment_id", 
        "campaign_id", "cart_id", "review_id", "wishlist_id"
    ]
    
    for table in dataframes.keys():
        for col in dataframes[table].columns:
            if col in all_ids:
                dataframes = drop_null_rows(dataframes, table, col)
    
    return dataframes


def check_nulls(dataframes):
    """
    Check for NULL values in all columns of all DataFrames.
    
    Args:
        dataframes (dict): Dictionary of table names to DataFrames
    """
    for df in dataframes.values():
        null_counts = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns])
        null_counts.show()


def fill_null_values(dataframes):
    """
    Fill NULL values in non-numeric columns with appropriate defaults.
    
    Args:
        dataframes (dict): Dictionary of table names to DataFrames
        
    Returns:
        dict: Updated dictionary with filled values
    """
    if "customers" in dataframes.keys():
        dataframes["customers"] = dataframes["customers"].fillna({
            "gender": "Unknown",
            "account_status": "Unknown",
            "city": "Unknown",
            "state_province": "Unknown",
            "postal_code": "00000",
            "country": "Unknown",
            "date_of_birth": "1900-01-01",
            "account_created_at": "1900-01-01",
            "last_login_date": "1900-01-01",
            "is_active": "false"
        })
    else:
        print("Customers DataFrame is missing.")

    if "suppliers" in dataframes.keys():
        dataframes["suppliers"] = dataframes["suppliers"].fillna({
            "supplier_rating": 0.0,
            "supplier_status": "Unknown",
            "is_preferred": "false",
            "is_verified": "false",
            "contract_start_date": "1900-01-01",
            "contract_end_date": "1900-01-01",
            "city": "Unknown",
            "state": "Unknown",
            "zip_code": "00000",
            "country": "Unknown",
        })
    else:
        print("Suppliers DataFrame is missing.")

    if "products" in dataframes.keys():
        dataframes["products"] = dataframes["products"].fillna({
            "product_name": "Unknown",
            "sku": "Unknown",
            "category": "Unknown",
            "sub_category": "Unknown",
            "brand": "Unknown",
            "launch_date": "1900-01-01",
            "weight": "0.0",
            "dimensions": "Unknown",
            "color": "Unknown",
            "size": "Unknown",
            "material": "Unknown"
        })
    else:
        print("Products DataFrame is missing.")

    if "inventory" in dataframes.keys():
        dataframes["inventory"] = dataframes["inventory"].fillna({
            "last_restocked_date": "1900-01-01"
        })

    if "wishlist" in dataframes.keys():
        dataframes["wishlist"] = dataframes["wishlist"].fillna({
            "added_date": "1900-01-01",
            "purchased_date": "1900-01-01",
            "removed_date": "1900-01-01"
        })

    if "shopping_cart" in dataframes.keys():
        dataframes["shopping_cart"] = dataframes["shopping_cart"].fillna({
            "added_date": "1900-01-01",
            "cart_status": "Unknown"
        })

    if "orders" in dataframes.keys():
        dataframes["orders"] = dataframes["orders"].fillna({
            "order_status": "Unknown",
            "currency": "Unknown",
            "order_placed_at": "1900-01-01",
            "order_shipped_at": "1900-01-01",
            "order_delivered_at": "1900-01-01"
        })

    if "payments" in dataframes.keys():
        dataframes["payments"] = dataframes["payments"].fillna({
            "payment_method": "Unknown",
            "payment_provider": "Unknown",
            "payment_status": "Unknown",
            "transaction_id": "Unknown",
            "refund_date": "1900-01-01",
            "payment_date": "1900-01-01"
        })

    if "reviews" in dataframes.keys():
        dataframes["reviews"] = dataframes["reviews"].fillna({
            "review_title": "Unknown",
            "review_desc": "Unknown",
            "review_date": "1900-01-01"
        })

    if "marketing_campaigns" in dataframes.keys():
        dataframes["marketing_campaigns"] = dataframes["marketing_campaigns"].fillna({
            "campaign_name": "Unknown",
            "campaign_type": "Unknown",
            "start_date": "1900-01-01",
            "end_date": "1900-01-01",
            "target_audience": "Unknown",
            "campaign_status": "Unknown"
        })

    if "customer_sessions" in dataframes.keys():
        dataframes["customer_sessions"] = dataframes["customer_sessions"].fillna({
            "session_start": "1900-01-01",
            "session_end": "1900-01-01",
            "device_type": "Unknown",
            "referrer_source": "Unknown",
            "conversion_flag": "false",
            "cart_abandonment_flag": "false"
        })

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
    df = dataframes[table]
    total_rows = df.count()
    print(f"Total rows: {total_rows}")
    
    # Get non-null counts for all columns at once
    non_null_counts = df.select([F.count(F.col(c)).alias(c) for c in numeric_cols]).collect()[0]
    
    valid_cols = []
    all_null_cols = []
    
    for col_name in numeric_cols:
        non_null_count = non_null_counts[col_name]
        
        if non_null_count == 0:
            all_null_cols.append(col_name)
            print(f"🚫 {col_name}: ALL NULL - will fill with 0")
        else:
            valid_cols.append(col_name)
            null_count = total_rows - non_null_count
            print(f"✅ {col_name}: {non_null_count} non-null, {null_count} null - will impute")
    
    print(f"\nAll-NULL columns: {all_null_cols}")
    print(f"Valid columns for imputation: {valid_cols}")
    
    # Fill all-NULL columns with 0
    if all_null_cols:
        fill_dict = {col: 0 for col in all_null_cols}
        df = df.fillna(fill_dict)
        dataframes[table] = df
        print(f"✅ Filled all-NULL columns with 0: {all_null_cols}")
    
    # Impute valid columns with median
    if valid_cols:
        imputer = Imputer(
            inputCols=valid_cols,
            outputCols=valid_cols,
            strategy="median"
        )
        
        model = imputer.fit(df)
        df_imputed = model.transform(df)
        dataframes[table] = df_imputed
        print(f"✅ Successfully imputed columns with median: {valid_cols}")
    else:
        print("⚠️ No valid columns found for imputation")
    
    print("\n" + "="*50)
    print(f"🔍 Final check for NULL values in {table}:")
    print("="*50)
    
    return dataframes


def impute_all_numeric(dataframes):
    """
    Impute all numeric columns across all DataFrames.
    
    Args:
        dataframes (dict): Dictionary of table names to DataFrames
        
    Returns:
        dict: Updated dictionary
    """
    all_ids = [
        "session_id", "customer_id", "address_id", "product_id", 
        "supplier_id", "order_id", "order_item_id", "payment_id", 
        "campaign_id", "cart_id", "review_id", "wishlist_id"
    ]
    
    for table in dataframes.keys():
        numeric_cols = [
            field.name for field in dataframes[table].schema.fields 
            if isinstance(field.dataType, (IntegerType, LongType, FloatType, DoubleType, DecimalType))
        ]
        numeric_cols = [col for col in numeric_cols if col not in all_ids]
        
        if numeric_cols:
            print(f"\nImputing missing values for table: {table}")
            dataframes = impute_missing_values(dataframes, table, numeric_cols)
        else:
            print(f"\nNo numeric columns found in table: {table}, skipping imputation.")
    
    return dataframes
