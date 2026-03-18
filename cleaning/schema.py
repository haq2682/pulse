"""
Schema module for casting DataFrames to correct data types.
"""
from pyspark.sql.types import *
from pyspark.sql.functions import col, when, from_unixtime


def _cast_existing(df, cast_map: list):
    """
    Cast only the columns that actually exist in *df*.

    ``cast_map`` is a list of ``(column_name, spark_type)`` tuples that
    defines the desired schema.  Columns absent from the DataFrame are
    silently skipped so that a partial schema match (common when source
    databases use non-standard column names that weren't fully auto-mapped)
    never aborts the cleaning pipeline.

    Args:
        df:        Spark DataFrame to cast.
        cast_map:  Ordered list of (col_name, DataType) pairs.

    Returns:
        DataFrame with the existing subset of columns cast to target types.
    """
    existing = set(df.columns)
    exprs = []
    skipped = []
    for col_name, dtype in cast_map:
        if col_name in existing:
            exprs.append(col(col_name).cast(dtype))
        else:
            skipped.append(col_name)
    if skipped:
        print(f"  ⚠️  Skipped missing columns during cast: {skipped}")
    if not exprs:
        return df  # nothing to cast — return as-is
    return df.select(*exprs)


def cast_dataframes(dataframes):
    """
    Cast all DataFrames to their correct data types.

    Uses ``_cast_existing`` so that columns absent from the DataFrame (e.g.
    because the source DB used a non-canonical name that wasn't fully mapped)
    are skipped rather than raising AnalysisException and aborting the run.

    Args:
        dataframes (dict): Dictionary of table names to DataFrames

    Returns:
        dict: Dictionary of table names to casted DataFrames
    """
    # 1. Addresses
    if "addresses" in dataframes:
        dataframes["addresses"] = _cast_existing(dataframes["addresses"], [
            ("address_id",      StringType()),
            ("city",            StringType()),
            ("state_province",  StringType()),
            ("postal_code",     StringType()),
            ("country",         StringType()),
        ])
        print("Cast addresses DataFrame")

    # 2. Customers
    if "customers" in dataframes:
        dataframes["customers"] = _cast_existing(dataframes["customers"], [
            ("customer_id",        StringType()),
            ("gender",             StringType()),
            ("date_of_birth",      DateType()),
            ("account_status",     StringType()),
            ("address_id",         StringType()),
            ("city",               StringType()),
            ("state_province",     StringType()),
            ("postal_code",        StringType()),
            ("country",            StringType()),
            ("account_created_at", TimestampType()),
            ("last_login_date",    DateType()),
            ("is_active",          BooleanType()),
        ])
        print("Cast customers DataFrame")

    # 3. Suppliers
    if "suppliers" in dataframes:
        dataframes["suppliers"] = _cast_existing(dataframes["suppliers"], [
            ("supplier_id",          StringType()),
            ("supplier_rating",      FloatType()),
            ("supplier_status",      StringType()),
            ("is_preferred",         BooleanType()),
            ("is_verified",          BooleanType()),
            ("contract_start_date",  DateType()),
            ("contract_end_date",    DateType()),
            ("city",                 StringType()),
            ("state",                StringType()),
            ("zip_code",             StringType()),
            ("country",              StringType()),
        ])
        print("Cast suppliers DataFrame")

    # 4. Categories
    # Canonical column name is "category" (per List.py mapping_dict_categories).
    # "category_name" is an alias that maps TO "category" during the mapping step.
    if "categories" in dataframes:
        dataframes["categories"] = _cast_existing(dataframes["categories"], [
            ("category_id",  StringType()),
            ("category",     StringType()),   # canonical name — NOT category_name
            ("sub_category", StringType()),
        ])
        print("Cast categories DataFrame")

    # 5. Products
    if "products" in dataframes:
        dataframes["products"] = _cast_existing(dataframes["products"], [
            ("product_id",   StringType()),
            ("product_name", StringType()),
            ("sku",          StringType()),
            ("category_id",  StringType()),
            ("category",     StringType()),
            ("sub_category", StringType()),
            ("brand",        StringType()),
            ("supplier_id",  StringType()),
            ("cost_price",   FloatType()),
            ("sell_price",   FloatType()),
            ("launch_date",  DateType()),
            ("weight",       FloatType()),
            ("dimensions",   StringType()),
            ("color",        StringType()),
            ("size",         StringType()),
            ("material",     StringType()),
        ])
        print("Cast products DataFrame")

    # 6. Inventory
    if "inventory" in dataframes:
        dataframes["inventory"] = _cast_existing(dataframes["inventory"], [
            ("inventory_id",       StringType()),
            ("product_id",         StringType()),
            ("supplier_id",        StringType()),
            ("stock_quantity",     IntegerType()),
            ("reserved_quantity",  IntegerType()),
            ("minimum_stock_level",IntegerType()),
            ("last_restocked_date",DateType()),
            ("storage_cost",       FloatType()),
            ("stock_status",       StringType()),
        ])
        print("Cast inventory DataFrame")

    # 7. Wishlist
    if "wishlist" in dataframes:
        dataframes["wishlist"] = _cast_existing(dataframes["wishlist"], [
            ("wishlist_id",    StringType()),
            ("customer_id",    StringType()),
            ("product_id",     StringType()),
            ("added_date",     DateType()),
            ("purchased_date", DateType()),
            ("removed_date",   DateType()),
        ])
        print("Cast wishlist DataFrame")

    # 8. Shopping Cart
    if "shopping_cart" in dataframes:
        dataframes["shopping_cart"] = _cast_existing(dataframes["shopping_cart"], [
            ("cart_id",     StringType()),
            ("customer_id", StringType()),
            ("session_id",  StringType()),
            ("cart_status", StringType()),
            ("created_at",  TimestampType()),
            ("updated_at",  TimestampType()),
        ])
        print("Cast shopping_cart DataFrame")

    # 8a. Cart Items
    if "cart_items" in dataframes:
        dataframes["cart_items"] = _cast_existing(dataframes["cart_items"], [
            ("cart_item_id", LongType()),
            ("cart_id",      StringType()),
            ("product_id",   StringType()),
            ("quantity",     IntegerType()),
            ("unit_price",   FloatType()),
            ("total_price",  FloatType()),
            ("added_at",     TimestampType()),
            ("updated_at",   TimestampType()),
            ("item_status",  StringType()),
        ])
        print("Cast cart_items DataFrame")

    # 9. Orders
    if "orders" in dataframes:
        dataframes["orders"] = _cast_existing(dataframes["orders"], [
            ("order_id",           StringType()),
            ("customer_id",        StringType()),
            ("order_status",       StringType()),
            ("subtotal",           FloatType()),
            ("tax_amount",         FloatType()),
            ("shipping_cost",      FloatType()),
            ("total_discount",     FloatType()),
            ("total_amount",       FloatType()),
            ("currency",           StringType()),
            ("order_placed_at",    TimestampType()),
            ("order_shipped_at",   DateType()),
            ("order_delivered_at", DateType()),
        ])
        print("Cast orders DataFrame")

    # 10. Order Items
    if "order_items" in dataframes:
        dataframes["order_items"] = _cast_existing(dataframes["order_items"], [
            ("order_item_id",  StringType()),
            ("order_id",       StringType()),
            ("product_id",     StringType()),
            ("quantity",       IntegerType()),
            ("discount_amount",FloatType()),
            ("product_price",  FloatType()),
        ])
        print("Cast order_items DataFrame")

    # 11. Payments
    if "payments" in dataframes:
        dataframes["payments"] = _cast_existing(dataframes["payments"], [
            ("payment_id",       StringType()),
            ("order_id",         StringType()),
            ("payment_method",   StringType()),
            ("payment_provider", StringType()),
            ("payment_status",   StringType()),
            ("transaction_id",   StringType()),
            ("processing_fee",   FloatType()),
            ("refund_amount",    FloatType()),
            ("refund_date",      DateType()),
            ("payment_date",     DateType()),
        ])
        print("Cast payments DataFrame")

    # 12. Reviews
    if "reviews" in dataframes:
        dataframes["reviews"] = _cast_existing(dataframes["reviews"], [
            ("review_id",   StringType()),
            ("product_id",  StringType()),
            ("customer_id", StringType()),
            ("rating",      IntegerType()),
            ("review_title",StringType()),
            ("review_desc", StringType()),
            ("review_date", TimestampType()),
        ])
        print("Cast reviews DataFrame")

    # 13. Marketing Campaigns
    if "marketing_campaigns" in dataframes:
        dataframes["marketing_campaigns"] = _cast_existing(dataframes["marketing_campaigns"], [
            ("campaign_id",      StringType()),
            ("campaign_name",    StringType()),
            ("campaign_type",    StringType()),
            ("start_date",       DateType()),
            ("end_date",         DateType()),
            ("budget",           FloatType()),
            ("spent_amount",     FloatType()),
            ("impressions",      IntegerType()),
            ("clicks",           IntegerType()),
            ("conversions",      IntegerType()),
            ("target_audience",  StringType()),
            ("campaign_status",  StringType()),
        ])
        print("Cast marketing_campaigns DataFrame")

    # 14. Customer Sessions
    if "customer_sessions" in dataframes:
        dataframes["customer_sessions"] = _cast_existing(dataframes["customer_sessions"], [
            ("session_id",             StringType()),
            ("customer_id",            StringType()),
            ("session_start",          TimestampType()),
            ("session_end",            TimestampType()),
            ("device_type",            StringType()),
            ("referrer_source",        StringType()),
            ("pages_viewed",           IntegerType()),
            ("products_viewed",        IntegerType()),
            ("conversion_flag",        BooleanType()),
            ("cart_abandonment_flag",  BooleanType()),
        ])
        print("Cast customer_sessions DataFrame")

    print("\n✅ All DataFrames cast successfully!")
    return dataframes
