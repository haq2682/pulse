"""
Schema module for casting DataFrames to correct data types.
"""
from pyspark.sql.types import *
from pyspark.sql.functions import col, when, from_unixtime


def cast_dataframes(dataframes):
    """
    Cast all DataFrames to their correct data types.
    
    Args:
        dataframes (dict): Dictionary of table names to DataFrames
        
    Returns:
        dict: Dictionary of table names to casted DataFrames
    """
    # 1. Addresses
    if "addresses" in dataframes:
        dataframes["addresses"] = dataframes["addresses"].select(
            col("address_id").cast(StringType()),
            col("city").cast(StringType()),
            col("state_province").cast(StringType()),
            col("postal_code").cast(StringType()),
            col("country").cast(StringType())
        )
        print("Cast addresses DataFrame")

    # 2. Customers
    if "customers" in dataframes:
        dataframes["customers"] = dataframes["customers"].select(
            col("customer_id").cast(StringType()),
            col("gender").cast(StringType()),
            col("date_of_birth").cast(DateType()),
            col("account_status").cast(StringType()),
            col("address_id").cast(StringType()),
            col("city").cast(StringType()),
            col("state_province").cast(StringType()),
            col("postal_code").cast(StringType()),
            col("country").cast(StringType()),
            col("account_created_at").cast(TimestampType()),
            col("last_login_date").cast(DateType()),
            col("is_active").cast(BooleanType())
        )
        print("Cast customers DataFrame")

    # 3. Suppliers
    if "suppliers" in dataframes:
        dataframes["suppliers"] = dataframes["suppliers"].select(
            col("supplier_id").cast(StringType()),
            col("supplier_rating").cast(FloatType()),
            col("supplier_status").cast(StringType()),
            col("is_preferred").cast(BooleanType()),
            col("is_verified").cast(BooleanType()),
            col("contract_start_date").cast(DateType()),
            col("contract_end_date").cast(DateType()),
            col("city").cast(StringType()),
            col("state").cast(StringType()),
            col("zip_code").cast(StringType()),
            col("country").cast(StringType())
        )
        print("Cast suppliers DataFrame")

    # 4. Categories
    if "categories" in dataframes:
        dataframes["categories"] = dataframes["categories"].select(
            col("category_id").cast(StringType()),
            col("category").cast(StringType()),
            col("sub_category").cast(StringType())
        )
        print("Cast categories DataFrame")

    # 5. Products
    if "products" in dataframes:
        dataframes["products"] = dataframes["products"].select(
            col("product_id").cast(StringType()),
            col("product_name").cast(StringType()),
            col("sku").cast(StringType()),
            col("category_id").cast(StringType()),
            col("category").cast(StringType()),
            col("sub_category").cast(StringType()),
            col("brand").cast(StringType()),
            col("supplier_id").cast(StringType()),
            col("cost_price").cast(FloatType()),
            col("sell_price").cast(FloatType()),
            col("launch_date").cast(DateType()),
            col("weight").cast(FloatType()),
            col("dimensions").cast(StringType()),
            col("color").cast(StringType()),
            col("size").cast(StringType()),
            col("material").cast(StringType())
        )
        print("Cast products DataFrame")

    # 6. Inventory
    if "inventory" in dataframes:
        dataframes["inventory"] = dataframes["inventory"].select(
            col("inventory_id").cast(StringType()),
            col("product_id").cast(StringType()),
            col("supplier_id").cast(StringType()),
            col("stock_quantity").cast(IntegerType()),
            col("reserved_quantity").cast(IntegerType()),
            col("minimum_stock_level").cast(IntegerType()),
            col("last_restocked_date").cast(DateType()),
            col("storage_cost").cast(FloatType())
        )
        print("Cast inventory DataFrame")

    # 7. Wishlist
    if "wishlist" in dataframes:
        dataframes["wishlist"] = dataframes["wishlist"].select(
            col("wishlist_id").cast(StringType()),
            col("customer_id").cast(StringType()),
            col("product_id").cast(StringType()),
            col("added_date").cast(DateType()),
            col("purchased_date").cast(DateType()),
            col("removed_date").cast(DateType())
        )
        print("Cast wishlist DataFrame")

    # 8. Shopping Cart
    if "shopping_cart" in dataframes:
        dataframes["shopping_cart"] = dataframes["shopping_cart"].select(
            col("cart_id").cast(StringType()),
            col("customer_id").cast(StringType()),
            col("session_id").cast(StringType()),
            col("product_id").cast(StringType()),
            col("quantity").cast(IntegerType()),
            col("unit_price").cast(FloatType()),
            col("added_date").cast(DateType()),
            col("cart_status").cast(StringType())
        )
        print("Cast shopping_cart DataFrame")

    # 9. Orders
    if "orders" in dataframes:
        dataframes["orders"] = dataframes["orders"].select(
            col("order_id").cast(StringType()),
            col("customer_id").cast(StringType()),
            col("order_status").cast(StringType()),
            col("subtotal").cast(FloatType()),
            col("tax_amount").cast(FloatType()),
            col("shipping_cost").cast(FloatType()),
            col("total_discount").cast(FloatType()),
            col("total_amount").cast(FloatType()),
            col("currency").cast(StringType()),
            col("order_placed_at").cast(TimestampType()),
            col("order_shipped_at").cast(DateType()),
            col("order_delivered_at").cast(DateType())
        )
        print("Cast orders DataFrame")

    # 10. Order Items
    if "order_items" in dataframes:
        dataframes["order_items"] = dataframes["order_items"].select(
            col("order_item_id").cast(StringType()),
            col("order_id").cast(StringType()),
            col("product_id").cast(StringType()),
            col("quantity").cast(IntegerType()),
            col("discount_amount").cast(FloatType()),
            col("product_cost").cast(FloatType())
        )
        print("Cast order_items DataFrame")

    # 11. Payments
    if "payments" in dataframes:
        dataframes["payments"] = dataframes["payments"].select(
            col("payment_id").cast(StringType()),
            col("order_id").cast(StringType()),
            col("payment_method").cast(StringType()),
            col("payment_provider").cast(StringType()),
            col("payment_status").cast(StringType()),
            col("transaction_id").cast(StringType()),
            col("processing_fee").cast(FloatType()),
            col("refund_amount").cast(FloatType()),
            col("refund_date").cast(DateType()),
            col("payment_date").cast(DateType())
        )
        print("Cast payments DataFrame")

    # 12. Reviews
    if "reviews" in dataframes:
        dataframes["reviews"] = dataframes["reviews"].select(
            col("review_id").cast(StringType()),
            col("product_id").cast(StringType()),
            col("customer_id").cast(StringType()),
            col("rating").cast(IntegerType()),
            col("review_title").cast(StringType()),
            col("review_desc").cast(StringType()),
            col("review_date").cast(TimestampType())
        )
        print("Cast reviews DataFrame")

    # 13. Marketing Campaigns
    if "marketing_campaigns" in dataframes:
        dataframes["marketing_campaigns"] = dataframes["marketing_campaigns"].select(
            col("campaign_id").cast(StringType()),
            col("campaign_name").cast(StringType()),
            col("campaign_type").cast(StringType()),
            col("start_date").cast(DateType()),
            when(col("end_date").isNotNull(), from_unixtime(col("end_date")).cast(DateType())).otherwise(None).alias("end_date"),
            col("budget").cast(FloatType()),
            col("spent_amount").cast(FloatType()),
            col("impressions").cast(IntegerType()),
            col("clicks").cast(IntegerType()),
            col("conversions").cast(IntegerType()),
            col("target_audience").cast(StringType()),
            col("campaign_status").cast(StringType())
        )
        print("Cast marketing_campaigns DataFrame")

    # 14. Customer Sessions
    if "customer_sessions" in dataframes:
        dataframes["customer_sessions"] = dataframes["customer_sessions"].select(
            col("session_id").cast(StringType()),
            col("customer_id").cast(StringType()),
            col("session_start").cast(TimestampType()),
            col("session_end").cast(TimestampType()),
            col("device_type").cast(StringType()),
            col("referrer_source").cast(StringType()),
            col("pages_viewed").cast(IntegerType()),
            col("products_viewed").cast(IntegerType()),
            col("conversion_flag").cast(BooleanType()),
            col("cart_abandonment_flag").cast(BooleanType())
        )
        print("Cast customer_sessions DataFrame")

    print("\n✅ All DataFrames cast successfully!")
    return dataframes
