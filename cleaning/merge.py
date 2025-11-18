"""
Merge module for joining related tables.
"""


def merge_tables(dataframes, spark):
    """
    Merge addresses into customers and categories into products.
    
    Args:
        dataframes (dict): Dictionary of table names to DataFrames
        spark (SparkSession): Active Spark session
        
    Returns:
        dict: Updated dictionary with merged tables
    """
    # Merge addresses into customers
    if "addresses" in dataframes and "customers" in dataframes:
        dataframes["addresses"].createOrReplaceTempView("addresses")
        dataframes["customers"].createOrReplaceTempView("customers")

        customers = spark.sql("""
        SELECT c.customer_id, c.gender, c.date_of_birth, c.account_status,
              a.city, a.state_province, a.postal_code, a.country,
              c.account_created_at, c.last_login_date, c.is_active
        FROM customers c
        LEFT JOIN addresses a
              ON c.address_id = a.address_id
        """)
        dataframes["customers"] = customers
        print("Merged addresses into customers.")
        dataframes.pop("addresses", None)
    elif "addresses" not in dataframes:
        print("Addresses DataFrame is missing.")
    
    # Merge categories into products
    if "categories" in dataframes and "products" in dataframes:
        dataframes["categories"].createOrReplaceTempView("categories")
        dataframes["products"].createOrReplaceTempView("products")

        products = spark.sql("""
        SELECT p.product_id, p.product_name, p.sku, cat.category, cat.sub_category,
              p.brand, p.supplier_id, p.cost_price, p.sell_price, p.launch_date,
              p.weight, p.dimensions, p.color, p.size, p.material
        FROM products p
        LEFT JOIN categories cat
              ON p.category_id = cat.category_id
        """)
        dataframes["products"] = products
        print("Merged categories into products.")
        dataframes.pop("categories", None)
    elif "categories" not in dataframes:
        print("Categories DataFrame is missing.")
    
    return dataframes
