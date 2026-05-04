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

        # Build dynamic column list for products based on available columns
        products_columns = dataframes["products"].columns
        
        # Define all possible product columns to select (in order)
        all_product_cols = [
            "product_id", "product_name", "sku", "brand", 
            "supplier_id", "cost_price", "sell_price", "launch_date",
            "weight", "dimensions", "color", "size", "material"
        ]
        
        # Filter to only include columns that exist
        available_cols = [col for col in all_product_cols if col in products_columns]
        
        # Build SELECT clause for products columns
        p_select = ", ".join([f"p.{col}" for col in available_cols])
        
        # Build the SQL query with dynamic columns
        products_query = f"""
        SELECT {p_select}, cat.category, cat.sub_category
        FROM products p
        LEFT JOIN categories cat
              ON p.category_id = cat.category_id
        """
        
        products = spark.sql(products_query)
        dataframes["products"] = products
        print("Merged categories into products.")
        dataframes.pop("categories", None)
    elif "categories" not in dataframes:
        print("Categories DataFrame is missing.")
    
    return dataframes
