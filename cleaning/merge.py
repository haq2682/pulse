"""
Merge module for joining related tables.
"""

import pyspark.sql.functions as F


def _first_available_expr(candidates):
    """Return the first available Spark column expression from ``[(alias, name), ...]``."""
    exprs = [F.col(f"{alias}.{name}") for alias, name in candidates]
    if not exprs:
        return None
    if len(exprs) == 1:
        return exprs[0]
    return F.coalesce(*exprs)


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
        customers_df = dataframes["customers"]
        addresses_df = dataframes["addresses"]

        if "address_id" not in customers_df.columns or "address_id" not in addresses_df.columns:
            print("⚠️ Skipping addresses → customers merge because 'address_id' is missing.")
        else:
            joined = customers_df.alias("c").join(
                addresses_df.alias("a"),
                F.col("c.address_id") == F.col("a.address_id"),
                "left",
            )

            output_specs = [
                ("customer_id", [("c", "customer_id")]),
                ("gender", [("c", "gender")]),
                ("date_of_birth", [("c", "date_of_birth")]),
                ("account_status", [("c", "account_status")]),
                ("city", [("a", "city"), ("c", "city")]),
                ("state_province", [("a", "state_province"), ("c", "state_province")]),
                ("postal_code", [("a", "postal_code"), ("c", "postal_code")]),
                ("country", [("a", "country"), ("c", "country")]),
                ("account_created_at", [("c", "account_created_at")]),
                ("last_login_date", [("c", "last_login_date")]),
                ("is_active", [("c", "is_active")]),
            ]

            select_exprs = []
            for output_name, candidates in output_specs:
                expr = _first_available_expr(candidates)
                if expr is not None:
                    select_exprs.append(expr.alias(output_name))

            if select_exprs:
                dataframes["customers"] = joined.select(*select_exprs)
                print("Merged addresses into customers.")
                dataframes.pop("addresses", None)
            else:
                print("⚠️ Skipping addresses → customers merge because no output columns were available.")
    elif "addresses" not in dataframes:
        print("Addresses DataFrame is missing.")
    
    # Merge categories into products
    if "categories" in dataframes and "products" in dataframes:
        products_df = dataframes["products"]
        categories_df = dataframes["categories"]

        if "category_id" not in products_df.columns or "category_id" not in categories_df.columns:
            print("⚠️ Skipping categories → products merge because 'category_id' is missing.")
        else:
            joined = products_df.alias("p").join(
                categories_df.alias("cat"),
                F.col("p.category_id") == F.col("cat.category_id"),
                "left",
            )

            all_product_cols = [
                "product_id", "product_name", "sku", "brand",
                "supplier_id", "cost_price", "sell_price", "launch_date",
                "weight", "dimensions", "color", "size", "material",
            ]

            output_specs = [(col_name, [("p", col_name)]) for col_name in all_product_cols]
            output_specs.extend([
                ("category", [("cat", "category"), ("p", "category")]),
                ("sub_category", [("cat", "sub_category"), ("p", "sub_category")]),
            ])

            select_exprs = []
            for output_name, candidates in output_specs:
                expr = _first_available_expr(candidates)
                if expr is not None:
                    select_exprs.append(expr.alias(output_name))

            if select_exprs:
                dataframes["products"] = joined.select(*select_exprs)
                print("Merged categories into products.")
                dataframes.pop("categories", None)
            else:
                print("⚠️ Skipping categories → products merge because no output columns were available.")
    elif "categories" not in dataframes:
        print("Categories DataFrame is missing.")
    
    return dataframes
