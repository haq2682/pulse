"""
Shared table name mapping utility for canonical schema.
Provides fuzzy matching and synonym mapping for table names across all ingestion modes.
"""

from typing import Optional
from rapidfuzz import fuzz, process


# Canonical tables in the e-commerce schema
VALID_TABLES = [
    "addresses",
    "cart_items",
    "categories",
    "customer_sessions",
    "customers",
    "inventory",
    "marketing_campaigns",
    "order_items",
    "orders",
    "payments",
    "products",
    "reviews",
    "shopping_cart",
    "suppliers",
    "wishlist",
]

# Synonym mapping for common table name variations
# Maps source table names (lowercase) to canonical table names
TABLE_SYNONYMS = {
    # Customers/Users
    "customers": "customers",
    "customer": "customers",
    "users": "customers",
    "user": "customers",
    "clients": "customers",
    "client": "customers",
    
    # Addresses
    "addresses": "addresses",
    "address": "addresses",
    "locations": "addresses",
    "location": "addresses",
    
    # Products
    "products": "products",
    "product": "products",
    "items": "products",
    "item": "products",
    "catalog": "products",
    
    # Inventory
    "inventories": "inventory",
    "inventory": "inventory",
    "stock": "inventory",
    "stocks": "inventory",
    
    # Orders
    "orders": "orders",
    "order": "orders",
    "sales": "orders",
    "sale": "orders",
    "transactions": "orders",
    "transaction": "orders",
    
    # Order Items
    "order_items": "order_items",
    "orderitems": "order_items",
    "order_item": "order_items",
    "orderitem": "order_items",
    "line_items": "order_items",
    "lineitems": "order_items",
    
    # Reviews
    "reviews": "reviews",
    "review": "reviews",
    "ratings": "reviews",
    "rating": "reviews",
    "feedback": "reviews",
    
    # Categories
    "categories": "categories",
    "category": "categories",
    "types": "categories",
    "type": "categories",
    
    # Wishlist
    "wishlists": "wishlist",
    "wishlist": "wishlist",
    "favorites": "wishlist",
    "favourite": "wishlist",
    "favourites": "wishlist",
    
    # Payments
    "payments": "payments",
    "payment": "payments",
    "billing": "payments",
    "invoices": "payments",
    "invoice": "payments",
    
    # Shopping Cart
    "shopping_carts": "shopping_cart",
    "shopping_cart": "shopping_cart",
    "cart": "shopping_cart",
    "carts": "shopping_cart",
    "basket": "shopping_cart",
    "baskets": "shopping_cart",
    
    # Cart Items
    "cart_items": "cart_items",
    "cartitems": "cart_items",
    "cart_item": "cart_items",
    "cartitem": "cart_items",
    "shopping_cart_items": "cart_items",
    "basket_items": "cart_items",
    
    # Customer Sessions
    "customer_sessions": "customer_sessions",
    "sessions": "customer_sessions",
    "session": "customer_sessions",
    "user_sessions": "customer_sessions",
    
    # Marketing Campaigns
    "marketing_campaigns": "marketing_campaigns",
    "campaigns": "marketing_campaigns",
    "campaign": "marketing_campaigns",
    "promotions": "marketing_campaigns",
    "promotion": "marketing_campaigns",
    
    # Suppliers
    "suppliers": "suppliers",
    "supplier": "suppliers",
    "vendors": "suppliers",
    "vendor": "suppliers",
    "partners": "suppliers",
    "partner": "suppliers",
}


def map_table_name(table_name: str, threshold: int = 85) -> Optional[str]:
    """
    Map a discovered table name to canonical schema using exact match,
    synonym lookup, and fuzzy matching.
    
    Args:
        table_name: Discovered table name from source
        threshold: Minimum similarity score for fuzzy matching (0-100)
        
    Returns:
        Canonical table name or None if no match found
        
    Examples:
        >>> map_table_name("orders")
        'orders'
        >>> map_table_name("customer")
        'customers'
        >>> map_table_name("orderitems")
        'order_items'
        >>> map_table_name("unknown_table")
        None
    """
    if not table_name:
        return None
        
    # Normalize: lowercase and strip whitespace
    table_lower = table_name.lower().strip()
    
    # Step 1: Try exact match with canonical tables
    if table_lower in VALID_TABLES:
        return table_lower
    
    # Step 2: Try synonym lookup
    if table_lower in TABLE_SYNONYMS:
        canonical = TABLE_SYNONYMS[table_lower]
        if canonical in VALID_TABLES:
            return canonical
    
    # Step 3: Try fuzzy matching against canonical tables
    # Use fuzzywuzzy's process.extractOne to find best match
    match = process.extractOne(
        table_lower,
        VALID_TABLES,
        scorer=fuzz.ratio,
        score_cutoff=threshold
    )
    
    if match:
        best_match, score = match[0], match[1]
        print(f"  ℹ️  Fuzzy match: '{table_name}' → '{best_match}' (similarity: {score}%)")
        return best_match
    
    # No match found
    return None


def get_all_canonical_tables() -> list:
    """
    Get list of all canonical table names.
    
    Returns:
        List of canonical table names
    """
    return VALID_TABLES.copy()


def is_canonical_table(table_name: str) -> bool:
    """
    Check if a table name is a canonical table (exact match).
    
    Args:
        table_name: Table name to check
        
    Returns:
        True if table is canonical, False otherwise
    """
    return table_name.lower() in VALID_TABLES
