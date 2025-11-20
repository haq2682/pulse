import os
import psycopg2

TABLE_MAPPINGS = {
    "customers": "agg_customers",
    "orders": "agg_orders",
    "products": "agg_products",
    "marketing_campaigns": "agg_marketing_campaigns",
    "suppliers": "agg_suppliers",
    "inventory": "agg_inventory",
    "customer_sessions": "agg_customer_sessions",
    "wishlist": "agg_wishlist",
    "shopping_cart": "agg_shopping_cart",
    "reviews": "agg_reviews",
    "order_items": "agg_order_items",
    "payments": "agg_payments",
    "daily_aggregations": "agg_daily_aggregations",
    "weekly_aggregations": "agg_weekly_aggregations",
    "monthly_aggregations": "agg_monthly_aggregations",
    "country_aggregations": "agg_country_aggregations",
    "state_aggregations": "agg_state_aggregations",
    "city_aggregations": "agg_city_aggregations",
    "categories": "agg_categories",
    "cart_abandonment_analysis": "agg_cart_abandonment_analysis",
    "product_inventory_health": "agg_product_inventory_health",
    "supplier_inventory_health": "agg_supplier_inventory_health",
    "rfm_segmentation": "agg_rfm_segmentation",
    "rfm_segment_summary": "agg_rfm_segment_summary",
    "product_affinity": "agg_product_affinity",
    "top_product_pairs": "agg_top_product_pairs",
    "product_recommendations": "agg_product_recommendations",
    "category_affinity": "agg_category_affinity",
    "global_aggregations": "agg_global_aggregations",
}


def export_to_postgres(dataframes):
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_SERVER", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DATABASE_NAME"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    conn.autocommit = True
    cursor = conn.cursor()

    print("Clearing existing data...")
    for table_name in TABLE_MAPPINGS.values():
        cursor.execute(f"TRUNCATE TABLE {table_name} CASCADE")

    print("\nLoading data to PostgreSQL...")
    jdbc_url = f"jdbc:postgresql://{os.getenv('POSTGRES_SERVER', 'localhost')}:{os.getenv('POSTGRES_PORT', '5432')}/pulse"

    try:
        for df_name, table_name in TABLE_MAPPINGS.items():
            if df_name in dataframes:
                print(f"  Loading {table_name}...")
                dataframes[df_name].write.format("jdbc").option("url", jdbc_url).option(
                    "dbtable", table_name
                ).option("user", os.getenv("POSTGRES_USER")).option(
                    "password", os.getenv("POSTGRES_PASSWORD")
                ).option(
                    "driver", "org.postgresql.Driver"
                ).mode(
                    "append"
                ).save()
                print(f"    ✓ Done")
        print("\n✅ All data loaded successfully!")
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()
