import os
from io import BytesIO
from minio import Minio

# Mapping from dataframe names to output file names (aligned with agg_schema.sql)
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
    "cart_items": "agg_cart_items",
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


def get_minio_client():
    """Create and return a MinIO client instance."""
    return Minio(
        os.getenv("MINIO_ENDPOINT"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False,
    )


def export_to_minio(dataframes, bucket_name=None):
    """
    Export transformed DataFrames to MinIO in the transformed/ directory.
    
    File names are aligned with the schema in ./sql/agg_schema.sql.
    
    Args:
        dataframes: Dictionary of dataframe names to Spark DataFrames
        bucket_name: Optional bucket name, defaults to env MINIO_BUCKET or 'pulse-bucket-1'
    """
    minio_client = get_minio_client()
    bucket_name = bucket_name or os.getenv("MINIO_BUCKET", "pulse-bucket-1")
    
    # Create bucket if not exists
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
        print(f"Created bucket: {bucket_name}")
    
    print("\n" + "=" * 60)
    print("📤 EXPORTING TRANSFORMED DATA TO MINIO")
    print("=" * 60)
    print(f"Bucket: {bucket_name}")
    print(f"Directory: transformed/")
    print("=" * 60)
    
    successful = 0
    failed = 0
    
    for df_name, table_name in TABLE_MAPPINGS.items():
        if df_name in dataframes and dataframes[df_name] is not None:
            try:
                df = dataframes[df_name]
                row_count = df.count()
                
                if row_count == 0:
                    print(f"  ⏭️  {table_name}: Skipped (empty)")
                    continue
                
                # Convert to Pandas and save as CSV
                pdf = df.toPandas()
                csv_buffer = BytesIO()
                pdf.to_csv(csv_buffer, index=False)
                csv_buffer.seek(0)
                
                # File path: transformed/{agg_table_name}.csv
                file_path = f"transformed/{table_name}.csv"
                
                minio_client.put_object(
                    bucket_name,
                    file_path,
                    csv_buffer,
                    length=len(csv_buffer.getvalue()),
                    content_type="text/csv",
                )
                
                csv_buffer.close()
                print(f"  ✅ {table_name}: {row_count} rows saved")
                successful += 1
                
            except Exception as e:
                print(f"  ❌ {table_name}: Error - {str(e)}")
                failed += 1
    
    print("\n" + "=" * 60)
    print(f"📊 EXPORT SUMMARY")
    print("=" * 60)
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print("=" * 60)
    
    return {"successful": successful, "failed": failed}


# Keep backward compatibility with existing code
def export_to_postgres(dataframes):
    """
    Export to MinIO instead of PostgreSQL.
    This function is kept for backward compatibility.
    """
    return export_to_minio(dataframes)
