import os
from minio import Minio
from minio.commonconfig import CopySource
from .schema_enforcer_parquet import (
    enforce_schema_with_types, 
    get_expected_schema
)

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


def export_to_minio(dataframes, bucket_name=None, sql_schema_path=None, 
                   enforce_schemas=True, preserve_types=True, compression='snappy'):
    """
    Export transformed DataFrames to MinIO in the transformed/ directory as Parquet files.
    """
    minio_client = get_minio_client()
    bucket_name = bucket_name or os.getenv("MINIO_BUCKET", "pulse-bucket-1")
    
    # Create bucket if not exists
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
        print(f"Created bucket: {bucket_name}")
    
    print("\n" + "=" * 60)
    print("📤 EXPORTING TRANSFORMED DATA TO MINIO (PARQUET)")
    print("=" * 60)
    print(f"Bucket: {bucket_name}")
    print(f"Directory: transformed/")
    print(f"Format: Parquet")
    print(f"Compression: {compression}")
    print(f"Schema Enforcement: {'✓ Enabled' if enforce_schemas else '✗ Disabled'}")
    print(f"Type Preservation: {'✓ Enabled' if preserve_types else '✗ Disabled'}")
    print("=" * 60)
    
    successful = 0
    failed = 0
    schema_enforced = 0
    
    for df_name, table_name in TABLE_MAPPINGS.items():
        if df_name in dataframes and dataframes[df_name] is not None:
            try:
                df = dataframes[df_name]
                row_count = df.count()
                
                if row_count == 0:
                    print(f"  ⭕ {table_name}: Skipped (empty)")
                    continue
                
                # Apply schema enforcement if enabled
                if enforce_schemas:
                    expected_schema = get_expected_schema(table_name, with_types=True)
                    
                    if expected_schema:
                        original_cols = set(df.columns)
                        df = enforce_schema_with_types(df, expected_schema, preserve_types=preserve_types)
                        
                        expected_col_names = [col_name for col_name, _ in expected_schema]
                        added_cols = set(expected_col_names) - original_cols
                        if added_cols:
                            print(f"  📝 {table_name}: Added {len(added_cols)} missing columns as NULL")
                            schema_enforced += 1
                    else:
                        print(f"  ⚠️  {table_name}: No schema definition found, exporting as-is")
                
                # Write to temporary Spark location in MinIO
                temp_path = f"s3a://{bucket_name}/temp_{table_name}"
                
                df.coalesce(1).write \
                    .mode("overwrite") \
                    .option("compression", compression) \
                    .parquet(temp_path)
                
                # Find the parquet file in the temp directory
                temp_objects = list(minio_client.list_objects(
                    bucket_name, 
                    prefix=f"temp_{table_name}/", 
                    recursive=True
                ))
                
                parquet_file = next(
                    (obj for obj in temp_objects if obj.object_name.endswith(".parquet")), 
                    None
                )
                
                if parquet_file:
                    final_path = f"transformed/{table_name}.parquet"
                    
                    minio_client.copy_object(
                        bucket_name,
                        final_path,
                        CopySource(bucket_name, parquet_file.object_name)
                    )
                    
                    # Clean up temp files
                    for obj in temp_objects:
                        minio_client.remove_object(bucket_name, obj.object_name)
                    
                    # Get file size for reporting
                    stat = minio_client.stat_object(bucket_name, final_path)
                    file_size_mb = stat.size / (1024 * 1024)
                    
                    print(f"  ✅ {table_name}: {row_count:,} rows saved ({file_size_mb:.2f} MB)")
                else:
                    print(f"  ⚠️  {table_name}: Parquet file not found in temp directory")
                    failed += 1
                    continue
                
                successful += 1
                
            except Exception as e:
                print(f"  ❌ {table_name}: Error - {str(e)}")
                import traceback
                traceback.print_exc()
                failed += 1
    
    print("\n" + "=" * 60)
    print(f"📊 EXPORT SUMMARY")
    print("=" * 60)
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    if enforce_schemas:
        print(f"Schema Enforced: {schema_enforced}")
    print("=" * 60)
    
    return {"successful": successful, "failed": failed, "schema_enforced": schema_enforced}
