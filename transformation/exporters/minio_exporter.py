import os
from minio import Minio
from .schema_enforcer_parquet import (
    enforce_schema_with_types, 
    get_expected_schema
)


def _is_df_empty(df):
    """
    JVM-side emptiness check to avoid expensive Python RDD conversion.
    Falls back to a minimal LIMIT 1 action when needed.
    """
    try:
        return bool(df._jdf.isEmpty())
    except Exception:
        return df.limit(1).count() == 0


def parse_minio_endpoint(endpoint_url):
    """
    Parse MinIO endpoint URL and strip protocol prefix if present.
    
    The MinIO Python client expects endpoint in format 'hostname:port' without
    protocol prefix. This function handles both formats:
    - With protocol: 'http://localhost:9000' -> 'localhost:9000'
    - Without protocol: 'localhost:9000' -> 'localhost:9000'
    
    Args:
        endpoint_url: MinIO endpoint URL (e.g., 'localhost:9000' or 'http://localhost:9000')
        
    Returns:
        str: Endpoint in 'hostname:port' format
        
    Raises:
        ValueError: If endpoint is empty or invalid after parsing
    """
    if not endpoint_url:
        raise ValueError("MINIO_ENDPOINT cannot be empty")
    
    # Strip protocol prefix if present
    if "://" in endpoint_url:
        endpoint_url = endpoint_url.split("://", 1)[1]
    
    # Validate that we have a non-empty endpoint after parsing
    endpoint_url = endpoint_url.strip()
    if not endpoint_url:
        raise ValueError("MINIO_ENDPOINT is invalid after removing protocol")
    
    return endpoint_url


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
    # Parse MINIO_ENDPOINT to strip protocol prefix if present
    minio_endpoint = parse_minio_endpoint(os.getenv("MINIO_ENDPOINT", "localhost:9000"))
    
    return Minio(
        minio_endpoint,
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
    
    # Full row counting can be very expensive on large aggregation outputs and
    # may trigger JVM heap pressure. Keep it opt-in for diagnostics only.
    log_row_counts = os.getenv("EXPORT_LOG_ROW_COUNTS", "false").lower() == "true"

    for df_name, table_name in TABLE_MAPPINGS.items():
        if df_name in dataframes and dataframes[df_name] is not None:
            try:
                df = dataframes[df_name]

                # Lightweight emptiness check without javaToPython() conversion.
                if _is_df_empty(df):
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
                
                # Write directly to the final parquet directory in MinIO.
                # Avoid coalesce(1), which can force all data through a single
                # partition and cause executor/driver OOM on larger datasets.
                final_path = f"s3a://{bucket_name}/transformed/{table_name}.parquet"

                (df.write
                   .mode("overwrite")
                   .option("compression", compression)
                   .parquet(final_path))

                # Calculate total written parquet size under this directory
                object_prefix = f"transformed/{table_name}.parquet/"
                written_objects = list(
                    minio_client.list_objects(bucket_name, prefix=object_prefix, recursive=True)
                )
                parquet_objects = [obj for obj in written_objects if obj.object_name.endswith(".parquet")]

                if not parquet_objects:
                    print(f"  ⚠️  {table_name}: No parquet part files found after write")
                    failed += 1
                    continue

                total_bytes = sum(obj.size for obj in parquet_objects)
                file_size_mb = total_bytes / (1024 * 1024)

                if log_row_counts:
                    row_count = df.count()
                    print(f"  ✅ {table_name}: {row_count:,} rows saved ({file_size_mb:.2f} MB)")
                else:
                    print(f"  ✅ {table_name}: saved ({file_size_mb:.2f} MB)")
                
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
