import os
from dotenv import load_dotenv, find_dotenv

from config.spark_config import create_spark_session
from config.minio_config import create_minio_client, BUCKET_NAME
from loaders.data_loader import load_data_from_minio
from transformations.orders import transform_orders
from transformations.customers import transform_customers, add_customer_aggregations
from transformations.marketing import transform_marketing, add_marketing_metrics
from transformations.products import transform_products
from transformations.inventory import transform_inventory
from transformations.sessions import transform_sessions, transform_wishlist, transform_shopping_cart
from transformations.reviews import transform_reviews
from transformations.suppliers import transform_suppliers
from transformations.aggregations import (
    create_time_aggregations,
    create_geography_aggregations,
    create_categories,
    create_cart_analysis,
    create_inventory_health,
    create_rfm_segmentation,
    create_product_affinity,
    create_global_aggregations
)
from exporters.postgres_exporter import export_to_postgres

load_dotenv(find_dotenv())

def main():
    spark = create_spark_session()
    minio_client = create_minio_client()
    
    dataframes = load_data_from_minio(spark, minio_client, BUCKET_NAME)
    
    transform_orders(dataframes)
    transform_customers(dataframes)
    transform_marketing(dataframes)
    transform_products(dataframes)
    transform_inventory(dataframes)
    transform_sessions(dataframes)
    transform_wishlist(dataframes)
    transform_shopping_cart(dataframes)
    transform_reviews(dataframes)
    transform_suppliers(dataframes)
    
    add_customer_aggregations(dataframes)
    add_marketing_metrics(dataframes)
    
    create_time_aggregations(dataframes)
    create_geography_aggregations(dataframes)
    create_categories(dataframes)
    create_cart_analysis(dataframes)
    create_inventory_health(dataframes)
    create_rfm_segmentation(dataframes)
    create_product_affinity(dataframes)
    create_global_aggregations(dataframes, spark)
    
    export_to_postgres(dataframes)
    
    spark.stop()

if __name__ == "__main__":
    main()
