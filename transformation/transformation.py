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
from transformations.sessions import (
    transform_sessions,
    transform_wishlist,
    transform_shopping_cart,
)
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
    create_global_aggregations,
)
from exporters.postgres_exporter import export_to_postgres

load_dotenv(find_dotenv())


def main():
    spark = create_spark_session()
    minio_client = create_minio_client()

    dataframes = load_data_from_minio(spark, minio_client, BUCKET_NAME)

    print("Transforming Orders...")
    transform_orders(dataframes)
    dataframes["orders"].show(5)
    print("Transforming Customers...")
    transform_customers(dataframes)
    dataframes["customers"].show(5)
    print("Transforming Marketing Campaigns...")
    transform_marketing(dataframes)
    dataframes["marketing_campaigns"].show(5)
    print("Transforming Products...")
    transform_products(dataframes)
    dataframes["products"].show(5)
    print("Transforming Inventory...")
    transform_inventory(dataframes)
    dataframes["inventory"].show(5)
    print("Transforming Sessions...")
    transform_sessions(dataframes)
    dataframes["customer_sessions"].show(5)
    print("Transforming Wishlist...")
    transform_wishlist(dataframes)
    dataframes["wishlist"].show(5)
    print("Transforming Shopping Cart...")
    transform_shopping_cart(dataframes)
    dataframes["shopping_cart"].show(5)
    print("Transforming Reviews...")
    transform_reviews(dataframes)
    dataframes["reviews"].show(5)
    print("Transforming Suppliers...")
    transform_suppliers(dataframes)
    dataframes["suppliers"].show(5)

    print("Adding Customer Aggregations...")
    add_customer_aggregations(dataframes)
    dataframes["customers"].show(5)
    print("Adding Marketing Metrics...")
    add_marketing_metrics(dataframes)
    dataframes["marketing_campaigns"].show(5)

    print("Creating Higher-Level Aggregations...")
    create_time_aggregations(dataframes)
    dataframes["daily_aggregations"].show(5)
    dataframes["weekly_aggregations"].show(5)
    dataframes["monthly_aggregations"].show(5)
    print("Creating Geography Aggregations...")
    create_geography_aggregations(dataframes)
    dataframes["country_aggregations"].show(5)
    dataframes["state_aggregations"].show(5)
    dataframes["city_aggregations"].show(5)
    print("Creating Categories...")
    create_categories(dataframes)
    dataframes["categories"].show(5)
    print("Creating Cart Analysis...")
    create_cart_analysis(dataframes)
    dataframes["cart_abandonment_analysis"].show(5)
    print("Creating Inventory Health...")
    create_inventory_health(dataframes)
    dataframes["product_inventory_health"].show(5)
    dataframes["supplier_inventory_health"].show(5)
    print("Creating RFM Segmentation...")
    create_rfm_segmentation(dataframes)
    dataframes["rfm_segmentation"].show(5)
    print("Creating Product Affinity...")
    create_product_affinity(dataframes)
    dataframes["product_affinity"].show(5)
    print("Creating Global Aggregations...")
    create_global_aggregations(dataframes, spark)
    dataframes["global_aggregations"].show(5)

    export_to_postgres(dataframes)

    spark.stop()


if __name__ == "__main__":
    main()
