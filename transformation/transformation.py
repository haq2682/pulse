import os
from dotenv import load_dotenv, find_dotenv

from config.spark_config import create_spark_session
from config.minio_config import create_minio_client, BUCKET_NAME
from loaders.data_loader import load_data_from_minio
from exporters.postgres_exporter import export_to_postgres
from transformations.campaigns import transform_campaigns
from transformations.carts import transform_carts
from transformations.customer_sessions import transform_customer_sessions
from transformations.customers import transform_customers
from transformations.orders import transform_orders
from transformations.inventory import transform_inventory
from transformations.products import transform_products
from transformations.reviews import transform_reviews
from transformations.wishlists import transform_wishlists
from aggregations.campaigns import aggregate_campaigns
from aggregations.cart_abandonment import cart_abandonment_aggregations
from aggregations.categories import aggregate_categories
from aggregations.customers import aggregate_customers
from aggregations.geographic import geographic_aggregations
from aggregations.global_aggregations import global_aggregations
from aggregations.inventory_health import inventory_health_aggregations
from aggregations.product_affinity import product_affinity
from aggregations.products import aggregate_products
from aggregations.rfm_segmentation import rfm_segmentation
from aggregations.sessions import session_aggregations
from aggregations.suppliers import aggregate_suppliers
from aggregations.time_based import time_based_aggregations

load_dotenv(find_dotenv())


def main():
    spark = create_spark_session()
    minio_client = create_minio_client()

    dataframes = load_data_from_minio(spark, minio_client, BUCKET_NAME)

    transform_orders(dataframes)
    transform_customers(dataframes)
    transform_campaigns(dataframes)
    transform_wishlists(dataframes)
    transform_inventory(dataframes)
    transform_customer_sessions(dataframes)
    transform_reviews(dataframes)

    aggregate_customers(dataframes)
    aggregate_products(dataframes)
    aggregate_categories(dataframes)
    aggregate_suppliers(dataframes)
    aggregate_campaigns(dataframes)
    time_based_aggregations(dataframes)
    geographic_aggregations(dataframes)
    session_aggregations(dataframes)
    cart_abandonment_aggregations(dataframes)
    inventory_health_aggregations(dataframes)
    rfm_segmentation(dataframes)
    product_affinity(dataframes)
    global_aggregations(spark, dataframes)
    if "order_items" in dataframes and dataframes["order_items"] is not None:
        dataframes["order_items"] = dataframes["order_items"].dropDuplicates(
            ["order_item_id"]
        )

    if "payments" in dataframes and dataframes["payments"] is not None:
        dataframes["payments"] = dataframes["payments"].dropDuplicates(["payment_id"])
    export_to_postgres(dataframes)
    spark.stop()


if __name__ == "__main__":
    main()
