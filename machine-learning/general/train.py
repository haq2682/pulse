from training.classification.train_cart_abandonment import main as cart_abandonment
from training.classification.train_customer_churn import main as customer_churn
from training.classification.train_customer_segments import main as customer_segments
from training.classification.train_payment_success import main as payment_success
from training.classification.train_review_sentiment import main as review_sentiment
from training.classification.train_stock_status import main as stock_status

from training.regression.train_aov_v2 import main as aov
from training.regression.train_clv import main as clv
from training.regression.train_restock_quantity import main as restock_quantity
from training.regression.train_revenue_forecast import main as revenue_forecast
from training.regression.train_safety_stock import main as safety_stock
from training.regression.train_session_conversion import main as session_conversion
from training.regression.train_stockout_probability import main as stockout_probability

from training.clustering.train_customer_segment import main as customer_segment
from training.clustering.train_geo_cluster import main as geo_cluster
from training.clustering.train_session_behavior import main as session_behavior
from training.clustering.train_supplier_performance import main as supplier_performance

def main(BUCKET_NAME):
    cart_abandonment(BUCKET_NAME)
    customer_churn(BUCKET_NAME)
    customer_segments(BUCKET_NAME)
    payment_success(BUCKET_NAME)
    review_sentiment(BUCKET_NAME)
    stock_status(BUCKET_NAME)

    aov(BUCKET_NAME)
    clv(BUCKET_NAME)
    restock_quantity(BUCKET_NAME)
    revenue_forecast(BUCKET_NAME)
    safety_stock(BUCKET_NAME)
    session_conversion(BUCKET_NAME)
    stockout_probability(BUCKET_NAME)

    customer_segment(BUCKET_NAME)
    geo_cluster(BUCKET_NAME)
    session_behavior(BUCKET_NAME)
    supplier_performance(BUCKET_NAME)

if __name__ == "__main__":
    BUCKET_NAME = "pulse-bucket-1"
    main(BUCKET_NAME)