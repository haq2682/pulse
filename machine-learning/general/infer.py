import sys
from pathlib import Path

# Add the machine-learning directory to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from general.inference.classification.infer_cart_abandonment import main as cart_abandonment
from general.inference.classification.infer_customer_churn import main as customer_churn
from general.inference.classification.infer_customer_segments import main as customer_segments
from general.inference.classification.infer_payment_success import main as payment_success
from general.inference.classification.infer_review_sentiment import main as review_sentiment
from general.inference.classification.infer_stock_status import main as stock_status

# from general.inference.regression.infer_aov_v2 import main as aov
from general.inference.regression.infer_clv import main as clv
from general.inference.regression.infer_restock_quantity import main as restock_quantity
from general.inference.regression.infer_safety_stock import main as safety_stock
from general.inference.regression.infer_session_conversion import main as session_conversion
from general.inference.regression.infer_stockout_probability import main as stockout_probability

from general.inference.clustering.infer_customer_segment import main as customer_segment
from general.inference.clustering.infer_geo_cluster import main as geo_cluster
from general.inference.clustering.infer_session_behavior import main as session_behavior
from general.inference.clustering.infer_supplier_performance import main as supplier_performance

def main(BUCKET_NAME):
    cart_abandonment(BUCKET_NAME)
    customer_churn(BUCKET_NAME)
    customer_segments(BUCKET_NAME)
    payment_success(BUCKET_NAME)
    review_sentiment(BUCKET_NAME)
    stock_status(BUCKET_NAME)

    # aov(BUCKET_NAME)
    clv(BUCKET_NAME)
    restock_quantity(BUCKET_NAME)
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