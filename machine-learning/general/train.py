import sys
from pathlib import Path

# Add the machine-learning directory to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from general.training.classification.train_cart_abandonment import main as cart_abandonment
from general.training.classification.train_customer_churn import main as customer_churn
from general.training.classification.train_customer_segments import main as customer_segments
from general.training.classification.train_payment_success import main as payment_success
from general.training.classification.train_review_sentiment import main as review_sentiment
from general.training.classification.train_stock_status import main as stock_status

from general.training.regression.train_aov_v2 import main as aov
from general.training.regression.train_clv import main as clv
from general.training.regression.train_restock_quantity import main as restock_quantity
from general.training.regression.train_safety_stock import main as safety_stock
from general.training.regression.train_session_conversion import main as session_conversion
from general.training.regression.train_stockout_probability import main as stockout_probability

from general.training.clustering.train_customer_segment import main as customer_segment
from general.training.clustering.train_geo_cluster import main as geo_cluster
from general.training.clustering.train_session_behavior import main as session_behavior
from general.training.clustering.train_supplier_performance import main as supplier_performance

def main():
    # cart_abandonment()
    # customer_churn()
    # customer_segments()
    # payment_success()
    # review_sentiment()
    # stock_status()

    # aov()
    # clv()
    # restock_quantity()
    safety_stock()
    # session_conversion()
    # stockout_probability()

    # customer_segment()
    # geo_cluster()
    # session_behavior()
    # supplier_performance()

if __name__ == "__main__":
    main()
