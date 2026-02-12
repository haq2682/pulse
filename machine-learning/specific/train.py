from training.classification.train_fulfillment_risk import main as fulfillment_risk
from training.classification.train_product_bundling import main as product_bundling

from training.regression.train_campaign_roi import main as campaign_roi
from training.regression.train_delivery_time import main as delivery_time
from training.regression.train_demand_forecasting import main as demand_forecasting
from training.regression.train_price_optimization import main as price_optimization

from training.clustering.train_product_affinity import main as product_affinity
from training.clustering.train_product_lifecycle import main as product_lifecycle

def main(BUCKET_NAME):
    fulfillment_risk(BUCKET_NAME)
    product_bundling(BUCKET_NAME)

    campaign_roi(BUCKET_NAME)
    delivery_time(BUCKET_NAME)
    demand_forecasting(BUCKET_NAME)
    price_optimization(BUCKET_NAME)
    
    product_affinity(BUCKET_NAME)
    product_lifecycle(BUCKET_NAME)
    
if __name__ == "__main__":
    BUCKET_NAME = "pulse-bucket-1"
    main(BUCKET_NAME)