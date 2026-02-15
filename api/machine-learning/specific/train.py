import sys
from pathlib import Path

# Add the machine-learning directory to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from specific.training.classification.infer_fulfillment_risk import main as fulfillment_risk
from specific.training.classification.infer_product_bundling import main as product_bundling

from specific.training.regression.infer_campaign_roi import main as campaign_roi
from specific.training.regression.infer_delivery_time import main as delivery_time
from specific.training.regression.infer_demand_forecast import main as demand_forecast
from specific.training.regression.infer_price_optimization import main as price_optimization

from specific.training.clustering.infer_product_affinity import main as product_affinity
from specific.training.clustering.infer_product_lifecycle import main as product_lifecycle

def main(BUCKET_NAME):
    fulfillment_risk(BUCKET_NAME)
    product_bundling(BUCKET_NAME)

    campaign_roi(BUCKET_NAME)
    delivery_time(BUCKET_NAME)
    demand_forecast(BUCKET_NAME)
    price_optimization(BUCKET_NAME)

    product_affinity(BUCKET_NAME)
    product_lifecycle(BUCKET_NAME)

if __name__ == "__main__":
    BUCKET_NAME = "pulse-bucket-1"
    main(BUCKET_NAME)