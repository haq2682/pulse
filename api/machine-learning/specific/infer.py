import sys
from pathlib import Path

# Add the machine-learning directory to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from specific.inference.classification.infer_fulfillment_risk import main as fulfillment_risk
from specific.inference.classification.infer_product_bundling import main as product_bundling

from specific.inference.regression.infer_campaign_roi import main as campaign_roi
from specific.inference.regression.infer_delivery_time import main as delivery_time
from specific.inference.regression.infer_demand_forecast import main as demand_forecast
from specific.inference.regression.infer_price_optimization import main as price_optimization
from specific.inference.regression.infer_revenue_forecast import main as revenue_forecast
from specific.inference.regression.infer_seasonal_trends import main as seasonal_trends

from specific.inference.clustering.infer_product_affinity import main as product_affinity
from specific.inference.clustering.infer_product_lifecycle import main as product_lifecycle
from specific.train import main as train_all

def main(BUCKET_NAME):
    train_all(BUCKET_NAME)
    
    fulfillment_risk(BUCKET_NAME)
    product_bundling(BUCKET_NAME)

    campaign_roi(BUCKET_NAME)
    delivery_time(BUCKET_NAME)
    demand_forecast(BUCKET_NAME)
    price_optimization(BUCKET_NAME)
    revenue_forecast(BUCKET_NAME)
    seasonal_trends(BUCKET_NAME)

    product_affinity(BUCKET_NAME)
    product_lifecycle(BUCKET_NAME)

if __name__ == "__main__":
    BUCKET_NAME = "pulse-bucket-1"
    main(BUCKET_NAME)