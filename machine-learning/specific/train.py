import argparse
import sys
from pathlib import Path

# Add the machine-learning directory to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from specific.training.classification.train_fulfillment_risk import main as fulfillment_risk
from specific.training.classification.train_product_bundling import main as product_bundling

from specific.training.regression.train_campaign_roi import main as campaign_roi
from specific.training.regression.train_delivery_time import main as delivery_time
from specific.training.regression.train_demand_forecast import main as demand_forecast
from specific.training.regression.train_price_optimization import main as price_optimization
from specific.training.regression.train_revenue_forecast import main as revenue_forecast
from specific.training.regression.train_seasonal_trends import main as seasonal_trends

from specific.training.clustering.train_product_affinity import main as product_affinity
from specific.training.clustering.train_product_lifecycle import main as product_lifecycle


def main(BUCKET_NAME):
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
    parser = argparse.ArgumentParser(description="Train all specific ML models for a business bucket")
    parser.add_argument("--bucket-name", type=str, required=True, help="Business MinIO bucket name")
    args = parser.parse_args()
    main(args.bucket_name)