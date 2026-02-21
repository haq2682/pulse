#!/bin/bash

# docker cp ./mapping python:/app/
# ./pydoc.sh mapping/run_mapping.py

# docker cp ./cleaning python:/app/
# ./pydoc.sh cleaning/cleaning.py

# docker cp ./transformation python:/app/
# ./pydoc.sh transformation/transformation.py

# docker cp ./analysis python:/app/
# ./pydoc.sh analysis/analysis.py

docker cp ./machine-learning python:/app/
# ./pydoc.sh machine-learning/training/classification/train_customer_churn.py
# ./pydoc.sh machine-learning/inference/classification/infer_customer_churn.py

# ./pydoc.sh machine-learning/training/classification/train_customer_segments.py
# ./pydoc.sh machine-learning/inference/classification/infer_customer_segments.py

# ./pydoc.sh machine-learning/training/classification/train_payment_success.py
# ./pydoc.sh machine-learning/inference/classification/infer_payment_success.py

# ./pydoc.sh machine-learning/training/classification/train_review_sentiment.py
# ./pydoc.sh machine-learning/inference/classification/infer_review_sentiment.py

# ./pydoc.sh machine-learning/training/classification/train_product_bundling.py
# ./pydoc.sh machine-learning/inference/classification/infer_product_bundling.py

# ./pydoc.sh machine-learning/training/classification/train_cart_abandonment.py
# ./pydoc.sh machine-learning/inference/classification/infer_cart_abandonment.py

# ./pydoc.sh machine-learning/training/classification/train_stock_status.py
# ./pydoc.sh machine-learning/inference/classification/infer_stock_status.py

# ./pydoc.sh machine-learning/training/regression/train_clv.py
# ./pydoc.sh machine-learning/inference/regression/infer_clv.py

# ./pydoc.sh machine-learning/training/regression/train_demand_forecast.py
# ./pydoc.sh machine-learning/inference/regression/infer_demand_forecast.py

# ./pydoc.sh machine-learning/training/regression/train_revenue_forecast.py
# ./pydoc.sh machine-learning/inference/regression/infer_revenue_forecast.py

# ./pydoc.sh machine-learning/specific/training/regression/train_revenue_forecast.py
# ./pydoc.sh machine-learning/specific/inference/regression/infer_revenue_forecast.py

# ./pydoc.sh machine-learning/general/training/regression/train_aov.py
# ./pydoc.sh machine-learning/general/inference/regression/infer_aov.py

# ./pydoc.sh machine-learning/general/training/regression/train_restock_quantity.py
# ./pydoc.sh machine-learning/general/inference/regression/infer_restock_quantity.py

# ./pydoc.sh machine-learning/general/training/regression/train_safety_stock.py
# ./pydoc.sh machine-learning/general/inference/regression/infer_safety_stock.py

# ./pydoc.sh machine-learning/general/training/regression/train_stockout_probability.py
# ./pydoc.sh machine-learning/general/inference/regression/infer_stockout_probability.py

# ./pydoc.sh machine-learning/general/training/regression/train_session_conversion.py
# ./pydoc.sh machine-learning/general/inference/regression/infer_session_conversion.py

# ./pydoc.sh machine-learning/specific/training/regression/train_campaign_roi.py
# ./pydoc.sh machine-learning/specific/inference/regression/infer_campaign_roi.py

# ./pydoc.sh machine-learning/specific/training/regression/train_price_optimization.py
# ./pydoc.sh machine-learning/specific/inference/regression/infer_price_optimization.py

# ./pydoc.sh machine-learning/specific/training/regression/train_delivery_time.py
# ./pydoc.sh machine-learning/specific/inference/regression/infer_delivery_time.py

# ./pydoc.sh machine-learning/general/training/clustering/train_customer_segment.py
# ./pydoc.sh machine-learning/general/inference/clustering/infer_customer_segment.py

# ./pydoc.sh machine-learning/specific/training/clustering/train_product_affinity.py
# ./pydoc.sh machine-learning/specific/inference/clustering/infer_product_affinity.py

# ./pydoc.sh machine-learning/general/training/clustering/train_geo_cluster.py
# ./pydoc.sh machine-learning/general/inference/clustering/infer_geo_cluster.py

# ./pydoc.sh machine-learning/general/training/clustering/train_supplier_performance.py
# ./pydoc.sh machine-learning/general/inference/clustering/infer_supplier_performance.py

# ./pydoc.sh machine-learning/general/training/clustering/train_session_behavior.py
# ./pydoc.sh machine-learning/general/inference/clustering/infer_session_behavior.py

# ./pydoc.sh machine-learning/specific/training/clustering/train_product_lifecycle.py
# ./pydoc.sh machine-learning/specific/inference/clustering/infer_product_lifecycle.py

# ./pydoc.sh machine-learning/specific/training/regression/train_seasonal_trends.py
# ./pydoc.sh machine-learning/specific/inference/regression/infer_seasonal_trends.py

read -p "Press Enter to exit..."
