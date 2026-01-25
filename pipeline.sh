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

./pydoc.sh machine-learning/training/regression/train_aov.py
./pydoc.sh machine-learning/inference/regression/infer_aov.py

read -p "Press Enter to exit..."
