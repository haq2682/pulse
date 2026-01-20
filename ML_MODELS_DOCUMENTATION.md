# Machine Learning Models Documentation

## Overview
This document outlines the machine learning models for the Pulse E-Commerce Analytics Engine, including classification, regression, clustering, and reinforcement learning models. Each model is designed to leverage the data from canonical and aggregated schemas to provide predictions, forecasts, and optimization recommendations.

---

## Table of Contents
1. [Classification Models](#classification-models)
2. [Regression Models](#regression-models)
3. [Clustering Models](#clustering-models)
4. [Reinforcement Learning Models](#reinforcement-learning-models)
5. [Output Schema Specifications](#output-schema-specifications)

---

## Classification Models

### 1. Customer Churn Prediction
**Task:** Predict which customers are likely to churn (stop purchasing)

**Input Features (from `agg_customers`):**
- `customer_id` (VARCHAR) - Primary key
- `days_since_last_purchase` (INTEGER) - Recency indicator
- `order_frequency` (BIGINT) - Purchase frequency
- `customer_lifetime_value` (DOUBLE) - Total value
- `avg_days_between_orders` (DOUBLE) - Purchase pattern
- `total_orders` (BIGINT) - Order count
- `total_revenue` (DOUBLE) - Revenue contribution
- `session_conversion_rate` (DOUBLE) - Engagement metric
- `cart_abandonment_rate` (DOUBLE) - Abandonment behavior
- `days_since_last_login` (INTEGER) - Activity recency
- `customer_tenure_days` (INTEGER) - Account age
- `recency_score` (INTEGER) - RFM component
- `frequency_score` (INTEGER) - RFM component
- `monetary_score` (INTEGER) - RFM component
- `avg_order_value` (DOUBLE) - Spending pattern
- `cancellation_rate` (DOUBLE) - Cancellation behavior

**Target Variable:**
- `churn_risk` (VARCHAR) - Labels: 'High', 'Medium', 'Low'

**Output Schema:** `ml_customer_churn_predictions`

---

### 2. Customer Segment Classification
**Task:** Classify customers into behavioral segments

**Input Features (from `agg_customers`, `agg_rfm_segmentation`):**
- `customer_id` (VARCHAR)
- `total_orders` (BIGINT)
- `total_revenue` (DOUBLE)
- `avg_order_value` (DOUBLE)
- `recency_score` (INTEGER)
- `frequency_score` (INTEGER)
- `monetary_score` (INTEGER)
- `days_since_last_order` (INTEGER)
- `session_conversion_rate` (DOUBLE)
- `cart_abandonment_rate` (DOUBLE)
- `preferred_device_type` (VARCHAR)
- `preferred_referrer_source` (VARCHAR)

**Target Variable:**
- `customer_segment_label` (VARCHAR) - Labels: 'Champions', 'Loyal', 'At Risk', 'Lost', etc.

**Output Schema:** `ml_customer_segment_predictions`

---

### 3. Payment Success Prediction
**Task:** Predict likelihood of payment success/failure

**Input Features (from `agg_payments`, `agg_orders`):**
- `payment_id` (VARCHAR)
- `order_id` (VARCHAR)
- `payment_method` (VARCHAR)
- `payment_provider` (VARCHAR)
- `total_amount` (DOUBLE) - from orders
- `customer_id` (VARCHAR)
- `country` (VARCHAR) - from customers
- `processing_fee` (DOUBLE)

**Target Variable:**
- `payment_status` (VARCHAR) - Labels: 'Completed', 'Failed', 'Pending'

**Output Schema:** `ml_payment_success_predictions`

---

### 4. Review Sentiment Classification
**Task:** Classify product review sentiment

**Input Features (from `agg_reviews`):**
- `review_id` (VARCHAR)
- `product_id` (VARCHAR)
- `customer_id` (VARCHAR)
- `rating` (INTEGER)
- `review_title` (VARCHAR)
- `review_desc` (TEXT)

**Target Variable:**
- `review_sentiment` (VARCHAR) - Labels: 'Positive', 'Neutral', 'Negative'

**Output Schema:** `ml_review_sentiment_predictions`

---

### 5. Product Category Classification
**Task:** Auto-classify products into categories based on attributes

**Input Features (from `agg_products`):**
- `product_id` (VARCHAR)
- `product_name` (VARCHAR)
- `brand` (VARCHAR)
- `material` (VARCHAR)
- `color` (VARCHAR)
- `size` (VARCHAR)
- `weight` (DOUBLE)
- `dimensions` (VARCHAR)
- `cost_price` (DOUBLE)
- `sell_price` (DOUBLE)

**Target Variable:**
- `category` (VARCHAR)
- `sub_category` (VARCHAR)

**Output Schema:** `ml_product_category_predictions`

---

### 6. Cart Abandonment Risk Classification
**Task:** Predict if a cart will be abandoned

**Input Features (from `agg_cart_abandonment_analysis`, `agg_customer_sessions`):**
- `cart_id` (VARCHAR)
- `customer_id` (VARCHAR)
- `cart_total_value` (DOUBLE)
- `cart_items_count` (BIGINT)
- `time_in_cart_hours` (INTEGER)
- `device_used` (VARCHAR)
- `cart_avg_item_price` (DOUBLE)
- `session_duration_minutes` (BIGINT)
- `pages_viewed` (INTEGER)

**Target Variable:**
- `cart_status` (VARCHAR) - Labels: 'Abandoned', 'Converted', 'Active'

**Output Schema:** `ml_cart_abandonment_predictions`

---

### 7. Stock Status Classification
**Task:** Classify inventory stock health

**Input Features (from `agg_product_inventory_health`, `agg_inventory`):**
- `product_id` (VARCHAR)
- `current_stock` (INTEGER)
- `available_stock` (INTEGER)
- `reserved_quantity` (INTEGER)
- `minimum_stock_level` (INTEGER)
- `avg_daily_sales` (DOUBLE)
- `days_of_supply` (DOUBLE)
- `inventory_turnover_ratio` (DOUBLE)
- `days_since_restock` (INTEGER)

**Target Variable:**
- `stock_status` (VARCHAR) - Labels: 'In Stock', 'Low Stock', 'Out of Stock', 'Overstock'

**Output Schema:** `ml_stock_status_predictions`

---

## Regression Models

### 1. Customer Lifetime Value (CLV) Prediction
**Task:** Predict future customer lifetime value

**Input Features (from `agg_customers`):**
- `customer_id` (VARCHAR)
- `total_orders` (BIGINT)
- `total_revenue` (DOUBLE)
- `avg_order_value` (DOUBLE)
- `customer_tenure_days` (INTEGER)
- `avg_days_between_orders` (DOUBLE)
- `order_frequency` (BIGINT)
- `total_discount_received` (DOUBLE)
- `session_conversion_rate` (DOUBLE)
- `cart_abandonment_rate` (DOUBLE)
- `recency_score` (INTEGER)
- `frequency_score` (INTEGER)
- `monetary_score` (INTEGER)

**Target Variable:**
- `predicted_clv` (DOUBLE) - Predicted lifetime value

**Output Schema:** `ml_clv_predictions`

---

### 2. Product Demand Forecasting
**Task:** Forecast future product demand (units)

**Input Features (from `agg_products`, `agg_orders`, time series data):**
- `product_id` (VARCHAR)
- `category` (VARCHAR)
- `total_units_sold` (BIGINT) - Historical
- `total_orders` (BIGINT) - Historical
- `avg_quantity_per_order` (DOUBLE)
- `sell_price` (DOUBLE)
- `season` (VARCHAR) - from orders
- `days_since_launch` (INTEGER)
- `avg_rating` (DOUBLE)
- `order_placed_month` (INTEGER) - Time component
- `order_placed_week` (INTEGER) - Time component

**Target Variable:**
- `predicted_demand_units` (DOUBLE) - Forecasted units for next period

**Output Schema:** `ml_demand_forecast_predictions`

---

### 3. Revenue Forecasting
**Task:** Forecast future revenue by time period

**Input Features (from `agg_daily_aggregations`, `agg_weekly_aggregations`, `agg_monthly_aggregations`):**
- `order_date` (DATE)
- `total_orders` (BIGINT) - Historical
- `total_revenue` (DOUBLE) - Historical
- `total_customers` (BIGINT)
- `new_customers` (BIGINT)
- `returning_customers` (BIGINT)
- `avg_order_value` (DOUBLE)
- `session_to_order_rate` (DOUBLE)
- `revenue_growth_rate` (DOUBLE)
- `order_year` (INTEGER)
- `order_month` (INTEGER)

**Target Variable:**
- `predicted_revenue` (DOUBLE) - Forecasted revenue for next period

**Output Schema:** `ml_revenue_forecast_predictions`

---

### 4. Average Order Value (AOV) Prediction
**Task:** Predict expected order value for a customer

**Input Features (from `agg_customers`, `agg_orders`):**
- `customer_id` (VARCHAR)
- `avg_order_value` (DOUBLE) - Historical
- `total_orders` (BIGINT)
- `customer_lifetime_value` (DOUBLE)
- `customer_segment_label` (VARCHAR)
- `preferred_payment_method` (VARCHAR)
- `preferred_device_type` (VARCHAR)
- `customer_tenure_days` (INTEGER)

**Target Variable:**
- `predicted_next_aov` (DOUBLE) - Predicted next order value

**Output Schema:** `ml_aov_predictions`

---

### 5. Inventory Restock Quantity Prediction
**Task:** Predict optimal restock quantity

**Input Features (from `agg_product_inventory_health`, `agg_products`):**
- `product_id` (VARCHAR)
- `current_stock` (INTEGER)
- `avg_daily_sales` (DOUBLE)
- `days_of_supply` (DOUBLE)
- `minimum_stock_level` (INTEGER)
- `inventory_turnover_ratio` (DOUBLE)
- `stockout_frequency` (BIGINT)
- `storage_cost_per_unit` (DOUBLE)
- `cost_price` (DOUBLE)
- `lead_time_days` (INTEGER) - Supplier lead time

**Target Variable:**
- `recommended_restock_quantity` (DOUBLE) - Optimal restock units

**Output Schema:** `ml_restock_quantity_predictions`

---

### 6. Campaign ROI Prediction
**Task:** Predict marketing campaign return on investment

**Input Features (from `agg_marketing_campaigns`):**
- `campaign_id` (VARCHAR)
- `campaign_type` (VARCHAR)
- `budget` (DOUBLE)
- `spent_amount` (DOUBLE)
- `impressions` (INTEGER)
- `clicks` (INTEGER)
- `conversions` (INTEGER)
- `click_through_rate` (DOUBLE)
- `conversion_rate` (DOUBLE)
- `target_audience` (VARCHAR)
- `days_active` (INTEGER)

**Target Variable:**
- `predicted_roi` (DOUBLE) - Predicted return on investment
- `predicted_revenue` (DOUBLE) - Predicted campaign revenue

**Output Schema:** `ml_campaign_roi_predictions`

---

### 7. Product Price Optimization
**Task:** Predict optimal product price for maximizing revenue

**Input Features (from `agg_products`, `agg_orders`):**
- `product_id` (VARCHAR)
- `cost_price` (DOUBLE)
- `sell_price` (DOUBLE)
- `profit_margin` (DOUBLE)
- `total_units_sold` (BIGINT)
- `avg_rating` (DOUBLE)
- `category` (VARCHAR)
- `brand` (VARCHAR)
- `price_elasticity` (DOUBLE) - Calculated metric

**Target Variable:**
- `optimal_price` (DOUBLE) - Recommended selling price

**Output Schema:** `ml_price_optimization_predictions`

---

### 8. Delivery Time Prediction
**Task:** Predict order delivery time

**Input Features (from `agg_orders`, `agg_customers`):**
- `order_id` (VARCHAR)
- `country` (VARCHAR)
- `state_province` (VARCHAR)
- `city` (VARCHAR)
- `total_quantity` (INTEGER)
- `total_amount` (DOUBLE)
- `shipping_cost` (DOUBLE)
- `order_placed_day_of_week` (INTEGER)

**Target Variable:**
- `predicted_delivery_days` (DOUBLE) - Estimated delivery time in days

**Output Schema:** `ml_delivery_time_predictions`

---

### 9. Session Conversion Value Prediction
**Task:** Predict potential conversion value from a session

**Input Features (from `agg_customer_sessions`):**
- `session_id` (VARCHAR)
- `customer_id` (VARCHAR)
- `pages_viewed` (INTEGER)
- `products_viewed` (INTEGER)
- `session_duration_minutes` (BIGINT)
- `device_type` (VARCHAR)
- `referrer_source` (VARCHAR)
- `items_added_to_cart` (BIGINT)
- `cart_value` (DOUBLE)

**Target Variable:**
- `predicted_conversion_value` (DOUBLE) - Expected order value if converted

**Output Schema:** `ml_session_conversion_predictions`

---

## Clustering Models

### 1. Customer Segmentation (RFM Clustering)
**Task:** Cluster customers based on RFM metrics

**Input Features (from `agg_customers`, `agg_rfm_segmentation`):**
- `customer_id` (VARCHAR)
- `days_since_last_order` (INTEGER) - Recency
- `total_orders` (BIGINT) - Frequency
- `total_revenue` (DOUBLE) - Monetary
- `avg_order_value` (DOUBLE)
- `customer_tenure_days` (INTEGER)
- `session_conversion_rate` (DOUBLE)

**Output:**
- `cluster_id` (INTEGER) - Cluster assignment (0, 1, 2, ...)
- `cluster_label` (VARCHAR) - Semantic label ('High Value', 'At Risk', etc.)
- `recency_score` (INTEGER)
- `frequency_score` (INTEGER)
- `monetary_score` (INTEGER)

**Output Schema:** `ml_customer_clustering`

---

### 2. Product Affinity Clustering
**Task:** Cluster products frequently purchased together

**Input Features (from `agg_product_affinity`, `agg_products`):**
- `product_id` (VARCHAR)
- `category` (VARCHAR)
- `brand` (VARCHAR)
- `price_range` (VARCHAR) - Derived from sell_price
- `co_occurrence_patterns` (ARRAY) - Products bought together
- `avg_rating` (DOUBLE)

**Output:**
- `cluster_id` (INTEGER) - Product cluster assignment
- `cluster_label` (VARCHAR) - Cluster description
- `recommended_products` (ARRAY) - Products in same cluster

**Output Schema:** `ml_product_affinity_clustering`

---

### 3. Geographic Market Segmentation
**Task:** Cluster geographic regions by market characteristics

**Input Features (from `agg_country_aggregations`, `agg_state_aggregations`, `agg_city_aggregations`):**
- `country` (VARCHAR)
- `state_province` (VARCHAR)
- `city` (VARCHAR)
- `total_customers` (BIGINT)
- `total_orders` (BIGINT)
- `total_revenue` (DOUBLE)
- `avg_order_value` (DOUBLE)
- `avg_customer_lifetime_value` (DOUBLE)
- `revenue_per_customer` (DOUBLE)

**Output:**
- `cluster_id` (INTEGER) - Geographic cluster
- `market_segment` (VARCHAR) - 'High Value', 'Growth', 'Emerging', etc.

**Output Schema:** `ml_geographic_clustering`

---

### 4. Supplier Performance Clustering
**Task:** Cluster suppliers by performance metrics

**Input Features (from `agg_suppliers`, `agg_supplier_inventory_health`):**
- `supplier_id` (VARCHAR)
- `supplier_rating` (DOUBLE)
- `total_revenue_generated` (DOUBLE)
- `avg_profit_margin` (DOUBLE)
- `stockout_rate` (DOUBLE)
- `supplier_reliability_score` (DOUBLE)
- `avg_restock_lead_time` (DOUBLE)
- `total_products_supplied` (BIGINT)

**Output:**
- `cluster_id` (INTEGER) - Supplier cluster
- `performance_tier` (VARCHAR) - 'Premium', 'Standard', 'At Risk'

**Output Schema:** `ml_supplier_clustering`

---

### 5. Session Behavior Clustering
**Task:** Cluster sessions by user behavior patterns

**Input Features (from `agg_customer_sessions`):**
- `session_id` (VARCHAR)
- `session_duration_minutes` (BIGINT)
- `pages_viewed` (INTEGER)
- `products_viewed` (INTEGER)
- `items_added_to_cart` (BIGINT)
- `conversion_flag` (INTEGER)
- `cart_abandonment_flag` (INTEGER)
- `device_type` (VARCHAR)
- `referrer_source` (VARCHAR)

**Output:**
- `cluster_id` (INTEGER) - Behavior cluster
- `behavior_type` (VARCHAR) - 'Browser', 'Researcher', 'Buyer', 'Abandoner'

**Output Schema:** `ml_session_behavior_clustering`

---

### 6. Product Lifecycle Clustering
**Task:** Cluster products by lifecycle stage

**Input Features (from `agg_products`):**
- `product_id` (VARCHAR)
- `days_since_launch` (INTEGER)
- `total_units_sold` (BIGINT)
- `total_revenue` (DOUBLE)
- `inventory_turnover_rate` (DOUBLE)
- `avg_rating` (DOUBLE)
- `total_reviews` (BIGINT)
- `stock_status` (VARCHAR)

**Output:**
- `cluster_id` (INTEGER) - Lifecycle cluster
- `lifecycle_stage` (VARCHAR) - 'Introduction', 'Growth', 'Maturity', 'Decline'

**Output Schema:** `ml_product_lifecycle_clustering`

---

## Reinforcement Learning Models

### 1. Dynamic Inventory Optimization (Ray RLLib)
**Task:** Optimize inventory levels using reinforcement learning

**Framework:** Ray RLLib (PPO, DQN, or A3C algorithms)

**State Space (from `agg_product_inventory_health`, `agg_products`):**
- `current_stock` (INTEGER) - Current inventory level
- `available_stock` (INTEGER) - Available for sale
- `reserved_quantity` (INTEGER) - Reserved items
- `avg_daily_sales` (DOUBLE) - Average sales rate
- `days_of_supply` (DOUBLE) - Current days of inventory
- `storage_cost_per_unit` (DOUBLE) - Holding cost
- `cost_price` (DOUBLE) - Unit cost
- `sell_price` (DOUBLE) - Selling price
- `lead_time_days` (INTEGER) - Supplier lead time
- `stockout_frequency` (BIGINT) - Historical stockouts
- `demand_volatility` (DOUBLE) - Demand variance
- `season` (VARCHAR) - Current season
- `day_of_week` (INTEGER) - Time context

**Action Space:**
- `reorder_quantity` (CONTINUOUS) - Units to order (0 to max_order_quantity)
- `reorder_timing` (DISCRETE) - When to reorder: 'now', 'wait_1_day', 'wait_3_days', 'wait_7_days'

**Reward Function:**
- Positive reward for sales made (revenue - cost)
- Negative reward for stockouts (lost sales penalty)
- Negative reward for holding costs (storage_cost * current_stock)
- Negative reward for excess inventory (overstock penalty)

**Output Schema:** `rl_inventory_optimization_actions`
```sql
CREATE TABLE rl_inventory_optimization_actions (
    action_id VARCHAR(50) PRIMARY KEY,
    product_id VARCHAR(255) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    current_stock INTEGER,
    recommended_reorder_quantity INTEGER,
    recommended_reorder_timing VARCHAR(50),
    expected_reward DOUBLE,
    confidence_score DOUBLE,
    state_representation JSON,
    model_version VARCHAR(50)
);
```

**Metrics Table:** `rl_inventory_metrics`
```sql
CREATE TABLE rl_inventory_metrics (
    metric_id VARCHAR(50) PRIMARY KEY,
    product_id VARCHAR(255),
    date DATE,
    total_revenue DOUBLE,
    total_holding_cost DOUBLE,
    total_stockout_penalty DOUBLE,
    total_reward DOUBLE,
    service_level DOUBLE,
    inventory_turnover DOUBLE,
    model_version VARCHAR(50)
);
```

---

### 2. Dynamic Pricing Optimization (Ray RLLib)
**Task:** Optimize product pricing dynamically

**Framework:** Ray RLLib (DDPG or TD3 for continuous actions)

**State Space (from `agg_products`, `agg_orders`, market data):**
- `current_price` (DOUBLE) - Current selling price
- `cost_price` (DOUBLE) - Unit cost
- `competitor_avg_price` (DOUBLE) - Market price
- `current_stock` (INTEGER) - Inventory level
- `recent_sales_velocity` (DOUBLE) - Sales rate
- `avg_rating` (DOUBLE) - Product rating
- `demand_elasticity` (DOUBLE) - Price sensitivity
- `season` (VARCHAR) - Seasonal context
- `day_of_week` (INTEGER) - Day context
- `campaign_active` (BOOLEAN) - Marketing campaign status

**Action Space:**
- `price_multiplier` (CONTINUOUS) - Price adjustment (0.7 to 1.5 of base price)

**Reward Function:**
- Maximize: (price - cost) * units_sold
- Penalty for prices below cost
- Penalty for excessive inventory
- Bonus for maintaining target margin

**Output Schema:** `rl_pricing_optimization_actions`
```sql
CREATE TABLE rl_pricing_optimization_actions (
    action_id VARCHAR(50) PRIMARY KEY,
    product_id VARCHAR(255) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    current_price DOUBLE,
    recommended_price DOUBLE,
    expected_revenue DOUBLE,
    expected_units_sold DOUBLE,
    expected_reward DOUBLE,
    confidence_score DOUBLE,
    state_representation JSON,
    model_version VARCHAR(50)
);
```

---

### 3. Marketing Budget Allocation (Ray RLLib)
**Task:** Optimize marketing budget allocation across campaigns

**Framework:** Ray RLLib (PPO algorithm)

**State Space (from `agg_marketing_campaigns`):**
- `total_budget` (DOUBLE) - Total marketing budget
- `remaining_budget` (DOUBLE) - Unallocated budget
- `campaign_type_performance` (ARRAY) - ROI by campaign type
- `target_audience_size` (ARRAY) - Audience sizes
- `historical_ctr` (ARRAY) - Click-through rates
- `historical_conversion_rate` (ARRAY) - Conversion rates
- `days_remaining_in_period` (INTEGER) - Time constraint
- `current_revenue` (DOUBLE) - Revenue to date

**Action Space:**
- `budget_allocation` (CONTINUOUS ARRAY) - Budget % for each campaign type

**Reward Function:**
- Maximize total ROI across all campaigns
- Penalty for budget overspend
- Bonus for balanced diversification

**Output Schema:** `rl_marketing_budget_actions`
```sql
CREATE TABLE rl_marketing_budget_actions (
    action_id VARCHAR(50) PRIMARY KEY,
    period VARCHAR(50),
    timestamp TIMESTAMP NOT NULL,
    total_budget DOUBLE,
    allocation_email DOUBLE,
    allocation_social DOUBLE,
    allocation_search DOUBLE,
    allocation_display DOUBLE,
    expected_total_roi DOUBLE,
    expected_revenue DOUBLE,
    confidence_score DOUBLE,
    state_representation JSON,
    model_version VARCHAR(50)
);
```

---

### 4. Cart Recovery Optimization (Ray RLLib)
**Task:** Optimize cart recovery actions and timing

**Framework:** Ray RLLib (DQN algorithm)

**State Space (from `agg_cart_abandonment_analysis`, `agg_customers`):**
- `cart_total_value` (DOUBLE) - Cart value
- `cart_items_count` (BIGINT) - Number of items
- `time_since_abandonment_hours` (INTEGER) - Time elapsed
- `customer_clv` (DOUBLE) - Customer lifetime value
- `customer_segment` (VARCHAR) - Customer segment
- `abandonment_reason` (VARCHAR) - Reason category
- `device_used` (VARCHAR) - Device type
- `previous_recovery_attempts` (INTEGER) - Prior attempts

**Action Space (DISCRETE):**
- `action_type`:
  - 0: No action
  - 1: Send email with discount (5%)
  - 2: Send email with discount (10%)
  - 3: Send email with free shipping
  - 4: Send SMS reminder
  - 5: Send push notification

**Reward Function:**
- Positive reward for successful cart recovery (cart value - discount cost)
- Negative reward for sending ineffective messages (communication cost)
- Penalty for over-communication (customer annoyance)

**Output Schema:** `rl_cart_recovery_actions`
```sql
CREATE TABLE rl_cart_recovery_actions (
    action_id VARCHAR(50) PRIMARY KEY,
    cart_id VARCHAR(255) NOT NULL,
    customer_id VARCHAR(255) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    recommended_action_type INTEGER,
    action_description VARCHAR(255),
    expected_recovery_probability DOUBLE,
    expected_revenue DOUBLE,
    expected_cost DOUBLE,
    confidence_score DOUBLE,
    state_representation JSON,
    model_version VARCHAR(50)
);
```

**Recovery Results Table:** `rl_cart_recovery_results`
```sql
CREATE TABLE rl_cart_recovery_results (
    result_id VARCHAR(50) PRIMARY KEY,
    action_id VARCHAR(50),
    cart_id VARCHAR(255),
    action_taken BOOLEAN,
    cart_recovered BOOLEAN,
    recovery_time_hours INTEGER,
    recovered_value DOUBLE,
    discount_given DOUBLE,
    actual_reward DOUBLE,
    FOREIGN KEY (action_id) REFERENCES rl_cart_recovery_actions(action_id)
);
```

---

### 5. Supplier Selection and Ordering (Ray RLLib)
**Task:** Optimize supplier selection for product restocking

**Framework:** Ray RLLib (A3C algorithm)

**State Space (from `agg_suppliers`, `agg_product_inventory_health`):**
- `current_stock_level` (INTEGER) - Current inventory
- `predicted_demand` (DOUBLE) - Forecasted demand
- `supplier_ratings` (ARRAY) - Ratings per supplier
- `supplier_lead_times` (ARRAY) - Lead times per supplier
- `supplier_costs` (ARRAY) - Unit costs per supplier
- `supplier_reliability` (ARRAY) - Reliability scores
- `supplier_stockout_history` (ARRAY) - Stockout rates
- `urgency_level` (VARCHAR) - Reorder urgency

**Action Space:**
- `selected_supplier_id` (DISCRETE) - Which supplier to use
- `order_quantity` (CONTINUOUS) - How much to order

**Reward Function:**
- Minimize total cost (unit cost + shipping + holding)
- Penalty for stockouts due to late delivery
- Bonus for reliable suppliers
- Penalty for quality issues

**Output Schema:** `rl_supplier_selection_actions`
```sql
CREATE TABLE rl_supplier_selection_actions (
    action_id VARCHAR(50) PRIMARY KEY,
    product_id VARCHAR(255) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    recommended_supplier_id VARCHAR(255),
    recommended_order_quantity INTEGER,
    expected_total_cost DOUBLE,
    expected_delivery_date DATE,
    expected_reliability_score DOUBLE,
    alternative_suppliers JSON,
    confidence_score DOUBLE,
    state_representation JSON,
    model_version VARCHAR(50)
);
```

---

## Output Schema Specifications

### Classification Model Outputs

#### `ml_customer_churn_predictions`
```sql
CREATE TABLE ml_customer_churn_predictions (
    prediction_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(255) NOT NULL,
    prediction_date TIMESTAMP NOT NULL,
    predicted_churn_risk VARCHAR(50),  -- 'High', 'Medium', 'Low'
    churn_probability DOUBLE,
    confidence_score DOUBLE,
    contributing_factors JSON,  -- Top features influencing prediction
    model_version VARCHAR(50),
    FOREIGN KEY (customer_id) REFERENCES agg_customers(customer_id)
);
```

#### `ml_customer_segment_predictions`
```sql
CREATE TABLE ml_customer_segment_predictions (
    prediction_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(255) NOT NULL,
    prediction_date TIMESTAMP NOT NULL,
    predicted_segment VARCHAR(100),  -- 'Champions', 'Loyal', etc.
    segment_probability DOUBLE,
    rfm_score DOUBLE,
    confidence_score DOUBLE,
    model_version VARCHAR(50),
    FOREIGN KEY (customer_id) REFERENCES agg_customers(customer_id)
);
```

#### `ml_payment_success_predictions`
```sql
CREATE TABLE ml_payment_success_predictions (
    prediction_id VARCHAR(50) PRIMARY KEY,
    payment_id VARCHAR(255) NOT NULL,
    order_id VARCHAR(255),
    prediction_date TIMESTAMP NOT NULL,
    predicted_status VARCHAR(50),  -- 'Success', 'Failure'
    success_probability DOUBLE,
    risk_factors JSON,
    confidence_score DOUBLE,
    model_version VARCHAR(50),
    FOREIGN KEY (payment_id) REFERENCES agg_payments(payment_id)
);
```

#### `ml_review_sentiment_predictions`
```sql
CREATE TABLE ml_review_sentiment_predictions (
    prediction_id VARCHAR(50) PRIMARY KEY,
    review_id VARCHAR(255) NOT NULL,
    product_id VARCHAR(255),
    prediction_date TIMESTAMP NOT NULL,
    predicted_sentiment VARCHAR(50),  -- 'Positive', 'Neutral', 'Negative'
    sentiment_score DOUBLE,  -- -1 to 1
    confidence_score DOUBLE,
    key_phrases JSON,
    model_version VARCHAR(50),
    FOREIGN KEY (review_id) REFERENCES agg_reviews(review_id)
);
```

#### `ml_product_category_predictions`
```sql
CREATE TABLE ml_product_category_predictions (
    prediction_id VARCHAR(50) PRIMARY KEY,
    product_id VARCHAR(255) NOT NULL,
    prediction_date TIMESTAMP NOT NULL,
    predicted_category VARCHAR(255),
    predicted_sub_category VARCHAR(255),
    category_probability DOUBLE,
    confidence_score DOUBLE,
    model_version VARCHAR(50),
    FOREIGN KEY (product_id) REFERENCES agg_products(product_id)
);
```

#### `ml_cart_abandonment_predictions`
```sql
CREATE TABLE ml_cart_abandonment_predictions (
    prediction_id VARCHAR(50) PRIMARY KEY,
    cart_id VARCHAR(255) NOT NULL,
    customer_id VARCHAR(255),
    prediction_date TIMESTAMP NOT NULL,
    predicted_status VARCHAR(50),  -- 'Will Abandon', 'Will Convert'
    abandonment_probability DOUBLE,
    abandonment_risk_score DOUBLE,
    key_risk_factors JSON,
    recommended_actions JSON,
    confidence_score DOUBLE,
    model_version VARCHAR(50),
    FOREIGN KEY (cart_id) REFERENCES agg_shopping_cart(cart_id)
);
```

#### `ml_stock_status_predictions`
```sql
CREATE TABLE ml_stock_status_predictions (
    prediction_id VARCHAR(50) PRIMARY KEY,
    product_id VARCHAR(255) NOT NULL,
    prediction_date TIMESTAMP NOT NULL,
    predicted_status VARCHAR(50),  -- 'In Stock', 'Low Stock', 'Out of Stock', 'Overstock'
    days_until_stockout DOUBLE,
    reorder_recommendation BOOLEAN,
    confidence_score DOUBLE,
    model_version VARCHAR(50),
    FOREIGN KEY (product_id) REFERENCES agg_products(product_id)
);
```

---

### Regression Model Outputs

#### `ml_clv_predictions`
```sql
CREATE TABLE ml_clv_predictions (
    prediction_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(255) NOT NULL,
    prediction_date TIMESTAMP NOT NULL,
    predicted_clv DOUBLE,
    prediction_horizon_days INTEGER,  -- e.g., 365 for 1-year prediction
    confidence_interval_lower DOUBLE,
    confidence_interval_upper DOUBLE,
    confidence_score DOUBLE,
    model_version VARCHAR(50),
    FOREIGN KEY (customer_id) REFERENCES agg_customers(customer_id)
);
```

#### `ml_demand_forecast_predictions`
```sql
CREATE TABLE ml_demand_forecast_predictions (
    prediction_id VARCHAR(50) PRIMARY KEY,
    product_id VARCHAR(255) NOT NULL,
    forecast_date DATE NOT NULL,
    prediction_date TIMESTAMP NOT NULL,
    predicted_demand_units DOUBLE,
    confidence_interval_lower DOUBLE,
    confidence_interval_upper DOUBLE,
    forecast_horizon_days INTEGER,
    seasonality_factor DOUBLE,
    trend_factor DOUBLE,
    confidence_score DOUBLE,
    model_version VARCHAR(50),
    FOREIGN KEY (product_id) REFERENCES agg_products(product_id)
);
```

#### `ml_revenue_forecast_predictions`
```sql
CREATE TABLE ml_revenue_forecast_predictions (
    prediction_id VARCHAR(50) PRIMARY KEY,
    forecast_date DATE NOT NULL,
    prediction_date TIMESTAMP NOT NULL,
    predicted_revenue DOUBLE,
    predicted_orders INTEGER,
    confidence_interval_lower DOUBLE,
    confidence_interval_upper DOUBLE,
    forecast_horizon_days INTEGER,
    seasonality_factor DOUBLE,
    trend_factor DOUBLE,
    confidence_score DOUBLE,
    model_version VARCHAR(50)
);
```

#### `ml_aov_predictions`
```sql
CREATE TABLE ml_aov_predictions (
    prediction_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(255) NOT NULL,
    prediction_date TIMESTAMP NOT NULL,
    predicted_next_aov DOUBLE,
    confidence_interval_lower DOUBLE,
    confidence_interval_upper DOUBLE,
    factors_influencing_aov JSON,
    confidence_score DOUBLE,
    model_version VARCHAR(50),
    FOREIGN KEY (customer_id) REFERENCES agg_customers(customer_id)
);
```

#### `ml_restock_quantity_predictions`
```sql
CREATE TABLE ml_restock_quantity_predictions (
    prediction_id VARCHAR(50) PRIMARY KEY,
    product_id VARCHAR(255) NOT NULL,
    prediction_date TIMESTAMP NOT NULL,
    recommended_restock_quantity INTEGER,
    expected_demand_next_30_days DOUBLE,
    optimal_order_point INTEGER,
    safety_stock_level INTEGER,
    estimated_cost DOUBLE,
    confidence_score DOUBLE,
    model_version VARCHAR(50),
    FOREIGN KEY (product_id) REFERENCES agg_products(product_id)
);
```

#### `ml_campaign_roi_predictions`
```sql
CREATE TABLE ml_campaign_roi_predictions (
    prediction_id VARCHAR(50) PRIMARY KEY,
    campaign_id VARCHAR(255) NOT NULL,
    prediction_date TIMESTAMP NOT NULL,
    predicted_roi DOUBLE,
    predicted_revenue DOUBLE,
    predicted_conversions INTEGER,
    predicted_ctr DOUBLE,
    confidence_interval_lower DOUBLE,
    confidence_interval_upper DOUBLE,
    optimization_recommendations JSON,
    confidence_score DOUBLE,
    model_version VARCHAR(50),
    FOREIGN KEY (campaign_id) REFERENCES agg_marketing_campaigns(campaign_id)
);
```

#### `ml_price_optimization_predictions`
```sql
CREATE TABLE ml_price_optimization_predictions (
    prediction_id VARCHAR(50) PRIMARY KEY,
    product_id VARCHAR(255) NOT NULL,
    prediction_date TIMESTAMP NOT NULL,
    current_price DOUBLE,
    optimal_price DOUBLE,
    expected_revenue_at_optimal DOUBLE,
    expected_units_at_optimal INTEGER,
    price_elasticity DOUBLE,
    competitor_price_range JSON,
    confidence_score DOUBLE,
    model_version VARCHAR(50),
    FOREIGN KEY (product_id) REFERENCES agg_products(product_id)
);
```

#### `ml_delivery_time_predictions`
```sql
CREATE TABLE ml_delivery_time_predictions (
    prediction_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(255) NOT NULL,
    prediction_date TIMESTAMP NOT NULL,
    predicted_delivery_days DOUBLE,
    predicted_delivery_date DATE,
    confidence_interval_lower DOUBLE,
    confidence_interval_upper DOUBLE,
    factors_affecting_delivery JSON,
    confidence_score DOUBLE,
    model_version VARCHAR(50),
    FOREIGN KEY (order_id) REFERENCES agg_orders(order_id)
);
```

#### `ml_session_conversion_predictions`
```sql
CREATE TABLE ml_session_conversion_predictions (
    prediction_id VARCHAR(50) PRIMARY KEY,
    session_id VARCHAR(255) NOT NULL,
    customer_id VARCHAR(255),
    prediction_date TIMESTAMP NOT NULL,
    predicted_conversion_value DOUBLE,
    conversion_probability DOUBLE,
    recommended_interventions JSON,
    confidence_score DOUBLE,
    model_version VARCHAR(50),
    FOREIGN KEY (session_id) REFERENCES agg_customer_sessions(session_id)
);
```

---

### Clustering Model Outputs

#### `ml_customer_clustering`
```sql
CREATE TABLE ml_customer_clustering (
    clustering_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(255) NOT NULL,
    cluster_date TIMESTAMP NOT NULL,
    cluster_id INTEGER,
    cluster_label VARCHAR(100),  -- 'High Value', 'At Risk', etc.
    cluster_centroid_distance DOUBLE,
    recency_score INTEGER,
    frequency_score INTEGER,
    monetary_score INTEGER,
    cluster_characteristics JSON,
    model_version VARCHAR(50),
    FOREIGN KEY (customer_id) REFERENCES agg_customers(customer_id)
);
```

#### `ml_product_affinity_clustering`
```sql
CREATE TABLE ml_product_affinity_clustering (
    clustering_id VARCHAR(50) PRIMARY KEY,
    product_id VARCHAR(255) NOT NULL,
    cluster_date TIMESTAMP NOT NULL,
    cluster_id INTEGER,
    cluster_label VARCHAR(100),
    cluster_centroid_distance DOUBLE,
    recommended_products JSON,  -- Array of product_ids in same cluster
    cross_sell_opportunities JSON,
    model_version VARCHAR(50),
    FOREIGN KEY (product_id) REFERENCES agg_products(product_id)
);
```

#### `ml_geographic_clustering`
```sql
CREATE TABLE ml_geographic_clustering (
    clustering_id VARCHAR(50) PRIMARY KEY,
    country VARCHAR(255),
    state_province VARCHAR(255),
    city VARCHAR(255),
    cluster_date TIMESTAMP NOT NULL,
    cluster_id INTEGER,
    market_segment VARCHAR(100),  -- 'High Value', 'Growth', etc.
    cluster_centroid_distance DOUBLE,
    segment_characteristics JSON,
    expansion_opportunity_score DOUBLE,
    model_version VARCHAR(50)
);
```

#### `ml_supplier_clustering`
```sql
CREATE TABLE ml_supplier_clustering (
    clustering_id VARCHAR(50) PRIMARY KEY,
    supplier_id VARCHAR(255) NOT NULL,
    cluster_date TIMESTAMP NOT NULL,
    cluster_id INTEGER,
    performance_tier VARCHAR(100),  -- 'Premium', 'Standard', 'At Risk'
    cluster_centroid_distance DOUBLE,
    performance_metrics JSON,
    improvement_recommendations JSON,
    model_version VARCHAR(50),
    FOREIGN KEY (supplier_id) REFERENCES agg_suppliers(supplier_id)
);
```

#### `ml_session_behavior_clustering`
```sql
CREATE TABLE ml_session_behavior_clustering (
    clustering_id VARCHAR(50) PRIMARY KEY,
    session_id VARCHAR(255) NOT NULL,
    cluster_date TIMESTAMP NOT NULL,
    cluster_id INTEGER,
    behavior_type VARCHAR(100),  -- 'Browser', 'Researcher', 'Buyer', etc.
    cluster_centroid_distance DOUBLE,
    behavior_characteristics JSON,
    engagement_recommendations JSON,
    model_version VARCHAR(50),
    FOREIGN KEY (session_id) REFERENCES agg_customer_sessions(session_id)
);
```

#### `ml_product_lifecycle_clustering`
```sql
CREATE TABLE ml_product_lifecycle_clustering (
    clustering_id VARCHAR(50) PRIMARY KEY,
    product_id VARCHAR(255) NOT NULL,
    cluster_date TIMESTAMP NOT NULL,
    cluster_id INTEGER,
    lifecycle_stage VARCHAR(100),  -- 'Introduction', 'Growth', 'Maturity', 'Decline'
    cluster_centroid_distance DOUBLE,
    stage_characteristics JSON,
    strategic_recommendations JSON,
    model_version VARCHAR(50),
    FOREIGN KEY (product_id) REFERENCES agg_products(product_id)
);
```

---

### Model Performance Tracking

#### `ml_model_performance_metrics`
```sql
CREATE TABLE ml_model_performance_metrics (
    metric_id VARCHAR(50) PRIMARY KEY,
    model_name VARCHAR(100) NOT NULL,
    model_version VARCHAR(50) NOT NULL,
    model_type VARCHAR(50),  -- 'classification', 'regression', 'clustering', 'reinforcement_learning'
    evaluation_date TIMESTAMP NOT NULL,

    -- Classification Metrics
    accuracy DOUBLE,
    precision DOUBLE,
    recall DOUBLE,
    f1_score DOUBLE,
    auc_roc DOUBLE,

    -- Regression Metrics
    mse DOUBLE,
    rmse DOUBLE,
    mae DOUBLE,
    r_squared DOUBLE,
    mape DOUBLE,

    -- Clustering Metrics
    silhouette_score DOUBLE,
    davies_bouldin_score DOUBLE,
    calinski_harabasz_score DOUBLE,

    -- RL Metrics
    avg_episode_reward DOUBLE,
    total_episodes INTEGER,
    convergence_rate DOUBLE,

    -- General
    training_samples INTEGER,
    validation_samples INTEGER,
    training_time_seconds DOUBLE,
    hyperparameters JSON,
    feature_importance JSON
);
```

#### `ml_prediction_logs`
```sql
CREATE TABLE ml_prediction_logs (
    log_id VARCHAR(50) PRIMARY KEY,
    model_name VARCHAR(100) NOT NULL,
    model_version VARCHAR(50) NOT NULL,
    prediction_id VARCHAR(50),
    entity_id VARCHAR(255),  -- customer_id, product_id, order_id, etc.
    entity_type VARCHAR(50),  -- 'customer', 'product', 'order', etc.
    prediction_timestamp TIMESTAMP NOT NULL,
    prediction_value VARCHAR(255),  -- The actual prediction
    confidence_score DOUBLE,
    input_features JSON,
    processing_time_ms INTEGER,

    -- For validation
    actual_value VARCHAR(255),  -- Actual outcome (filled later)
    prediction_error DOUBLE,
    is_accurate BOOLEAN
);
```

---

## Model Training and Deployment Pipeline

### Recommended Workflow

1. **Data Extraction**: Query aggregated tables from PostgreSQL
2. **Feature Engineering**: Create additional features from raw aggregated data
3. **Train/Test Split**: Use time-based split for time-series data
4. **Model Training**:
   - Classification: XGBoost, Random Forest, Neural Networks
   - Regression: LightGBM, CatBoost, LSTM for time-series
   - Clustering: K-Means, DBSCAN, Hierarchical
   - RL: Ray RLLib with PPO/DQN/A3C algorithms
5. **Model Validation**: Cross-validation and hold-out test sets
6. **Model Deployment**: Save to MinIO and track version in database
7. **Prediction Storage**: Write predictions to respective output tables
8. **Monitoring**: Track model performance over time

### Key Technologies
- **Spark ML**: For distributed model training
- **Ray RLLib**: For reinforcement learning models
- **MinIO**: For model artifact storage
- **PostgreSQL**: For prediction storage and retrieval
- **Kafka**: For real-time prediction streaming (optional)

---

## Summary

This document outlines **40+ machine learning models** across four categories:

- **7 Classification Models**: Churn, segmentation, payment success, sentiment, category classification, cart abandonment, stock status
- **9 Regression Models**: CLV, demand forecasting, revenue forecasting, AOV, restock quantity, campaign ROI, price optimization, delivery time, session conversion
- **6 Clustering Models**: Customer segmentation, product affinity, geographic markets, supplier performance, session behavior, product lifecycle
- **5 Reinforcement Learning Models**: Inventory optimization, dynamic pricing, marketing budget allocation, cart recovery, supplier selection

Each model includes:
- Clearly defined input features from aggregated schemas
- Target variables or objectives
- Output schema for storing predictions
- Use case and business value

All models are designed to integrate seamlessly with the existing Pulse data pipeline and leverage the rich aggregated data available in the `agg_*` tables.
