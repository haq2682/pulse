# Enhanced Machine Learning Problems for E-commerce Optimization

## Table of Contents
1. [Regression Problems](#regression-problems)
2. [Classification Problems](#classification-problems)
3. [Multi-Output & Hybrid Problems](#multi-output--hybrid-problems)

---

## REGRESSION PROBLEMS

### 1. Intelligent Inventory Reorder Quantity Prediction ✅

**Objective:** Predict optimal reorder quantity to minimize holding costs while preventing stockouts

#### Input Tables
- `agg_product_inventory_health`
- `agg_products`
- `agg_suppliers`
- `agg_orders`
- `agg_daily_aggregations`

#### Input Features (28 features)

**Inventory Metrics:**
- `current_stock` (INTEGER)
- `available_stock` (INTEGER)
- `reserved_quantity` (INTEGER)
- `minimum_stock_level` (INTEGER)
- `reorder_point_breach_count` (BIGINT)
- `stockout_frequency` (BIGINT)
- `days_of_supply` (DOUBLE)
- `inventory_turnover_ratio` (DOUBLE)
- `days_since_restock` (INTEGER)

**Product Performance:**
- `avg_daily_sales` (DOUBLE)
- `total_units_sold` (BIGINT)
- `total_revenue` (DOUBLE)
- `profit_margin` (DOUBLE)
- `avg_rating` (DOUBLE)
- `product_performance_score` (DOUBLE)

**Financial Metrics:**
- `cost_price` (DOUBLE)
- `sell_price` (DOUBLE)
- `storage_cost_per_unit` (DOUBLE)
- `total_storage_cost` (DOUBLE)

**Supplier Metrics:**
- `supplier_reliability_score` (DOUBLE)
- `avg_restock_lead_time` (DOUBLE)
- `supplier_stockout_rate` (DOUBLE)

**Temporal & Contextual:**
- `season` (VARCHAR) → one-hot encoded
- `order_placed_quarter` (INTEGER)
- `order_placed_day_of_week` (INTEGER)
- `is_holiday_period` (BOOLEAN)
- `stock_status` (VARCHAR) → encoded

#### Output Schema: `ml_inventory_reorder_predictions`

```sql
CREATE TABLE ml_inventory_reorder_predictions (
    prediction_id VARCHAR(50) PRIMARY KEY,
    product_id VARCHAR(255) NOT NULL,
    supplier_id VARCHAR(255),
    prediction_timestamp TIMESTAMP NOT NULL,
    
    -- Current State
    current_stock_level INTEGER,
    days_of_supply_current DOUBLE,
    
    -- Predictions
    predicted_reorder_quantity INTEGER NOT NULL,
    predicted_stockout_date DATE,
    predicted_optimal_stock_level INTEGER,
    
    -- Financial Impact
    expected_holding_cost DOUBLE,
    expected_stockout_cost DOUBLE,
    expected_total_cost DOUBLE,
    expected_revenue_impact DOUBLE,
    
    -- Confidence & Model Info
    prediction_confidence DOUBLE,
    prediction_uncertainty_range JSON, -- {lower_bound, upper_bound}
    feature_importance JSON,
    model_version VARCHAR(50),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    execution_batch_id VARCHAR(50)
);

CREATE INDEX idx_reorder_pred_product ON ml_inventory_reorder_predictions(product_id);
CREATE INDEX idx_reorder_pred_timestamp ON ml_inventory_reorder_predictions(prediction_timestamp);
```

#### Performance Metrics Table: `ml_inventory_reorder_metrics`

```sql
CREATE TABLE ml_inventory_reorder_metrics (
    metric_id VARCHAR(50) PRIMARY KEY,
    evaluation_date DATE NOT NULL,
    
    -- Model Performance
    mae DOUBLE, -- Mean Absolute Error
    rmse DOUBLE, -- Root Mean Squared Error
    mape DOUBLE, -- Mean Absolute Percentage Error
    r_squared DOUBLE,
    
    -- Business Metrics
    total_products_evaluated INTEGER,
    avg_cost_reduction_pct DOUBLE,
    stockout_prevention_rate DOUBLE,
    inventory_turnover_improvement DOUBLE,
    
    -- Financial Impact
    total_cost_savings DOUBLE,
    avg_holding_cost_reduction DOUBLE,
    avg_stockout_loss_prevention DOUBLE,
    
    model_version VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

### 2. Dynamic Pricing Optimization Engine ✅

**Objective:** Predict optimal product price to maximize revenue and profit

#### Input Tables
- `agg_products`
- `agg_orders`
- `agg_order_items`
- `agg_customer_sessions`
- `agg_marketing_campaigns`
- `agg_categories`

#### Input Features (32 features)

**Current Pricing:**
- `current_sell_price` (DOUBLE)
- `cost_price` (DOUBLE)
- `current_profit_margin` (DOUBLE)
- `avg_discount_amount` (DOUBLE)

**Market Position:**
- `competitor_avg_price` (DOUBLE) -- external data
- `competitor_min_price` (DOUBLE)
- `competitor_max_price` (DOUBLE)
- `price_position_index` (DOUBLE) -- current_price / competitor_avg

**Demand Indicators:**
- `total_units_sold_7d` (BIGINT)
- `total_units_sold_30d` (BIGINT)
- `sales_velocity_trend` (DOUBLE) -- velocity change rate
- `demand_elasticity_estimate` (DOUBLE)
- `view_to_purchase_rate` (DOUBLE)
- `cart_to_purchase_rate` (DOUBLE)

**Inventory Pressure:**
- `current_stock_level` (INTEGER)
- `days_of_supply` (DOUBLE)
- `stock_status` (VARCHAR) → encoded
- `stockout_risk_score` (DOUBLE)

**Product Performance:**
- `avg_rating` (DOUBLE)
- `total_reviews` (BIGINT)
- `positive_review_rate` (DOUBLE)
- `product_performance_score` (DOUBLE)

**Category Context:**
- `category_avg_price` (DOUBLE)
- `category_total_revenue` (DOUBLE)
- `product_category_revenue_share` (DOUBLE)

**Campaign & Promotion:**
- `active_campaign_flag` (BOOLEAN)
- `campaign_discount_rate` (DOUBLE)

**Temporal Features:**
- `season` (VARCHAR) → one-hot
- `order_placed_day_of_week` (INTEGER)
- `is_weekend` (BOOLEAN)
- `is_holiday_period` (BOOLEAN)
- `days_since_launch` (INTEGER)

#### Output Schema: `ml_pricing_optimization_predictions`

```sql
CREATE TABLE ml_pricing_optimization_predictions (
    prediction_id VARCHAR(50) PRIMARY KEY,
    product_id VARCHAR(255) NOT NULL,
    prediction_timestamp TIMESTAMP NOT NULL,
    
    -- Current State
    current_price DOUBLE,
    current_daily_units DOUBLE,
    current_daily_revenue DOUBLE,
    
    -- Price Recommendations
    predicted_optimal_price DOUBLE NOT NULL,
    predicted_price_lower_bound DOUBLE,
    predicted_price_upper_bound DOUBLE,
    price_change_pct DOUBLE,
    
    -- Expected Outcomes
    expected_units_sold_daily DOUBLE,
    expected_revenue_daily DOUBLE,
    expected_profit_daily DOUBLE,
    expected_revenue_lift_pct DOUBLE,
    expected_profit_lift_pct DOUBLE,
    
    -- Risk Assessment
    demand_sensitivity_score DOUBLE,
    competitor_response_risk DOUBLE,
    brand_perception_risk DOUBLE,
    
    -- Confidence & Model Info
    prediction_confidence DOUBLE,
    feature_importance JSON,
    price_elasticity_at_point DOUBLE,
    
    -- Metadata
    model_version VARCHAR(50),
    pricing_strategy VARCHAR(100), -- maximize_revenue, maximize_profit, maximize_share
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    execution_batch_id VARCHAR(50)
);

CREATE INDEX idx_pricing_pred_product ON ml_pricing_optimization_predictions(product_id);
CREATE INDEX idx_pricing_pred_timestamp ON ml_pricing_optimization_predictions(prediction_timestamp);
```

---

### 3. Marketing Budget Allocation Optimizer

**Objective:** Predict optimal budget distribution across marketing channels

#### Input Tables
- `agg_marketing_campaigns`
- `agg_orders`
- `agg_customer_sessions`
- `agg_daily_aggregations`

#### Input Features (28 features)

**Budget Constraints:**
- `total_budget_available` (DOUBLE)
- `budget_spent_to_date` (DOUBLE)
- `remaining_budget` (DOUBLE)
- `days_remaining_in_period` (INTEGER)

**Channel Performance History:**
- `email_historical_roi` (DOUBLE)
- `email_historical_ctr` (DOUBLE)
- `email_historical_conversion_rate` (DOUBLE)
- `social_historical_roi` (DOUBLE)
- `social_historical_ctr` (DOUBLE)
- `social_historical_conversion_rate` (DOUBLE)
- `search_historical_roi` (DOUBLE)
- `search_historical_ctr` (DOUBLE)
- `search_historical_conversion_rate` (DOUBLE)
- `display_historical_roi` (DOUBLE)
- `display_historical_ctr` (DOUBLE)
- `display_historical_conversion_rate` (DOUBLE)

**Audience Metrics:**
- `email_audience_size` (INTEGER)
- `social_audience_size` (INTEGER)
- `search_audience_size` (INTEGER)
- `display_audience_size` (INTEGER)

**Business Context:**
- `current_revenue_to_date` (DOUBLE)
- `revenue_target` (DOUBLE)
- `revenue_gap` (DOUBLE)
- `current_customer_acquisition_cost` (DOUBLE)
- `target_customer_acquisition_cost` (DOUBLE)

**Temporal:**
- `season` (VARCHAR) → one-hot
- `is_promotional_period` (BOOLEAN)
- `days_to_major_event` (INTEGER)

#### Output Schema: `ml_marketing_budget_predictions`

```sql
CREATE TABLE ml_marketing_budget_predictions (
    prediction_id VARCHAR(50) PRIMARY KEY,
    planning_period VARCHAR(50) NOT NULL, -- e.g., "2024-Q1", "2024-01"
    prediction_timestamp TIMESTAMP NOT NULL,
    
    -- Budget Input
    total_budget DOUBLE,
    
    -- Predicted Allocations (Multi-output)
    predicted_email_budget DOUBLE NOT NULL,
    predicted_social_budget DOUBLE NOT NULL,
    predicted_search_budget DOUBLE NOT NULL,
    predicted_display_budget DOUBLE NOT NULL,
    predicted_other_budget DOUBLE,
    
    -- Allocation Percentages
    email_allocation_pct DOUBLE,
    social_allocation_pct DOUBLE,
    search_allocation_pct DOUBLE,
    display_allocation_pct DOUBLE,
    
    -- Expected Channel Outcomes
    email_expected_conversions INTEGER,
    email_expected_revenue DOUBLE,
    social_expected_conversions INTEGER,
    social_expected_revenue DOUBLE,
    search_expected_conversions INTEGER,
    search_expected_revenue DOUBLE,
    display_expected_conversions INTEGER,
    display_expected_revenue DOUBLE,
    
    -- Aggregate Expectations
    total_expected_conversions INTEGER,
    total_expected_revenue DOUBLE,
    expected_blended_roi DOUBLE,
    expected_blended_cac DOUBLE,
    
    -- Confidence & Model Info
    prediction_confidence DOUBLE,
    allocation_risk_score DOUBLE,
    sensitivity_analysis JSON,
    feature_importance JSON,
    
    -- Metadata
    model_version VARCHAR(50),
    optimization_objective VARCHAR(100), -- max_roi, max_revenue, balanced
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    execution_batch_id VARCHAR(50)
);

CREATE INDEX idx_budget_pred_period ON ml_marketing_budget_predictions(planning_period);
CREATE INDEX idx_budget_pred_timestamp ON ml_marketing_budget_predictions(prediction_timestamp);
```

---

### 4. Customer Lifetime Value Prediction ✅

**Objective:** Predict future customer lifetime value for segmentation and targeting

#### Input Tables
- `agg_customers`
- `agg_orders`
- `agg_customer_sessions`
- `agg_rfm_segmentation`

#### Input Features (34 features)

**Purchase History:**
- `total_orders` (BIGINT)
- `total_revenue` (DOUBLE)
- `avg_order_value` (DOUBLE)
- `avg_items_per_order` (DOUBLE)
- `order_frequency` (BIGINT)
- `avg_days_between_orders` (DOUBLE)
- `days_since_last_purchase` (INTEGER)
- `order_recency_days` (INTEGER)

**Customer Profile:**
- `customer_tenure_days` (INTEGER)
- `customer_age` (BIGINT)
- `customer_age_group` (VARCHAR) → encoded
- `gender` (VARCHAR) → encoded

**Engagement Metrics:**
- `total_sessions` (BIGINT)
- `avg_session_duration` (DOUBLE)
- `total_pages_viewed` (BIGINT)
- `session_conversion_rate` (DOUBLE)
- `days_since_last_login` (INTEGER)

**Cart & Wishlist:**
- `total_abandoned_carts` (BIGINT)
- `cart_abandonment_rate` (DOUBLE)
- `wishlist_items_count` (BIGINT)
- `wishlist_conversion_rate` (DOUBLE)

**Discount Behavior:**
- `total_discount_received` (DOUBLE)
- `avg_discount_per_order` (DOUBLE)
- `discount_sensitivity_score` (DOUBLE)

**RFM Scores:**
- `recency_score` (INTEGER)
- `frequency_score` (INTEGER)
- `monetary_score` (INTEGER)
- `rfm_overall_score` (DOUBLE)

**Reviews & Ratings:**
- `total_reviews_written` (BIGINT)
- `avg_review_rating` (DOUBLE)

**Geographic:**
- `country` (VARCHAR) → encoded
- `city_customer_density` (BIGINT)

**Behavioral Flags:**
- `is_repeat_customer` (INTEGER)
- `preferred_device_type` (VARCHAR) → encoded

#### Output Schema: `ml_customer_ltv_predictions`

```sql
CREATE TABLE ml_customer_ltv_predictions (
    prediction_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(255) NOT NULL,
    prediction_timestamp TIMESTAMP NOT NULL,
    
    -- Current State
    current_lifetime_value DOUBLE,
    current_total_orders BIGINT,
    days_since_first_order INTEGER,
    
    -- LTV Predictions
    predicted_ltv_12_months DOUBLE NOT NULL,
    predicted_ltv_24_months DOUBLE NOT NULL,
    predicted_ltv_36_months DOUBLE NOT NULL,
    
    -- Activity Predictions
    predicted_orders_next_12m INTEGER,
    predicted_avg_order_value_12m DOUBLE,
    predicted_purchase_frequency_12m DOUBLE,
    
    -- Risk Assessment
    churn_probability_score DOUBLE,
    engagement_decline_risk DOUBLE,
    value_tier VARCHAR(50), -- high_value, medium_value, low_value, at_risk
    
    -- Confidence & Model Info
    prediction_confidence DOUBLE,
    prediction_uncertainty_range JSON,
    feature_importance JSON,
    
    -- Metadata
    model_version VARCHAR(50),
    customer_segment_at_prediction VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    execution_batch_id VARCHAR(50)
);

CREATE INDEX idx_ltv_pred_customer ON ml_customer_ltv_predictions(customer_id);
CREATE INDEX idx_ltv_pred_tier ON ml_customer_ltv_predictions(value_tier);
CREATE INDEX idx_ltv_pred_timestamp ON ml_customer_ltv_predictions(prediction_timestamp);
```

---

## CLASSIFICATION PROBLEMS

### 5. Intelligent Cart Recovery Action Classifier ✅

**Objective:** Predict the most effective recovery action for abandoned carts

#### Input Tables
- `agg_cart_abandonment_analysis`
- `agg_customers`
- `agg_shopping_cart`
- `agg_cart_items`
- `agg_customer_sessions`

#### Input Features (30 features)

**Cart Characteristics:**
- `cart_total_value` (DOUBLE)
- `cart_items_count` (BIGINT)
- `cart_avg_item_price` (DOUBLE)
- `cart_value_tier` (VARCHAR) → encoded
- `abandoned_cart_category` (VARCHAR) → encoded

**Abandonment Context:**
- `time_since_abandonment_hours` (INTEGER)
- `cart_age_hours` (INTEGER)
- `abandonment_risk_score` (DOUBLE)
- `cart_abandonment_reason` (VARCHAR) → encoded
- `device_used` (VARCHAR) → encoded

**Customer Profile:**
- `customer_lifetime_value` (DOUBLE)
- `customer_segment` (VARCHAR) → encoded
- `rfm_overall_score` (DOUBLE)
- `total_orders` (BIGINT)
- `avg_order_value` (DOUBLE)
- `cart_abandonment_rate` (DOUBLE)
- `days_since_last_purchase` (INTEGER)

**Recovery History:**
- `previous_recovery_attempts` (INTEGER)
- `previous_recovery_success_rate` (DOUBLE)
- `avg_time_to_recovery` (DOUBLE)
- `preferred_recovery_channel` (VARCHAR) → encoded

**Session Context:**
- `session_duration_minutes` (BIGINT)
- `pages_viewed` (INTEGER)
- `products_viewed` (INTEGER)
- `session_engagement_score` (DOUBLE)

**Temporal:**
- `abandonment_day_of_week` (INTEGER)
- `abandonment_hour_of_day` (INTEGER)
- `is_weekend` (BOOLEAN)
- `season` (VARCHAR) → one-hot

#### Output Schema: `ml_cart_recovery_predictions`

```sql
CREATE TABLE ml_cart_recovery_predictions (
    prediction_id VARCHAR(50) PRIMARY KEY,
    cart_id VARCHAR(255) NOT NULL,
    customer_id VARCHAR(255) NOT NULL,
    prediction_timestamp TIMESTAMP NOT NULL,
    
    -- Cart State
    cart_total_value DOUBLE,
    hours_since_abandonment INTEGER,
    
    -- Primary Prediction
    predicted_action_class INTEGER NOT NULL, -- 0-5
    predicted_action_label VARCHAR(100) NOT NULL,
    -- Actions: 0=no_action, 1=email_reminder, 2=discount_offer, 
    --          3=personalized_recommendation, 4=urgent_scarcity, 5=loyalty_incentive
    
    -- Class Probabilities
    prob_no_action DOUBLE,
    prob_email_reminder DOUBLE,
    prob_discount_offer DOUBLE,
    prob_personalized_recommendation DOUBLE,
    prob_urgent_scarcity DOUBLE,
    prob_loyalty_incentive DOUBLE,
    
    -- Recovery Predictions
    predicted_recovery_probability DOUBLE,
    predicted_recovery_time_hours INTEGER,
    predicted_recovered_value DOUBLE,
    
    -- Financial Impact
    expected_revenue DOUBLE,
    expected_cost_of_action DOUBLE,
    expected_net_benefit DOUBLE,
    
    -- Discount Optimization (if applicable)
    recommended_discount_pct DOUBLE,
    recommended_discount_amount DOUBLE,
    
    -- Timing Recommendations
    optimal_contact_time TIMESTAMP,
    contact_urgency_level VARCHAR(50), -- immediate, within_24h, within_48h
    
    -- Alternative Actions
    alternative_action_1 VARCHAR(100),
    alternative_action_1_prob DOUBLE,
    alternative_action_2 VARCHAR(100),
    alternative_action_2_prob DOUBLE,
    
    -- Confidence & Model Info
    prediction_confidence DOUBLE,
    feature_importance JSON,
    shap_values JSON,
    
    -- Metadata
    model_version VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    execution_batch_id VARCHAR(50)
);

CREATE INDEX idx_cart_recovery_pred_cart ON ml_cart_recovery_predictions(cart_id);
CREATE INDEX idx_cart_recovery_pred_customer ON ml_cart_recovery_predictions(customer_id);
CREATE INDEX idx_cart_recovery_pred_action ON ml_cart_recovery_predictions(predicted_action_class);
CREATE INDEX idx_cart_recovery_pred_timestamp ON ml_cart_recovery_predictions(prediction_timestamp);
```

#### Action Results Table: `ml_cart_recovery_results`

```sql
CREATE TABLE ml_cart_recovery_results (
    result_id VARCHAR(50) PRIMARY KEY,
    prediction_id VARCHAR(50) NOT NULL,
    cart_id VARCHAR(255) NOT NULL,
    customer_id VARCHAR(255) NOT NULL,
    
    -- Action Taken
    action_executed BOOLEAN,
    action_executed_timestamp TIMESTAMP,
    actual_action_taken VARCHAR(100),
    discount_given_pct DOUBLE,
    
    -- Outcome
    cart_recovered BOOLEAN,
    recovery_timestamp TIMESTAMP,
    recovery_time_hours INTEGER,
    recovered_value DOUBLE,
    final_order_id VARCHAR(255),
    
    -- Financial Tracking
    action_cost DOUBLE,
    revenue_generated DOUBLE,
    actual_net_benefit DOUBLE,
    
    -- Prediction Accuracy
    prediction_was_accurate BOOLEAN,
    actual_vs_predicted_recovery BOOLEAN,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (prediction_id) REFERENCES ml_cart_recovery_predictions(prediction_id)
);

CREATE INDEX idx_cart_recovery_results_pred ON ml_cart_recovery_results(prediction_id);
CREATE INDEX idx_cart_recovery_results_outcome ON ml_cart_recovery_results(cart_recovered);
```

---

### 6. Customer Churn Risk Classifier ✅

**Objective:** Predict customer churn risk level for proactive retention

#### Input Tables
- `agg_customers`
- `agg_rfm_segmentation`
- `agg_orders`
- `agg_customer_sessions`

#### Input Features (28 features)

**Engagement Metrics:**
- `days_since_last_purchase` (INTEGER)
- `days_since_last_login` (INTEGER)
- `total_sessions_last_30d` (BIGINT)
- `total_sessions_last_90d` (BIGINT)
- `session_frequency_decline_rate` (DOUBLE)
- `avg_session_duration` (DOUBLE)

**Purchase Behavior:**
- `total_orders` (BIGINT)
- `orders_last_30d` (BIGINT)
- `orders_last_90d` (BIGINT)
- `order_frequency_decline_rate` (DOUBLE)
- `avg_days_between_orders` (DOUBLE)
- `avg_order_value` (DOUBLE)
- `order_value_trend` (DOUBLE)

**Customer Value:**
- `customer_lifetime_value` (DOUBLE)
- `rfm_overall_score` (DOUBLE)
- `recency_score` (INTEGER)
- `frequency_score` (INTEGER)
- `monetary_score` (INTEGER)

**Cart & Wishlist:**
- `cart_abandonment_rate` (DOUBLE)
- `total_abandoned_carts_90d` (BIGINT)
- `wishlist_items_count` (BIGINT)

**Satisfaction Indicators:**
- `avg_review_rating` (DOUBLE)
- `total_reviews_written` (BIGINT)
- `customer_service_contacts` (BIGINT)

**Profile:**
- `customer_tenure_days` (INTEGER)
- `customer_segment` (VARCHAR) → encoded
- `preferred_device_type` (VARCHAR) → encoded
- `is_repeat_customer` (INTEGER)

#### Output Schema: `ml_customer_churn_predictions`

```sql
CREATE TABLE ml_customer_churn_predictions (
    prediction_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(255) NOT NULL,
    prediction_timestamp TIMESTAMP NOT NULL,
    
    -- Churn Risk Classification
    predicted_churn_risk_class INTEGER NOT NULL, -- 0=low, 1=medium, 2=high, 3=critical
    predicted_churn_risk_label VARCHAR(50) NOT NULL,
    
    -- Class Probabilities
    prob_low_risk DOUBLE,
    prob_medium_risk DOUBLE,
    prob_high_risk DOUBLE,
    prob_critical_risk DOUBLE,
    
    -- Churn Predictions
    churn_probability_score DOUBLE,
    predicted_days_to_churn INTEGER,
    predicted_churn_date DATE,
    
    -- Value at Risk
    customer_lifetime_value DOUBLE,
    value_at_risk DOUBLE,
    retention_value_12m DOUBLE,
    
    -- Risk Factors
    primary_churn_driver VARCHAR(100),
    secondary_churn_driver VARCHAR(100),
    risk_factor_scores JSON, -- {engagement: 0.8, satisfaction: 0.6, ...}
    
    -- Retention Recommendations
    recommended_retention_action VARCHAR(255),
    recommended_incentive_type VARCHAR(100),
    recommended_discount_budget DOUBLE,
    retention_success_probability DOUBLE,
    
    -- Customer Segments
    customer_segment_current VARCHAR(100),
    customer_value_tier VARCHAR(50),
    
    -- Confidence & Model Info
    prediction_confidence DOUBLE,
    feature_importance JSON,
    shap_values JSON,
    
    -- Metadata
    model_version VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    execution_batch_id VARCHAR(50)
);

CREATE INDEX idx_churn_pred_customer ON ml_customer_churn_predictions(customer_id);
CREATE INDEX idx_churn_pred_risk_class ON ml_customer_churn_predictions(predicted_churn_risk_class);
CREATE INDEX idx_churn_pred_timestamp ON ml_customer_churn_predictions(prediction_timestamp);
```

---

### 7. Product Stock Status Classifier ✅

**Objective:** Classify products into stock health categories for inventory management

#### Input Tables
- `agg_product_inventory_health`
- `agg_products`
- `agg_suppliers`

#### Input Features (22 features)

**Inventory Levels:**
- `current_stock` (INTEGER)
- `available_stock` (INTEGER)
- `reserved_quantity` (INTEGER)
- `minimum_stock_level` (INTEGER)
- `days_of_supply` (DOUBLE)

**Sales Velocity:**
- `avg_daily_sales` (DOUBLE)
- `sales_velocity_7d` (DOUBLE)
- `sales_velocity_30d` (DOUBLE)
- `sales_trend` (DOUBLE)

**Reorder Metrics:**
- `reorder_point_breach_count` (BIGINT)
- `stockout_frequency` (BIGINT)
- `days_since_restock` (INTEGER)
- `inventory_turnover_ratio` (DOUBLE)

**Supplier Context:**
- `supplier_reliability_score` (DOUBLE)
- `avg_restock_lead_time` (DOUBLE)
- `supplier_stockout_rate` (DOUBLE)

**Financial:**
- `storage_cost_per_unit` (DOUBLE)
- `cost_price` (DOUBLE)
- `total_storage_cost` (DOUBLE)

**Product Performance:**
- `product_performance_score` (DOUBLE)
- `total_revenue` (DOUBLE)
- `profit_margin` (DOUBLE)

#### Output Schema: `ml_stock_status_predictions`

```sql
CREATE TABLE ml_stock_status_predictions (
    prediction_id VARCHAR(50) PRIMARY KEY,
    product_id VARCHAR(255) NOT NULL,
    supplier_id VARCHAR(255),
    prediction_timestamp TIMESTAMP NOT NULL,
    
    -- Stock Status Classification
    predicted_status_class INTEGER NOT NULL, 
    -- 0=healthy, 1=warning, 2=critical, 3=stockout_risk, 4=overstock
    predicted_status_label VARCHAR(50) NOT NULL,
    
    -- Class Probabilities
    prob_healthy DOUBLE,
    prob_warning DOUBLE,
    prob_critical DOUBLE,
    prob_stockout_risk DOUBLE,
    prob_overstock DOUBLE,
    
    -- Current Metrics
    current_stock_level INTEGER,
    current_days_of_supply DOUBLE,
    current_stock_health_score DOUBLE,
    
    -- Risk Assessment
    stockout_probability DOUBLE,
    predicted_stockout_date DATE,
    days_until_stockout INTEGER,
    overstock_probability DOUBLE,
    
    -- Action Recommendations
    recommended_action VARCHAR(100), -- reorder_urgent, reorder_soon, monitor, reduce_order, no_action
    recommended_urgency_level VARCHAR(50), -- immediate, high, medium, low
    recommended_reorder_quantity INTEGER,
    
    -- Financial Impact
    stockout_risk_cost DOUBLE,
    overstock_holding_cost DOUBLE,
    optimal_stock_value DOUBLE,
    
    -- Confidence & Model Info
    prediction_confidence DOUBLE,
    feature_importance JSON,
    
    -- Metadata
    model_version VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    execution_batch_id VARCHAR(50)
);

CREATE INDEX idx_stock_status_pred_product ON ml_stock_status_predictions(product_id);
CREATE INDEX idx_stock_status_pred_class ON ml_stock_status_predictions(predicted_status_class);
CREATE INDEX idx_stock_status_pred_timestamp ON ml_stock_status_predictions(prediction_timestamp);
```

---

### 8. Order Fulfillment Risk Classifier ✅ ⛔

**Objective:** Predict likelihood of delayed or failed order fulfillment

#### Input Tables
- `agg_orders`
- `agg_order_items`
- `agg_products`
- `agg_inventory`
- `agg_suppliers`

#### Input Features (26 features)

**Order Characteristics:**
- `total_quantity` (INTEGER)
- `unique_products_ordered` (INTEGER)
- `total_amount` (DOUBLE)
- `order_size_category` (VARCHAR) → encoded
- `has_custom_items` (BOOLEAN)

**Inventory Status:**
- `products_in_stock_count` (INTEGER)
- `products_low_stock_count` (INTEGER)
- `avg_product_availability` (DOUBLE)
- `total_reserved_quantity` (INTEGER)

**Supplier Factors:**
- `primary_supplier_reliability` (DOUBLE)
- `avg_supplier_lead_time` (DOUBLE)
- `supplier_stockout_rate` (DOUBLE)
- `multiple_suppliers_required` (BOOLEAN)

**Shipping Context:**
- `shipping_distance_km` (DOUBLE) -- external
- `shipping_complexity_score` (DOUBLE)
- `destination_remote_flag` (BOOLEAN)

**Historical Performance:**
- `customer_past_delivery_issues` (INTEGER)
- `avg_fulfillment_time_for_category` (DOUBLE)
- `warehouse_current_load` (DOUBLE)

**Temporal:**
- `order_placed_day_of_week` (INTEGER)
- `order_placed_hour` (INTEGER)
- `is_holiday_period` (BOOLEAN)
- `season` (VARCHAR) → one-hot
- `is_peak_shopping_season` (BOOLEAN)

**External Factors:**
- `weather_risk_score` (DOUBLE) -- external
- `logistics_disruption_flag` (BOOLEAN) -- external

#### Output Schema: `ml_fulfillment_risk_predictions`

```sql
CREATE TABLE ml_fulfillment_risk_predictions (
    prediction_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(255) NOT NULL,
    customer_id VARCHAR(255),
    prediction_timestamp TIMESTAMP NOT NULL,
    
    -- Risk Classification
    predicted_risk_class INTEGER NOT NULL, -- 0=low, 1=medium, 2=high, 3=critical
    predicted_risk_label VARCHAR(50) NOT NULL,
    
    -- Class Probabilities
    prob_low_risk DOUBLE,
    prob_medium_risk DOUBLE,
    prob_high_risk DOUBLE,
    prob_critical_risk DOUBLE,
    
    -- Specific Risk Predictions
    delay_probability DOUBLE,
    failure_probability DOUBLE,
    partial_fulfillment_probability DOUBLE,
    
    -- Timing Predictions
    predicted_ship_date DATE,
    predicted_delivery_date DATE,
    expected_delay_days INTEGER,
    delivery_window_confidence DOUBLE,
    
    -- Risk Factors
    primary_risk_factor VARCHAR(100),
    secondary_risk_factor VARCHAR(100),
    risk_factor_breakdown JSON,
    
    -- Mitigation Recommendations
    recommended_action VARCHAR(255),
    alternative_supplier_available BOOLEAN,
    expedited_shipping_recommended BOOLEAN,
    customer_communication_recommended BOOLEAN,
    
    -- Financial Impact
    potential_delay_cost DOUBLE,
    expedited_shipping_cost DOUBLE,
    customer_compensation_estimate DOUBLE,
    
    -- Confidence & Model Info
    prediction_confidence DOUBLE,
    feature_importance JSON,
    
    -- Metadata
    model_version VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    execution_batch_id VARCHAR(50)
);

CREATE INDEX idx_fulfillment_pred_order ON ml_fulfillment_risk_predictions(order_id);
CREATE INDEX idx_fulfillment_pred_risk_class ON ml_fulfillment_risk_predictions(predicted_risk_class);
CREATE INDEX idx_fulfillment_pred_timestamp ON ml_fulfillment_risk_predictions(prediction_timestamp);
```

---

## MULTI-OUTPUT & HYBRID PROBLEMS

### 9. Optimal Supplier Selection & Order Quantity (Hybrid) ⛔

**Objective:** Select best supplier (classification) and determine order quantity (regression)

#### Input Tables
- `agg_product_inventory_health`
- `agg_suppliers`
- `agg_supplier_inventory_health`
- `agg_products`

#### Input Features (32 features)

**Product Demand:**
- `current_stock_level` (INTEGER)
- `avg_daily_sales` (DOUBLE)
- `predicted_demand_30d` (DOUBLE)
- `demand_variability` (DOUBLE)
- `days_of_supply` (DOUBLE)
- `stockout_frequency` (BIGINT)

**Supplier Metrics (for each candidate supplier):**
- `supplier_reliability_score` (DOUBLE)
- `supplier_lead_time_days` (DOUBLE)
- `supplier_cost_per_unit` (DOUBLE)
- `supplier_min_order_quantity` (INTEGER)
- `supplier_current_stock` (INTEGER)
- `supplier_stockout_rate` (DOUBLE)
- `supplier_quality_score` (DOUBLE)
- `supplier_delivery_success_rate` (DOUBLE)

**Supplier Relationship:**
- `is_preferred_supplier` (BOOLEAN)
- `contract_status` (VARCHAR) → encoded
- `days_until_contract_expiry` (INTEGER)
- `historical_order_count` (BIGINT)
- `historical_issue_count` (BIGINT)

**Financial Factors:**
- `product_profit_margin` (DOUBLE)
- `storage_cost_per_unit` (DOUBLE)
- `urgency_premium_acceptable` (DOUBLE)

**Risk Factors:**
- `supplier_geographic_risk` (DOUBLE) -- external
- `supply_chain_disruption_risk` (DOUBLE) -- external
- `currency_fluctuation_risk` (DOUBLE) -- external

**Temporal:**
- `season` (VARCHAR) → one-hot
- `is_peak_demand_period` (BOOLEAN)
- `days_to_major_event` (INTEGER)

#### Output Schema: `ml_supplier_selection_predictions`

```sql
CREATE TABLE ml_supplier_selection_predictions (
    prediction_id VARCHAR(50) PRIMARY KEY,
    product_id VARCHAR(255) NOT NULL,
    prediction_timestamp TIMESTAMP NOT NULL,
    
    -- Supplier Selection (Classification)
    predicted_supplier_id VARCHAR(255) NOT NULL,
    predicted_supplier_name VARCHAR(255),
    supplier_selection_confidence DOUBLE,
    
    -- Supplier Ranking
    supplier_rank_1_id VARCHAR(255),
    supplier_rank_1_score DOUBLE,
    supplier_rank_2_id VARCHAR(255),
    supplier_rank_2_score DOUBLE,
    supplier_rank_3_id VARCHAR(255),
    supplier_rank_3_score DOUBLE,
    
    -- Order Quantity (Regression)
    predicted_order_quantity INTEGER NOT NULL,
    order_quantity_lower_bound INTEGER,
    order_quantity_upper_bound INTEGER,
    order_quantity_confidence DOUBLE,
    
    -- Logistics Predictions
    predicted_delivery_date DATE,
    predicted_lead_time_days INTEGER,
    delivery_reliability_score DOUBLE,
    
    -- Financial Analysis
    total_order_cost DOUBLE,
    cost_per_unit DOUBLE,
    expected_total_cost DOUBLE, -- including shipping, storage
    expected_profit_margin DOUBLE,
    cost_efficiency_score DOUBLE,
    
    -- Risk Assessment
    overall_risk_score DOUBLE,
    supply_disruption_risk DOUBLE,
    quality_risk DOUBLE,
    delivery_risk DOUBLE,
    
    -- Alternative Scenarios
    alternative_supplier_available BOOLEAN,
    split_order_recommended BOOLEAN,
    split_order_details JSON, -- if recommended
    
    -- Decision Factors
    selection_primary_reason VARCHAR(255),
    selection_factors JSON,
    feature_importance JSON,
    
    -- Metadata
    model_version VARCHAR(50),
    classification_model_version VARCHAR(50),
    regression_model_version VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    execution_batch_id VARCHAR(50)
);

CREATE INDEX idx_supplier_select_pred_product ON ml_supplier_selection_predictions(product_id);
CREATE INDEX idx_supplier_select_pred_supplier ON ml_supplier_selection_predictions(predicted_supplier_id);
CREATE INDEX idx_supplier_select_pred_timestamp ON ml_supplier_selection_predictions(prediction_timestamp);
```

---

### 10. Campaign Performance Forecasting & Budget Rebalancing (Multi-output)

**Objective:** Forecast campaign KPIs and recommend budget adjustments

#### Input Tables
- `agg_marketing_campaigns`
- `agg_orders`
- `agg_customer_sessions`
- `agg_customers`

#### Input Features (30 features)

**Campaign Characteristics:**
- `campaign_type` (VARCHAR) → encoded
- `target_audience` (VARCHAR) → encoded
- `campaign_duration_days` (INTEGER)
- `days_elapsed` (INTEGER)
- `days_remaining` (INTEGER)

**Current Performance:**
- `total_impressions` (INTEGER)
- `total_clicks` (INTEGER)
- `total_conversions` (INTEGER)
- `current_ctr` (DOUBLE)
- `current_conversion_rate` (DOUBLE)
- `current_roi` (DOUBLE)
- `revenue_generated` (DOUBLE)

**Budget Metrics:**
- `original_budget` (DOUBLE)
- `spent_to_date` (DOUBLE)
- `remaining_budget` (DOUBLE)
- `daily_budget_avg` (DOUBLE)
- `daily_spend_trend` (DOUBLE)

**Audience Metrics:**
- `audience_size` (INTEGER)
- `audience_reached_pct` (DOUBLE)
- `new_audience_potential` (INTEGER)
- `audience_engagement_score` (DOUBLE)

**Competitive Context:**
- `competitor_campaign_count` (INTEGER) -- external
- `market_saturation_level` (DOUBLE) -- external
- `industry_avg_ctr` (DOUBLE) -- external

**Historical Context:**
- `similar_campaign_avg_performance` (DOUBLE)
- `channel_historical_performance` (DOUBLE)
- `seasonal_performance_factor` (DOUBLE)

**Temporal:**
- `current_day_of_week` (INTEGER)
- `is_weekend` (BOOLEAN)
- `season` (VARCHAR) → one-hot

#### Output Schema: `ml_campaign_performance_predictions`

```sql
CREATE TABLE ml_campaign_performance_predictions (
    prediction_id VARCHAR(50) PRIMARY KEY,
    campaign_id VARCHAR(255) NOT NULL,
    prediction_timestamp TIMESTAMP NOT NULL,
    
    -- Current State
    days_elapsed INTEGER,
    days_remaining INTEGER,
    budget_spent_to_date DOUBLE,
    current_roi DOUBLE,
    
    -- Forecasted KPIs (Multi-output Regression)
    predicted_total_impressions INTEGER,
    predicted_total_clicks INTEGER,
    predicted_total_conversions INTEGER,
    predicted_total_revenue DOUBLE,
    
    -- Predicted End-of-Campaign Metrics
    predicted_final_ctr DOUBLE,
    predicted_final_conversion_rate DOUBLE,
    predicted_final_roi DOUBLE,
    predicted_final_roas DOUBLE,
    predicted_final_cac DOUBLE,
    
    -- Performance vs Target
    target_revenue DOUBLE,
    revenue_gap DOUBLE,
    probability_of_target_hit DOUBLE,
    performance_tier VARCHAR(50), -- exceeding, on_track, underperforming, failing
    
    -- Budget Rebalancing Recommendations
    recommended_budget_adjustment DOUBLE, -- positive = increase, negative = decrease
    recommended_daily_spend DOUBLE,
    budget_adjustment_reason VARCHAR(255),
    optimal_remaining_budget DOUBLE,
    
    -- Tactical Recommendations
    recommended_bid_adjustment_pct DOUBLE,
    recommended_audience_expansion BOOLEAN,
    recommended_creative_refresh BOOLEAN,
    pause_campaign_recommended BOOLEAN,
    
    -- Risk & Opportunity
    underperformance_risk_score DOUBLE,
    budget_waste_risk DOUBLE,
    growth_opportunity_score DOUBLE,
    
    -- Confidence & Model Info
    prediction_confidence DOUBLE,
    confidence_interval_lower JSON,
    confidence_interval_upper JSON,
    feature_importance JSON,
    
    -- Metadata
    model_version VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    execution_batch_id VARCHAR(50)
);

CREATE INDEX idx_campaign_perf_pred_campaign ON ml_campaign_performance_predictions(campaign_id);
CREATE INDEX idx_campaign_perf_pred_tier ON ml_campaign_performance_predictions(performance_tier);
CREATE INDEX idx_campaign_perf_pred_timestamp ON ml_campaign_performance_predictions(prediction_timestamp);
```

---

## CROSS-CUTTING INFRASTRUCTURE TABLES

### Model Training Metadata

```sql
CREATE TABLE ml_model_training_runs (
    run_id VARCHAR(50) PRIMARY KEY,
    model_name VARCHAR(255) NOT NULL,
    model_type VARCHAR(100), -- regression, classification, multi-output
    model_version VARCHAR(50) NOT NULL,
    
    -- Training Configuration
    algorithm VARCHAR(100),
    hyperparameters JSON,
    feature_count INTEGER,
    training_dataset_size INTEGER,
    validation_dataset_size INTEGER,
    test_dataset_size INTEGER,
    
    -- Performance Metrics
    training_metrics JSON,
    validation_metrics JSON,
    test_metrics JSON,
    
    -- Feature Engineering
    feature_importance JSON,
    selected_features JSON,
    feature_transformations JSON,
    
    -- Model Artifacts
    model_artifact_path VARCHAR(500),
    model_size_mb DOUBLE,
    
    -- Training Infrastructure
    training_duration_seconds INTEGER,
    training_start_timestamp TIMESTAMP,
    training_end_timestamp TIMESTAMP,
    compute_resources_used VARCHAR(255),
    
    -- Metadata
    created_by VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notes TEXT
);

CREATE INDEX idx_model_training_name_version ON ml_model_training_runs(model_name, model_version);
CREATE INDEX idx_model_training_timestamp ON ml_model_training_runs(training_start_timestamp);
```

### Model Deployment Tracking

```sql
CREATE TABLE ml_model_deployments (
    deployment_id VARCHAR(50) PRIMARY KEY,
    model_name VARCHAR(255) NOT NULL,
    model_version VARCHAR(50) NOT NULL,
    run_id VARCHAR(50),
    
    -- Deployment Info
    deployment_environment VARCHAR(50), -- dev, staging, production
    deployment_timestamp TIMESTAMP NOT NULL,
    deployment_status VARCHAR(50), -- active, inactive, deprecated
    
    -- Performance Monitoring
    total_predictions_served BIGINT DEFAULT 0,
    avg_prediction_latency_ms DOUBLE,
    error_rate DOUBLE,
    
    -- Model Drift Detection
    feature_drift_score DOUBLE,
    prediction_drift_score DOUBLE,
    data_quality_score DOUBLE,
    last_drift_check_timestamp TIMESTAMP,
    
    -- Rollback Info
    previous_version VARCHAR(50),
    rollback_available BOOLEAN DEFAULT TRUE,
    
    -- Metadata
    deployed_by VARCHAR(255),
    deactivated_at TIMESTAMP,
    deactivated_by VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (run_id) REFERENCES ml_model_training_runs(run_id)
);

CREATE INDEX idx_model_deployment_name ON ml_model_deployments(model_name);
CREATE INDEX idx_model_deployment_status ON ml_model_deployments(deployment_status);
```

### Model Performance Monitoring

```sql
CREATE TABLE ml_model_performance_monitoring (
    monitoring_id VARCHAR(50) PRIMARY KEY,
    deployment_id VARCHAR(50) NOT NULL,
    model_name VARCHAR(255) NOT NULL,
    model_version VARCHAR(50) NOT NULL,
    monitoring_date DATE NOT NULL,
    
    -- Prediction Volume
    total_predictions INTEGER,
    successful_predictions INTEGER,
    failed_predictions INTEGER,
    
    -- Performance Metrics
    avg_prediction_confidence DOUBLE,
    prediction_accuracy DOUBLE, -- when ground truth available
    precision_score DOUBLE,
    recall_score DOUBLE,
    f1_score DOUBLE,
    
    -- Business Impact
    business_value_generated DOUBLE,
    cost_savings DOUBLE,
    revenue_impact DOUBLE,
    
    -- Data Quality
    missing_features_count INTEGER,
    outlier_detection_count INTEGER,
    data_quality_issues JSON,
    
    -- Drift Metrics
    feature_drift_detected BOOLEAN,
    prediction_drift_detected BOOLEAN,
    drift_severity VARCHAR(50),
    
    -- System Health
    avg_latency_ms DOUBLE,
    p95_latency_ms DOUBLE,
    p99_latency_ms DOUBLE,
    error_rate_pct DOUBLE,
    
    -- Alerts
    alerts_triggered JSON,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (deployment_id) REFERENCES ml_model_deployments(deployment_id)
);

CREATE INDEX idx_model_perf_mon_deployment ON ml_model_performance_monitoring(deployment_id);
CREATE INDEX idx_model_perf_mon_date ON ml_model_performance_monitoring(monitoring_date);
```

---

## IMPLEMENTATION NOTES

### Feature Engineering Pipeline

1. **Aggregation Layer**: Compute features from raw aggregated tables
2. **Temporal Features**: Extract time-based features (day_of_week, season, etc.)
3. **Encoding Layer**: One-hot encode categorical variables, normalize numeric features
4. **Feature Store**: Maintain versioned feature sets for reproducibility

### Model Training Pipeline

1. **Data Extraction**: Query aggregated tables with appropriate time windows
2. **Feature Engineering**: Apply transformations and create derived features
3. **Train/Validation/Test Split**: Time-based splits for temporal data
4. **Model Training**: Train ensemble models (XGBoost, Random Forest, Neural Networks)
5. **Hyperparameter Tuning**: Grid search or Bayesian optimization
6. **Model Evaluation**: Comprehensive metrics on test set
7. **Model Registration**: Version and register in model registry

### Prediction Pipeline

1. **Scheduled Batch Predictions**: Daily/hourly predictions for all applicable entities
2. **Real-time Predictions**: On-demand predictions for critical decisions
3. **Result Storage**: Write predictions to output tables
4. **Action Triggering**: Integrate with business systems for automated actions
5. **Monitoring**: Track prediction quality and business impact

### Model Monitoring & Retraining

1. **Performance Monitoring**: Daily metrics on prediction quality
2. **Drift Detection**: Monitor feature and prediction drift
3. **Business Impact Tracking**: Measure actual vs predicted outcomes
4. **Retraining Triggers**: Automatic retraining when drift exceeds thresholds
5. **A/B Testing**: Compare new models against production before full deployment

---

## DATABASE PERFORMANCE OPTIMIZATION

```sql
-- Composite indexes for common prediction queries
CREATE INDEX idx_predictions_product_timestamp 
ON ml_inventory_reorder_predictions(product_id, prediction_timestamp DESC);

CREATE INDEX idx_predictions_customer_timestamp 
ON ml_customer_ltv_predictions(customer_id, prediction_timestamp DESC);

-- Partitioning strategy for large prediction tables
-- Partition by month for better query performance and maintenance
ALTER TABLE ml_inventory_reorder_predictions 
PARTITION BY RANGE (prediction_timestamp);

ALTER TABLE ml_pricing_optimization_predictions 
PARTITION BY RANGE (prediction_timestamp);

-- Materialized views for frequently accessed aggregations
CREATE MATERIALIZED VIEW mv_daily_prediction_summary AS
SELECT 
    DATE(prediction_timestamp) as prediction_date,
    COUNT(*) as total_predictions,
    AVG(prediction_confidence) as avg_confidence,
    SUM(expected_revenue_impact) as total_expected_impact
FROM ml_inventory_reorder_predictions
GROUP BY DATE(prediction_timestamp);

-- Refresh materialized view daily
REFRESH MATERIALIZED VIEW mv_daily_prediction_summary;
```

---

## APPENDIX: Feature Importance Examples

### Common High-Impact Features Across Problems

**Inventory Problems:**
- days_of_supply
- avg_daily_sales
- stockout_frequency
- supplier_reliability_score

**Customer Problems:**
- rfm_overall_score
- days_since_last_purchase
- customer_lifetime_value
- session_conversion_rate

**Pricing Problems:**
- current_stock_level
- competitor_avg_price
- demand_elasticity
- avg_rating

**Marketing Problems:**
- historical_roi
- conversion_rate
- audience_size
- days_remaining_in_period
