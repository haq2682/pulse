-- ============================================================================
-- AGGREGATED TABLES SCHEMA - THREE-TIER APPROACH
-- ============================================================================
-- Tier 1: PRIMARY KEYS & CRITICAL FIELDS -
-- Tier 2: COUNTS, BOOLEANS, FLAGS - with DEFAULTS
-- Tier 3: DERIVED/CALCULATED FIELDS - NULLABLE
-- ============================================================================

-- User table
CREATE TABLE users (
    user_id VARCHAR(50) PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    reset_token VARCHAR(255),
    reset_token_expires TIMESTAMP
);
-- Business table
CREATE TABLE businesses (
    business_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    business_name VARCHAR(255) NOT NULL,
    business_region VARCHAR(100),
    business_currency VARCHAR(50),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- 1. agg_customer_sessions
CREATE TABLE agg_customer_sessions (
    -- Tier 1: Critical Fields
    session_id VARCHAR(255) PRIMARY KEY,
    customer_id VARCHAR(255),
    session_start TIMESTAMP,
    
    -- Tier 2: Fields with Defaults
    conversion_flag INTEGER DEFAULT 0,
    cart_abandonment_flag INTEGER DEFAULT 0,
    pages_viewed INTEGER DEFAULT 0,
    products_viewed INTEGER DEFAULT 0,
    total_pages_viewed INTEGER DEFAULT 0,
    total_products_viewed INTEGER DEFAULT 0,
    converted INTEGER DEFAULT 0,
    abandoned INTEGER DEFAULT 0,
    items_added_to_cart BIGINT DEFAULT 0,
    orders_from_session BIGINT DEFAULT 0,
    
    -- Tier 3: Nullable Derived Fields
    session_end TIMESTAMP NULL,
    device_type VARCHAR(100) NULL,
    referrer_source VARCHAR(255) NULL,
    session_duration_seconds BIGINT NULL,
    session_duration_minutes BIGINT NULL,
    session_duration_hours BIGINT NULL,
    pages_per_minute DOUBLE PRECISION NULL,
    products_per_page DOUBLE PRECISION NULL,
    cart_value DOUBLE PRECISION NULL,
    cart_add_rate DOUBLE PRECISION NULL,
    avg_cart_item_value DOUBLE PRECISION NULL,
    session_engagement_score DOUBLE PRECISION NULL,
    session_type VARCHAR(100) NULL
);

-- 2. agg_customers
CREATE TABLE agg_customers (
    -- Tier 1: Critical Fields
    customer_id VARCHAR(255) PRIMARY KEY,
    account_created_at TIMESTAMP,
    account_status VARCHAR(100),
    
    -- Tier 2: Fields with Defaults
    is_active BOOLEAN DEFAULT FALSE,
    is_repeat_customer INTEGER DEFAULT 0,
    total_orders BIGINT DEFAULT 0,
    total_items_purchased BIGINT DEFAULT 0,
    total_cancelled_orders BIGINT DEFAULT 0,
    total_reviews_written BIGINT DEFAULT 0,
    total_sessions BIGINT DEFAULT 0,
    total_pages_viewed BIGINT DEFAULT 0,
    total_products_viewed BIGINT DEFAULT 0,
    wishlist_items_count BIGINT DEFAULT 0,
    total_carts_created BIGINT DEFAULT 0,
    total_abandoned_carts BIGINT DEFAULT 0,
    total_purchased_carts BIGINT DEFAULT 0,
    order_frequency BIGINT DEFAULT 0,
    
    -- Tier 3: Nullable Derived Fields
    gender VARCHAR(50) NULL,
    date_of_birth DATE NULL,
    city VARCHAR(255) NULL,
    state_province VARCHAR(255) NULL,
    postal_code VARCHAR(50) NULL,
    country VARCHAR(255) NULL,
    last_login_date DATE NULL,
    order_recency_days INTEGER NULL,
    order_total_spent DOUBLE PRECISION NULL,
    customer_age BIGINT NULL,
    customer_tenure_days INTEGER NULL,
    days_since_last_login INTEGER NULL,
    customer_age_group VARCHAR(100) NULL,
    customer_activity_status VARCHAR(100) NULL,
    customer_segment VARCHAR(100) NULL,
    customer_lifetime_value DOUBLE PRECISION NULL,
    total_revenue DOUBLE PRECISION NULL,
    avg_order_value DOUBLE PRECISION NULL,
    avg_items_per_order DOUBLE PRECISION NULL,
    total_discount_received DOUBLE PRECISION NULL,
    avg_discount_per_order DOUBLE PRECISION NULL,
    first_order_date TIMESTAMP NULL,
    last_order_date TIMESTAMP NULL,
    avg_days_between_orders DOUBLE PRECISION NULL,
    avg_review_rating DOUBLE PRECISION NULL,
    avg_session_duration DOUBLE PRECISION NULL,
    session_conversion_rate DOUBLE PRECISION NULL,
    cart_abandonment_rate DOUBLE PRECISION NULL,
    preferred_device_type VARCHAR(100) NULL,
    preferred_referrer_source VARCHAR(255) NULL,
    wishlist_conversion_rate DOUBLE PRECISION NULL,
    preferred_payment_method VARCHAR(100) NULL,
    days_since_last_purchase INTEGER NULL,
    cancellation_rate DOUBLE PRECISION NULL,
    customer_activity_score DOUBLE PRECISION NULL,
    total_abandoned_value DOUBLE PRECISION NULL,
    avg_time_in_cart_days DOUBLE PRECISION NULL,
    customer_abandonment_rate DOUBLE PRECISION NULL,
    customer_purchase_rate DOUBLE PRECISION NULL,
    recency_score INTEGER NULL,
    frequency_score INTEGER NULL,
    monetary_score INTEGER NULL,
    rfm_segment VARCHAR(50) NULL,
    customer_segment_label VARCHAR(100) NULL,
    rfm_overall_score DOUBLE PRECISION NULL,
    rfm_category VARCHAR(100) NULL,
    churn_risk VARCHAR(100) NULL
);

-- 3. agg_inventory
CREATE TABLE agg_inventory (
    -- Tier 1: Critical Fields
    inventory_id VARCHAR(255) PRIMARY KEY,
    product_id VARCHAR(255),
    supplier_id VARCHAR(255),
    
    -- Tier 2: Fields with Defaults
    stock_quantity INTEGER DEFAULT 0,
    reserved_quantity INTEGER DEFAULT 0,
    minimum_stock_level INTEGER DEFAULT 0,
    available_stock INTEGER DEFAULT 0,
    reorder_point_breach INTEGER DEFAULT 0,
    total_sold BIGINT DEFAULT 0,
    
    -- Tier 3: Nullable Derived Fields
    last_restocked_date DATE NULL,
    storage_cost DOUBLE PRECISION NULL,
    avg_inventory DOUBLE PRECISION NULL,
    storage_cost_per_unit DOUBLE PRECISION NULL,
    stock_status VARCHAR(100) NULL,
    stock_coverage_days DOUBLE PRECISION NULL,
    stock_turnover_ratio DOUBLE PRECISION NULL
);

-- 4. agg_marketing_campaigns
CREATE TABLE agg_marketing_campaigns (
    -- Tier 1: Critical Fields
    campaign_id VARCHAR(255) PRIMARY KEY,
    campaign_name VARCHAR(500),
    campaign_type VARCHAR(100),
    start_date DATE,
    campaign_status VARCHAR(100),
    
    -- Tier 2: Fields with Defaults
    impressions INTEGER DEFAULT 0,
    clicks INTEGER DEFAULT 0,
    conversions INTEGER DEFAULT 0,
    total_impressions INTEGER DEFAULT 0,
    total_clicks INTEGER DEFAULT 0,
    total_conversions INTEGER DEFAULT 0,
    orders_from_campaign BIGINT DEFAULT 0,
    days_active INTEGER DEFAULT 0,
    
    -- Tier 3: Nullable Derived Fields
    end_date VARCHAR(255) NULL,
    budget DOUBLE PRECISION NULL,
    spent_amount DOUBLE PRECISION NULL,
    target_audience VARCHAR(100) NULL,
    campaign_duration_days INTEGER NULL,
    campaign_roi DOUBLE PRECISION NULL,
    click_through_rate DOUBLE PRECISION NULL,
    conversion_rate DOUBLE PRECISION NULL,
    cost_per_conversion DOUBLE PRECISION NULL,
    cost_per_click DOUBLE PRECISION NULL,
    campaign_efficiency_score DOUBLE PRECISION NULL,
    revenue_generated DOUBLE PRECISION NULL,
    total_budget DOUBLE PRECISION NULL,
    total_spent DOUBLE PRECISION NULL,
    budget_utilization_rate DOUBLE PRECISION NULL,
    ctr DOUBLE PRECISION NULL,
    roi DOUBLE PRECISION NULL,
    roas DOUBLE PRECISION NULL,
    avg_order_value DOUBLE PRECISION NULL,
    revenue_per_impression DOUBLE PRECISION NULL,
    revenue_per_click DOUBLE PRECISION NULL,
    campaign_profit DOUBLE PRECISION NULL,
    cost_efficiency_ratio DOUBLE PRECISION NULL,
    engagement_rate DOUBLE PRECISION NULL,
    campaign_status_derived VARCHAR(100) NULL,
    days_until_end INTEGER NULL,
    performance_tier VARCHAR(100) NULL,
    budget_status VARCHAR(100) NULL
);

-- 5. agg_order_items
CREATE TABLE agg_order_items (
    -- Tier 1: Critical Fields
    order_item_id VARCHAR(255) PRIMARY KEY,
    order_id VARCHAR(255),
    product_id VARCHAR(255),
    
    -- Tier 2: Fields with Defaults
    quantity INTEGER DEFAULT 0,
    
    -- Tier 3: Nullable Derived Fields
    discount_amount DOUBLE PRECISION NULL,
    product_cost DOUBLE PRECISION NULL
);

-- 6. agg_orders
CREATE TABLE agg_orders (
    -- Tier 1: Critical Fields
    order_id VARCHAR(255) PRIMARY KEY,
    customer_id VARCHAR(255),
    order_status VARCHAR(100),
    order_placed_at TIMESTAMP,
    
    -- Tier 2: Fields with Defaults
    order_placed_year INTEGER DEFAULT 0,
    order_placed_month INTEGER DEFAULT 0,
    order_placed_quarter INTEGER DEFAULT 0,
    order_placed_day_of_week INTEGER DEFAULT 0,
    order_placed_week_of_year INTEGER DEFAULT 0,
    order_placed_day_of_month INTEGER DEFAULT 0,
    
    -- Tier 3: Nullable Derived Fields
    subtotal DOUBLE PRECISION NULL,
    tax_amount DOUBLE PRECISION NULL,
    shipping_cost DOUBLE PRECISION NULL,
    total_discount DOUBLE PRECISION NULL,
    total_amount DOUBLE PRECISION NULL,
    currency VARCHAR(10) NULL,
    order_shipped_at DATE NULL,
    order_delivered_at DATE NULL,
    order_shipped_year INTEGER NULL,
    order_shipped_month INTEGER NULL,
    order_shipped_quarter INTEGER NULL,
    order_shipped_day_of_week INTEGER NULL,
    order_shipped_week_of_year INTEGER NULL,
    order_shipped_day_of_month INTEGER NULL,
    order_delivered_year INTEGER NULL,
    order_delivered_month INTEGER NULL,
    order_delivered_quarter INTEGER NULL,
    order_delivered_day_of_week INTEGER NULL,
    order_delivered_week_of_year INTEGER NULL,
    order_delivered_day_of_month INTEGER NULL,
    order_processing_seconds_diff BIGINT NULL,
    order_processing_minutes_diff BIGINT NULL,
    order_processing_hours_diff BIGINT NULL,
    order_processing_days_diff INTEGER NULL,
    order_processing_weeks_diff DOUBLE PRECISION NULL,
    order_processing_months_diff DOUBLE PRECISION NULL,
    order_processing_years_diff DOUBLE PRECISION NULL,
    delivery_seconds_diff BIGINT NULL,
    delivery_minutes_diff BIGINT NULL,
    delivery_hours_diff BIGINT NULL,
    delivery_days_diff INTEGER NULL,
    delivery_weeks_diff DOUBLE PRECISION NULL,
    delivery_months_diff DOUBLE PRECISION NULL,
    delivery_years_diff DOUBLE PRECISION NULL,
    total_order_fulfillment_time_seconds BIGINT NULL,
    total_order_fulfillment_time_minutes BIGINT NULL,
    total_order_fulfillment_time_hours BIGINT NULL,
    total_order_fulfillment_time_days INTEGER NULL,
    total_order_fulfillment_time_weeks DOUBLE PRECISION NULL,
    total_order_fulfillment_time_months DOUBLE PRECISION NULL,
    total_order_fulfillment_time_years DOUBLE PRECISION NULL,
    total_product_cost DOUBLE PRECISION DEFAULT 0,
    total_quantity INTEGER DEFAULT 0,
    avg_product_cost DOUBLE PRECISION DEFAULT 0,
    max_item_discount DOUBLE PRECISION DEFAULT 0,
    unique_products_ordered INTEGER DEFAULT 0,
    order_profit DOUBLE PRECISION NULL,
    net_revenue DOUBLE PRECISION NULL,
    net_profit DOUBLE PRECISION NULL,
    discount_percentage DOUBLE PRECISION NULL,
    average_item_value DOUBLE PRECISION NULL,
    cost_per_item DOUBLE PRECISION NULL,
    order_size_category VARCHAR(100) NULL,
    season VARCHAR(50) NULL
);

-- 7. agg_payments
CREATE TABLE agg_payments (
    -- Tier 1: Critical Fields
    payment_id VARCHAR(255) PRIMARY KEY,
    order_id VARCHAR(255),
    payment_method VARCHAR(100),
    payment_status VARCHAR(100),
    payment_date DATE,
    
    -- Tier 3: Nullable Derived Fields
    payment_provider VARCHAR(255) NULL,
    transaction_id VARCHAR(255) NULL,
    processing_fee DOUBLE PRECISION NULL,
    refund_amount DOUBLE PRECISION NULL,
    refund_date DATE NULL
);

-- 8. agg_products
CREATE TABLE agg_products (
    -- Tier 1: Critical Fields
    product_id VARCHAR(255) PRIMARY KEY,
    product_name VARCHAR(500),
    sku VARCHAR(255),
    category VARCHAR(255),
    
    -- Tier 2: Fields with Defaults
    current_stock_level INTEGER DEFAULT 0,
    total_units_sold BIGINT DEFAULT 0,
    total_orders BIGINT DEFAULT 0,
    unique_customers BIGINT DEFAULT 0,
    total_reviews BIGINT DEFAULT 0,
    total_wishlist_adds BIGINT DEFAULT 0,
    total_cart_adds BIGINT DEFAULT 0,
    stockout_occurrences BIGINT DEFAULT 0,
    products_in_category BIGINT DEFAULT 0,
    current_stock INTEGER DEFAULT 0,
    
    -- Tier 3: Nullable Derived Fields
    sub_category VARCHAR(255) NULL,
    brand VARCHAR(255) NULL,
    supplier_id VARCHAR(255) NULL,
    cost_price DOUBLE PRECISION NULL,
    sell_price DOUBLE PRECISION NULL,
    launch_date DATE NULL,
    weight DOUBLE PRECISION NULL,
    dimensions VARCHAR(255) NULL,
    color VARCHAR(100) NULL,
    size VARCHAR(100) NULL,
    material VARCHAR(255) NULL,
    profit_margin DOUBLE PRECISION NULL,
    total_revenue DOUBLE PRECISION NULL,
    total_profit DOUBLE PRECISION NULL,
    avg_profit_margin DOUBLE PRECISION NULL,
    avg_quantity_per_order DOUBLE PRECISION NULL,
    avg_discount_amount DOUBLE PRECISION NULL,
    avg_rating DOUBLE PRECISION NULL,
    rating_std_dev DOUBLE PRECISION NULL,
    positive_review_rate DOUBLE PRECISION NULL,
    wishlist_to_purchase_rate DOUBLE PRECISION NULL,
    cart_to_purchase_rate DOUBLE PRECISION NULL,
    avg_restock_frequency DOUBLE PRECISION NULL,
    view_to_purchase_rate DOUBLE PRECISION NULL,
    revenue_per_view DOUBLE PRECISION NULL,
    days_since_launch INTEGER NULL,
    stockout_days BIGINT NULL,
    product_performance_score DOUBLE PRECISION NULL,
    inventory_turnover_rate DOUBLE PRECISION NULL,
    avg_order_value_product DOUBLE PRECISION NULL,
    customer_penetration DOUBLE PRECISION NULL,
    category_total_revenue DOUBLE PRECISION NULL,
    category_avg_rating DOUBLE PRECISION NULL,
    product_category_revenue_share DOUBLE PRECISION NULL,
    revenue_share_percentage DOUBLE PRECISION NULL,
    avg_category_growth_rate DOUBLE PRECISION NULL,
    category_performance_tier VARCHAR(100) NULL,
    stock_status VARCHAR(100) NULL,
    days_of_supply DOUBLE PRECISION NULL,
    reorder_urgency VARCHAR(100) NULL
);

-- 9. agg_reviews
CREATE TABLE agg_reviews (
    -- Tier 1: Critical Fields
    review_id VARCHAR(255) PRIMARY KEY,
    product_id VARCHAR(255),
    customer_id VARCHAR(255),
    review_date TIMESTAMP,
    
    -- Tier 2: Fields with Defaults
    rating INTEGER DEFAULT 0,
    
    -- Tier 3: Nullable Derived Fields
    review_title VARCHAR(500) NULL,
    review_desc TEXT NULL,
    review_sentiment VARCHAR(100) NULL
);

-- 10. agg_shopping_cart
CREATE TABLE agg_shopping_cart (
    -- Tier 1: Critical Fields
    cart_id VARCHAR(255),
    customer_id VARCHAR(255),
    product_id VARCHAR(255),
    added_date DATE,
    cart_status VARCHAR(100),
    
    -- Tier 2: Fields with Defaults
    quantity INTEGER DEFAULT 0,
    
    -- Tier 3: Nullable Derived Fields
    session_id VARCHAR(255) NULL,
    unit_price DOUBLE PRECISION NULL,
    cart_age_time BIGINT NULL,
    cart_abandonment_flag VARCHAR(50) NULL,
    
    PRIMARY KEY (cart_id, product_id)
);

-- 11. agg_suppliers
CREATE TABLE agg_suppliers (
    -- Tier 1: Critical Fields
    supplier_id VARCHAR(255) PRIMARY KEY,
    supplier_status VARCHAR(100),
    
    -- Tier 2: Fields with Defaults
    is_preferred BOOLEAN DEFAULT FALSE,
    is_verified BOOLEAN DEFAULT FALSE,
    total_products_supplied BIGINT DEFAULT 0,
    total_units_sold BIGINT DEFAULT 0,
    total_orders_fulfilled BIGINT DEFAULT 0,
    total_reviews BIGINT DEFAULT 0,
    total_stockouts BIGINT DEFAULT 0,
    
    -- Tier 3: Nullable Derived Fields
    supplier_rating DOUBLE PRECISION NULL,
    contract_start_date DATE NULL,
    contract_end_date DATE NULL,
    city VARCHAR(255) NULL,
    state VARCHAR(255) NULL,
    zip_code VARCHAR(255) NULL,
    country VARCHAR(255) NULL,
    total_revenue_generated DOUBLE PRECISION NULL,
    avg_profit_margin DOUBLE PRECISION NULL,
    avg_product_rating DOUBLE PRECISION NULL,
    total_stock_value DOUBLE PRECISION NULL,
    avg_stock_quantity DOUBLE PRECISION NULL,
    avg_restock_lead_time DOUBLE PRECISION NULL,
    contract_status_flag VARCHAR(100) NULL,
    days_until_contract_expiry INTEGER NULL,
    contract_duration_days INTEGER NULL,
    revenue_per_product DOUBLE PRECISION NULL,
    avg_order_value DOUBLE PRECISION NULL,
    avg_units_per_product DOUBLE PRECISION NULL,
    supplier_performance_score DOUBLE PRECISION NULL,
    stock_efficiency_ratio DOUBLE PRECISION NULL,
    supplier_reliability_score DOUBLE PRECISION NULL,
    stockout_rate DOUBLE PRECISION NULL,
    supplier_inventory_health_score DOUBLE PRECISION NULL
);

-- 12. agg_wishlist
CREATE TABLE agg_wishlist (
    -- Tier 1: Critical Fields
    wishlist_id VARCHAR(255) PRIMARY KEY,
    customer_id VARCHAR(255),
    product_id VARCHAR(255),
    added_date DATE,
    
    -- Tier 3: Nullable Derived Fields
    purchased_date DATE NULL,
    removed_date DATE NULL,
    wishlist_to_purchase_time BIGINT NULL
);

-- 13. agg_categories
CREATE TABLE agg_categories (
    -- Tier 1: Critical Fields
    category VARCHAR(255) PRIMARY KEY,
    
    -- Tier 2: Fields with Defaults
    total_products_in_category BIGINT DEFAULT 0,
    total_units_sold BIGINT DEFAULT 0,
    total_orders BIGINT DEFAULT 0,
    unique_customers BIGINT DEFAULT 0,
    total_reviews BIGINT DEFAULT 0,
    revenue_rank INTEGER DEFAULT 0,
    rating_rank INTEGER DEFAULT 0,
    growth_rank INTEGER DEFAULT 0,
    
    -- Tier 3: Nullable Derived Fields
    total_revenue DOUBLE PRECISION NULL,
    avg_items_per_order DOUBLE PRECISION NULL,
    avg_product_price DOUBLE PRECISION NULL,
    avg_rating DOUBLE PRECISION NULL,
    avg_category_growth_rate DOUBLE PRECISION NULL,
    revenue_share_percentage DOUBLE PRECISION NULL,
    avg_order_value DOUBLE PRECISION NULL,
    revenue_per_customer DOUBLE PRECISION NULL,
    avg_units_per_customer DOUBLE PRECISION NULL,
    category_popularity_score DOUBLE PRECISION NULL,
    product_diversity_index DOUBLE PRECISION NULL,
    avg_orders_per_product DOUBLE PRECISION NULL,
    peak_season DOUBLE PRECISION NULL,
    seasonal_index_fall DOUBLE PRECISION NULL,
    seasonal_index_winter DOUBLE PRECISION NULL,
    seasonal_index_spring DOUBLE PRECISION NULL,
    seasonal_index_summer DOUBLE PRECISION NULL
);

-- 14. agg_daily_aggregations
CREATE TABLE agg_daily_aggregations (
    -- Tier 1: Critical Fields
    order_date DATE PRIMARY KEY,
    order_year INTEGER,
    order_month INTEGER,
    
    -- Tier 2: Fields with Defaults
    total_orders BIGINT DEFAULT 0,
    total_customers BIGINT DEFAULT 0,
    new_customers BIGINT DEFAULT 0,
    returning_customers BIGINT DEFAULT 0,
    total_units_sold BIGINT DEFAULT 0,
    total_sessions BIGINT DEFAULT 0,
    total_conversions BIGINT DEFAULT 0,
    prev_day_customers BIGINT DEFAULT 0,
    
    -- Tier 3: Nullable Derived Fields
    total_revenue DOUBLE PRECISION NULL,
    avg_order_value DOUBLE PRECISION NULL,
    session_to_order_rate DOUBLE PRECISION NULL,
    prev_day_revenue DOUBLE PRECISION NULL,
    revenue_growth_rate DOUBLE PRECISION NULL,
    customer_retention_rate DOUBLE PRECISION NULL
);

-- 15. agg_weekly_aggregations
CREATE TABLE agg_weekly_aggregations (
    -- Tier 1: Critical Fields
    year_week VARCHAR(50) PRIMARY KEY,
    order_year INTEGER,
    order_week INTEGER,
    
    -- Tier 2: Fields with Defaults
    total_orders BIGINT DEFAULT 0,
    total_customers BIGINT DEFAULT 0,
    new_customers BIGINT DEFAULT 0,
    returning_customers BIGINT DEFAULT 0,
    total_units_sold BIGINT DEFAULT 0,
    total_sessions BIGINT DEFAULT 0,
    total_conversions BIGINT DEFAULT 0,
    prev_week_customers BIGINT DEFAULT 0,
    
    -- Tier 3: Nullable Derived Fields
    total_revenue DOUBLE PRECISION NULL,
    avg_order_value DOUBLE PRECISION NULL,
    session_to_order_rate DOUBLE PRECISION NULL,
    prev_week_revenue DOUBLE PRECISION NULL,
    revenue_growth_rate DOUBLE PRECISION NULL,
    customer_retention_rate DOUBLE PRECISION NULL
);

-- 16. agg_monthly_aggregations
CREATE TABLE agg_monthly_aggregations (
    -- Tier 1: Critical Fields
    year_month VARCHAR(50) PRIMARY KEY,
    order_year INTEGER,
    order_month INTEGER,
    
    -- Tier 2: Fields with Defaults
    total_orders BIGINT DEFAULT 0,
    total_customers BIGINT DEFAULT 0,
    new_customers BIGINT DEFAULT 0,
    returning_customers BIGINT DEFAULT 0,
    total_units_sold BIGINT DEFAULT 0,
    total_sessions BIGINT DEFAULT 0,
    total_conversions BIGINT DEFAULT 0,
    prev_month_customers BIGINT DEFAULT 0,
    
    -- Tier 3: Nullable Derived Fields
    total_revenue DOUBLE PRECISION NULL,
    avg_order_value DOUBLE PRECISION NULL,
    session_to_order_rate DOUBLE PRECISION NULL,
    prev_month_revenue DOUBLE PRECISION NULL,
    revenue_growth_rate DOUBLE PRECISION NULL,
    customer_retention_rate DOUBLE PRECISION NULL,
    churn_rate DOUBLE PRECISION NULL
);

-- 17. agg_country_aggregations
CREATE TABLE agg_country_aggregations (
    -- Tier 1: Critical Fields
    country VARCHAR(255) PRIMARY KEY,
    
    -- Tier 2: Fields with Defaults
    total_customers BIGINT DEFAULT 0,
    total_orders BIGINT DEFAULT 0,
    total_suppliers BIGINT DEFAULT 0,
    
    -- Tier 3: Nullable Derived Fields
    total_revenue DOUBLE PRECISION NULL,
    avg_order_value DOUBLE PRECISION NULL,
    avg_customer_lifetime_value DOUBLE PRECISION NULL,
    preferred_category VARCHAR(255) NULL,
    revenue_per_customer DOUBLE PRECISION NULL,
    orders_per_customer DOUBLE PRECISION NULL
);

-- 18. agg_state_aggregations
CREATE TABLE agg_state_aggregations (
    -- Tier 1: Critical Fields
    country VARCHAR(255),
    state_province VARCHAR(255),
    
    -- Tier 2: Fields with Defaults
    total_customers BIGINT DEFAULT 0,
    total_orders BIGINT DEFAULT 0,
    total_suppliers BIGINT DEFAULT 0,
    
    -- Tier 3: Nullable Derived Fields
    total_revenue DOUBLE PRECISION NULL,
    avg_order_value DOUBLE PRECISION NULL,
    avg_customer_lifetime_value DOUBLE PRECISION NULL,
    preferred_category VARCHAR(255) NULL,
    revenue_per_customer DOUBLE PRECISION NULL,
    orders_per_customer DOUBLE PRECISION NULL,
    
    PRIMARY KEY (country, state_province)
);

-- 19. agg_city_aggregations
CREATE TABLE agg_city_aggregations (
    -- Tier 1: Critical Fields
    country VARCHAR(255),
    state_province VARCHAR(255),
    city VARCHAR(255),
    
    -- Tier 2: Fields with Defaults
    total_customers BIGINT DEFAULT 0,
    total_orders BIGINT DEFAULT 0,
    total_suppliers BIGINT DEFAULT 0,
    customer_density BIGINT DEFAULT 0,
    
    -- Tier 3: Nullable Derived Fields
    total_revenue DOUBLE PRECISION NULL,
    avg_order_value DOUBLE PRECISION NULL,
    avg_customer_lifetime_value DOUBLE PRECISION NULL,
    preferred_category VARCHAR(255) NULL,
    revenue_per_customer DOUBLE PRECISION NULL,
    orders_per_customer DOUBLE PRECISION NULL,
    
    PRIMARY KEY (country, state_province, city)
);

-- 20. agg_cart_abandonment_analysis
CREATE TABLE agg_cart_abandonment_analysis (
    -- Tier 1: Critical Fields
    cart_id VARCHAR(255) PRIMARY KEY,
    cart_status VARCHAR(100),
    cart_added_date DATE,
    customer_id VARCHAR(255),
    
    -- Tier 2: Fields with Defaults
    cart_items_count BIGINT DEFAULT 0,
    session_converted INTEGER DEFAULT 0,
    time_in_cart_days INTEGER DEFAULT 0,
    time_in_cart_hours INTEGER DEFAULT 0,
    recovery_potential_score INTEGER DEFAULT 0,
    
    -- Tier 3: Nullable Derived Fields
    cart_total_value DOUBLE PRECISION NULL,
    cart_avg_item_price DOUBLE PRECISION NULL,
    device_used VARCHAR(100) NULL,
    abandoned_cart_category VARCHAR(255) NULL,
    first_added_date DATE NULL,
    last_added_date DATE NULL,
    session_id VARCHAR(255) NULL,
    cart_status_derived VARCHAR(100) NULL,
    cart_abandonment_reason VARCHAR(255) NULL,
    cart_value_tier VARCHAR(100) NULL,
    cart_size_category VARCHAR(100) NULL,
    abandonment_risk_score DOUBLE PRECISION NULL
);

-- 21. agg_product_inventory_health
CREATE TABLE agg_product_inventory_health (
    -- Tier 1: Critical Fields
    product_id VARCHAR(255) PRIMARY KEY,
    supplier_id VARCHAR(255),
    
    -- Tier 2: Fields with Defaults
    current_stock INTEGER DEFAULT 0,
    available_stock INTEGER DEFAULT 0,
    reorder_point_breach_count BIGINT DEFAULT 0,
    stockout_frequency BIGINT DEFAULT 0,
    reserved_quantity INTEGER DEFAULT 0,
    minimum_stock_level INTEGER DEFAULT 0,
    stock_health_score INTEGER DEFAULT 0,
    
    -- Tier 3: Nullable Derived Fields
    avg_stock_quantity DOUBLE PRECISION NULL,
    storage_cost_per_unit DOUBLE PRECISION NULL,
    last_restock_date DATE NULL,
    storage_cost DOUBLE PRECISION NULL,
    cost_price DOUBLE PRECISION NULL,
    avg_daily_sales DOUBLE PRECISION NULL,
    total_cogs DOUBLE PRECISION NULL,
    stock_status VARCHAR(100) NULL,
    days_of_supply DOUBLE PRECISION NULL,
    inventory_turnover_ratio DOUBLE PRECISION NULL,
    days_since_restock INTEGER NULL,
    reorder_urgency VARCHAR(100) NULL
);

-- 22. agg_supplier_inventory_health
CREATE TABLE agg_supplier_inventory_health (
    -- Tier 1: Critical Fields
    supplier_id VARCHAR(255) PRIMARY KEY,
    
    -- Tier 2: Fields with Defaults
    total_products BIGINT DEFAULT 0,
    total_current_stock BIGINT DEFAULT 0,
    total_available_stock BIGINT DEFAULT 0,
    total_reorder_breaches BIGINT DEFAULT 0,
    total_stockouts BIGINT DEFAULT 0,
    
    -- Tier 3: Nullable Derived Fields
    total_storage_cost DOUBLE PRECISION NULL,
    avg_stock_per_product DOUBLE PRECISION NULL,
    last_restock_date DATE NULL,
    stockout_rate DOUBLE PRECISION NULL,
    breach_rate DOUBLE PRECISION NULL,
    avg_storage_cost_per_unit DOUBLE PRECISION NULL,
    days_since_last_restock INTEGER NULL,
    supplier_inventory_health_score DOUBLE PRECISION NULL
);

-- 23. agg_rfm_segmentation
CREATE TABLE agg_rfm_segmentation (
    -- Tier 1: Critical Fields
    customer_id VARCHAR(255) PRIMARY KEY,
    
    -- Tier 2: Fields with Defaults
    total_orders_rfm BIGINT DEFAULT 0,
    
    -- Tier 3: Nullable Derived Fields
    days_since_last_order INTEGER NULL,
    total_revenue_rfm DOUBLE PRECISION NULL,
    recency_score INTEGER NULL,
    frequency_score INTEGER NULL,
    monetary_score INTEGER NULL,
    rfm_segment VARCHAR(50) NULL,
    customer_segment_label VARCHAR(100) NULL,
    rfm_overall_score DOUBLE PRECISION NULL,
    rfm_category VARCHAR(100) NULL,
    engagement_level VARCHAR(100) NULL,
    purchase_behavior VARCHAR(100) NULL,
    spending_pattern VARCHAR(100) NULL,
    churn_risk VARCHAR(100) NULL
);

-- 24. agg_rfm_segment_summary
CREATE TABLE agg_rfm_segment_summary (
    -- Tier 1: Critical Fields
    customer_segment_label VARCHAR(100) PRIMARY KEY,
    
    -- Tier 2: Fields with Defaults
    customer_count BIGINT DEFAULT 0,
    
    -- Tier 3: Nullable Derived Fields
    avg_revenue DOUBLE PRECISION NULL,
    avg_orders DOUBLE PRECISION NULL,
    avg_days_since_order DOUBLE PRECISION NULL,
    avg_rfm_score DOUBLE PRECISION NULL
);

-- 25. agg_product_affinity
CREATE TABLE agg_product_affinity (
    -- Tier 1: Critical Fields
    product_a_id VARCHAR(255),
    product_b_id VARCHAR(255),
    
    -- Tier 2: Fields with Defaults
    co_occurrence_count BIGINT DEFAULT 0,
    product_a_count BIGINT DEFAULT 0,
    product_b_count BIGINT DEFAULT 0,
    is_cross_category BOOLEAN DEFAULT FALSE,
    
    -- Tier 3: Nullable Derived Fields
    support DOUBLE PRECISION NULL,
    confidence_a_to_b DOUBLE PRECISION NULL,
    confidence_b_to_a DOUBLE PRECISION NULL,
    prob_b DOUBLE PRECISION NULL,
    prob_a DOUBLE PRECISION NULL,
    lift_a_to_b DOUBLE PRECISION NULL,
    lift_b_to_a DOUBLE PRECISION NULL,
    avg_lift DOUBLE PRECISION NULL,
    product_a_name VARCHAR(500) NULL,
    product_a_category VARCHAR(255) NULL,
    product_b_name VARCHAR(500) NULL,
    product_b_category VARCHAR(255) NULL,
    affinity_strength VARCHAR(100) NULL,
    affinity_score DOUBLE PRECISION NULL,
    
    PRIMARY KEY (product_a_id, product_b_id)
);

-- 26. agg_top_product_pairs
CREATE TABLE agg_top_product_pairs (
    -- Tier 1: Critical Fields
    product_a_id VARCHAR(255),
    product_b_id VARCHAR(255),
    
    -- Tier 2: Fields with Defaults
    co_occurrence_count BIGINT DEFAULT 0,
    product_a_count BIGINT DEFAULT 0,
    product_b_count BIGINT DEFAULT 0,
    is_cross_category BOOLEAN DEFAULT FALSE,
    
    -- Tier 3: Nullable Derived Fields
    support DOUBLE PRECISION NULL,
    confidence_a_to_b DOUBLE PRECISION NULL,
    confidence_b_to_a DOUBLE PRECISION NULL,
    prob_b DOUBLE PRECISION NULL,
    prob_a DOUBLE PRECISION NULL,
    lift_a_to_b DOUBLE PRECISION NULL,
    lift_b_to_a DOUBLE PRECISION NULL,
    avg_lift DOUBLE PRECISION NULL,
    product_a_name VARCHAR(500) NULL,
    product_a_category VARCHAR(255) NULL,
    product_b_name VARCHAR(500) NULL,
    product_b_category VARCHAR(255) NULL,
    affinity_strength VARCHAR(100) NULL,
    affinity_score DOUBLE PRECISION NULL,
    
    PRIMARY KEY (product_a_id, product_b_id)
);

-- 27. agg_product_recommendations
CREATE TABLE agg_product_recommendations (
    product_a_id VARCHAR(255) PRIMARY KEY,
    recommendation_count BIGINT DEFAULT 0,
    product_a_name VARCHAR(500) NULL,
    recommended_products TEXT NULL,
    avg_affinity_score DOUBLE PRECISION NULL
);

-- 28. agg_category_affinity
CREATE TABLE agg_category_affinity (
    -- Tier 1: Critical Fields
    product_a_category VARCHAR(255),
    product_b_category VARCHAR(255),
    
    -- Tier 2: Fields with Defaults
    pair_count BIGINT DEFAULT 0,
    total_co_occurrences BIGINT DEFAULT 0,
    
    -- Tier 3: Nullable Derived Fields
    avg_lift_between_categories DOUBLE PRECISION NULL,
    avg_support DOUBLE PRECISION NULL,
    
    PRIMARY KEY (product_a_category, product_b_category)
);

-- 29. agg_global_aggregations
CREATE TABLE agg_global_aggregations (
    -- Tier 1: Critical Fields
    metric_name VARCHAR(255) PRIMARY KEY,
    calculated_at VARCHAR(50),
    
    -- Tier 3: Nullable Derived Fields
    metric_value DOUBLE PRECISION NULL
);

-- ============================================================================
-- INDEXES FOR PERFORMANCE OPTIMIZATION
-- ============================================================================

-- Customer Sessions Indexes
CREATE INDEX idx_agg_customer_sessions_customer ON agg_customer_sessions(customer_id);
CREATE INDEX idx_agg_customer_sessions_start ON agg_customer_sessions(session_start);
CREATE INDEX idx_agg_customer_sessions_device ON agg_customer_sessions(device_type) WHERE device_type IS;

-- Customers Indexes
CREATE INDEX idx_agg_customers_country ON agg_customers(country) WHERE country IS;
CREATE INDEX idx_agg_customers_segment ON agg_customers(customer_segment) WHERE customer_segment IS;
CREATE INDEX idx_agg_customers_rfm ON agg_customers(rfm_segment) WHERE rfm_segment IS;
CREATE INDEX idx_agg_customers_churn ON agg_customers(churn_risk) WHERE churn_risk IS;
CREATE INDEX idx_agg_customers_active ON agg_customers(is_active);
CREATE INDEX idx_agg_customers_status ON agg_customers(account_status);

-- Orders Indexes
CREATE INDEX idx_agg_orders_customer ON agg_orders(customer_id);
CREATE INDEX idx_agg_orders_placed_at ON agg_orders(order_placed_at);
CREATE INDEX idx_agg_orders_status ON agg_orders(order_status);
CREATE INDEX idx_agg_orders_year_month ON agg_orders(order_placed_year, order_placed_month);
CREATE INDEX idx_agg_orders_season ON agg_orders(season) WHERE season IS;

-- Order Items Indexes
CREATE INDEX idx_agg_order_items_order ON agg_order_items(order_id);
CREATE INDEX idx_agg_order_items_product ON agg_order_items(product_id);

-- Products Indexes
CREATE INDEX idx_agg_products_category ON agg_products(category);
CREATE INDEX idx_agg_products_supplier ON agg_products(supplier_id) WHERE supplier_id IS;
CREATE INDEX idx_agg_products_stock_status ON agg_products(stock_status) WHERE stock_status IS;
CREATE INDEX idx_agg_products_performance ON agg_products(product_performance_score) WHERE product_performance_score IS;
CREATE INDEX idx_agg_products_sku ON agg_products(sku);

-- Inventory Indexes
CREATE INDEX idx_agg_inventory_product ON agg_inventory(product_id);
CREATE INDEX idx_agg_inventory_supplier ON agg_inventory(supplier_id);
CREATE INDEX idx_agg_inventory_status ON agg_inventory(stock_status) WHERE stock_status IS;
CREATE INDEX idx_agg_inventory_reorder ON agg_inventory(reorder_point_breach) WHERE reorder_point_breach = 1;

-- Marketing Campaigns Indexes
CREATE INDEX idx_agg_campaigns_dates ON agg_marketing_campaigns(start_date, end_date);
CREATE INDEX idx_agg_campaigns_status ON agg_marketing_campaigns(campaign_status);
CREATE INDEX idx_agg_campaigns_tier ON agg_marketing_campaigns(performance_tier) WHERE performance_tier IS;
CREATE INDEX idx_agg_campaigns_type ON agg_marketing_campaigns(campaign_type);

-- Payments Indexes
CREATE INDEX idx_agg_payments_order ON agg_payments(order_id);
CREATE INDEX idx_agg_payments_method ON agg_payments(payment_method);
CREATE INDEX idx_agg_payments_status ON agg_payments(payment_status);
CREATE INDEX idx_agg_payments_date ON agg_payments(payment_date);

-- Reviews Indexes
CREATE INDEX idx_agg_reviews_product ON agg_reviews(product_id);
CREATE INDEX idx_agg_reviews_customer ON agg_reviews(customer_id);
CREATE INDEX idx_agg_reviews_sentiment ON agg_reviews(review_sentiment) WHERE review_sentiment IS;
CREATE INDEX idx_agg_reviews_date ON agg_reviews(review_date);
CREATE INDEX idx_agg_reviews_rating ON agg_reviews(rating);

-- Shopping Cart Indexes
CREATE INDEX idx_agg_cart_customer ON agg_shopping_cart(customer_id);
CREATE INDEX idx_agg_cart_session ON agg_shopping_cart(session_id) WHERE session_id IS;
CREATE INDEX idx_agg_cart_status ON agg_shopping_cart(cart_status);
CREATE INDEX idx_agg_cart_added_date ON agg_shopping_cart(added_date);

-- Suppliers Indexes
CREATE INDEX idx_agg_suppliers_status ON agg_suppliers(supplier_status);
CREATE INDEX idx_agg_suppliers_preferred ON agg_suppliers(is_preferred);
CREATE INDEX idx_agg_suppliers_verified ON agg_suppliers(is_verified);

-- Wishlist Indexes
CREATE INDEX idx_agg_wishlist_customer ON agg_wishlist(customer_id);
CREATE INDEX idx_agg_wishlist_product ON agg_wishlist(product_id);
CREATE INDEX idx_agg_wishlist_added ON agg_wishlist(added_date);

-- Aggregation Tables Indexes
CREATE INDEX idx_agg_daily_year_month ON agg_daily_aggregations(order_year, order_month);
CREATE INDEX idx_agg_weekly_year ON agg_weekly_aggregations(order_year);
CREATE INDEX idx_agg_monthly_year ON agg_monthly_aggregations(order_year);

-- Geographic Aggregations Indexes
CREATE INDEX idx_agg_state_country ON agg_state_aggregations(country);
CREATE INDEX idx_agg_city_country_state ON agg_city_aggregations(country, state_province);

-- Cart Abandonment Indexes
CREATE INDEX idx_agg_cart_abandon_customer ON agg_cart_abandonment_analysis(customer_id);
CREATE INDEX idx_agg_cart_abandon_status ON agg_cart_abandonment_analysis(cart_status);
CREATE INDEX idx_agg_cart_abandon_date ON agg_cart_abandonment_analysis(cart_added_date);
CREATE INDEX idx_agg_cart_abandon_tier ON agg_cart_abandonment_analysis(cart_value_tier) WHERE cart_value_tier IS;

-- Inventory Health Indexes
CREATE INDEX idx_agg_prod_inv_health_supplier ON agg_product_inventory_health(supplier_id);
CREATE INDEX idx_agg_prod_inv_health_status ON agg_product_inventory_health(stock_status) WHERE stock_status IS;
CREATE INDEX idx_agg_prod_inv_health_urgency ON agg_product_inventory_health(reorder_urgency) WHERE reorder_urgency IS;

-- RFM Indexes
CREATE INDEX idx_agg_rfm_segment ON agg_rfm_segmentation(customer_segment_label) WHERE customer_segment_label IS;
CREATE INDEX idx_agg_rfm_churn ON agg_rfm_segmentation(churn_risk) WHERE churn_risk IS;
CREATE INDEX idx_agg_rfm_category ON agg_rfm_segmentation(rfm_category) WHERE rfm_category IS;

-- Product Affinity Indexes
CREATE INDEX idx_agg_affinity_product_a ON agg_product_affinity(product_a_id);
CREATE INDEX idx_agg_affinity_product_b ON agg_product_affinity(product_b_id);
CREATE INDEX idx_agg_affinity_strength ON agg_product_affinity(affinity_strength) WHERE affinity_strength IS;
CREATE INDEX idx_agg_affinity_cross_category ON agg_product_affinity(is_cross_category);

-- Top Product Pairs Indexes
CREATE INDEX idx_agg_top_pairs_product_a ON agg_top_product_pairs(product_a_id);
CREATE INDEX idx_agg_top_pairs_product_b ON agg_top_product_pairs(product_b_id);
CREATE INDEX idx_agg_top_pairs_score ON agg_top_product_pairs(affinity_score) WHERE affinity_score IS;

-- Global Aggregations Index
CREATE INDEX idx_agg_global_calculated ON agg_global_aggregations(calculated_at);

-- ============================================================================
-- COMMENTS FOR DOCUMENTATION
-- ============================================================================

-- Table Comments
COMMENT ON TABLE agg_customers IS 'Aggregated customer data with RFM segmentation and lifetime value metrics. NULL values in derived fields indicate incomplete source data.';
COMMENT ON TABLE agg_orders IS 'Aggregated order data with fulfillment metrics and profit calculations. Shipping/delivery dates may be NULL for pending orders.';
COMMENT ON TABLE agg_products IS 'Aggregated product performance metrics including inventory and sales data. Performance scores NULL when insufficient data.';
COMMENT ON TABLE agg_rfm_segmentation IS 'Customer segmentation based on Recency, Frequency, and Monetary analysis. Scores NULL for customers with no order history.';
COMMENT ON TABLE agg_product_affinity IS 'Product association rules for recommendation engine. Lift/confidence NULL when insufficient co-occurrences.';
COMMENT ON TABLE agg_cart_abandonment_analysis IS 'Detailed cart abandonment patterns and recovery opportunities. Risk scores NULL when cart is too new.';
COMMENT ON TABLE agg_global_aggregations IS 'Business-wide KPIs and metrics. Updated periodically based on calculated_at timestamp.';
COMMENT ON TABLE agg_customer_sessions IS 'Session-level analytics with engagement metrics. NULL session_end indicates active sessions.';
COMMENT ON TABLE agg_marketing_campaigns IS 'Campaign performance tracking with ROI metrics. Revenue metrics NULL until orders are attributed.';
COMMENT ON TABLE agg_inventory IS 'Product inventory status with turnover and health metrics. Coverage days NULL when no sales history.';

-- Column Comments (Key Examples)
COMMENT ON COLUMN agg_customers.rfm_overall_score IS 'Overall RFM score (1-15). NULL if customer has no order history.';
COMMENT ON COLUMN agg_customers.churn_risk IS 'Churn risk level: High/Medium/Low. NULL if insufficient data for prediction.';
COMMENT ON COLUMN agg_orders.order_processing_days_diff IS 'Days between order placed and shipped. NULL if not yet shipped.';
COMMENT ON COLUMN agg_orders.season IS 'Season when order was placed. NULL if order_placed_at is missing.';
COMMENT ON COLUMN agg_products.product_performance_score IS 'Composite performance score (0-100). NULL if product has no sales.';
COMMENT ON COLUMN agg_products.days_of_supply IS 'Days until stockout based on avg sales. NULL if no sales velocity data.';
COMMENT ON COLUMN agg_customer_sessions.session_engagement_score IS 'Engagement score based on activity. NULL if session has no interactions.';
COMMENT ON COLUMN agg_marketing_campaigns.roas IS 'Return on Ad Spend. NULL until campaign generates revenue.';
COMMENT ON COLUMN agg_product_affinity.lift_a_to_b IS 'Association rule lift. NULL if insufficient co-purchase data.';
COMMENT ON COLUMN agg_cart_abandonment_analysis.abandonment_risk_score IS 'Risk score (0-100). NULL for carts less than 1 hour old.';

-- -- ============================================================================
-- -- DATA QUALITY VIEWS (Optional - Helps Monitor NULL Values)
-- -- ============================================================================

-- -- View to monitor NULL percentages in critical tables
-- CREATE OR REPLACE VIEW v_agg_data_quality_metrics AS
-- SELECT 
--     'agg_customers' as table_name,
--     COUNT(*) as total_records,
--     COUNT(*) FILTER (WHERE rfm_overall_score IS NULL) as null_rfm_scores,
--     ROUND(100.0 * COUNT(*) FILTER (WHERE rfm_overall_score IS NULL) / COUNT(*), 2) as pct_null_rfm,
--     COUNT(*) FILTER (WHERE total_revenue IS NULL) as null_revenues,
--     ROUND(100.0 * COUNT(*) FILTER (WHERE total_revenue IS NULL) / COUNT(*), 2) as pct_null_revenue
-- FROM agg_customers
-- UNION ALL
-- SELECT 
--     'agg_orders',
--     COUNT(*),
--     COUNT(*) FILTER (WHERE order_profit IS NULL),
--     ROUND(100.0 * COUNT(*) FILTER (WHERE order_profit IS NULL) / COUNT(*), 2),
--     COUNT(*) FILTER (WHERE order_delivered_at IS NULL),
--     ROUND(100.0 * COUNT(*) FILTER (WHERE order_delivered_at IS NULL) / COUNT(*), 2)
-- FROM agg_orders
-- UNION ALL
-- SELECT 
--     'agg_products',
--     COUNT(*),
--     COUNT(*) FILTER (WHERE product_performance_score IS NULL),
--     ROUND(100.0 * COUNT(*) FILTER (WHERE product_performance_score IS NULL) / COUNT(*), 2),
--     COUNT(*) FILTER (WHERE total_revenue IS NULL),
--     ROUND(100.0 * COUNT(*) FILTER (WHERE total_revenue IS NULL) / COUNT(*), 2)
-- FROM agg_products;

-- COMMENT ON VIEW v_agg_data_quality_metrics IS 'Monitor NULL value percentages in key aggregated tables to identify data quality issues.';

-- -- ============================================================================
-- -- HELPFUL NULL-HANDLING FUNCTIONS
-- -- ============================================================================

-- -- Function to safely calculate percentages (returns 0 instead of NULL)
-- CREATE OR REPLACE FUNCTION safe_percentage(numerator NUMERIC, denominator NUMERIC)
-- RETURNS NUMERIC AS $$
-- BEGIN
--     IF denominator IS NULL OR denominator = 0 THEN
--         RETURN 0;
--     END IF;
--     RETURN ROUND(100.0 * COALESCE(numerator, 0) / denominator, 2);
-- END;
-- $$ LANGUAGE plpgsql IMMUTABLE;

-- COMMENT ON FUNCTION safe_percentage IS 'Calculate percentage safely, returning 0 when denominator is NULL or zero.';

-- -- Function to get non-null count
-- CREATE OR REPLACE FUNCTION coalesce_to_zero(val NUMERIC)
-- RETURNS NUMERIC AS $$
-- BEGIN
--     RETURN COALESCE(val, 0);
-- END;
-- $$ LANGUAGE plpgsql IMMUTABLE;

-- COMMENT ON FUNCTION coalesce_to_zero IS 'Convert NULL numeric values to 0 for aggregations.';