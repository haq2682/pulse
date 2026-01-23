from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType, IntegerType, LongType, DoubleType, BooleanType, DateType, TimestampType
from typing import Dict, List, Optional, Tuple


# Hardcoded schemas extracted from agg_schema.sql
SCHEMAS = {
    'agg_customer_sessions': [
        ('session_id', 'string'), ('customer_id', 'string'), ('session_start', 'timestamp'),
        ('conversion_flag', 'integer'), ('cart_abandonment_flag', 'integer'),
        ('pages_viewed', 'integer'), ('products_viewed', 'integer'),
        ('total_pages_viewed', 'integer'), ('total_products_viewed', 'integer'),
        ('converted', 'integer'), ('abandoned', 'integer'),
        ('items_added_to_cart', 'long'), ('orders_from_session', 'long'),
        ('session_end', 'timestamp'), ('device_type', 'string'), ('referrer_source', 'string'),
        ('session_duration_seconds', 'long'), ('session_duration_minutes', 'long'),
        ('session_duration_hours', 'long'), ('pages_per_minute', 'double'),
        ('products_per_page', 'double'), ('cart_value', 'double'),
        ('cart_add_rate', 'double'), ('avg_cart_item_value', 'double'),
        ('session_engagement_score', 'double'), ('session_type', 'string')
    ],
    'agg_customers': [
        ('customer_id', 'string'), ('account_created_at', 'timestamp'), ('account_status', 'string'),
        ('is_active', 'boolean'), ('is_repeat_customer', 'integer'),
        ('total_orders', 'long'), ('total_items_purchased', 'long'),
        ('total_cancelled_orders', 'long'), ('total_reviews_written', 'long'),
        ('total_sessions', 'long'), ('total_pages_viewed', 'long'),
        ('total_products_viewed', 'long'), ('wishlist_items_count', 'long'),
        ('total_carts_created', 'long'), ('total_abandoned_carts', 'long'),
        ('total_purchased_carts', 'long'), ('order_frequency', 'long'),
        ('gender', 'string'), ('date_of_birth', 'date'), ('city', 'string'),
        ('state_province', 'string'), ('postal_code', 'string'), ('country', 'string'),
        ('last_login_date', 'date'), ('order_recency_days', 'integer'),
        ('order_total_spent', 'double'), ('customer_age', 'long'),
        ('customer_tenure_days', 'integer'), ('days_since_last_login', 'integer'),
        ('customer_age_group', 'string'), ('customer_activity_status', 'string'),
        ('customer_segment', 'string'), ('customer_lifetime_value', 'double'),
        ('total_revenue', 'double'), ('avg_order_value', 'double'),
        ('avg_items_per_order', 'double'), ('total_discount_received', 'double'),
        ('avg_discount_per_order', 'double'), ('first_order_date', 'timestamp'),
        ('last_order_date', 'timestamp'), ('avg_days_between_orders', 'double'),
        ('avg_review_rating', 'double'), ('avg_session_duration', 'double'),
        ('session_conversion_rate', 'double'), ('cart_abandonment_rate', 'double'),
        ('preferred_device_type', 'string'), ('preferred_referrer_source', 'string'),
        ('wishlist_conversion_rate', 'double'), ('preferred_payment_method', 'string'),
        ('days_since_last_purchase', 'integer'), ('cancellation_rate', 'double'),
        ('customer_activity_score', 'double'), ('total_abandoned_value', 'double'),
        ('avg_time_in_cart_days', 'double'), ('customer_abandonment_rate', 'double'),
        ('customer_purchase_rate', 'double'), ('recency_score', 'integer'),
        ('frequency_score', 'integer'), ('monetary_score', 'integer'),
        ('rfm_segment', 'string'), ('customer_segment_label', 'string'),
        ('rfm_overall_score', 'double'), ('rfm_category', 'string'), ('churn_risk', 'string')
    ],
    'agg_inventory': [
        ('inventory_id', 'string'), ('product_id', 'string'), ('supplier_id', 'string'),
        ('stock_quantity', 'integer'), ('reserved_quantity', 'integer'),
        ('minimum_stock_level', 'integer'), ('available_stock', 'integer'),
        ('reorder_point_breach', 'integer'), ('total_sold', 'long'),
        ('last_restocked_date', 'date'), ('storage_cost', 'double'),
        ('avg_inventory', 'double'), ('storage_cost_per_unit', 'double'),
        ('stock_status', 'string'), ('stock_coverage_days', 'double'),
        ('stock_turnover_ratio', 'double')
    ],
    'agg_marketing_campaigns': [
        ('campaign_id', 'string'), ('campaign_name', 'string'), ('campaign_type', 'string'),
        ('start_date', 'date'), ('campaign_status', 'string'),
        ('impressions', 'integer'), ('clicks', 'integer'), ('conversions', 'integer'),
        ('total_impressions', 'integer'), ('total_clicks', 'integer'),
        ('total_conversions', 'integer'), ('orders_from_campaign', 'long'),
        ('days_active', 'integer'), ('end_date', 'date'), ('budget', 'double'),
        ('spent_amount', 'double'), ('target_audience', 'string'),
        ('campaign_duration_days', 'integer'), ('campaign_roi', 'double'),
        ('click_through_rate', 'double'), ('conversion_rate', 'double'),
        ('cost_per_conversion', 'double'), ('cost_per_click', 'double'),
        ('campaign_efficiency_score', 'double'), ('revenue_generated', 'double'),
        ('total_budget', 'double'), ('total_spent', 'double'),
        ('budget_utilization_rate', 'double'), ('ctr', 'double'), ('roi', 'double'),
        ('roas', 'double'), ('avg_order_value', 'double'),
        ('revenue_per_impression', 'double'), ('revenue_per_click', 'double'),
        ('campaign_profit', 'double'), ('cost_efficiency_ratio', 'double'),
        ('engagement_rate', 'double'), ('campaign_status_derived', 'string'),
        ('days_until_end', 'integer'), ('performance_tier', 'string'), ('budget_status', 'string')
    ],
    'agg_order_items': [
        ('order_item_id', 'string'), ('order_id', 'string'), ('product_id', 'string'),
        ('quantity', 'integer'), ('discount_amount', 'double'), ('product_price', 'double')
    ],
    'agg_orders': [
        ('order_id', 'string'), ('customer_id', 'string'), ('order_status', 'string'),
        ('order_placed_at', 'timestamp'), ('order_placed_year', 'integer'),
        ('order_placed_month', 'integer'), ('order_placed_quarter', 'integer'),
        ('order_placed_day_of_week', 'integer'), ('order_placed_week_of_year', 'integer'),
        ('order_placed_day_of_month', 'integer'), ('subtotal', 'double'),
        ('tax_amount', 'double'), ('shipping_cost', 'double'),
        ('total_discount', 'double'), ('total_amount', 'double'), ('currency', 'string'),
        ('order_shipped_at', 'date'), ('order_delivered_at', 'date'),
        ('order_shipped_year', 'integer'), ('order_shipped_month', 'integer'),
        ('order_shipped_quarter', 'integer'), ('order_shipped_day_of_week', 'integer'),
        ('order_shipped_week_of_year', 'integer'), ('order_shipped_day_of_month', 'integer'),
        ('order_delivered_year', 'integer'), ('order_delivered_month', 'integer'),
        ('order_delivered_quarter', 'integer'), ('order_delivered_day_of_week', 'integer'),
        ('order_delivered_week_of_year', 'integer'), ('order_delivered_day_of_month', 'integer'),
        ('order_processing_seconds_diff', 'long'), ('order_processing_minutes_diff', 'long'),
        ('order_processing_hours_diff', 'long'), ('order_processing_days_diff', 'integer'),
        ('order_processing_weeks_diff', 'double'), ('order_processing_months_diff', 'double'),
        ('order_processing_years_diff', 'double'), ('delivery_seconds_diff', 'long'),
        ('delivery_minutes_diff', 'long'), ('delivery_hours_diff', 'long'),
        ('delivery_days_diff', 'integer'), ('delivery_weeks_diff', 'double'),
        ('delivery_months_diff', 'double'), ('delivery_years_diff', 'double'),
        ('total_order_fulfillment_time_seconds', 'long'), ('total_order_fulfillment_time_minutes', 'long'),
        ('total_order_fulfillment_time_hours', 'long'), ('total_order_fulfillment_time_days', 'integer'),
        ('total_order_fulfillment_time_weeks', 'double'), ('total_order_fulfillment_time_months', 'double'),
        ('total_order_fulfillment_time_years', 'double'), ('total_product_price', 'double'),
        ('total_quantity', 'integer'), ('avg_product_price', 'double'),
        ('max_item_discount', 'double'), ('unique_products_ordered', 'integer'),
        ('order_profit', 'double'), ('net_revenue', 'double'), ('net_profit', 'double'),
        ('total_discount_from_items', 'double'), ('discount_percentage', 'double'),
        ('average_item_value', 'double'), ('cost_per_item', 'double'),
        ('order_size_category', 'string'), ('season', 'string')
    ],
    'agg_payments': [
        ('payment_id', 'string'), ('order_id', 'string'), ('payment_method', 'string'),
        ('payment_status', 'string'), ('payment_date', 'date'),
        ('payment_provider', 'string'), ('transaction_id', 'string'),
        ('processing_fee', 'double'), ('refund_amount', 'double'), ('refund_date', 'date')
    ],
    'agg_products': [
        ('product_id', 'string'), ('product_name', 'string'), ('sku', 'string'), ('category', 'string'),
        ('current_stock_level', 'integer'), ('total_units_sold', 'long'),
        ('total_orders', 'long'), ('unique_customers', 'long'),
        ('total_reviews', 'long'), ('total_wishlist_adds', 'long'),
        ('total_cart_adds', 'long'), ('stockout_occurrences', 'long'),
        ('products_in_category', 'long'), ('current_stock', 'integer'),
        ('sub_category', 'string'), ('brand', 'string'), ('supplier_id', 'string'),
        ('cost_price', 'double'), ('sell_price', 'double'), ('launch_date', 'date'),
        ('weight', 'double'), ('dimensions', 'string'), ('color', 'string'),
        ('size', 'string'), ('material', 'string'), ('profit_margin', 'double'),
        ('total_revenue', 'double'), ('total_profit', 'double'),
        ('avg_profit_margin', 'double'), ('avg_quantity_per_order', 'double'),
        ('avg_discount_amount', 'double'), ('avg_rating', 'double'),
        ('rating_std_dev', 'double'), ('positive_review_rate', 'double'),
        ('wishlist_to_purchase_rate', 'double'), ('cart_to_purchase_rate', 'double'),
        ('avg_restock_frequency', 'double'), ('view_to_purchase_rate', 'double'),
        ('revenue_per_view', 'double'), ('days_since_launch', 'integer'),
        ('stockout_days', 'long'), ('product_performance_score', 'double'),
        ('inventory_turnover_rate', 'double'), ('avg_order_value_product', 'double'),
        ('customer_penetration', 'double'), ('category_total_revenue', 'double'),
        ('category_avg_rating', 'double'), ('product_category_revenue_share', 'double'),
        ('revenue_share_percentage', 'double'), ('avg_category_growth_rate', 'double'),
        ('category_performance_tier', 'string'), ('stock_status', 'string'),
        ('days_of_supply', 'double'), ('reorder_urgency', 'string')
    ],
    'agg_reviews': [
        ('review_id', 'string'), ('product_id', 'string'), ('customer_id', 'string'),
        ('review_date', 'timestamp'), ('rating', 'integer'),
        ('review_title', 'string'), ('review_desc', 'string'), ('review_sentiment', 'string')
    ],
    'agg_shopping_cart': [
        ('cart_id', 'string'), ('customer_id', 'string'), ('cart_status', 'string'),
        ('created_at', 'timestamp'), ('session_id', 'string'),
        ('updated_at', 'timestamp'), ('cart_abandonment_flag', 'string')
    ],
    'agg_cart_items': [
        ('cart_item_id', 'long'), ('cart_id', 'string'), ('product_id', 'string'),
        ('quantity', 'integer'), ('unit_price', 'double'), ('total_price', 'double'),
        ('added_at', 'timestamp'), ('updated_at', 'timestamp'),
        ('item_status', 'string'), ('cart_age_time', 'long')
    ],
    'agg_suppliers': [
        ('supplier_id', 'string'), ('supplier_status', 'string'),
        ('is_preferred', 'boolean'), ('is_verified', 'boolean'),
        ('total_products_supplied', 'long'), ('total_units_sold', 'long'),
        ('total_orders_fulfilled', 'long'), ('total_reviews', 'long'),
        ('total_stockouts', 'long'), ('supplier_rating', 'double'),
        ('contract_start_date', 'date'), ('contract_end_date', 'date'),
        ('city', 'string'), ('state', 'string'), ('zip_code', 'string'),
        ('country', 'string'), ('total_revenue_generated', 'double'),
        ('avg_profit_margin', 'double'), ('avg_product_rating', 'double'),
        ('total_stock_value', 'double'), ('avg_stock_quantity', 'double'),
        ('avg_restock_lead_time', 'double'), ('contract_status_flag', 'string'),
        ('days_until_contract_expiry', 'integer'), ('contract_duration_days', 'integer'),
        ('revenue_per_product', 'double'), ('avg_order_value', 'double'),
        ('avg_units_per_product', 'double'), ('supplier_performance_score', 'double'),
        ('stock_efficiency_ratio', 'double'), ('supplier_reliability_score', 'double'),
        ('stockout_rate', 'double'), ('supplier_inventory_health_score', 'double')
    ],
    'agg_wishlist': [
        ('wishlist_id', 'string'), ('customer_id', 'string'), ('product_id', 'string'),
        ('added_date', 'date'), ('purchased_date', 'date'),
        ('removed_date', 'date'), ('wishlist_to_purchase_time', 'long')
    ],
    'agg_categories': [
        ('category', 'string'), ('total_products_in_category', 'long'),
        ('total_units_sold', 'long'), ('total_orders', 'long'),
        ('unique_customers', 'long'), ('total_reviews', 'long'),
        ('revenue_rank', 'integer'), ('rating_rank', 'integer'),
        ('growth_rank', 'integer'), ('total_revenue', 'double'),
        ('avg_items_per_order', 'double'), ('avg_product_price', 'double'),
        ('avg_rating', 'double'), ('avg_category_growth_rate', 'double'),
        ('revenue_share_percentage', 'double'), ('avg_order_value', 'double'),
        ('revenue_per_customer', 'double'), ('avg_units_per_customer', 'double'),
        ('category_popularity_score', 'double'), ('product_diversity_index', 'double'),
        ('avg_orders_per_product', 'double'), ('peak_season', 'double'),
        ('seasonal_index_fall', 'double'), ('seasonal_index_winter', 'double'),
        ('seasonal_index_spring', 'double'), ('seasonal_index_summer', 'double')
    ],
    'agg_daily_aggregations': [
        ('order_date', 'date'), ('order_year', 'integer'), ('order_month', 'integer'),
        ('total_orders', 'long'), ('total_customers', 'long'),
        ('new_customers', 'long'), ('returning_customers', 'long'),
        ('total_units_sold', 'long'), ('total_sessions', 'long'),
        ('total_conversions', 'long'), ('prev_day_customers', 'long'),
        ('total_revenue', 'double'), ('avg_order_value', 'double'),
        ('session_to_order_rate', 'double'), ('prev_day_revenue', 'double'),
        ('revenue_growth_rate', 'double'), ('customer_retention_rate', 'double')
    ],
    'agg_weekly_aggregations': [
        ('year_week', 'string'), ('order_year', 'integer'), ('order_week', 'integer'),
        ('total_orders', 'long'), ('total_customers', 'long'),
        ('new_customers', 'long'), ('returning_customers', 'long'),
        ('total_units_sold', 'long'), ('total_sessions', 'long'),
        ('total_conversions', 'long'), ('prev_week_customers', 'long'),
        ('total_revenue', 'double'), ('avg_order_value', 'double'),
        ('session_to_order_rate', 'double'), ('prev_week_revenue', 'double'),
        ('revenue_growth_rate', 'double'), ('customer_retention_rate', 'double')
    ],
    'agg_monthly_aggregations': [
        ('year_month', 'string'), ('order_year', 'integer'), ('order_month', 'integer'),
        ('total_orders', 'long'), ('total_customers', 'long'),
        ('new_customers', 'long'), ('returning_customers', 'long'),
        ('total_units_sold', 'long'), ('total_sessions', 'long'),
        ('total_conversions', 'long'), ('prev_month_customers', 'long'),
        ('total_revenue', 'double'), ('avg_order_value', 'double'),
        ('session_to_order_rate', 'double'), ('prev_month_revenue', 'double'),
        ('revenue_growth_rate', 'double'), ('customer_retention_rate', 'double'),
        ('churn_rate', 'double')
    ],
    'agg_country_aggregations': [
        ('country', 'string'), ('total_customers', 'long'), ('total_orders', 'long'),
        ('total_suppliers', 'long'), ('total_revenue', 'double'),
        ('avg_order_value', 'double'), ('avg_customer_lifetime_value', 'double'),
        ('preferred_category', 'string'), ('revenue_per_customer', 'double'),
        ('orders_per_customer', 'double')
    ],
    'agg_state_aggregations': [
        ('country', 'string'), ('state_province', 'string'),
        ('total_customers', 'long'), ('total_orders', 'long'),
        ('total_suppliers', 'long'), ('total_revenue', 'double'),
        ('avg_order_value', 'double'), ('avg_customer_lifetime_value', 'double'),
        ('preferred_category', 'string'), ('revenue_per_customer', 'double'),
        ('orders_per_customer', 'double')
    ],
    'agg_city_aggregations': [
        ('country', 'string'), ('state_province', 'string'), ('city', 'string'),
        ('total_customers', 'long'), ('total_orders', 'long'),
        ('total_suppliers', 'long'), ('customer_density', 'long'),
        ('total_revenue', 'double'), ('avg_order_value', 'double'),
        ('avg_customer_lifetime_value', 'double'), ('preferred_category', 'string'),
        ('revenue_per_customer', 'double'), ('orders_per_customer', 'double')
    ],
    'agg_cart_abandonment_analysis': [
        ('cart_id', 'string'), ('cart_status', 'string'), ('cart_added_date', 'date'),
        ('customer_id', 'string'), ('cart_items_count', 'long'),
        ('session_converted', 'integer'), ('time_in_cart_days', 'integer'),
        ('time_in_cart_hours', 'integer'), ('recovery_potential_score', 'integer'),
        ('cart_total_value', 'double'), ('cart_avg_item_price', 'double'),
        ('device_used', 'string'), ('abandoned_cart_category', 'string'),
        ('first_added_date', 'date'), ('last_added_date', 'date'),
        ('session_id', 'string'), ('cart_status_derived', 'string'),
        ('cart_abandonment_reason', 'string'), ('cart_value_tier', 'string'),
        ('cart_size_category', 'string'), ('abandonment_risk_score', 'double')
    ],
    'agg_product_inventory_health': [
        ('product_id', 'string'), ('supplier_id', 'string'),
        ('current_stock', 'integer'), ('available_stock', 'integer'),
        ('reorder_point_breach_count', 'long'), ('stockout_frequency', 'long'),
        ('reserved_quantity', 'integer'), ('minimum_stock_level', 'integer'),
        ('stock_health_score', 'integer'), ('avg_stock_quantity', 'double'),
        ('storage_cost_per_unit', 'double'), ('last_restock_date', 'date'),
        ('storage_cost', 'double'), ('cost_price', 'double'),
        ('avg_daily_sales', 'double'), ('total_cogs', 'double'),
        ('stock_status', 'string'), ('days_of_supply', 'double'),
        ('inventory_turnover_ratio', 'double'), ('days_since_restock', 'integer'),
        ('reorder_urgency', 'string')
    ],
    'agg_supplier_inventory_health': [
        ('supplier_id', 'string'), ('total_products', 'long'),
        ('total_current_stock', 'long'), ('total_available_stock', 'long'),
        ('total_reorder_breaches', 'long'), ('total_stockouts', 'long'),
        ('total_storage_cost', 'double'), ('avg_stock_per_product', 'double'),
        ('last_restock_date', 'date'), ('stockout_rate', 'double'),
        ('breach_rate', 'double'), ('avg_storage_cost_per_unit', 'double'),
        ('days_since_last_restock', 'integer'), ('supplier_inventory_health_score', 'double')
    ],
    'agg_rfm_segmentation': [
        ('customer_id', 'string'), ('total_orders_rfm', 'long'),
        ('days_since_last_order', 'integer'), ('total_revenue_rfm', 'double'),
        ('recency_score', 'integer'), ('frequency_score', 'integer'),
        ('monetary_score', 'integer'), ('rfm_segment', 'string'),
        ('customer_segment_label', 'string'), ('rfm_overall_score', 'double'),
        ('rfm_category', 'string'), ('engagement_level', 'string'),
        ('purchase_behavior', 'string'), ('spending_pattern', 'string'),
        ('churn_risk', 'string')
    ],
    'agg_rfm_segment_summary': [
        ('customer_segment_label', 'string'), ('customer_count', 'long'),
        ('avg_revenue', 'double'), ('avg_orders', 'double'),
        ('avg_days_since_order', 'double'), ('avg_rfm_score', 'double')
    ],
    'agg_product_affinity': [
        ('product_a_id', 'string'), ('product_b_id', 'string'),
        ('co_occurrence_count', 'long'), ('product_a_count', 'long'),
        ('product_b_count', 'long'), ('is_cross_category', 'boolean'),
        ('support', 'double'), ('confidence_a_to_b', 'double'),
        ('confidence_b_to_a', 'double'), ('prob_b', 'double'),
        ('prob_a', 'double'), ('lift_a_to_b', 'double'),
        ('lift_b_to_a', 'double'), ('avg_lift', 'double'),
        ('product_a_name', 'string'), ('product_a_category', 'string'),
        ('product_b_name', 'string'), ('product_b_category', 'string'),
        ('affinity_strength', 'string'), ('affinity_score', 'double')
    ],
    'agg_top_product_pairs': [
        ('product_a_id', 'string'), ('product_b_id', 'string'),
        ('co_occurrence_count', 'long'), ('product_a_count', 'long'),
        ('product_b_count', 'long'), ('is_cross_category', 'boolean'),
        ('support', 'double'), ('confidence_a_to_b', 'double'),
        ('confidence_b_to_a', 'double'), ('prob_b', 'double'),
        ('prob_a', 'double'), ('lift_a_to_b', 'double'),
        ('lift_b_to_a', 'double'), ('avg_lift', 'double'),
        ('product_a_name', 'string'), ('product_a_category', 'string'),
        ('product_b_name', 'string'), ('product_b_category', 'string'),
        ('affinity_strength', 'string'), ('affinity_score', 'double')
    ],
    'agg_product_recommendations': [
        ('product_a_id', 'string'), ('recommendation_count', 'long'),
        ('product_a_name', 'string'), ('recommended_products', 'string'),
        ('avg_affinity_score', 'double')
    ],
    'agg_category_affinity': [
        ('product_a_category', 'string'), ('product_b_category', 'string'),
        ('pair_count', 'long'), ('total_co_occurrences', 'long'),
        ('avg_lift_between_categories', 'double'), ('avg_support', 'double')
    ],
    'agg_global_aggregations': [
        ('metric_name', 'string'), ('calculated_at', 'string'),
        ('metric_value', 'double')
    ]
}


def get_spark_type(type_string: str):
    """Convert type string to Spark DataType."""
    type_map = {
        'string': StringType(),
        'integer': IntegerType(),
        'long': LongType(),
        'double': DoubleType(),
        'boolean': BooleanType(),
        'date': DateType(),
        'timestamp': TimestampType(),
    }
    return type_map.get(type_string.lower(), StringType())


def enforce_schema_with_types(df: DataFrame, schema_def: List[Tuple[str, str]], 
                              preserve_types: bool = True) -> DataFrame:
    """Enforce schema with column types for Parquet export.
    
    This function handles duplicate columns by renaming them temporarily,
    then selecting only the first occurrence of each column, and finally
    enforcing the expected schema.
    
    Args:
        df: Input DataFrame
        schema_def: List of (column_name, type_string) tuples defining expected schema
        preserve_types: If True, cast columns to expected types
        
    Returns:
        DataFrame with enforced schema
    """
    
    # Step 1: Handle duplicate columns by renaming them
    all_columns = df.columns
    
    # Check if there are any duplicates
    if len(all_columns) != len(set(all_columns)):
        # Rename duplicate columns with a suffix
        renamed_cols = []
        seen_count = {}
        
        for col_name in all_columns:
            if col_name not in seen_count:
                seen_count[col_name] = 0
                renamed_cols.append(col_name)
            else:
                seen_count[col_name] += 1
                renamed_cols.append(f"{col_name}_dup_{seen_count[col_name]}")
        
        # Rename all columns in the DataFrame
        df = df.toDF(*renamed_cols)
        
        # Now select only the non-duplicate columns (keeping first occurrence)
        seen = set()
        cols_to_keep = []
        final_col_names = []
        
        for new_col in renamed_cols:
            # Extract the base name (remove _dup_N suffix if present)
            base_name = new_col.split('_dup_')[0]
            if base_name not in seen:
                seen.add(base_name)
                cols_to_keep.append(new_col)
                final_col_names.append(base_name)
        
        # Select the columns to keep
        df = df.select(*cols_to_keep)
        
        # Rename back to original names
        df = df.toDF(*final_col_names)
    
    current_columns = set(df.columns)
    expected_columns = [col_name for col_name, _ in schema_def]
    
    # Step 2: Remove extra columns not in schema
    extra_columns = current_columns - set(expected_columns)
    if extra_columns:
        df = df.drop(*extra_columns)
    
    # Step 3: Add missing columns with appropriate NULL type
    current_columns = set(df.columns)  # Refresh after drop
    for col_name, col_type in schema_def:
        if col_name not in current_columns:
            spark_type = get_spark_type(col_type)
            df = df.withColumn(col_name, lit(None).cast(spark_type))
    
    # Step 4: Reorder and optionally cast columns to expected types
    select_exprs = []
    for col_name, col_type in schema_def:
        if preserve_types:
            spark_type = get_spark_type(col_type)
            select_exprs.append(df[col_name].cast(spark_type).alias(col_name))
        else:
            select_exprs.append(df[col_name])
    
    return df.select(*select_exprs)


def get_expected_schema(table_name: str, sql_schema_path: Optional[str] = None, 
                       with_types: bool = True) -> Optional[List[Tuple[str, str]]]:
    """Get expected schema for a table."""
    schema = SCHEMAS.get(table_name)
    
    if schema is None:
        return None
    
    if with_types:
        return schema
    else:
        return [col_name for col_name, _ in schema]


def get_expected_columns(table_name: str, sql_schema_path: Optional[str] = None) -> Optional[List[str]]:
    """Get expected column names for a table."""
    schema = get_expected_schema(table_name, sql_schema_path, with_types=False)
    return schema if schema else None