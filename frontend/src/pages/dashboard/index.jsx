import { useState, useRef, useEffect, useMemo, useCallback } from 'react';
import Sidebar from './Sidebar';
import Heading from '@/components/global/Typography/Heading';
import Text from '@/components/global/Typography/Text';
import { InputText } from 'primereact/inputtext';
import { Button } from 'primereact/button';
import { Dropdown } from 'primereact/dropdown';
import { Dialog } from 'primereact/dialog';
import { Toast } from 'primereact/toast';
import PrimaryButton from '@/components/global/Button/PrimaryButton';
import SecondaryButton from '@/components/global/Button/SecondaryButton';
import { useAuth } from '@/context/AuthContext';
import { usePipelineProgress } from '@/context/PipelineProgressContext';
import { CurrencyProvider } from '@/context/CurrencyContext';
import axiosInstance from '@/services/api/axiosInstance';
import { useNavigate, useParams, useLocation } from 'react-router-dom';
import InlinePipelineProgress from '@/components/global/InlinePipelineProgress';
import ExecutiveOverview from './analytics/pages/ExecutiveOverview';
import CustomerOverview from './analytics/pages/CustomerOverview';
import CustomerSegmentation from './analytics/pages/CustomerSegmentation';
import CustomerHealthRetention from './analytics/pages/CustomerHealthRetention';
import CustomerValueAnalysis from './analytics/pages/CustomerValueAnalysis';
import ProductPerformance from './analytics/pages/ProductPerformance';
import ProductProfitability from './analytics/pages/ProductProfitability';
import ProductEngagement from './analytics/pages/ProductEngagement';
import ProductTrends from './analytics/pages/ProductTrends';
import InventoryHealth from './analytics/pages/InventoryHealth';
import InventoryReorderManagement from './analytics/pages/InventoryReorderManagement';
import InventoryEfficiency from './analytics/pages/InventoryEfficiency';
import InventorySupplier from './analytics/pages/InventorySupplier';
import SupplierPerformance from './analytics/pages/SupplierPerformance';
import SupplierOperations from './analytics/pages/SupplierOperations';
import SupplierEconomics from './analytics/pages/SupplierEconomics';
import MarketingCampaigns from './analytics/pages/MarketingCampaigns';
import MarketingAttribution from './analytics/pages/MarketingAttribution';
import MarketingChannels from './analytics/pages/MarketingChannels';
import FunnelOverview from './analytics/pages/FunnelOverview';
import FunnelCart from './analytics/pages/FunnelCart';
import FunnelCheckout from './analytics/pages/FunnelCheckout';
import FunnelWishlist from './analytics/pages/FunnelWishlist';
import PaymentMethods from './analytics/pages/PaymentMethods';
import PaymentRefunds from './analytics/pages/PaymentRefunds';
import PaymentFinancialMetrics from './analytics/pages/PaymentFinancialMetrics';
import OperationsProcessing from './analytics/pages/OperationsProcessing';
import OperationsDelivery from './analytics/pages/OperationsDelivery';
import OperationsShipping from './analytics/pages/OperationsShipping';
import RecommendationsProductAffinity from './analytics/pages/RecommendationsProductAffinity';
import RecommendationsCategoryAffinity from './analytics/pages/RecommendationsCategoryAffinity';
import RecommendationsCoverage from './analytics/pages/RecommendationsCoverage';
import ReviewsOverview from './analytics/pages/ReviewsOverview';
import ReviewsSentiment from './analytics/pages/ReviewsSentiment';
import ReviewsImpact from './analytics/pages/ReviewsImpact';
import EngagementMetrics from './analytics/pages/EngagementMetrics';
import EngagementBehavior from './analytics/pages/EngagementBehavior';
import EngagementConversion from './analytics/pages/EngagementConversion';
import Forecasts from './analytics/pages/Forecasts';
import ExplainableAI from './analytics/pages/ExplainableAI';
import ExportAnalytics from './analytics/pages/ExportAnalytics';
import usePageTitle from '@/hooks/usePageTitle';

// ---------------------------------------------------------------------------
// Insight Catalog — every insight mapped to a page route segment
// ---------------------------------------------------------------------------

const INSIGHT_CATALOG = [
    // Executive Overview
    { key: 'business_health_daily', title: 'Business Health (Daily)', category: 'KPIs', page: '', section: 'Executive Overview' },
    { key: 'business_health_weekly', title: 'Business Health (Weekly)', category: 'KPIs', page: '', section: 'Executive Overview' },
    { key: 'business_health_monthly', title: 'Business Health (Monthly)', category: 'KPIs', page: '', section: 'Executive Overview' },
    { key: 'clv_summary', title: 'Customer Lifetime Value Summary', category: 'KPIs', page: '', section: 'Executive Overview' },
    { key: 'funnel_summary', title: 'Funnel Summary', category: 'KPIs', page: '', section: 'Executive Overview' },
    { key: 'cart_abandon_summary', title: 'Cart Abandonment Summary', category: 'KPIs', page: '', section: 'Executive Overview' },
    { key: 'session_to_order_analysis', title: 'Session to Order Analysis', category: 'KPIs', page: '', section: 'Executive Overview' },
    { key: 'customer_engagement_summary', title: 'Customer Engagement Summary', category: 'KPIs', page: '', section: 'Executive Overview' },
    // Customer Overview
    { key: 'customer_account_status_distribution_daily', title: 'Account Status Distribution (Daily)', category: 'Customers', page: 'customers/overview', section: 'Customer Overview' },
    { key: 'customer_account_status_distribution_weekly', title: 'Account Status Distribution (Weekly)', category: 'Customers', page: 'customers/overview', section: 'Customer Overview' },
    { key: 'customer_account_status_distribution_monthly', title: 'Account Status Distribution (Monthly)', category: 'Customers', page: 'customers/overview', section: 'Customer Overview' },
    { key: 'new_customers_daily', title: 'New Customers (Daily)', category: 'Customers', page: 'customers/overview', section: 'Customer Overview' },
    { key: 'new_customers_weekly', title: 'New Customers (Weekly)', category: 'Customers', page: 'customers/overview', section: 'Customer Overview' },
    { key: 'new_customers_monthly', title: 'New Customers (Monthly)', category: 'Customers', page: 'customers/overview', section: 'Customer Overview' },
    { key: 'cumulative_customers_daily', title: 'Cumulative Customers (Daily)', category: 'Customers', page: 'customers/overview', section: 'Customer Overview' },
    { key: 'cumulative_customers_weekly', title: 'Cumulative Customers (Weekly)', category: 'Customers', page: 'customers/overview', section: 'Customer Overview' },
    { key: 'cumulative_customers_monthly', title: 'Cumulative Customers (Monthly)', category: 'Customers', page: 'customers/overview', section: 'Customer Overview' },
    { key: 'new_customers_geo_acquisition_daily', title: 'Geo Acquisition by New Customers (Daily)', category: 'Customers', page: 'customers/overview', section: 'Customer Overview' },
    { key: 'new_customers_geo_acquisition_monthly', title: 'Geo Acquisition by New Customers (Monthly)', category: 'Customers', page: 'customers/overview', section: 'Customer Overview' },
    { key: 'customer_age_group_distribution', title: 'Customer Age Group Distribution', category: 'Customers', page: 'customers/overview', section: 'Customer Overview' },
    { key: 'customer_city_distribution', title: 'Customer City Distribution', category: 'Customers', page: 'customers/overview', section: 'Customer Overview' },
    { key: 'customer_state_distribution', title: 'Customer State Distribution', category: 'Customers', page: 'customers/overview', section: 'Customer Overview' },
    { key: 'customer_country_distribution', title: 'Customer Country Distribution', category: 'Customers', page: 'customers/overview', section: 'Customer Overview' },
    { key: 'customer_age_group_spending', title: 'Age Group Spending Analysis', category: 'Customers', page: 'customers/overview', section: 'Customer Overview' },
    { key: 'new_vs_returning_customer_country', title: 'New vs Returning by Country', category: 'Customers', page: 'customers/overview', section: 'Customer Overview' },
    { key: 'new_vs_returning_customer_city', title: 'New vs Returning by City', category: 'Customers', page: 'customers/overview', section: 'Customer Overview' },
    { key: 'new_vs_returning_customer_state', title: 'New vs Returning by State', category: 'Customers', page: 'customers/overview', section: 'Customer Overview' },
    { key: 'geo_acquisition', title: 'Geographic Customer Acquisition', category: 'Customers', page: 'customers/overview', section: 'Customer Overview' },
    // Customer Segmentation
    { key: 'customer_engagement', title: 'Customer Engagement', category: 'Customers', page: 'customers/segmentation', section: 'Customer Segmentation' },
    { key: 'session_conversion_distribution', title: 'Session Conversion Distribution', category: 'Customers', page: 'customers/segmentation', section: 'Customer Segmentation' },
    { key: 'cart_abandonment_distribution', title: 'Cart Abandonment Distribution', category: 'Customers', page: 'customers/segmentation', section: 'Customer Segmentation' },
    { key: 'rfm_segment_summary', title: 'RFM Segment Summary', category: 'Customers', page: 'customers/segmentation', section: 'Customer Segmentation' },
    { key: 'rfm_churn_crosstab', title: 'RFM vs Churn Cross-tab', category: 'Customers', page: 'customers/segmentation', section: 'Customer Segmentation' },
    { key: 'seg_referrer_crosstab', title: 'Segment vs Referrer Cross-tab', category: 'Customers', page: 'customers/segmentation', section: 'Customer Segmentation' },
    { key: 'seg_device_crosstab', title: 'Segment vs Device Cross-tab', category: 'Customers', page: 'customers/segmentation', section: 'Customer Segmentation' },
    { key: 'payment_method_vs_clv_churn', title: 'Payment Method vs CLV & Churn', category: 'Customers', page: 'customers/segmentation', section: 'Customer Segmentation' },
    { key: 'payment_method_summary', title: 'Payment Method Summary', category: 'Customers', page: 'customers/segmentation', section: 'Customer Segmentation' },
    { key: 'referrer_source_summary', title: 'Referrer Source Summary', category: 'Customers', page: 'customers/segmentation', section: 'Customer Segmentation' },
    { key: 'referrer_churn_summary', title: 'Referrer vs Churn Summary', category: 'Customers', page: 'customers/segmentation', section: 'Customer Segmentation' },
    { key: 'gender_category_preference', title: 'Gender × Category Preference', category: 'Customers', page: 'customers/segmentation', section: 'Customer Segmentation' },
    { key: 'gender_product_preference', title: 'Gender × Product Preference', category: 'Customers', page: 'customers/segmentation', section: 'Customer Segmentation' },
    // Customer Health & Retention
    { key: 'churn_risk_summary', title: 'Churn Risk Summary', category: 'Customers', page: 'customers/health', section: 'Customer Health & Retention' },
    { key: 'high_clv_at_risk', title: 'High CLV Customers At Risk', category: 'Customers', page: 'customers/health', section: 'Customer Health & Retention' },
    { key: 'signup_cohort_summary', title: 'Signup Cohort Summary', category: 'Customers', page: 'customers/health', section: 'Customer Health & Retention' },
    { key: 'customer_cohort_retention', title: 'Customer Cohort Retention', category: 'Customers', page: 'customers/health', section: 'Customer Health & Retention' },
    { key: 'high_intent_non_buyers', title: 'High Intent Non-Buyers', category: 'Customers', page: 'customers/health', section: 'Customer Health & Retention' },
    { key: 'customers_cohorts', title: 'Customer Cohorts', category: 'Customers', page: 'customers/health', section: 'Customer Health & Retention' },
    // Customer Value Analysis
    { key: 'top_customers_by_revenue', title: 'Top Customers by Revenue', category: 'Customers', page: 'customers/value', section: 'Customer Value Analysis' },
    { key: 'top_customers_by_profit', title: 'Top Customers by Profit', category: 'Customers', page: 'customers/value', section: 'Customer Value Analysis' },
    { key: 'discount_customers_summary', title: 'Discount Customers Summary', category: 'Customers', page: 'customers/value', section: 'Customer Value Analysis' },
    { key: 'discount_customers', title: 'Discount Customers Detail', category: 'Customers', page: 'customers/value', section: 'Customer Value Analysis' },
    { key: 'correlation_discount_vs_clv', title: 'Discount vs CLV Correlation', category: 'Customers', page: 'customers/value', section: 'Customer Value Analysis' },
    { key: 'high_discount_customers', title: 'High Discount Customers', category: 'Customers', page: 'customers/value', section: 'Customer Value Analysis' },
    { key: 'cart_behavior_summary', title: 'Cart Behavior Summary', category: 'Customers', page: 'customers/value', section: 'Customer Value Analysis' },
    { key: 'high_value_abandoners', title: 'High Value Cart Abandoners', category: 'Customers', page: 'customers/value', section: 'Customer Value Analysis' },
    { key: 'customer_profit_per_segment', title: 'Customer Profit per Segment', category: 'Customers', page: 'customers/value', section: 'Customer Value Analysis' },
    { key: 'customer_overall_health_summary', title: 'Customer Overall Health Summary', category: 'Customers', page: 'customers/value', section: 'Customer Value Analysis' },
    // Product Performance
    { key: 'best_selling_products', title: 'Best Selling Products', category: 'Products', page: 'products/performance', section: 'Product Performance' },
    { key: 'out_of_stock_products', title: 'Out of Stock Products', category: 'Products', page: 'products/performance', section: 'Product Performance' },
    { key: 'low_conversion_products', title: 'Low Conversion Products', category: 'Products', page: 'products/performance', section: 'Product Performance' },
    { key: 'product_rating_summary', title: 'Product Rating Summary', category: 'Products', page: 'products/performance', section: 'Product Performance' },
    { key: 'category_view_patterns', title: 'Category View Patterns', category: 'Products', page: 'products/performance', section: 'Product Performance' },
    { key: 'top_view_to_purchase_products', title: 'Top View-to-Purchase Products', category: 'Products', page: 'products/performance', section: 'Product Performance' },
    { key: 'product_performance_score', title: 'Product Performance Score', category: 'Products', page: 'products/performance', section: 'Product Performance' },
    // Product Profitability
    { key: 'highest_margin_products', title: 'Highest Margin Products', category: 'Products', page: 'products/profitability', section: 'Product Profitability' },
    { key: 'low_margin_high_traffic_products', title: 'Low Margin High Traffic Products', category: 'Products', page: 'products/profitability', section: 'Product Profitability' },
    { key: 'category_revenue_share', title: 'Category Revenue Share', category: 'Products', page: 'products/profitability', section: 'Product Profitability' },
    { key: 'low_performing_categories', title: 'Low Performing Categories', category: 'Products', page: 'products/profitability', section: 'Product Profitability' },
    { key: 'category_popularity_score', title: 'Category Popularity Score', category: 'Products', page: 'products/profitability', section: 'Product Profitability' },
    { key: 'category_profitability', title: 'Category Profitability', category: 'Products', page: 'products/profitability', section: 'Product Profitability' },
    // Product Engagement
    { key: 'product_lifecycle_segments', title: 'Product Lifecycle Segments', category: 'Products', page: 'products/engagement', section: 'Product Engagement' },
    { key: 'product_lifecycle_summary', title: 'Product Lifecycle Summary', category: 'Products', page: 'products/engagement', section: 'Product Engagement' },
    { key: 'supplier_product_performance', title: 'Supplier Product Performance', category: 'Products', page: 'products/engagement', section: 'Product Engagement' },
    { key: 'stockout_rate_by_product', title: 'Stockout Rate by Product', category: 'Products', page: 'products/engagement', section: 'Product Engagement' },
    { key: 'supplier_stockout_impact_on_products', title: 'Supplier Stockout Impact on Products', category: 'Products', page: 'products/engagement', section: 'Product Engagement' },
    // Product Trends
    { key: 'product_monthly_trends', title: 'Product Monthly Trends', category: 'Products', page: 'products/trends', section: 'Product Trends' },
    { key: 'category_monthly_trends', title: 'Category Monthly Trends', category: 'Products', page: 'products/trends', section: 'Product Trends' },
    { key: 'product_calendar_month_seasonality', title: 'Product Calendar Month Seasonality', category: 'Products', page: 'products/trends', section: 'Product Trends' },
    { key: 'category_calendar_month_seasonality', title: 'Category Calendar Month Seasonality', category: 'Products', page: 'products/trends', section: 'Product Trends' },
    { key: 'category_monthly_seasonality', title: 'Category Monthly Seasonality', category: 'Products', page: 'products/trends', section: 'Product Trends' },
    { key: 'category_peak_season', title: 'Category Peak Season', category: 'Products', page: 'products/trends', section: 'Product Trends' },
    // Inventory Health
    { key: 'product_stockout_risk', title: 'Product Stockout Risk', category: 'Inventory', page: 'inventory/health', section: 'Inventory Health' },
    { key: 'product_stockout_replenishment', title: 'Product Stockout Replenishment', category: 'Inventory', page: 'inventory/health', section: 'Inventory Health' },
    { key: 'product_dead_stock', title: 'Product Dead Stock', category: 'Inventory', page: 'inventory/health', section: 'Inventory Health' },
    { key: 'product_inventory_health', title: 'Product Inventory Health', category: 'Inventory', page: 'inventory/health', section: 'Inventory Health' },
    { key: 'product_inventory_critical', title: 'Critical Inventory Products', category: 'Inventory', page: 'inventory/health', section: 'Inventory Health' },
    // Inventory Reorder Management
    { key: 'sku_reorder_urgency', title: 'SKU Reorder Urgency', category: 'Inventory', page: 'inventory/reorder', section: 'Inventory Reorder' },
    { key: 'reorder_point_breach_frequency', title: 'Reorder Point Breach Frequency', category: 'Inventory', page: 'inventory/reorder', section: 'Inventory Reorder' },
    { key: 'inventory_stock_status', title: 'Inventory Stock Status', category: 'Inventory', page: 'inventory/reorder', section: 'Inventory Reorder' },
    { key: 'days_of_supply', title: 'Days of Supply', category: 'Inventory', page: 'inventory/reorder', section: 'Inventory Reorder' },
    // Inventory Efficiency
    { key: 'overstock_analysis', title: 'Overstock Analysis', category: 'Inventory', page: 'inventory/efficiency', section: 'Inventory Efficiency' },
    { key: 'reserved_vs_available', title: 'Reserved vs Available Stock', category: 'Inventory', page: 'inventory/efficiency', section: 'Inventory Efficiency' },
    { key: 'excess_inventory_not_selling', title: 'Excess Inventory Not Selling', category: 'Inventory', page: 'inventory/efficiency', section: 'Inventory Efficiency' },
    { key: 'margin_erosion_risk', title: 'Margin Erosion Risk', category: 'Inventory', page: 'inventory/efficiency', section: 'Inventory Efficiency' },
    { key: 'inventory_carrying_cost_by_product', title: 'Inventory Carrying Cost by Product', category: 'Inventory', page: 'inventory/efficiency', section: 'Inventory Efficiency' },
    { key: 'inventory_carrying_cost_overall', title: 'Total Inventory Carrying Cost', category: 'Inventory', page: 'inventory/efficiency', section: 'Inventory Efficiency' },
    // Inventory Supplier
    { key: 'supplier_ranking_core', title: 'Supplier Ranking (Core)', category: 'Inventory', page: 'inventory/supplier', section: 'Inventory Supplier' },
    // Supplier Performance
    { key: 'stockout_rate_by_supplier', title: 'Stockout Rate by Supplier', category: 'Suppliers', page: 'suppliers/performance', section: 'Supplier Performance' },
    { key: 'supplier_reliability', title: 'Supplier Reliability', category: 'Suppliers', page: 'suppliers/performance', section: 'Supplier Performance' },
    { key: 'supplier_revenue_contribution', title: 'Supplier Revenue Contribution', category: 'Suppliers', page: 'suppliers/performance', section: 'Supplier Performance' },
    { key: 'supplier_profit_margin', title: 'Supplier Profit Margin', category: 'Suppliers', page: 'suppliers/performance', section: 'Supplier Performance' },
    // Supplier Operations
    { key: 'supplier_fulfillment_performance', title: 'Supplier Fulfillment Performance', category: 'Suppliers', page: 'suppliers/operations', section: 'Supplier Operations' },
    { key: 'supplier_stockouts', title: 'Supplier Stockouts', category: 'Suppliers', page: 'suppliers/operations', section: 'Supplier Operations' },
    { key: 'supplier_days_since_last_restock', title: 'Supplier Days Since Last Restock', category: 'Suppliers', page: 'suppliers/operations', section: 'Supplier Operations' },
    { key: 'supplier_contract_expiry', title: 'Supplier Contract Expiry', category: 'Suppliers', page: 'suppliers/operations', section: 'Supplier Operations' },
    // Supplier Economics
    { key: 'storage_cost_efficiency_by_supplier', title: 'Storage Cost Efficiency by Supplier', category: 'Suppliers', page: 'suppliers/economics', section: 'Supplier Economics' },
    { key: 'inventory_carrying_cost_by_supplier', title: 'Inventory Carrying Cost by Supplier', category: 'Suppliers', page: 'suppliers/economics', section: 'Supplier Economics' },
    // Marketing Campaigns
    { key: 'campaign_performance_summary', title: 'Campaign Performance Summary', category: 'Marketing', page: 'marketing/campaigns', section: 'Marketing Campaigns' },
    { key: 'campaign_product_contribution', title: 'Campaign Product Contribution', category: 'Marketing', page: 'marketing/campaigns', section: 'Marketing Campaigns' },
    { key: 'campaign_ltv', title: 'Campaign LTV Analysis', category: 'Marketing', page: 'marketing/campaigns', section: 'Marketing Campaigns' },
    { key: 'campaign_customer_ltv_summary', title: 'Campaign Customer LTV Summary', category: 'Marketing', page: 'marketing/campaigns', section: 'Marketing Campaigns' },
    { key: 'campaign_wasteful_campaigns', title: 'Wasteful Campaigns', category: 'Marketing', page: 'marketing/campaigns', section: 'Marketing Campaigns' },
    { key: 'campaign_margin_profile', title: 'Campaign Margin Profile', category: 'Marketing', page: 'marketing/campaigns', section: 'Marketing Campaigns' },
    { key: 'campaign_performance', title: 'Campaign Performance (Tiered)', category: 'Marketing', page: 'marketing/channels', section: 'Marketing Channels' },
    // Conversion Funnel
    { key: 'high_value_funnel', title: 'High Value Funnel Sessions', category: 'Funnel', page: 'funnel/overview', section: 'Funnel Overview' },
    { key: 'high_value_vs_regular', title: 'High Value vs Regular Sessions', category: 'Funnel', page: 'funnel/overview', section: 'Funnel Overview' },
    { key: 'funnel_by_device', title: 'Funnel by Device', category: 'Funnel', page: 'funnel/overview', section: 'Funnel Overview' },
    { key: 'funnel_by_referrer', title: 'Funnel by Referrer', category: 'Funnel', page: 'funnel/overview', section: 'Funnel Overview' },
    { key: 'abandoned_vs_converted', title: 'Abandoned vs Converted', category: 'Funnel', page: 'funnel/cart', section: 'Cart Analysis' },
    { key: 'checkout_dropoff_reasons', title: 'Checkout Drop-off Reasons', category: 'Funnel', page: 'funnel/checkout', section: 'Checkout' },
    { key: 'checkout_dropoff_buckets', title: 'Checkout Drop-off Buckets', category: 'Funnel', page: 'funnel/checkout', section: 'Checkout' },
    { key: 'checkout_dropoff_by_device_and_reason', title: 'Drop-off by Device & Reason', category: 'Funnel', page: 'funnel/checkout', section: 'Checkout' },
    { key: 'device_conversion_rates', title: 'Device Conversion Rates', category: 'Funnel', page: 'funnel/checkout', section: 'Checkout' },
    // Cart Analytics
    { key: 'cart_overall_stats', title: 'Cart Overall Stats', category: 'Funnel', page: 'funnel/cart', section: 'Cart Analysis' },
    { key: 'cart_status_distribution', title: 'Cart Status Distribution', category: 'Funnel', page: 'funnel/cart', section: 'Cart Analysis' },
    { key: 'cart_value_stats', title: 'Cart Value Stats', category: 'Funnel', page: 'funnel/cart', section: 'Cart Analysis' },
    { key: 'high_value_abandoned_carts', title: 'High Value Abandoned Carts', category: 'Funnel', page: 'funnel/cart', section: 'Cart Analysis' },
    { key: 'time_to_purchase_overall', title: 'Time to Purchase (Overall)', category: 'Funnel', page: 'funnel/cart', section: 'Cart Analysis' },
    { key: 'time_to_purchase_by_tier', title: 'Time to Purchase by Tier', category: 'Funnel', page: 'funnel/cart', section: 'Cart Analysis' },
    { key: 'time_to_purchase_buckets', title: 'Time to Purchase Buckets', category: 'Funnel', page: 'funnel/cart', section: 'Cart Analysis' },
    // Wishlist Analytics
    { key: 'wishlist_overall_summary', title: 'Wishlist Summary', category: 'Funnel', page: 'funnel/wishlist', section: 'Wishlist' },
    { key: 'wishlist_by_product', title: 'Wishlist by Product', category: 'Funnel', page: 'funnel/wishlist', section: 'Wishlist' },
    { key: 'wishlist_by_customer', title: 'Wishlist by Customer', category: 'Funnel', page: 'funnel/wishlist', section: 'Wishlist' },
    { key: 'wishlist_time_to_purchase_stats', title: 'Wishlist Time to Purchase Stats', category: 'Funnel', page: 'funnel/wishlist', section: 'Wishlist' },
    { key: 'wishlist_time_to_purchase_distribution', title: 'Wishlist Time to Purchase Distribution', category: 'Funnel', page: 'funnel/wishlist', section: 'Wishlist' },
    { key: 'abandoned_wishlist_items', title: 'Abandoned Wishlist Items', category: 'Funnel', page: 'funnel/wishlist', section: 'Wishlist' },
    { key: 'abandoned_wishlist_by_customer', title: 'Abandoned Wishlist by Customer', category: 'Funnel', page: 'funnel/wishlist', section: 'Wishlist' },
    { key: 'abandoned_wishlist_by_product', title: 'Abandoned Wishlist by Product', category: 'Funnel', page: 'funnel/wishlist', section: 'Wishlist' },
    { key: 'wishlist_adds_by_month', title: 'Wishlist Adds by Month', category: 'Funnel', page: 'funnel/wishlist', section: 'Wishlist' },
    // Payments & Finance
    { key: 'payment_counts_by_country_method', title: 'Payment Counts by Country & Method', category: 'Payments', page: 'payments/methods', section: 'Payment Methods' },
    { key: 'payment_counts_by_state_method', title: 'Payment Counts by State & Method', category: 'Payments', page: 'payments/methods', section: 'Payment Methods' },
    { key: 'payment_method_success_rates', title: 'Payment Method Success Rates', category: 'Payments', page: 'payments/methods', section: 'Payment Methods' },
    { key: 'payment_method_success_rates_by_country', title: 'Payment Success Rates by Country', category: 'Payments', page: 'payments/methods', section: 'Payment Methods' },
    { key: 'payment_method_aov', title: 'Payment Method Average Order Value', category: 'Payments', page: 'payments/methods', section: 'Payment Methods' },
    { key: 'refund_rate_by_payment_method', title: 'Refund Rate by Payment Method', category: 'Payments', page: 'payments/refunds', section: 'Refunds' },
    { key: 'refund_rate_by_product', title: 'Refund Rate by Product', category: 'Payments', page: 'payments/refunds', section: 'Refunds' },
    { key: 'refund_rate_by_month', title: 'Refund Rate by Month', category: 'Payments', page: 'payments/refunds', section: 'Refunds' },
    { key: 'time_to_refund_by_payment_method', title: 'Time to Refund by Payment Method', category: 'Payments', page: 'payments/refunds', section: 'Refunds' },
    { key: 'low_margin_categories', title: 'Low Margin Categories', category: 'Payments', page: 'payments/metrics', section: 'Financial Metrics' },
    { key: 'rev_by_country_city', title: 'Revenue by Country & City', category: 'Payments', page: 'payments/metrics', section: 'Financial Metrics' },
    { key: 'rev_by_customer_segment', title: 'Revenue by Customer Segment', category: 'Payments', page: 'payments/metrics', section: 'Financial Metrics' },
    { key: 'rev_by_rfm_segment', title: 'Revenue by RFM Segment', category: 'Payments', page: 'payments/metrics', section: 'Financial Metrics' },
    { key: 'rev_by_segment_label', title: 'Revenue by Segment Label', category: 'Payments', page: 'payments/metrics', section: 'Financial Metrics' },
    { key: 'rev_by_referrer', title: 'Revenue by Referrer', category: 'Payments', page: 'payments/metrics', section: 'Financial Metrics' },
    { key: 'rev_by_device', title: 'Revenue by Device', category: 'Payments', page: 'payments/metrics', section: 'Financial Metrics' },
    { key: 'aov_trend_daily', title: 'AOV Trend (Daily)', category: 'Payments', page: 'payments/metrics', section: 'Financial Metrics' },
    { key: 'aov_trend_weekly', title: 'AOV Trend (Weekly)', category: 'Payments', page: 'payments/metrics', section: 'Financial Metrics' },
    { key: 'aov_trend_monthly', title: 'AOV Trend (Monthly)', category: 'Payments', page: 'payments/metrics', section: 'Financial Metrics' },
    { key: 'segment_aov_by_rfm', title: 'Segment AOV by RFM', category: 'Payments', page: 'payments/metrics', section: 'Financial Metrics' },
    // Operations Processing
    { key: 'processing_by_category', title: 'Processing by Category', category: 'Operations', page: 'operations/processing', section: 'Order Processing' },
    { key: 'processing_by_subcategory', title: 'Processing by Subcategory', category: 'Operations', page: 'operations/processing', section: 'Order Processing' },
    { key: 'processing_by_hour', title: 'Processing by Hour of Day', category: 'Operations', page: 'operations/processing', section: 'Order Processing' },
    { key: 'processing_by_day_of_week', title: 'Processing by Day of Week', category: 'Operations', page: 'operations/processing', section: 'Order Processing' },
    { key: 'weekend_vs_weekday', title: 'Weekend vs Weekday Processing', category: 'Operations', page: 'operations/processing', section: 'Order Processing' },
    { key: 'processing_by_season', title: 'Processing by Season', category: 'Operations', page: 'operations/processing', section: 'Order Processing' },
    { key: 'processing_by_season_and_status', title: 'Processing by Season & Status', category: 'Operations', page: 'operations/processing', section: 'Order Processing' },
    // Operations Delivery
    { key: 'delivery_days_by_country', title: 'Delivery Days by Country', category: 'Operations', page: 'operations/delivery', section: 'Delivery' },
    { key: 'delivery_days_by_state', title: 'Delivery Days by State', category: 'Operations', page: 'operations/delivery', section: 'Delivery' },
    { key: 'delivery_days_by_city', title: 'Delivery Days by City', category: 'Operations', page: 'operations/delivery', section: 'Delivery' },
    { key: 'ontime_delivery_by_country', title: 'On-Time Delivery by Country', category: 'Operations', page: 'operations/delivery', section: 'Delivery' },
    { key: 'ontime_delivery_by_state', title: 'On-Time Delivery by State', category: 'Operations', page: 'operations/delivery', section: 'Delivery' },
    { key: 'ontime_delivery_by_city', title: 'On-Time Delivery by City', category: 'Operations', page: 'operations/delivery', section: 'Delivery' },
    // Operations Shipping
    { key: 'shipping_efficiency_by_country', title: 'Shipping Efficiency by Country', category: 'Operations', page: 'operations/shipping', section: 'Shipping' },
    { key: 'shipping_efficiency_by_state', title: 'Shipping Efficiency by State', category: 'Operations', page: 'operations/shipping', section: 'Shipping' },
    { key: 'shipping_efficiency_by_city', title: 'Shipping Efficiency by City', category: 'Operations', page: 'operations/shipping', section: 'Shipping' },
    // Recommendations
    { key: 'product_affinity_pairs', title: 'Product Affinity Pairs', category: 'Recommendations', page: 'recommendations/product', section: 'Product Affinity' },
    { key: 'product_affinity_top_per_product', title: 'Top Product Affinities per Product', category: 'Recommendations', page: 'recommendations/product', section: 'Product Affinity' },
    { key: 'category_affinity_pairs', title: 'Category Affinity Pairs', category: 'Recommendations', page: 'recommendations/category', section: 'Category Affinity' },
    { key: 'category_affinity_top_per_category', title: 'Top Category Affinities per Category', category: 'Recommendations', page: 'recommendations/category', section: 'Category Affinity' },
    { key: 'precomputed_product_recommendations', title: 'Pre-computed Product Recommendations', category: 'Recommendations', page: 'recommendations/coverage', section: 'Coverage' },
    { key: 'precomputed_reco_coverage', title: 'Recommendation Coverage Rate', category: 'Recommendations', page: 'recommendations/coverage', section: 'Coverage' },
    // Reviews & Sentiment
    { key: 'review_velocity_daily', title: 'Review Velocity (Daily)', category: 'Reviews', page: 'reviews/sentiment', section: 'Review Sentiment' },
    { key: 'review_velocity_weekly', title: 'Review Velocity (Weekly)', category: 'Reviews', page: 'reviews/sentiment', section: 'Review Sentiment' },
    { key: 'review_velocity_monthly', title: 'Review Velocity (Monthly)', category: 'Reviews', page: 'reviews/sentiment', section: 'Review Sentiment' },
    { key: 'sentiment_by_category', title: 'Sentiment by Category', category: 'Reviews', page: 'reviews/sentiment', section: 'Review Sentiment' },
    { key: 'rating_tier_per_product', title: 'Rating Tier per Product', category: 'Reviews', page: 'reviews/overview', section: 'Reviews Overview' },
    { key: 'rating_tier_sales_velocity', title: 'Rating Tier Sales Velocity', category: 'Reviews', page: 'reviews/overview', section: 'Reviews Overview' },
    { key: 'product_monthly_rating_trends', title: 'Product Monthly Rating Trends', category: 'Reviews', page: 'reviews/impact', section: 'Reviews Impact' },
    { key: 'low_rated_product_monthly_trends_rating_only', title: 'Low Rated Product Trends', category: 'Reviews', page: 'reviews/impact', section: 'Reviews Impact' },
    // Engagement
    { key: 'customer_engagement_summary', title: 'Engagement Metrics Summary', category: 'Engagement', page: 'engagement/metrics', section: 'Engagement Metrics' },
    { key: 'funnel_summary', title: 'Funnel Summary (Engagement)', category: 'Engagement', page: 'engagement/metrics', section: 'Engagement Metrics' },
];

const Dashboard = () => {
    usePageTitle('Dashboard');
    const { logout, user } = useAuth();
    const { startPipeline } = usePipelineProgress();
    const [sidebarOpen, setSidebarOpen] = useState(false);
    const [selectedBusiness, setSelectedBusiness] = useState(null);
    const navigate = useNavigate();
    const [isAddBusinessLoading, setIsAddBusinessLoading] = useState(false);
    const [isDeleteBusinessLoading, setIsDeleteBusinessLoading] = useState(false);
    const [businessIngestionType, setBusinessIngestionType] = useState(null);
    const [showDeleteDialog, setShowDeleteDialog] = useState(false);

    const { businessId } = useParams();
    // NEW: State to toggle the custom profile menu
    const [isProfileOpen, setIsProfileOpen] = useState(false);
    // NEW: Ref to detect clicks outside the menu to close it
    const profileRef = useRef(null);
    // NEW: Pipeline status for streaming modes
    const [pipelineStatus, setPipelineStatus] = useState('idle');
    // NEW: Toast ref for notifications
    const toastRef = useRef(null);

    // Search Insight state
    const [searchQuery, setSearchQuery] = useState('');
    const [searchOpen, setSearchOpen] = useState(false);
    const searchRef = useRef(null);

    // Mock data
    const [businesses, setBusinesses] = useState([]);

    // Close menu when clicking outside
    useEffect(() => {
        const handleClickOutside = (event) => {
            if (profileRef.current && !profileRef.current.contains(event.target)) {
                setIsProfileOpen(false);
            }
            if (searchRef.current && !searchRef.current.contains(event.target)) {
                setSearchOpen(false);
            }
        };
        document.addEventListener('mousedown', handleClickOutside);
        return () => document.removeEventListener('mousedown', handleClickOutside);
    }, []);

    // Filtered insight results
    const searchResults = useMemo(() => {
        const q = searchQuery.trim().toLowerCase();
        if (!q) return [];
        return INSIGHT_CATALOG.filter(
            (item) =>
                item.title.toLowerCase().includes(q) ||
                item.key.toLowerCase().includes(q) ||
                item.category.toLowerCase().includes(q) ||
                item.section.toLowerCase().includes(q),
        ).slice(0, 12);
    }, [searchQuery]);

    const handleInsightSelect = useCallback((insight) => {
        const base = businessId ? `/analytics/${businessId}` : '/analytics';
        const path = insight.page ? `${base}/${insight.page}` : base;
        navigate(path);
        setSearchQuery('');
        setSearchOpen(false);
    }, [businessId, navigate]);

    const handleAddBusiness = async () => {
        setIsAddBusinessLoading(true);
        const response = await axiosInstance.post('/onboarding/create', {userId: user.user_id});
        let current_step = response.data.current_step;
        if(current_step === 'mapping-in-progress') {
            current_step = 'connect';
        }
        if(response.data.status === 200) {
            navigate(`/onboarding/${current_step}/${response.data.onboarding_id}`);
        }
        setIsAddBusinessLoading(false);
    };

    const getBusinesses = async () => {
        try {
            const response = await axiosInstance.get('/analytics/get-businesses', {
                params: { userId: user.user_id }
            });
            const businessList = response.data.businesses || [];
            setBusinesses(businessList);

            // Redirect to first business if URL has no business ID
            if (!businessId && businessList.length > 0) {
                setBusinessIngestionType(businessList[0].ingestion_type);
                navigate(`/analytics/${businessList[0].business_id}`);
            }

        } catch (error) {
            console.error("Error fetching businesses:", error);
        }
    }

    const handleBusinessChange = (e) => {
        setSelectedBusiness(e.value);
        setBusinessIngestionType(e.option?.ingestion_type || null);
        navigate(`/analytics/${e.value}`);
    }

    useEffect(() => {
        getBusinesses();
    }, []);

    useEffect(() => {
        if (businessId && businesses.length > 0) {
            // Only set if different
            if (selectedBusiness !== businessId) {
                const business = businesses.find(b => b.business_id === businessId);
                if (business) {
                    setBusinessIngestionType(business.ingestion_type);
                }
                setSelectedBusiness(businessId);
            }
        }
    }, [businessId, businesses]);
    
    // Handle starting analysis
    const handleStartAnalysis = async () => {
        if (!businessId || !user?.user_id) {
            console.error('Missing businessId or user_id');
            return;
        }
        
        try {
            const result = await startPipeline(businessId);
            if (!result.success) {
                console.error('Failed to start pipeline:', result.error);
            }
        } catch (err) {
            console.error('Error starting pipeline:', err);
        }
    };
    
    // Handle delete business with confirmation
    const handleDeleteBusiness = () => {
        if (!selectedBusiness) {
            return;
        }
        setShowDeleteDialog(true);
    };
    
    const performDeleteBusiness = async () => {
        if (!selectedBusiness || !user?.user_id) {
            return;
        }
        
        setIsDeleteBusinessLoading(true);
        
        try {
            const response = await axiosInstance.delete('/analytics/delete-business', {
                data: {
                    userId: user.user_id,
                    businessId: selectedBusiness
                }
            });
            
            if (response.data.status === 200) {
                setShowDeleteDialog(false);
                // Redirect to analytics page without business ID
                navigate('/analytics/');
                // Refresh business list
                await getBusinesses();
            }
        } catch (error) {
            console.error('Error deleting business:', error);
            alert('Failed to delete business. Please try again.');
        } finally {
            setIsDeleteBusinessLoading(false);
        }
    };

    // Function to trigger streaming pipeline
    const triggerStreamingPipeline = async () => {
        setPipelineStatus('running');
        
        try {
            const response = await axiosInstance.post('/pipeline/trigger-streaming', {
                businessId: businessId
            });
            
            if (response.data.success) {
                setPipelineStatus('success');
                toastRef.current?.show({
                    severity: 'success',
                    summary: 'Success',
                    detail: 'Streaming pipeline triggered successfully',
                    life: 3000
                });
                // Return to idle after 3 seconds
                setTimeout(() => setPipelineStatus('idle'), 3000);
            } else {
                throw new Error('Pipeline trigger failed');
            }
        } catch (error) {
            setPipelineStatus('failed');
            toastRef.current?.show({
                severity: 'error',
                summary: 'Error',
                detail: 'Failed to trigger streaming pipeline',
                life: 5000
            });
            // Return to idle after 5 seconds
            setTimeout(() => setPipelineStatus('idle'), 5000);
        }
    };

    // Ingestion Status Indicator Component
    const IngestionStatusIndicator = ({
        ingestionType,
        pipelineStatus,
        onTriggerPipeline
    }) => {

        const getStatusConfig = () => {
            if (ingestionType === 'batch') {
            return {
                borderColor: 'border-purple-500',
                dotColor: 'bg-purple-500',
                glow: 'shadow-[0_0_5px_2px_rgba(168,85,247,0.7)]',
                text: 'Batch',
                showRefresh: false,
                rotating: false,
                disabled: false,
                pulse: true
            };
            }

            const text = ingestionType === 'api' ? 'API' : 'Database';

            if (pipelineStatus === 'running') {
            return {
                borderColor: 'border-yellow-500',
                dotColor: 'bg-yellow-500',
                glow: 'shadow-[0_0_10px_3px_rgba(234,179,8,0.9)]',
                text,
                showRefresh: true,
                rotating: true,
                disabled: true,
                pulse: false
            };
            }

            if (pipelineStatus === 'failed') {
            return {
                borderColor: 'border-red-500',
                dotColor: 'bg-red-500',
                glow: 'shadow-[0_0_8px_2px_rgba(239,68,68,0.8)]',
                text,
                showRefresh: true,
                rotating: false,
                disabled: false,
                pulse: true
            };
            }

            return {
            borderColor: 'border-green-500',
            dotColor: 'bg-green-500',
            glow: 'shadow-[0_0_8px_2px_rgba(34,197,94,0.8)]',
            text,
            showRefresh: true,
            rotating: false,
            disabled: false,
            pulse: true
            };
        };

        const config = getStatusConfig();

        return (
            <div
            className={`
                border-2 ${config.borderColor}
                rounded-lg px-3 py-2 
                flex items-center gap-2
                transition-all duration-300
            `}
            >
            {/* Glowing Status Dot */}
            <div
                className={`
                w-2.5 h-2.5 rounded-full
                ${config.dotColor}
                ${config.glow}
                ${config.pulse ? 'animate-pulse' : ''}
                transition-all duration-300
                `}
            />

            <span className="text-sm font-medium text-gray-700">
                {config.text}
            </span>

            {config.showRefresh && (
                <button
                onClick={onTriggerPipeline}
                disabled={config.disabled}
                className={`
                    ml-1 p-1 hover:bg-gray-100 rounded
                    transition-colors duration-200
                    ${config.rotating ? 'animate-spin' : ''}
                    ${config.disabled ? 'opacity-50 cursor-not-allowed' : ''}
                `}
                title="Trigger streaming pipeline"
                >
                <i className="pi pi-refresh text-sm text-gray-600" />
                </button>
            )}
            </div>
        );
    };

    const businessName = businesses.find(b => b.business_id === selectedBusiness)?.business_name || 'this business';

    const deleteDialogFooter = (
        <div className="flex justify-end gap-2 mb-5 mr-5">
            <SecondaryButton 
                onClick={() => setShowDeleteDialog(false)}
                disabled={isDeleteBusinessLoading}
                label="Cancel"
                success
            >
            </SecondaryButton>
            <PrimaryButton 
                onClick={performDeleteBusiness}
                loading={isDeleteBusinessLoading}
                label={isDeleteBusinessLoading ? 'Deleting...' : 'Delete'}
                danger
            />
        </div>
    );

    // Get current location for route-based rendering
    const location = useLocation();
    const pathname = location.pathname;

    // Render appropriate analytics content based on route
    const renderAnalyticsContent = () => {
        if (!businessId) return null;

        // Executive Overview - exact match
        if (pathname === `/analytics/${businessId}` || pathname === `/analytics/${businessId}/`) {
            return <ExecutiveOverview />;
        }

        // Customers routes
        if (pathname.includes('/customers/overview')) {
            return <CustomerOverview />;
        }

        if (pathname.includes('/customers/segmentation')) {
            return <CustomerSegmentation />;
        }

        if (pathname.includes('/customers/health')) {
            return <CustomerHealthRetention />;
        }

        if (pathname.includes('/customers/value')) {
            return <CustomerValueAnalysis />;
        }

        // Products routes
        if (pathname.includes('/products/performance')) {
            return <ProductPerformance />;
        }

        if (pathname.includes('/products/profitability')) {
            return <ProductProfitability />;
        }

        if (pathname.includes('/products/engagement')) {
            return <ProductEngagement />;
        }

        if (pathname.includes('/products/trends')) {
            return <ProductTrends />;
        }

        // Inventory routes
        if (pathname.includes('/inventory/health')) {
            return <InventoryHealth />;
        }

        if (pathname.includes('/inventory/reorder')) {
            return <InventoryReorderManagement />;
        }

        if (pathname.includes('/inventory/efficiency')) {
            return <InventoryEfficiency />;
        }

        if (pathname.includes('/inventory/supplier')) {
            return <InventorySupplier />;
        }

        // Supplier routes
        if (pathname.includes('/suppliers/performance')) {
            return <SupplierPerformance />;
        }

        if (pathname.includes('/suppliers/operations')) {
            return <SupplierOperations />;
        }

        if (pathname.includes('/suppliers/economics')) {
            return <SupplierEconomics />;
        }

        // Marketing routes
        if (pathname.includes('/marketing/campaigns')) {
            return <MarketingCampaigns />;
        }

        if (pathname.includes('/marketing/attribution')) {
            return <MarketingAttribution />;
        }

        if (pathname.includes('/marketing/channels')) {
            return <MarketingChannels />;
        }

        // Funnel routes
        if (pathname.includes('/funnel/overview')) {
            return <FunnelOverview />;
        }

        if (pathname.includes('/funnel/cart')) {
            return <FunnelCart />;
        }

        if (pathname.includes('/funnel/checkout')) {
            return <FunnelCheckout />;
        }

        if (pathname.includes('/funnel/wishlist')) {
            return <FunnelWishlist />;
        }

        // Payments & Finance routes
        if (pathname.includes('/payments/methods')) {
            return <PaymentMethods />;
        }

        if (pathname.includes('/payments/refunds')) {
            return <PaymentRefunds />;
        }

        if (pathname.includes('/payments/metrics')) {
            return <PaymentFinancialMetrics />;
        }

        // Operations routes
        if (pathname.includes('/operations/processing')) {
            return <OperationsProcessing />;
        }

        if (pathname.includes('/operations/delivery')) {
            return <OperationsDelivery />;
        }

        if (pathname.includes('/operations/shipping')) {
            return <OperationsShipping />;
        }

        // Recommendations routes
        if (pathname.includes('/recommendations/product')) {
            return <RecommendationsProductAffinity />;
        }

        if (pathname.includes('/recommendations/category')) {
            return <RecommendationsCategoryAffinity />;
        }

        if (pathname.includes('/recommendations/coverage')) {
            return <RecommendationsCoverage />;
        }

        // Reviews & Sentiment routes
        if (pathname.includes('/reviews/overview')) {
            return <ReviewsOverview />;
        }

        if (pathname.includes('/reviews/sentiment')) {
            return <ReviewsSentiment />;
        }

        if (pathname.includes('/reviews/impact')) {
            return <ReviewsImpact />;
        }

        // Engagement routes
        if (pathname.includes('/engagement/metrics')) {
            return <EngagementMetrics />;
        }

        if (pathname.includes('/engagement/behavior')) {
            return <EngagementBehavior />;
        }

        if (pathname.includes('/engagement/conversion')) {
            return <EngagementConversion />;
        }

        if (pathname.includes('/forecasts')) {
            return <Forecasts />;
        }

        if (pathname.includes('/xai')) {
            return <ExplainableAI />;
        }

        if (pathname.includes('/export')) {
            return <ExportAnalytics />;
        }

        // Default fallback for unrecognized routes
        return (
            <div className="flex items-center justify-center min-h-[60vh]">
                <div className="text-center max-w-md">
                    <p className="text-gray-500 text-lg">
                        Page not fond. Please select a valid analytics page from the sidebar.
                    </p>
                </div>
            </div>
        );
    };

    return (
        <div className="flex h-screen overflow-hidden bg-gray-50">
            {/* Toast for notifications */}
            <Toast ref={toastRef} />
            
            {/* Delete Business Dialog */}
            <Dialog
                visible={showDeleteDialog}
                onHide={() => setShowDeleteDialog(false)}
                header="Delete Business"
                footer={deleteDialogFooter}
                style={{ width: '450px' }}
                modal
            >
                <div>
                    <p>Are you sure you want to delete <strong>{businessName}</strong>?</p>
                    <p className="text-red-600 text-sm mt-2">
                        This will permanently delete:
                    </p>
                    <ul className="text-sm text-red-600 list-disc list-inside mt-1">
                        <li>All pipeline data</li>
                        <li>All processed data from storage</li>
                        <li>All business records</li>
                    </ul>
                    <p className="text-sm text-gray-600 mt-2">
                        This action cannot be undone.
                    </p>
                </div>
            </Dialog>
            
            {/* Sidebar */}
            <Sidebar isOpen={sidebarOpen} onClose={() => setSidebarOpen(false)} />

            {/* Main Content */}
            <div className="flex-1 flex flex-col overflow-hidden">
                {/* Top Header */}
                <header className="bg-white border-b border-gray-200 px-4 md:px-6 py-4 flex items-center justify-between gap-4">
                    <button
                        onClick={() => setSidebarOpen(true)}
                        className="lg:hidden p-2 hover:bg-gray-100 rounded-lg transition-colors"
                    >
                        <i className="pi pi-bars text-xl text-gray-700"></i>
                    </button>

                    <div className="flex items-center gap-4">
                        <Heading level={3} gradient={true} className="hidden md:block text-xl md:text-2xl m-0">
                            Dashboard
                        </Heading>
                        {/* Ingestion Status Indicator */}
                        {businessId && businessIngestionType && (
                            <IngestionStatusIndicator 
                                ingestionType={businessIngestionType}
                                pipelineStatus={pipelineStatus}
                                onTriggerPipeline={triggerStreamingPipeline}
                            />
                        )}
                    </div>
                    {/* Search Insight */}
                    <div className="relative w-2/4" ref={searchRef}>
                        <span className="p-input-icon-left w-full">
                            <i className="pi pi-search" />
                            <InputText
                                type="text"
                                className="p-inputtext-sm w-full"
                                placeholder="Search Insight..."
                                value={searchQuery}
                                onChange={(e) => { setSearchQuery(e.target.value); setSearchOpen(true); }}
                                onFocus={() => setSearchOpen(true)}
                            />
                        </span>
                        {searchOpen && searchResults.length > 0 && (
                            <div className="absolute top-full left-0 right-0 mt-1 bg-white border border-gray-200 rounded-xl shadow-xl z-50 max-h-80 overflow-y-auto">
                                {searchResults.map((item) => (
                                    <button
                                        key={item.key}
                                        onClick={() => handleInsightSelect(item)}
                                        className="w-full text-left px-4 py-3 hover:bg-gray-50 flex items-start gap-3 transition-colors border-b border-gray-100 last:border-b-0"
                                    >
                                        <i className="pi pi-chart-bar text-[var(--color-primary)] mt-0.5 shrink-0" />
                                        <div className="min-w-0">
                                            <p className="text-sm font-medium text-gray-900 truncate">{item.title}</p>
                                            <p className="text-xs text-gray-400 truncate">{item.section} · {item.category}</p>
                                        </div>
                                        <i className="pi pi-arrow-right text-gray-300 text-xs ml-auto mt-1 shrink-0" />
                                    </button>
                                ))}
                            </div>
                        )}
                        {searchOpen && searchQuery.trim() && searchResults.length === 0 && (
                            <div className="absolute top-full left-0 right-0 mt-1 bg-white border border-gray-200 rounded-xl shadow-xl z-50 px-4 py-3 text-sm text-gray-500">
                                No insights found for &ldquo;{searchQuery}&rdquo;
                            </div>
                        )}
                    </div>

                    {/* Right Side - Notifications & Avatar */}
                    <div className="flex items-center gap-3">
                        
                        {/* PROFILE DROPDOWN CONTAINER */}
                        <div className="relative" ref={profileRef}>
                            {/* Avatar Trigger */}
                            <div 
                                onClick={() => setIsProfileOpen(!isProfileOpen)}
                                className={`
                                    w-10 h-10 rounded-full bg-gradient-primary 
                                    flex items-center justify-center text-white font-bold 
                                    cursor-pointer hover:opacity-90 transition-all shadow-sm
                                    ${isProfileOpen ? 'ring-2 ring-offset-1 ring-[var(--color-primary)]' : ''}
                                `}
                            >
                                {user?.username ? (
                                    <span className="uppercase">{user.username.charAt(0)}</span>
                                ) : (
                                    <i className="pi pi-user text-lg"></i>
                                )}
                            </div>

                            {/* CUSTOM MENU DROPDOWN */}
                            {isProfileOpen && (
                                <div className="absolute right-0 mt-3 w-48 bg-white rounded-xl shadow-xl border border-gray-100 py-2 z-50 animate-fade-in-down origin-top-right">
                                    {/* User Info (Optional Header) */}
                                    <div className="px-4 py-2 border-b border-gray-100 mb-1">
                                        <p className="text-sm font-semibold text-gray-800 truncate">{user?.username || 'User'}</p>
                                        <p className="text-xs text-gray-500 truncate">{user?.email}</p>
                                    </div>

                                    {/* Menu Items */}
                                    <button 
                                        onClick={() => {}}
                                        className="w-full text-left px-4 py-2.5 text-sm text-gray-700 hover:bg-gray-50 flex items-center gap-2 transition-colors"
                                    >
                                        <i className="pi pi-user text-gray-500"></i>
                                        Profile
                                    </button>

                                    <button 
                                        onClick={logout}
                                        className="w-full text-left px-4 py-2.5 text-sm text-red-600 hover:bg-red-50 flex items-center gap-2 transition-colors"
                                    >
                                        <i className="pi pi-sign-out text-red-500"></i>
                                        Log Out
                                    </button>
                                </div>
                            )}
                        </div>
                    </div>
                </header>

                {/* Main Content Area */}
                <main className="flex-1 overflow-y-auto p-4 md:p-6">
                    {!pathname.includes('/xai') && (
                    <div className="flex items-center justify-between">
                        <div className="mx-10 flex items-center gap-2">
                            {/* Add Business Button */}
                            <SecondaryButton
                                onClick={handleAddBusiness}
                                icon="pi pi-building"
                                disabled={isAddBusinessLoading}
                                label={isAddBusinessLoading ? 'Adding...' : 'Add Business'}
                                black
                            >
                            </SecondaryButton>
                            
                            {/* Delete Business Button (Icon Only) */}
                            {selectedBusiness && (
                                <Button
                                    onClick={handleDeleteBusiness}
                                    className="bg-white text-red-600 border border-red-300 hover:border-red-500 hover:bg-red-50 transition-all p-2"
                                    style={{
                                        background: 'white',
                                        boxShadow: '0 2px 4px rgba(0, 0, 0, 0.1)',
                                        width: '44px',
                                        height: '44px',
                                        display: 'flex',
                                        alignItems: 'center',
                                        justifyContent: 'center'
                                    }}
                                    disabled={isDeleteBusinessLoading}
                                    title="Delete Business"
                                >
                                    <i className={`pi ${isDeleteBusinessLoading ? 'pi-spin pi-spinner' : 'pi-trash'} text-lg`}></i>
                                </Button>
                            )}
                        </div>
                        <div className="w-48">
                            <Dropdown 
                                value={selectedBusiness} 
                                onChange={handleBusinessChange} 
                                options={businesses.map (b => ({ label: b.business_name, value: b.business_id }))}
                                virtualScrollerOptions={{ itemSize: 38 }}
                                placeholder="Select Business" 
                                className="w-full" 
                            />
                        </div>
                    </div>
                    )}
                    
                    {/* Show inline pipeline progress when business is selected */}
                    {businessId ? (
                        <CurrencyProvider businessId={businessId}>
                            <InlinePipelineProgress
                                businessId={businessId}
                                onStartAnalysis={handleStartAnalysis}
                            />
                            {/* Render appropriate analytics content based on route */}
                            {renderAnalyticsContent()}
                        </CurrencyProvider>
                    ) : (
                        <div className="flex items-center justify-center min-h-[60vh]">
                            <div className="text-center max-w-md">
                                <Text className="text-gray-500 text-base md:text-lg leading-relaxed">
                                    You have not added any business yet. Please click on the "Add Business Button" above to add a business.
                                </Text>
                            </div>
                        </div>
                    )}
                </main>
            </div>
        </div>
    );
};

export default Dashboard;