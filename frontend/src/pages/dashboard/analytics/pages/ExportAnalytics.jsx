import React, { useState, useCallback, useRef } from 'react';
import { useParams } from 'react-router-dom';
import { ProgressSpinner } from 'primereact/progressspinner';
import { Toast } from 'primereact/toast';
import { Checkbox } from 'primereact/checkbox';
import { RadioButton } from 'primereact/radiobutton';
import PrimaryButton from '@/components/global/Button/PrimaryButton';
import SecondaryButton from '@/components/global/Button/SecondaryButton';
import Heading from '@/components/global/Typography/Heading';
import Text from '@/components/global/Typography/Text';
import { useAuth } from '@/context/AuthContext';
import axiosInstance from '@/services/api/axiosInstance';
import jsPDF from 'jspdf';
import autoTable from 'jspdf-autotable';

// ---------------------------------------------------------------------------
// Section catalogue — mirrors Sidebar.jsx exactly
// ---------------------------------------------------------------------------

const EXPORT_SECTIONS = [
    {
        key: 'executive_overview',
        label: 'Executive Overview',
        icon: 'pi-home',
        categories: ['kpis'],
        analyticsKeys: [
            'business_health_daily',
            'business_health_weekly',
            'business_health_monthly',
            'clv_summary',
            'funnel_summary',
            'cart_abandon_summary',
            'session_to_order_analysis',
            'customer_engagement_summary',
        ],
    },
    {
        key: 'customers',
        label: 'Customers',
        icon: 'pi-users',
        subItems: ['Overview', 'Segmentation', 'Health & Retention', 'Value Analysis'],
        categories: ['customer_analytics', 'geo_analytics'],
        analyticsKeys: [
            'customer_account_status_distribution_daily',
            'customer_account_status_distribution_weekly',
            'customer_account_status_distribution_monthly',
            'new_customers_daily',
            'new_customers_weekly',
            'new_customers_monthly',
            'cumulative_customers_daily',
            'cumulative_customers_weekly',
            'cumulative_customers_monthly',
            'new_customers_geo_acquisition_daily',
            'new_customers_geo_acquisition_monthly',
            'customer_age_group_distribution',
            'customer_city_distribution',
            'customer_state_distribution',
            'customer_country_distribution',
            'customer_age_group_spending',
            'new_vs_returning_customer_country',
            'new_vs_returning_customer_city',
            'new_vs_returning_customer_state',
            'customer_engagement',
            'session_conversion_distribution',
            'cart_abandonment_distribution',
            'rfm_segment_summary',
            'rfm_churn_crosstab',
            'seg_referrer_crosstab',
            'seg_device_crosstab',
            'payment_method_vs_clv_churn',
            'payment_method_summary',
            'referrer_source_summary',
            'referrer_churn_summary',
            'gender_category_preference',
            'gender_product_preference',
            'churn_risk_summary',
            'high_clv_at_risk',
            'signup_cohort_summary',
            'customer_cohort_retention',
            'high_intent_non_buyers',
            'customers_cohorts',
            'top_customers_by_revenue',
            'top_customers_by_profit',
            'discount_customers_summary',
            'discount_customers',
            'correlation_discount_vs_clv',
            'high_discount_customers',
            'cart_behavior_summary',
            'high_value_abandoners',
            'customer_profit_per_segment',
            'customer_overall_health_summary',
            'geo_acquisition',
        ],
    },
    {
        key: 'products',
        label: 'Products',
        icon: 'pi-box',
        subItems: ['Performance', 'Profitability', 'Engagement', 'Trends'],
        categories: ['product_analytics'],
        analyticsKeys: [
            'best_selling_products',
            'out_of_stock_products',
            'low_conversion_products',
            'product_rating_summary',
            'category_view_patterns',
            'top_view_to_purchase_products',
            'product_performance_score',
            'highest_margin_products',
            'low_margin_high_traffic_products',
            'category_revenue_share',
            'low_performing_categories',
            'category_popularity_score',
            'category_profitability',
            'product_lifecycle_segments',
            'product_lifecycle_summary',
            'supplier_product_performance',
            'stockout_rate_by_product',
            'supplier_stockout_impact_on_products',
            'product_monthly_trends',
            'category_monthly_trends',
            'product_calendar_month_seasonality',
            'category_calendar_month_seasonality',
            'category_monthly_seasonality',
            'category_peak_season',
        ],
    },
    {
        key: 'inventory',
        label: 'Inventory',
        icon: 'pi-inbox',
        subItems: ['Health', 'Reorder Management', 'Efficiency', 'Supplier Inventory'],
        categories: ['product_analytics'],
        analyticsKeys: [
            'product_stockout_risk',
            'product_stockout_replenishment',
            'product_dead_stock',
            'product_inventory_health',
            'product_inventory_critical',
            'sku_reorder_urgency',
            'reorder_point_breach_frequency',
            'inventory_stock_status',
            'days_of_supply',
            'overstock_analysis',
            'reserved_vs_available',
            'excess_inventory_not_selling',
            'margin_erosion_risk',
            'inventory_carrying_cost_by_product',
            'supplier_ranking_core',
        ],
    },
    {
        key: 'suppliers',
        label: 'Suppliers',
        icon: 'pi-building',
        subItems: ['Performance', 'Operations', 'Economics'],
        categories: ['supplier_analytics'],
        analyticsKeys: [
            'stockout_rate_by_supplier',
            'supplier_reliability',
            'supplier_revenue_contribution',
            'supplier_profit_margin',
            'supplier_fulfillment_performance',
            'supplier_stockouts',
            'supplier_days_since_last_restock',
            'supplier_contract_expiry',
            'storage_cost_efficiency_by_supplier',
            'inventory_carrying_cost_by_supplier',
        ],
    },
    {
        key: 'marketing',
        label: 'Marketing',
        icon: 'pi-megaphone',
        subItems: ['Campaigns', 'Attribution', 'Channels'],
        categories: ['marketing_analytics'],
        analyticsKeys: [
            'campaign_performance_summary',
            'campaign_product_contribution',
            'campaign_ltv',
            'campaign_customer_ltv_summary',
            'campaign_wasteful_campaigns',
            'campaign_margin_profile',
            'campaign_performance',
        ],
    },
    {
        key: 'conversion_funnel',
        label: 'Conversion Funnel',
        icon: 'pi-shopping-cart',
        subItems: ['Funnel Overview', 'Cart Analysis', 'Checkout', 'Wishlist'],
        categories: ['funnel_analytics', 'cart_analytics', 'wishlist_analytics'],
        analyticsKeys: [
            'high_value_funnel',
            'high_value_vs_regular',
            'funnel_by_device',
            'funnel_by_referrer',
            'abandoned_vs_converted',
            'checkout_dropoff_reasons',
            'checkout_dropoff_buckets',
            'checkout_dropoff_by_device_and_reason',
            'device_conversion_rates',
            'cart_overall_stats',
            'cart_status_distribution',
            'cart_value_stats',
            'high_value_abandoned_carts',
            'time_to_purchase_overall',
            'time_to_purchase_by_tier',
            'time_to_purchase_buckets',
            'wishlist_overall_summary',
            'wishlist_by_product',
            'wishlist_by_customer',
            'wishlist_time_to_purchase_stats',
            'wishlist_time_to_purchase_distribution',
            'abandoned_wishlist_items',
            'abandoned_wishlist_by_customer',
            'abandoned_wishlist_by_product',
            'wishlist_adds_by_month',
        ],
    },
    {
        key: 'payments_finance',
        label: 'Payments & Finance',
        icon: 'pi-credit-card',
        subItems: ['Payment Methods', 'Refunds', 'Financial Metrics'],
        categories: ['payment_analytics', 'revenue_analytics'],
        analyticsKeys: [
            'payment_counts_by_country_method',
            'payment_counts_by_state_method',
            'payment_method_success_rates',
            'payment_method_success_rates_by_country',
            'payment_method_aov',
            'refund_rate_by_payment_method',
            'refund_rate_by_product',
            'refund_rate_by_month',
            'time_to_refund_by_payment_method',
            'low_margin_categories',
            'rev_by_country_city',
            'rev_by_customer_segment',
            'rev_by_rfm_segment',
            'rev_by_segment_label',
            'rev_by_referrer',
            'rev_by_device',
            'aov_trend_daily',
            'aov_trend_weekly',
            'aov_trend_monthly',
            'segment_aov_by_rfm',
            'inventory_carrying_cost_overall',
        ],
    },
    {
        key: 'operations',
        label: 'Operations',
        icon: 'pi-truck',
        subItems: ['Processing', 'Delivery', 'Shipping'],
        categories: ['operations_analytics'],
        analyticsKeys: [
            'processing_by_category',
            'processing_by_subcategory',
            'processing_by_hour',
            'processing_by_day_of_week',
            'weekend_vs_weekday',
            'processing_by_season',
            'processing_by_season_and_status',
            'delivery_days_by_country',
            'delivery_days_by_state',
            'delivery_days_by_city',
            'ontime_delivery_by_country',
            'ontime_delivery_by_state',
            'ontime_delivery_by_city',
            'shipping_efficiency_by_country',
            'shipping_efficiency_by_state',
            'shipping_efficiency_by_city',
        ],
    },
    {
        key: 'recommendations',
        label: 'Recommendations',
        icon: 'pi-link',
        subItems: ['Product Affinity', 'Category Affinity', 'Coverage'],
        categories: ['product_analytics'],
        analyticsKeys: [
            'product_affinity_pairs',
            'product_affinity_top_per_product',
            'category_affinity_pairs',
            'category_affinity_top_per_category',
            'precomputed_product_recommendations',
            'precomputed_reco_coverage',
        ],
    },
    {
        key: 'reviews_sentiment',
        label: 'Reviews & Sentiment',
        icon: 'pi-star',
        subItems: ['Overview', 'Sentiment', 'Impact'],
        categories: ['review_analytics'],
        analyticsKeys: [
            'rating_tier_per_product',
            'rating_tier_sales_velocity',
            'sentiment_by_category',
            'review_velocity_daily',
            'review_velocity_weekly',
            'review_velocity_monthly',
            'product_monthly_rating_trends',
            'low_rated_product_monthly_trends_rating_only',
        ],
    },
    {
        key: 'engagement',
        label: 'Engagement',
        icon: 'pi-chart-line',
        subItems: ['Metrics', 'Behavior', 'Conversion'],
        categories: ['kpis', 'customer_analytics', 'funnel_analytics'],
        analyticsKeys: [
            'customer_engagement_summary',
            'funnel_summary',
            'cart_abandon_summary',
            'session_to_order_analysis',
            'customer_engagement',
            'session_conversion_distribution',
            'cart_abandonment_distribution',
            'high_intent_non_buyers',
            'high_value_funnel',
            'high_value_vs_regular',
            'funnel_by_device',
            'funnel_by_referrer',
            'abandoned_vs_converted',
            'checkout_dropoff_reasons',
            'checkout_dropoff_buckets',
            'checkout_dropoff_by_device_and_reason',
            'device_conversion_rates',
        ],
    },
    {
        key: 'forecasts',
        label: 'Forecasts & Predictions',
        icon: 'pi-chart-bar',
        isForecast: true,      // signals different fetch endpoint
        categories: [],         // not used for forecast fetch
        analyticsKeys: [
            'cart_abandonment_predictions',
            'customer_churn_predictions',
            'customer_segment_predictions',
            'payment_success_predictions',
            'review_sentiment_predictions',
            'stock_status_predictions',
            'customer_segmentation',
            'geographic_clustering',
            'session_behavior_clustering',
            'supplier_clustering',
            'aov_prediction',
            'clv_predictions',
            'restock_quantity',
            'safety_stock_adjusted',
            'session_conversion_value',
            'stockout_probability',
            'fulfillment_risk_predictions',
            'product_bundling_predictions',
            'product_affinity_clustering',
            'product_lifecycle_clustering',
            'campaign_roi',
            'delivery_time',
            'demand_forecast',
            'price_optimization',
            'revenue_forecast',
            'seasonal_trends',
        ],
    },
];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Maximum characters shown in bar-chart row labels
const MAX_LABEL_LEN = 22;
// Maximum characters shown on line-chart x-axis ticks
const MAX_XAXIS_LEN = 12;
// Column-name pattern for picking the categorical label in a bar chart
const LABEL_COL_RE =
    /name|category|product|supplier|country|state|city|segment|status|type|method|device|referrer|tier|source|channel|campaign|gender|group|brand|reason|bucket/;
// Column-name pattern for picking the primary numeric value in a bar chart
const VALUE_COL_RE =
    /revenue|total|count|orders|amount|value|sales|profit|rate|score|spend|clicks|impressions/;
// Column-name pattern for proportion / distribution data → prefer pie chart
const PIE_COL_RE = /share|pct|percentage|ratio|proportion|distribution/;
// Pie chart radius (mm)
const PIE_CHART_R = 26;
// Minimum slice angle (radians) to draw a pie slice — avoids zero-area artifacts
const MIN_SLICE_ANGLE = 0.001;
// Max data points before dots are suppressed on line/area charts (prevents clutter)
const MAX_DOTS_THRESHOLD = 40;

/** Human-readable column header from snake_case key */
const colHeader = (key) =>
    key
        .split('_')
        .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
        .join(' ');

/** Format a cell value for display in the PDF */
const fmtCell = (val) => {
    if (val === null || val === undefined) return '';
    if (typeof val === 'number') {
        if (Number.isNaN(val) || !Number.isFinite(val)) return '';
        return Number.isInteger(val)
            ? val.toLocaleString()
            : parseFloat(val.toFixed(2)).toLocaleString();
    }
    return String(val);
};

// Maximum rows per table to keep PDF size manageable
const MAX_TABLE_ROWS = 200;
// Maximum KPI cards shown in a single card grid (3 per row)
const MAX_KPI_CARDS = 24;

// ---------------------------------------------------------------------------
// Color palette
// ---------------------------------------------------------------------------
const PALETTE = [
    [99, 102, 241],  // indigo
    [16, 185, 129],  // emerald
    [245, 158, 11],  // amber
    [239, 68, 68],   // red
    [59, 130, 246],  // blue
    [168, 85, 247],  // purple
    [236, 72, 153],  // pink
    [20, 184, 166],  // teal
];

// ---------------------------------------------------------------------------
// Chart drawing helpers
// ---------------------------------------------------------------------------

/**
 * Horizontal bar chart. Returns the Y coordinate after the last element.
 */
const drawHBar = (doc, rows, labelKey, valueKey, x, y, w, maxBars = 12) => {
    const items = rows.slice(0, maxBars);
    if (!items.length) return y;
    const vals = items.map((r) => Math.max(0, Number(r[valueKey] ?? 0)));
    const maxV = Math.max(...vals, 1);
    const LABEL_W = 46, BAR_AREA = w - LABEL_W - 24, ROW_H = 5.5, GAP = 1.8;
    items.forEach((row, i) => {
        const ry = y + i * (ROW_H + GAP);
        const barLen = (vals[i] / maxV) * BAR_AREA;
        doc.setFontSize(6.2);
        doc.setFont('helvetica', 'normal');
        doc.setTextColor(55, 65, 81);
        doc.text(String(row[labelKey] ?? '').substring(0, MAX_LABEL_LEN), x + LABEL_W, ry + ROW_H - 0.5, { align: 'right' });
        // Track (background bar)
        doc.setFillColor(229, 231, 235);
        doc.rect(x + LABEL_W + 2, ry, BAR_AREA, ROW_H, 'F');
        // Filled bar
        if (barLen > 0) {
            const [r, g, b] = PALETTE[i % PALETTE.length];
            doc.setFillColor(r, g, b);
            doc.rect(x + LABEL_W + 2, ry, barLen, ROW_H, 'F');
        }
        // Value label
        doc.setFontSize(5.8);
        doc.setTextColor(75, 85, 99);
        doc.text(fmtCell(vals[i]), x + LABEL_W + BAR_AREA + 4, ry + ROW_H - 0.5);
    });
    let endY = y + items.length * (ROW_H + GAP) + 1;
    if (rows.length > maxBars) {
        doc.setFontSize(6);
        doc.setFont('helvetica', 'italic');
        doc.setTextColor(156, 163, 175);
        doc.text(`… and ${rows.length - maxBars} more`, x + LABEL_W + 2, endY + 3);
        endY += 6;
    }
    return endY;
};

/**
 * Line chart for time-series data (up to 3 y-series). Returns Y after chart.
 */
const drawLineChart = (doc, rows, xKey, yKeys, x, y, w, chartH = 42) => {
    if (rows.length < 2) return y;
    const ML = 15, MB = 10, MR = 6, MT = 2;
    const plotW = w - ML - MR, plotH = chartH - MT - MB;
    const bx = x + ML, by = y + MT;
    const allVals = yKeys.flatMap((yk) => rows.map((r) => Number(r[yk] ?? 0)));
    if (!allVals.length) return y;
    // Use reduce instead of spread to avoid RangeError: Maximum call stack size exceeded
    // on large datasets (JS spread passes each element as a function argument, which
    // hits engine limits when allVals has tens-of-thousands of elements).
    const minV = allVals.reduce((acc, v) => (v < acc ? v : acc), allVals[0]);
    // Start maxV at minV+1 to guarantee range > 0 (prevents division-by-zero in scaling).
    const maxV = allVals.reduce((acc, v) => (v > acc ? v : acc), minV + 1);
    const range = maxV - minV;
    // Plot background
    doc.setFillColor(249, 250, 251);
    doc.rect(bx, by, plotW, plotH, 'F');
    // Grid lines
    doc.setDrawColor(229, 231, 235);
    doc.setLineWidth(0.15);
    [0, 0.25, 0.5, 0.75, 1].forEach((frac) => {
        const gy = by + frac * plotH;
        doc.line(bx, gy, bx + plotW, gy);
        if (frac === 0 || frac === 0.5 || frac === 1) {
            doc.setFontSize(4.8);
            doc.setTextColor(156, 163, 175);
            doc.text(fmtCell(maxV - frac * range), bx - 1, gy + 1.5, { align: 'right' });
        }
    });
    // Border
    doc.setDrawColor(209, 213, 219);
    doc.setLineWidth(0.2);
    doc.rect(bx, by, plotW, plotH, 'S');
    // X-axis labels (max 10)
    const n = rows.length;
    const step = Math.max(1, Math.ceil(n / 10));
    rows.forEach((row, i) => {
        if (i % step !== 0 && i !== n - 1) return;
        const px = bx + (i / (n - 1)) * plotW;
        doc.setFontSize(4.8);
        doc.setTextColor(156, 163, 175);
        doc.text(String(row[xKey] ?? '').substring(0, MAX_XAXIS_LEN), px, by + plotH + 7, { align: 'center' });
    });
    // Series
    yKeys.forEach((yk, si) => {
        const [lr, lg, lb] = PALETTE[si % PALETTE.length];
        doc.setDrawColor(lr, lg, lb);
        doc.setLineWidth(0.65);
        rows.forEach((row, i) => {
            if (i === 0) return;
            const prev = rows[i - 1];
            const x1 = bx + ((i - 1) / (n - 1)) * plotW;
            const x2 = bx + (i / (n - 1)) * plotW;
            const y1 = by + plotH - ((Number(prev[yk] ?? 0) - minV) / range) * plotH;
            const y2 = by + plotH - ((Number(row[yk] ?? 0) - minV) / range) * plotH;
            doc.line(x1, y1, x2, y2);
        });
        // Dots (only when not too many points)
        doc.setFillColor(lr, lg, lb);
        if (n <= MAX_DOTS_THRESHOLD) {
            rows.forEach((row, i) => {
                const px = bx + (i / (n - 1)) * plotW;
                const py = by + plotH - ((Number(row[yk] ?? 0) - minV) / range) * plotH;
                doc.circle(px, py, 0.9, 'F');
            });
        }
        // Legend (multi-series only)
        if (yKeys.length > 1) {
            const lx = bx + si * 40;
            const ly = by + plotH + MB;
            doc.setFillColor(lr, lg, lb);
            doc.rect(lx, ly - 2.5, 4, 2.5, 'F');
            doc.setFontSize(5.5);
            doc.setTextColor(75, 85, 99);
            doc.text(colHeader(yk), lx + 5.5, ly);
        }
    });
    return y + chartH + (yKeys.length > 1 ? 6 : 2);
};

/**
 * KPI card grid for single-row datasets (3 cards per row). Returns Y after grid.
 */
const drawKPICards = (doc, row, x, y, w, maxCards = MAX_KPI_CARDS) => {
    const entries = Object.entries(row)
        .filter(([, v]) => v !== null && v !== undefined)
        .slice(0, maxCards);
    const COLS = 3, CARD_W = (w - (COLS - 1) * 3) / COLS, CARD_H = 17;
    entries.forEach(([key, val], idx) => {
        const col = idx % COLS, rowIdx = Math.floor(idx / COLS);
        const cx = x + col * (CARD_W + 3), cy = y + rowIdx * (CARD_H + 3);
        // Card background
        doc.setFillColor(243, 244, 246);
        doc.rect(cx, cy, CARD_W, CARD_H, 'F');
        doc.setDrawColor(229, 231, 235);
        doc.setLineWidth(0.2);
        doc.rect(cx, cy, CARD_W, CARD_H, 'S');
        // Coloured top accent strip
        const [r, g, b] = PALETTE[idx % PALETTE.length];
        doc.setFillColor(r, g, b);
        doc.rect(cx, cy, CARD_W, 2, 'F');
        // Value
        doc.setTextColor(r, g, b);
        doc.setFontSize(10);
        doc.setFont('helvetica', 'bold');
        doc.text(fmtCell(val), cx + CARD_W / 2, cy + 10, { align: 'center' });
        // Label
        doc.setFontSize(5.5);
        doc.setFont('helvetica', 'normal');
        doc.setTextColor(107, 114, 128);
        doc.text(colHeader(key), cx + CARD_W / 2, cy + 14.5, { align: 'center' });
    });
    const rowsCount = Math.ceil(entries.length / COLS);
    return y + rowsCount * (CARD_H + 3) + 4;
};

/**
 * Vertical bar chart for ≤10 categories. Returns Y after chart.
 */
const drawVBar = (doc, rows, labelKey, valueKey, x, y, w, maxBars = 10) => {
    const items = rows.slice(0, maxBars);
    if (!items.length) return y;
    const vals = items.map((r) => Math.max(0, Number(r[valueKey] ?? 0)));
    const maxV = vals.reduce((acc, v) => (v > acc ? v : acc), 1);
    const CHART_H = 48, ML = 14, MR = 4, MB = 16, MT = 2;
    const plotW = w - ML - MR, plotH = CHART_H - MT - MB;
    const n = items.length;
    const barW = Math.max(4, Math.min(18, (plotW / n) * 0.65));
    const spacing = (plotW - barW * n) / (n + 1);
    const bx = x + ML, by = y + MT;
    // Background
    doc.setFillColor(249, 250, 251);
    doc.rect(bx, by, plotW, plotH, 'F');
    // Horizontal grid lines
    doc.setDrawColor(229, 231, 235);
    doc.setLineWidth(0.15);
    [0, 0.25, 0.5, 0.75, 1].forEach((frac) => {
        const gy = by + frac * plotH;
        doc.line(bx, gy, bx + plotW, gy);
        if (frac === 0 || frac === 0.5 || frac === 1) {
            doc.setFontSize(4.5);
            doc.setTextColor(156, 163, 175);
            doc.text(fmtCell(maxV * (1 - frac)), bx - 1, gy + 1.5, { align: 'right' });
        }
    });
    // Border
    doc.setDrawColor(209, 213, 219);
    doc.setLineWidth(0.2);
    doc.rect(bx, by, plotW, plotH, 'S');
    // Bars and labels
    items.forEach((row, i) => {
        const barH = Math.max(0.5, (vals[i] / maxV) * plotH);
        const barX = bx + spacing + i * (barW + spacing);
        const barY = by + plotH - barH;
        const [r, g, b] = PALETTE[i % PALETTE.length];
        doc.setFillColor(r, g, b);
        doc.rect(barX, barY, barW, barH, 'F');
        // Value on top
        doc.setFontSize(5.0);
        doc.setFont('helvetica', 'bold');
        doc.setTextColor(55, 65, 81);
        doc.text(fmtCell(vals[i]), barX + barW / 2, Math.max(by + 3, barY - 1.5), { align: 'center' });
        // X-axis label below
        doc.setFontSize(4.8);
        doc.setFont('helvetica', 'normal');
        doc.setTextColor(107, 114, 128);
        doc.text(
            String(row[labelKey] ?? '').substring(0, 14),
            barX + barW / 2, by + plotH + 5,
            { align: 'center' }
        );
    });
    let endY = y + CHART_H + 2;
    if (rows.length > maxBars) {
        doc.setFontSize(5.5);
        doc.setFont('helvetica', 'italic');
        doc.setTextColor(156, 163, 175);
        doc.text(`… and ${rows.length - maxBars} more`, bx, endY + 2.5);
        endY += 6;
    }
    return endY;
};

/**
 * Pie chart for proportional data (≤8 slices). Returns Y after chart.
 * Uses jsPDF doc.lines() polygon approximation for each sector.
 */
const drawPieChart = (doc, rows, labelKey, valueKey, x, y, w, maxSlices = 8) => {
    const items = rows.slice(0, maxSlices);
    if (!items.length) return y;
    const vals = items.map((r) => Math.max(0, Number(r[valueKey] ?? 0)));
    const total = vals.reduce((s, v) => s + v, 0);
    // Fall back to horizontal bar if all values are zero
    if (total === 0) return drawHBar(doc, rows, labelKey, valueKey, x, y, w);
    const PIE_R = PIE_CHART_R; // radius mm
    const cx = x + PIE_R + 2;
    const cy = y + PIE_R + 2;
    let currentAngle = -Math.PI / 2; // start from 12 o'clock
    items.forEach((row, i) => {
        const sliceAngle = (vals[i] / total) * 2 * Math.PI;
        if (sliceAngle < MIN_SLICE_ANGLE) { currentAngle += sliceAngle; return; }
        const steps = Math.max(4, Math.ceil(36 * (sliceAngle / (2 * Math.PI))));
        const aStep = sliceAngle / steps;
        // Polygon: start at center, line to arc start, trace arc, close back to center
        const lines = [[PIE_R * Math.cos(currentAngle), PIE_R * Math.sin(currentAngle)]];
        let prevAX = cx + PIE_R * Math.cos(currentAngle);
        let prevAY = cy + PIE_R * Math.sin(currentAngle);
        for (let s = 1; s <= steps; s++) {
            const a = currentAngle + s * aStep;
            const ax = cx + PIE_R * Math.cos(a);
            const ay = cy + PIE_R * Math.sin(a);
            lines.push([ax - prevAX, ay - prevAY]);
            prevAX = ax;
            prevAY = ay;
        }
        const [r, g, b] = PALETTE[i % PALETTE.length];
        doc.setFillColor(r, g, b);
        doc.setDrawColor(255, 255, 255);
        doc.setLineWidth(0.4);
        doc.lines(lines, cx, cy, [1, 1], 'FD', true);
        currentAngle += sliceAngle;
    });
    // Legend
    const legendX = x + PIE_R * 2 + 12;
    let legendY = y + 6;
    items.forEach((row, i) => {
        const pct = ((vals[i] / total) * 100).toFixed(1);
        const [lr, lg, lb] = PALETTE[i % PALETTE.length];
        doc.setFillColor(lr, lg, lb);
        doc.rect(legendX, legendY - 3.2, 5.5, 4, 'F');
        doc.setFontSize(6.2);
        doc.setFont('helvetica', 'normal');
        doc.setTextColor(55, 65, 81);
        doc.text(
            `${String(row[labelKey] ?? '').substring(0, MAX_LABEL_LEN)}: ${fmtCell(vals[i])}  (${pct}%)`,
            legendX + 7.5, legendY
        );
        legendY += 8;
    });
    if (rows.length > maxSlices) {
        doc.setFontSize(5.5);
        doc.setFont('helvetica', 'italic');
        doc.setTextColor(156, 163, 175);
        doc.text(`… and ${rows.length - maxSlices} more`, legendX, legendY + 2);
        legendY += 7;
    }
    return Math.max(cy + PIE_R + 4, legendY + 2);
};

/**
 * Area chart (filled line) for a single time-series. Returns Y after chart.
 */
const drawAreaChart = (doc, rows, xKey, yKey, x, y, w, chartH = 44) => {
    if (rows.length < 2) return y;
    const ML = 16, MB = 10, MR = 6, MT = 2;
    const plotW = w - ML - MR, plotH = chartH - MT - MB;
    const bx = x + ML, by = y + MT;
    const n = rows.length;
    const vals = rows.map((r) => Number(r[yKey] ?? 0));
    const minV = vals.reduce((a, v) => (v < a ? v : a), vals[0]);
    const maxV = vals.reduce((a, v) => (v > a ? v : a), minV + 1);
    const range = maxV - minV;
    const [pr, pg, pb] = PALETTE[0];
    // Light fill: blend 30% palette colour into white
    const fillR = Math.round(pr * 0.30 + 255 * 0.70);
    const fillG = Math.round(pg * 0.30 + 255 * 0.70);
    const fillB = Math.round(pb * 0.30 + 255 * 0.70);
    // Background
    doc.setFillColor(249, 250, 251);
    doc.rect(bx, by, plotW, plotH, 'F');
    // Grid lines
    doc.setDrawColor(229, 231, 235);
    doc.setLineWidth(0.15);
    const baseY = by + plotH;
    [0, 0.25, 0.5, 0.75, 1].forEach((frac) => {
        const gy = by + frac * plotH;
        doc.line(bx, gy, bx + plotW, gy);
        if (frac === 0 || frac === 0.5 || frac === 1) {
            doc.setFontSize(4.8);
            doc.setTextColor(156, 163, 175);
            doc.text(fmtCell(maxV - frac * range), bx - 1, gy + 1.5, { align: 'right' });
        }
    });
    // Build area polygon: (bx, baseY) → first point → trace → last point → (lastPx, baseY) → close
    const lines = [];
    const firstPy = baseY - ((vals[0] - minV) / range) * plotH;
    lines.push([0, firstPy - baseY]); // go up to first data point
    let prevX = bx, prevPy = firstPy;
    for (let i = 1; i < n; i++) {
        const px = bx + (i / (n - 1)) * plotW;
        const py = baseY - ((vals[i] - minV) / range) * plotH;
        lines.push([px - prevX, py - prevPy]);
        prevX = px;
        prevPy = py;
    }
    lines.push([0, baseY - prevPy]); // go down to bottom-right (close will return to bx, baseY)
    doc.setFillColor(fillR, fillG, fillB);
    doc.setDrawColor(fillR, fillG, fillB);
    doc.setLineWidth(0);
    doc.lines(lines, bx, baseY, [1, 1], 'F', true);
    // Line on top
    doc.setDrawColor(pr, pg, pb);
    doc.setLineWidth(0.7);
    for (let i = 1; i < n; i++) {
        const x1 = bx + ((i - 1) / (n - 1)) * plotW;
        const y1 = baseY - ((vals[i - 1] - minV) / range) * plotH;
        const x2 = bx + (i / (n - 1)) * plotW;
        const y2 = baseY - ((vals[i] - minV) / range) * plotH;
        doc.line(x1, y1, x2, y2);
    }
    // Dots for small datasets
    doc.setFillColor(pr, pg, pb);
    if (n <= MAX_DOTS_THRESHOLD) {
        for (let i = 0; i < n; i++) {
            const px = bx + (i / (n - 1)) * plotW;
            const py = baseY - ((vals[i] - minV) / range) * plotH;
            doc.circle(px, py, 0.9, 'F');
        }
    }
    // X-axis labels (max 10)
    const step = Math.max(1, Math.ceil(n / 10));
    rows.forEach((row, i) => {
        if (i % step !== 0 && i !== n - 1) return;
        const px = bx + (i / (n - 1)) * plotW;
        doc.setFontSize(4.8);
        doc.setTextColor(156, 163, 175);
        doc.text(String(row[xKey] ?? '').substring(0, MAX_XAXIS_LEN), px, baseY + 7, { align: 'center' });
    });
    // Border
    doc.setDrawColor(209, 213, 219);
    doc.setLineWidth(0.2);
    doc.rect(bx, by, plotW, plotH, 'S');
    // Series label
    doc.setFontSize(5.5);
    doc.setFont('helvetica', 'normal');
    doc.setTextColor(pr, pg, pb);
    doc.text(colHeader(yKey), bx, baseY + MB - 1);
    return y + chartH + 3;
};

// ---------------------------------------------------------------------------
// Visualization type detection
// ---------------------------------------------------------------------------

const _DATE_FIELDS = new Set([
    'date', 'order_date', 'week_start', 'month_start', 'year_month',
    'review_date', 'week', 'month', 'year', 'period', 'created_at', 'month_year',
]);

const detectViz = (rows, columns) => {
    if (!rows?.length) return null;
    if (rows.length === 1) return { type: 'kpi' };
    const sample = rows[0];

    // Detect a date/time column
    const dateCol = columns.find((c) => {
        const lc = c.toLowerCase();
        if (_DATE_FIELDS.has(lc)) return true;
        if (
            lc.endsWith('_date') ||
            lc.includes('year_month') ||
            lc.includes('week_start') ||
            lc.includes('month_start')
        ) return true;
        // Inspect actual value format
        return /^\d{4}[-/]/.test(String(sample[c] ?? ''));
    });

    const numericCols = columns.filter((c) => {
        const v = sample[c];
        return typeof v === 'number' || (typeof v === 'string' && v !== '' && !isNaN(Number(v)));
    });

    const strCols = columns.filter((c) => {
        const v = sample[c];
        return typeof v === 'string' && isNaN(Number(v));
    });

    // Time-series → area (single series) or line (multi-series)
    if (dateCol && numericCols.length >= 1 && rows.length >= 3) {
        const yKeys = numericCols.filter((c) => c !== dateCol).slice(0, 3);
        if (yKeys.length === 1) return { type: 'area', xKey: dateCol, yKey: yKeys[0] };
        if (yKeys.length > 1) return { type: 'line', xKey: dateCol, yKeys };
    }

    // Categorical → choose chart type based on row count and data semantics
    if (strCols.length >= 1 && numericCols.length >= 1) {
        const labelKey =
            strCols.find((c) => LABEL_COL_RE.test(c.toLowerCase())) ?? strCols[0];
        const valueKey =
            numericCols.find((c) => VALUE_COL_RE.test(c.toLowerCase())) ?? numericCols[0];
        // Pie: small dataset that looks like distribution/proportion data
        const isProportion =
            PIE_COL_RE.test(valueKey.toLowerCase()) ||
            PIE_COL_RE.test(labelKey.toLowerCase()) ||
            columns.some((c) => PIE_COL_RE.test(c.toLowerCase()));
        if (rows.length <= 8 && isProportion) return { type: 'pie', labelKey, valueKey };
        // Vertical bar: small dataset (nice for side-by-side comparison)
        if (rows.length <= 8) return { type: 'vbar', labelKey, valueKey };
        // Horizontal bar: many rows
        return { type: 'bar', labelKey, valueKey };
    }

    return { type: 'table_only' };
};

// ---------------------------------------------------------------------------
// PDF builder
// ---------------------------------------------------------------------------

const buildPDF = ({ businessName, businessId, reportDate, sections, analyticsData, graphsOnly = false }) => {
    const doc = new jsPDF({ orientation: 'landscape', unit: 'mm', format: 'a4' });
    const PAGE_W = doc.internal.pageSize.getWidth();
    const PAGE_H = doc.internal.pageSize.getHeight();
    const SM = 12;           // side margin
    const CW = PAGE_W - 2 * SM; // usable content width
    const BOTTOM_MARGIN = 16; // reserved for footer

    // Re-usable: draw the indigo accent bar on the left edge
    const accentBar = () => {
        doc.setFillColor(99, 102, 241);
        doc.rect(0, 0, 6, PAGE_H, 'F');
    };

    // Add running footer to every page after all content is drawn
    const addFooter = () => {
        const total = doc.getNumberOfPages();
        for (let i = 1; i <= total; i++) {
            doc.setPage(i);
            doc.setFontSize(7.5);
            doc.setTextColor(150);
            doc.text(
                `Pulse Analytics Report  ·  ${businessName}  ·  ${reportDate}  ·  Page ${i} of ${total}`,
                PAGE_W / 2,
                PAGE_H - 5,
                { align: 'center' }
            );
        }
    };

    // ---- Cover page ----
    doc.setFillColor(17, 24, 39);
    doc.rect(0, 0, PAGE_W, PAGE_H, 'F');
    accentBar();
    doc.setTextColor(255, 255, 255);
    doc.setFontSize(36);
    doc.setFont('helvetica', 'bold');
    doc.text('Pulse Analytics', PAGE_W / 2, PAGE_H / 2 - 28, { align: 'center' });
    doc.setFontSize(18);
    doc.setFont('helvetica', 'normal');
    doc.text('Analytics & Insights Report', PAGE_W / 2, PAGE_H / 2 - 12, { align: 'center' });
    doc.setFontSize(13);
    doc.setTextColor(209, 213, 219);
    doc.text(`Business: ${businessName}`, PAGE_W / 2, PAGE_H / 2 + 6, { align: 'center' });
    doc.text(`Business ID: ${businessId}`, PAGE_W / 2, PAGE_H / 2 + 16, { align: 'center' });
    doc.text(`Generated: ${reportDate}`, PAGE_W / 2, PAGE_H / 2 + 26, { align: 'center' });
    doc.setFontSize(10);
    doc.setTextColor(107, 114, 128);
    doc.text(
        `This report contains ${sections.length} section(s) with full analytics data.`,
        PAGE_W / 2, PAGE_H / 2 + 44, { align: 'center' }
    );

    // ---- Table of Contents ----
    doc.addPage();
    doc.setFillColor(249, 250, 251);
    doc.rect(0, 0, PAGE_W, PAGE_H, 'F');
    accentBar();
    doc.setTextColor(17, 24, 39);
    doc.setFontSize(22);
    doc.setFont('helvetica', 'bold');
    doc.text('Table of Contents', 20, 22);
    doc.setFontSize(11);
    doc.setFont('helvetica', 'normal');
    doc.setTextColor(55, 65, 81);
    let tocY = 36;
    sections.forEach((sec, idx) => {
        doc.text(`${idx + 1}.  ${sec.label}`, 22, tocY);
        if (sec.subItems?.length) {
            doc.setFontSize(9);
            doc.setTextColor(107, 114, 128);
            doc.text(`     ${sec.subItems.join('  ·  ')}`, 22, tocY + 5);
            tocY += 10;
            doc.setFontSize(11);
            doc.setTextColor(55, 65, 81);
        } else {
            tocY += 8;
        }
    });

    // ---- Section data pages ----
    // curY tracks the vertical position across items packed onto the same page.
    // Each section starts on its own page; items within a section are packed
    // continuously and a new page is only added when there is not enough room.
    let curY = SM;

    const newPageFn = () => {
        doc.addPage();
        doc.setFillColor(249, 250, 251);
        doc.rect(0, 0, PAGE_W, PAGE_H, 'F');
        accentBar();
        curY = SM;
    };

    const ensureSpace = (needed) => {
        if (curY + needed > PAGE_H - BOTTOM_MARGIN) newPageFn();
    };

    sections.forEach((sec) => {
        const catData = analyticsData[sec.key] ?? {};

        const analyticsItems = sec.analyticsKeys
            .map((k) => ({ key: k, rows: catData[k] ?? [] }))
            .filter((item) => item.rows.length > 0);

        // Every section starts on its own fresh page
        newPageFn();

        // Section heading + divider
        doc.setTextColor(17, 24, 39);
        doc.setFontSize(17);
        doc.setFont('helvetica', 'bold');
        doc.text(`${sec.label}`, SM + 4, curY + 7);
        curY += 15;
        doc.setDrawColor(229, 231, 235);
        doc.setLineWidth(0.3);
        doc.line(SM, curY, PAGE_W - SM, curY);
        curY += 5;

        if (analyticsItems.length === 0) {
            doc.setFontSize(11);
            doc.setFont('helvetica', 'normal');
            doc.setTextColor(107, 114, 128);
            doc.text('No data available for this section.', SM + 4, curY + 6);
            return;
        }

        analyticsItems.forEach(({ key, rows }) => {
            const tableTitle = colHeader(key);
            const columns = Object.keys(rows[0]);
            const displayRows = rows.slice(0, MAX_TABLE_ROWS);
            const truncated = rows.length > MAX_TABLE_ROWS;
            const viz = detectViz(rows, columns);

            // Estimate how much vertical space the heading + viz will need so we
            // can decide whether to start a new page before drawing anything.
            const estVizH = (() => {
                if (!viz || viz.type === 'table_only') return 0;
                if (viz.type === 'kpi') {
                    const cnt = Math.min(
                        Object.keys(rows[0]).filter((k) => rows[0][k] != null).length, MAX_KPI_CARDS
                    );
                    return Math.ceil(cnt / 3) * 20 + 4;
                }
                if (viz.type === 'vbar') return 54;
                if (viz.type === 'pie') return Math.max(58, Math.min(rows.length, 8) * 8 + 14);
                if (viz.type === 'bar') return Math.min(rows.length, 12) * 7.4 + 8;
                return 50; // area / line
            })();

            // Need at least heading (13mm) + viz + small gap (8mm)
            ensureSpace(13 + estVizH + 8);

            // Item heading
            doc.setFontSize(11);
            doc.setFont('helvetica', 'bold');
            doc.setTextColor(55, 65, 81);
            doc.text(tableTitle, SM + 4, curY + 4);
            curY += 8;

            // Record count
            doc.setFontSize(6.5);
            doc.setFont('helvetica', 'normal');
            doc.setTextColor(107, 114, 128);
            doc.text(
                `${rows.length.toLocaleString()} record${rows.length !== 1 ? 's' : ''}`,
                SM + 4, curY
            );
            curY += 5;

            // ---- Visualization (chart / KPI cards) ----
            if (viz?.type === 'kpi') {
                curY = drawKPICards(doc, rows[0], SM + 4, curY, CW - 8);
            } else if (viz?.type === 'vbar') {
                curY = drawVBar(doc, rows, viz.labelKey, viz.valueKey, SM + 4, curY, CW - 8);
            } else if (viz?.type === 'pie') {
                curY = drawPieChart(doc, rows, viz.labelKey, viz.valueKey, SM + 4, curY, CW - 8);
            } else if (viz?.type === 'bar') {
                curY = drawHBar(doc, rows, viz.labelKey, viz.valueKey, SM + 4, curY, CW - 8);
            } else if (viz?.type === 'area') {
                curY = drawAreaChart(doc, rows, viz.xKey, viz.yKey, SM + 4, curY, CW - 8);
            } else if (viz?.type === 'line') {
                curY = drawLineChart(doc, rows, viz.xKey, viz.yKeys, SM + 4, curY, CW - 8);
            }
            curY += 3;

            // Truncation notice
            if (truncated) {
                doc.setFontSize(6.5);
                doc.setFont('helvetica', 'italic');
                doc.setTextColor(156, 163, 175);
                doc.text(
                    `Showing first ${MAX_TABLE_ROWS.toLocaleString()} of ${rows.length.toLocaleString()} rows`,
                    SM + 4, curY
                );
                curY += 4;
            }

            // ---- Full data table (skipped in "Graphs Only" mode) ----
            if (!graphsOnly) {
                // Scale font + min-col-width by column count to prevent vertical character wrapping
                const colCount = columns.length;
                const tFontSize = colCount > 20 ? 5.5 : colCount > 12 ? 6.5 : 7.5;
                const minCW = colCount > 20 ? 14 : colCount > 12 ? 17 : 20;

                autoTable(doc, {
                    head: [columns.map(colHeader)],
                    body: displayRows.map((row) => columns.map((c) => fmtCell(row[c]))),
                    startY: curY,
                    margin: { left: SM, right: SM },
                    styles: {
                        fontSize: tFontSize,
                        cellPadding: { top: 1.5, right: 2, bottom: 1.5, left: 2 },
                        overflow: 'ellipsize',
                        minCellWidth: minCW,
                        halign: 'left',
                    },
                    headStyles: {
                        fillColor: [99, 102, 241],
                        textColor: 255,
                        fontStyle: 'bold',
                        fontSize: tFontSize,
                    },
                    alternateRowStyles: { fillColor: [243, 244, 246] },
                    tableLineColor: [209, 213, 219],
                    tableLineWidth: 0.2,
                    // Re-draw the accent bar on pages autoTable creates internally
                    didDrawPage: () => {
                        doc.setFillColor(99, 102, 241);
                        doc.rect(0, 0, 6, PAGE_H, 'F');
                    },
                });
                // Track Y after autoTable (it may have added pages internally)
                curY = doc.lastAutoTable.finalY + 6;
            } else {
                curY += 5; // small gap between chart items in graphs-only mode
            }
        });
    });

    addFooter();
    return doc;
};

// ---------------------------------------------------------------------------
// HTML builder — inline SVG charts, self-contained file, no external deps
// ---------------------------------------------------------------------------

const HTML_PALETTE = ['#6366f1','#10b981','#f59e0b','#ef4444','#3b82f6','#a855f7','#ec4899','#14b8a6'];
const HTML_MAX_ROWS = 500;
// Vertical text baseline factor — positions SVG text at ~72% of the row height
// so it appears vertically centred within a bar row.
const SVG_TEXT_BASELINE = 0.72;

/** Escape special HTML characters to prevent XSS when inserting into HTML strings */
const _escHtml = (str) =>
    String(str ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');

/** Horizontal bar chart SVG */
const _svgHBar = (rows, labelKey, valueKey, maxBars = 15) => {
    const items = rows.slice(0, maxBars);
    if (!items.length) return '';
    const vals = items.map((r) => Math.max(0, Number(r[valueKey] ?? 0)));
    const maxV = vals.reduce((a, v) => (v > a ? v : a), 1);
    const W = 580, LW = 150, AREA = W - LW - 55, RH = 22, GAP = 5;
    const H = items.length * (RH + GAP) + 24;
    let s = `<svg viewBox="0 0 ${W} ${H}" width="${W}" height="${H}" xmlns="http://www.w3.org/2000/svg" style="max-width:100%">`;
    items.forEach((row, i) => {
        const y = 12 + i * (RH + GAP);
        const bl = (vals[i] / maxV) * AREA;
        const c = HTML_PALETTE[i % HTML_PALETTE.length];
        s += `<text x="${LW - 6}" y="${(y + RH * SVG_TEXT_BASELINE).toFixed(1)}" text-anchor="end" font-family="sans-serif" font-size="11" fill="#374151">${_escHtml(String(row[labelKey] ?? '').substring(0, MAX_LABEL_LEN))}</text>`;
        s += `<rect x="${LW}" y="${y}" width="${AREA}" height="${RH}" rx="3" fill="#e5e7eb"/>`;
        if (bl > 0) s += `<rect x="${LW}" y="${y}" width="${bl.toFixed(1)}" height="${RH}" rx="3" fill="${c}"/>`;
        s += `<text x="${LW + AREA + 6}" y="${(y + RH * SVG_TEXT_BASELINE).toFixed(1)}" font-family="sans-serif" font-size="10" fill="#6b7280">${_escHtml(fmtCell(vals[i]))}</text>`;
    });
    if (rows.length > maxBars) s += `<text x="${LW}" y="${H - 2}" font-family="sans-serif" font-size="9" fill="#9ca3af" font-style="italic">… and ${rows.length - maxBars} more</text>`;
    return s + '</svg>';
};

/** Vertical bar chart SVG */
const _svgVBar = (rows, labelKey, valueKey, maxBars = 10) => {
    const items = rows.slice(0, maxBars);
    if (!items.length) return '';
    const vals = items.map((r) => Math.max(0, Number(r[valueKey] ?? 0)));
    const maxV = vals.reduce((a, v) => (v > a ? v : a), 1);
    const W = 580, CH = 200, ML = 44, MR = 12, MB = 48, MT = 20;
    const plotW = W - ML - MR, plotH = CH - MT - MB;
    const n = items.length;
    const barW = Math.max(16, Math.min(55, (plotW / n) * 0.6));
    const spacing = (plotW - barW * n) / (n + 1);
    let s = `<svg viewBox="0 0 ${W} ${CH}" width="${W}" height="${CH}" xmlns="http://www.w3.org/2000/svg" style="max-width:100%">`;
    s += `<rect x="${ML}" y="${MT}" width="${plotW}" height="${plotH}" fill="#f9fafb"/>`;
    [0, 0.25, 0.5, 0.75, 1].forEach((f) => {
        const gy = MT + f * plotH;
        s += `<line x1="${ML}" y1="${gy.toFixed(1)}" x2="${ML + plotW}" y2="${gy.toFixed(1)}" stroke="#e5e7eb" stroke-width="0.6"/>`;
        if (f === 0 || f === 0.5 || f === 1) s += `<text x="${ML - 5}" y="${(gy + 3.5).toFixed(1)}" text-anchor="end" font-family="sans-serif" font-size="9" fill="#9ca3af">${fmtCell(maxV * (1 - f))}</text>`;
    });
    items.forEach((row, i) => {
        const bh = Math.max(1, (vals[i] / maxV) * plotH);
        const bx = ML + spacing + i * (barW + spacing);
        const by = MT + plotH - bh;
        const c = HTML_PALETTE[i % HTML_PALETTE.length];
        s += `<rect x="${bx.toFixed(1)}" y="${by.toFixed(1)}" width="${barW}" height="${bh.toFixed(1)}" rx="3" fill="${c}"/>`;
        s += `<text x="${(bx + barW / 2).toFixed(1)}" y="${Math.max(MT + 13, by - 3).toFixed(1)}" text-anchor="middle" font-family="sans-serif" font-size="9" font-weight="bold" fill="#374151">${_escHtml(fmtCell(vals[i]))}</text>`;
        s += `<text x="${(bx + barW / 2).toFixed(1)}" y="${(MT + plotH + 14).toFixed(1)}" text-anchor="middle" font-family="sans-serif" font-size="9" fill="#6b7280">${_escHtml(String(row[labelKey] ?? '').substring(0, 12))}</text>`;
    });
    s += `<rect x="${ML}" y="${MT}" width="${plotW}" height="${plotH}" fill="none" stroke="#d1d5db" stroke-width="0.5"/>`;
    return s + '</svg>';
};

/** Pie / donut chart SVG */
const _svgPie = (rows, labelKey, valueKey, maxSlices = 8) => {
    const items = rows.slice(0, maxSlices);
    if (!items.length) return '';
    const vals = items.map((r) => Math.max(0, Number(r[valueKey] ?? 0)));
    const total = vals.reduce((acc, v) => acc + v, 0);
    if (total === 0) return _svgHBar(rows, labelKey, valueKey);
    const R = 95, CX = 115, CY = 120, LX = 235;
    const W = 580, H = Math.max(260, items.length * 26 + 30);
    let s = `<svg viewBox="0 0 ${W} ${H}" width="${W}" height="${H}" xmlns="http://www.w3.org/2000/svg" style="max-width:100%">`;
    let ang = -Math.PI / 2;
    items.forEach((row, i) => {
        const sa = (vals[i] / total) * 2 * Math.PI;
        const ea = ang + sa;
        const x1 = CX + R * Math.cos(ang), y1 = CY + R * Math.sin(ang);
        const x2 = CX + R * Math.cos(ea), y2 = CY + R * Math.sin(ea);
        const lg = sa > Math.PI ? 1 : 0;
        const c = HTML_PALETTE[i % HTML_PALETTE.length];
        s += `<path d="M${CX},${CY} L${x1.toFixed(2)},${y1.toFixed(2)} A${R},${R},0,${lg},1,${x2.toFixed(2)},${y2.toFixed(2)} Z" fill="${c}" stroke="white" stroke-width="1.5"/>`;
        ang = ea;
    });
    items.forEach((row, i) => {
        const pct = ((vals[i] / total) * 100).toFixed(1);
        const c = HTML_PALETTE[i % HTML_PALETTE.length];
        const ly = 24 + i * 26;
        s += `<rect x="${LX}" y="${ly - 10}" width="14" height="14" rx="2" fill="${c}"/>`;
        s += `<text x="${LX + 20}" y="${ly}" font-family="sans-serif" font-size="11" fill="#374151">${_escHtml(String(row[labelKey] ?? '').substring(0, 24))}: ${_escHtml(fmtCell(vals[i]))} (${pct}%)</text>`;
    });
    if (rows.length > maxSlices) s += `<text x="${LX}" y="${24 + items.length * 26 + 12}" font-family="sans-serif" font-size="9" fill="#9ca3af" font-style="italic">&#8230; and ${rows.length - maxSlices} more</text>`;
    return s + '</svg>';
};

/** Multi-series line chart SVG */
const _svgLine = (rows, xKey, yKeys) => {
    if (rows.length < 2) return '';
    const W = 640, CH = 220, ML = 50, MR = 16, MB = 42, MT = 16;
    const plotW = W - ML - MR, plotH = CH - MT - MB;
    const n = rows.length;
    const allVals = yKeys.flatMap((yk) => rows.map((r) => Number(r[yk] ?? 0)));
    const minV = allVals.reduce((a, v) => (v < a ? v : a), allVals[0]);
    const maxV = allVals.reduce((a, v) => (v > a ? v : a), minV + 1);
    const range = maxV - minV;
    let s = `<svg viewBox="0 0 ${W} ${CH}" width="${W}" height="${CH}" xmlns="http://www.w3.org/2000/svg" style="max-width:100%">`;
    s += `<rect x="${ML}" y="${MT}" width="${plotW}" height="${plotH}" fill="#f9fafb"/>`;
    [0, 0.25, 0.5, 0.75, 1].forEach((f) => {
        const gy = MT + f * plotH;
        s += `<line x1="${ML}" y1="${gy.toFixed(1)}" x2="${ML + plotW}" y2="${gy.toFixed(1)}" stroke="#e5e7eb" stroke-width="0.5"/>`;
        if (f === 0 || f === 0.5 || f === 1) s += `<text x="${ML - 5}" y="${(gy + 3.5).toFixed(1)}" text-anchor="end" font-family="sans-serif" font-size="9" fill="#9ca3af">${fmtCell(maxV - f * range)}</text>`;
    });
    const step = Math.max(1, Math.ceil(n / 10));
    rows.forEach((row, i) => {
        if (i % step !== 0 && i !== n - 1) return;
        const px = ML + (i / (n - 1)) * plotW;
        s += `<text x="${px.toFixed(1)}" y="${(MT + plotH + 14).toFixed(1)}" text-anchor="middle" font-family="sans-serif" font-size="9" fill="#9ca3af">${_escHtml(String(row[xKey] ?? '').substring(0, 10))}</text>`;
    });
    yKeys.forEach((yk, si) => {
        const c = HTML_PALETTE[si % HTML_PALETTE.length];
        const pts = rows.map((row, i) => {
            const px = ML + (i / (n - 1)) * plotW;
            const py = MT + plotH - ((Number(row[yk] ?? 0) - minV) / range) * plotH;
            return `${px.toFixed(1)},${py.toFixed(1)}`;
        }).join(' ');
        s += `<polyline points="${pts}" fill="none" stroke="${c}" stroke-width="2"/>`;
        if (n <= 40) rows.forEach((row, i) => {
            const px = ML + (i / (n - 1)) * plotW;
            const py = MT + plotH - ((Number(row[yk] ?? 0) - minV) / range) * plotH;
            s += `<circle cx="${px.toFixed(1)}" cy="${py.toFixed(1)}" r="3" fill="${c}"/>`;
        });
        if (yKeys.length > 1) {
            s += `<rect x="${ML + si * 100}" y="${MT + plotH + MB - 10}" width="14" height="8" rx="1" fill="${c}"/>`;
            s += `<text x="${ML + si * 100 + 18}" y="${MT + plotH + MB - 3}" font-family="sans-serif" font-size="9" fill="#6b7280">${_escHtml(colHeader(yk))}</text>`;
        }
    });
    s += `<rect x="${ML}" y="${MT}" width="${plotW}" height="${plotH}" fill="none" stroke="#d1d5db" stroke-width="0.5"/>`;
    return s + '</svg>';
};

/** Single-series filled area chart SVG */
const _svgArea = (rows, xKey, yKey) => {
    if (rows.length < 2) return '';
    const W = 640, CH = 210, ML = 50, MR = 16, MB = 36, MT = 16;
    const plotW = W - ML - MR, plotH = CH - MT - MB;
    const n = rows.length;
    const vals = rows.map((r) => Number(r[yKey] ?? 0));
    const minV = vals.reduce((a, v) => (v < a ? v : a), vals[0]);
    const maxV = vals.reduce((a, v) => (v > a ? v : a), minV + 1);
    const range = maxV - minV;
    const baseY = MT + plotH;
    const c = HTML_PALETTE[0];
    let pathD = `M ${ML},${baseY}`;
    rows.forEach((row, i) => {
        const px = ML + (i / (n - 1)) * plotW;
        const py = MT + plotH - ((vals[i] - minV) / range) * plotH;
        pathD += ` L ${px.toFixed(1)},${py.toFixed(1)}`;
    });
    pathD += ` L ${ML + plotW},${baseY} Z`;
    const pts = rows.map((row, i) => {
        const px = ML + (i / (n - 1)) * plotW;
        const py = MT + plotH - ((vals[i] - minV) / range) * plotH;
        return `${px.toFixed(1)},${py.toFixed(1)}`;
    }).join(' ');
    let s = `<svg viewBox="0 0 ${W} ${CH}" width="${W}" height="${CH}" xmlns="http://www.w3.org/2000/svg" style="max-width:100%">`;
    s += `<rect x="${ML}" y="${MT}" width="${plotW}" height="${plotH}" fill="#f9fafb"/>`;
    [0, 0.25, 0.5, 0.75, 1].forEach((f) => {
        const gy = MT + f * plotH;
        s += `<line x1="${ML}" y1="${gy.toFixed(1)}" x2="${ML + plotW}" y2="${gy.toFixed(1)}" stroke="#e5e7eb" stroke-width="0.5"/>`;
        if (f === 0 || f === 0.5 || f === 1) s += `<text x="${ML - 5}" y="${(gy + 3.5).toFixed(1)}" text-anchor="end" font-family="sans-serif" font-size="9" fill="#9ca3af">${fmtCell(maxV - f * range)}</text>`;
    });
    const step = Math.max(1, Math.ceil(n / 10));
    rows.forEach((row, i) => {
        if (i % step !== 0 && i !== n - 1) return;
        const px = ML + (i / (n - 1)) * plotW;
        s += `<text x="${px.toFixed(1)}" y="${(baseY + 14).toFixed(1)}" text-anchor="middle" font-family="sans-serif" font-size="9" fill="#9ca3af">${_escHtml(String(row[xKey] ?? '').substring(0, 10))}</text>`;
    });
    s += `<path d="${pathD}" fill="${c}" opacity="0.2"/>`;
    s += `<polyline points="${pts}" fill="none" stroke="${c}" stroke-width="2"/>`;
    if (n <= 40) rows.forEach((row, i) => {
        const px = ML + (i / (n - 1)) * plotW;
        const py = MT + plotH - ((vals[i] - minV) / range) * plotH;
        s += `<circle cx="${px.toFixed(1)}" cy="${py.toFixed(1)}" r="3" fill="${c}"/>`;
    });
    s += `<rect x="${ML}" y="${MT}" width="${plotW}" height="${plotH}" fill="none" stroke="#d1d5db" stroke-width="0.5"/>`;
    return s + '</svg>';
};

/** KPI card grid HTML */
const _htmlKPI = (row) => {
    const entries = Object.entries(row).filter(([, v]) => v != null);
    const cards = entries.map(([key, val], i) => {
        const c = HTML_PALETTE[i % HTML_PALETTE.length];
        return `<div class="kpi-card"><div class="kpi-accent" style="background:${c}"></div><div class="kpi-value" style="color:${c}">${_escHtml(fmtCell(val))}</div><div class="kpi-label">${_escHtml(colHeader(key))}</div></div>`;
    }).join('');
    return `<div class="kpi-grid">${cards}</div>`;
};

/** Full data table HTML (up to HTML_MAX_ROWS rows) */
const _htmlTable = (rows, columns) => {
    const display = rows.slice(0, HTML_MAX_ROWS);
    const head = columns.map((c) => `<th>${_escHtml(colHeader(c))}</th>`).join('');
    const body = display.map((row, ri) => {
        const cls = ri % 2 === 1 ? ' class="alt"' : '';
        return `<tr${cls}>${columns.map((c) => `<td>${_escHtml(fmtCell(row[c]))}</td>`).join('')}</tr>`;
    }).join('');
    const note = rows.length > HTML_MAX_ROWS
        ? `<p class="trunc">Showing first ${HTML_MAX_ROWS} of ${rows.length.toLocaleString()} records</p>`
        : '';
    return `<div class="table-wrap"><table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>${note}</div>`;
};

const buildHTML = ({ businessName, businessId, reportDate, sections, analyticsData, graphsOnly = false }) => {
    const css = `body{font-family:system-ui,sans-serif;background:#f9fafb;color:#111827;margin:0;padding:0}
.cover{background:#111827;color:#fff;padding:60px 48px;border-left:8px solid #6366f1}
.cover h1{font-size:2.4rem;margin:0 0 8px}.cover p{color:#d1d5db;margin:4px 0}
.toc{background:#fff;padding:40px 48px;border-left:8px solid #6366f1;margin-bottom:2px}
.toc h2{font-size:1.4rem;color:#111827;margin:0 0 20px}
.toc-item{padding:6px 0;border-bottom:1px solid #f3f4f6;font-size:.95rem}
.toc-sub{font-size:.8rem;color:#9ca3af;margin-top:3px}
.section{background:#fff;margin:16px 0;padding:32px 48px;border-top:4px solid #6366f1}
.section-title{font-size:1.5rem;font-weight:700;color:#111827;margin:0 0 4px}
.section-hr{border:none;border-top:1px solid #e5e7eb;margin:12px 0 20px}
.analytic-item{margin-bottom:32px;padding-bottom:20px;border-bottom:1px solid #f3f4f6}
.analytic-item:last-child{border-bottom:none}
.analytic-title{font-size:1rem;font-weight:600;color:#374151;margin:0 0 4px}
.record-count{font-size:.75rem;color:#9ca3af;margin-bottom:12px}
.kpi-grid{display:flex;flex-wrap:wrap;gap:12px;margin-bottom:12px}
.kpi-card{background:#f3f4f6;border-radius:8px;width:200px;overflow:hidden;border:1px solid #e5e7eb}
.kpi-accent{height:4px}.kpi-value{font-size:1.3rem;font-weight:700;text-align:center;padding:14px 8px 4px}
.kpi-label{font-size:.72rem;color:#6b7280;text-align:center;padding:0 6px 12px}
.table-wrap{overflow-x:auto;margin-top:12px}
table{border-collapse:collapse;width:100%;font-size:.78rem}
thead tr{background:#6366f1;color:#fff}
th{padding:7px 10px;text-align:left;white-space:nowrap;font-weight:600}
td{padding:6px 10px;color:#374151;border-bottom:1px solid #f3f4f6}
tr.alt td{background:#f9fafb}
.trunc{font-size:.72rem;color:#9ca3af;margin:6px 0 0;font-style:italic}
.no-data{color:#9ca3af;font-size:.9rem;padding:16px 0}
footer{background:#111827;color:#6b7280;text-align:center;padding:24px;font-size:.75rem;margin-top:32px}
svg{display:block;margin-bottom:12px}`;

    const toc = sections.map((s) => {
        const sub = s.subItems ? `<div class="toc-sub">${s.subItems.map(_escHtml).join(' · ')}</div>` : '';
        return `<div class="toc-item"><strong>${_escHtml(s.label)}</strong>${sub}</div>`;
    }).join('');

    const body = sections.map((sec) => {
        const catData = analyticsData[sec.key] ?? {};
        const items = sec.analyticsKeys
            .map((k) => ({ key: k, rows: catData[k] ?? [] }))
            .filter((item) => item.rows.length > 0);

        const content = items.length === 0
            ? '<p class="no-data">No data available for this section.</p>'
            : items.map(({ key, rows }) => {
                const columns = Object.keys(rows[0]);
                const viz = detectViz(rows, columns);
                let chart = '';
                if (viz?.type === 'kpi') chart = _htmlKPI(rows[0]);
                else if (viz?.type === 'vbar') chart = _svgVBar(rows, viz.labelKey, viz.valueKey);
                else if (viz?.type === 'pie') chart = _svgPie(rows, viz.labelKey, viz.valueKey);
                else if (viz?.type === 'bar') chart = _svgHBar(rows, viz.labelKey, viz.valueKey);
                else if (viz?.type === 'area') chart = _svgArea(rows, viz.xKey, viz.yKey);
                else if (viz?.type === 'line') chart = _svgLine(rows, viz.xKey, viz.yKeys);
                const table = !graphsOnly ? _htmlTable(rows, columns) : '';
                return `<div class="analytic-item"><h3 class="analytic-title">${_escHtml(colHeader(key))}</h3><p class="record-count">${rows.length.toLocaleString()} record${rows.length !== 1 ? 's' : ''}</p>${chart}${table}</div>`;
            }).join('');

        return `<div class="section"><h2 class="section-title">${_escHtml(sec.label)}</h2><hr class="section-hr">${content}</div>`;
    }).join('');

    const eBizName = _escHtml(businessName);
    const eBizId = _escHtml(businessId);
    const eDate = _escHtml(reportDate);
    return `<!DOCTYPE html>\n<html lang="en">\n<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Pulse Analytics \u2014 ${eBizName}</title>\n<style>${css}</style>\n</head><body>\n<div class="cover">\n  <h1>Pulse Analytics Report</h1>\n  <p style="font-size:1.1rem;margin:8px 0 20px;color:#e5e7eb">Analytics &amp; Insights Report</p>\n  <p><strong>Business:</strong> ${eBizName}</p>\n  <p><strong>Business ID:</strong> ${eBizId}</p>\n  <p><strong>Generated:</strong> ${eDate}</p>\n  <p><strong>Sections:</strong> ${sections.length}</p>\n</div>\n<div class="toc"><h2>Table of Contents</h2>${toc}</div>\n${body}\n<footer>Pulse Analytics \u00B7 ${eBizName} \u00B7 ${eDate}</footer>\n</body></html>`;
};

const ExportAnalytics = () => {
    const { businessId } = useParams();
    const { user } = useAuth();
    const toastRef = useRef(null);

    const allKeys = EXPORT_SECTIONS.map((s) => s.key);
    const [selected, setSelected] = useState(new Set(allKeys));
    const [exportMode, setExportMode] = useState('graphs_and_tables');
    const [isExporting, setIsExporting] = useState(false);
    const [exportStep, setExportStep] = useState('');

    const toggleSection = useCallback((key) => {
        setSelected((prev) => {
            const next = new Set(prev);
            if (next.has(key)) next.delete(key);
            else next.add(key);
            return next;
        });
    }, []);

    const handleSelectAll = () => setSelected(new Set(allKeys));
    const handleSelectNone = () => setSelected(new Set());

    const handleExport = useCallback(async () => {
        if (!businessId) {
            toastRef.current?.show({
                severity: 'warn',
                summary: 'No Business Selected',
                detail: 'Please select a business before exporting.',
                life: 4000,
            });
            return;
        }
        if (selected.size === 0) {
            toastRef.current?.show({
                severity: 'warn',
                summary: 'Nothing Selected',
                detail: 'Please select at least one section to export.',
                life: 4000,
            });
            return;
        }

        setIsExporting(true);
        setExportStep('Fetching business info…');

        try {
            // 1. Get business name
            let businessName = businessId;
            try {
                const bizRes = await axiosInstance.get('/analytics/get-businesses', {
                    params: { userId: user?.user_id },
                });
                const match = (bizRes.data.businesses ?? []).find(
                    (b) => b.business_id === businessId
                );
                if (match) businessName = match.business_name;
            } catch {
                // non-fatal — use id as fallback
            }

            // 2. Collect unique categories needed for selected non-forecast sections
            const selectedSections = EXPORT_SECTIONS.filter((s) => selected.has(s.key));
            const regularSections  = selectedSections.filter((s) => !s.isForecast);
            const forecastSection  = selectedSections.find((s) => s.isForecast);

            const categorySet = new Set();
            regularSections.forEach((s) => s.categories.forEach((c) => categorySet.add(c)));

            setExportStep('Fetching analytics data…');
            const base = import.meta.env.VITE_API_URL || 'http://localhost:8000';

            // 3a. Fetch regular analytics (skip if only forecasts selected)
            let cats = {};
            if (categorySet.size > 0) {
                const dataRes = await fetch(
                    `${base}/analytics/data/${businessId}?categories=${encodeURIComponent([...categorySet].join(','))}`
                );
                if (!dataRes.ok) {
                    throw new Error('Analytics data not available. Please run the analytics pipeline first.');
                }
                const dataJson = await dataRes.json();
                cats = dataJson.categories ?? {};
            }

            // 3b. Fetch forecast inferences (if forecasts section selected)
            let forecastInferences = {};
            if (forecastSection) {
                setExportStep('Fetching forecast data…');
                try {
                    const fRes = await fetch(`${base}/analytics/forecasts/${businessId}?row_limit=500`);
                    if (fRes.ok) {
                        const fJson = await fRes.json();
                        forecastInferences = fJson.inferences ?? {};
                    }
                } catch (e) {
                    // Non-fatal: ML pipeline may not have run yet; section will show
                    // "No data available" in the exported report instead of failing.
                    console.warn('[ExportAnalytics] forecast fetch unavailable:', e?.message);
                }
            }

            // 3c. Build per-section flat lookup: section.key → { analyticsKey → rows[] }
            const analyticsData = {};
            regularSections.forEach((sec) => {
                const lookup = {};
                sec.categories.forEach((cat) => {
                    const catAnalytics = cats[cat]?.analytics ?? {};
                    sec.analyticsKeys.forEach((k) => {
                        if (catAnalytics[k]) {
                            lookup[k] = catAnalytics[k].data ?? [];
                        }
                    });
                });
                analyticsData[sec.key] = lookup;
            });
            if (forecastSection) {
                const lookup = {};
                forecastSection.analyticsKeys.forEach((k) => {
                    if (forecastInferences[k]?.data?.length) {
                        lookup[k] = forecastInferences[k].data;
                    }
                });
                analyticsData[forecastSection.key] = lookup;
            }

            setExportStep('Generating PDF…');
            const reportDate = new Date().toLocaleString('en-US', {
                year: 'numeric',
                month: 'long',
                day: 'numeric',
                hour: '2-digit',
                minute: '2-digit',
            });

            const doc = buildPDF({
                businessName,
                businessId,
                reportDate,
                sections: selectedSections,
                analyticsData,
                graphsOnly: exportMode === 'graphs_only',
            });

            // 4. Record export in backend (non-fatal if it fails)
            const filenameSafeName = businessName.replace(/[^a-zA-Z0-9_-]/g, '_');
            const safeDate = new Date()
                .toISOString()
                .slice(0, 10)
                .replace(/-/g, '');
            const fileName = `Pulse_Analytics_${filenameSafeName}_${safeDate}.pdf`;

            try {
                await axiosInstance.post('/analytics/exports', {
                    business_id: businessId,
                    user_id: user?.user_id,
                    file_name: fileName,
                    sections_exported: selectedSections.map((s) => s.label),
                    total_sections: selectedSections.length,
                });
            } catch {
                // non-fatal
            }

            // 5. Download
            await doc.save(fileName);

            toastRef.current?.show({
                severity: 'success',
                summary: 'Export Complete',
                detail: `${fileName} downloaded successfully.`,
                life: 5000,
            });
        } catch (err) {
            console.error('[ExportAnalytics]', err);
            toastRef.current?.show({
                severity: 'error',
                summary: 'Export Failed',
                detail: 'Could not generate the PDF report. Please try again.',
                life: 5000,
            });
        } finally {
            setIsExporting(false);
            setExportStep('');
        }
    }, [businessId, selected, exportMode, user]);

    const handleExportHTML = useCallback(async () => {
        if (!businessId) {
            toastRef.current?.show({
                severity: 'warn',
                summary: 'No Business Selected',
                detail: 'Please select a business before exporting.',
                life: 4000,
            });
            return;
        }
        if (selected.size === 0) {
            toastRef.current?.show({
                severity: 'warn',
                summary: 'Nothing Selected',
                detail: 'Please select at least one section to export.',
                life: 4000,
            });
            return;
        }

        setIsExporting(true);
        setExportStep('Fetching business info…');

        try {
            let businessName = businessId;
            try {
                const bizRes = await axiosInstance.get('/analytics/get-businesses', {
                    params: { userId: user?.user_id },
                });
                const match = (bizRes.data.businesses ?? []).find(
                    (b) => b.business_id === businessId
                );
                if (match) businessName = match.business_name;
            } catch {
                // non-fatal
            }

            const selectedSections = EXPORT_SECTIONS.filter((s) => selected.has(s.key));
            const regularSections  = selectedSections.filter((s) => !s.isForecast);
            const forecastSection  = selectedSections.find((s) => s.isForecast);

            const categorySet = new Set();
            regularSections.forEach((s) => s.categories.forEach((c) => categorySet.add(c)));

            setExportStep('Fetching analytics data…');
            const base = import.meta.env.VITE_API_URL || 'http://localhost:8000';

            let cats = {};
            if (categorySet.size > 0) {
                const dataRes = await fetch(
                    `${base}/analytics/data/${businessId}?categories=${encodeURIComponent([...categorySet].join(','))}`
                );
                if (!dataRes.ok) {
                    throw new Error('Analytics data not available. Please run the analytics pipeline first.');
                }
                const dataJson = await dataRes.json();
                cats = dataJson.categories ?? {};
            }

            let forecastInferences = {};
            if (forecastSection) {
                setExportStep('Fetching forecast data…');
                try {
                    const fRes = await fetch(`${base}/analytics/forecasts/${businessId}?row_limit=500`);
                    if (fRes.ok) {
                        const fJson = await fRes.json();
                        forecastInferences = fJson.inferences ?? {};
                    }
                } catch (e) {
                    // Non-fatal: ML pipeline may not have run yet; section will show
                    // "No data available" in the exported report instead of failing.
                    console.warn('[ExportAnalytics HTML] forecast fetch unavailable:', e?.message);
                }
            }

            const analyticsData = {};
            regularSections.forEach((sec) => {
                const lookup = {};
                sec.categories.forEach((cat) => {
                    const catAnalytics = cats[cat]?.analytics ?? {};
                    sec.analyticsKeys.forEach((k) => {
                        if (catAnalytics[k]) lookup[k] = catAnalytics[k].data ?? [];
                    });
                });
                analyticsData[sec.key] = lookup;
            });
            if (forecastSection) {
                const lookup = {};
                forecastSection.analyticsKeys.forEach((k) => {
                    if (forecastInferences[k]?.data?.length) {
                        lookup[k] = forecastInferences[k].data;
                    }
                });
                analyticsData[forecastSection.key] = lookup;
            }

            setExportStep('Generating HTML…');
            const reportDate = new Date().toLocaleString('en-US', {
                year: 'numeric',
                month: 'long',
                day: 'numeric',
                hour: '2-digit',
                minute: '2-digit',
            });

            const html = buildHTML({
                businessName,
                businessId,
                reportDate,
                sections: selectedSections,
                analyticsData,
                graphsOnly: exportMode === 'graphs_only',
            });

            const filenameSafeName = businessName.replace(/[^a-zA-Z0-9_-]/g, '_');
            const safeDate = new Date().toISOString().slice(0, 10).replace(/-/g, '');
            const fileName = `Pulse_Analytics_${filenameSafeName}_${safeDate}.html`;

            try {
                await axiosInstance.post('/analytics/exports', {
                    business_id: businessId,
                    user_id: user?.user_id,
                    file_name: fileName,
                    sections_exported: selectedSections.map((s) => s.label),
                    total_sections: selectedSections.length,
                });
            } catch {
                // non-fatal
            }

            const blob = new Blob([html], { type: 'text/html' });
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = fileName;
            a.click();
            URL.revokeObjectURL(url);

            toastRef.current?.show({
                severity: 'success',
                summary: 'Export Complete',
                detail: `${fileName} downloaded successfully.`,
                life: 5000,
            });
        } catch (err) {
            console.error('[ExportAnalytics HTML]', err);
            toastRef.current?.show({
                severity: 'error',
                summary: 'Export Failed',
                detail: 'Could not generate the HTML report. Please try again.',
                life: 5000,
            });
        } finally {
            setIsExporting(false);
            setExportStep('');
        }
    }, [businessId, selected, exportMode, user]);

    return (
        <div className="p-6 space-y-6 max-w-4xl">
            <Toast ref={toastRef} />

            {/* Page header */}
            <div>
                <Heading level={2} className="text-2xl font-bold text-gray-800">
                    <i className="pi pi-file-export mr-2 text-indigo-600" />Export Analytics
                </Heading>
                <Text className="text-gray-500 mt-1">
                    Select the sections you want to include in the exported report. All data tables
                    will be exported in full (no pagination). Use <strong>Graphs &amp; Tables</strong> to
                    include both visualizations and raw data, or <strong>Graphs Only</strong> for a
                    compact chart-only report. Export as <strong>PDF</strong> for printing or sharing,
                    or as <strong>HTML</strong> for interactive browsing.
                </Text>
            </div>

            {/* Export mode radio buttons */}
            <div className="flex items-center gap-6 p-4 bg-gray-50 border border-gray-200 rounded-xl">
                <span className="text-sm font-semibold text-gray-700 mr-1">Export Format:</span>
                <label className="flex items-center gap-2 cursor-pointer select-none">
                    <RadioButton
                        inputId="mode_graphs_only"
                        name="exportMode"
                        value="graphs_only"
                        checked={exportMode === 'graphs_only'}
                        onChange={(e) => setExportMode(e.value)}
                        disabled={isExporting}
                    />
                    <span className="text-sm text-gray-700">Insert Graphs Only</span>
                </label>
                <label className="flex items-center gap-2 cursor-pointer select-none">
                    <RadioButton
                        inputId="mode_graphs_and_tables"
                        name="exportMode"
                        value="graphs_and_tables"
                        checked={exportMode === 'graphs_and_tables'}
                        onChange={(e) => setExportMode(e.value)}
                        disabled={isExporting}
                    />
                    <span className="text-sm text-gray-700">Insert Graphs &amp; Tables</span>
                </label>
            </div>

            {/* Select All / None */}
            <div className="flex items-center gap-3">
                <SecondaryButton
                    label="Select All"
                    icon="pi pi-check-square"
                    onClick={handleSelectAll}
                    disabled={isExporting}
                    success
                />
                <SecondaryButton
                    label="Select None"
                    icon="pi pi-stop"
                    onClick={handleSelectNone}
                    disabled={isExporting}
                    black
                />
                <span className="text-sm text-gray-500">
                    {selected.size} of {EXPORT_SECTIONS.length} section{selected.size !== 1 ? 's' : ''} selected
                </span>
            </div>

            {/* Section list */}
            <div className="bg-white border border-gray-200 rounded-xl shadow-sm divide-y divide-gray-100">
                {EXPORT_SECTIONS.map((sec) => (
                    <label
                        key={sec.key}
                        className="flex items-center gap-4 px-5 py-4 cursor-pointer hover:bg-gray-50 transition-colors"
                    >
                        <Checkbox
                            checked={selected.has(sec.key)}
                            onChange={() => toggleSection(sec.key)}
                            disabled={isExporting}
                        />
                        <i className={`pi ${sec.icon} text-base text-indigo-600`} />
                        <div className="flex-1 min-w-0">
                            <p className="text-sm font-semibold text-gray-900">{sec.label}</p>
                            {sec.subItems && (
                                <p className="text-xs text-gray-400 mt-0.5 truncate">
                                    {sec.subItems.join('  ·  ')}
                                </p>
                            )}
                        </div>
                        <span className="text-xs text-gray-400 shrink-0">
                            {sec.analyticsKeys.length} metric{sec.analyticsKeys.length !== 1 ? 's' : ''}
                        </span>
                    </label>
                ))}
            </div>

            {/* Export buttons */}
            <div className="flex flex-wrap items-center gap-4 pt-2">
                <PrimaryButton
                    label={isExporting ? exportStep || 'Exporting…' : 'Export to PDF'}
                    icon="pi pi-file-pdf"
                    onClick={handleExport}
                    loading={isExporting}
                    disabled={isExporting || selected.size === 0 || !businessId}
                    info
                />
                <PrimaryButton
                    label={isExporting ? exportStep || 'Exporting…' : 'Export to HTML'}
                    icon="pi pi-code"
                    onClick={handleExportHTML}
                    loading={isExporting}
                    disabled={isExporting || selected.size === 0 || !businessId}
                />
                {!businessId && (
                    <Text className="text-sm text-amber-600">
                        <i className="pi pi-exclamation-triangle mr-1" />
                        Select a business from the dropdown above to enable export.
                    </Text>
                )}
            </div>

            {/* Exporting overlay feedback */}
            {isExporting && (
                <div className="flex items-center gap-3 p-4 bg-blue-50 border border-blue-200 rounded-xl">
                    <ProgressSpinner style={{ width: '28px', height: '28px' }} strokeWidth="4" />
                    <Text className="text-blue-700 text-sm font-medium">{exportStep}</Text>
                </div>
            )}
        </div>
    );
};

export default ExportAnalytics;

