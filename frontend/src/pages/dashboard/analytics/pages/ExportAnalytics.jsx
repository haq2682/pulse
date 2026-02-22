import React, { useState, useCallback, useRef } from 'react';
import { useParams } from 'react-router-dom';
import { ProgressSpinner } from 'primereact/progressspinner';
import { Toast } from 'primereact/toast';
import { Checkbox } from 'primereact/checkbox';
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
        emoji: '📊',
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
        emoji: '👥',
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
        emoji: '🛍️',
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
        emoji: '📦',
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
        emoji: '🤝',
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
        emoji: '🎯',
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
        emoji: '🛒',
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
        emoji: '💳',
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
        emoji: '🚚',
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
        emoji: '🔗',
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
        emoji: '⭐',
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
        emoji: '📈',
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
    const minV = Math.min(...allVals), maxV = Math.max(...allVals, minV + 1);
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
        if (n <= 40) {
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
const drawKPICards = (doc, row, x, y, w, maxCards = 24) => {
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

    // Time-series → line chart
    if (dateCol && numericCols.length >= 1 && rows.length >= 3) {
        const yKeys = numericCols.filter((c) => c !== dateCol).slice(0, 3);
        if (yKeys.length) return { type: 'line', xKey: dateCol, yKeys };
    }

    // Categorical → bar chart
    if (strCols.length >= 1 && numericCols.length >= 1) {
        const labelKey =
            strCols.find((c) => LABEL_COL_RE.test(c.toLowerCase())) ?? strCols[0];
        const valueKey =
            numericCols.find((c) => VALUE_COL_RE.test(c.toLowerCase())) ?? numericCols[0];
        return { type: 'bar', labelKey, valueKey };
    }

    return { type: 'table_only' };
};

// ---------------------------------------------------------------------------
// PDF builder
// ---------------------------------------------------------------------------

const buildPDF = ({ businessName, businessId, reportDate, sections, analyticsData }) => {
    const doc = new jsPDF({ orientation: 'landscape', unit: 'mm', format: 'a4' });
    const PAGE_W = doc.internal.pageSize.getWidth();
    const PAGE_H = doc.internal.pageSize.getHeight();
    const SM = 12;           // side margin
    const CW = PAGE_W - 2 * SM; // usable content width

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
        doc.text(`${idx + 1}.  ${sec.emoji}  ${sec.label}`, 22, tocY);
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
    sections.forEach((sec) => {
        const catData = analyticsData[sec.key] ?? {};

        const analyticsItems = sec.analyticsKeys
            .map((k) => ({ key: k, rows: catData[k] ?? [] }))
            .filter((item) => item.rows.length > 0);

        if (analyticsItems.length === 0) {
            doc.addPage();
            doc.setFillColor(249, 250, 251);
            doc.rect(0, 0, PAGE_W, PAGE_H, 'F');
            accentBar();
            doc.setTextColor(17, 24, 39);
            doc.setFontSize(20);
            doc.setFont('helvetica', 'bold');
            doc.text(`${sec.emoji}  ${sec.label}`, SM + 4, 22);
            doc.setFontSize(12);
            doc.setFont('helvetica', 'normal');
            doc.setTextColor(107, 114, 128);
            doc.text('No data available for this section.', SM + 4, 40);
            return;
        }

        analyticsItems.forEach(({ key, rows }, itemIdx) => {
            const tableTitle = colHeader(key);
            const columns = Object.keys(rows[0]);
            const displayRows = rows.slice(0, MAX_TABLE_ROWS);
            const truncated = rows.length > MAX_TABLE_ROWS;
            const viz = detectViz(rows, columns);

            // Each analytics item starts on a fresh page
            doc.addPage();
            doc.setFillColor(249, 250, 251);
            doc.rect(0, 0, PAGE_W, PAGE_H, 'F');
            accentBar();

            let curY = SM;

            // Section header on the first item of this section
            if (itemIdx === 0) {
                doc.setTextColor(17, 24, 39);
                doc.setFontSize(18);
                doc.setFont('helvetica', 'bold');
                doc.text(`${sec.emoji}  ${sec.label}`, SM + 4, curY + 7);
                curY += 16;
                doc.setDrawColor(229, 231, 235);
                doc.setLineWidth(0.3);
                doc.line(SM, curY, PAGE_W - SM, curY);
                curY += 5;
            }

            // Analytics item heading
            doc.setFontSize(13);
            doc.setFont('helvetica', 'bold');
            doc.setTextColor(55, 65, 81);
            doc.text(tableTitle, SM + 4, curY + 4);
            curY += 9;

            // Record count
            doc.setFontSize(7.5);
            doc.setFont('helvetica', 'normal');
            doc.setTextColor(107, 114, 128);
            doc.text(
                `${rows.length.toLocaleString()} record${rows.length !== 1 ? 's' : ''}`,
                SM + 4, curY
            );
            curY += 6;

            // ---- Visualization (chart / KPI cards) ----
            if (viz?.type === 'kpi') {
                curY = drawKPICards(doc, rows[0], SM + 4, curY, CW - 8);
            } else if (viz?.type === 'bar') {
                curY = drawHBar(doc, rows, viz.labelKey, viz.valueKey, SM + 4, curY, CW - 8);
            } else if (viz?.type === 'line') {
                curY = drawLineChart(doc, rows, viz.xKey, viz.yKeys, SM + 4, curY, CW - 8);
            }
            curY += 3;

            // Truncation notice
            if (truncated) {
                doc.setFontSize(7.5);
                doc.setFont('helvetica', 'italic');
                doc.setTextColor(156, 163, 175);
                doc.text(
                    `Showing first ${MAX_TABLE_ROWS.toLocaleString()} of ${rows.length.toLocaleString()} rows`,
                    SM + 4, curY
                );
                curY += 5;
            }

            // ---- Full data table ----
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
        });
    });

    addFooter();
    return doc;
};

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

const ExportAnalytics = () => {
    const { businessId } = useParams();
    const { user } = useAuth();
    const toastRef = useRef(null);

    const allKeys = EXPORT_SECTIONS.map((s) => s.key);
    const [selected, setSelected] = useState(new Set(allKeys));
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

            // 2. Collect unique categories needed for selected sections
            const selectedSections = EXPORT_SECTIONS.filter((s) => selected.has(s.key));
            const categorySet = new Set();
            selectedSections.forEach((s) => s.categories.forEach((c) => categorySet.add(c)));
            const categoriesParam = [...categorySet].join(',');

            setExportStep('Fetching analytics data…');
            const base = import.meta.env.VITE_API_URL || 'http://localhost:8000';
            const dataRes = await fetch(
                `${base}/analytics/data/${businessId}?categories=${encodeURIComponent(categoriesParam)}`
            );
            if (!dataRes.ok) {
                throw new Error('Analytics data not available. Please run the analytics pipeline first.');
            }
            const dataJson = await dataRes.json();
            const cats = dataJson.categories ?? {};

            // 3. Build per-section flat lookup: section.key → { analyticsKey → rows[] }
            const analyticsData = {};
            selectedSections.forEach((sec) => {
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
            });

            // 4. Record export in backend (non-fatal if it fails)
            const safeBusinessName = businessName.replace(/[^a-zA-Z0-9_-]/g, '_');
            const safeDate = new Date()
                .toISOString()
                .slice(0, 10)
                .replace(/-/g, '');
            const fileName = `Pulse_Analytics_${safeBusinessName}_${safeDate}.pdf`;

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
            doc.save(fileName);

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
    }, [businessId, selected, user]);

    return (
        <div className="p-6 space-y-6 max-w-4xl">
            <Toast ref={toastRef} />

            {/* Page header */}
            <div>
                <Heading level={2} className="text-2xl font-bold text-gray-800">
                    📤 Export Analytics
                </Heading>
                <Text className="text-gray-500 mt-1">
                    Select the sections you want to include in the PDF report. All data tables
                    will be exported in full (no pagination). Charts are represented as their
                    underlying data tables for maximum detail.
                </Text>
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
                        <span className="text-xl leading-none select-none">{sec.emoji}</span>
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

            {/* Export button */}
            <div className="flex items-center gap-4 pt-2">
                <PrimaryButton
                    label={isExporting ? exportStep || 'Exporting…' : 'Export to PDF'}
                    icon="pi pi-file-pdf"
                    onClick={handleExport}
                    loading={isExporting}
                    disabled={isExporting || selected.size === 0 || !businessId}
                    info
                />
                {!businessId && (
                    <Text className="text-sm text-amber-600">
                        ⚠️ Select a business from the dropdown above to enable export.
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

