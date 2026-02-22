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

/** Human-readable column header from snake_case key */
const colHeader = (key) =>
    key
        .split('_')
        .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
        .join(' ');

/** Format a cell value for display in the PDF table */
const fmtCell = (val) => {
    if (val === null || val === undefined) return '';
    if (typeof val === 'number') {
        if (Number.isNaN(val) || !Number.isFinite(val)) return '';
        return Number.isInteger(val) ? val.toLocaleString() : val.toFixed(2);
    }
    return String(val);
};

// Maximum rows per table to keep PDF file sizes manageable
const MAX_TABLE_ROWS = 200;

// ---------------------------------------------------------------------------
// PDF builder
// ---------------------------------------------------------------------------

const buildPDF = ({ businessName, businessId, reportDate, sections, analyticsData }) => {
    const doc = new jsPDF({ orientation: 'landscape', unit: 'mm', format: 'a4' });
    const PAGE_W = doc.internal.pageSize.getWidth();
    const PAGE_H = doc.internal.pageSize.getHeight();

    // Helper: add a page footer with page number
    const addFooter = () => {
        const totalPages = doc.getNumberOfPages();
        for (let i = 1; i <= totalPages; i++) {
            doc.setPage(i);
            doc.setFontSize(8);
            doc.setTextColor(150);
            doc.text(
                `Pulse Analytics Report  |  ${businessName}  |  ${reportDate}  |  Page ${i} of ${totalPages}`,
                PAGE_W / 2,
                PAGE_H - 6,
                { align: 'center' }
            );
        }
    };

    // ---- Cover page ----
    doc.setFillColor(17, 24, 39); // gray-900
    doc.rect(0, 0, PAGE_W, PAGE_H, 'F');

    // Gradient-like accent bar
    doc.setFillColor(99, 102, 241); // indigo-500
    doc.rect(0, 0, 6, PAGE_H, 'F');

    doc.setTextColor(255, 255, 255);
    doc.setFontSize(36);
    doc.setFont('helvetica', 'bold');
    doc.text('Pulse Analytics', PAGE_W / 2, PAGE_H / 2 - 28, { align: 'center' });

    doc.setFontSize(18);
    doc.setFont('helvetica', 'normal');
    doc.text('Analytics & Insights Report', PAGE_W / 2, PAGE_H / 2 - 12, { align: 'center' });

    doc.setFontSize(13);
    doc.setTextColor(209, 213, 219); // gray-300
    doc.text(`Business: ${businessName}`, PAGE_W / 2, PAGE_H / 2 + 6, { align: 'center' });
    doc.text(`Business ID: ${businessId}`, PAGE_W / 2, PAGE_H / 2 + 16, { align: 'center' });
    doc.text(`Generated: ${reportDate}`, PAGE_W / 2, PAGE_H / 2 + 26, { align: 'center' });

    doc.setFontSize(10);
    doc.setTextColor(107, 114, 128);
    doc.text(
        `This report contains ${sections.length} section(s) with full analytics data.`,
        PAGE_W / 2,
        PAGE_H / 2 + 44,
        { align: 'center' }
    );

    // ---- Table of Contents ----
    doc.addPage();
    doc.setFillColor(249, 250, 251); // gray-50
    doc.rect(0, 0, PAGE_W, PAGE_H, 'F');
    doc.setFillColor(99, 102, 241);
    doc.rect(0, 0, 6, PAGE_H, 'F');

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

    // ---- Section pages ----
    sections.forEach((sec) => {
        const catData = analyticsData[sec.key] ?? {};

        // Gather tables that have at least 1 row
        const tables = sec.analyticsKeys
            .map((k) => ({ key: k, rows: catData[k] ?? [] }))
            .filter((t) => t.rows.length > 0);

        if (tables.length === 0) {
            // No data page for this section
            doc.addPage();
            doc.setFillColor(249, 250, 251);
            doc.rect(0, 0, PAGE_W, PAGE_H, 'F');
            doc.setFillColor(99, 102, 241);
            doc.rect(0, 0, 6, PAGE_H, 'F');

            doc.setTextColor(17, 24, 39);
            doc.setFontSize(20);
            doc.setFont('helvetica', 'bold');
            doc.text(`${sec.emoji}  ${sec.label}`, 18, 22);

            doc.setFontSize(12);
            doc.setFont('helvetica', 'normal');
            doc.setTextColor(107, 114, 128);
            doc.text('No data available for this section.', 18, 40);
            return;
        }

        let isFirstTableInSection = true;

        tables.forEach(({ key, rows }) => {
            const tableTitle = colHeader(key);
            const displayRows = rows.slice(0, MAX_TABLE_ROWS);
            const truncated = rows.length > MAX_TABLE_ROWS;

            // Column headers from first row
            const columns = Object.keys(displayRows[0]);
            const head = [columns.map(colHeader)];
            const body = displayRows.map((row) => columns.map((c) => fmtCell(row[c])));

            doc.addPage();
            doc.setFillColor(249, 250, 251);
            doc.rect(0, 0, PAGE_W, PAGE_H, 'F');
            doc.setFillColor(99, 102, 241);
            doc.rect(0, 0, 6, PAGE_H, 'F');

            let startY = 12;

            if (isFirstTableInSection) {
                // Section header
                doc.setTextColor(17, 24, 39);
                doc.setFontSize(20);
                doc.setFont('helvetica', 'bold');
                doc.text(`${sec.emoji}  ${sec.label}`, 18, startY + 6);
                startY += 16;
                isFirstTableInSection = false;
            }

            // Table title
            doc.setFontSize(13);
            doc.setFont('helvetica', 'bold');
            doc.setTextColor(55, 65, 81);
            doc.text(tableTitle, 18, startY + 2);
            startY += 8;

            if (truncated) {
                doc.setFontSize(8);
                doc.setFont('helvetica', 'italic');
                doc.setTextColor(156, 163, 175);
                doc.text(
                    `Showing first ${MAX_TABLE_ROWS} of ${rows.length} rows`,
                    18,
                    startY
                );
                startY += 5;
            }

            autoTable(doc, {
                head,
                body,
                startY,
                margin: { left: 12, right: 12 },
                styles: {
                    fontSize: 7.5,
                    cellPadding: 2,
                    overflow: 'linebreak',
                    halign: 'left',
                },
                headStyles: {
                    fillColor: [99, 102, 241],
                    textColor: 255,
                    fontStyle: 'bold',
                    fontSize: 8,
                },
                alternateRowStyles: { fillColor: [243, 244, 246] },
                tableLineColor: [209, 213, 219],
                tableLineWidth: 0.2,
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

