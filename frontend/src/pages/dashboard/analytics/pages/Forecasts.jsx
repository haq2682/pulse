import React, { useState, useEffect, useRef, useMemo, useCallback } from 'react';
import { useParams } from 'react-router-dom';
import { Card } from 'primereact/card';
import { ProgressSpinner } from 'primereact/progressspinner';
import { Toast } from 'primereact/toast';
import { DataTable } from 'primereact/datatable';
import { Column } from 'primereact/column';
import { Tag } from 'primereact/tag';
import {
    Chart as ChartJS,
    CategoryScale, LinearScale, BarElement, ArcElement,
    PointElement, LineElement, Title, Tooltip, Legend,
} from 'chart.js';
import { Bar, Doughnut, Line } from 'react-chartjs-2';
import { useAnalyticsWebSocket } from '../../../../hooks/useAnalyticsWebSocket';
import ChartWrapper from '../components/ChartWrapper';
import { usePipelineProgress } from '@/context/PipelineProgressContext';
import { useFormatters } from '@/hooks/useFormatters';

ChartJS.register(
    CategoryScale, LinearScale, BarElement, ArcElement,
    PointElement, LineElement, Title, Tooltip, Legend,
);

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PALETTE = [
    'rgba(59,130,246,0.82)', 'rgba(34,197,94,0.82)',  'rgba(249,115,22,0.82)',
    'rgba(239,68,68,0.82)',  'rgba(139,92,246,0.82)', 'rgba(6,182,212,0.82)',
    'rgba(234,179,8,0.82)',  'rgba(236,72,153,0.82)', 'rgba(20,184,166,0.82)',
    'rgba(168,85,247,0.82)',
];

const RISK_COLOR = {
    High: 'danger', Medium: 'warning', Low: 'success',
    Critical: 'danger', 'High Risk': 'danger', 'Medium Risk': 'warning', 'Low Risk': 'success',
};

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

const KPICard = ({ icon, iconBg, iconColor, value, label }) => (
    <Card className="bg-gradient-to-br from-white to-gray-50 border border-gray-200 rounded-xl p-0 shadow-sm hover:shadow-md hover:-translate-y-1 transition-all duration-300">
        <div className="flex items-center gap-5 p-6">
            <i className={`pi ${icon} text-4xl p-4 ${iconBg} ${iconColor} rounded-xl`} />
            <div>
                <h3 className="text-2xl font-bold text-gray-900 mb-2">{value}</h3>
                <p className="text-xs font-medium text-gray-500 uppercase tracking-wider">{label}</p>
            </div>
        </div>
    </Card>
);

const SectionHeader = ({ color, title, badge }) => (
    <div className="flex items-center gap-3 mb-6">
        <div className={`h-1 w-8 ${color} rounded-full`} />
        <h2 className="text-xl font-bold text-gray-800">{title}</h2>
        {badge && (
            <span className="px-2 py-0.5 text-xs font-medium rounded-full bg-gray-100 text-gray-600">
                {badge}
            </span>
        )}
    </div>
);

const InferenceHeader = ({ label, modelType, description, count }) => {
    const fmt = useFormatters();
    const modelColors = {
        directory: 'bg-blue-50 text-blue-700',
        file: 'bg-purple-50 text-purple-700',
    };
    return (
        <div className="mb-4">
            <div className="flex items-center gap-2 mb-1">
                <h3 className="text-lg font-semibold text-gray-800">{label}</h3>
                {modelType && (
                    <span className={`px-2 py-0.5 text-xs font-medium rounded-full ${modelColors[modelType] ?? 'bg-gray-100 text-gray-600'}`}>
                        {modelType === 'file' ? 'Single File' : 'Partitioned'}
                    </span>
                )}
                {count !== undefined && (
                    <span className="px-2 py-0.5 text-xs font-medium rounded-full bg-green-50 text-green-700">
                        {fmt.number(count)} predictions
                    </span>
                )}
            </div>
            {description && <p className="text-sm text-gray-500">{description}</p>}
        </div>
    );
};

const NoInferenceNotice = ({ label }) => (
    <div className="text-center py-6 text-gray-400 italic text-sm">
        <i className="pi pi-info-circle mr-2" />
        {label} not available for this business — run the ML pipeline first.
    </div>
);

// ---------------------------------------------------------------------------
// Chart options helpers
// ---------------------------------------------------------------------------

const barOpts = (horizontal = false) => ({
    indexAxis: horizontal ? 'y' : 'x',
    responsive: true, maintainAspectRatio: false,
    plugins: { legend: { display: false }, title: { display: false } },
    scales: {
        x: { grid: { color: 'rgba(0,0,0,0.05)' } },
        y: { beginAtZero: true, grid: { color: 'rgba(0,0,0,0.05)' } },
    },
});

const groupedBarOpts = () => ({
    responsive: true, maintainAspectRatio: false,
    plugins: { legend: { position: 'top' }, title: { display: false } },
    scales: {
        x: { grid: { color: 'rgba(0,0,0,0.05)' } },
        y: { beginAtZero: true, grid: { color: 'rgba(0,0,0,0.05)' } },
    },
});

const doughnutOpts = () => ({
    responsive: true, maintainAspectRatio: false,
    plugins: { legend: { position: 'right' }, title: { display: false } },
});

const lineOpts = () => ({
    responsive: true, maintainAspectRatio: false,
    plugins: { legend: { display: false }, title: { display: false } },
    scales: {
        x: { grid: { color: 'rgba(0,0,0,0.05)' } },
        y: { beginAtZero: false, grid: { color: 'rgba(0,0,0,0.05)' } },
    },
});

// ---------------------------------------------------------------------------
// Distribution helper — count by a key column
// ---------------------------------------------------------------------------
const countBy = (rows, col) => {
    const map = {};
    for (const r of rows) {
        const k = r[col] ?? 'Unknown';
        map[k] = (map[k] ?? 0) + 1;
    }
    return Object.entries(map).sort((a, b) => b[1] - a[1]);
};

const distBarData = (pairs) => ({
    labels: pairs.map(([k]) => k),
    datasets: [{ data: pairs.map(([, v]) => v), backgroundColor: PALETTE }],
});

const distDoughnutData = (pairs) => ({
    labels: pairs.map(([k]) => k),
    datasets: [{ data: pairs.map(([, v]) => v), backgroundColor: PALETTE }],
});

// ---------------------------------------------------------------------------
// Main Component
// ---------------------------------------------------------------------------

export default function Forecasts() {
    const fmt = useFormatters();
    const { businessId } = useParams();
    const toastRef = useRef(null);
    const { pipelineStatus } = usePipelineProgress();
    const { lastUpdate } = useAnalyticsWebSocket(businessId);

    const [rawData, setRawData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [fetchError, setFetchError] = useState(false);

    // -------------------------------------------------------------------------
    // Fetch
    // -------------------------------------------------------------------------

    const buildUrl = useCallback(() => {
        const base = import.meta.env.VITE_API_URL || 'http://localhost:8000';
        return `${base}/analytics/forecasts/${businessId}?row_limit=500`;
    }, [businessId]);

    const fetchData = useCallback(async () => {
        if (!businessId) return;
        try {
            setLoading(true);
            setFetchError(false);
            const res = await fetch(buildUrl());
            if (!res.ok) {
                setRawData(null);
                return;
            }
            const json = await res.json();
            setRawData(json);
        } catch {
            console.error('[Forecasts] fetch error');
            setFetchError(true);
            setRawData(null);
        } finally {
            setLoading(false);
        }
    }, [buildUrl, businessId]);

    useEffect(() => { fetchData(); }, [businessId, fetchData]); // eslint-disable-line react-hooks/exhaustive-deps
    useEffect(() => {
        if (!lastUpdate) return;
        fetchData();
        toastRef.current?.show({ severity: 'info', summary: 'Data Updated', detail: 'ML inference results refreshed.', life: 3000 });
    }, [lastUpdate, fetchData]); // eslint-disable-line react-hooks/exhaustive-deps

    // -------------------------------------------------------------------------
    // Derived data
    // -------------------------------------------------------------------------

    const derived = useMemo(() => {
        if (!rawData) return null;
        const inferences = rawData.inferences ?? {};

        const get = (name) => inferences[name]?.data ?? null;
        const meta = (name) => inferences[name]?.meta ?? null;
        // Full-dataset row count (before row_limit truncation)
        const rowCount = (name) => inferences[name]?.row_count ?? null;
        // Server-side aggregate stats computed on the full dataset
        const summaryStats = (name) => inferences[name]?.meta?.summary_stats ?? {};

        // ── Customer Intelligence ──────────────────────────────────────────────

        const churnRows    = get('customer_churn_predictions');
        const segClsRows   = get('customer_segment_predictions');
        const segClustRows = get('customer_segmentation');
        const aovRows      = get('aov_prediction');
        const clvRows      = get('clv_predictions');

        // ── Inventory Intelligence ─────────────────────────────────────────────

        const stockRows    = get('stock_status_predictions');
        const restockRows  = get('restock_quantity');
        const safetyRows   = get('safety_stock_adjusted');
        const stockoutRows = get('stockout_probability');
        const demandRows   = get('demand_forecast');
        const priceRows    = get('price_optimization');

        // ── Operations Intelligence ───────────────────────────────────────────

        const fulfillRows   = get('fulfillment_risk_predictions');
        const deliveryRows  = get('delivery_time');
        const supplierClust = get('supplier_clustering');

        // ── Revenue & Marketing ───────────────────────────────────────────────

        const campaignRows = get('campaign_roi');
        const revRows      = get('revenue_forecast');
        const seasonRows   = get('seasonal_trends');
        const geoRows      = get('geographic_clustering');

        // ── Behaviour & Engagement ────────────────────────────────────────────

        const cartRows     = get('cart_abandonment_predictions');
        const sessionClust = get('session_behavior_clustering');
        const sessConvRows = get('session_conversion_value');

        // ── Reviews, Payments, Products ──────────────────────────────────────

        const sentimentRows = get('review_sentiment_predictions');
        const paymentRows   = get('payment_success_predictions');
        const bundleRows    = get('product_bundling_predictions');
        const affinityRows  = get('product_affinity_clustering');
        const lifecycleRows = get('product_lifecycle_clustering');

        // ── Overall KPIs ─────────────────────────────────────────────────────
        const availableCount = rawData.available_count ?? 0;
        const totalCatalog   = rawData.total_catalog   ?? 26;

        return {
            availableCount, totalCatalog,
            // Customer
            churnRows, segClsRows, segClustRows, aovRows, clvRows,
            // Inventory
            stockRows, restockRows, safetyRows, stockoutRows, demandRows, priceRows,
            // Operations
            fulfillRows, deliveryRows, supplierClust,
            // Revenue
            campaignRows, revRows, seasonRows, geoRows,
            // Behaviour
            cartRows, sessionClust, sessConvRows,
            // Reviews/Payments/Products
            sentimentRows, paymentRows, bundleRows, affinityRows, lifecycleRows,
            // metas
            getMeta: meta,
            getRowCount: rowCount,
            getSummaryStats: summaryStats,
        };
    }, [rawData]);

    // -------------------------------------------------------------------------
    // Render states
    // -------------------------------------------------------------------------

    const hasData = !!(derived && derived.availableCount > 0);

    if (loading && pipelineStatus !== 'loading') {
        return (
            <div className="flex flex-col items-center justify-center min-h-[400px] gap-4">
                <ProgressSpinner />
                <p className="text-gray-500 text-base">Loading ML forecasts & predictions…</p>
            </div>
        );
    }

    if (fetchError) {
        return (
            <div className="p-6 min-h-[calc(100vh-120px)]">
                <Toast ref={toastRef} />
                <div className="flex items-center justify-center min-h-[50vh]">
                    <div className="text-center">
                        <i className="pi pi-exclamation-circle text-5xl text-red-400 mb-3 block" />
                        <p className="text-gray-700 font-medium text-lg">Something went wrong</p>
                        <p className="text-gray-500 text-sm mt-1">Unable to load forecast data. Please try again later.</p>
                    </div>
                </div>
            </div>
        );
    }

    if (!hasData && !loading && pipelineStatus !== 'loading') {
        return (
            <div className="p-6 min-h-[calc(100vh-120px)]">
                <Toast ref={toastRef} />
                <div className="flex items-center justify-center min-h-[50vh]">
                    <div className="text-center space-y-3">
                        <i className="pi pi-chart-line text-5xl text-gray-300 block" />
                        <p className="text-gray-700 font-medium text-lg">No ML Predictions Available</p>
                        <p className="text-gray-500 text-sm">Run the ML inference pipeline to generate forecasts for this business.</p>
                    </div>
                </div>
            </div>
        );
    }

    // Guard against derived being null while the pipeline is loading
    if (!derived) return null;

    const {
        availableCount, totalCatalog,
        churnRows, segClsRows, segClustRows, aovRows, clvRows,
        stockRows, restockRows, safetyRows, stockoutRows, demandRows, priceRows,
        fulfillRows, deliveryRows, supplierClust,
        campaignRows, revRows, seasonRows, geoRows,
        cartRows, sessionClust, sessConvRows,
        sentimentRows, paymentRows, bundleRows, affinityRows, lifecycleRows,
        getRowCount, getSummaryStats,
    } = derived;

    // =========================================================================
    // Section helpers — build chart data from inference rows
    // =========================================================================

    // ── Customer Churn ────────────────────────────────────────────────────────
    const churnDist = churnRows ? distDoughnutData(countBy(churnRows, 'predicted_churn_risk')) : null;
    const highChurn = churnRows ? [...churnRows].sort((a, b) => (+(b.churn_probability ?? 0)) - (+(a.churn_probability ?? 0))).slice(0, 20) : [];

    // ── Customer Segments (classification) ────────────────────────────────────
    const segClsDist = segClsRows ? distDoughnutData(countBy(segClsRows, 'predicted_segment')) : null;

    // ── Customer Segments (clustering) ────────────────────────────────────────
    const segClustDist = segClustRows ? distDoughnutData(countBy(segClustRows, 'customer_label')) : null;

    // ── AOV Prediction ────────────────────────────────────────────────────────
    const topAov = aovRows ? [...aovRows].sort((a, b) => (+(b.predicted_next_aov ?? 0)) - (+(a.predicted_next_aov ?? 0))).slice(0, 12) : [];
    const aovBarData = topAov.length > 0 ? {
        labels: topAov.map((r) => String(r.customer_id ?? '').slice(0, 12)),
        datasets: [{ label: 'Predicted AOV ($)', data: topAov.map((r) => +(r.predicted_next_aov ?? 0)), backgroundColor: PALETTE[0] }],
    } : null;

    // ── CLV Prediction ────────────────────────────────────────────────────────
    const topClv = clvRows ? [...clvRows].sort((a, b) => (+(b.predicted_clv ?? 0)) - (+(a.predicted_clv ?? 0))).slice(0, 12) : [];
    const clvBarData = topClv.length > 0 ? {
        labels: topClv.map((r) => String(r.customer_id ?? '').slice(0, 12)),
        datasets: [{ label: 'Predicted CLV ($)', data: topClv.map((r) => +(r.predicted_clv ?? 0)), backgroundColor: PALETTE[1] }],
    } : null;

    // ── Stock Status ──────────────────────────────────────────────────────────
    const stockDist = stockRows ? distDoughnutData(countBy(stockRows, 'predicted_status')) : null;
    const criticalStock = stockRows ? [...stockRows].filter((r) => r.predicted_status !== 'In Stock').sort((a, b) => (+(a.days_until_stockout ?? 9999)) - (+(b.days_until_stockout ?? 9999))).slice(0, 20) : [];

    // ── Restock Quantity ──────────────────────────────────────────────────────
    const topRestock = restockRows ? [...restockRows].sort((a, b) => (+(b.recommended_restock_quantity ?? 0)) - (+(a.recommended_restock_quantity ?? 0))).slice(0, 12) : [];
    const restockBarData = topRestock.length > 0 ? {
        labels: topRestock.map((r) => String(r.product_id ?? '').slice(0, 12)),
        datasets: [{ label: 'Restock Qty', data: topRestock.map((r) => +(r.recommended_restock_quantity ?? 0)), backgroundColor: PALETTE[2] }],
    } : null;
    const totalRestockCost = getSummaryStats('restock_quantity').total_estimated_cost
        ?? (restockRows ? restockRows.reduce((s, r) => s + (+(r.estimated_cost ?? 0)), 0) : 0);

    // ── Safety Stock ──────────────────────────────────────────────────────────
    const safetyDist = safetyRows ? distBarData(countBy(safetyRows, 'demand_pattern')) : null;

    // ── Stockout Probability ──────────────────────────────────────────────────
    const stockoutDist = stockoutRows ? distDoughnutData(countBy(stockoutRows, 'stockout_risk_level')) : null;
    const criticalStockout = stockoutRows ? [...stockoutRows].filter((r) => ['Critical', 'High'].includes(r.stockout_risk_level)).sort((a, b) => (+(b.stockout_probability ?? 0)) - (+(a.stockout_probability ?? 0))).slice(0, 20) : [];

    // ── Demand Forecast ───────────────────────────────────────────────────────
    const topDemand = demandRows ? [...demandRows].sort((a, b) => (+(b.predicted_demand_units ?? 0)) - (+(a.predicted_demand_units ?? 0))).slice(0, 12) : [];
    const demandBarData = topDemand.length > 0 ? {
        labels: topDemand.map((r) => String(r.product_id ?? '').slice(0, 12)),
        datasets: [{ label: 'Predicted Units', data: topDemand.map((r) => +(r.predicted_demand_units ?? 0)), backgroundColor: PALETTE[4] }],
    } : null;

    // ── Price Optimization ────────────────────────────────────────────────────
    const topPriceGap = priceRows ? [...priceRows].sort((a, b) => Math.abs(+(b.optimal_price ?? 0) - +(b.current_price ?? 0)) - Math.abs(+(a.optimal_price ?? 0) - +(a.current_price ?? 0))).slice(0, 10) : [];
    const priceGroupedData = topPriceGap.length > 0 ? {
        labels: topPriceGap.map((r) => String(r.product_id ?? '').slice(0, 12)),
        datasets: [
            { label: 'Current Price ($)', data: topPriceGap.map((r) => +(r.current_price ?? 0)), backgroundColor: PALETTE[3] },
            { label: 'Optimal Price ($)', data: topPriceGap.map((r) => +(r.optimal_price ?? 0)), backgroundColor: PALETTE[1] },
        ],
    } : null;

    // ── Fulfillment Risk ──────────────────────────────────────────────────────
    const fulfillDist = fulfillRows ? distBarData(countBy(fulfillRows, 'predicted_risk_label')) : null;
    const highRiskOrders = fulfillRows ? [...fulfillRows].filter((r) => r.predicted_risk_label && (r.predicted_risk_label.includes('High') || r.predicted_risk_label === 'Critical Risk')).sort((a, b) => (+(b.delay_probability ?? 0)) - (+(a.delay_probability ?? 0))).slice(0, 20) : [];

    // ── Delivery Time ─────────────────────────────────────────────────────────
    const deliveryBuckets = (() => {
        if (!deliveryRows) return null;
        const bins = { '1-3 days': 0, '4-7 days': 0, '8-14 days': 0, '15-30 days': 0, '>30 days': 0 };
        for (const r of deliveryRows) {
            const d = +(r.predicted_delivery_days ?? 0);
            if (d <= 3) bins['1-3 days']++;
            else if (d <= 7) bins['4-7 days']++;
            else if (d <= 14) bins['8-14 days']++;
            else if (d <= 30) bins['15-30 days']++;
            else bins['>30 days']++;
        }
        const entries = Object.entries(bins).filter(([, v]) => v > 0);
        return entries.length ? { labels: entries.map(([k]) => k), datasets: [{ data: entries.map(([, v]) => v), backgroundColor: PALETTE }] } : null;
    })();
    const avgDeliveryDays = getSummaryStats('delivery_time').avg_delivery_days
        ?? (deliveryRows && deliveryRows.length > 0 ? (deliveryRows.reduce((s, r) => s + (+(r.predicted_delivery_days ?? 0)), 0) / deliveryRows.length) : 0);

    // ── Supplier Clustering ───────────────────────────────────────────────────
    const supplierTierDist = supplierClust ? distDoughnutData(countBy(supplierClust, 'performance_tier')) : null;

    // ── Campaign ROI ──────────────────────────────────────────────────────────
    const topCampaigns = campaignRows ? [...campaignRows].sort((a, b) => (+(b.predicted_roi ?? 0)) - (+(a.predicted_roi ?? 0))).slice(0, 10) : [];
    const campaignBarData = topCampaigns.length > 0 ? {
        labels: topCampaigns.map((r) => String(r.campaign_id ?? '').slice(0, 16)),
        datasets: [{ label: 'Predicted ROI (%)', data: topCampaigns.map((r) => +(r.predicted_roi ?? 0).toFixed(1)), backgroundColor: PALETTE }],
    } : null;
    const totalCampaignRev = getSummaryStats('campaign_roi').total_predicted_revenue
        ?? (campaignRows ? campaignRows.reduce((s, r) => s + (+(r.predicted_revenue ?? 0)), 0) : 0);

    // ── Revenue Forecast ──────────────────────────────────────────────────────
    const revSorted = revRows ? [...revRows].sort((a, b) => String(a.forecast_date ?? '').localeCompare(String(b.forecast_date ?? ''))) : [];
    const revLineData = revSorted.length > 0 ? {
        labels: revSorted.map((r) => String(r.forecast_date ?? '').slice(0, 10)),
        datasets: [{
            label: 'Predicted Revenue ($)',
            data: revSorted.map((r) => +(r.predicted_revenue ?? 0)),
            borderColor: 'rgba(59,130,246,0.9)', backgroundColor: 'rgba(59,130,246,0.15)',
            fill: true, tension: 0.3,
        }],
    } : null;
    const totalPredRevenue = getSummaryStats('revenue_forecast').total_predicted_revenue
        ?? (revRows ? revRows.reduce((s, r) => s + (+(r.predicted_revenue ?? 0)), 0) : 0);

    // ── Seasonal Trends ───────────────────────────────────────────────────────
    const seasonSorted = seasonRows ? [...seasonRows].sort((a, b) => (+(a.forecast_month ?? 0)) - (+(b.forecast_month ?? 0))) : [];
    const seasonBarData = seasonSorted.length > 0 ? {
        labels: seasonSorted.map((r) => `Month ${r.forecast_month ?? ''}`),
        datasets: [{
            label: 'Seasonal Index',
            data: seasonSorted.map((r) => +(r.predicted_seasonal_index ?? 0)),
            backgroundColor: seasonSorted.map((r) => {
                const idx = +(r.predicted_seasonal_index ?? 1);
                if (idx > 1.1) return 'rgba(34,197,94,0.82)';
                if (idx < 0.9) return 'rgba(239,68,68,0.82)';
                return 'rgba(234,179,8,0.82)';
            }),
        }],
    } : null;

    // ── Geographic Clustering ─────────────────────────────────────────────────
    const geoDist = geoRows ? distBarData(countBy(geoRows, 'market_segment')) : null;
    const topExpansion = geoRows ? [...geoRows].sort((a, b) => (+(b.expansion_opportunity_score ?? 0)) - (+(a.expansion_opportunity_score ?? 0))).slice(0, 15) : [];

    // ── Cart Abandonment ──────────────────────────────────────────────────────
    const cartDist = cartRows ? distDoughnutData(countBy(cartRows, 'predicted_status')) : null;
    const highRiskCarts = cartRows ? [...cartRows].filter((r) => r.predicted_status === 'Abandoned').sort((a, b) => (+(b.abandonment_probability ?? 0)) - (+(a.abandonment_probability ?? 0))).slice(0, 20) : [];

    // ── Session Behavior Clustering ───────────────────────────────────────────
    const sessionBehavDist = sessionClust ? distBarData(countBy(sessionClust, 'behavior_type')) : null;

    // ── Session Conversion Value ──────────────────────────────────────────────
    const topSessions = sessConvRows ? [...sessConvRows].sort((a, b) => (+(b.predicted_conversion_value ?? 0)) - (+(a.predicted_conversion_value ?? 0))).slice(0, 12) : [];
    const sessConvBarData = topSessions.length > 0 ? {
        labels: topSessions.map((r) => String(r.session_id ?? '').slice(0, 12)),
        datasets: [{ label: 'Predicted Value ($)', data: topSessions.map((r) => +(r.predicted_conversion_value ?? 0)), backgroundColor: PALETTE[5] }],
    } : null;

    // ── Review Sentiment ──────────────────────────────────────────────────────
    const sentimentDist = sentimentRows ? distDoughnutData(countBy(sentimentRows, 'predicted_sentiment')) : null;
    const negativeReviews = sentimentRows ? [...sentimentRows].filter((r) => r.predicted_sentiment === 'Negative').sort((a, b) => (+(a.sentiment_score ?? 0)) - (+(b.sentiment_score ?? 0))).slice(0, 20) : [];

    // ── Payment Success ───────────────────────────────────────────────────────
    const paymentDist = paymentRows ? distDoughnutData(countBy(paymentRows, 'predicted_status')) : null;
    const failedPayments = paymentRows ? [...paymentRows].filter((r) => r.predicted_status !== 'Completed' && r.predicted_status !== 'Success').sort((a, b) => (+(a.success_probability ?? 1)) - (+(b.success_probability ?? 1))).slice(0, 20) : [];

    // ── Product Bundling ──────────────────────────────────────────────────────
    const bundleCatDist = bundleRows ? distBarData(countBy(bundleRows, 'bundle_category')) : null;
    const topBundles = bundleRows ? [...bundleRows].filter((r) => r.is_complementary).sort((a, b) => (+(b.affinity_score ?? 0)) - (+(a.affinity_score ?? 0))).slice(0, 20) : [];

    // ── Product Affinity Clustering ───────────────────────────────────────────
    const affinityDist = affinityRows ? distBarData(countBy(affinityRows, 'cluster_label')) : null;

    // ── Product Lifecycle Clustering ──────────────────────────────────────────
    const lifecycleDist = lifecycleRows ? distDoughnutData(countBy(lifecycleRows, 'lifecycle_stage')) : null;

    // =========================================================================
    // RENDER
    // =========================================================================

    return (
        <div className="p-6 space-y-10">
            <Toast ref={toastRef} />

            {/* ── Static notice ────────────────────────────────────────────────── */}
            <p className="text-xs text-gray-400 italic">
                * These results are point-in-time predictions generated by the last ML pipeline run.
                Date filtering does not apply.
            </p>

            {/* ── Overview KPIs ─────────────────────────────────────────────────── */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <KPICard icon="pi-check-circle" iconBg="bg-green-100" iconColor="text-green-600"
                    value={fmt.number(availableCount)} label="Available Inferences" />
                <KPICard icon="pi-database" iconBg="bg-blue-100" iconColor="text-blue-600"
                    value={`${availableCount} / ${totalCatalog}`} label="Catalog Coverage" />
                <KPICard icon="pi-users" iconBg="bg-purple-100" iconColor="text-purple-600"
                    value={churnRows ? fmt.number(getRowCount('customer_churn_predictions') ?? churnRows.length) : '—'} label="Churn Predictions" />
                <KPICard icon="pi-box" iconBg="bg-amber-100" iconColor="text-amber-600"
                    value={stockoutRows ? fmt.number(
                        getSummaryStats('stockout_probability').high_risk_count
                        ?? stockoutRows.filter((r) => ['Critical', 'High'].includes(r.stockout_risk_level)).length
                    ) : '—'}
                    label="High Stockout Risk Products" />
            </div>

            {/* ================================================================ */}
            {/* SECTION 1 — Customer Intelligence                                */}
            {/* ================================================================ */}
            <section className="space-y-8">
                <SectionHeader color="bg-blue-500" title="Customer Intelligence" />

                {/* Churn */}
                <div className="space-y-4">
                    <InferenceHeader
                        label="Customer Churn Predictions"
                        modelType={churnRows ? 'directory' : null}
                        description="Classifies each customer's churn risk (High / Medium / Low) with contributing factors."
                        count={getRowCount('customer_churn_predictions') ?? churnRows?.length}
                    />
                    {churnRows ? (
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                            <ChartWrapper title="Churn Risk Distribution">
                                <div style={{ height: 260 }}>
                                    <Doughnut data={churnDist} options={doughnutOpts()} />
                                </div>
                            </ChartWrapper>
                            <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                                <div className="p-6">
                                    <h3 className="text-base font-semibold text-gray-900 mb-3 pb-2 border-b border-gray-200">
                                        Top High-Risk Customers
                                    </h3>
                                    <DataTable value={highChurn} rows={8} className="p-datatable-sm" stripedRows>
                                        <Column field="customer_id" header="Customer ID" />
                                        <Column field="predicted_churn_risk" header="Risk" body={(r) => <Tag value={r.predicted_churn_risk ?? '—'} severity={RISK_COLOR[r.predicted_churn_risk] ?? 'info'} />} />
                                        <Column field="churn_probability" header="Probability" body={(r) => fmt.probToPct(r.churn_probability)} />
                                        <Column field="confidence_score" header="Confidence" body={(r) => fmt.probToPct(r.confidence_score)} />
                                    </DataTable>
                                </div>
                            </Card>
                        </div>
                    ) : <NoInferenceNotice label="Customer Churn Predictions" />}
                </div>

                <hr className="border-gray-100" />

                {/* Segment Classification */}
                <div className="space-y-4">
                    <InferenceHeader
                        label="Customer Segment Predictions (Classification)"
                        modelType={segClsRows ? 'directory' : null}
                        description="RFM-based segment classification per customer."
                        count={getRowCount('customer_segment_predictions') ?? segClsRows?.length}
                    />
                    {segClsRows ? (
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                            <ChartWrapper title="Segment Distribution">
                                <div style={{ height: 260 }}>
                                    <Doughnut data={segClsDist} options={doughnutOpts()} />
                                </div>
                            </ChartWrapper>
                            <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                                <div className="p-6">
                                    <h3 className="text-base font-semibold text-gray-900 mb-3 pb-2 border-b border-gray-200">
                                        Customer Segment Table
                                    </h3>
                                    <DataTable value={segClsRows} paginator rows={8} className="p-datatable-sm" stripedRows>
                                        <Column field="customer_id" header="Customer ID" />
                                        <Column field="predicted_segment" header="Segment" />
                                        <Column field="segment_probability" header="Probability" body={(r) => fmt.probToPct(r.segment_probability)} />
                                        <Column field="rfm_score" header="RFM Score" body={(r) => fmt.decimal(r.rfm_score, 1)} />
                                    </DataTable>
                                </div>
                            </Card>
                        </div>
                    ) : <NoInferenceNotice label="Customer Segment Predictions" />}
                </div>

                <hr className="border-gray-100" />

                {/* Segment Clustering */}
                <div className="space-y-4">
                    <InferenceHeader
                        label="Customer Segmentation (RFM Clustering)"
                        modelType={segClustRows ? 'file' : null}
                        description="K-Means clusters customers into semantic personas based on Recency, Frequency, Monetary scores."
                        count={getRowCount('customer_segmentation') ?? segClustRows?.length}
                    />
                    {segClustRows ? (
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                            <ChartWrapper title="Cluster Persona Distribution">
                                <div style={{ height: 260 }}>
                                    <Doughnut data={segClustDist} options={doughnutOpts()} />
                                </div>
                            </ChartWrapper>
                            <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                                <div className="p-6">
                                    <h3 className="text-base font-semibold text-gray-900 mb-3 pb-2 border-b border-gray-200">
                                        Cluster Assignments
                                    </h3>
                                    <DataTable value={segClustRows} paginator rows={8} className="p-datatable-sm" stripedRows>
                                        <Column field="customer_id" header="Customer ID" />
                                        <Column field="customer_label" header="Persona" />
                                        <Column field="recency_score" header="Recency" body={(r) => fmt.decimal(r.recency_score, 1)} />
                                        <Column field="frequency_score" header="Frequency" body={(r) => fmt.decimal(r.frequency_score, 1)} />
                                        <Column field="monetary_score" header="Monetary" body={(r) => fmt.decimal(r.monetary_score, 1)} />
                                    </DataTable>
                                </div>
                            </Card>
                        </div>
                    ) : <NoInferenceNotice label="Customer Segmentation (Clustering)" />}
                </div>

                <hr className="border-gray-100" />

                {/* AOV + CLV */}
                <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                    <div className="space-y-4">
                        <InferenceHeader label="AOV Prediction" modelType={aovRows ? 'directory' : null} description="Predicted next average order value per customer." count={getRowCount('aov_prediction') ?? aovRows?.length} />
                        {aovBarData ? (
                            <ChartWrapper title="Top 12 Customers — Predicted AOV">
                                <div style={{ height: 280 }}>
                                    <Bar data={aovBarData} options={barOpts()} />
                                </div>
                            </ChartWrapper>
                        ) : <NoInferenceNotice label="AOV Prediction" />}
                    </div>
                    <div className="space-y-4">
                        <InferenceHeader label="Customer Lifetime Value Prediction" modelType={clvRows ? 'directory' : null} description="1-year CLV forecast per customer with confidence intervals." count={getRowCount('clv_predictions') ?? clvRows?.length} />
                        {clvBarData ? (
                            <ChartWrapper title="Top 12 Customers — Predicted CLV">
                                <div style={{ height: 280 }}>
                                    <Bar data={clvBarData} options={barOpts()} />
                                </div>
                            </ChartWrapper>
                        ) : <NoInferenceNotice label="CLV Prediction" />}
                    </div>
                </div>
                {aovRows && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-base font-semibold text-gray-900 mb-3 pb-2 border-b border-gray-200">AOV Predictions Table</h3>
                            <DataTable value={[...aovRows].sort((a, b) => (+(b.predicted_next_aov ?? 0)) - (+(a.predicted_next_aov ?? 0)))} paginator rows={10} className="p-datatable-sm" stripedRows>
                                <Column field="customer_id" header="Customer ID" />
                                <Column field="predicted_next_aov" header="Predicted AOV" body={(r) => fmt.currency(r.predicted_next_aov)} sortable />
                                <Column field="confidence_interval_lower" header="CI Lower" body={(r) => fmt.currency(r.confidence_interval_lower)} />
                                <Column field="confidence_interval_upper" header="CI Upper" body={(r) => fmt.currency(r.confidence_interval_upper)} />
                                <Column field="confidence_score" header="Confidence" body={(r) => fmt.probToPct(r.confidence_score)} sortable />
                            </DataTable>
                        </div>
                    </Card>
                )}
            </section>

            {/* ================================================================ */}
            {/* SECTION 2 — Inventory Intelligence                               */}
            {/* ================================================================ */}
            <section className="space-y-8">
                <SectionHeader color="bg-orange-500" title="Inventory Intelligence" />

                {/* Stock Status */}
                <div className="space-y-4">
                    <InferenceHeader label="Stock Status Predictions" modelType={stockRows ? 'directory' : null} description="Predicts In Stock / Low Stock / Out of Stock / Overstock per product." count={getRowCount('stock_status_predictions') ?? stockRows?.length} />
                    {stockRows ? (
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                            <ChartWrapper title="Predicted Stock Status Distribution">
                                <div style={{ height: 260 }}>
                                    <Doughnut data={stockDist} options={doughnutOpts()} />
                                </div>
                            </ChartWrapper>
                            <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                                <div className="p-6">
                                    <h3 className="text-base font-semibold text-gray-900 mb-3 pb-2 border-b border-gray-200">Critical & Low Stock Products</h3>
                                    <DataTable value={criticalStock} rows={8} className="p-datatable-sm" stripedRows>
                                        <Column field="product_id" header="Product ID" />
                                        <Column field="predicted_status" header="Status" body={(r) => <Tag value={r.predicted_status ?? '—'} severity={r.predicted_status === 'Out of Stock' ? 'danger' : r.predicted_status === 'Low Stock' ? 'warning' : 'info'} />} />
                                        <Column field="days_until_stockout" header="Days to Stockout" body={(r) => fmt.days(r.days_until_stockout)} sortable />
                                        <Column field="reorder_recommendation" header="Reorder?" body={(r) => <Tag value={r.reorder_recommendation ? 'Yes' : 'No'} severity={r.reorder_recommendation ? 'danger' : 'success'} />} />
                                    </DataTable>
                                </div>
                            </Card>
                        </div>
                    ) : <NoInferenceNotice label="Stock Status Predictions" />}
                </div>

                <hr className="border-gray-100" />

                {/* Stockout Probability */}
                <div className="space-y-4">
                    <InferenceHeader label="Stockout Probability Predictions" modelType={stockoutRows ? 'directory' : null} description="Forecasts stockout risk level and expected days until stockout." count={getRowCount('stockout_probability') ?? stockoutRows?.length} />
                    {stockoutRows ? (
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                            <ChartWrapper title="Stockout Risk Distribution">
                                <div style={{ height: 260 }}>
                                    <Doughnut data={stockoutDist} options={doughnutOpts()} />
                                </div>
                            </ChartWrapper>
                            <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                                <div className="p-6">
                                    <h3 className="text-base font-semibold text-gray-900 mb-3 pb-2 border-b border-gray-200">Critical / High Risk Products</h3>
                                    <DataTable value={criticalStockout} rows={8} className="p-datatable-sm" stripedRows>
                                        <Column field="product_id" header="Product ID" />
                                        <Column field="stockout_risk_level" header="Risk" body={(r) => <Tag value={r.stockout_risk_level ?? '—'} severity={r.stockout_risk_level === 'Critical' ? 'danger' : 'warning'} />} />
                                        <Column field="stockout_probability" header="Probability" body={(r) => fmt.probToPct(r.stockout_probability)} sortable />
                                        <Column field="days_until_stockout" header="Days Left" body={(r) => fmt.days(r.days_until_stockout)} sortable />
                                        <Column field="urgency_score" header="Urgency" body={(r) => fmt.decimal(r.urgency_score, 1)} />
                                    </DataTable>
                                </div>
                            </Card>
                        </div>
                    ) : <NoInferenceNotice label="Stockout Probability Predictions" />}
                </div>

                <hr className="border-gray-100" />

                {/* Restock + Safety */}
                <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                    <div className="space-y-4">
                        <InferenceHeader label="Restock Quantity Predictions" modelType={restockRows ? 'directory' : null} description={`Recommended restock units per product. Total estimated cost: ${fmt.currency(totalRestockCost)}`} count={getRowCount('restock_quantity') ?? restockRows?.length} />
                        {restockBarData ? (
                            <ChartWrapper title="Top 12 Products — Recommended Restock Qty">
                                <div style={{ height: 280 }}>
                                    <Bar data={restockBarData} options={barOpts()} />
                                </div>
                            </ChartWrapper>
                        ) : <NoInferenceNotice label="Restock Quantity" />}
                    </div>
                    <div className="space-y-4">
                        <InferenceHeader label="Safety Stock Adjustment" modelType={safetyRows ? 'directory' : null} description="ML-adjusted safety stock levels by demand pattern." count={getRowCount('safety_stock_adjusted') ?? safetyRows?.length} />
                        {safetyDist ? (
                            <ChartWrapper title="Demand Pattern Distribution">
                                <div style={{ height: 280 }}>
                                    <Bar data={safetyDist} options={barOpts()} />
                                </div>
                            </ChartWrapper>
                        ) : <NoInferenceNotice label="Safety Stock" />}
                    </div>
                </div>

                <hr className="border-gray-100" />

                {/* Demand Forecast + Price Optimization */}
                <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                    <div className="space-y-4">
                        <InferenceHeader label="Product Demand Forecast" modelType={demandRows ? 'directory' : null} description="Predicted demand units with seasonality and trend factors." count={getRowCount('demand_forecast') ?? demandRows?.length} />
                        {demandBarData ? (
                            <ChartWrapper title="Top 12 Products — Predicted Demand">
                                <div style={{ height: 280 }}>
                                    <Bar data={demandBarData} options={barOpts()} />
                                </div>
                            </ChartWrapper>
                        ) : <NoInferenceNotice label="Demand Forecast" />}
                    </div>
                    <div className="space-y-4">
                        <InferenceHeader label="Price Optimization" modelType={priceRows ? 'directory' : null} description="Optimal price recommendations based on elasticity and expected units." count={getRowCount('price_optimization') ?? priceRows?.length} />
                        {priceGroupedData ? (
                            <ChartWrapper title="Current vs Optimal Price — Top 10 Price Gaps">
                                <div style={{ height: 280 }}>
                                    <Bar data={priceGroupedData} options={groupedBarOpts()} />
                                </div>
                            </ChartWrapper>
                        ) : <NoInferenceNotice label="Price Optimization" />}
                    </div>
                </div>
                {(safetyRows) && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-base font-semibold text-gray-900 mb-3 pb-2 border-b border-gray-200">Safety Stock Table</h3>
                            <DataTable value={[...safetyRows].sort((a, b) => (+(b.adjustment_factor ?? 0)) - (+(a.adjustment_factor ?? 0)))} paginator rows={10} className="p-datatable-sm" stripedRows>
                                <Column field="product_id" header="Product ID" />
                                <Column field="required_safety_stock_units" header="Safety Stock (units)" sortable />
                                <Column field="adjustment_factor" header="Adjustment Factor" body={(r) => fmt.decimal(r.adjustment_factor, 2)} sortable />
                                <Column field="demand_pattern" header="Demand Pattern" body={(r) => <Tag value={r.demand_pattern ?? '—'} severity={r.demand_pattern === 'Erratic' ? 'danger' : r.demand_pattern === 'Variable' ? 'warning' : 'success'} />} />
                                <Column field="service_level_target" header="Service Level" body={(r) => fmt.probToPct100(r.service_level_target ?? 0)} />
                            </DataTable>
                        </div>
                    </Card>
                )}
            </section>

            {/* ================================================================ */}
            {/* SECTION 3 — Operations Intelligence                              */}
            {/* ================================================================ */}
            <section className="space-y-8">
                <SectionHeader color="bg-cyan-500" title="Operations Intelligence" />

                {/* Fulfillment Risk */}
                <div className="space-y-4">
                    <InferenceHeader label="Order Fulfillment Risk" modelType={fulfillRows ? 'directory' : null} description="Classifies fulfillment risk (Low / Medium / High / Critical) per order." count={getRowCount('fulfillment_risk_predictions') ?? fulfillRows?.length} />
                    {fulfillRows ? (
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                            <ChartWrapper title="Risk Level Distribution">
                                <div style={{ height: 260 }}>
                                    <Bar data={fulfillDist} options={barOpts()} />
                                </div>
                            </ChartWrapper>
                            <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                                <div className="p-6">
                                    <h3 className="text-base font-semibold text-gray-900 mb-3 pb-2 border-b border-gray-200">High-Risk Orders</h3>
                                    <DataTable value={highRiskOrders} rows={8} className="p-datatable-sm" stripedRows>
                                        <Column field="order_id" header="Order ID" />
                                        <Column field="predicted_risk_label" header="Risk" body={(r) => <Tag value={r.predicted_risk_label ?? '—'} severity={RISK_COLOR[r.predicted_risk_label] ?? 'info'} />} />
                                        <Column field="delay_probability" header="Delay Prob" body={(r) => fmt.probToPct(r.delay_probability)} sortable />
                                        <Column field="expected_delay_days" header="Exp. Delay (d)" sortable />
                                        <Column field="recommended_action" header="Action" style={{ maxWidth: '180px' }} />
                                    </DataTable>
                                </div>
                            </Card>
                        </div>
                    ) : <NoInferenceNotice label="Order Fulfillment Risk" />}
                </div>

                <hr className="border-gray-100" />

                {/* Delivery Time + Supplier Clustering */}
                <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                    <div className="space-y-4">
                        <InferenceHeader label="Delivery Time Prediction" modelType={deliveryRows ? 'directory' : null} description={`Predicted delivery days per order. Avg: ${fmt.days(avgDeliveryDays)}.`} count={getRowCount('delivery_time') ?? deliveryRows?.length} />
                        {deliveryBuckets ? (
                            <ChartWrapper title="Delivery Day Buckets">
                                <div style={{ height: 280 }}>
                                    <Bar data={deliveryBuckets} options={barOpts()} />
                                </div>
                            </ChartWrapper>
                        ) : <NoInferenceNotice label="Delivery Time Prediction" />}
                    </div>
                    <div className="space-y-4">
                        <InferenceHeader label="Supplier Performance Clustering" modelType={supplierClust ? 'file' : null} description="Supplier segments: Strategic Partners, Reliable Performers, Risk Suppliers, etc." count={getRowCount('supplier_clustering') ?? supplierClust?.length} />
                        {supplierTierDist ? (
                            <ChartWrapper title="Performance Tier Distribution">
                                <div style={{ height: 280 }}>
                                    <Doughnut data={supplierTierDist} options={doughnutOpts()} />
                                </div>
                            </ChartWrapper>
                        ) : <NoInferenceNotice label="Supplier Performance Clustering" />}
                    </div>
                </div>
                {supplierClust && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-base font-semibold text-gray-900 mb-3 pb-2 border-b border-gray-200">Supplier Clustering Table</h3>
                            <DataTable value={supplierClust} paginator rows={10} className="p-datatable-sm" stripedRows>
                                <Column field="supplier_id" header="Supplier ID" />
                                <Column field="business_persona" header="Persona" />
                                <Column field="performance_tier" header="Tier" body={(r) => <Tag value={r.performance_tier ?? '—'} severity={r.performance_tier === 'Premium' ? 'success' : r.performance_tier === 'At Risk' ? 'danger' : 'info'} />} />
                                <Column field="action_urgency" header="Urgency" body={(r) => <Tag value={r.action_urgency ?? '—'} severity={['Immediate', 'Urgent'].includes(r.action_urgency) ? 'danger' : r.action_urgency === 'High' ? 'warning' : 'success'} />} />
                                <Column field="confidence_score" header="Confidence" body={(r) => fmt.probToPct(r.confidence_score)} />
                            </DataTable>
                        </div>
                    </Card>
                )}
            </section>

            {/* ================================================================ */}
            {/* SECTION 4 — Revenue & Marketing Intelligence                     */}
            {/* ================================================================ */}
            <section className="space-y-8">
                <SectionHeader color="bg-green-500" title="Revenue & Marketing Intelligence" />

                {/* Revenue Forecast */}
                <div className="space-y-4">
                    <InferenceHeader label="Revenue Forecast" modelType={revRows ? 'directory' : null} description={`Total predicted revenue: ${fmt.currency(totalPredRevenue)} across ${getRowCount('revenue_forecast') ?? revRows?.length ?? 0} forecast dates.`} count={getRowCount('revenue_forecast') ?? revRows?.length} />
                    {revLineData ? (
                        <ChartWrapper title="Predicted Revenue Over Forecast Dates">
                            <div style={{ height: 300 }}>
                                <Line data={revLineData} options={lineOpts()} />
                            </div>
                        </ChartWrapper>
                    ) : <NoInferenceNotice label="Revenue Forecast" />}
                </div>

                <hr className="border-gray-100" />

                {/* Seasonal Trends */}
                <div className="space-y-4">
                    <InferenceHeader label="Seasonal Trends Forecast" modelType={seasonRows ? 'directory' : null} description="Seasonal index per forecast month (> 1.0 = above average, < 1.0 = below average)." count={getRowCount('seasonal_trends') ?? seasonRows?.length} />
                    {seasonBarData ? (
                        <ChartWrapper title="Seasonal Index by Forecast Month (green = peak, red = low season)">
                            <div style={{ height: 280 }}>
                                <Bar data={seasonBarData} options={barOpts()} />
                            </div>
                        </ChartWrapper>
                    ) : <NoInferenceNotice label="Seasonal Trends" />}
                    {seasonRows && (
                        <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                            <div className="p-6">
                                <h3 className="text-base font-semibold text-gray-900 mb-3 pb-2 border-b border-gray-200">Seasonal Trends Table</h3>
                                <DataTable value={seasonSorted} className="p-datatable-sm" stripedRows>
                                    <Column field="forecast_month" header="Month" />
                                    <Column field="forecast_date" header="Date" body={(r) => String(r.forecast_date ?? '').slice(0, 10)} />
                                    <Column field="predicted_seasonal_index" header="Seasonal Index" body={(r) => fmt.decimal(r.predicted_seasonal_index, 3)} sortable />
                                    <Column field="season_classification" header="Classification" body={(r) => <Tag value={r.season_classification ?? '—'} severity={r.season_classification === 'peak_season' ? 'success' : r.season_classification === 'low_season' ? 'danger' : 'info'} />} />
                                    <Column field="estimated_revenue" header="Est. Revenue" body={(r) => fmt.currency(r.estimated_revenue)} sortable />
                                </DataTable>
                            </div>
                        </Card>
                    )}
                </div>

                <hr className="border-gray-100" />

                {/* Campaign ROI */}
                <div className="space-y-4">
                    <InferenceHeader label="Campaign ROI Prediction" modelType={campaignRows ? 'directory' : null} description={`Predicted total campaign revenue: ${fmt.currency(totalCampaignRev)}.`} count={getRowCount('campaign_roi') ?? campaignRows?.length} />
                    {campaignBarData ? (
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                            <ChartWrapper title="Top 10 Campaigns — Predicted ROI (%)">
                                <div style={{ height: 280 }}>
                                    <Bar data={campaignBarData} options={barOpts()} />
                                </div>
                            </ChartWrapper>
                            <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                                <div className="p-6">
                                    <h3 className="text-base font-semibold text-gray-900 mb-3 pb-2 border-b border-gray-200">Campaign ROI Table</h3>
                                    <DataTable value={[...campaignRows].sort((a, b) => (+(b.predicted_roi ?? 0)) - (+(a.predicted_roi ?? 0)))} rows={8} className="p-datatable-sm" stripedRows>
                                        <Column field="campaign_id" header="Campaign ID" />
                                        <Column field="predicted_roi" header="ROI (%)" body={(r) => fmt.decimal(r.predicted_roi, 1)} sortable />
                                        <Column field="predicted_revenue" header="Revenue" body={(r) => fmt.currency(r.predicted_revenue)} sortable />
                                        <Column field="predicted_conversions" header="Conversions" sortable />
                                        <Column field="confidence_score" header="Confidence" body={(r) => fmt.probToPct(r.confidence_score)} />
                                    </DataTable>
                                </div>
                            </Card>
                        </div>
                    ) : <NoInferenceNotice label="Campaign ROI Prediction" />}
                </div>

                <hr className="border-gray-100" />

                {/* Geographic Clustering */}
                <div className="space-y-4">
                    <InferenceHeader label="Geographic Sales Clustering" modelType={geoRows ? 'file' : null} description="Groups regions by sales performance and market potential." count={getRowCount('geographic_clustering') ?? geoRows?.length} />
                    {geoDist ? (
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                            <ChartWrapper title="Market Segment Distribution">
                                <div style={{ height: 260 }}>
                                    <Bar data={geoDist} options={barOpts()} />
                                </div>
                            </ChartWrapper>
                            <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                                <div className="p-6">
                                    <h3 className="text-base font-semibold text-gray-900 mb-3 pb-2 border-b border-gray-200">Top Expansion Opportunities</h3>
                                    <DataTable value={topExpansion} rows={8} className="p-datatable-sm" stripedRows>
                                        <Column field="country" header="Country" />
                                        <Column field="state_province" header="State/Province" />
                                        <Column field="city" header="City" />
                                        <Column field="market_segment" header="Segment" />
                                        <Column field="expansion_opportunity_score" header="Opp. Score" body={(r) => fmt.decimal(r.expansion_opportunity_score, 2)} sortable />
                                    </DataTable>
                                </div>
                            </Card>
                        </div>
                    ) : <NoInferenceNotice label="Geographic Sales Clustering" />}
                </div>
            </section>

            {/* ================================================================ */}
            {/* SECTION 5 — Behaviour & Engagement Intelligence                  */}
            {/* ================================================================ */}
            <section className="space-y-8">
                <SectionHeader color="bg-purple-500" title="Behaviour & Engagement Intelligence" />

                {/* Cart Abandonment */}
                <div className="space-y-4">
                    <InferenceHeader label="Cart Abandonment Predictions" modelType={cartRows ? 'directory' : null} description="Predicts whether each active cart will be abandoned or converted." count={getRowCount('cart_abandonment_predictions') ?? cartRows?.length} />
                    {cartRows ? (
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                            <ChartWrapper title="Predicted Cart Status Distribution">
                                <div style={{ height: 260 }}>
                                    <Doughnut data={cartDist} options={doughnutOpts()} />
                                </div>
                            </ChartWrapper>
                            <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                                <div className="p-6">
                                    <h3 className="text-base font-semibold text-gray-900 mb-3 pb-2 border-b border-gray-200">High-Risk Abandonment Carts</h3>
                                    <DataTable value={highRiskCarts} rows={8} className="p-datatable-sm" stripedRows>
                                        <Column field="cart_id" header="Cart ID" />
                                        <Column field="customer_id" header="Customer ID" />
                                        <Column field="abandonment_probability" header="Abandon Prob" body={(r) => fmt.probToPct(r.abandonment_probability)} sortable />
                                        <Column field="abandonment_risk_score" header="Risk Score" body={(r) => fmt.decimal(r.abandonment_risk_score, 1)} sortable />
                                        <Column field="confidence_score" header="Confidence" body={(r) => fmt.probToPct(r.confidence_score)} />
                                    </DataTable>
                                </div>
                            </Card>
                        </div>
                    ) : <NoInferenceNotice label="Cart Abandonment Predictions" />}
                </div>

                <hr className="border-gray-100" />

                {/* Session Behavior + Session Conversion */}
                <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                    <div className="space-y-4">
                        <InferenceHeader label="Session Behavior Clustering" modelType={sessionClust ? 'file' : null} description="Behavior personas: Quick Buyers, Researchers, Cart Abandoners, etc." count={getRowCount('session_behavior_clustering') ?? sessionClust?.length} />
                        {sessionBehavDist ? (
                            <ChartWrapper title="Behavior Type Distribution">
                                <div style={{ height: 280 }}>
                                    <Bar data={sessionBehavDist} options={barOpts()} />
                                </div>
                            </ChartWrapper>
                        ) : <NoInferenceNotice label="Session Behavior Clustering" />}
                    </div>
                    <div className="space-y-4">
                        <InferenceHeader label="Session Conversion Value Prediction" modelType={sessConvRows ? 'directory' : null} description="Predicted order value if a session converts, with engagement recommendations." count={getRowCount('session_conversion_value') ?? sessConvRows?.length} />
                        {sessConvBarData ? (
                            <ChartWrapper title="Top 12 Sessions — Predicted Conversion Value">
                                <div style={{ height: 280 }}>
                                    <Bar data={sessConvBarData} options={barOpts()} />
                                </div>
                            </ChartWrapper>
                        ) : <NoInferenceNotice label="Session Conversion Value" />}
                    </div>
                </div>
                {sessionClust && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-base font-semibold text-gray-900 mb-3 pb-2 border-b border-gray-200">Session Behavior Assignments</h3>
                            <DataTable value={sessionClust} paginator rows={10} className="p-datatable-sm" stripedRows>
                                <Column field="session_id" header="Session ID" />
                                <Column field="behavior_type" header="Behavior Type" />
                                <Column field="confidence_score" header="Confidence" body={(r) => fmt.probToPct(r.confidence_score)} sortable />
                                <Column field="validation_flag" header="Validation" body={(r) => <Tag value={r.validation_flag ?? '—'} severity={r.validation_flag === 'Confident' ? 'success' : r.validation_flag === 'Review Pattern' ? 'danger' : 'warning'} />} />
                            </DataTable>
                        </div>
                    </Card>
                )}
            </section>

            {/* ================================================================ */}
            {/* SECTION 6 — Reviews, Payments & Product Intelligence             */}
            {/* ================================================================ */}
            <section className="space-y-8">
                <SectionHeader color="bg-rose-500" title="Reviews, Payments & Product Intelligence" />

                {/* Review Sentiment */}
                <div className="space-y-4">
                    <InferenceHeader label="Review Sentiment Predictions" modelType={sentimentRows ? 'directory' : null} description="Classifies each review as Positive / Neutral / Negative." count={getRowCount('review_sentiment_predictions') ?? sentimentRows?.length} />
                    {sentimentRows ? (
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                            <ChartWrapper title="Sentiment Distribution">
                                <div style={{ height: 260 }}>
                                    <Doughnut data={sentimentDist} options={doughnutOpts()} />
                                </div>
                            </ChartWrapper>
                            <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                                <div className="p-6">
                                    <h3 className="text-base font-semibold text-gray-900 mb-3 pb-2 border-b border-gray-200">Most Negative Reviews</h3>
                                    <DataTable value={negativeReviews} rows={8} className="p-datatable-sm" stripedRows>
                                        <Column field="review_id" header="Review ID" />
                                        <Column field="product_id" header="Product ID" />
                                        <Column field="predicted_sentiment" header="Sentiment" body={(r) => <Tag value={r.predicted_sentiment ?? '—'} severity={r.predicted_sentiment === 'Positive' ? 'success' : r.predicted_sentiment === 'Negative' ? 'danger' : 'warning'} />} />
                                        <Column field="sentiment_score" header="Score" body={(r) => fmt.decimal(r.sentiment_score, 2)} sortable />
                                        <Column field="confidence_score" header="Confidence" body={(r) => fmt.probToPct(r.confidence_score)} />
                                    </DataTable>
                                </div>
                            </Card>
                        </div>
                    ) : <NoInferenceNotice label="Review Sentiment Predictions" />}
                </div>

                <hr className="border-gray-100" />

                {/* Payment Success */}
                <div className="space-y-4">
                    <InferenceHeader label="Payment Success Predictions" modelType={paymentRows ? 'directory' : null} description="Predicts whether each payment will succeed or fail." count={getRowCount('payment_success_predictions') ?? paymentRows?.length} />
                    {paymentRows ? (
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                            <ChartWrapper title="Predicted Payment Status Distribution">
                                <div style={{ height: 260 }}>
                                    <Doughnut data={paymentDist} options={doughnutOpts()} />
                                </div>
                            </ChartWrapper>
                            <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                                <div className="p-6">
                                    <h3 className="text-base font-semibold text-gray-900 mb-3 pb-2 border-b border-gray-200">At-Risk / Failed Payments</h3>
                                    <DataTable value={failedPayments} rows={8} className="p-datatable-sm" stripedRows>
                                        <Column field="payment_id" header="Payment ID" />
                                        <Column field="order_id" header="Order ID" />
                                        <Column field="predicted_status" header="Status" body={(r) => <Tag value={r.predicted_status ?? '—'} severity={['Failed', 'Failure'].includes(r.predicted_status) ? 'danger' : 'warning'} />} />
                                        <Column field="success_probability" header="Success Prob" body={(r) => fmt.probToPct(r.success_probability)} sortable />
                                        <Column field="confidence_score" header="Confidence" body={(r) => fmt.probToPct(r.confidence_score)} />
                                    </DataTable>
                                </div>
                            </Card>
                        </div>
                    ) : <NoInferenceNotice label="Payment Success Predictions" />}
                </div>

                <hr className="border-gray-100" />

                {/* Product Bundling */}
                <div className="space-y-4">
                    <InferenceHeader label="Product Bundling Predictions" modelType={bundleRows ? 'directory' : null} description="Complementary product pairs with affinity scores and expected bundle revenue." count={getRowCount('product_bundling_predictions') ?? bundleRows?.length} />
                    {bundleRows ? (
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                            <ChartWrapper title="Bundle Category Distribution">
                                <div style={{ height: 260 }}>
                                    <Bar data={bundleCatDist} options={barOpts()} />
                                </div>
                            </ChartWrapper>
                            <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                                <div className="p-6">
                                    <h3 className="text-base font-semibold text-gray-900 mb-3 pb-2 border-b border-gray-200">Top Complementary Pairs</h3>
                                    <DataTable value={topBundles} rows={8} className="p-datatable-sm" stripedRows>
                                        <Column field="product_id_a" header="Product A" />
                                        <Column field="product_id_b" header="Product B" />
                                        <Column field="bundle_category" header="Category" />
                                        <Column field="affinity_score" header="Affinity" body={(r) => fmt.decimal(r.affinity_score, 3)} sortable />
                                        <Column field="lift" header="Lift" body={(r) => fmt.decimal(r.lift, 2)} sortable />
                                    </DataTable>
                                </div>
                            </Card>
                        </div>
                    ) : <NoInferenceNotice label="Product Bundling" />}
                </div>

                <hr className="border-gray-100" />

                {/* Product Affinity + Lifecycle */}
                <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                    <div className="space-y-4">
                        <InferenceHeader label="Product Affinity Clustering" modelType={affinityRows ? 'file' : null} description="Products clustered by co-purchase patterns for cross-sell." count={getRowCount('product_affinity_clustering') ?? affinityRows?.length} />
                        {affinityDist ? (
                            <ChartWrapper title="Products per Affinity Cluster">
                                <div style={{ height: 280 }}>
                                    <Bar data={affinityDist} options={barOpts()} />
                                </div>
                            </ChartWrapper>
                        ) : <NoInferenceNotice label="Product Affinity Clustering" />}
                        {affinityRows && (
                            <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                                <div className="p-6">
                                    <h3 className="text-base font-semibold text-gray-900 mb-3 pb-2 border-b border-gray-200">Affinity Cluster Table</h3>
                                    <DataTable value={affinityRows} paginator rows={8} className="p-datatable-sm" stripedRows>
                                        <Column field="product_id" header="Product ID" />
                                        <Column field="cluster_id" header="Cluster" />
                                        <Column field="cluster_label" header="Label" />
                                    </DataTable>
                                </div>
                            </Card>
                        )}
                    </div>
                    <div className="space-y-4">
                        <InferenceHeader label="Product Lifecycle Clustering" modelType={lifecycleRows ? 'file' : null} description="Introduction / Growth / Maturity / Decline stage per product." count={getRowCount('product_lifecycle_clustering') ?? lifecycleRows?.length} />
                        {lifecycleDist ? (
                            <ChartWrapper title="Lifecycle Stage Distribution">
                                <div style={{ height: 280 }}>
                                    <Doughnut data={lifecycleDist} options={doughnutOpts()} />
                                </div>
                            </ChartWrapper>
                        ) : <NoInferenceNotice label="Product Lifecycle Clustering" />}
                        {lifecycleRows && (
                            <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                                <div className="p-6">
                                    <h3 className="text-base font-semibold text-gray-900 mb-3 pb-2 border-b border-gray-200">Lifecycle Stage Table</h3>
                                    <DataTable value={lifecycleRows} paginator rows={8} className="p-datatable-sm" stripedRows>
                                        <Column field="product_id" header="Product ID" />
                                        <Column field="lifecycle_stage" header="Stage" body={(r) => <Tag value={r.lifecycle_stage ?? '—'} severity={r.lifecycle_stage === 'Growth' ? 'success' : r.lifecycle_stage === 'Decline' ? 'danger' : 'info'} />} />
                                        <Column field="cluster_centroid_distance" header="Centroid Dist." body={(r) => fmt.decimal(r.cluster_centroid_distance, 3)} sortable />
                                    </DataTable>
                                </div>
                            </Card>
                        )}
                    </div>
                </div>
            </section>
        </div>
    );
}

