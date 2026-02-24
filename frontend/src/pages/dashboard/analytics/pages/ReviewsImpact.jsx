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
    CategoryScale, LinearScale, BarElement, PointElement, LineElement,
    ArcElement, Title, Tooltip, Legend,
} from 'chart.js';
import { Bar, Line, Doughnut } from 'react-chartjs-2';
import { useAnalyticsWebSocket } from '../../../../hooks/useAnalyticsWebSocket';
import ChartWrapper from '../components/ChartWrapper';
import { usePipelineProgress } from '@/context/PipelineProgressContext';
import useAnalyticsDateFilter from '@/hooks/useAnalyticsDateFilter';
import DateFilterBar from '../components/DateFilterBar';
import { useFormatters } from '@/hooks/useFormatters';

ChartJS.register(
    CategoryScale, LinearScale, BarElement, PointElement, LineElement,
    ArcElement, Title, Tooltip, Legend,
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

const TIER_SEVERITY = {
    'Excellent': 'success',
    'Good':      'info',
    'Average':   'warning',
    'Poor':      'danger',
};

const truncate = (s, n = 28) => (s && s.length > n ? `${s.slice(0, n)}…` : (s ?? '—'));

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

// ---------------------------------------------------------------------------
// Chart options
// ---------------------------------------------------------------------------

const barOpts = (horizontal = false) => ({
    indexAxis: horizontal ? 'y' : 'x',
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { display: false }, title: { display: false } },
    scales: {
        x: { grid: { color: 'rgba(0,0,0,0.05)' } },
        y: { beginAtZero: true, grid: { color: 'rgba(0,0,0,0.05)' } },
    },
});

const groupedBarOpts = () => ({
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { position: 'top' }, title: { display: false } },
    scales: {
        x: { grid: { color: 'rgba(0,0,0,0.05)' } },
        y: { beginAtZero: true, grid: { color: 'rgba(0,0,0,0.05)' } },
    },
});

const doughnutOpts = () => ({
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { position: 'right' }, title: { display: false } },
});

const lineOpts = () => ({
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { display: true, position: 'top' }, title: { display: false } },
    scales: {
        x: { grid: { color: 'rgba(0,0,0,0.05)' } },
        y: { beginAtZero: true, grid: { color: 'rgba(0,0,0,0.05)' } },
    },
});

// ---------------------------------------------------------------------------
// Main Component
// ---------------------------------------------------------------------------

export default function ReviewsImpact() {
    const fmt = useFormatters();
    const { businessId } = useParams();
    const toastRef = useRef(null);
    const { pipelineStatus } = usePipelineProgress();
    const { dateRange, setDateRange, quickFilter, isFiltered, applyQuickFilter, resetFilters } = useAnalyticsDateFilter();
    const { lastUpdate } = useAnalyticsWebSocket(businessId);

    const [rawReview, setRawReview] = useState(null);
    const [loading, setLoading] = useState(true);
    const [fetchError, setFetchError] = useState(false);
    const [dataMode, setDataMode] = useState('unknown');

    // -------------------------------------------------------------------------
    // Fetch
    // -------------------------------------------------------------------------

    const buildUrl = useCallback(() => {
        const base = import.meta.env.VITE_API_URL || 'http://localhost:8000';
        const params = new URLSearchParams({ categories: 'review_analytics' });
        return `${base}/analytics/data/${businessId}?${params.toString()}`;
    }, [businessId]);

    const fetchData = useCallback(async () => {
        if (!businessId) return;
        try {
            setLoading(true);
            setFetchError(false);
            const res = await fetch(buildUrl());
            if (!res.ok) {
                setRawReview(null);
                return;
            }
            const json = await res.json();
            if (json.mode) setDataMode(json.mode);
            setRawReview(json.categories?.review_analytics ?? null);
        } catch {
            console.error('[ReviewsImpact] fetch error');
            setFetchError(true);
            setRawReview(null);
        } finally {
            setLoading(false);
        }
    }, [buildUrl, businessId]);

    useEffect(() => { fetchData(); }, [businessId]); // eslint-disable-line
    useEffect(() => {
        if (!lastUpdate) return;
        fetchData();
        toastRef.current?.show({ severity: 'info', summary: 'Data Updated', detail: 'Analytics pipeline completed — refreshing impact data.', life: 3000 });
    }, [lastUpdate]); // eslint-disable-line

    // -------------------------------------------------------------------------
    // Derived data
    // -------------------------------------------------------------------------

    const derived = useMemo(() => {
        if (!rawReview) return null;
        const a = rawReview.analytics ?? {};

        const tierVelocity   = a.rating_tier_sales_velocity?.data                 ?? [];
        const prodMonthly    = a.product_monthly_rating_trends?.data               ?? [];
        const lowRatedTrends = a.low_rated_product_monthly_trends_rating_only?.data ?? [];
        const tierPerProduct = a.rating_tier_per_product?.data                     ?? [];

        if (tierVelocity.length === 0 && tierPerProduct.length === 0) return null;

        // ---- KPIs -----------------------------------------------------------
        const excellentTier  = tierVelocity.find((r) => r.rating_tier === 'Excellent');
        const poorTier       = tierVelocity.find((r) => r.rating_tier === 'Poor');
        const excellentRev   = excellentTier?.total_revenue_tier ?? 0;
        const poorRev        = poorTier?.total_revenue_tier ?? 0;
        const revImpactDiff  = (+(excellentTier?.avg_revenue_per_product ?? 0)) - (+(poorTier?.avg_revenue_per_product ?? 0));
        const lowRatedCount  = tierPerProduct.filter((r) => r.rating_tier === 'Poor' || r.rating_tier === 'Average').length;

        // ---- Tier revenue comparison (grouped bar) --------------------------
        const tierRevGrouped = tierVelocity.length > 0 ? {
            labels: tierVelocity.map((r) => r.rating_tier ?? 'Unknown'),
            datasets: [
                { label: 'Total Revenue ($)', data: tierVelocity.map((r) => +(r.total_revenue_tier ?? 0).toFixed(0)),          backgroundColor: 'rgba(59,130,246,0.82)' },
                { label: 'Total Units Sold',  data: tierVelocity.map((r) => +(r.total_units_sold_tier ?? 0)),                   backgroundColor: 'rgba(34,197,94,0.82)' },
            ],
        } : null;

        // ---- Avg revenue per product by tier (bar) --------------------------
        const avgRevTierData = tierVelocity.length > 0 ? {
            labels: tierVelocity.map((r) => r.rating_tier ?? 'Unknown'),
            datasets: [{
                label: 'Avg Revenue per Product ($)',
                data: tierVelocity.map((r) => +(r.avg_revenue_per_product ?? 0).toFixed(0)),
                backgroundColor: tierVelocity.map((r) => {
                    const t = r.rating_tier ?? '';
                    if (t === 'Excellent') return 'rgba(34,197,94,0.82)';
                    if (t === 'Good')      return 'rgba(59,130,246,0.82)';
                    if (t === 'Average')   return 'rgba(234,179,8,0.82)';
                    return 'rgba(239,68,68,0.82)';
                }),
            }],
        } : null;

        // ---- Avg units sold per product by tier (bar) ----------------------
        const avgUnitsTierData = tierVelocity.length > 0 ? {
            labels: tierVelocity.map((r) => r.rating_tier ?? 'Unknown'),
            datasets: [{
                label: 'Avg Units Sold per Product',
                data: tierVelocity.map((r) => +(r.avg_units_sold_per_product ?? 0).toFixed(1)),
                backgroundColor: 'rgba(139,92,246,0.82)',
            }],
        } : null;

        // ---- Revenue share by tier (doughnut) --------------------------------
        const revShareDoughnut = tierVelocity.length > 0 ? {
            labels: tierVelocity.map((r) => r.rating_tier ?? 'Unknown'),
            datasets: [{
                data: tierVelocity.map((r) => +(r.total_revenue_tier ?? 0).toFixed(0)),
                backgroundColor: tierVelocity.map((r) => {
                    const t = r.rating_tier ?? '';
                    if (t === 'Excellent') return 'rgba(34,197,94,0.82)';
                    if (t === 'Good')      return 'rgba(59,130,246,0.82)';
                    if (t === 'Average')   return 'rgba(234,179,8,0.82)';
                    return 'rgba(239,68,68,0.82)';
                }),
                borderWidth: 2,
            }],
        } : null;

        // ---- Top 10 products by revenue × rating (scatter-like grouped) ----
        const topImpactProds = [...tierPerProduct]
            .sort((a, b) => (+(b.total_revenue ?? 0)) - (+(a.total_revenue ?? 0)))
            .slice(0, 10);
        const topImpactGrouped = topImpactProds.length > 0 ? {
            labels: topImpactProds.map((r) => truncate(r.product_name ?? `ID ${r.product_id}`, 20)),
            datasets: [
                { label: 'Total Revenue ($)', data: topImpactProds.map((r) => +(r.total_revenue ?? 0).toFixed(0)),       backgroundColor: 'rgba(59,130,246,0.82)' },
                { label: 'Units Sold',        data: topImpactProds.map((r) => +(r.total_units_sold ?? 0)),                backgroundColor: 'rgba(34,197,94,0.82)' },
            ],
        } : null;

        // ---- Low rated trending products (monthly avg rating line, top 5 products) ----
        // Aggregate by product_id — build a line per product
        const lowRatedProducts = [...new Map(lowRatedTrends.map((r) => [r.product_id, r])).values()]
            .sort((a, b) => (+(b.total_revenue ?? 0)) - (+(a.total_revenue ?? 0)))
            .slice(0, 5);
        const monthLabelsLow = [...new Set(
            lowRatedTrends
                .map((r) => r.year_month ?? `${r.review_year}-${String(r.review_month ?? 1).padStart(2, '0')}`)
        )].sort();
        const lowRatedLineData = (lowRatedProducts.length > 0 && monthLabelsLow.length > 0) ? {
            labels: monthLabelsLow,
            datasets: lowRatedProducts.map((prod, i) => ({
                label: truncate(prod.product_name ?? `ID ${prod.product_id}`, 20),
                data: monthLabelsLow.map((m) => {
                    const row = lowRatedTrends.find((r) =>
                        r.product_id === prod.product_id &&
                        (r.year_month ?? `${r.review_year}-${String(r.review_month ?? 1).padStart(2, '0')}`) === m
                    );
                    return row ? +(row.avg_rating_month ?? 0).toFixed(2) : null;
                }),
                borderColor: PALETTE[i % PALETTE.length],
                backgroundColor: PALETTE[i % PALETTE.length].replace('0.82)', '0.2)'),
                tension: 0.3, spanGaps: true,
            })),
        } : null;

        // ---- Top improving / declining product trends (from prodMonthly) ----
        // For each product compute first-month vs last-month rating delta
        const prodIds = [...new Set(prodMonthly.map((r) => r.product_id))];
        const prodDeltas = prodIds.map((id) => {
            const rows = prodMonthly
                .filter((r) => r.product_id === id)
                .sort((a, b) => {
                    const ka = a.year_month ?? `${a.review_year}-${a.review_month}`;
                    const kb = b.year_month ?? `${b.review_year}-${b.review_month}`;
                    return ka.localeCompare(kb);
                });
            if (rows.length < 2) return null;
            const first = +(rows[0].avg_rating_month ?? 0);
            const last  = +(rows[rows.length - 1].avg_rating_month ?? 0);
            return { product_id: id, product_name: rows[0].product_name, category: rows[0].category, delta: last - first, lastRating: last };
        }).filter(Boolean);

        const topImproving = prodDeltas
            .filter((p) => p.delta > 0)
            .sort((a, b) => b.delta - a.delta)
            .slice(0, 10);
        const topDeclining = prodDeltas
            .filter((p) => p.delta < 0)
            .sort((a, b) => a.delta - b.delta)
            .slice(0, 10);

        const improvingBarData = topImproving.length > 0 ? {
            labels: topImproving.map((p) => truncate(p.product_name ?? `ID ${p.product_id}`, 22)),
            datasets: [{
                label: 'Rating Change',
                data: topImproving.map((p) => +p.delta.toFixed(2)),
                backgroundColor: 'rgba(34,197,94,0.82)',
            }],
        } : null;

        const decliningBarData = topDeclining.length > 0 ? {
            labels: topDeclining.map((p) => truncate(p.product_name ?? `ID ${p.product_id}`, 22)),
            datasets: [{
                label: 'Rating Change',
                data: topDeclining.map((p) => +p.delta.toFixed(2)),
                backgroundColor: 'rgba(239,68,68,0.82)',
            }],
        } : null;

        return {
            kpis: { excellentRev, poorRev, revImpactDiff, lowRatedCount },
            tierRevGrouped, avgRevTierData, avgUnitsTierData, revShareDoughnut,
            topImpactGrouped, lowRatedLineData, improvingBarData, decliningBarData,
            tierVelocity, tierPerProduct,
        };
    }, [rawReview]);

    const hasData = derived !== null;

    // -------------------------------------------------------------------------
    // Render states
    // -------------------------------------------------------------------------

    if (loading && pipelineStatus !== 'loading') {
        return (
            <div className="flex flex-col items-center justify-center min-h-[400px] gap-4">
                <ProgressSpinner />
                <p className="text-gray-500 text-base">Loading review impact analytics…</p>
            </div>
        );
    }

    if (fetchError) {
        return (
            <div className="p-6 min-h-[calc(100vh-120px)]">
                <Toast ref={toastRef} />
                <DateFilterBar
                    quickFilter={quickFilter} dateRange={dateRange} isFiltered={isFiltered}
                    onQuickFilter={applyQuickFilter} onDateChange={setDateRange} onReset={resetFilters}
                    dataMode={dataMode}
                />
                <div className="flex items-center justify-center min-h-[50vh]">
                    <div className="text-center">
                        <i className="pi pi-exclamation-circle text-5xl text-red-400 mb-3 block" />
                        <p className="text-gray-700 font-medium text-lg">Something went wrong</p>
                        <p className="text-gray-500 text-sm mt-1">Unable to load review impact data. Please try again later.</p>
                    </div>
                </div>
            </div>
        );
    }

    if (!hasData && !loading && pipelineStatus !== 'loading') {
        return (
            <div className="p-6 min-h-[calc(100vh-120px)]">
                <Toast ref={toastRef} />
                <DateFilterBar
                    quickFilter={quickFilter} dateRange={dateRange} isFiltered={isFiltered}
                    onQuickFilter={applyQuickFilter} onDateChange={setDateRange} onReset={resetFilters}
                    dataMode={dataMode}
                />
                <div className="flex items-center justify-center min-h-[50vh]">
                    <p className="text-gray-500 text-lg">
                        {isFiltered
                            ? 'No data found for the selected date range.'
                            : 'No review impact data to display.'}
                    </p>
                </div>
            </div>
        );
    }
    if (!derived) return null;
    const {
        kpis, tierRevGrouped, avgRevTierData, avgUnitsTierData, revShareDoughnut,
        topImpactGrouped, lowRatedLineData, improvingBarData, decliningBarData,
        tierVelocity, tierPerProduct,
    } = derived;

    return (
        <div className="p-6 space-y-8">
            <Toast ref={toastRef} />
            <DateFilterBar
                    quickFilter={quickFilter}
                    dateRange={dateRange}
                    isFiltered={isFiltered}
                    onQuickFilter={applyQuickFilter}
                    onDateChange={setDateRange}
                    onReset={resetFilters}
                    dataMode={dataMode}
                />

            {/* ── Static-data notice ─────────────────────────────────────── */}
            {isFiltered && (
                <p className="mb-4 text-xs text-gray-400 italic">
                    * Review impact analytics are static aggregates computed over all available data and do not change with the date filter.
                    Trend charts reflect the full historical period.
                </p>
            )}

            {/* ── KPI Cards ──────────────────────────────────────────────── */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <KPICard
                    icon="pi-star-fill" iconBg="bg-green-100" iconColor="text-green-600"
                    value={fmt.currency(kpis.excellentRev)}
                    label="Excellent-Tier Revenue"
                />
                <KPICard
                    icon="pi-times-circle" iconBg="bg-red-100" iconColor="text-red-600"
                    value={fmt.currency(kpis.poorRev)}
                    label="Poor-Tier Revenue"
                />
                <KPICard
                    icon="pi-chart-bar" iconBg="bg-blue-100" iconColor="text-blue-600"
                    value={fmt.currency(kpis.revImpactDiff)}
                    label="Avg Rev/Product: Excellent vs Poor"
                />
                <KPICard
                    icon="pi-exclamation-triangle" iconBg="bg-amber-100" iconColor="text-amber-600"
                    value={fmt.number(kpis.lowRatedCount)}
                    label="Low/Average Rated Products"
                />
            </div>

            {/* ── Revenue by Rating Tier ─────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-blue-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Revenue by Rating Tier</h2>
                </div>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {revShareDoughnut && (
                        <ChartWrapper title="Revenue Share by Rating Tier" height={280}>
                            <Doughnut data={revShareDoughnut} options={doughnutOpts()} />
                        </ChartWrapper>
                    )}
                    {avgRevTierData && (
                        <ChartWrapper title="Avg Revenue per Product by Rating Tier" height={280}>
                            <Bar data={avgRevTierData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                    {tierRevGrouped && (
                        <ChartWrapper title="Total Revenue & Units Sold by Rating Tier" height={300}>
                            <Bar data={tierRevGrouped} options={groupedBarOpts()} />
                        </ChartWrapper>
                    )}
                    {avgUnitsTierData && (
                        <ChartWrapper title="Avg Units Sold per Product by Rating Tier" height={280}>
                            <Bar data={avgUnitsTierData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                </div>
            </section>

            {/* ── Top Revenue Products ───────────────────────────────────── */}
            {topImpactGrouped && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-green-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Top Products by Revenue</h2>
                    </div>
                    <ChartWrapper title="Top 10 Products — Revenue & Units Sold" height={340}>
                        <Bar data={topImpactGrouped} options={groupedBarOpts()} />
                    </ChartWrapper>
                </section>
            )}

            {/* ── Improving & Declining Products ────────────────────────── */}
            {(improvingBarData || decliningBarData) && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-purple-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Rating Trend: Improving vs Declining</h2>
                    </div>
                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                        {improvingBarData && (
                            <ChartWrapper title="Top 10 Improving Products (Rating Change)" height={400}>
                                <Bar data={improvingBarData} options={barOpts(true)} />
                            </ChartWrapper>
                        )}
                        {decliningBarData && (
                            <ChartWrapper title="Top 10 Declining Products (Rating Change)" height={400}>
                                <Bar data={decliningBarData} options={barOpts(true)} />
                            </ChartWrapper>
                        )}
                    </div>
                </section>
            )}

            {/* ── Low Rated Product Trends ───────────────────────────────── */}
            {lowRatedLineData && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-red-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Low-Rated Product Rating Trends</h2>
                    </div>
                    <ChartWrapper title="Monthly Avg Rating Trend for Low-Revenue Low-Rated Products (Top 5)" height={360}>
                        <Line data={lowRatedLineData} options={lineOpts()} />
                    </ChartWrapper>
                </section>
            )}

            {/* ── Summary Tables ─────────────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-gray-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Summary Tables</h2>
                </div>

                {/* Tier Velocity Table */}
                {tierVelocity.length > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                Sales Velocity by Rating Tier
                            </h3>
                            <DataTable value={tierVelocity} scrollable stripedRows emptyMessage="No data" className="text-sm">
                                <Column field="rating_tier"               header="Rating Tier"         sortable body={(r) => (
                                    <Tag value={r.rating_tier ?? '—'} severity={TIER_SEVERITY[r.rating_tier] ?? 'secondary'} />
                                )} />
                                <Column field="products_in_tier"          header="Products"            sortable body={(r) => fmt.number(r.products_in_tier)} />
                                <Column field="avg_rating_in_tier"        header="Avg Rating"          sortable body={(r) => fmt.decimal(r.avg_rating_in_tier, 2)} />
                                <Column field="total_units_sold_tier"     header="Total Units Sold"    sortable body={(r) => fmt.number(r.total_units_sold_tier)} />
                                <Column field="avg_units_sold_per_product" header="Avg Units/Product" sortable body={(r) => fmt.decimal(r.avg_units_sold_per_product, 1)} />
                                <Column field="total_revenue_tier"        header="Total Revenue"       sortable body={(r) => fmt.currency(r.total_revenue_tier)} />
                                <Column field="avg_revenue_per_product"   header="Avg Revenue/Product" sortable body={(r) => fmt.currency(r.avg_revenue_per_product)} />
                            </DataTable>
                        </div>
                    </Card>
                )}

                {/* Low/Average Rated Products */}
                {tierPerProduct.filter((r) => r.rating_tier === 'Poor' || r.rating_tier === 'Average').length > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                Products Needing Attention (Poor or Average Rating)
                            </h3>
                            <DataTable
                                value={[...tierPerProduct]
                                    .filter((r) => r.rating_tier === 'Poor' || r.rating_tier === 'Average')
                                    .sort((a, b) => (+(b.total_revenue ?? 0)) - (+(a.total_revenue ?? 0)))}
                                paginator rows={15}
                                scrollable stripedRows emptyMessage="No data" className="text-sm">
                                <Column header="Product"           sortable sortField="product_name" body={(r) => truncate(r.product_name ?? `ID ${r.product_id}`, 32)} />
                                <Column field="category"           header="Category"     sortable />
                                <Column field="total_reviews"      header="Reviews"      sortable body={(r) => fmt.number(r.total_reviews)} />
                                <Column field="avg_rating_product" header="Avg Rating"   sortable body={(r) => fmt.decimal(r.avg_rating_product, 2)} />
                                <Column field="total_units_sold"   header="Units Sold"   sortable body={(r) => fmt.number(r.total_units_sold)} />
                                <Column field="total_revenue"      header="Revenue"      sortable body={(r) => fmt.currency(r.total_revenue)} />
                                <Column field="rating_tier"        header="Tier"         sortable body={(r) => (
                                    <Tag value={r.rating_tier ?? '—'} severity={TIER_SEVERITY[r.rating_tier] ?? 'secondary'} />
                                )} />
                            </DataTable>
                        </div>
                    </Card>
                )}
            </section>
        </div>
    );
}
