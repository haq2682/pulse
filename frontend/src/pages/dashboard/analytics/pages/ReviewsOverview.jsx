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
    CategoryScale, LinearScale, BarElement, ArcElement, Title, Tooltip, Legend,
} from 'chart.js';
import { Bar, Doughnut } from 'react-chartjs-2';
import { useAnalyticsWebSocket } from '../../../../hooks/useAnalyticsWebSocket';
import ChartWrapper from '../components/ChartWrapper';
import { usePipelineProgress } from '@/context/PipelineProgressContext';
import useAnalyticsDateFilter from '@/hooks/useAnalyticsDateFilter';
import DateFilterBar from '../components/DateFilterBar';
import { useFormatters } from '@/hooks/useFormatters';

ChartJS.register(CategoryScale, LinearScale, BarElement, ArcElement, Title, Tooltip, Legend);

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PALETTE = [
    'rgba(59,130,246,0.82)', 'rgba(34,197,94,0.82)',  'rgba(249,115,22,0.82)',
    'rgba(239,68,68,0.82)',  'rgba(139,92,246,0.82)', 'rgba(6,182,212,0.82)',
    'rgba(234,179,8,0.82)',  'rgba(236,72,153,0.82)', 'rgba(20,184,166,0.82)',
    'rgba(168,85,247,0.82)',
];

const TIER_COLORS = {
    'Excellent': 'rgba(34,197,94,0.82)',
    'Good':      'rgba(59,130,246,0.82)',
    'Average':   'rgba(234,179,8,0.82)',
    'Poor':      'rgba(239,68,68,0.82)',
};

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

// ---------------------------------------------------------------------------
// Main Component
// ---------------------------------------------------------------------------

export default function ReviewsOverview() {
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
            console.error('[ReviewsOverview] fetch error');
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
        toastRef.current?.show({ severity: 'info', summary: 'Data Updated', detail: 'Analytics pipeline completed — refreshing review data.', life: 3000 });
    }, [lastUpdate]); // eslint-disable-line

    // -------------------------------------------------------------------------
    // Derived data
    // -------------------------------------------------------------------------

    const derived = useMemo(() => {
        if (!rawReview) return null;
        const a = rawReview.analytics ?? {};

        const tierPerProduct  = a.rating_tier_per_product?.data     ?? [];
        const tierVelocity    = a.rating_tier_sales_velocity?.data   ?? [];
        const sentByCat       = a.sentiment_by_category?.data        ?? [];

        if (tierPerProduct.length === 0 && sentByCat.length === 0) return null;

        // ---- KPIs -----------------------------------------------------------
        const totalProducts  = tierPerProduct.length;
        const totalReviews   = tierPerProduct.reduce((s, r) => s + (+(r.total_reviews ?? 0)), 0);
        const overallRating  = totalReviews > 0
            ? tierPerProduct.reduce((s, r) => s + (+(r.avg_rating_product ?? 0)) * (+(r.total_reviews ?? 0)), 0) / totalReviews
            : 0;
        const excellentCount = tierPerProduct.filter((r) => r.rating_tier === 'Excellent').length;

        // ---- Rating tier distribution (doughnut) ----------------------------
        const tierCounts = {};
        tierPerProduct.forEach((r) => {
            const t = r.rating_tier ?? 'Unknown';
            tierCounts[t] = (tierCounts[t] ?? 0) + 1;
        });
        const tierOrder = ['Excellent', 'Good', 'Average', 'Poor'];
        const tierLabels = tierOrder.filter((t) => tierCounts[t] != null);
        const tierDoughnut = tierLabels.length > 0 ? {
            labels: tierLabels,
            datasets: [{
                data: tierLabels.map((t) => tierCounts[t]),
                backgroundColor: tierLabels.map((t) => TIER_COLORS[t] ?? 'rgba(156,163,175,0.82)'),
                borderWidth: 2,
            }],
        } : null;

        // ---- Avg rating by category bar -------------------------------------
        const catRatingSorted = [...sentByCat].sort((a, b) => (+(b.avg_rating ?? 0)) - (+(a.avg_rating ?? 0)));
        const catRatingBarData = catRatingSorted.length > 0 ? {
            labels: catRatingSorted.map((r) => r.category ?? 'Unknown'),
            datasets: [{
                label: 'Avg Rating',
                data: catRatingSorted.map((r) => +(r.avg_rating ?? 0).toFixed(2)),
                backgroundColor: catRatingSorted.map((r) => {
                    const v = +(r.avg_rating ?? 0);
                    if (v >= 4.0) return 'rgba(34,197,94,0.82)';
                    if (v >= 3.0) return 'rgba(59,130,246,0.82)';
                    if (v >= 2.0) return 'rgba(234,179,8,0.82)';
                    return 'rgba(239,68,68,0.82)';
                }),
            }],
        } : null;

        // ---- Total reviews by category bar ----------------------------------
        const catReviewsSorted = [...sentByCat].sort((a, b) => (+(b.total_reviews ?? 0)) - (+(a.total_reviews ?? 0)));
        const catReviewsBarData = catReviewsSorted.length > 0 ? {
            labels: catReviewsSorted.map((r) => r.category ?? 'Unknown'),
            datasets: [{
                label: 'Total Reviews',
                data: catReviewsSorted.map((r) => +(r.total_reviews ?? 0)),
                backgroundColor: PALETTE,
            }],
        } : null;

        // ---- Review share doughnut (by category) ----------------------------
        const catReviewsDoughnut = catReviewsSorted.length > 0 ? {
            labels: catReviewsSorted.map((r) => r.category ?? 'Unknown'),
            datasets: [{ data: catReviewsSorted.map((r) => +(r.total_reviews ?? 0)), backgroundColor: PALETTE }],
        } : null;

        // ---- Top 15 products by avg rating ----------------------------------
        const topRatedProducts = [...tierPerProduct]
            .sort((a, b) => (+(b.avg_rating_product ?? 0)) - (+(a.avg_rating_product ?? 0)))
            .slice(0, 15);
        const topRatedBarData = topRatedProducts.length > 0 ? {
            labels: topRatedProducts.map((r) => truncate(r.product_name ?? `ID ${r.product_id}`, 22)),
            datasets: [{
                label: 'Avg Rating',
                data: topRatedProducts.map((r) => +(r.avg_rating_product ?? 0).toFixed(2)),
                backgroundColor: topRatedProducts.map((r) =>
                    TIER_COLORS[r.rating_tier] ?? 'rgba(156,163,175,0.82)'
                ),
            }],
        } : null;

        // ---- Bottom 15 products by avg rating -------------------------------
        const bottomRatedProducts = [...tierPerProduct]
            .filter((r) => (+(r.total_reviews ?? 0)) >= 1)
            .sort((a, b) => (+(a.avg_rating_product ?? 5)) - (+(b.avg_rating_product ?? 5)))
            .slice(0, 15);
        const bottomRatedBarData = bottomRatedProducts.length > 0 ? {
            labels: bottomRatedProducts.map((r) => truncate(r.product_name ?? `ID ${r.product_id}`, 22)),
            datasets: [{
                label: 'Avg Rating',
                data: bottomRatedProducts.map((r) => +(r.avg_rating_product ?? 0).toFixed(2)),
                backgroundColor: bottomRatedProducts.map((r) =>
                    TIER_COLORS[r.rating_tier] ?? 'rgba(156,163,175,0.82)'
                ),
            }],
        } : null;

        // ---- Rating tier velocity grouped bar --------------------------------
        const tierVelLabels = tierVelocity.map((r) => r.rating_tier ?? 'Unknown');
        const tierVelGrouped = tierVelocity.length > 0 ? {
            labels: tierVelLabels,
            datasets: [
                { label: 'Avg Units Sold/Product', data: tierVelocity.map((r) => +(r.avg_units_sold_per_product ?? 0).toFixed(0)), backgroundColor: 'rgba(59,130,246,0.82)' },
                { label: 'Avg Revenue/Product ($)', data: tierVelocity.map((r) => +(r.avg_revenue_per_product ?? 0).toFixed(0)), backgroundColor: 'rgba(34,197,94,0.82)' },
                { label: 'Avg Reviews/Product',    data: tierVelocity.map((r) => +(r.avg_reviews_per_product ?? 0).toFixed(1)),  backgroundColor: 'rgba(249,115,22,0.82)' },
            ],
        } : null;

        return {
            kpis: { totalProducts, totalReviews, overallRating, excellentCount },
            tierDoughnut, catRatingBarData, catReviewsBarData, catReviewsDoughnut,
            topRatedBarData, bottomRatedBarData, tierVelGrouped,
            tierPerProduct, tierVelocity, sentByCat,
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
                <p className="text-gray-500 text-base">Loading reviews overview…</p>
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
                        <p className="text-gray-500 text-sm mt-1">Unable to load review data. Please try again later.</p>
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
                            : 'No review data to display.'}
                    </p>
                </div>
            </div>
        );
    }
    if (!derived) return null;
    const {
        kpis, tierDoughnut, catRatingBarData, catReviewsBarData, catReviewsDoughnut,
        topRatedBarData, bottomRatedBarData, tierVelGrouped,
        tierPerProduct, tierVelocity,
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
                    * Review analytics are static aggregates computed over all available data and do not change with the date filter.
                </p>
            )}

            {/* ── KPI Cards ──────────────────────────────────────────────── */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <KPICard
                    icon="pi-star" iconBg="bg-amber-100" iconColor="text-amber-600"
                    value={fmt.decimal(kpis.overallRating, 2)}
                    label="Overall Avg Rating"
                />
                <KPICard
                    icon="pi-comments" iconBg="bg-blue-100" iconColor="text-blue-600"
                    value={fmt.number(kpis.totalReviews)}
                    label="Total Reviews"
                />
                <KPICard
                    icon="pi-box" iconBg="bg-purple-100" iconColor="text-purple-600"
                    value={fmt.number(kpis.totalProducts)}
                    label="Reviewed Products"
                />
                <KPICard
                    icon="pi-check-circle" iconBg="bg-green-100" iconColor="text-green-600"
                    value={fmt.number(kpis.excellentCount)}
                    label="Excellent-Rated Products"
                />
            </div>

            {/* ── Rating Tier Distribution ───────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-amber-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Rating Tier Distribution</h2>
                </div>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {tierDoughnut && (
                        <ChartWrapper title="Products by Rating Tier" height={280}>
                            <Doughnut data={tierDoughnut} options={doughnutOpts()} />
                        </ChartWrapper>
                    )}
                    {tierVelGrouped && (
                        <ChartWrapper title="Avg Sales & Reviews per Product by Rating Tier" height={320}>
                            <Bar data={tierVelGrouped} options={groupedBarOpts()} />
                        </ChartWrapper>
                    )}
                </div>
            </section>

            {/* ── Category Ratings ───────────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-blue-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Ratings by Category</h2>
                </div>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {catRatingBarData && (
                        <ChartWrapper title="Avg Rating by Category (color = tier)" height={340}>
                            <Bar data={catRatingBarData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                    {catReviewsDoughnut && (
                        <ChartWrapper title="Review Volume Share by Category" height={280}>
                            <Doughnut data={catReviewsDoughnut} options={doughnutOpts()} />
                        </ChartWrapper>
                    )}
                    {catReviewsBarData && (
                        <ChartWrapper title="Total Reviews by Category" height={340}>
                            <Bar data={catReviewsBarData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                </div>
            </section>

            {/* ── Top & Bottom Products ──────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-green-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Top & Bottom Rated Products</h2>
                </div>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {topRatedBarData && (
                        <ChartWrapper title="Top 15 Products by Avg Rating" height={420}>
                            <Bar data={topRatedBarData} options={barOpts(true)} />
                        </ChartWrapper>
                    )}
                    {bottomRatedBarData && (
                        <ChartWrapper title="Bottom 15 Products by Avg Rating" height={420}>
                            <Bar data={bottomRatedBarData} options={barOpts(true)} />
                        </ChartWrapper>
                    )}
                </div>
            </section>

            {/* ── Summary Tables ─────────────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-gray-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Summary Tables</h2>
                </div>

                {/* Rating Tier Velocity */}
                {tierVelocity.length > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                Sales Velocity by Rating Tier
                            </h3>
                            <DataTable value={tierVelocity} scrollable stripedRows emptyMessage="No data" className="text-sm">
                                <Column field="rating_tier"               header="Rating Tier"           sortable body={(r) => (
                                    <Tag value={r.rating_tier ?? '—'} severity={TIER_SEVERITY[r.rating_tier] ?? 'secondary'} />
                                )} />
                                <Column field="products_in_tier"          header="Products"              sortable body={(r) => fmt.number(r.products_in_tier)} />
                                <Column field="avg_rating_in_tier"        header="Avg Rating"            sortable body={(r) => fmt.decimal(r.avg_rating_in_tier, 2)} />
                                <Column field="avg_reviews_per_product"   header="Avg Reviews/Product"   sortable body={(r) => fmt.decimal(r.avg_reviews_per_product, 1)} />
                                <Column field="total_units_sold_tier"     header="Total Units Sold"      sortable body={(r) => fmt.number(r.total_units_sold_tier)} />
                                <Column field="avg_units_sold_per_product" header="Avg Units/Product"   sortable body={(r) => fmt.decimal(r.avg_units_sold_per_product, 1)} />
                                <Column field="total_revenue_tier"        header="Total Revenue"         sortable body={(r) => fmt.currency(r.total_revenue_tier)} />
                                <Column field="avg_revenue_per_product"   header="Avg Revenue/Product"   sortable body={(r) => fmt.currency(r.avg_revenue_per_product)} />
                            </DataTable>
                        </div>
                    </Card>
                )}

                {/* All Products Rating Tier */}
                {tierPerProduct.length > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                Product Rating Details
                            </h3>
                            <DataTable
                                value={[...tierPerProduct].sort((a, b) => (+(b.avg_rating_product ?? 0)) - (+(a.avg_rating_product ?? 0)))}
                                paginator rows={15}
                                scrollable stripedRows emptyMessage="No data" className="text-sm">
                                <Column header="Product"              sortable sortField="product_name" body={(r) => truncate(r.product_name ?? `ID ${r.product_id}`, 32)} />
                                <Column field="category"              header="Category"     sortable />
                                <Column field="total_reviews"         header="Reviews"      sortable body={(r) => fmt.number(r.total_reviews)} />
                                <Column field="avg_rating_product"    header="Avg Rating"   sortable body={(r) => fmt.decimal(r.avg_rating_product, 2)} />
                                <Column field="total_units_sold"      header="Units Sold"   sortable body={(r) => fmt.number(r.total_units_sold)} />
                                <Column field="total_revenue"         header="Revenue"      sortable body={(r) => fmt.currency(r.total_revenue)} />
                                <Column field="rating_tier"           header="Tier"         sortable body={(r) => (
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
