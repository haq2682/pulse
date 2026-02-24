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
    CategoryScale, LinearScale, PointElement, LineElement,
    BarElement, ArcElement, Title, Tooltip, Legend,
} from 'chart.js';
import { Bar, Doughnut } from 'react-chartjs-2';
import { useAnalyticsWebSocket } from '../../../../hooks/useAnalyticsWebSocket';
import ChartWrapper from '../components/ChartWrapper';
import { usePipelineProgress } from '@/context/PipelineProgressContext';
import useAnalyticsDateFilter from '@/hooks/useAnalyticsDateFilter';
import DateFilterBar from '../components/DateFilterBar';
import { useFormatters } from '@/hooks/useFormatters';

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, BarElement, ArcElement, Title, Tooltip, Legend);

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PALETTE = [
    'rgba(59,130,246,0.82)', 'rgba(34,197,94,0.82)',  'rgba(249,115,22,0.82)',
    'rgba(239,68,68,0.82)',  'rgba(139,92,246,0.82)', 'rgba(6,182,212,0.82)',
    'rgba(234,179,8,0.82)',  'rgba(236,72,153,0.82)', 'rgba(20,184,166,0.82)',
    'rgba(168,85,247,0.82)',
];

const AFFINITY_COLORS = {
    'Strong':   'rgba(34,197,94,0.85)',
    'Moderate': 'rgba(59,130,246,0.85)',
    'Weak':     'rgba(234,179,8,0.85)',
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

const MetricRow = ({ label, value }) => (
    <div className="flex justify-between items-center p-3 bg-gray-50 rounded-lg hover:bg-gray-100 transition-colors">
        <span className="text-gray-700 font-medium">{label}</span>
        <span className="text-gray-900 font-semibold text-lg">{value}</span>
    </div>
);

const MetricsCard = ({ title, rows }) => {
    const visible = rows.filter((r) => r.show);
    if (visible.length === 0) return null;
    return (
        <Card className="bg-white border border-gray-200 rounded-xl p-0 shadow-sm">
            <div className="p-6">
                <h3 className="text-xl font-semibold text-gray-900 mb-6 pb-3 border-b-2 border-gray-200">{title}</h3>
                <div className="flex flex-col gap-4">
                    {visible.map((r) => <MetricRow key={r.label} label={r.label} value={r.value} />)}
                </div>
            </div>
        </Card>
    );
};

// Coverage Gauge Card
const CoverageCard = ({ total, withRecs, rate }) => {
    const fmt = useFormatters();
    return (
        <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
            <div className="p-6">
                <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">Recommendation Coverage *</h3>
                <div className="flex flex-col gap-4">
                    <div className="text-center py-4">
                        <p className="text-5xl font-bold text-blue-600 mb-2">{fmt.pct((rate ?? 0) * 100)}</p>
                        <p className="text-sm text-gray-500">of products have recommendations</p>
                    </div>
                    <div className="w-full bg-gray-200 rounded-full h-3">
                        <div className="bg-blue-500 h-3 rounded-full transition-all duration-700" style={{ width: `${Math.min((rate ?? 0) * 100, 100)}%` }} />
                    </div>
                    <div className="grid grid-cols-2 gap-4 mt-2">
                        <div className="text-center p-3 bg-blue-50 rounded-lg">
                            <p className="text-xl font-bold text-blue-700">{fmt.number(withRecs)}</p>
                            <p className="text-xs text-gray-500">With recommendations</p>
                        </div>
                        <div className="text-center p-3 bg-gray-50 rounded-lg">
                            <p className="text-xl font-bold text-gray-700">{fmt.number(total)}</p>
                            <p className="text-xs text-gray-500">Total products</p>
                        </div>
                    </div>
                </div>
            </div>
        </Card>
    );
};

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

const ProductEngagement = () => {
    const fmt = useFormatters();
    const { businessId } = useParams();
    const toastRef = useRef(null);
    const { pipelineStatus } = usePipelineProgress();

    const [loading, setLoading]   = useState(true);
    const [fetchError, setFetchError] = useState(false);
    const [rawData, setRawData]   = useState(null);
    const [dataMode, setDataMode] = useState('unknown');

    const { dateRange, setDateRange, quickFilter, isFiltered, applyQuickFilter, resetFilters, toISODate } = useAnalyticsDateFilter();
    const { lastUpdate } = useAnalyticsWebSocket(businessId);

    // -----------------------------------------------------------------------
    // Fetch
    // -----------------------------------------------------------------------

    const buildUrl = useCallback((from, to) => {
        const base   = import.meta.env.VITE_API_URL || 'http://localhost:8000';
        const params = new URLSearchParams({ categories: 'product_analytics' });
        if (from) params.set('date_from', toISODate(from));
        if (to)   params.set('date_to',   toISODate(to));
        return `${base}/analytics/data/${businessId}?${params.toString()}`;
    }, [businessId, toISODate]);

    const fetchData = useCallback(async (from, to) => {
        if (!businessId) return;
        setLoading(true);
            setFetchError(false);
        try {
            const res = await fetch(buildUrl(from, to));
            if (!res.ok) {
                toastRef.current?.show({ severity: 'warn', summary: 'No Data', detail: 'Run the analytics pipeline first.', life: 5000 });
                setRawData(null);
                return;
            }
            const json = await res.json();
            if (json.mode) setDataMode(json.mode);
            setRawData(json.categories?.product_analytics ?? null);
        } catch {
            console.error('[fetch] Analytics load error');
            setFetchError(true);
            setRawData(null);
        } finally {
            setLoading(false);
        }
    }, [businessId, buildUrl]);

    useEffect(() => { fetchData(null, null); }, [businessId]);           // eslint-disable-line
    useEffect(() => { fetchData(dateRange.from, dateRange.to); }, [dateRange]); // eslint-disable-line
    useEffect(() => {
        if (lastUpdate?.files) {
            toastRef.current?.show({ severity: 'info', summary: 'Data Updated', detail: `${lastUpdate.total_files} metric(s) updated`, life: 3000 });
            fetchData(dateRange.from, dateRange.to);
        }
    }, [lastUpdate]); // eslint-disable-line

    // -----------------------------------------------------------------------
    // Derived data — all static aggregates
    // -----------------------------------------------------------------------

    const derived = useMemo(() => {
        if (!rawData) return null;
        const a = rawData.analytics ?? {};

        const categoryViewPatterns       = a.category_view_patterns?.data ?? [];
        const categoryPopularity         = a.category_popularity_score?.data ?? [];
        const productAffinityPairs       = a.product_affinity_pairs?.data ?? [];
        const categoryAffinityPairs      = a.category_affinity_pairs?.data ?? [];
        const categoryAffinityTopPerCat  = a.category_affinity_top_per_category?.data ?? [];
        const productAffinityTopPerProd  = a.product_affinity_top_per_product?.data ?? [];
        const recoCoverage               = (a.precomputed_reco_coverage?.data ?? [])[0] ?? {};
        const topVtoP                    = a.top_view_to_purchase_products?.data ?? [];
        const supplierProductPerf        = a.supplier_product_performance?.data ?? [];
        const stockoutRateByProduct      = a.stockout_rate_by_product?.data ?? [];
        const supplierStockoutImpact     = a.supplier_stockout_impact_on_products?.data ?? [];

        // KPIs
        const topPopCat     = [...categoryPopularity].sort((a, b) => (b.category_popularity_score ?? 0) - (a.category_popularity_score ?? 0))[0];
        const topViewCat    = [...categoryViewPatterns].sort((a, b) => (b.avg_view_to_purchase_rate ?? 0) - (a.avg_view_to_purchase_rate ?? 0))[0];
        const totalRevFromViews = categoryViewPatterns.reduce((s, r) => s + (r.total_revenue ?? 0), 0);

        return {
            categoryViewPatterns, categoryPopularity, productAffinityPairs,
            categoryAffinityPairs, categoryAffinityTopPerCat, productAffinityTopPerProd,
            recoCoverage, topVtoP,
            supplierProductPerf, stockoutRateByProduct, supplierStockoutImpact,
            topPopCat, topViewCat, totalRevFromViews,
        };
    }, [rawData]);

    // -----------------------------------------------------------------------
    // Chart data
    // -----------------------------------------------------------------------

    // Category View Patterns — avg view-to-purchase bar
    const viewPatternData = useMemo(() => {
        const rows = [...(derived?.categoryViewPatterns ?? [])].sort((a, b) => (b.avg_view_to_purchase_rate ?? 0) - (a.avg_view_to_purchase_rate ?? 0));
        return {
            labels: rows.map((r) => r.category ?? ''),
            datasets: [
                { label: 'Avg View→Purchase (%)', data: rows.map((r) => ((r.avg_view_to_purchase_rate ?? 0) * 100).toFixed(2)), backgroundColor: 'rgba(34,197,94,0.8)' },
                { label: 'Avg Revenue / View ($)', data: rows.map((r) => (r.avg_revenue_per_view ?? 0).toFixed(2)), backgroundColor: 'rgba(59,130,246,0.8)' },
            ],
        };
    }, [derived]);

    // Category Popularity Score — bar
    const popScoreData = useMemo(() => {
        const rows = [...(derived?.categoryPopularity ?? [])].sort((a, b) => (b.category_popularity_score ?? 0) - (a.category_popularity_score ?? 0));
        return {
            labels: rows.map((r) => r.category ?? ''),
            datasets: [{ label: 'Popularity Score', data: rows.map((r) => (r.category_popularity_score ?? 0).toFixed(2)), backgroundColor: PALETTE }],
        };
    }, [derived]);

    // Category Wishlist + Cart adds — grouped bar
    const wishCartData = useMemo(() => {
        const rows = [...(derived?.categoryPopularity ?? [])].sort((a, b) => (b.total_wishlist_adds ?? 0) - (a.total_wishlist_adds ?? 0));
        return {
            labels: rows.map((r) => r.category ?? ''),
            datasets: [
                { label: 'Wishlist Adds', data: rows.map((r) => r.total_wishlist_adds ?? 0), backgroundColor: 'rgba(139,92,246,0.8)' },
                { label: 'Cart Adds',     data: rows.map((r) => r.total_cart_adds ?? 0),     backgroundColor: 'rgba(249,115,22,0.8)' },
            ],
        };
    }, [derived]);

    // Category Affinity Pairs — bar (top 10 by avg_lift)
    const catAffinityData = useMemo(() => {
        const top10 = [...(derived?.categoryAffinityPairs ?? [])].sort((a, b) => (b.avg_lift_between_categories ?? 0) - (a.avg_lift_between_categories ?? 0)).slice(0, 10);
        return {
            labels: top10.map((r) => `${r.product_a_category ?? ''} → ${r.product_b_category ?? ''}`),
            datasets: [{ label: 'Avg Lift', data: top10.map((r) => (r.avg_lift_between_categories ?? 0).toFixed(3)), backgroundColor: 'rgba(6,182,212,0.8)' }],
        };
    }, [derived]);

    // Category units sold — doughnut
    const catUnitsDoughnutData = useMemo(() => {
        const rows = [...(derived?.categoryViewPatterns ?? [])].sort((a, b) => (b.total_units_sold ?? 0) - (a.total_units_sold ?? 0));
        return {
            labels: rows.map((r) => r.category ?? ''),
            datasets: [{ data: rows.map((r) => r.total_units_sold ?? 0), backgroundColor: PALETTE }],
        };
    }, [derived]);

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    const affinitySeverity = (strength) => {
        if (!strength) return 'secondary';
        const s = strength.toLowerCase();
        if (s === 'strong') return 'success';
        if (s === 'moderate') return 'info';
        return 'warning';
    };

    const hasData = useMemo(() => !!(derived?.categoryViewPatterns?.length || derived?.productAffinityPairs?.length || derived?.categoryPopularity?.length), [derived]);

    // -----------------------------------------------------------------------
    // Render
    // -----------------------------------------------------------------------

    if (fetchError && !loading) {
        return (
            <div className="p-6 min-h-[calc(100vh-120px)]">
                <Toast ref={toastRef} />
                <div className="flex items-center justify-center min-h-[50vh]">
                    <div className="text-center">
                        <i className="pi pi-exclamation-circle text-5xl text-red-400 mb-3 block" />
                        <p className="text-gray-700 font-medium text-lg">Something went wrong</p>
                        <p className="text-gray-500 text-sm mt-1">Unable to load analytics data. Please try again later.</p>
                    </div>
                </div>
            </div>
        );
    }

    if (loading && pipelineStatus !== 'loading') {
        return (
            <div className="flex flex-col items-center justify-center min-h-[400px] gap-4">
                <ProgressSpinner />
                <p className="text-gray-500 text-base">Loading product engagement…</p>
            </div>
        );
    }

    if (!hasData && !loading && pipelineStatus !== 'loading') {
        return (
            <div className="p-6 min-h-[calc(100vh-120px)]">
                <Toast ref={toastRef} />
                <DateFilterBar quickFilter={quickFilter} dateRange={dateRange} isFiltered={isFiltered} onQuickFilter={applyQuickFilter} onDateChange={setDateRange} onReset={resetFilters} dataMode={dataMode} />
                <div className="flex items-center justify-center min-h-[50vh]">
                    <p className="text-gray-500 text-lg">
                        {isFiltered
                            ? 'No data found for the selected date range.'
                            : 'No data to display.'}
                    </p>
                </div>
            </div>
        );
    }
    if (!derived) return null;
    const rec = derived?.recoCoverage ?? {};

    return (
        <div className="p-6 bg-gray-50 min-h-[calc(100vh-120px)]">
            <Toast ref={toastRef} />

            <DateFilterBar quickFilter={quickFilter} dateRange={dateRange} isFiltered={isFiltered} onQuickFilter={applyQuickFilter} onDateChange={setDateRange} onReset={resetFilters} dataMode={dataMode} hidden={loading && pipelineStatus === 'loading'} />

            <p className="mb-6 text-xs text-gray-400 italic">
                * All product engagement, affinity, and recommendation metrics are static aggregates over all-time records and are not filtered by the date picker.
            </p>

            {/* KPI Cards */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-8">
                {derived?.topPopCat?.category && (
                    <KPICard icon="pi-star" iconBg="bg-yellow-50" iconColor="text-yellow-500" value={derived.topPopCat.category} label="Most Popular Category *" />
                )}
                {(derived?.topViewCat?.category) && (
                    <KPICard icon="pi-eye" iconBg="bg-green-50" iconColor="text-green-500" value={derived.topViewCat.category} label="Best View-to-Purchase Category *" />
                )}
                {(derived?.totalRevFromViews ?? 0) > 0 && (
                    <KPICard icon="pi-dollar" iconBg="bg-blue-50" iconColor="text-blue-500" value={new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', notation: 'compact', maximumFractionDigits: 1 }).format(derived.totalRevFromViews)} label="Total Category Revenue *" />
                )}
                {(derived?.productAffinityPairs?.length ?? 0) > 0 && (
                    <KPICard icon="pi-link" iconBg="bg-purple-50" iconColor="text-purple-500" value={fmt.number(derived.productAffinityPairs.length)} label="Product Affinity Pairs *" />
                )}
                {(derived?.categoryAffinityPairs?.length ?? 0) > 0 && (
                    <KPICard icon="pi-sitemap" iconBg="bg-cyan-50" iconColor="text-cyan-500" value={fmt.number(derived.categoryAffinityPairs.length)} label="Category Affinity Pairs *" />
                )}
                {(rec.coverage_rate ?? 0) > 0 && (
                    <KPICard icon="pi-check-circle" iconBg="bg-emerald-50" iconColor="text-emerald-500" value={fmt.pct((rec.coverage_rate ?? 0) * 100)} label="Recommendation Coverage *" />
                )}
            </div>

            {/* View Patterns + Popularity */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
                {(derived?.categoryViewPatterns?.length ?? 0) > 0 && (
                    <div className="col-span-1 lg:col-span-2">
                        <ChartWrapper title="Category View-to-Purchase Rate &amp; Revenue per View *" showUpdateBadge={false}>
                            <div className="h-[300px]">
                                <Bar data={viewPatternData} options={{ responsive: true, maintainAspectRatio: false, plugins: { legend: { display: true, position: 'top' } }, scales: { y: { beginAtZero: true } } }} />
                            </div>
                        </ChartWrapper>
                    </div>
                )}

                {(derived?.categoryPopularity?.length ?? 0) > 0 && (
                    <ChartWrapper title="Category Popularity Score *" showUpdateBadge={false}>
                        <div className="h-[300px]">
                            <Bar data={popScoreData} options={{ responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true } } }} />
                        </div>
                    </ChartWrapper>
                )}

                {(derived?.categoryViewPatterns?.length ?? 0) > 0 && (
                    <ChartWrapper title="Units Sold by Category *" showUpdateBadge={false}>
                        <div className="h-[300px]">
                            <Doughnut data={catUnitsDoughnutData} options={{ responsive: true, maintainAspectRatio: false, plugins: { legend: { display: true, position: 'right' } } }} />
                        </div>
                    </ChartWrapper>
                )}
            </div>

            {/* Wishlist + Cart + Affinity */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
                {(derived?.categoryPopularity?.length ?? 0) > 0 && (
                    <div className="col-span-1 lg:col-span-2">
                        <ChartWrapper title="Wishlist vs Cart Adds by Category *" showUpdateBadge={false}>
                            <div className="h-[280px]">
                                <Bar data={wishCartData} options={{ responsive: true, maintainAspectRatio: false, plugins: { legend: { display: true, position: 'top' } }, scales: { y: { beginAtZero: true } } }} />
                            </div>
                        </ChartWrapper>
                    </div>
                )}

                {(derived?.categoryAffinityPairs?.length ?? 0) > 0 && (
                    <div className="col-span-1 lg:col-span-2">
                        <ChartWrapper title="Top Category Affinity Pairs by Avg Lift *" showUpdateBadge={false}>
                            <div className="h-[280px]">
                                <Bar data={catAffinityData} options={{ responsive: true, maintainAspectRatio: false, indexAxis: 'y', plugins: { legend: { display: false } }, scales: { x: { beginAtZero: true } } }} />
                            </div>
                        </ChartWrapper>
                    </div>
                )}
            </div>

            {/* Category View Patterns Metrics */}
            {(derived?.categoryViewPatterns?.length ?? 0) > 0 && (
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-8">
                    {[...derived.categoryViewPatterns].sort((a, b) => (b.total_revenue ?? 0) - (a.total_revenue ?? 0)).slice(0, 6).map((row) => (
                        <MetricsCard
                            key={row.category}
                            title={`${row.category ?? 'Unknown'} *`}
                            rows={[
                                { label: 'Products',          value: fmt.number(row.products_in_category),        show: (row.products_in_category ?? 0) > 0 },
                                { label: 'Units Sold',        value: fmt.number(row.total_units_sold),            show: (row.total_units_sold ?? 0) > 0 },
                                { label: 'Avg View→Purchase', value: fmt.pct((row.avg_view_to_purchase_rate ?? 0) * 100), show: true },
                                { label: 'Avg Rev / View',    value: fmt.currency(row.avg_revenue_per_view),       show: (row.avg_revenue_per_view ?? 0) > 0 },
                                { label: 'Total Revenue',     value: fmt.currency(row.total_revenue),              show: (row.total_revenue ?? 0) > 0 },
                            ]}
                        />
                    ))}
                </div>
            )}

            {/* Recommendation Coverage */}
            {(rec.total_products ?? 0) > 0 && (
                <div className="mb-8">
                    <CoverageCard total={rec.total_products} withRecs={rec.products_with_recommendations} rate={rec.coverage_rate} />
                </div>
            )}

            {/* DataTable — Category Affinity Top Per Category */}
            {(derived?.categoryAffinityTopPerCat?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">Category Affinity — Top Pair per Category *</h3>
                        <DataTable value={[...derived.categoryAffinityTopPerCat].sort((a, b) => (b.avg_lift ?? 0) - (a.avg_lift ?? 0))} paginator rows={10} stripedRows size="small">
                            <Column field="base_category" header="Category" sortable />
                            <Column field="affinity_category" header="Top Affinity Category" sortable />
                            <Column field="pair_count" header="Pair Count" sortable body={(r) => fmt.number(r.pair_count)} />
                            <Column field="total_co_occurrences" header="Co-occurrences" sortable body={(r) => fmt.number(r.total_co_occurrences)} />
                            <Column field="avg_lift" header="Avg Lift" sortable body={(r) => fmt.decimal(r.avg_lift, 3)} />
                            <Column field="avg_support" header="Avg Support" sortable body={(r) => fmt.decimal(r.avg_support, 4)} />
                        </DataTable>
                    </div>
                </Card>
            )}

            {/* DataTable — Product Affinity Top Per Product */}
            {(derived?.productAffinityTopPerProd?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">Product Affinity — Top Recommendation per Product *</h3>
                        <DataTable value={[...derived.productAffinityTopPerProd].sort((a, b) => (b.affinity_score ?? 0) - (a.affinity_score ?? 0))} paginator rows={10} stripedRows size="small">
                            <Column field="product_a_name" header="Product" sortable />
                            <Column field="product_a_category" header="Category" sortable />
                            <Column field="recommended_product_name" header="Recommended" sortable />
                            <Column field="recommended_product_category" header="Rec. Category" sortable />
                            <Column field="co_occurrence_count" header="Co-occurrences" sortable body={(r) => fmt.number(r.co_occurrence_count)} />
                            <Column field="confidence_a_to_b" header="Confidence" sortable body={(r) => fmt.pct((r.confidence_a_to_b ?? 0) * 100)} />
                            <Column field="lift_a_to_b" header="Lift" sortable body={(r) => fmt.decimal(r.lift_a_to_b, 3)} />
                            <Column field="affinity_strength" header="Strength" sortable body={(r) => <Tag value={r.affinity_strength} severity={affinitySeverity(r.affinity_strength)} />} />
                            <Column field="affinity_score" header="Score" sortable body={(r) => fmt.decimal(r.affinity_score, 3)} />
                        </DataTable>
                    </div>
                </Card>
            )}

            {/* DataTable — Top Product Affinity Pairs */}
            {(derived?.productAffinityPairs?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">Top Product Affinity Pairs *</h3>
                        <DataTable value={[...derived.productAffinityPairs].sort((a, b) => (b.affinity_score ?? 0) - (a.affinity_score ?? 0))} paginator rows={10} stripedRows size="small">
                            <Column field="product_a_name" header="Product A" sortable />
                            <Column field="product_a_category" header="Category A" sortable />
                            <Column field="product_b_name" header="Product B" sortable />
                            <Column field="product_b_category" header="Category B" sortable />
                            <Column field="co_occurrence_count" header="Co-occurrences" sortable body={(r) => fmt.number(r.co_occurrence_count)} />
                            <Column field="avg_lift" header="Avg Lift" sortable body={(r) => fmt.decimal(r.avg_lift, 3)} />
                            <Column field="affinity_strength" header="Strength" sortable body={(r) => <Tag value={r.affinity_strength} severity={affinitySeverity(r.affinity_strength)} />} />
                            <Column field="affinity_score" header="Score" sortable body={(r) => fmt.decimal(r.affinity_score, 3)} />
                        </DataTable>
                    </div>
                </Card>
            )}

            {/* DataTable — Top View-to-Purchase */}
            {(derived?.topVtoP?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">Top View-to-Purchase Products *</h3>
                        <DataTable value={[...derived.topVtoP].sort((a, b) => (b.view_to_purchase_rate ?? 0) - (a.view_to_purchase_rate ?? 0))} paginator rows={10} stripedRows size="small">
                            <Column field="product_name" header="Product" sortable />
                            <Column field="category" header="Category" sortable />
                            <Column field="view_to_purchase_rate" header="View→Purchase Rate" sortable body={(r) => fmt.pct((r.view_to_purchase_rate ?? 0) * 100)} />
                            <Column field="revenue_per_view" header="Revenue / View" sortable body={(r) => fmt.currency(r.revenue_per_view)} />
                            <Column field="total_units_sold" header="Units Sold" sortable body={(r) => fmt.number(r.total_units_sold)} />
                            <Column field="total_orders" header="Orders" sortable body={(r) => fmt.number(r.total_orders)} />
                        </DataTable>
                    </div>
                </Card>
            )}

            {/* DataTable — Supplier Product Performance */}
            {(derived?.supplierProductPerf?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">Supplier Product Performance *</h3>
                        <DataTable value={[...derived.supplierProductPerf].sort((a, b) => (b.supplier_performance_score ?? 0) - (a.supplier_performance_score ?? 0))} paginator rows={10} stripedRows size="small">
                            <Column field="product_name" header="Product" sortable />
                            <Column field="category" header="Category" sortable />
                            <Column field="supplier_id" header="Supplier ID" sortable />
                            <Column field="total_units_sold" header="Units Sold" sortable body={(r) => fmt.number(r.total_units_sold)} />
                            <Column field="total_revenue" header="Revenue" sortable body={(r) => fmt.currency(r.total_revenue)} />
                            <Column field="profit_margin" header="Margin" sortable body={(r) => fmt.pct(r.profit_margin)} />
                            <Column field="supplier_performance_score" header="Perf. Score" sortable body={(r) => fmt.decimal(r.supplier_performance_score, 2)} />
                            <Column field="supplier_reliability_score" header="Reliability" sortable body={(r) => fmt.decimal(r.supplier_reliability_score, 2)} />
                        </DataTable>
                    </div>
                </Card>
            )}

            {/* DataTable — Stockout Rate by Product */}
            {(derived?.stockoutRateByProduct?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">Stockout Rate by Product *</h3>
                        <DataTable value={[...derived.stockoutRateByProduct].sort((a, b) => (b.stockout_frequency ?? 0) - (a.stockout_frequency ?? 0))} paginator rows={10} stripedRows size="small">
                            <Column field="product_name" header="Product" sortable />
                            <Column field="category" header="Category" sortable />
                            <Column field="supplier_id" header="Supplier ID" sortable />
                            <Column field="stockout_frequency" header="Stockout Freq." sortable body={(r) => fmt.number(r.stockout_frequency)} />
                            <Column field="reorder_point_breach_count" header="Reorder Breaches" sortable body={(r) => fmt.number(r.reorder_point_breach_count)} />
                            <Column field="current_stock" header="Current Stock" sortable body={(r) => fmt.number(r.current_stock)} />
                            <Column field="days_of_supply" header="Days of Supply" sortable body={(r) => fmt.decimal(r.days_of_supply, 1)} />
                            <Column field="stock_status" header="Stock Status" sortable />
                        </DataTable>
                    </div>
                </Card>
            )}

            {/* DataTable — Supplier Stockout Impact on Products */}
            {(derived?.supplierStockoutImpact?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">Supplier Stockout Impact on Products *</h3>
                        <DataTable value={[...derived.supplierStockoutImpact].sort((a, b) => (b.sup_stockout_rate ?? 0) - (a.sup_stockout_rate ?? 0))} paginator rows={10} stripedRows size="small">
                            <Column field="product_name" header="Product" sortable />
                            <Column field="category" header="Category" sortable />
                            <Column field="supplier_id" header="Supplier ID" sortable />
                            <Column field="total_revenue" header="Revenue" sortable body={(r) => fmt.currency(r.total_revenue)} />
                            <Column field="sup_total_stockouts" header="Supplier Stockouts" sortable body={(r) => fmt.number(r.sup_total_stockouts)} />
                            <Column field="sup_stockout_rate" header="Stockout Rate" sortable body={(r) => fmt.pct(r.sup_stockout_rate)} />
                            <Column field="supplier_performance_score" header="Perf. Score" sortable body={(r) => fmt.decimal(r.supplier_performance_score, 2)} />
                            <Column field="supplier_reliability_score" header="Reliability" sortable body={(r) => fmt.decimal(r.supplier_reliability_score, 2)} />
                        </DataTable>
                    </div>
                </Card>
            )}
        </div>
    );
};

export default ProductEngagement;
