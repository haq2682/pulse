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
import { useFormatters } from '@/hooks/useFormatters';
import useAnalyticsDateFilter from '@/hooks/useAnalyticsDateFilter';
import DateFilterBar from '../components/DateFilterBar';

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, BarElement, ArcElement, Title, Tooltip, Legend);

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PALETTE = [
    'rgba(59,130,246,0.82)',
    'rgba(34,197,94,0.82)',
    'rgba(249,115,22,0.82)',
    'rgba(239,68,68,0.82)',
    'rgba(139,92,246,0.82)',
    'rgba(6,182,212,0.82)',
    'rgba(234,179,8,0.82)',
    'rgba(236,72,153,0.82)',
    'rgba(20,184,166,0.82)',
    'rgba(168,85,247,0.82)',
];

const LIFECYCLE_COLORS = {
    'Growth':    'rgba(34,197,94,0.85)',
    'Maturity':  'rgba(59,130,246,0.85)',
    'Decline':   'rgba(239,68,68,0.85)',
    'Launch':    'rgba(249,115,22,0.85)',
    'Emerging':  'rgba(6,182,212,0.85)',
    'Stable':    'rgba(139,92,246,0.85)',
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

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

const ProductPerformance = () => {
    const { businessId } = useParams();
    const toastRef = useRef(null);
    const { pipelineStatus } = usePipelineProgress();
    const fmt = useFormatters();

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
    // Derived data — all static aggregates in product_analytics performance view
    // -----------------------------------------------------------------------

    const derived = useMemo(() => {
        if (!rawData) return null;
        const a = rawData.analytics ?? {};

        const bestSelling       = a.best_selling_products?.data ?? [];
        const perfScore         = a.product_performance_score?.data ?? [];
        const ratingSummary     = a.product_rating_summary?.data ?? [];
        const topVtoP           = a.top_view_to_purchase_products?.data ?? [];
        const lowConversion     = a.low_conversion_products?.data ?? [];
        const outOfStock        = a.out_of_stock_products?.data ?? [];
        const lifecycleSummary  = a.product_lifecycle_summary?.data ?? [];
        const lifecycleSegments = a.product_lifecycle_segments?.data ?? [];
        const categoryRevShare  = a.category_revenue_share?.data ?? [];

        // KPIs
        const totalRevenue   = bestSelling.reduce((s, r) => s + (r.total_revenue ?? 0), 0);
        const totalUnitsSold = bestSelling.reduce((s, r) => s + (r.total_units_sold ?? 0), 0);
        const avgRating      = perfScore.length > 0
            ? perfScore.reduce((s, r) => s + (r.avg_rating ?? 0), 0) / perfScore.length
            : 0;

        // Enrich rating summary with product names from perfScore (join on product_id)
        const perfById = Object.fromEntries(perfScore.map((r) => [r.product_id, r]));
        const ratingEnriched = ratingSummary
            .map((r) => ({ ...r, product_name: perfById[r.product_id]?.product_name ?? `#${r.product_id}`, category: perfById[r.product_id]?.category ?? '' }))
            .sort((a, b) => (b.avg_rating ?? 0) - (a.avg_rating ?? 0));

        return {
            bestSelling, perfScore, ratingEnriched, topVtoP, lowConversion,
            outOfStock, lifecycleSummary, lifecycleSegments, categoryRevShare,
            totalRevenue, totalUnitsSold, avgRating,
        };
    }, [rawData]);

    // -----------------------------------------------------------------------
    // Chart data
    // -----------------------------------------------------------------------

    // Top 10 Best Selling — horizontal bar (units sold)
    const bestSellingData = useMemo(() => {
        const top10 = [...(derived?.bestSelling ?? [])].sort((a, b) => (b.total_units_sold ?? 0) - (a.total_units_sold ?? 0)).slice(0, 10);
        return {
            labels: top10.map((r) => r.product_name ?? ''),
            datasets: [
                { label: 'Units Sold', data: top10.map((r) => r.total_units_sold ?? 0), backgroundColor: 'rgba(59,130,246,0.8)' },
                { label: 'Orders', data: top10.map((r) => r.total_orders ?? 0), backgroundColor: 'rgba(34,197,94,0.8)' },
            ],
        };
    }, [derived]);

    // Category Revenue Share — doughnut
    const catRevShareData = useMemo(() => {
        const rows = [...(derived?.categoryRevShare ?? [])].sort((a, b) => (b.category_revenue ?? 0) - (a.category_revenue ?? 0));
        return {
            labels: rows.map((r) => r.category ?? ''),
            datasets: [{ data: rows.map((r) => r.category_revenue ?? 0), backgroundColor: PALETTE }],
        };
    }, [derived]);

    // Top View-to-Purchase — bar
    const vtoPData = useMemo(() => {
        const top10 = [...(derived?.topVtoP ?? [])].sort((a, b) => (b.view_to_purchase_rate ?? 0) - (a.view_to_purchase_rate ?? 0)).slice(0, 10);
        return {
            labels: top10.map((r) => r.product_name ?? ''),
            datasets: [{ label: 'View-to-Purchase Rate (%)', data: top10.map((r) => ((r.view_to_purchase_rate ?? 0) * 100).toFixed(2)), backgroundColor: 'rgba(34,197,94,0.8)' }],
        };
    }, [derived]);

    // Product Lifecycle Summary — doughnut
    const lifecycleData = useMemo(() => {
        const rows = derived?.lifecycleSummary ?? [];
        return {
            labels: rows.map((r) => r.lifecycle_stage ?? ''),
            datasets: [{ data: rows.map((r) => r.products_count ?? 0), backgroundColor: rows.map((r) => LIFECYCLE_COLORS[r.lifecycle_stage] ?? 'rgba(107,114,128,0.8)') }],
        };
    }, [derived]);

    // Top Performance Score — bar (top 10 by score)
    const perfScoreData = useMemo(() => {
        const top10 = [...(derived?.perfScore ?? [])].sort((a, b) => (b.product_performance_score_computed ?? 0) - (a.product_performance_score_computed ?? 0)).slice(0, 10);
        return {
            labels: top10.map((r) => r.product_name ?? ''),
            datasets: [
                { label: 'Performance Score', data: top10.map((r) => (r.product_performance_score_computed ?? 0).toFixed(2)), backgroundColor: 'rgba(139,92,246,0.8)' },
                { label: 'Avg Rating', data: top10.map((r) => r.avg_rating ?? 0), backgroundColor: 'rgba(234,179,8,0.8)' },
            ],
        };
    }, [derived]);

    // Top Rated Products — bar (top 10 by avg_rating)
    const ratingData = useMemo(() => {
        const top10 = (derived?.ratingEnriched ?? []).slice(0, 10);
        return {
            labels: top10.map((r) => r.product_name ?? ''),
            datasets: [{ label: 'Avg Rating', data: top10.map((r) => r.avg_rating ?? 0), backgroundColor: 'rgba(234,179,8,0.8)' }],
        };
    }, [derived]);

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    const lifecycleSeverity = (stage) => {
        if (!stage) return 'secondary';
        const s = stage.toLowerCase();
        if (s === 'growth' || s === 'emerging') return 'success';
        if (s === 'decline') return 'danger';
        if (s === 'launch') return 'warning';
        return 'info';
    };

    const hasData = useMemo(() => !!(derived?.bestSelling?.length || derived?.perfScore?.length || derived?.categoryRevShare?.length), [derived]);

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
                <p className="text-gray-500 text-base">Loading product performance…</p>
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
    return (
        <div className="p-6 bg-gray-50 min-h-[calc(100vh-120px)]">
            <Toast ref={toastRef} />

            <DateFilterBar quickFilter={quickFilter} dateRange={dateRange} isFiltered={isFiltered} onQuickFilter={applyQuickFilter} onDateChange={setDateRange} onReset={resetFilters} dataMode={dataMode} hidden={loading && pipelineStatus === 'loading'} />

            <p className="mb-6 text-xs text-gray-400 italic">
                * All product performance metrics are static aggregates over all-time records and are not filtered by the date picker. Use the Trends page for time-series analysis.
            </p>

            {/* KPI Cards */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-8">
                {(derived?.totalRevenue ?? 0) > 0 && (
                    <KPICard icon="pi-dollar" iconBg="bg-blue-50" iconColor="text-blue-500" value={fmt.currency(derived.totalRevenue)} label="Total Product Revenue *" />
                )}
                {(derived?.totalUnitsSold ?? 0) > 0 && (
                    <KPICard icon="pi-box" iconBg="bg-green-50" iconColor="text-green-500" value={fmt.number(derived.totalUnitsSold)} label="Total Units Sold *" />
                )}
                {(derived?.bestSelling?.length ?? 0) > 0 && (
                    <KPICard icon="pi-th-large" iconBg="bg-purple-50" iconColor="text-purple-500" value={fmt.number(derived.bestSelling.length)} label="Products Tracked *" />
                )}
                {(derived?.avgRating ?? 0) > 0 && (
                    <KPICard icon="pi-star" iconBg="bg-yellow-50" iconColor="text-yellow-500" value={fmt.decimal(derived.avgRating, 2)} label="Avg Product Rating *" />
                )}
                {(derived?.outOfStock?.length ?? 0) > 0 && (
                    <KPICard icon="pi-exclamation-triangle" iconBg="bg-red-50" iconColor="text-red-500" value={fmt.number(derived.outOfStock.length)} label="Out of Stock Products *" />
                )}
                {(derived?.categoryRevShare?.length ?? 0) > 0 && (
                    <KPICard icon="pi-chart-pie" iconBg="bg-cyan-50" iconColor="text-cyan-500" value={fmt.number(derived.categoryRevShare.length)} label="Active Categories *" />
                )}
            </div>

            {/* Charts Row 1 */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
                {(derived?.bestSelling?.length ?? 0) > 0 && (
                    <div className="col-span-1 lg:col-span-2">
                        <ChartWrapper title="Top 10 Best-Selling Products *" showUpdateBadge={false}>
                            <div className="h-[300px]">
                                <Bar data={bestSellingData} options={{ responsive: true, maintainAspectRatio: false, indexAxis: 'y', plugins: { legend: { display: true, position: 'top' } }, scales: { x: { beginAtZero: true } } }} />
                            </div>
                        </ChartWrapper>
                    </div>
                )}

                {(derived?.categoryRevShare?.length ?? 0) > 0 && (
                    <ChartWrapper title="Category Revenue Share *" showUpdateBadge={false}>
                        <div className="h-[300px]">
                            <Doughnut data={catRevShareData} options={{ responsive: true, maintainAspectRatio: false, plugins: { legend: { display: true, position: 'right' } } }} />
                        </div>
                    </ChartWrapper>
                )}

                {(derived?.lifecycleSummary?.length ?? 0) > 0 && (
                    <ChartWrapper title="Product Lifecycle Stage Distribution *" showUpdateBadge={false}>
                        <div className="h-[300px]">
                            <Doughnut data={lifecycleData} options={{ responsive: true, maintainAspectRatio: false, plugins: { legend: { display: true, position: 'right' } } }} />
                        </div>
                    </ChartWrapper>
                )}
            </div>

            {/* Charts Row 2 */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
                {(derived?.topVtoP?.length ?? 0) > 0 && (
                    <ChartWrapper title="Top View-to-Purchase Products *" showUpdateBadge={false}>
                        <div className="h-[300px]">
                            <Bar data={vtoPData} options={{ responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true, ticks: { callback: (v) => v + '%' } } } }} />
                        </div>
                    </ChartWrapper>
                )}

                {(derived?.ratingEnriched?.length ?? 0) > 0 && (
                    <ChartWrapper title="Top Rated Products *" showUpdateBadge={false}>
                        <div className="h-[300px]">
                            <Bar data={ratingData} options={{ responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true, min: 0, max: 5 } } }} />
                        </div>
                    </ChartWrapper>
                )}

                {(derived?.perfScore?.length ?? 0) > 0 && (
                    <div className="col-span-1 lg:col-span-2">
                        <ChartWrapper title="Top 10 Products by Performance Score *" showUpdateBadge={false}>
                            <div className="h-[300px]">
                                <Bar data={perfScoreData} options={{ responsive: true, maintainAspectRatio: false, plugins: { legend: { display: true, position: 'top' } }, scales: { y: { beginAtZero: true } } }} />
                            </div>
                        </ChartWrapper>
                    </div>
                )}
            </div>

            {/* Lifecycle Summary Metrics */}
            {(derived?.lifecycleSummary?.length ?? 0) > 0 && (
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-8">
                    {derived.lifecycleSummary.map((row) => (
                        <MetricsCard
                            key={row.lifecycle_stage}
                            title={`${row.lifecycle_stage ?? 'Unknown'} Stage *`}
                            rows={[
                                { label: 'Products',          value: fmt.number(row.products_count),          show: (row.products_count ?? 0) > 0 },
                                { label: 'Avg Units Sold',    value: fmt.number(row.avg_units_sold),          show: (row.avg_units_sold ?? 0) > 0 },
                                { label: 'Avg Revenue',       value: fmt.currency(row.avg_revenue),           show: (row.avg_revenue ?? 0) > 0 },
                                { label: 'Avg Rating',        value: fmt.decimal(row.avg_rating, 2),          show: (row.avg_rating ?? 0) > 0 },
                                { label: 'Avg View→Purchase', value: fmt.pct((row.avg_view_to_purchase_rate ?? 0) * 100), show: (row.avg_view_to_purchase_rate ?? 0) > 0 },
                                { label: 'Avg Inv. Turnover', value: fmt.decimal(row.avg_inventory_turnover_rate, 2), show: (row.avg_inventory_turnover_rate ?? 0) > 0 },
                            ]}
                        />
                    ))}
                </div>
            )}

            {/* DataTable — Best Selling Products */}
            {(derived?.bestSelling?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">Best-Selling Products *</h3>
                        <DataTable value={[...derived.bestSelling].sort((a, b) => (b.total_units_sold ?? 0) - (a.total_units_sold ?? 0))} paginator rows={10} stripedRows size="small">
                            <Column field="product_name" header="Product" sortable />
                            <Column field="category" header="Category" sortable />
                            <Column field="brand" header="Brand" sortable />
                            <Column field="total_units_sold" header="Units Sold" sortable body={(r) => fmt.number(r.total_units_sold)} />
                            <Column field="total_orders" header="Orders" sortable body={(r) => fmt.number(r.total_orders)} />
                            <Column field="total_revenue" header="Revenue" sortable body={(r) => fmt.currency(r.total_revenue)} />
                        </DataTable>
                    </div>
                </Card>
            )}

            {/* DataTable — Product Performance Score */}
            {(derived?.perfScore?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">Product Performance Scores *</h3>
                        <DataTable value={[...derived.perfScore].sort((a, b) => (b.product_performance_score_computed ?? 0) - (a.product_performance_score_computed ?? 0))} paginator rows={10} stripedRows size="small">
                            <Column field="product_name" header="Product" sortable />
                            <Column field="category" header="Category" sortable />
                            <Column field="product_performance_score_computed" header="Score" sortable body={(r) => fmt.decimal(r.product_performance_score_computed, 2)} />
                            <Column field="avg_rating" header="Avg Rating" sortable body={(r) => fmt.decimal(r.avg_rating, 2)} />
                            <Column field="view_to_purchase_rate" header="View→Purchase" sortable body={(r) => fmt.pct((r.view_to_purchase_rate ?? 0) * 100)} />
                            <Column field="total_units_sold" header="Units Sold" sortable body={(r) => fmt.number(r.total_units_sold)} />
                            <Column field="total_revenue" header="Revenue" sortable body={(r) => fmt.currency(r.total_revenue)} />
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
                        </DataTable>
                    </div>
                </Card>
            )}

            {/* DataTable — Low Conversion Products */}
            {(derived?.lowConversion?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">
                            <span className="text-red-500 mr-2">⚠</span> Low Conversion Products *
                        </h3>
                        <DataTable value={[...derived.lowConversion].sort((a, b) => (a.view_to_purchase_rate ?? 0) - (b.view_to_purchase_rate ?? 0))} paginator rows={10} stripedRows size="small">
                            <Column field="product_name" header="Product" sortable />
                            <Column field="category" header="Category" sortable />
                            <Column field="view_to_purchase_rate" header="View→Purchase" sortable body={(r) => fmt.pct((r.view_to_purchase_rate ?? 0) * 100)} />
                            <Column field="cart_to_purchase_rate" header="Cart→Purchase" sortable body={(r) => fmt.pct((r.cart_to_purchase_rate ?? 0) * 100)} />
                            <Column field="total_wishlist_adds" header="Wishlist Adds" sortable body={(r) => fmt.number(r.total_wishlist_adds)} />
                            <Column field="total_cart_adds" header="Cart Adds" sortable body={(r) => fmt.number(r.total_cart_adds)} />
                            <Column field="total_revenue" header="Revenue" sortable body={(r) => fmt.currency(r.total_revenue)} />
                        </DataTable>
                    </div>
                </Card>
            )}

            {/* DataTable — Out of Stock */}
            {(derived?.outOfStock?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">
                            <span className="text-red-500 mr-2">🚫</span> Out of Stock Products *
                        </h3>
                        <DataTable value={[...derived.outOfStock].sort((a, b) => (b.total_revenue ?? 0) - (a.total_revenue ?? 0))} paginator rows={10} stripedRows size="small">
                            <Column field="product_name" header="Product" sortable />
                            <Column field="category" header="Category" sortable />
                            <Column field="brand" header="Brand" sortable />
                            <Column field="current_stock_level" header="Stock Level" sortable body={(r) => fmt.number(r.current_stock_level)} />
                            <Column field="total_units_sold" header="Units Sold" sortable body={(r) => fmt.number(r.total_units_sold)} />
                            <Column field="total_revenue" header="Revenue at Risk" sortable body={(r) => fmt.currency(r.total_revenue)} />
                        </DataTable>
                    </div>
                </Card>
            )}

            {/* DataTable — Product Lifecycle Segments */}
            {(derived?.lifecycleSegments?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">Product Lifecycle Details *</h3>
                        <DataTable value={derived.lifecycleSegments} paginator rows={10} stripedRows size="small">
                            <Column field="product_name" header="Product" sortable />
                            <Column field="category" header="Category" sortable />
                            <Column field="lifecycle_stage" header="Stage" sortable body={(r) => <Tag value={r.lifecycle_stage} severity={lifecycleSeverity(r.lifecycle_stage)} />} />
                            <Column field="days_since_launch" header="Days Since Launch" sortable body={(r) => fmt.number(r.days_since_launch)} />
                            <Column field="total_units_sold" header="Units Sold" sortable body={(r) => fmt.number(r.total_units_sold)} />
                            <Column field="total_revenue" header="Revenue" sortable body={(r) => fmt.currency(r.total_revenue)} />
                            <Column field="avg_rating" header="Avg Rating" sortable body={(r) => fmt.decimal(r.avg_rating, 2)} />
                            <Column field="inventory_turnover_rate" header="Inv. Turnover" sortable body={(r) => fmt.decimal(r.inventory_turnover_rate, 2)} />
                        </DataTable>
                    </div>
                </Card>
            )}
        </div>
    );
};

export default ProductPerformance;
