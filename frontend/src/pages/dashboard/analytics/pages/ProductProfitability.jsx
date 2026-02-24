import React, { useState, useEffect, useRef, useMemo, useCallback } from 'react';
import { useParams } from 'react-router-dom';
import { Card } from 'primereact/card';
import { ProgressSpinner } from 'primereact/progressspinner';
import { Toast } from 'primereact/toast';
import { DataTable } from 'primereact/datatable';
import { Column } from 'primereact/column';
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

const ProductProfitability = () => {
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

        const highestMargin       = a.highest_margin_products?.data ?? [];
        const lowMarginHighTraffic = a.low_margin_high_traffic_products?.data ?? [];
        const categoryProfit      = a.category_profitability?.data ?? [];
        const lowPerfCategories   = a.low_performing_categories?.data ?? [];
        const marginErosion       = a.margin_erosion_risk?.data ?? [];
        const carryingCost        = a.inventory_carrying_cost_by_product?.data ?? [];
        const categoryRevShare    = a.category_revenue_share?.data ?? [];

        // KPIs
        const totalCategoryRevenue = categoryProfit.reduce((s, r) => s + (r.category_revenue ?? 0), 0);
        const totalCategoryProfit  = categoryProfit.reduce((s, r) => s + (r.category_profit ?? 0), 0);
        const bestMarginCat        = [...categoryProfit].sort((a, b) => (b.avg_profit_margin ?? 0) - (a.avg_profit_margin ?? 0))[0];
        const totalCarryingCost    = carryingCost.reduce((s, r) => s + (r.storage_cost ?? 0), 0);

        return {
            highestMargin, lowMarginHighTraffic, categoryProfit, lowPerfCategories,
            marginErosion, carryingCost, categoryRevShare,
            totalCategoryRevenue, totalCategoryProfit, bestMarginCat, totalCarryingCost,
        };
    }, [rawData]);

    // -----------------------------------------------------------------------
    // Chart data
    // -----------------------------------------------------------------------

    // Category Profitability — grouped bar (revenue + profit + margin)
    const catProfitBarData = useMemo(() => {
        const rows = [...(derived?.categoryProfit ?? [])].sort((a, b) => (b.category_revenue ?? 0) - (a.category_revenue ?? 0));
        return {
            labels: rows.map((r) => r.category ?? ''),
            datasets: [
                { label: 'Revenue',     data: rows.map((r) => r.category_revenue ?? 0), backgroundColor: 'rgba(59,130,246,0.8)' },
                { label: 'Profit',      data: rows.map((r) => r.category_profit ?? 0),  backgroundColor: 'rgba(34,197,94,0.8)' },
            ],
        };
    }, [derived]);

    // Category avg profit margin — bar
    const catMarginData = useMemo(() => {
        const rows = [...(derived?.categoryProfit ?? [])].sort((a, b) => (b.avg_profit_margin ?? 0) - (a.avg_profit_margin ?? 0));
        return {
            labels: rows.map((r) => r.category ?? ''),
            datasets: [{ label: 'Avg Profit Margin (%)', data: rows.map((r) => ((r.avg_profit_margin ?? 0) * 100).toFixed(2)), backgroundColor: PALETTE }],
        };
    }, [derived]);

    // Highest margin products — horizontal bar (top 10)
    const highMarginBarData = useMemo(() => {
        const top10 = [...(derived?.highestMargin ?? [])].sort((a, b) => (b.profit_margin ?? 0) - (a.profit_margin ?? 0)).slice(0, 10);
        return {
            labels: top10.map((r) => r.product_name ?? ''),
            datasets: [
                { label: 'Profit Margin (%)', data: top10.map((r) => ((r.profit_margin ?? 0) * 100).toFixed(2)), backgroundColor: 'rgba(34,197,94,0.8)' },
            ],
        };
    }, [derived]);

    // Category revenue share — doughnut
    const catRevShareData = useMemo(() => {
        const rows = [...(derived?.categoryRevShare ?? [])].sort((a, b) => (b.category_revenue ?? 0) - (a.category_revenue ?? 0));
        return {
            labels: rows.map((r) => r.category ?? ''),
            datasets: [{ data: rows.map((r) => r.category_revenue ?? 0), backgroundColor: PALETTE }],
        };
    }, [derived]);

    // Category profit share — doughnut
    const catProfitShareData = useMemo(() => {
        const rows = [...(derived?.categoryProfit ?? [])].sort((a, b) => (b.profit_share ?? 0) - (a.profit_share ?? 0));
        return {
            labels: rows.map((r) => r.category ?? ''),
            datasets: [{ data: rows.map((r) => r.category_profit ?? 0), backgroundColor: PALETTE }],
        };
    }, [derived]);

    // Top inventory carrying cost — horizontal bar (top 10)
    const carryingCostData = useMemo(() => {
        const top10 = [...(derived?.carryingCost ?? [])].sort((a, b) => (b.storage_cost ?? 0) - (a.storage_cost ?? 0)).slice(0, 10);
        return {
            labels: top10.map((r) => r.product_name ?? ''),
            datasets: [{ label: 'Storage Cost ($)', data: top10.map((r) => r.storage_cost ?? 0), backgroundColor: 'rgba(239,68,68,0.8)' }],
        };
    }, [derived]);

    // -----------------------------------------------------------------------
    // Visibility
    // -----------------------------------------------------------------------

    const hasData = useMemo(() => !!(derived?.categoryProfit?.length || derived?.highestMargin?.length || derived?.categoryRevShare?.length), [derived]);

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
                <p className="text-gray-500 text-base">Loading product profitability…</p>
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
                * All profitability metrics are static aggregates over all-time records and are not filtered by the date picker.
            </p>

            {/* KPI Cards */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-8">
                {(derived?.totalCategoryRevenue ?? 0) > 0 && (
                    <KPICard icon="pi-dollar" iconBg="bg-blue-50" iconColor="text-blue-500" value={fmt.currency(derived.totalCategoryRevenue)} label="Total Category Revenue *" />
                )}
                {(derived?.totalCategoryProfit ?? 0) > 0 && (
                    <KPICard icon="pi-chart-line" iconBg="bg-green-50" iconColor="text-green-500" value={fmt.currency(derived.totalCategoryProfit)} label="Total Category Profit *" />
                )}
                {derived?.bestMarginCat && (
                    <KPICard icon="pi-star" iconBg="bg-yellow-50" iconColor="text-yellow-500" value={derived.bestMarginCat.category ?? ''} label="Best Margin Category *" />
                )}
                {(derived?.bestMarginCat?.avg_profit_margin ?? 0) > 0 && (
                    <KPICard icon="pi-percentage" iconBg="bg-purple-50" iconColor="text-purple-500" value={fmt.pct((derived.bestMarginCat.avg_profit_margin ?? 0) * 100)} label="Best Category Margin % *" />
                )}
                {(derived?.marginErosion?.length ?? 0) > 0 && (
                    <KPICard icon="pi-exclamation-triangle" iconBg="bg-red-50" iconColor="text-red-500" value={fmt.number(derived.marginErosion.length)} label="Margin Erosion Risk Products *" />
                )}
                {(derived?.totalCarryingCost ?? 0) > 0 && (
                    <KPICard icon="pi-inbox" iconBg="bg-orange-50" iconColor="text-orange-500" value={fmt.currency(derived.totalCarryingCost)} label="Total Inventory Carrying Cost *" />
                )}
            </div>

            {/* Category Profitability Charts */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
                {(derived?.categoryProfit?.length ?? 0) > 0 && (
                    <div className="col-span-1 lg:col-span-2">
                        <ChartWrapper title="Category Revenue vs Profit *" showUpdateBadge={false}>
                            <div className="h-[300px]">
                                <Bar data={catProfitBarData} options={{ responsive: true, maintainAspectRatio: false, plugins: { legend: { display: true, position: 'top' } }, scales: { y: { beginAtZero: true, ticks: { callback: (v) => '$' + v.toLocaleString() } } } }} />
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

                {(derived?.categoryProfit?.length ?? 0) > 0 && (
                    <ChartWrapper title="Category Profit Share *" showUpdateBadge={false}>
                        <div className="h-[300px]">
                            <Doughnut data={catProfitShareData} options={{ responsive: true, maintainAspectRatio: false, plugins: { legend: { display: true, position: 'right' } } }} />
                        </div>
                    </ChartWrapper>
                )}

                {(derived?.categoryProfit?.length ?? 0) > 0 && (
                    <div className="col-span-1 lg:col-span-2">
                        <ChartWrapper title="Category Avg Profit Margin % *" showUpdateBadge={false}>
                            <div className="h-[280px]">
                                <Bar data={catMarginData} options={{ responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true, ticks: { callback: (v) => v + '%' } } } }} />
                            </div>
                        </ChartWrapper>
                    </div>
                )}
            </div>

            {/* Highest Margin + Carrying Cost */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
                {(derived?.highestMargin?.length ?? 0) > 0 && (
                    <ChartWrapper title="Top 10 Highest Margin Products *" showUpdateBadge={false}>
                        <div className="h-[300px]">
                            <Bar data={highMarginBarData} options={{ responsive: true, maintainAspectRatio: false, indexAxis: 'y', plugins: { legend: { display: false } }, scales: { x: { beginAtZero: true, ticks: { callback: (v) => v + '%' } } } }} />
                        </div>
                    </ChartWrapper>
                )}

                {(derived?.carryingCost?.length ?? 0) > 0 && (
                    <ChartWrapper title="Top 10 Products by Inventory Carrying Cost *" showUpdateBadge={false}>
                        <div className="h-[300px]">
                            <Bar data={carryingCostData} options={{ responsive: true, maintainAspectRatio: false, indexAxis: 'y', plugins: { legend: { display: false } }, scales: { x: { beginAtZero: true, ticks: { callback: (v) => '$' + v.toLocaleString() } } } }} />
                        </div>
                    </ChartWrapper>
                )}
            </div>

            {/* Category Profitability Summary MetricsCards */}
            {(derived?.categoryProfit?.length ?? 0) > 0 && (
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-8">
                    {[...derived.categoryProfit].sort((a, b) => (b.category_profit ?? 0) - (a.category_profit ?? 0)).slice(0, 6).map((row) => (
                        <MetricsCard
                            key={row.category}
                            title={`${row.category ?? 'Unknown'} *`}
                            rows={[
                                { label: 'Revenue',          value: fmt.currency(row.category_revenue),      show: (row.category_revenue ?? 0) > 0 },
                                { label: 'Profit',           value: fmt.currency(row.category_profit),       show: (row.category_profit ?? 0) > 0 },
                                { label: 'Profit Margin',    value: fmt.pct((row.avg_profit_margin ?? 0) * 100), show: true },
                                { label: 'Products',         value: fmt.number(row.products_in_category),    show: (row.products_in_category ?? 0) > 0 },
                                { label: 'Profit / Product', value: fmt.currency(row.profit_per_product),    show: (row.profit_per_product ?? 0) > 0 },
                                { label: 'Revenue / Product',value: fmt.currency(row.revenue_per_product),   show: (row.revenue_per_product ?? 0) > 0 },
                            ]}
                        />
                    ))}
                </div>
            )}

            {/* DataTable — Highest Margin Products */}
            {(derived?.highestMargin?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">Highest Margin Products *</h3>
                        <DataTable value={[...derived.highestMargin].sort((a, b) => (b.profit_margin ?? 0) - (a.profit_margin ?? 0))} paginator rows={10} stripedRows size="small">
                            <Column field="product_name" header="Product" sortable />
                            <Column field="category" header="Category" sortable />
                            <Column field="brand" header="Brand" sortable />
                            <Column field="profit_margin" header="Margin %" sortable body={(r) => fmt.pct((r.profit_margin ?? 0) * 100)} />
                            <Column field="total_revenue" header="Revenue" sortable body={(r) => fmt.currency(r.total_revenue)} />
                            <Column field="total_units_sold" header="Units Sold" sortable body={(r) => fmt.number(r.total_units_sold)} />
                        </DataTable>
                    </div>
                </Card>
            )}

            {/* DataTable — Low Margin High Traffic */}
            {(derived?.lowMarginHighTraffic?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">
                            <span className="text-orange-500 mr-2">⚠</span> Low Margin, High Traffic Products *
                        </h3>
                        <DataTable value={[...derived.lowMarginHighTraffic].sort((a, b) => (b.traffic_score ?? 0) - (a.traffic_score ?? 0))} paginator rows={10} stripedRows size="small">
                            <Column field="product_name" header="Product" sortable />
                            <Column field="category" header="Category" sortable />
                            <Column field="profit_margin" header="Margin %" sortable body={(r) => fmt.pct((r.profit_margin ?? 0) * 100)} />
                            <Column field="traffic_score" header="Traffic Score" sortable body={(r) => fmt.decimal(r.traffic_score, 2)} />
                            <Column field="view_to_purchase_rate" header="View→Purchase" sortable body={(r) => fmt.pct((r.view_to_purchase_rate ?? 0) * 100)} />
                            <Column field="total_revenue" header="Revenue" sortable body={(r) => fmt.currency(r.total_revenue)} />
                            <Column field="total_wishlist_adds" header="Wishlist Adds" sortable body={(r) => fmt.number(r.total_wishlist_adds)} />
                            <Column field="total_cart_adds" header="Cart Adds" sortable body={(r) => fmt.number(r.total_cart_adds)} />
                        </DataTable>
                    </div>
                </Card>
            )}

            {/* DataTable — Low Performing Categories */}
            {(derived?.lowPerfCategories?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">
                            <span className="text-red-500 mr-2">🔻</span> Low Performing Categories *
                        </h3>
                        <DataTable value={[...derived.lowPerfCategories].sort((a, b) => (a.total_revenue ?? 0) - (b.total_revenue ?? 0))} paginator rows={10} stripedRows size="small">
                            <Column field="category" header="Category" sortable />
                            <Column field="products_in_category" header="Products" sortable body={(r) => fmt.number(r.products_in_category)} />
                            <Column field="total_revenue" header="Revenue" sortable body={(r) => fmt.currency(r.total_revenue)} />
                            <Column field="avg_profit_margin" header="Avg Margin %" sortable body={(r) => fmt.pct((r.avg_profit_margin ?? 0) * 100)} />
                            <Column field="avg_view_to_purchase_rate" header="Avg View→Purchase" sortable body={(r) => fmt.pct((r.avg_view_to_purchase_rate ?? 0) * 100)} />
                            <Column field="revenue_share" header="Revenue Share %" sortable body={(r) => fmt.pct((r.revenue_share ?? 0) * 100)} />
                        </DataTable>
                    </div>
                </Card>
            )}

            {/* DataTable — Margin Erosion Risk */}
            {(derived?.marginErosion?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">
                            <span className="text-red-500 mr-2">📉</span> Margin Erosion Risk Products *
                        </h3>
                        <DataTable value={[...derived.marginErosion].sort((a, b) => (b.storage_cost_to_revenue ?? 0) - (a.storage_cost_to_revenue ?? 0))} paginator rows={10} stripedRows size="small">
                            <Column field="product_name" header="Product" sortable />
                            <Column field="category" header="Category" sortable />
                            <Column field="storage_cost" header="Storage Cost" sortable body={(r) => fmt.currency(r.storage_cost)} />
                            <Column field="storage_cost_to_revenue" header="Cost/Revenue Ratio" sortable body={(r) => fmt.pct((r.storage_cost_to_revenue ?? 0) * 100)} />
                            <Column field="storage_cost_per_unit" header="Storage Cost/Unit" sortable body={(r) => fmt.currency(r.storage_cost_per_unit)} />
                            <Column field="total_revenue" header="Revenue" sortable body={(r) => fmt.currency(r.total_revenue)} />
                            <Column field="available_stock" header="Available Stock" sortable body={(r) => fmt.number(r.available_stock)} />
                        </DataTable>
                    </div>
                </Card>
            )}

            {/* DataTable — Category Profitability Full */}
            {(derived?.categoryProfit?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">Full Category Profitability *</h3>
                        <DataTable value={[...derived.categoryProfit].sort((a, b) => (b.category_profit ?? 0) - (a.category_profit ?? 0))} paginator rows={10} stripedRows size="small">
                            <Column field="category" header="Category" sortable />
                            <Column field="products_in_category" header="Products" sortable body={(r) => fmt.number(r.products_in_category)} />
                            <Column field="category_revenue" header="Revenue" sortable body={(r) => fmt.currency(r.category_revenue)} />
                            <Column field="category_profit" header="Profit" sortable body={(r) => fmt.currency(r.category_profit)} />
                            <Column field="avg_profit_margin" header="Avg Margin %" sortable body={(r) => fmt.pct((r.avg_profit_margin ?? 0) * 100)} />
                            <Column field="profit_share" header="Profit Share %" sortable body={(r) => fmt.pct((r.profit_share ?? 0) * 100)} />
                            <Column field="revenue_per_product" header="Rev / Product" sortable body={(r) => fmt.currency(r.revenue_per_product)} />
                            <Column field="profit_per_product" header="Profit / Product" sortable body={(r) => fmt.currency(r.profit_per_product)} />
                        </DataTable>
                    </div>
                </Card>
            )}
        </div>
    );
};

export default ProductProfitability;
