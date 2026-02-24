import React, { useState, useEffect, useRef, useMemo, useCallback } from 'react';
import { useParams } from 'react-router-dom';
import { Card } from 'primereact/card';
import { ProgressSpinner } from 'primereact/progressspinner';
import { Toast } from 'primereact/toast';
import { DataTable } from 'primereact/datatable';
import { Column } from 'primereact/column';
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

const barOpts = (horizontal = false) => ({
    indexAxis: horizontal ? 'y' : 'x',
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { display: false }, title: { display: false } },
    scales: {
        x: { grid: { color: 'rgba(0,0,0,0.05)' } },
        y: { grid: { color: 'rgba(0,0,0,0.05)' } },
    },
});

const groupedBarOpts = () => ({
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { position: 'top' }, title: { display: false } },
    scales: {
        x: { grid: { color: 'rgba(0,0,0,0.05)' } },
        y: { grid: { color: 'rgba(0,0,0,0.05)' } },
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

export default function FunnelWishlist() {
    const fmt = useFormatters();
    const { businessId } = useParams();
    const toastRef = useRef(null);
    const { pipelineStatus } = usePipelineProgress();
    const { dateRange, setDateRange, quickFilter, isFiltered, applyQuickFilter, resetFilters } = useAnalyticsDateFilter();
    const { lastUpdate } = useAnalyticsWebSocket(businessId);

    const [rawWishlist, setRawWishlist] = useState(null);
    const [loading, setLoading] = useState(true);
    const [fetchError, setFetchError] = useState(false);
    const [dataMode, setDataMode] = useState('unknown');

    // -------------------------------------------------------------------------
    // Fetch
    // -------------------------------------------------------------------------

    const buildUrl = useCallback(() => {
        const base = import.meta.env.VITE_API_URL || 'http://localhost:8000';
        const params = new URLSearchParams({ categories: 'wishlist_analytics' });
        return `${base}/analytics/data/${businessId}?${params.toString()}`;
    }, [businessId]);

    const fetchData = useCallback(async () => {
        if (!businessId) return;
        try {
            setLoading(true);
            setFetchError(false);
            const res = await fetch(buildUrl());
            if (!res.ok) {
                setRawWishlist(null);
                return;
            }
            const json = await res.json();
            if (json.mode) setDataMode(json.mode);
            setRawWishlist(json.categories?.wishlist_analytics ?? null);
        } catch {
            console.error('[FunnelWishlist] fetch error');
            setFetchError(true);
            setRawWishlist(null);
        } finally {
            setLoading(false);
        }
    }, [buildUrl, businessId]);

    useEffect(() => { fetchData(); }, [businessId]); // eslint-disable-line
    useEffect(() => {
        if (!lastUpdate) return;
        fetchData();
        toastRef.current?.show({ severity: 'info', summary: 'Data Updated', detail: 'Analytics pipeline completed — refreshing wishlist data.', life: 3000 });
    }, [lastUpdate]); // eslint-disable-line

    // -------------------------------------------------------------------------
    // Derived data
    // -------------------------------------------------------------------------

    const derived = useMemo(() => {
        if (!rawWishlist) return null;
        const a = rawWishlist.analytics ?? {};

        const summary           = a.wishlist_overall_summary?.data               ?? [];
        const byProduct         = a.wishlist_by_product?.data                  ?? [];
        const byCustomer        = a.wishlist_by_customer?.data                 ?? [];
        const ttpStats          = a.wishlist_time_to_purchase_stats?.data      ?? [];
        const ttpDist           = a.wishlist_time_to_purchase_distribution?.data ?? [];
        const abandonedItems    = a.abandoned_wishlist_items?.data             ?? [];
        const abandonByProduct  = a.abandoned_wishlist_by_product?.data        ?? [];
        const abandonByCustomer = a.abandoned_wishlist_by_customer?.data       ?? [];
        const addsByMonth       = a.wishlist_adds_by_month?.data               ?? [];

        if (summary.length === 0 && byProduct.length === 0) return null;

        // ---- KPIs (from summary row 0) --------------------------------------
        const s = summary[0] ?? {};
        const totalItems       = +(s.total_wishlist_items ?? 0);
        const customersUsing   = +(s.customers_using_wishlist ?? 0);
        const productsInList   = +(s.products_in_wishlist ?? 0);
        const convRate         = +(s.wishlist_conversion_rate ?? 0);

        // ---- Top products by wishlist adds (top 12) -------------------------
        const topAddsSorted = [...byProduct]
            .sort((a, b) => (+(b.wishlist_adds ?? 0)) - (+(a.wishlist_adds ?? 0)))
            .slice(0, 12);
        const topAddsBarData = {
            labels: topAddsSorted.map((r) => `Product ${r.product_id}`),
            datasets: [{ label: 'Wishlist Adds', data: topAddsSorted.map((r) => +(r.wishlist_adds ?? 0)), backgroundColor: 'rgba(59,130,246,0.82)' }],
        };

        // ---- Top products by purchases (top 12) -----------------------------
        const topPurchSorted = [...byProduct]
            .sort((a, b) => (+(b.wishlist_purchases ?? 0)) - (+(a.wishlist_purchases ?? 0)))
            .slice(0, 12);
        const topPurchBarData = {
            labels: topPurchSorted.map((r) => `Product ${r.product_id}`),
            datasets: [{ label: 'Wishlist Purchases', data: topPurchSorted.map((r) => +(r.wishlist_purchases ?? 0)), backgroundColor: 'rgba(34,197,94,0.82)' }],
        };

        // ---- Conversion rate by product (top 12 by rate) --------------------
        const convRateSorted = [...byProduct]
            .filter((r) => r.wishlist_conversion_rate != null)
            .sort((a, b) => (+(b.wishlist_conversion_rate ?? 0)) - (+(a.wishlist_conversion_rate ?? 0)))
            .slice(0, 12);
        const convRateBarData = {
            labels: convRateSorted.map((r) => `Product ${r.product_id}`),
            datasets: [{ label: 'Conversion Rate %', data: convRateSorted.map((r) => +(r.wishlist_conversion_rate ?? 0).toFixed(2)), backgroundColor: 'rgba(139,92,246,0.82)' }],
        };

        // ---- Adds vs Purchases grouped bar (top 10) -------------------------
        const top10 = [...byProduct]
            .sort((a, b) => (+(b.wishlist_adds ?? 0)) - (+(a.wishlist_adds ?? 0)))
            .slice(0, 10);
        const addVsPurchBarData = {
            labels: top10.map((r) => `Product ${r.product_id}`),
            datasets: [
                { label: 'Wishlist Adds',      data: top10.map((r) => +(r.wishlist_adds ?? 0)),      backgroundColor: 'rgba(59,130,246,0.75)' },
                { label: 'Wishlist Purchases', data: top10.map((r) => +(r.wishlist_purchases ?? 0)), backgroundColor: 'rgba(34,197,94,0.75)' },
            ],
        };

        // ---- Wishlist adds by month bar -------------------------------------
        const addsByMonthData = addsByMonth.length > 0 ? {
            labels: addsByMonth.map((r) => r.year_month ?? ''),
            datasets: [{ label: 'Wishlist Adds', data: addsByMonth.map((r) => +(r.wishlist_adds ?? 0)), backgroundColor: 'rgba(6,182,212,0.82)' }],
        } : null;

        // ---- Time-to-purchase distribution bar ------------------------------
        const ttpDistData = ttpDist.length > 0 ? {
            labels: ttpDist.map((r) => r.time_bucket ?? 'Unknown'),
            datasets: [{ label: 'Count', data: ttpDist.map((r) => +(r.count ?? 0)), backgroundColor: PALETTE }],
        } : null;

        // ---- Time-to-purchase distribution doughnut -------------------------
        const ttpDoughnutData = ttpDist.length > 0 ? {
            labels: ttpDist.map((r) => r.time_bucket ?? 'Unknown'),
            datasets: [{ data: ttpDist.map((r) => +(r.count ?? 0)), backgroundColor: PALETTE }],
        } : null;

        // ---- Abandoned by product (top 12) ----------------------------------
        const abandonProdSorted = [...abandonByProduct]
            .sort((a, b) => (+(b.abandoned_wishlist_count ?? 0)) - (+(a.abandoned_wishlist_count ?? 0)))
            .slice(0, 12);
        const abandonProdBarData = abandonProdSorted.length > 0 ? {
            labels: abandonProdSorted.map((r) => `Product ${r.product_id}`),
            datasets: [{ label: 'Abandoned Wishlist Count', data: abandonProdSorted.map((r) => +(r.abandoned_wishlist_count ?? 0)), backgroundColor: 'rgba(239,68,68,0.82)' }],
        } : null;

        return {
            kpis: { totalItems, customersUsing, productsInList, convRate },
            topAddsBarData, topPurchBarData, convRateBarData, addVsPurchBarData,
            addsByMonthData, ttpDistData, ttpDoughnutData, abandonProdBarData,
            byProduct, byCustomer, ttpStats, ttpDist, abandonByCustomer, abandonedItems,
        };
    }, [rawWishlist]);

    // -------------------------------------------------------------------------
    // Render
    // -------------------------------------------------------------------------

    const hasData = derived !== null;

    if (loading && pipelineStatus !== 'loading') {
        return (
            <div className="flex flex-col items-center justify-center min-h-[400px] gap-4">
                <ProgressSpinner />
                <p className="text-gray-500 text-base">Loading funnel wishlist data…</p>
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
                        <p className="text-gray-500 text-sm mt-1">Unable to load wishlist data. Please try again later.</p>
                    </div>
                </div>
            </div>
        );
    }

    if (!hasData && !loading && pipelineStatus !== 'loading') {
        return (
            <div className="p-6 space-y-4">
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
                            : 'No data to display.'}
                    </p>
                </div>
            </div>
        );
    }

    if (!derived) return null;
    const { kpis, topAddsBarData, topPurchBarData, convRateBarData, addVsPurchBarData,
            addsByMonthData, ttpDistData, ttpDoughnutData, abandonProdBarData,
            byProduct, ttpStats, ttpDist } = derived;

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
                    * Funnel analytics are static aggregates computed over all available data and do not change with the date filter.
                </p>
            )}

            {/* ── KPI Cards ──────────────────────────────────────────────── */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <KPICard icon="pi-heart"       iconBg="bg-pink-50"   iconColor="text-pink-600"   value={fmt.number(kpis.totalItems)}      label="Total Wishlist Items" />
                <KPICard icon="pi-users"       iconBg="bg-blue-50"   iconColor="text-blue-600"   value={fmt.number(kpis.customersUsing)}  label="Customers Using Wishlist" />
                <KPICard icon="pi-box"         iconBg="bg-purple-50" iconColor="text-purple-600" value={fmt.number(kpis.productsInList)}  label="Products in Wishlist" />
                <KPICard icon="pi-percentage"  iconBg="bg-green-50"  iconColor="text-green-600"  value={fmt.pct(kpis.convRate)}           label="Wishlist Conv. Rate" />
            </div>

            {/* ── Wishlist Activity ──────────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-pink-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Wishlist Activity</h2>
                </div>
                {addsByMonthData && (
                    <ChartWrapper title="Wishlist Adds by Month" height={340}>
                        <Bar data={addsByMonthData} options={barOpts()} />
                    </ChartWrapper>
                )}
            </section>

            {/* ── Product Performance ────────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-blue-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Product Performance</h2>
                </div>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {topAddsBarData.labels.length > 0 && (
                        <ChartWrapper title="Top Products by Wishlist Adds (Top 12)" height={340}>
                            <Bar data={topAddsBarData} options={barOpts(true)} />
                        </ChartWrapper>
                    )}
                    {topPurchBarData.labels.length > 0 && (
                        <ChartWrapper title="Top Products by Wishlist Purchases (Top 12)" height={340}>
                            <Bar data={topPurchBarData} options={barOpts(true)} />
                        </ChartWrapper>
                    )}
                    {convRateBarData.labels.length > 0 && (
                        <ChartWrapper title="Conversion Rate % by Product (Top 12)" height={340}>
                            <Bar data={convRateBarData} options={barOpts(true)} />
                        </ChartWrapper>
                    )}
                    {addVsPurchBarData.labels.length > 0 && (
                        <ChartWrapper title="Wishlist Adds vs Purchases (Top 10)" height={340}>
                            <Bar data={addVsPurchBarData} options={groupedBarOpts()} />
                        </ChartWrapper>
                    )}
                </div>

                {/* Abandoned products bar */}
                {abandonProdBarData && (
                    <ChartWrapper title="Most Abandoned Products (Top 12)" height={340}>
                        <Bar data={abandonProdBarData} options={barOpts(true)} />
                    </ChartWrapper>
                )}
            </section>

            {/* ── Time Analysis ──────────────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-green-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Time Analysis</h2>
                </div>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {ttpDistData && (
                        <ChartWrapper title="Time to Purchase Distribution" height={340}>
                            <Bar data={ttpDistData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                    {ttpDoughnutData && (
                        <ChartWrapper title="Time to Purchase Breakdown" height={280}>
                            <Doughnut data={ttpDoughnutData} options={doughnutOpts()} />
                        </ChartWrapper>
                    )}
                </div>

                {/* Time-to-purchase stats KPI row */}
                {ttpStats.length > 0 && (() => {
                    const t = ttpStats[0];
                    return (
                        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                            <KPICard icon="pi-database"   iconBg="bg-blue-50"   iconColor="text-blue-600"   value={fmt.number(t.records)}      label="Wishlist Purchase Records" />
                            <KPICard icon="pi-clock"      iconBg="bg-green-50"  iconColor="text-green-600"  value={fmt.decimal(t.avg_time)}    label="Avg Days to Purchase" />
                            <KPICard icon="pi-chart-line" iconBg="bg-orange-50" iconColor="text-orange-600" value={fmt.decimal(t.median_time)} label="Median Days to Purchase" />
                            <KPICard icon="pi-arrow-up"   iconBg="bg-purple-50" iconColor="text-purple-600" value={fmt.decimal(t.p90_time)}    label="P90 Days to Purchase" />
                        </div>
                    );
                })()}
            </section>

            {/* ── Performance Tables ─────────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-purple-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Performance Tables</h2>
                </div>

                {/* Time-to-purchase distribution table */}
                {ttpDist.length > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">Time-to-Purchase Distribution Detail</h3>
                            <DataTable value={ttpDist} scrollable stripedRows emptyMessage="No data" className="text-sm">
                                <Column field="time_bucket" header="Time Bucket"   sortable />
                                <Column field="count"       header="Count"         sortable body={(r) => fmt.number(r.count)} />
                                <Column field="share"       header="Share %"       sortable body={(r) => fmt.pct(r.share)} />
                            </DataTable>
                        </div>
                    </Card>
                )}

                {/* Wishlist by product table */}
                {byProduct.length > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">Wishlist Performance by Product</h3>
                            <DataTable value={byProduct} paginator rows={15} scrollable stripedRows emptyMessage="No data" className="text-sm">
                                <Column field="product_id"              header="Product ID"          sortable />
                                <Column field="wishlist_adds"           header="Wishlist Adds"       sortable body={(r) => fmt.number(r.wishlist_adds)} />
                                <Column field="wishlist_purchases"      header="Purchases"           sortable body={(r) => fmt.number(r.wishlist_purchases)} />
                                <Column field="wishlist_conversion_rate" header="Conv. Rate %"       sortable body={(r) => fmt.pct(r.wishlist_conversion_rate)} />
                            </DataTable>
                        </div>
                    </Card>
                )}

                {/* Wishlist by Customer */}
                {(derived?.byCustomer?.length ?? 0) > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">Wishlist Activity by Customer</h3>
                            <DataTable
                                value={[...derived.byCustomer].sort((a, b) => (+(b.wishlist_adds ?? 0)) - (+(a.wishlist_adds ?? 0)))}
                                paginator rows={15} stripedRows emptyMessage="No data" className="text-sm"
                            >
                                <Column field="customer_id"              header="Customer ID"       sortable />
                                <Column field="wishlist_adds"            header="Wishlist Adds"     sortable body={(r) => fmt.number(r.wishlist_adds)} />
                                <Column field="wishlist_purchases"       header="Purchases"         sortable body={(r) => fmt.number(r.wishlist_purchases)} />
                                <Column field="wishlist_conversion_rate" header="Conv. Rate %"      sortable body={(r) => fmt.pct(r.wishlist_conversion_rate)} />
                            </DataTable>
                        </div>
                    </Card>
                )}

                {/* Abandoned Wishlist by Customer */}
                {(derived?.abandonByCustomer?.length ?? 0) > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">Abandoned Wishlist by Customer</h3>
                            <DataTable
                                value={[...derived.abandonByCustomer].sort((a, b) => (+(b.abandoned_wishlist_count ?? 0)) - (+(a.abandoned_wishlist_count ?? 0)))}
                                paginator rows={15} stripedRows emptyMessage="No data" className="text-sm"
                            >
                                <Column field="customer_id"              header="Customer ID"          sortable />
                                <Column field="abandoned_wishlist_count" header="Abandoned Items"      sortable body={(r) => fmt.number(r.abandoned_wishlist_count)} />
                            </DataTable>
                        </div>
                    </Card>
                )}

                {/* Abandoned Wishlist Items Detail */}
                {(derived?.abandonedItems?.length ?? 0) > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">Abandoned Wishlist Items Detail</h3>
                            <DataTable
                                value={derived.abandonedItems}
                                paginator rows={15} stripedRows emptyMessage="No data" className="text-sm"
                            >
                                <Column field="wishlist_id"    header="Wishlist ID"    sortable />
                                <Column field="customer_id"    header="Customer ID"    sortable />
                                <Column field="product_id"     header="Product ID"     sortable />
                                <Column field="added_date"     header="Added Date"     sortable />
                                <Column field="purchased_date" header="Purchased Date" sortable body={(r) => r.purchased_date ?? '—'} />
                                <Column field="removed_date"   header="Removed Date"   sortable body={(r) => r.removed_date ?? '—'} />
                            </DataTable>
                        </div>
                    </Card>
                )}
            </section>
        </div>
    );
}
