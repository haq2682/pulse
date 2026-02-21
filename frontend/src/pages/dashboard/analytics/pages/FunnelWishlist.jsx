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
// Formatters
// ---------------------------------------------------------------------------

const fmt = {
    number:   (v) => new Intl.NumberFormat('en-US').format(Math.round(v ?? 0)),
    decimal:  (v, d = 2) => (+(v ?? 0)).toFixed(d),
    pct:      (v) => `${(+(v ?? 0)).toFixed(1)}%`,
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

const barOpts = (horizontal = false) => ({
    indexAxis: horizontal ? 'y' : 'x',
    responsive: true,
    plugins: { legend: { display: false }, title: { display: false } },
    scales: {
        x: { grid: { color: 'rgba(0,0,0,0.05)' } },
        y: { grid: { color: 'rgba(0,0,0,0.05)' } },
    },
});

const groupedBarOpts = () => ({
    responsive: true,
    plugins: { legend: { position: 'top' }, title: { display: false } },
    scales: {
        x: { grid: { color: 'rgba(0,0,0,0.05)' } },
        y: { grid: { color: 'rgba(0,0,0,0.05)' } },
    },
});

const doughnutOpts = () => ({
    responsive: true,
    plugins: { legend: { position: 'right' }, title: { display: false } },
});

// ---------------------------------------------------------------------------
// Main Component
// ---------------------------------------------------------------------------

export default function FunnelWishlist() {
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
                toastRef.current?.show({
                    severity: 'warn', summary: 'No Data',
                    detail: 'Analytics data not available. Run the analytics pipeline first.',
                    life: 5000,
                });
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
            toastRef.current?.show({ severity: 'error', summary: 'Error', detail: 'Unable to load wishlist data.', life: 5000 });
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

        const summary          = a.wishlist_overall_summary?.data          ?? [];
        const byProduct        = a.wishlist_by_product?.data               ?? [];
        const byCustomer       = a.wishlist_by_customer?.data              ?? [];
        const ttpStats         = a.wishlist_time_to_purchase_stats?.data   ?? [];
        const ttpDist          = a.wishlist_time_to_purchase_distribution?.data ?? [];
        const abandonedItems   = a.abandoned_wishlist_items?.data          ?? [];
        const abandonByProduct = a.abandoned_wishlist_by_product?.data     ?? [];
        const addsByMonth      = a.wishlist_adds_by_month?.data            ?? [];

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
            byProduct, ttpStats, ttpDist,
        };
    }, [rawWishlist]);

    // -------------------------------------------------------------------------
    // Render
    // -------------------------------------------------------------------------

    const hasData = derived !== null;

    if (loading && pipelineStatus !== 'running') {
        return (
            <div className="flex items-center justify-center min-h-[60vh]">
                <ProgressSpinner style={{ width: '48px', height: '48px' }} />
            </div>
        );
    }

    if (fetchError) {
        return (
            <div className="flex items-center justify-center min-h-[60vh]">
                <div className="text-center max-w-md">
                    <i className="pi pi-exclamation-triangle text-5xl text-red-400 mb-4" />
                    <p className="text-gray-600 text-lg font-medium">Something went wrong</p>
                    <p className="text-gray-400 text-sm mt-2">Please try refreshing the page.</p>
                </div>
            </div>
        );
    }

    if (!hasData) {
        return (
            <div className="p-6 space-y-4">
                <Toast ref={toastRef} />
                <DateFilterBar
                    quickFilter={quickFilter} dateRange={dateRange} isFiltered={isFiltered}
                    onQuickFilter={applyQuickFilter} onDateChange={setDateRange} onReset={resetFilters}
                    dataMode={dataMode}
                />
                <div className="flex items-center justify-center min-h-[50vh]">
                    <div className="text-center max-w-md">
                        <i className="pi pi-chart-bar text-5xl text-gray-300 mb-4" />
                        <p className="text-gray-500 text-lg font-medium">No data to display</p>
                        <p className="text-gray-400 text-sm mt-2">Run the analytics pipeline first.</p>
                    </div>
                </div>
            </div>
        );
    }

    const { kpis, topAddsBarData, topPurchBarData, convRateBarData, addVsPurchBarData,
            addsByMonthData, ttpDistData, ttpDoughnutData, abandonProdBarData,
            byProduct, ttpStats, ttpDist } = derived;

    return (
        <div className="p-6 space-y-8">
            <Toast ref={toastRef} />

            {/* Date Filter */}
            <DateFilterBar
                quickFilter={quickFilter} dateRange={dateRange} isFiltered={isFiltered}
                onQuickFilter={applyQuickFilter} onDateChange={setDateRange} onReset={resetFilters}
                dataMode={dataMode}
            />

            {/* KPIs */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <KPICard icon="pi-heart"       iconBg="bg-pink-50"   iconColor="text-pink-600"   value={fmt.number(kpis.totalItems)}      label="Total Wishlist Items" />
                <KPICard icon="pi-users"       iconBg="bg-blue-50"   iconColor="text-blue-600"   value={fmt.number(kpis.customersUsing)}  label="Customers Using Wishlist" />
                <KPICard icon="pi-box"         iconBg="bg-purple-50" iconColor="text-purple-600" value={fmt.number(kpis.productsInList)}  label="Products in Wishlist" />
                <KPICard icon="pi-percentage"  iconBg="bg-green-50"  iconColor="text-green-600"  value={fmt.pct(kpis.convRate)}           label="Wishlist Conv. Rate" />
            </div>

            {/* Adds by month */}
            {addsByMonthData && (
                <Card className="rounded-xl shadow-sm border border-gray-200">
                    <h3 className="text-sm font-semibold text-gray-700 mb-4">Wishlist Adds by Month</h3>
                    <ChartWrapper><Bar data={addsByMonthData} options={barOpts()} /></ChartWrapper>
                </Card>
            )}

            {/* Top adds + top purchases */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {topAddsBarData.labels.length > 0 && (
                    <Card className="rounded-xl shadow-sm border border-gray-200">
                        <h3 className="text-sm font-semibold text-gray-700 mb-4">Top Products by Wishlist Adds (Top 12)</h3>
                        <ChartWrapper><Bar data={topAddsBarData} options={barOpts(true)} /></ChartWrapper>
                    </Card>
                )}
                {topPurchBarData.labels.length > 0 && (
                    <Card className="rounded-xl shadow-sm border border-gray-200">
                        <h3 className="text-sm font-semibold text-gray-700 mb-4">Top Products by Wishlist Purchases (Top 12)</h3>
                        <ChartWrapper><Bar data={topPurchBarData} options={barOpts(true)} /></ChartWrapper>
                    </Card>
                )}
            </div>

            {/* Conversion rate + adds vs purchases grouped */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {convRateBarData.labels.length > 0 && (
                    <Card className="rounded-xl shadow-sm border border-gray-200">
                        <h3 className="text-sm font-semibold text-gray-700 mb-4">Conversion Rate % by Product (Top 12)</h3>
                        <ChartWrapper><Bar data={convRateBarData} options={barOpts(true)} /></ChartWrapper>
                    </Card>
                )}
                {addVsPurchBarData.labels.length > 0 && (
                    <Card className="rounded-xl shadow-sm border border-gray-200">
                        <h3 className="text-sm font-semibold text-gray-700 mb-4">Wishlist Adds vs Purchases (Top 10)</h3>
                        <ChartWrapper><Bar data={addVsPurchBarData} options={groupedBarOpts()} /></ChartWrapper>
                    </Card>
                )}
            </div>

            {/* Time-to-purchase distribution */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {ttpDistData && (
                    <Card className="rounded-xl shadow-sm border border-gray-200">
                        <h3 className="text-sm font-semibold text-gray-700 mb-4">Time to Purchase Distribution</h3>
                        <ChartWrapper><Bar data={ttpDistData} options={barOpts()} /></ChartWrapper>
                    </Card>
                )}
                {ttpDoughnutData && (
                    <Card className="rounded-xl shadow-sm border border-gray-200">
                        <h3 className="text-sm font-semibold text-gray-700 mb-4">Time to Purchase Breakdown</h3>
                        <ChartWrapper><Doughnut data={ttpDoughnutData} options={doughnutOpts()} /></ChartWrapper>
                    </Card>
                )}
            </div>

            {/* Abandoned products bar */}
            {abandonProdBarData && (
                <Card className="rounded-xl shadow-sm border border-gray-200">
                    <h3 className="text-sm font-semibold text-gray-700 mb-4">Most Abandoned Products (Top 12)</h3>
                    <ChartWrapper><Bar data={abandonProdBarData} options={barOpts(true)} /></ChartWrapper>
                </Card>
            )}

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

            {/* Time-to-purchase distribution table */}
            {ttpDist.length > 0 && (
                <Card className="rounded-xl shadow-sm border border-gray-200">
                    <h3 className="text-sm font-semibold text-gray-700 mb-4">Time-to-Purchase Distribution Detail</h3>
                    <DataTable value={ttpDist} scrollable stripedRows emptyMessage="No data" className="text-sm">
                        <Column field="time_bucket" header="Time Bucket"   sortable />
                        <Column field="count"       header="Count"         sortable body={(r) => fmt.number(r.count)} />
                        <Column field="share"       header="Share %"       sortable body={(r) => fmt.pct(r.share)} />
                    </DataTable>
                </Card>
            )}

            {/* Wishlist by product table */}
            {byProduct.length > 0 && (
                <Card className="rounded-xl shadow-sm border border-gray-200">
                    <h3 className="text-sm font-semibold text-gray-700 mb-4">Wishlist Performance by Product</h3>
                    <DataTable value={byProduct} paginator rows={15} scrollable stripedRows emptyMessage="No data" className="text-sm">
                        <Column field="product_id"              header="Product ID"          sortable />
                        <Column field="wishlist_adds"           header="Wishlist Adds"       sortable body={(r) => fmt.number(r.wishlist_adds)} />
                        <Column field="wishlist_purchases"      header="Purchases"           sortable body={(r) => fmt.number(r.wishlist_purchases)} />
                        <Column field="wishlist_conversion_rate" header="Conv. Rate %"       sortable body={(r) => fmt.pct(r.wishlist_conversion_rate)} />
                    </DataTable>
                </Card>
            )}
        </div>
    );
}
