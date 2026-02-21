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

// Risk score → colour helper
const riskColor = (score) => {
    const s = +(score ?? 0);
    if (s >= 0.7) return 'danger';
    if (s >= 0.4) return 'warning';
    return 'success';
};

// ---------------------------------------------------------------------------
// Formatters
// ---------------------------------------------------------------------------

const fmt = {
    number:   (v) => new Intl.NumberFormat('en-US').format(Math.round(v ?? 0)),
    currency: (v) => `$${new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(v ?? 0)}`,
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

export default function FunnelCart() {
    const { businessId } = useParams();
    const toastRef = useRef(null);
    const { pipelineStatus } = usePipelineProgress();
    const { dateRange, setDateRange, quickFilter, isFiltered, applyQuickFilter, resetFilters } = useAnalyticsDateFilter();
    const { lastUpdate } = useAnalyticsWebSocket(businessId);

    const [rawCart, setRawCart] = useState(null);
    const [loading, setLoading] = useState(true);
    const [fetchError, setFetchError] = useState(false);
    const [dataMode, setDataMode] = useState('unknown');

    // -------------------------------------------------------------------------
    // Fetch
    // -------------------------------------------------------------------------

    const buildUrl = useCallback(() => {
        const base = import.meta.env.VITE_API_URL || 'http://localhost:8000';
        const params = new URLSearchParams({ categories: 'cart_analytics' });
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
                setRawCart(null);
                return;
            }
            const json = await res.json();
            if (json.mode) setDataMode(json.mode);
            setRawCart(json.categories?.cart_analytics ?? null);
        } catch {
            console.error('[FunnelCart] fetch error');
            setFetchError(true);
            setRawCart(null);
            toastRef.current?.show({ severity: 'error', summary: 'Error', detail: 'Unable to load cart data.', life: 5000 });
        } finally {
            setLoading(false);
        }
    }, [buildUrl, businessId]);

    useEffect(() => { fetchData(); }, [businessId]); // eslint-disable-line
    useEffect(() => {
        if (!lastUpdate) return;
        fetchData();
        toastRef.current?.show({ severity: 'info', summary: 'Data Updated', detail: 'Analytics pipeline completed — refreshing cart data.', life: 3000 });
    }, [lastUpdate]); // eslint-disable-line

    // -------------------------------------------------------------------------
    // Derived data
    // -------------------------------------------------------------------------

    const derived = useMemo(() => {
        if (!rawCart) return null;
        const a = rawCart.analytics ?? {};

        const overallStats   = a.cart_overall_stats?.data            ?? [];
        const statusDist     = a.cart_status_distribution?.data      ?? [];
        const valueStats     = a.cart_value_stats?.data              ?? [];
        const highAbandoned  = a.high_value_abandoned_carts?.data    ?? [];
        const ttpOverall     = a.time_to_purchase_overall?.data      ?? [];
        const ttpByTier      = a.time_to_purchase_by_tier?.data      ?? [];
        const ttpBuckets     = a.time_to_purchase_buckets?.data      ?? [];

        if (statusDist.length === 0 && valueStats.length === 0) return null;

        // ---- KPIs -----------------------------------------------------------
        const totals          = overallStats[0] ?? {};
        const totalCarts      = +(totals.total_carts ?? 0);
        const totalCartLines  = +(totals.total_cart_lines ?? 0);
        const ttpRow          = ttpOverall[0] ?? {};
        const avgTimeInCart   = +(ttpRow.avg_time_in_cart_days ?? 0);
        const avgCartValue    = valueStats.length > 0
            ? valueStats.reduce((s, r) => s + (+(r.avg_cart_value ?? 0)), 0) / valueStats.length
            : 0;

        // ---- Cart status distribution doughnut ------------------------------
        const statusDoughnutData = {
            labels: statusDist.map((r) => r.cart_status ?? 'Unknown'),
            datasets: [{ data: statusDist.map((r) => +(r.carts_count ?? 0)), backgroundColor: PALETTE }],
        };

        // ---- Cart lines by status bar ---------------------------------------
        const cartLinesBarData = {
            labels: statusDist.map((r) => r.cart_status ?? 'Unknown'),
            datasets: [{ label: 'Cart Lines', data: statusDist.map((r) => +(r.cart_lines_count ?? 0)), backgroundColor: PALETTE }],
        };

        // ---- Avg cart value by status bar -----------------------------------
        const avgValByStatusData = {
            labels: valueStats.map((r) => r.cart_status ?? 'Unknown'),
            datasets: [{ label: 'Avg Cart Value ($)', data: valueStats.map((r) => +(r.avg_cart_value ?? 0).toFixed(2)), backgroundColor: 'rgba(59,130,246,0.82)' }],
        };

        // ---- Avg items by status bar ----------------------------------------
        const avgItemsByStatusData = {
            labels: valueStats.map((r) => r.cart_status ?? 'Unknown'),
            datasets: [{ label: 'Avg Cart Items', data: valueStats.map((r) => +(r.avg_cart_items ?? 0).toFixed(2)), backgroundColor: 'rgba(34,197,94,0.82)' }],
        };

        // ---- Avg time in cart by status bar ---------------------------------
        const avgTimeByStatusData = {
            labels: valueStats.map((r) => r.cart_status ?? 'Unknown'),
            datasets: [{ label: 'Avg Time in Cart (days)', data: valueStats.map((r) => +(r.avg_time_in_cart_days ?? 0).toFixed(2)), backgroundColor: 'rgba(249,115,22,0.82)' }],
        };

        // ---- Recovery potential score by status -----------------------------
        const recoveryScoreData = {
            labels: valueStats.map((r) => r.cart_status ?? 'Unknown'),
            datasets: [{ label: 'Avg Recovery Potential Score', data: valueStats.map((r) => +(r.avg_recovery_potential_score ?? 0).toFixed(3)), backgroundColor: 'rgba(139,92,246,0.82)' }],
        };

        // ---- Time-to-purchase by tier bar -----------------------------------
        const ttpByTierData = ttpByTier.length > 0 ? {
            labels: ttpByTier.map((r) => r.cart_value_tier ?? 'Unknown'),
            datasets: [
                { label: 'Avg Days to Purchase', data: ttpByTier.map((r) => +(r.avg_time_in_cart_days ?? 0).toFixed(2)), backgroundColor: 'rgba(6,182,212,0.82)' },
            ],
        } : null;

        // ---- Time-to-purchase buckets bar -----------------------------------
        const ttpBucketsData = ttpBuckets.length > 0 ? {
            labels: ttpBuckets.map((r) => r.time_to_purchase_bucket ?? 'Unknown'),
            datasets: [{ label: 'Completed Carts', data: ttpBuckets.map((r) => +(r.completed_carts ?? 0)), backgroundColor: PALETTE }],
        } : null;

        return {
            kpis: { totalCarts, totalCartLines, avgCartValue, avgTimeInCart, completedCarts: +(ttpRow.completed_carts ?? 0) },
            statusDoughnutData, cartLinesBarData,
            avgValByStatusData, avgItemsByStatusData, avgTimeByStatusData, recoveryScoreData,
            ttpByTierData, ttpBucketsData,
            valueStats, highAbandoned, ttpByTier,
        };
    }, [rawCart]);

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

    const { kpis, statusDoughnutData, cartLinesBarData,
            avgValByStatusData, avgItemsByStatusData, avgTimeByStatusData, recoveryScoreData,
            ttpByTierData, ttpBucketsData, valueStats, highAbandoned, ttpByTier } = derived;

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
                <KPICard icon="pi-shopping-cart" iconBg="bg-blue-50"   iconColor="text-blue-600"   value={fmt.number(kpis.totalCarts)}      label="Total Carts" />
                <KPICard icon="pi-list"          iconBg="bg-green-50"  iconColor="text-green-600"  value={fmt.number(kpis.totalCartLines)}   label="Total Cart Lines" />
                <KPICard icon="pi-dollar"        iconBg="bg-orange-50" iconColor="text-orange-600" value={fmt.currency(kpis.avgCartValue)}   label="Avg Cart Value" />
                <KPICard icon="pi-clock"         iconBg="bg-purple-50" iconColor="text-purple-600" value={`${fmt.decimal(kpis.avgTimeInCart)}d`} label="Avg Time in Cart" />
            </div>

            {/* Status distribution doughnut + cart lines bar */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {statusDoughnutData.labels.length > 0 && (
                    <Card className="rounded-xl shadow-sm border border-gray-200">
                        <h3 className="text-sm font-semibold text-gray-700 mb-4">Cart Status Distribution</h3>
                        <ChartWrapper><Doughnut data={statusDoughnutData} options={doughnutOpts()} /></ChartWrapper>
                    </Card>
                )}
                {cartLinesBarData.labels.length > 0 && (
                    <Card className="rounded-xl shadow-sm border border-gray-200">
                        <h3 className="text-sm font-semibold text-gray-700 mb-4">Cart Lines by Status</h3>
                        <ChartWrapper><Bar data={cartLinesBarData} options={barOpts()} /></ChartWrapper>
                    </Card>
                )}
            </div>

            {/* Avg value + avg items by status */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {avgValByStatusData.labels.length > 0 && (
                    <Card className="rounded-xl shadow-sm border border-gray-200">
                        <h3 className="text-sm font-semibold text-gray-700 mb-4">Avg Cart Value by Status</h3>
                        <ChartWrapper><Bar data={avgValByStatusData} options={barOpts()} /></ChartWrapper>
                    </Card>
                )}
                {avgItemsByStatusData.labels.length > 0 && (
                    <Card className="rounded-xl shadow-sm border border-gray-200">
                        <h3 className="text-sm font-semibold text-gray-700 mb-4">Avg Items in Cart by Status</h3>
                        <ChartWrapper><Bar data={avgItemsByStatusData} options={barOpts()} /></ChartWrapper>
                    </Card>
                )}
            </div>

            {/* Avg time in cart + recovery potential */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {avgTimeByStatusData.labels.length > 0 && (
                    <Card className="rounded-xl shadow-sm border border-gray-200">
                        <h3 className="text-sm font-semibold text-gray-700 mb-4">Avg Time in Cart by Status (days)</h3>
                        <ChartWrapper><Bar data={avgTimeByStatusData} options={barOpts()} /></ChartWrapper>
                    </Card>
                )}
                {recoveryScoreData.labels.length > 0 && (
                    <Card className="rounded-xl shadow-sm border border-gray-200">
                        <h3 className="text-sm font-semibold text-gray-700 mb-4">Avg Recovery Potential Score by Status</h3>
                        <ChartWrapper><Bar data={recoveryScoreData} options={barOpts()} /></ChartWrapper>
                    </Card>
                )}
            </div>

            {/* Time-to-purchase by tier + buckets */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {ttpByTierData && (
                    <Card className="rounded-xl shadow-sm border border-gray-200">
                        <h3 className="text-sm font-semibold text-gray-700 mb-4">Time to Purchase by Cart Value Tier</h3>
                        <ChartWrapper><Bar data={ttpByTierData} options={barOpts()} /></ChartWrapper>
                    </Card>
                )}
                {ttpBucketsData && (
                    <Card className="rounded-xl shadow-sm border border-gray-200">
                        <h3 className="text-sm font-semibold text-gray-700 mb-4">Time-to-Purchase Distribution</h3>
                        <ChartWrapper><Bar data={ttpBucketsData} options={barOpts()} /></ChartWrapper>
                    </Card>
                )}
            </div>

            {/* Cart Value Stats Table */}
            {valueStats.length > 0 && (
                <Card className="rounded-xl shadow-sm border border-gray-200">
                    <h3 className="text-sm font-semibold text-gray-700 mb-4">Cart Value Stats by Status</h3>
                    <DataTable value={valueStats} scrollable stripedRows emptyMessage="No data" className="text-sm">
                        <Column field="cart_status"                 header="Status"              sortable />
                        <Column field="carts_count"                 header="Carts"               sortable body={(r) => fmt.number(r.carts_count)} />
                        <Column field="avg_cart_value"              header="Avg Value"            sortable body={(r) => fmt.currency(r.avg_cart_value)} />
                        <Column field="avg_cart_items"              header="Avg Items"            sortable body={(r) => fmt.decimal(r.avg_cart_items)} />
                        <Column field="avg_time_in_cart_days"       header="Avg Time (days)"      sortable body={(r) => fmt.decimal(r.avg_time_in_cart_days)} />
                        <Column field="avg_recovery_potential_score" header="Recovery Score"      sortable body={(r) => (
                            <Tag value={fmt.decimal(r.avg_recovery_potential_score, 3)}
                                severity={riskColor(r.avg_recovery_potential_score)} />
                        )} />
                    </DataTable>
                </Card>
            )}

            {/* Time-to-purchase by tier table */}
            {ttpByTier.length > 0 && (
                <Card className="rounded-xl shadow-sm border border-gray-200">
                    <h3 className="text-sm font-semibold text-gray-700 mb-4">Time to Purchase by Cart Value Tier</h3>
                    <DataTable value={ttpByTier} scrollable stripedRows emptyMessage="No data" className="text-sm">
                        <Column field="cart_value_tier"       header="Tier"                sortable />
                        <Column field="completed_carts"       header="Completed Carts"     sortable body={(r) => fmt.number(r.completed_carts)} />
                        <Column field="avg_time_in_cart_days" header="Avg Days to Purchase" sortable body={(r) => fmt.decimal(r.avg_time_in_cart_days)} />
                        <Column field="avg_time_in_cart_hours" header="Avg Hours"          sortable body={(r) => fmt.decimal(r.avg_time_in_cart_hours)} />
                        <Column field="avg_cart_items_count"  header="Avg Items"           sortable body={(r) => fmt.decimal(r.avg_cart_items_count)} />
                    </DataTable>
                </Card>
            )}

            {/* High-Value Abandoned Carts Table */}
            {highAbandoned.length > 0 && (
                <Card className="rounded-xl shadow-sm border border-gray-200">
                    <h3 className="text-sm font-semibold text-gray-700 mb-2">High-Value Abandoned Carts</h3>
                    <p className="text-xs text-gray-400 mb-4">Carts with highest revenue risk that have been abandoned.</p>
                    <DataTable value={highAbandoned} paginator rows={10} scrollable stripedRows emptyMessage="No data" className="text-sm">
                        <Column field="cart_id"                 header="Cart ID"              sortable />
                        <Column field="customer_id"             header="Customer ID"          sortable />
                        <Column field="cart_total_value"        header="Cart Value"            sortable body={(r) => fmt.currency(r.cart_total_value)} />
                        <Column field="cart_items_count"        header="Items"                sortable body={(r) => fmt.number(r.cart_items_count)} />
                        <Column field="time_in_cart_days"       header="Days in Cart"          sortable body={(r) => fmt.decimal(r.time_in_cart_days)} />
                        <Column field="recovery_potential_score" header="Recovery Score"      sortable body={(r) => (
                            <Tag value={fmt.decimal(r.recovery_potential_score, 3)}
                                severity={riskColor(r.recovery_potential_score)} />
                        )} />
                        <Column field="abandonment_risk_score"  header="Abandon Risk"         sortable body={(r) => (
                            <Tag value={fmt.decimal(r.abandonment_risk_score, 3)}
                                severity={riskColor(r.abandonment_risk_score)} />
                        )} />
                    </DataTable>
                </Card>
            )}
        </div>
    );
}
