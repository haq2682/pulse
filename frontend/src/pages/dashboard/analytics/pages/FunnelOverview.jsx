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

const FUNNEL_COLORS = [
    'rgba(59,130,246,0.85)',
    'rgba(34,197,94,0.85)',
    'rgba(249,115,22,0.85)',
    'rgba(239,68,68,0.85)',
];

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

const barOpts = (horizontal = false, stacked = false) => ({
    indexAxis: horizontal ? 'y' : 'x',
    responsive: true,
    plugins: { legend: { display: stacked, position: 'top' }, title: { display: false } },
    scales: {
        x: { stacked, grid: { color: 'rgba(0,0,0,0.05)' } },
        y: { stacked, grid: { color: 'rgba(0,0,0,0.05)' } },
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

export default function FunnelOverview() {
    const { businessId } = useParams();
    const toastRef = useRef(null);
    const { pipelineStatus } = usePipelineProgress();
    const { dateRange, setDateRange, quickFilter, isFiltered, applyQuickFilter, resetFilters } = useAnalyticsDateFilter();
    const { lastUpdate } = useAnalyticsWebSocket(businessId);

    const [rawFunnel, setRawFunnel] = useState(null);
    const [loading, setLoading] = useState(true);
    const [fetchError, setFetchError] = useState(false);
    const [dataMode, setDataMode] = useState('unknown');

    // -------------------------------------------------------------------------
    // Fetch
    // -------------------------------------------------------------------------

    const buildUrl = useCallback(() => {
        const base = import.meta.env.VITE_API_URL || 'http://localhost:8000';
        const params = new URLSearchParams({ categories: 'funnel_analytics' });
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
                setRawFunnel(null);
                return;
            }
            const json = await res.json();
            if (json.mode) setDataMode(json.mode);
            setRawFunnel(json.categories?.funnel_analytics ?? null);
        } catch {
            console.error('[FunnelOverview] fetch error');
            setFetchError(true);
            setRawFunnel(null);
            toastRef.current?.show({ severity: 'error', summary: 'Error', detail: 'Unable to load funnel data.', life: 5000 });
        } finally {
            setLoading(false);
        }
    }, [buildUrl, businessId]);

    useEffect(() => { fetchData(); }, [businessId]); // eslint-disable-line
    useEffect(() => {
        if (!lastUpdate) return;
        fetchData();
        toastRef.current?.show({ severity: 'info', summary: 'Data Updated', detail: 'Analytics pipeline completed — refreshing funnel data.', life: 3000 });
    }, [lastUpdate]); // eslint-disable-line

    // -------------------------------------------------------------------------
    // Derived data
    // -------------------------------------------------------------------------

    const derived = useMemo(() => {
        if (!rawFunnel) return null;
        const a = rawFunnel.analytics ?? {};

        const byDevice    = a.funnel_by_device?.data    ?? [];
        const byReferrer  = a.funnel_by_referrer?.data  ?? [];
        const hvVsReg     = a.high_value_vs_regular?.data ?? [];
        const abandonVsConv = a.abandoned_vs_converted?.data ?? [];

        if (byDevice.length === 0 && byReferrer.length === 0) return null;

        // ---- KPIs (aggregate funnel_by_device) ------------------------------
        const totalSessions     = byDevice.reduce((s, r) => s + (+(r.total_sessions ?? 0)), 0);
        const totalViews        = byDevice.reduce((s, r) => s + (+(r.sessions_with_views ?? 0)), 0);
        const totalCart         = byDevice.reduce((s, r) => s + (+(r.sessions_with_cart ?? 0)), 0);
        const totalOrders       = byDevice.reduce((s, r) => s + (+(r.sessions_with_orders ?? 0)), 0);
        const totalHighValue    = byDevice.reduce((s, r) => s + (+(r.high_value_sessions ?? 0)), 0);
        const avgConvRate       = byDevice.length > 0
            ? byDevice.reduce((s, r) => s + (+(r.conversion_rate ?? 0)), 0) / byDevice.length
            : 0;
        const overallConvRate   = totalSessions > 0 ? (totalOrders / totalSessions) * 100 : 0;
        const viewToCartRate    = totalViews > 0 ? (totalCart / totalViews) * 100 : 0;
        const cartToOrderRate   = totalCart > 0 ? (totalOrders / totalCart) * 100 : 0;

        // ---- Funnel waterfall bar (aggregate) --------------------------------
        const funnelStepsData = {
            labels: ['Sessions', 'With Views', 'With Cart', 'With Orders'],
            datasets: [{
                label: 'Sessions',
                data: [totalSessions, totalViews, totalCart, totalOrders],
                backgroundColor: FUNNEL_COLORS,
            }],
        };

        // ---- Conversion rate by device bar ----------------------------------
        const convByDeviceData = {
            labels: byDevice.map((r) => r.device_type ?? 'Unknown'),
            datasets: [{
                label: 'Conversion Rate %',
                data: byDevice.map((r) => +(r.conversion_rate ?? 0).toFixed(2)),
                backgroundColor: 'rgba(59,130,246,0.82)',
            }],
        };

        // ---- Conversion rate by referrer bar --------------------------------
        const convByRefData = {
            labels: byReferrer.map((r) => r.referrer_source ?? 'Unknown'),
            datasets: [{
                label: 'Conversion Rate %',
                data: byReferrer.map((r) => +(r.conversion_rate ?? 0).toFixed(2)),
                backgroundColor: 'rgba(34,197,94,0.82)',
            }],
        };

        // ---- Funnel steps by device (grouped bar) ---------------------------
        const deviceLabels = byDevice.map((r) => r.device_type ?? 'Unknown');
        const funnelByDeviceData = {
            labels: deviceLabels,
            datasets: [
                { label: 'Total Sessions',    data: byDevice.map((r) => +(r.total_sessions ?? 0)),     backgroundColor: 'rgba(59,130,246,0.75)' },
                { label: 'With Views',        data: byDevice.map((r) => +(r.sessions_with_views ?? 0)), backgroundColor: 'rgba(34,197,94,0.75)' },
                { label: 'With Cart',         data: byDevice.map((r) => +(r.sessions_with_cart ?? 0)),  backgroundColor: 'rgba(249,115,22,0.75)' },
                { label: 'With Orders',       data: byDevice.map((r) => +(r.sessions_with_orders ?? 0)),backgroundColor: 'rgba(139,92,246,0.75)' },
            ],
        };

        // ---- Funnel steps by referrer (grouped bar) -------------------------
        const refLabels = byReferrer.map((r) => r.referrer_source ?? 'Unknown');
        const funnelByRefData = {
            labels: refLabels,
            datasets: [
                { label: 'Total Sessions',    data: byReferrer.map((r) => +(r.total_sessions ?? 0)),     backgroundColor: 'rgba(59,130,246,0.75)' },
                { label: 'With Views',        data: byReferrer.map((r) => +(r.sessions_with_views ?? 0)), backgroundColor: 'rgba(34,197,94,0.75)' },
                { label: 'With Cart',         data: byReferrer.map((r) => +(r.sessions_with_cart ?? 0)),  backgroundColor: 'rgba(249,115,22,0.75)' },
                { label: 'With Orders',       data: byReferrer.map((r) => +(r.sessions_with_orders ?? 0)),backgroundColor: 'rgba(139,92,246,0.75)' },
            ],
        };

        // ---- Avg session value by device ------------------------------------
        const avgValByDeviceData = {
            labels: deviceLabels,
            datasets: [{
                label: 'Avg Session Value ($)',
                data: byDevice.map((r) => +(r.avg_session_value ?? 0).toFixed(2)),
                backgroundColor: 'rgba(6,182,212,0.82)',
            }],
        };

        // ---- High-value sessions doughnut -----------------------------------
        const hvDoughnutData = {
            labels: deviceLabels,
            datasets: [{ data: byDevice.map((r) => +(r.high_value_sessions ?? 0)), backgroundColor: PALETTE }],
        };

        // ---- High-Value vs Regular comparison bar ---------------------------
        const hvBarData = hvVsReg.length > 0 ? {
            labels: hvVsReg.map((r) => r.is_high_value_session ? 'High-Value' : 'Regular'),
            datasets: [
                { label: 'Avg Session Value ($)',  data: hvVsReg.map((r) => +(r.avg_session_value ?? 0).toFixed(2)), backgroundColor: 'rgba(59,130,246,0.82)' },
                { label: 'Avg Products Viewed',    data: hvVsReg.map((r) => +(r.avg_products_viewed ?? 0).toFixed(2)), backgroundColor: 'rgba(34,197,94,0.82)' },
                { label: 'Avg Items in Cart',      data: hvVsReg.map((r) => +(r.avg_items_in_cart ?? 0).toFixed(2)), backgroundColor: 'rgba(249,115,22,0.82)' },
            ],
        } : null;

        // ---- Abandoned vs Converted comparison ------------------------------
        const abandonBarData = abandonVsConv.length > 0 ? {
            labels: abandonVsConv.map((r) => r.converted ? 'Converted' : 'Abandoned'),
            datasets: [
                { label: 'Session Count',        data: abandonVsConv.map((r) => +(r.session_count ?? 0)), backgroundColor: 'rgba(59,130,246,0.82)' },
                { label: 'Avg Products Viewed',  data: abandonVsConv.map((r) => +(r.avg_products_viewed ?? 0).toFixed(2)), backgroundColor: 'rgba(34,197,94,0.82)' },
                { label: 'Avg Items in Cart',    data: abandonVsConv.map((r) => +(r.avg_items_in_cart ?? 0).toFixed(2)), backgroundColor: 'rgba(249,115,22,0.82)' },
                { label: 'Avg Cart Value ($)',   data: abandonVsConv.map((r) => +(r.avg_cart_value ?? 0).toFixed(2)), backgroundColor: 'rgba(139,92,246,0.82)' },
            ],
        } : null;

        return {
            kpis: { totalSessions, totalViews, totalCart, totalOrders, totalHighValue, overallConvRate, viewToCartRate, cartToOrderRate },
            funnelStepsData, convByDeviceData, convByRefData,
            funnelByDeviceData, funnelByRefData, avgValByDeviceData, hvDoughnutData,
            hvBarData, abandonBarData,
            byDevice, byReferrer,
        };
    }, [rawFunnel]);

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

    const { kpis, funnelStepsData, convByDeviceData, convByRefData,
            funnelByDeviceData, funnelByRefData, avgValByDeviceData, hvDoughnutData,
            hvBarData, abandonBarData, byDevice, byReferrer } = derived;

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
                <KPICard icon="pi-users"      iconBg="bg-blue-50"   iconColor="text-blue-600"   value={fmt.number(kpis.totalSessions)}    label="Total Sessions" />
                <KPICard icon="pi-check-circle" iconBg="bg-green-50" iconColor="text-green-600" value={fmt.pct(kpis.overallConvRate)}      label="Overall Conv. Rate" />
                <KPICard icon="pi-shopping-cart" iconBg="bg-orange-50" iconColor="text-orange-600" value={fmt.pct(kpis.viewToCartRate)}   label="View-to-Cart Rate" />
                <KPICard icon="pi-star"       iconBg="bg-purple-50" iconColor="text-purple-600" value={fmt.pct(kpis.cartToOrderRate)}     label="Cart-to-Order Rate" />
            </div>

            {/* Secondary KPI row */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <KPICard icon="pi-eye"        iconBg="bg-cyan-50"   iconColor="text-cyan-600"   value={fmt.number(kpis.totalViews)}       label="Sessions w/ Views" />
                <KPICard icon="pi-shopping-bag" iconBg="bg-yellow-50" iconColor="text-yellow-600" value={fmt.number(kpis.totalCart)}     label="Sessions w/ Cart" />
                <KPICard icon="pi-box"        iconBg="bg-indigo-50" iconColor="text-indigo-600" value={fmt.number(kpis.totalOrders)}      label="Sessions w/ Orders" />
                <KPICard icon="pi-star-fill"  iconBg="bg-pink-50"   iconColor="text-pink-600"   value={fmt.number(kpis.totalHighValue)}   label="High-Value Sessions" />
            </div>

            {/* Funnel Waterfall + High-Value doughnut */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                <Card className="rounded-xl shadow-sm border border-gray-200">
                    <h3 className="text-sm font-semibold text-gray-700 mb-4">Conversion Funnel Steps (All Devices)</h3>
                    <ChartWrapper><Bar data={funnelStepsData} options={barOpts(false, false)} /></ChartWrapper>
                </Card>
                {hvDoughnutData.labels.length > 0 && (
                    <Card className="rounded-xl shadow-sm border border-gray-200">
                        <h3 className="text-sm font-semibold text-gray-700 mb-4">High-Value Sessions by Device</h3>
                        <ChartWrapper><Doughnut data={hvDoughnutData} options={doughnutOpts()} /></ChartWrapper>
                    </Card>
                )}
            </div>

            {/* Conversion rate by device + referrer */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {convByDeviceData.labels.length > 0 && (
                    <Card className="rounded-xl shadow-sm border border-gray-200">
                        <h3 className="text-sm font-semibold text-gray-700 mb-4">Conversion Rate % by Device</h3>
                        <ChartWrapper><Bar data={convByDeviceData} options={barOpts()} /></ChartWrapper>
                    </Card>
                )}
                {convByRefData.labels.length > 0 && (
                    <Card className="rounded-xl shadow-sm border border-gray-200">
                        <h3 className="text-sm font-semibold text-gray-700 mb-4">Conversion Rate % by Referrer</h3>
                        <ChartWrapper><Bar data={convByRefData} options={barOpts(true)} /></ChartWrapper>
                    </Card>
                )}
            </div>

            {/* Funnel steps by device grouped bar */}
            {funnelByDeviceData.labels.length > 0 && (
                <Card className="rounded-xl shadow-sm border border-gray-200">
                    <h3 className="text-sm font-semibold text-gray-700 mb-4">Funnel Steps by Device</h3>
                    <ChartWrapper><Bar data={funnelByDeviceData} options={groupedBarOpts()} /></ChartWrapper>
                </Card>
            )}

            {/* Funnel steps by referrer grouped bar */}
            {funnelByRefData.labels.length > 0 && (
                <Card className="rounded-xl shadow-sm border border-gray-200">
                    <h3 className="text-sm font-semibold text-gray-700 mb-4">Funnel Steps by Referrer Source</h3>
                    <ChartWrapper><Bar data={funnelByRefData} options={groupedBarOpts()} /></ChartWrapper>
                </Card>
            )}

            {/* Avg session value by device */}
            {avgValByDeviceData.labels.length > 0 && (
                <Card className="rounded-xl shadow-sm border border-gray-200">
                    <h3 className="text-sm font-semibold text-gray-700 mb-4">Avg Session Value by Device</h3>
                    <ChartWrapper><Bar data={avgValByDeviceData} options={barOpts()} /></ChartWrapper>
                </Card>
            )}

            {/* High-Value vs Regular */}
            {hvBarData && (
                <Card className="rounded-xl shadow-sm border border-gray-200">
                    <h3 className="text-sm font-semibold text-gray-700 mb-4">High-Value vs Regular Sessions</h3>
                    <ChartWrapper><Bar data={hvBarData} options={groupedBarOpts()} /></ChartWrapper>
                </Card>
            )}

            {/* Abandoned vs Converted */}
            {abandonBarData && (
                <Card className="rounded-xl shadow-sm border border-gray-200">
                    <h3 className="text-sm font-semibold text-gray-700 mb-4">Abandoned vs Converted Sessions</h3>
                    <ChartWrapper><Bar data={abandonBarData} options={groupedBarOpts()} /></ChartWrapper>
                </Card>
            )}

            {/* By Device Table */}
            {byDevice.length > 0 && (
                <Card className="rounded-xl shadow-sm border border-gray-200">
                    <h3 className="text-sm font-semibold text-gray-700 mb-4">Funnel Breakdown by Device</h3>
                    <DataTable value={byDevice} scrollable stripedRows emptyMessage="No data" className="text-sm">
                        <Column field="device_type"          header="Device"          sortable />
                        <Column field="total_sessions"       header="Total Sessions"  sortable body={(r) => fmt.number(r.total_sessions)} />
                        <Column field="sessions_with_views"  header="With Views"      sortable body={(r) => fmt.number(r.sessions_with_views)} />
                        <Column field="sessions_with_cart"   header="With Cart"       sortable body={(r) => fmt.number(r.sessions_with_cart)} />
                        <Column field="sessions_with_orders" header="With Orders"     sortable body={(r) => fmt.number(r.sessions_with_orders)} />
                        <Column field="high_value_sessions"  header="High-Value"      sortable body={(r) => fmt.number(r.high_value_sessions)} />
                        <Column field="avg_session_value"    header="Avg Session Val" sortable body={(r) => fmt.currency(r.avg_session_value)} />
                        <Column field="conversion_rate"      header="Conv. Rate %"    sortable body={(r) => fmt.pct(r.conversion_rate)} />
                    </DataTable>
                </Card>
            )}

            {/* By Referrer Table */}
            {byReferrer.length > 0 && (
                <Card className="rounded-xl shadow-sm border border-gray-200">
                    <h3 className="text-sm font-semibold text-gray-700 mb-4">Funnel Breakdown by Referrer</h3>
                    <DataTable value={byReferrer} scrollable stripedRows emptyMessage="No data" className="text-sm">
                        <Column field="referrer_source"      header="Referrer"        sortable />
                        <Column field="total_sessions"       header="Total Sessions"  sortable body={(r) => fmt.number(r.total_sessions)} />
                        <Column field="sessions_with_views"  header="With Views"      sortable body={(r) => fmt.number(r.sessions_with_views)} />
                        <Column field="sessions_with_cart"   header="With Cart"       sortable body={(r) => fmt.number(r.sessions_with_cart)} />
                        <Column field="sessions_with_orders" header="With Orders"     sortable body={(r) => fmt.number(r.sessions_with_orders)} />
                        <Column field="high_value_sessions"  header="High-Value"      sortable body={(r) => fmt.number(r.high_value_sessions)} />
                        <Column field="avg_session_value"    header="Avg Session Val" sortable body={(r) => fmt.currency(r.avg_session_value)} />
                        <Column field="conversion_rate"      header="Conv. Rate %"    sortable body={(r) => fmt.pct(r.conversion_rate)} />
                    </DataTable>
                </Card>
            )}
        </div>
    );
}
