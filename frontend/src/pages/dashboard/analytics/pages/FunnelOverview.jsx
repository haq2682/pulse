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

const FUNNEL_COLORS = [
    'rgba(59,130,246,0.85)',
    'rgba(34,197,94,0.85)',
    'rgba(249,115,22,0.85)',
    'rgba(239,68,68,0.85)',
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

const barOpts = (horizontal = false, stacked = false) => ({
    indexAxis: horizontal ? 'y' : 'x',
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { display: stacked, position: 'top' }, title: { display: false } },
    scales: {
        x: { stacked, grid: { color: 'rgba(0,0,0,0.05)' } },
        y: { stacked, grid: { color: 'rgba(0,0,0,0.05)' } },
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

export default function FunnelOverview() {
    const fmt = useFormatters();
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
        const highValueFunnel = a.high_value_funnel?.data ?? [];

        if (byDevice.length === 0 && byReferrer.length === 0) return null;

        // ---- KPIs (aggregate funnel_by_device) ------------------------------
        const totalSessions     = byDevice.reduce((s, r) => s + (+(r.total_sessions ?? 0)), 0);
        const totalViews        = byDevice.reduce((s, r) => s + (+(r.sessions_with_views ?? 0)), 0);
        const totalCart         = byDevice.reduce((s, r) => s + (+(r.sessions_with_cart ?? 0)), 0);
        const totalOrders       = byDevice.reduce((s, r) => s + (+(r.sessions_with_orders ?? 0)), 0);
        const totalHighValue    = byDevice.reduce((s, r) => s + (+(r.high_value_sessions ?? 0)), 0);
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
            byDevice, byReferrer, highValueFunnel,
        };
    }, [rawFunnel]);

    // -------------------------------------------------------------------------
    // Render
    // -------------------------------------------------------------------------

    const hasData = derived !== null;

    if (loading && pipelineStatus !== 'loading') {
        return (
            <div className="flex flex-col items-center justify-center min-h-[400px] gap-4">
                <ProgressSpinner />
                <p className="text-gray-500 text-base">Loading funnel overview…</p>
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
                        <p className="text-gray-500 text-sm mt-1">Unable to load funnel data. Please try again later.</p>
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
    const { kpis, funnelStepsData, convByDeviceData, convByRefData,
            funnelByDeviceData, funnelByRefData, avgValByDeviceData, hvDoughnutData,
            hvBarData, abandonBarData, byDevice, byReferrer } = derived;

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

            {/* ── Funnel Overview ────────────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-blue-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Funnel Overview</h2>
                </div>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    <ChartWrapper title="Conversion Funnel Steps (All Devices)" height={340}>
                        <Bar data={funnelStepsData} options={barOpts(false, false)} />
                    </ChartWrapper>
                    {hvDoughnutData.labels.length > 0 && (
                        <ChartWrapper title="High-Value Sessions by Device" height={280}>
                            <Doughnut data={hvDoughnutData} options={doughnutOpts()} />
                        </ChartWrapper>
                    )}
                </div>
            </section>

            {/* ── Conversion Rates ───────────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-green-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Conversion Rates</h2>
                </div>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {convByDeviceData.labels.length > 0 && (
                        <ChartWrapper title="Conversion Rate % by Device" height={340}>
                            <Bar data={convByDeviceData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                    {convByRefData.labels.length > 0 && (
                        <ChartWrapper title="Conversion Rate % by Referrer" height={340}>
                            <Bar data={convByRefData} options={barOpts(true)} />
                        </ChartWrapper>
                    )}
                </div>
            </section>

            {/* ── Funnel Breakdown ───────────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-purple-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Funnel Breakdown</h2>
                </div>
                <div className="grid grid-cols-1 gap-6">
                    {funnelByDeviceData.labels.length > 0 && (
                        <ChartWrapper title="Funnel Steps by Device" height={340}>
                            <Bar data={funnelByDeviceData} options={groupedBarOpts()} />
                        </ChartWrapper>
                    )}
                    {funnelByRefData.labels.length > 0 && (
                        <ChartWrapper title="Funnel Steps by Referrer Source" height={340}>
                            <Bar data={funnelByRefData} options={groupedBarOpts()} />
                        </ChartWrapper>
                    )}
                    {avgValByDeviceData.labels.length > 0 && (
                        <ChartWrapper title="Avg Session Value by Device" height={340}>
                            <Bar data={avgValByDeviceData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                </div>
            </section>

            {/* ── Session Analysis ───────────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-orange-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Session Analysis</h2>
                </div>
                <div className="grid grid-cols-1 gap-6">
                    {hvBarData && (
                        <ChartWrapper title="High-Value vs Regular Sessions" height={340}>
                            <Bar data={hvBarData} options={groupedBarOpts()} />
                        </ChartWrapper>
                    )}
                    {abandonBarData && (
                        <ChartWrapper title="Abandoned vs Converted Sessions" height={340}>
                            <Bar data={abandonBarData} options={groupedBarOpts()} />
                        </ChartWrapper>
                    )}
                </div>

                {/* By Device Table */}
                {byDevice.length > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">Funnel Breakdown by Device</h3>
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
                        </div>
                    </Card>
                )}

                {/* By Referrer Table */}
                {byReferrer.length > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">Funnel Breakdown by Referrer</h3>
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
                        </div>
                    </Card>
                )}
                {/* High Value Funnel Sessions Table */}
                {(derived?.highValueFunnel?.length ?? 0) > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">High Value Funnel Sessions</h3>
                            <DataTable
                                value={[...derived.highValueFunnel].sort((a, b) => (+(b.session_monetary_value ?? 0)) - (+(a.session_monetary_value ?? 0)))}
                                paginator rows={15} stripedRows emptyMessage="No data" className="text-sm"
                            >
                                <Column field="session_id"             header="Session ID"          sortable />
                                <Column field="device_type"            header="Device"              sortable />
                                <Column field="referrer_source"        header="Referrer"            sortable />
                                <Column field="total_products_viewed"  header="Products Viewed"     sortable body={(r) => fmt.number(r.total_products_viewed)} />
                                <Column field="items_added_to_cart"    header="Cart Items"          sortable body={(r) => fmt.number(r.items_added_to_cart)} />
                                <Column field="orders_from_session"    header="Orders"              sortable body={(r) => fmt.number(r.orders_from_session)} />
                                <Column field="session_monetary_value" header="Session Value"       sortable body={(r) => fmt.currency(r.session_monetary_value)} />
                                <Column field="session_engagement_score" header="Engagement"        sortable body={(r) => fmt.decimal(r.session_engagement_score)} />
                                <Column field="view_to_order_rate"     header="View→Order Rate"     sortable body={(r) => fmt.pct(r.view_to_order_rate)} />
                            </DataTable>
                        </div>
                    </Card>
                )}
            </section>
        </div>
    );
}
