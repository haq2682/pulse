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

export default function EngagementBehavior() {
    const fmt = useFormatters();
    const { businessId } = useParams();
    const toastRef = useRef(null);
    const { pipelineStatus } = usePipelineProgress();
    const { dateRange, setDateRange, quickFilter, isFiltered, applyQuickFilter, resetFilters } = useAnalyticsDateFilter();
    const { lastUpdate } = useAnalyticsWebSocket(businessId);

    const [rawCustomer, setRawCustomer] = useState(null);
    const [rawFunnel, setRawFunnel] = useState(null);
    const [loading, setLoading] = useState(true);
    const [fetchError, setFetchError] = useState(false);
    const [dataMode, setDataMode] = useState('unknown');

    // -------------------------------------------------------------------------
    // Fetch
    // -------------------------------------------------------------------------

    const buildUrl = useCallback(() => {
        const base = import.meta.env.VITE_API_URL || 'http://localhost:8000';
        const params = new URLSearchParams({ categories: 'customer_analytics,funnel_analytics' });
        return `${base}/analytics/data/${businessId}?${params.toString()}`;
    }, [businessId]);

    const fetchData = useCallback(async () => {
        if (!businessId) return;
        try {
            setLoading(true);
            setFetchError(false);
            const res = await fetch(buildUrl());
            if (!res.ok) {
                setRawCustomer(null);
                setRawFunnel(null);
                return;
            }
            const json = await res.json();
            if (json.mode) setDataMode(json.mode);
            setRawCustomer(json.categories?.customer_analytics ?? null);
            setRawFunnel(json.categories?.funnel_analytics ?? null);
        } catch {
            console.error('[EngagementBehavior] fetch error');
            setFetchError(true);
            setRawCustomer(null);
            setRawFunnel(null);
        } finally {
            setLoading(false);
        }
    }, [buildUrl, businessId]);

    useEffect(() => { fetchData(); }, [businessId]); // eslint-disable-line
    useEffect(() => {
        if (!lastUpdate) return;
        fetchData();
        toastRef.current?.show({ severity: 'info', summary: 'Data Updated', detail: 'Analytics pipeline completed — refreshing behavior data.', life: 3000 });
    }, [lastUpdate]); // eslint-disable-line

    // -------------------------------------------------------------------------
    // Derived data
    // -------------------------------------------------------------------------

    const derived = useMemo(() => {
        const ca = rawCustomer?.analytics ?? {};
        const fa = rawFunnel?.analytics   ?? {};

        const custEng        = ca.customer_engagement?.data              ?? [];
        const sessConvDist   = ca.session_conversion_distribution?.data  ?? [];
        const cartAbandDist  = ca.cart_abandonment_distribution?.data    ?? [];
        const highIntent     = ca.high_intent_non_buyers?.data           ?? [];

        const funnelByDevice   = fa.funnel_by_device?.data   ?? [];
        const funnelByReferrer = fa.funnel_by_referrer?.data ?? [];
        const highVsRegular    = fa.high_value_vs_regular?.data ?? [];

        if (
            custEng.length === 0 &&
            sessConvDist.length === 0 &&
            funnelByDevice.length === 0 &&
            funnelByReferrer.length === 0
        ) return null;

        // ---- KPIs -----------------------------------------------------------
        const totalTrackedCustomers  = custEng.length;
        const avgSessions            = custEng.length > 0
            ? custEng.reduce((s, r) => s + (+(r.total_sessions ?? 0)), 0) / custEng.length
            : 0;
        const avgPagesViewed         = custEng.length > 0
            ? custEng.reduce((s, r) => s + (+(r.total_pages_viewed ?? 0)), 0) / custEng.length
            : 0;
        const highIntentCount        = highIntent.length;

        // ---- Session conversion distribution bar --------------------------
        const sessConvBarData = sessConvDist.length > 0 ? {
            labels: sessConvDist.map((r) => `${r.session_conversion_percentage ?? 0}%`),
            datasets: [{
                label: 'Customers',
                data: sessConvDist.map((r) => +(r.customer_count ?? 0)),
                backgroundColor: PALETTE,
            }],
        } : null;

        // ---- Cart abandonment distribution bar ---------------------------
        const cartAbandBarData = cartAbandDist.length > 0 ? {
            labels: cartAbandDist.map((r) => `${r.cart_abandonment_percentage ?? 0}%`),
            datasets: [{
                label: 'Customers',
                data: cartAbandDist.map((r) => +(r.customer_count ?? 0)),
                backgroundColor: cartAbandDist.map((r) => {
                    const v = +(r.cart_abandonment_percentage ?? 0);
                    if (v === 0)   return 'rgba(34,197,94,0.82)';
                    if (v <= 50)   return 'rgba(234,179,8,0.82)';
                    return 'rgba(239,68,68,0.82)';
                }),
            }],
        } : null;

        // ---- Customer engagement distribution buckets (total sessions) ------
        const sessionBuckets = { '1': 0, '2-5': 0, '6-10': 0, '11-20': 0, '20+': 0 };
        custEng.forEach((r) => {
            const s = +(r.total_sessions ?? 0);
            if (s === 1)       sessionBuckets['1']++;
            else if (s <= 5)   sessionBuckets['2-5']++;
            else if (s <= 10)  sessionBuckets['6-10']++;
            else if (s <= 20)  sessionBuckets['11-20']++;
            else               sessionBuckets['20+']++;
        });
        const sessionBucketBar = {
            labels: Object.keys(sessionBuckets),
            datasets: [{
                label: 'Customers',
                data: Object.values(sessionBuckets),
                backgroundColor: PALETTE,
            }],
        };

        // ---- Pages viewed per customer distribution (bucketed) ---------------
        const pagesBuckets = { '1-5': 0, '6-20': 0, '21-50': 0, '51-100': 0, '100+': 0 };
        custEng.forEach((r) => {
            const p = +(r.total_pages_viewed ?? 0);
            if (p <= 5)        pagesBuckets['1-5']++;
            else if (p <= 20)  pagesBuckets['6-20']++;
            else if (p <= 50)  pagesBuckets['21-50']++;
            else if (p <= 100) pagesBuckets['51-100']++;
            else               pagesBuckets['100+']++;
        });
        const pagesBucketBar = {
            labels: Object.keys(pagesBuckets),
            datasets: [{
                label: 'Customers',
                data: Object.values(pagesBuckets),
                backgroundColor: PALETTE,
            }],
        };

        // ---- Funnel by device grouped bar -----------------------------------
        const funnelDeviceGrouped = funnelByDevice.length > 0 ? {
            labels: funnelByDevice.map((r) => r.device_type ?? 'Unknown'),
            datasets: [
                { label: 'Total Sessions',      data: funnelByDevice.map((r) => +(r.total_sessions ?? 0)),      backgroundColor: 'rgba(59,130,246,0.82)' },
                { label: 'With Cart',           data: funnelByDevice.map((r) => +(r.sessions_with_cart ?? 0)),   backgroundColor: 'rgba(249,115,22,0.82)' },
                { label: 'With Orders',         data: funnelByDevice.map((r) => +(r.sessions_with_orders ?? 0)), backgroundColor: 'rgba(34,197,94,0.82)' },
                { label: 'High Value Sessions', data: funnelByDevice.map((r) => +(r.high_value_sessions ?? 0)),  backgroundColor: 'rgba(139,92,246,0.82)' },
            ],
        } : null;

        // ---- Conversion rate by device (bar, color-coded) ---------------------
        const convByDeviceBar = funnelByDevice.length > 0 ? {
            labels: funnelByDevice.map((r) => r.device_type ?? 'Unknown'),
            datasets: [{
                label: 'Conversion Rate %',
                data: funnelByDevice.map((r) => +(r.conversion_rate ?? 0).toFixed(1)),
                backgroundColor: funnelByDevice.map((r) => {
                    const v = +(r.conversion_rate ?? 0);
                    if (v >= 10) return 'rgba(34,197,94,0.82)';
                    if (v >= 5)  return 'rgba(234,179,8,0.82)';
                    return 'rgba(239,68,68,0.82)';
                }),
            }],
        } : null;

        // ---- Funnel by referrer grouped bar ----------------------------------
        const funnelReferrerGrouped = funnelByReferrer.length > 0 ? {
            labels: funnelByReferrer.map((r) => r.referrer_source ?? 'Unknown'),
            datasets: [
                { label: 'Total Sessions',      data: funnelByReferrer.map((r) => +(r.total_sessions ?? 0)),      backgroundColor: 'rgba(59,130,246,0.82)' },
                { label: 'With Cart',           data: funnelByReferrer.map((r) => +(r.sessions_with_cart ?? 0)),   backgroundColor: 'rgba(249,115,22,0.82)' },
                { label: 'With Orders',         data: funnelByReferrer.map((r) => +(r.sessions_with_orders ?? 0)), backgroundColor: 'rgba(34,197,94,0.82)' },
            ],
        } : null;

        // ---- Conversion rate by referrer (horizontal bar) -------------------
        const convByReferrerBar = funnelByReferrer.length > 0 ? {
            labels: funnelByReferrer.map((r) => r.referrer_source ?? 'Unknown'),
            datasets: [{
                label: 'Conversion Rate %',
                data: funnelByReferrer.map((r) => +(r.conversion_rate ?? 0).toFixed(1)),
                backgroundColor: funnelByReferrer.map((r) => {
                    const v = +(r.conversion_rate ?? 0);
                    if (v >= 10) return 'rgba(34,197,94,0.82)';
                    if (v >= 5)  return 'rgba(234,179,8,0.82)';
                    return 'rgba(239,68,68,0.82)';
                }),
            }],
        } : null;

        // ---- High value vs regular session doughnut -------------------------
        const highVsRegDoughnut = highVsRegular.length > 0 ? {
            labels: highVsRegular.map((r) => r.is_high_value_session ? 'High Value' : 'Regular'),
            datasets: [{
                data: highVsRegular.map((r) => +(r.session_count ?? 0)),
                backgroundColor: ['rgba(139,92,246,0.82)', 'rgba(59,130,246,0.82)'],
                borderWidth: 2,
            }],
        } : null;

        // ---- High value vs regular grouped metrics bar ----------------------
        const highVsRegGrouped = highVsRegular.length > 0 ? {
            labels: highVsRegular.map((r) => r.is_high_value_session ? 'High Value' : 'Regular'),
            datasets: [
                { label: 'Avg Products Viewed', data: highVsRegular.map((r) => +(r.avg_products_viewed ?? 0).toFixed(2)), backgroundColor: 'rgba(59,130,246,0.82)' },
                { label: 'Avg Items in Cart',   data: highVsRegular.map((r) => +(r.avg_items_in_cart ?? 0).toFixed(2)),   backgroundColor: 'rgba(249,115,22,0.82)' },
                { label: 'Avg Session Value',   data: highVsRegular.map((r) => +(r.avg_session_value ?? 0).toFixed(2)),   backgroundColor: 'rgba(34,197,94,0.82)' },
            ],
        } : null;

        return {
            kpis: { totalTrackedCustomers, avgSessions, avgPagesViewed, highIntentCount },
            sessConvBarData, cartAbandBarData, sessionBucketBar, pagesBucketBar,
            funnelDeviceGrouped, convByDeviceBar, funnelReferrerGrouped, convByReferrerBar,
            highVsRegDoughnut, highVsRegGrouped,
            highIntent,
        };
    }, [rawCustomer, rawFunnel]);

    const hasData = derived !== null;

    // -------------------------------------------------------------------------
    // Render states
    // -------------------------------------------------------------------------

    if (loading && pipelineStatus !== 'loading') {
        return (
            <div className="flex flex-col items-center justify-center min-h-[400px] gap-4">
                <ProgressSpinner />
                <p className="text-gray-500 text-base">Loading engagement behavior data…</p>
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
                        <p className="text-gray-500 text-sm mt-1">Unable to load engagement behavior data. Please try again later.</p>
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
                            : 'No engagement behavior data to display.'}
                    </p>
                </div>
            </div>
        );
    }

    if (!derived) return null;
    const {
        kpis, sessConvBarData, cartAbandBarData, sessionBucketBar, pagesBucketBar,
        funnelDeviceGrouped, convByDeviceBar, funnelReferrerGrouped, convByReferrerBar,
        highVsRegDoughnut, highVsRegGrouped,
        highIntent,
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
                    * Engagement behavior analytics are static aggregates computed over all available data and do not change with the date filter.
                </p>
            )}

            {/* ── KPI Cards ──────────────────────────────────────────────── */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <KPICard
                    icon="pi-users" iconBg="bg-blue-100" iconColor="text-blue-600"
                    value={fmt.number(kpis.totalTrackedCustomers)}
                    label="Customers Tracked"
                />
                <KPICard
                    icon="pi-refresh" iconBg="bg-purple-100" iconColor="text-purple-600"
                    value={fmt.decimal(kpis.avgSessions, 2)}
                    label="Avg Sessions / Customer"
                />
                <KPICard
                    icon="pi-eye" iconBg="bg-cyan-100" iconColor="text-cyan-600"
                    value={fmt.decimal(kpis.avgPagesViewed, 1)}
                    label="Avg Pages Viewed / Customer"
                />
                <KPICard
                    icon="pi-bolt" iconBg="bg-amber-100" iconColor="text-amber-600"
                    value={fmt.number(kpis.highIntentCount)}
                    label="High-Intent Non-Buyers"
                />
            </div>

            {/* ── Customer Activity Distribution ────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-blue-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Customer Activity Distribution</h2>
                </div>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    <ChartWrapper title="Customers by Number of Sessions" height={300}>
                        <Bar data={sessionBucketBar} options={barOpts()} />
                    </ChartWrapper>
                    <ChartWrapper title="Customers by Total Pages Viewed" height={300}>
                        <Bar data={pagesBucketBar} options={barOpts()} />
                    </ChartWrapper>
                </div>
            </section>

            {/* ── Conversion & Abandonment Distribution ─────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-green-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Session Conversion & Abandonment Distribution</h2>
                </div>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {sessConvBarData && (
                        <ChartWrapper title="Customer Count by Session Conversion Rate Bucket" height={300}>
                            <Bar data={sessConvBarData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                    {cartAbandBarData && (
                        <ChartWrapper title="Customer Count by Cart Abandonment Rate Bucket" height={300}>
                            <Bar data={cartAbandBarData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                </div>
            </section>

            {/* ── Funnel by Device ──────────────────────────────────────── */}
            {(funnelDeviceGrouped || convByDeviceBar) && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-purple-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Funnel Performance by Device</h2>
                    </div>
                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                        {funnelDeviceGrouped && (
                            <ChartWrapper title="Session Stages by Device Type" height={320}>
                                <Bar data={funnelDeviceGrouped} options={groupedBarOpts()} />
                            </ChartWrapper>
                        )}
                        {convByDeviceBar && (
                            <ChartWrapper title="Conversion Rate % by Device (color = level)" height={280}>
                                <Bar data={convByDeviceBar} options={barOpts()} />
                            </ChartWrapper>
                        )}
                    </div>
                </section>
            )}

            {/* ── Funnel by Referrer ────────────────────────────────────── */}
            {(funnelReferrerGrouped || convByReferrerBar) && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-orange-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Funnel Performance by Referrer</h2>
                    </div>
                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                        {funnelReferrerGrouped && (
                            <ChartWrapper title="Session Stages by Referrer Source" height={320}>
                                <Bar data={funnelReferrerGrouped} options={groupedBarOpts()} />
                            </ChartWrapper>
                        )}
                        {convByReferrerBar && (
                            <ChartWrapper title="Conversion Rate % by Referrer Source (color = level)" height={280}>
                                <Bar data={convByReferrerBar} options={barOpts()} />
                            </ChartWrapper>
                        )}
                    </div>
                </section>
            )}

            {/* ── High Value vs Regular Sessions ───────────────────────── */}
            {(highVsRegDoughnut || highVsRegGrouped) && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-indigo-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">High Value vs Regular Sessions</h2>
                    </div>
                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                        {highVsRegDoughnut && (
                            <ChartWrapper title="Session Count: High Value vs Regular" height={260}>
                                <Doughnut data={highVsRegDoughnut} options={doughnutOpts()} />
                            </ChartWrapper>
                        )}
                        {highVsRegGrouped && (
                            <ChartWrapper title="Avg Activity Metrics: High Value vs Regular" height={300}>
                                <Bar data={highVsRegGrouped} options={groupedBarOpts()} />
                            </ChartWrapper>
                        )}
                    </div>
                </section>
            )}

            {/* ── High Intent Non-Buyers Table ──────────────────────────── */}
            {highIntent.length > 0 && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-amber-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">High-Intent Non-Buyers</h2>
                    </div>
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                Customers with High Engagement but Low Conversion
                            </h3>
                            <DataTable
                                value={[...highIntent].sort((a, b) => (+(b.total_products_viewed ?? 0)) - (+(a.total_products_viewed ?? 0)))}
                                paginator rows={15}
                                scrollable stripedRows emptyMessage="No data" className="text-sm">
                                <Column field="customer_id"          header="Customer ID"          sortable />
                                <Column field="total_products_viewed" header="Products Viewed"      sortable body={(r) => fmt.number(r.total_products_viewed)} />
                                <Column field="wishlist_items_count"  header="Wishlist Items"       sortable body={(r) => fmt.number(r.wishlist_items_count)} />
                                <Column field="total_carts_created"   header="Carts Created"        sortable body={(r) => fmt.number(r.total_carts_created)} />
                                <Column field="total_purchased_carts" header="Purchased Carts"      sortable body={(r) => fmt.number(r.total_purchased_carts)} />
                                <Column field="cart_abandonment_rate" header="Cart Abandonment %"   sortable body={(r) => fmt.pct(r.cart_abandonment_rate)} />
                                <Column field="session_conversion_rate" header="Session Conv. %"    sortable body={(r) => fmt.pct(r.session_conversion_rate)} />
                            </DataTable>
                        </div>
                    </Card>
                </section>
            )}
        </div>
    );
}
