import React, { useState, useEffect, useRef, useMemo, useCallback } from 'react';
import { useParams } from 'react-router-dom';
import { Card } from 'primereact/card';
import { ProgressSpinner } from 'primereact/progressspinner';
import { Toast } from 'primereact/toast';
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

const doughnutOpts = () => ({
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { position: 'right' }, title: { display: false } },
});

// ---------------------------------------------------------------------------
// Main Component
// ---------------------------------------------------------------------------

export default function EngagementMetrics() {
    const fmt = useFormatters();
    const { businessId } = useParams();
    const toastRef = useRef(null);
    const { pipelineStatus } = usePipelineProgress();
    const { dateRange, setDateRange, quickFilter, isFiltered, applyQuickFilter, resetFilters } = useAnalyticsDateFilter();
    const { lastUpdate } = useAnalyticsWebSocket(businessId);

    const [rawKpis, setRawKpis] = useState(null);
    const [loading, setLoading] = useState(true);
    const [fetchError, setFetchError] = useState(false);
    const [dataMode, setDataMode] = useState('unknown');

    // -------------------------------------------------------------------------
    // Fetch
    // -------------------------------------------------------------------------

    const buildUrl = useCallback(() => {
        const base = import.meta.env.VITE_API_URL || 'http://localhost:8000';
        const params = new URLSearchParams({ categories: 'kpis' });
        return `${base}/analytics/data/${businessId}?${params.toString()}`;
    }, [businessId]);

    const fetchData = useCallback(async () => {
        if (!businessId) return;
        try {
            setLoading(true);
            setFetchError(false);
            const res = await fetch(buildUrl());
            if (!res.ok) {
                setRawKpis(null);
                return;
            }
            const json = await res.json();
            if (json.mode) setDataMode(json.mode);
            setRawKpis(json.categories?.kpis ?? null);
        } catch {
            console.error('[EngagementMetrics] fetch error');
            setFetchError(true);
            setRawKpis(null);
        } finally {
            setLoading(false);
        }
    }, [buildUrl, businessId]);

    useEffect(() => { fetchData(); }, [businessId]); // eslint-disable-line
    useEffect(() => {
        if (!lastUpdate) return;
        fetchData();
        toastRef.current?.show({ severity: 'info', summary: 'Data Updated', detail: 'Analytics pipeline completed — refreshing engagement metrics.', life: 3000 });
    }, [lastUpdate]); // eslint-disable-line

    // -------------------------------------------------------------------------
    // Derived data
    // -------------------------------------------------------------------------

    const derived = useMemo(() => {
        if (!rawKpis) return null;
        const a = rawKpis.analytics ?? {};

        const engSummary  = a.customer_engagement_summary?.data?.[0]  ?? null;
        const funnelSum   = a.funnel_summary?.data?.[0]                ?? null;
        const cartSum     = a.cart_abandon_summary?.data?.[0]          ?? null;
        const sessionSum  = a.session_to_order_analysis?.data?.[0]     ?? null;

        if (!engSummary && !funnelSum && !cartSum) return null;

        // ---- KPIs -----------------------------------------------------------
        const totalSessions        = +(engSummary?.total_sessions_all_customers ?? funnelSum?.total_sessions ?? 0);
        const avgSessionsPerCust   = +(engSummary?.avg_sessions_per_customer ?? 0);
        const totalPagesViewed     = +(engSummary?.total_pages_viewed_all_customers ?? 0);
        const avgPagesPerCust      = +(engSummary?.avg_pages_viewed_per_customer ?? 0);
        const avgProductsViewed    = +(engSummary?.avg_products_viewed_per_customer ?? funnelSum?.avg_products_viewed ?? 0);
        const overallConvRate      = +(funnelSum?.overall_conversion_rate ?? sessionSum?.avg_session_conversion_rate ?? 0);
        const cartAbandRate        = +(cartSum?.abandonment_rate ?? sessionSum?.avg_cart_abandonment_rate ?? 0);
        const avgSessionValue      = +(funnelSum?.avg_session_value ?? 0);

        // ---- Funnel stages bar (sessions → views → cart → orders) ----------
        const funnelStagesData = funnelSum ? {
            labels: ['Total Sessions', 'With Views', 'With Cart', 'With Orders'],
            datasets: [{
                label: 'Sessions',
                data: [
                    +(funnelSum.total_sessions ?? 0),
                    +(funnelSum.sessions_with_views ?? 0),
                    +(funnelSum.sessions_with_cart ?? 0),
                    +(funnelSum.sessions_with_orders ?? 0),
                ],
                backgroundColor: [
                    'rgba(59,130,246,0.82)',
                    'rgba(139,92,246,0.82)',
                    'rgba(249,115,22,0.82)',
                    'rgba(34,197,94,0.82)',
                ],
            }],
        } : null;

        // ---- Conversion rates at each funnel step bar ----------------------
        const convRatesData = funnelSum ? {
            labels: ['View→Cart', 'Cart→Order', 'Overall'],
            datasets: [{
                label: 'Conversion Rate %',
                data: [
                    +(funnelSum.view_to_cart_conversion ?? 0).toFixed(1),
                    +(funnelSum.cart_to_order_conversion ?? 0).toFixed(1),
                    +(funnelSum.overall_conversion_rate ?? 0).toFixed(1),
                ],
                backgroundColor: [
                    'rgba(59,130,246,0.82)',
                    'rgba(34,197,94,0.82)',
                    'rgba(139,92,246,0.82)',
                ],
            }],
        } : null;

        // ---- Cart abandoned vs converted doughnut ---------------------------
        const cartDoughnut = cartSum ? {
            labels: ['Abandoned', 'Converted'],
            datasets: [{
                data: [
                    +(cartSum.abandoned_carts ?? 0),
                    +(cartSum.converted_carts ?? 0),
                ],
                backgroundColor: ['rgba(239,68,68,0.82)', 'rgba(34,197,94,0.82)'],
                borderWidth: 2,
            }],
        } : null;

        // ---- Avg per-session metrics grouped bar ----------------------------
        const sessionAvgData = funnelSum ? {
            labels: ['Avg Products Viewed', 'Avg Items in Cart', 'Avg Orders/Session'],
            datasets: [{
                label: 'Avg Per Session',
                data: [
                    +(funnelSum.avg_products_viewed ?? 0).toFixed(2),
                    +(funnelSum.avg_items_added_to_cart ?? 0).toFixed(2),
                    +(funnelSum.avg_orders_per_session ?? 0).toFixed(3),
                ],
                backgroundColor: [
                    'rgba(59,130,246,0.82)',
                    'rgba(249,115,22,0.82)',
                    'rgba(34,197,94,0.82)',
                ],
            }],
        } : null;

        // ---- Customer engagement per-customer grouped bar ------------------
        const custEngAvgData = engSummary ? {
            labels: ['Sessions/Customer', 'Pages Viewed/Customer', 'Products Viewed/Customer'],
            datasets: [{
                label: 'Avg per Customer',
                data: [
                    +(engSummary.avg_sessions_per_customer ?? 0).toFixed(2),
                    +(engSummary.avg_pages_viewed_per_customer ?? 0).toFixed(2),
                    +(engSummary.avg_products_viewed_per_customer ?? 0).toFixed(2),
                ],
                backgroundColor: [
                    'rgba(139,92,246,0.82)',
                    'rgba(6,182,212,0.82)',
                    'rgba(234,179,8,0.82)',
                ],
            }],
        } : null;

        return {
            kpis: { totalSessions, avgSessionsPerCust, totalPagesViewed, avgPagesPerCust, avgProductsViewed, overallConvRate, cartAbandRate, avgSessionValue },
            funnelStagesData, convRatesData, cartDoughnut, sessionAvgData, custEngAvgData,
            funnelSum, cartSum, engSummary,
        };
    }, [rawKpis]);

    const hasData = derived !== null;

    // -------------------------------------------------------------------------
    // Render states
    // -------------------------------------------------------------------------

    if (loading && pipelineStatus !== 'loading') {
        return (
            <div className="flex flex-col items-center justify-center min-h-[400px] gap-4">
                <ProgressSpinner />
                <p className="text-gray-500 text-base">Loading engagement metrics…</p>
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
                        <p className="text-gray-500 text-sm mt-1">Unable to load engagement metrics. Please try again later.</p>
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
                            : 'No engagement metrics to display.'}
                    </p>
                </div>
            </div>
        );
    }

    if (!derived) return null;
    const {
        kpis, funnelStagesData, convRatesData, cartDoughnut, sessionAvgData, custEngAvgData,
        funnelSum, cartSum, engSummary,
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
                    * Engagement metrics are static aggregates computed over all available data and do not change with the date filter.
                </p>
            )}

            {/* ── KPI Cards ──────────────────────────────────────────────── */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <KPICard
                    icon="pi-user" iconBg="bg-blue-100" iconColor="text-blue-600"
                    value={fmt.number(kpis.totalSessions)}
                    label="Total Sessions"
                />
                <KPICard
                    icon="pi-eye" iconBg="bg-purple-100" iconColor="text-purple-600"
                    value={fmt.number(kpis.totalPagesViewed)}
                    label="Total Pages Viewed"
                />
                <KPICard
                    icon="pi-percentage" iconBg="bg-green-100" iconColor="text-green-600"
                    value={fmt.pct(kpis.overallConvRate)}
                    label="Overall Conversion Rate"
                />
                <KPICard
                    icon="pi-shopping-cart" iconBg="bg-red-100" iconColor="text-red-600"
                    value={fmt.pct(kpis.cartAbandRate)}
                    label="Cart Abandonment Rate"
                />
            </div>

            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <KPICard
                    icon="pi-refresh" iconBg="bg-cyan-100" iconColor="text-cyan-600"
                    value={fmt.decimal(kpis.avgSessionsPerCust, 2)}
                    label="Avg Sessions / Customer"
                />
                <KPICard
                    icon="pi-file" iconBg="bg-indigo-100" iconColor="text-indigo-600"
                    value={fmt.decimal(kpis.avgPagesPerCust, 1)}
                    label="Avg Pages / Customer"
                />
                <KPICard
                    icon="pi-search" iconBg="bg-orange-100" iconColor="text-orange-600"
                    value={fmt.decimal(kpis.avgProductsViewed, 1)}
                    label="Avg Products Viewed / Customer"
                />
                <KPICard
                    icon="pi-dollar" iconBg="bg-emerald-100" iconColor="text-emerald-600"
                    value={`$${fmt.decimal(kpis.avgSessionValue, 2)}`}
                    label="Avg Session Value"
                />
            </div>

            {/* ── Funnel Overview ───────────────────────────────────────── */}
            {funnelSum && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-blue-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Session Funnel Overview</h2>
                    </div>
                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                        {funnelStagesData && (
                            <ChartWrapper title="Sessions at Each Funnel Stage" height={300}>
                                <Bar data={funnelStagesData} options={barOpts()} />
                            </ChartWrapper>
                        )}
                        {convRatesData && (
                            <ChartWrapper title="Conversion Rates at Each Funnel Step (%)" height={300}>
                                <Bar data={convRatesData} options={barOpts()} />
                            </ChartWrapper>
                        )}
                    </div>
                </section>
            )}

            {/* ── Per-Session & Per-Customer Averages ───────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-purple-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Per-Session & Per-Customer Averages</h2>
                </div>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {sessionAvgData && (
                        <ChartWrapper title="Avg Activity per Session" height={300}>
                            <Bar data={sessionAvgData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                    {custEngAvgData && (
                        <ChartWrapper title="Avg Engagement per Customer" height={300}>
                            <Bar data={custEngAvgData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                </div>
            </section>

            {/* ── Cart Engagement ────────────────────────────────────────── */}
            {cartSum && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-red-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Cart Engagement</h2>
                    </div>
                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                        {cartDoughnut && (
                            <ChartWrapper title="Carts: Abandoned vs Converted" height={280}>
                                <Doughnut data={cartDoughnut} options={doughnutOpts()} />
                            </ChartWrapper>
                        )}
                        <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                            <div className="p-6 space-y-4">
                                <h3 className="text-xl font-semibold text-gray-900 pb-3 border-b border-gray-200">
                                    Cart Summary
                                </h3>
                                {[
                                    { label: 'Total Carts Tracked',   value: fmt.number(cartSum.total_carts_tracked) },
                                    { label: 'Abandoned Carts',        value: fmt.number(cartSum.abandoned_carts) },
                                    { label: 'Converted Carts',        value: fmt.number(cartSum.converted_carts) },
                                    { label: 'Abandonment Rate',       value: fmt.pct(cartSum.abandonment_rate) },
                                    { label: 'Purchase Rate',          value: fmt.pct(cartSum.purchase_rate) },
                                ].map(({ label, value }) => (
                                    <div key={label} className="flex justify-between items-center py-2 border-b border-gray-100 last:border-b-0">
                                        <span className="text-gray-600 text-sm">{label}</span>
                                        <span className="font-semibold text-gray-900">{value}</span>
                                    </div>
                                ))}
                            </div>
                        </Card>
                    </div>
                </section>
            )}

            {/* ── Engagement Totals Summary Card ────────────────────────── */}
            {engSummary && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-cyan-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Customer Engagement Totals</h2>
                    </div>
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                Platform-Wide Engagement Summary
                            </h3>
                            <div className="grid grid-cols-2 md:grid-cols-3 gap-6">
                                {[
                                    { label: 'Total Sessions (all customers)',   value: fmt.number(engSummary.total_sessions_all_customers) },
                                    { label: 'Avg Sessions per Customer',        value: fmt.decimal(engSummary.avg_sessions_per_customer, 2) },
                                    { label: 'Total Pages Viewed',               value: fmt.number(engSummary.total_pages_viewed_all_customers) },
                                    { label: 'Avg Pages per Customer',           value: fmt.decimal(engSummary.avg_pages_viewed_per_customer, 1) },
                                    { label: 'Total Products Viewed',            value: fmt.number(engSummary.total_products_viewed_all_customers) },
                                    { label: 'Avg Products Viewed per Customer', value: fmt.decimal(engSummary.avg_products_viewed_per_customer, 1) },
                                ].map(({ label, value }) => (
                                    <div key={label} className="text-center p-4 bg-gray-50 rounded-xl">
                                        <p className="text-2xl font-bold text-gray-900 mb-1">{value}</p>
                                        <p className="text-xs text-gray-500 uppercase tracking-wider">{label}</p>
                                    </div>
                                ))}
                            </div>
                        </div>
                    </Card>
                </section>
            )}
        </div>
    );
}
