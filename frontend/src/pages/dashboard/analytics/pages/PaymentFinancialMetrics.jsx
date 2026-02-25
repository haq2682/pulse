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
    CategoryScale, LinearScale, BarElement, PointElement, LineElement,
    ArcElement, Title, Tooltip, Legend,
} from 'chart.js';
import { Bar, Line, Doughnut } from 'react-chartjs-2';
import { useAnalyticsWebSocket } from '../../../../hooks/useAnalyticsWebSocket';
import ChartWrapper from '../components/ChartWrapper';
import { usePipelineProgress } from '@/context/PipelineProgressContext';
import useAnalyticsDateFilter from '@/hooks/useAnalyticsDateFilter';
import DateFilterBar from '../components/DateFilterBar';
import { useFormatters } from '@/hooks/useFormatters';

ChartJS.register(
    CategoryScale, LinearScale, BarElement, PointElement, LineElement,
    ArcElement, Title, Tooltip, Legend,
);

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

const lineOpts = () => ({
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { display: true, position: 'top' }, title: { display: false } },
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

export default function PaymentFinancialMetrics() {
    const fmt = useFormatters();
    const { businessId } = useParams();
    const toastRef = useRef(null);
    const { pipelineStatus } = usePipelineProgress();
    const { dateRange, setDateRange, quickFilter, isFiltered, applyQuickFilter, resetFilters } = useAnalyticsDateFilter();
    const { lastUpdate } = useAnalyticsWebSocket(businessId);

    const [rawRevenue, setRawRevenue] = useState(null);
    const [loading, setLoading] = useState(true);
    const [fetchError, setFetchError] = useState(false);
    const [dataMode, setDataMode] = useState('unknown');

    // -------------------------------------------------------------------------
    // Fetch
    // -------------------------------------------------------------------------

    const buildUrl = useCallback(() => {
        const base = import.meta.env.VITE_API_URL || 'http://localhost:8000';
        const params = new URLSearchParams({ categories: 'revenue_analytics' });
        return `${base}/analytics/data/${businessId}?${params.toString()}`;
    }, [businessId]);

    const fetchData = useCallback(async () => {
        if (!businessId) return;
        try {
            setLoading(true);
            setFetchError(false);
            const res = await fetch(buildUrl());
            if (!res.ok) {
                setRawRevenue(null);
                return;
            }
            const json = await res.json();
            if (json.mode) setDataMode(json.mode);
            setRawRevenue(json.categories?.revenue_analytics ?? null);
        } catch {
            console.error('[PaymentFinancialMetrics] fetch error');
            setFetchError(true);
            setRawRevenue(null);
        } finally {
            setLoading(false);
        }
    }, [buildUrl, businessId]);

    useEffect(() => { fetchData(); }, [businessId]); // eslint-disable-line
    useEffect(() => {
        if (!lastUpdate) return;
        fetchData();
        toastRef.current?.show({ severity: 'info', summary: 'Data Updated', detail: 'Analytics pipeline completed — refreshing financial data.', life: 3000 });
    }, [lastUpdate]); // eslint-disable-line

    // -------------------------------------------------------------------------
    // Derived data
    // -------------------------------------------------------------------------

    const derived = useMemo(() => {
        if (!rawRevenue) return null;
        const a = rawRevenue.analytics ?? {};

        const aovMonthly      = a.aov_trend_monthly?.data              ?? [];
        const aovWeekly       = a.aov_trend_weekly?.data               ?? [];
        const aovDaily        = a.aov_trend_daily?.data                ?? [];
        const revBySegment    = a.rev_by_customer_segment?.data        ?? [];
        const revByRfm        = a.rev_by_rfm_segment?.data             ?? [];
        const revByLabel      = a.rev_by_segment_label?.data           ?? [];
        const revByDevice     = a.rev_by_device?.data                  ?? [];
        const revByReferrer   = a.rev_by_referrer?.data                ?? [];
        const revByCountryCity = a.rev_by_country_city?.data           ?? [];
        const lowMarginCats   = a.low_margin_categories?.data          ?? [];
        const segmentAov      = a.segment_aov_by_rfm?.data             ?? [];
        const carryingCost    = a.inventory_carrying_cost_overall?.data ?? [];

        // Check if we have any meaningful data
        if (
            aovMonthly.length === 0 &&
            revBySegment.length === 0 &&
            revByRfm.length === 0 &&
            lowMarginCats.length === 0
        ) return null;

        // ---- KPIs -----------------------------------------------------------
        const totalRevenue = revBySegment.reduce((s, r) => s + (+(r.segment_revenue ?? 0)), 0)
            || revByRfm.reduce((s, r) => s + (+(r.segment_revenue ?? 0)), 0);

        const latestMonthAov = aovMonthly.length > 0
            ? [...aovMonthly].sort((a, b) => {
                if ((a.order_year ?? 0) !== (b.order_year ?? 0)) return (+(b.order_year ?? 0)) - (+(a.order_year ?? 0));
                return (+(b.order_month ?? 0)) - (+(a.order_month ?? 0));
            })[0]?.avg_order_value ?? 0
            : 0;

        const topSegmentByRev = [...revByRfm].sort((a, b) => (+(b.segment_revenue ?? 0)) - (+(a.segment_revenue ?? 0)))[0];
        const lowMarginCount  = lowMarginCats.filter((r) => (+(r.avg_profit_margin ?? 100)) < 20).length;
        const carryingCostVal = carryingCost[0]?.total_inventory_carrying_cost ?? null;

        // ---- AOV trend monthly (line) -------------------------------------
        const aovMonthSorted = [...aovMonthly].sort((a, b) => {
            if ((a.order_year ?? 0) !== (b.order_year ?? 0)) return (+(a.order_year ?? 0)) - (+(b.order_year ?? 0));
            return (+(a.order_month ?? 0)) - (+(b.order_month ?? 0));
        });
        const aovLineData = aovMonthSorted.length > 0 ? {
            labels: aovMonthSorted.map((r) => r.year_month ?? `${r.order_year}-${String(r.order_month).padStart(2, '0')}`),
            datasets: [
                {
                    label: 'Avg Order Value ($)',
                    data: aovMonthSorted.map((r) => +(r.avg_order_value ?? 0).toFixed(2)),
                    borderColor: 'rgba(59,130,246,0.9)',
                    backgroundColor: 'rgba(59,130,246,0.15)',
                    tension: 0.3, fill: true,
                },
                {
                    label: 'Total Revenue ($k)',
                    data: aovMonthSorted.map((r) => +((+(r.total_revenue ?? 0)) / 1000).toFixed(2)),
                    borderColor: 'rgba(34,197,94,0.9)',
                    backgroundColor: 'rgba(34,197,94,0.15)',
                    tension: 0.3, fill: true,
                },
            ],
        } : null;

        // ---- Revenue by customer segment bar ----------------------------
        const revSegSorted = [...revBySegment].sort((a, b) => (+(b.segment_revenue ?? 0)) - (+(a.segment_revenue ?? 0)));
        const revBySegBarData = revSegSorted.length > 0 ? {
            labels: revSegSorted.map((r) => r.customer_segment ?? 'Unknown'),
            datasets: [{
                label: 'Revenue ($)',
                data: revSegSorted.map((r) => +(r.segment_revenue ?? 0).toFixed(2)),
                backgroundColor: PALETTE,
            }],
        } : null;

        // ---- Revenue by RFM segment bar ---------------------------------
        const revRfmSorted = [...revByRfm].sort((a, b) => (+(b.segment_revenue ?? 0)) - (+(a.segment_revenue ?? 0)));
        const revByRfmBarData = revRfmSorted.length > 0 ? {
            labels: revRfmSorted.map((r) => r.rfm_segment ?? 'Unknown'),
            datasets: [{
                label: 'Revenue ($)',
                data: revRfmSorted.map((r) => +(r.segment_revenue ?? 0).toFixed(2)),
                backgroundColor: PALETTE,
            }],
        } : null;

        // ---- Revenue share doughnut by RFM segment ----------------------
        const revRfmDoughnutData = revRfmSorted.length > 0 ? {
            labels: revRfmSorted.map((r) => r.rfm_segment ?? 'Unknown'),
            datasets: [{ data: revRfmSorted.map((r) => +(r.segment_revenue ?? 0).toFixed(2)), backgroundColor: PALETTE }],
        } : null;

        // ---- Revenue per customer by RFM segment -------------------------
        const revPerCustRfmData = revRfmSorted.length > 0 ? {
            labels: revRfmSorted.map((r) => r.rfm_segment ?? 'Unknown'),
            datasets: [{
                label: 'Revenue per Customer ($)',
                data: revRfmSorted.map((r) => +(r.revenue_per_customer ?? 0).toFixed(2)),
                backgroundColor: 'rgba(139,92,246,0.82)',
            }],
        } : null;

        // ---- AOV by RFM segment -----------------------------------------
        const aovRfmSorted = [...segmentAov].sort((a, b) => (+(b.avg_order_value_segment ?? 0)) - (+(a.avg_order_value_segment ?? 0)));
        const aovByRfmBarData = aovRfmSorted.length > 0 ? {
            labels: aovRfmSorted.map((r) => r.rfm_segment ?? 'Unknown'),
            datasets: [{
                label: 'Avg Order Value ($)',
                data: aovRfmSorted.map((r) => +(r.avg_order_value_segment ?? 0).toFixed(2)),
                backgroundColor: 'rgba(59,130,246,0.82)',
            }],
        } : null;

        // ---- Revenue by device bar ----------------------------------------
        const revDevSorted = [...revByDevice].sort((a, b) => (+(b.segment_revenue ?? 0)) - (+(a.segment_revenue ?? 0)));
        const revByDeviceBarData = revDevSorted.length > 0 ? {
            labels: revDevSorted.map((r) => r.preferred_device_type ?? 'Unknown'),
            datasets: [{
                label: 'Revenue ($)',
                data: revDevSorted.map((r) => +(r.segment_revenue ?? 0).toFixed(2)),
                backgroundColor: PALETTE,
            }],
        } : null;

        // ---- Revenue by referrer bar -------------------------------------
        const revRefSorted = [...revByReferrer].sort((a, b) => (+(b.segment_revenue ?? 0)) - (+(a.segment_revenue ?? 0)));
        const revByReferrerBarData = revRefSorted.length > 0 ? {
            labels: revRefSorted.map((r) => r.preferred_referrer_source ?? 'Unknown'),
            datasets: [{
                label: 'Revenue ($)',
                data: revRefSorted.map((r) => +(r.segment_revenue ?? 0).toFixed(2)),
                backgroundColor: PALETTE,
            }],
        } : null;

        // ---- Low margin categories bar -----------------------------------
        const lowMarginSorted = [...lowMarginCats].sort((a, b) => (+(a.avg_profit_margin ?? 0)) - (+(b.avg_profit_margin ?? 0)));
        const lowMarginBarData = lowMarginSorted.length > 0 ? {
            labels: lowMarginSorted.map((r) => r.category ?? 'Unknown'),
            datasets: [{
                label: 'Avg Profit Margin %',
                data: lowMarginSorted.map((r) => +(r.avg_profit_margin ?? 0).toFixed(2)),
                backgroundColor: lowMarginSorted.map((r) => {
                    const m = +(r.avg_profit_margin ?? 0);
                    if (m < 10) return 'rgba(239,68,68,0.82)';
                    if (m < 20) return 'rgba(234,179,8,0.82)';
                    return 'rgba(34,197,94,0.82)';
                }),
            }],
        } : null;

        // ---- Segment revenue grouped (label variants) -------------------
        const revLabelSorted = [...revByLabel].sort((a, b) => (+(b.segment_revenue ?? 0)) - (+(a.segment_revenue ?? 0)));
        const revByLabelBarData = revLabelSorted.length > 0 ? {
            labels: revLabelSorted.map((r) => r.customer_segment_label ?? 'Unknown'),
            datasets: [{
                label: 'Revenue ($)',
                data: revLabelSorted.map((r) => +(r.segment_revenue ?? 0).toFixed(2)),
                backgroundColor: PALETTE,
            }],
        } : null;

        return {
            kpis: { totalRevenue, latestMonthAov, topSegmentByRev, lowMarginCount, carryingCostVal },
            aovLineData, revBySegBarData, revByRfmBarData, revRfmDoughnutData, revPerCustRfmData,
            aovByRfmBarData, revByDeviceBarData, revByReferrerBarData, lowMarginBarData, revByLabelBarData,
            revByRfm, lowMarginCats, aovRfmSorted,
            aovWeekly, aovDaily, revByCountryCity,
        };
    }, [rawRevenue]);

    const hasData = derived !== null;

    // -------------------------------------------------------------------------
    // Render states
    // -------------------------------------------------------------------------

    if (loading && pipelineStatus !== 'loading') {
        return (
            <div className="flex flex-col items-center justify-center min-h-[400px] gap-4">
                <ProgressSpinner />
                <p className="text-gray-500 text-base">Loading financial metrics…</p>
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
                        <p className="text-gray-500 text-sm mt-1">Unable to load financial data. Please try again later.</p>
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
                            : 'No financial data to display.'}
                    </p>
                </div>
            </div>
        );
    }

    if (!derived) return null;

    const {
        kpis, aovLineData, revBySegBarData, revByRfmBarData, revRfmDoughnutData, revPerCustRfmData,
        aovByRfmBarData, revByDeviceBarData, revByReferrerBarData, lowMarginBarData, revByLabelBarData,
        revByRfm, lowMarginCats, aovRfmSorted,
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
                    * Financial metrics are static aggregates computed over all available data and do not change with the date filter.
                    AOV trend charts reflect the full historical period.
                </p>
            )}

            {/* ── KPI Cards ──────────────────────────────────────────────── */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <KPICard
                    icon="pi-dollar" iconBg="bg-green-100" iconColor="text-green-600"
                    value={fmt.currencyShort(kpis.totalRevenue)}
                    label="Total Revenue"
                />
                <KPICard
                    icon="pi-chart-line" iconBg="bg-blue-100" iconColor="text-blue-600"
                    value={fmt.currency(kpis.latestMonthAov)}
                    label="Latest Month AOV"
                />
                <KPICard
                    icon="pi-star" iconBg="bg-purple-100" iconColor="text-purple-600"
                    value={kpis.topSegmentByRev?.rfm_segment ?? '—'}
                    label="Top Revenue Segment"
                />
                <KPICard
                    icon="pi-exclamation-triangle" iconBg="bg-amber-100" iconColor="text-amber-600"
                    value={fmt.number(kpis.lowMarginCount)}
                    label="Low-Margin Categories (<20%)"
                />
            </div>

            {/* Inventory carrying cost callout (if available) */}
            {kpis.carryingCostVal != null && (
                <div className="flex items-center gap-3 p-4 bg-amber-50 border border-amber-200 rounded-xl">
                    <i className="pi pi-warehouse text-amber-600 text-2xl" />
                    <div>
                        <p className="text-sm font-semibold text-amber-800">Inventory Carrying Cost</p>
                        <p className="text-xs text-amber-600">Total: {fmt.currency(kpis.carryingCostVal)}</p>
                    </div>
                </div>
            )}

            {/* ── AOV Trend ─────────────────────────────────────────────── */}
            {aovLineData && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-blue-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Average Order Value Trend</h2>
                    </div>
                    <ChartWrapper title="Monthly AOV & Revenue Trend" height={360}>
                        <Line data={aovLineData} options={lineOpts()} />
                    </ChartWrapper>
                </section>
            )}

            {/* ── Revenue by Segment ────────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-purple-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Revenue by Customer Segment</h2>
                </div>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {revByRfmBarData && (
                        <ChartWrapper title="Revenue by RFM Segment" height={340}>
                            <Bar data={revByRfmBarData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                    {revRfmDoughnutData && (
                        <ChartWrapper title="Revenue Share by RFM Segment" height={280}>
                            <Doughnut data={revRfmDoughnutData} options={doughnutOpts()} />
                        </ChartWrapper>
                    )}
                    {revBySegBarData && (
                        <ChartWrapper title="Revenue by Customer Segment" height={340}>
                            <Bar data={revBySegBarData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                    {revPerCustRfmData && (
                        <ChartWrapper title="Revenue per Customer by RFM Segment" height={340}>
                            <Bar data={revPerCustRfmData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                    {revByLabelBarData && (
                        <ChartWrapper title="Revenue by Segment Label" height={340}>
                            <Bar data={revByLabelBarData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                    {aovByRfmBarData && (
                        <ChartWrapper title="Avg Order Value by RFM Segment" height={340}>
                            <Bar data={aovByRfmBarData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                </div>
            </section>

            {/* ── Revenue by Channel ────────────────────────────────────── */}
            {(revByDeviceBarData || revByReferrerBarData) && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-cyan-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Revenue by Channel</h2>
                    </div>
                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                        {revByDeviceBarData && (
                            <ChartWrapper title="Revenue by Device Type" height={320}>
                                <Bar data={revByDeviceBarData} options={barOpts()} />
                            </ChartWrapper>
                        )}
                        {revByReferrerBarData && (
                            <ChartWrapper title="Revenue by Referrer Source" height={320}>
                                <Bar data={revByReferrerBarData} options={barOpts(true)} />
                            </ChartWrapper>
                        )}
                    </div>
                </section>
            )}

            {/* ── Margin Analysis ────────────────────────────────────────── */}
            {lowMarginBarData && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-red-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Margin Analysis</h2>
                    </div>
                    <ChartWrapper title="Avg Profit Margin % by Category (Low-Margin Alert)" height={340}>
                        <Bar data={lowMarginBarData} options={barOpts()} />
                    </ChartWrapper>
                </section>
            )}

            {/* ── Summary Tables ─────────────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-gray-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Summary Tables</h2>
                </div>

                {/* Revenue by RFM Segment Table */}
                {revByRfm.length > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                Revenue by RFM Segment
                            </h3>
                            <DataTable value={[...revByRfm].sort((a, b) => (+(b.segment_revenue ?? 0)) - (+(a.segment_revenue ?? 0)))}
                                scrollable stripedRows emptyMessage="No data" className="text-sm">
                                <Column field="rfm_segment"           header="RFM Segment"         sortable />
                                <Column field="customer_count"        header="Customers"            sortable body={(r) => fmt.number(r.customer_count)} />
                                <Column field="segment_revenue"       header="Revenue"              sortable body={(r) => fmt.currency(r.segment_revenue)} />
                                <Column field="revenue_per_customer"  header="Rev per Customer"     sortable body={(r) => fmt.currency(r.revenue_per_customer)} />
                                <Column field="revenue_share"         header="Revenue Share"        sortable body={(r) => fmt.pct(r.revenue_share)} />
                            </DataTable>
                        </div>
                    </Card>
                )}

                {/* AOV by RFM Segment Table */}
                {aovRfmSorted.length > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                AOV & Revenue by RFM Segment
                            </h3>
                            <DataTable value={aovRfmSorted} scrollable stripedRows emptyMessage="No data" className="text-sm">
                                <Column field="rfm_segment"            header="RFM Segment"         sortable />
                                <Column field="orders"                 header="Orders"              sortable body={(r) => fmt.number(r.orders)} />
                                <Column field="unique_customers"       header="Customers"           sortable body={(r) => fmt.number(r.unique_customers)} />
                                <Column field="total_revenue"          header="Total Revenue"       sortable body={(r) => fmt.currency(r.total_revenue)} />
                                <Column field="avg_order_value_segment" header="Avg Order Value"    sortable body={(r) => fmt.currency(r.avg_order_value_segment)} />
                            </DataTable>
                        </div>
                    </Card>
                )}

                {/* Low Margin Categories Table */}
                {lowMarginCats.length > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                Category Margin Analysis
                            </h3>
                            <DataTable value={[...lowMarginCats].sort((a, b) => (+(a.avg_profit_margin ?? 0)) - (+(b.avg_profit_margin ?? 0)))}
                                scrollable stripedRows emptyMessage="No data" className="text-sm">
                                <Column field="category"              header="Category"            sortable />
                                <Column field="units_sold"            header="Units Sold"          sortable body={(r) => fmt.number(r.units_sold)} />
                                <Column field="total_category_revenue" header="Revenue"            sortable body={(r) => fmt.currency(r.total_category_revenue)} />
                                <Column field="total_category_profit" header="Profit"              sortable body={(r) => fmt.currency(r.total_category_profit)} />
                                <Column field="avg_profit_margin"     header="Avg Margin"          sortable body={(r) => (
                                    <Tag value={fmt.pct(r.avg_profit_margin)}
                                        severity={(+(r.avg_profit_margin ?? 0)) < 10 ? 'danger' : (+(r.avg_profit_margin ?? 0)) < 20 ? 'warning' : 'success'} />
                                )} />
                            </DataTable>
                        </div>
                    </Card>
                )}

                {/* AOV Weekly Trend */}
                {(derived?.aovWeekly?.length ?? 0) > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                AOV Trend (Weekly)
                            </h3>
                            <div className="h-[260px]">
                                <Line
                                    data={{
                                        labels: derived.aovWeekly.map((r) => `${r.order_year}-W${String(r.order_week ?? 0).padStart(2,'0')}`),
                                        datasets: [{
                                            label: 'Avg Order Value',
                                            data: derived.aovWeekly.map((r) => +(r.avg_order_value ?? 0)),
                                            borderColor: 'rgb(139,92,246)',
                                            backgroundColor: 'rgba(139,92,246,0.15)',
                                            tension: 0.4, fill: true,
                                        }],
                                    }}
                                    options={{ responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { y: { beginAtZero: false, ticks: { callback: (v) => '$' + v.toLocaleString() } } } }}
                                />
                            </div>
                        </div>
                    </Card>
                )}

                {/* AOV Daily Trend */}
                {(derived?.aovDaily?.length ?? 0) > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                AOV Trend (Daily)
                            </h3>
                            <div className="h-[260px]">
                                <Line
                                    data={{
                                        labels: derived.aovDaily.map((r) => r.order_date ?? ''),
                                        datasets: [{
                                            label: 'Avg Order Value',
                                            data: derived.aovDaily.map((r) => +(r.avg_order_value ?? 0)),
                                            borderColor: 'rgb(6,182,212)',
                                            backgroundColor: 'rgba(6,182,212,0.15)',
                                            tension: 0.4, fill: true,
                                            pointRadius: 2,
                                        }],
                                    }}
                                    options={{ responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { y: { beginAtZero: false, ticks: { callback: (v) => '$' + v.toLocaleString() } } } }}
                                />
                            </div>
                        </div>
                    </Card>
                )}

                {/* Revenue by Country & City Table */}
                {(derived?.revByCountryCity?.length ?? 0) > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                Revenue by Country &amp; City
                            </h3>
                            <DataTable
                                value={[...derived.revByCountryCity].sort((a, b) => (+(b.segment_revenue ?? 0)) - (+(a.segment_revenue ?? 0)))}
                                paginator rows={10} stripedRows emptyMessage="No data" className="text-sm"
                            >
                                <Column field="country" header="Country" sortable />
                                <Column field="city" header="City" sortable />
                                <Column field="customer_count" header="Customers" sortable body={(r) => fmt.number(r.customer_count)} />
                                <Column field="segment_revenue" header="Revenue" sortable body={(r) => fmt.currency(r.segment_revenue)} />
                                <Column field="revenue_per_customer" header="Rev / Customer" sortable body={(r) => fmt.currency(r.revenue_per_customer)} />
                                <Column field="revenue_share" header="Revenue Share" sortable body={(r) => fmt.pct(r.revenue_share)} />
                            </DataTable>
                        </div>
                    </Card>
                )}
            </section>
        </div>
    );
}
