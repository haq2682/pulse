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
// Aggregation helper — group campaign_performance_summary by campaign_type
// ---------------------------------------------------------------------------

function aggregateByType(rows) {
    const map = {};
    rows.forEach((r) => {
        const t = r.campaign_type ?? 'unknown';
        if (!map[t]) {
            map[t] = {
                channel: t,
                campaigns: 0,
                totalImpressions: 0,
                totalClicks: 0,
                totalOrders: 0,
                totalRevenue: 0,
                totalSpend: 0,
                totalBudget: 0,
                roasSum: 0, roasCount: 0,
                roiSum:  0, roiCount:  0,
                cvrSum:  0, cvrCount:  0,
                aovSum:  0, aovCount:  0,
                effSum:  0, effCount:  0,
            };
        }
        const e = map[t];
        e.campaigns       += 1;
        e.totalImpressions += +(r.impressions ?? 0);
        e.totalClicks      += +(r.clicks ?? 0);
        e.totalOrders      += +(r.orders_from_campaign ?? 0);
        e.totalRevenue     += +(r.revenue_generated ?? 0);
        e.totalSpend       += +(r.spent_amount ?? 0);
        e.totalBudget      += +(r.budget ?? 0);
        if (r.roas != null)                     { e.roasSum += +(r.roas); e.roasCount++; }
        if (r.roi  != null)                     { e.roiSum  += +(r.roi);  e.roiCount++;  }
        if (r.conversion_rate != null)          { e.cvrSum  += +(r.conversion_rate); e.cvrCount++; }
        if (r.avg_order_value != null)          { e.aovSum  += +(r.avg_order_value); e.aovCount++; }
        if (r.campaign_efficiency_score != null){ e.effSum  += +(r.campaign_efficiency_score); e.effCount++; }
    });

    return Object.values(map).map((e) => ({
        channel: e.channel,
        campaigns: e.campaigns,
        total_impressions: e.totalImpressions,
        total_clicks: e.totalClicks,
        total_orders: e.totalOrders,
        total_revenue: e.totalRevenue,
        total_spend: e.totalSpend,
        total_budget: e.totalBudget,
        avg_roas: e.roasCount > 0 ? e.roasSum / e.roasCount : null,
        avg_roi:  e.roiCount  > 0 ? e.roiSum  / e.roiCount  : null,
        avg_cvr:  e.cvrCount  > 0 ? e.cvrSum  / e.cvrCount  : null,
        avg_aov:  e.aovCount  > 0 ? e.aovSum  / e.aovCount  : null,
        avg_eff:  e.effCount  > 0 ? e.effSum  / e.effCount  : null,
        ctr: e.totalImpressions > 0 ? (e.totalClicks / e.totalImpressions) * 100 : 0,
    })).sort((a, b) => b.total_revenue - a.total_revenue);
}

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

export default function MarketingChannels() {
    const fmt = useFormatters();
    const { businessId } = useParams();
    const toastRef = useRef(null);
    const { pipelineStatus } = usePipelineProgress();
    const { dateRange, setDateRange, quickFilter, isFiltered, applyQuickFilter, resetFilters } = useAnalyticsDateFilter();
    const { lastUpdate } = useAnalyticsWebSocket(businessId);

    const [rawMarketing, setRawMarketing] = useState(null);
    const [loading, setLoading] = useState(true);
    const [fetchError, setFetchError] = useState(false);
    const [dataMode, setDataMode] = useState('unknown');

    // -------------------------------------------------------------------------
    // Fetch
    // -------------------------------------------------------------------------

    const buildUrl = useCallback(() => {
        const base = import.meta.env.VITE_API_URL || 'http://localhost:8000';
        const params = new URLSearchParams({ categories: 'marketing_analytics' });
        return `${base}/analytics/data/${businessId}?${params.toString()}`;
    }, [businessId]);

    const fetchData = useCallback(async () => {
        if (!businessId) return;
        try {
            setLoading(true);
            setFetchError(false);
            const res = await fetch(buildUrl());
            if (!res.ok) {
                setRawMarketing(null);
                return;
            }
            const json = await res.json();
            if (json.mode) setDataMode(json.mode);
            setRawMarketing(json.categories?.marketing_analytics ?? null);
        } catch {
            console.error('[MarketingChannels] fetch error');
            setFetchError(true);
            setRawMarketing(null);
        } finally {
            setLoading(false);
        }
    }, [buildUrl, businessId]);

    useEffect(() => { fetchData(); }, [businessId]); // eslint-disable-line
    useEffect(() => {
        if (!lastUpdate) return;
        fetchData();
        toastRef.current?.show({ severity: 'info', summary: 'Data Updated', detail: 'Analytics pipeline completed — refreshing data.', life: 3000 });
    }, [lastUpdate]); // eslint-disable-line

    // -------------------------------------------------------------------------
    // Derived data
    // -------------------------------------------------------------------------

    const derived = useMemo(() => {
        if (!rawMarketing) return null;
        const a = rawMarketing.analytics ?? {};

        const summary = a.campaign_performance_summary?.data ?? [];
        const perf    = a.campaign_performance?.data ?? [];

        if (summary.length === 0 && perf.length === 0) return null;

        // Aggregate by channel type using summary (richer base data)
        const channels = aggregateByType(summary);

        if (channels.length === 0) return null;

        const labels = channels.map((c) => c.channel);

        // ---- KPIs -----------------------------------------------------------
        const totalChannels   = channels.length;
        const totalImpressions = channels.reduce((s, c) => s + c.total_impressions, 0);
        const totalClicks      = channels.reduce((s, c) => s + c.total_clicks, 0);
        const bestROI          = channels.reduce(
            (best, c) => (!best || (c.avg_roi ?? -Infinity) > (best.avg_roi ?? -Infinity)) ? c : best,
            null,
        );

        // ---- Revenue by channel bar ----------------------------------------
        const revenueBarData = {
            labels,
            datasets: [{ label: 'Total Revenue ($)', data: channels.map((c) => +c.total_revenue.toFixed(2)), backgroundColor: PALETTE }],
        };

        // ---- ROAS by channel ------------------------------------------------
        const roasBarData = {
            labels,
            datasets: [{ label: 'Avg ROAS', data: channels.map((c) => +(c.avg_roas ?? 0).toFixed(2)), backgroundColor: 'rgba(34,197,94,0.82)' }],
        };

        // ---- ROI by channel -------------------------------------------------
        const roiBarData = {
            labels,
            datasets: [{ label: 'Avg ROI %', data: channels.map((c) => +(c.avg_roi ?? 0).toFixed(2)), backgroundColor: 'rgba(59,130,246,0.82)' }],
        };

        // ---- CTR by channel -------------------------------------------------
        const ctrBarData = {
            labels,
            datasets: [{ label: 'CTR %', data: channels.map((c) => +c.ctr.toFixed(2)), backgroundColor: 'rgba(249,115,22,0.82)' }],
        };

        // ---- Conversion rate by channel -------------------------------------
        const cvrBarData = {
            labels,
            datasets: [{ label: 'Avg Conv. Rate %', data: channels.map((c) => +(c.avg_cvr ?? 0).toFixed(2)), backgroundColor: 'rgba(139,92,246,0.82)' }],
        };

        // ---- Efficiency score by channel ------------------------------------
        const effBarData = {
            labels,
            datasets: [{ label: 'Avg Efficiency Score', data: channels.map((c) => +(c.avg_eff ?? 0).toFixed(2)), backgroundColor: 'rgba(6,182,212,0.82)' }],
        };

        // ---- Budget vs Spend grouped bar ------------------------------------
        const budgetVsSpendData = {
            labels,
            datasets: [
                { label: 'Total Budget ($)',  data: channels.map((c) => +c.total_budget.toFixed(2)),  backgroundColor: 'rgba(59,130,246,0.7)' },
                { label: 'Total Spend ($)',   data: channels.map((c) => +c.total_spend.toFixed(2)),   backgroundColor: 'rgba(249,115,22,0.7)' },
            ],
        };

        // ---- Impressions vs Clicks grouped bar --------------------------------
        const impVsClicksData = {
            labels,
            datasets: [
                { label: 'Impressions', data: channels.map((c) => c.total_impressions), backgroundColor: 'rgba(59,130,246,0.7)' },
                { label: 'Clicks',      data: channels.map((c) => c.total_clicks),      backgroundColor: 'rgba(34,197,94,0.7)' },
            ],
        };

        // ---- Revenue doughnut by channel ------------------------------------
        const revDoughnutData = {
            labels,
            datasets: [{ data: channels.map((c) => +c.total_revenue.toFixed(2)), backgroundColor: PALETTE }],
        };

        // ---- Orders doughnut by channel -------------------------------------
        const ordersDoughnutData = {
            labels,
            datasets: [{ data: channels.map((c) => c.total_orders), backgroundColor: PALETTE }],
        };

        return {
            kpis: { totalChannels, totalImpressions, totalClicks, bestROIChannel: bestROI?.channel ?? '—', bestROI: bestROI?.avg_roi ?? 0 },
            revenueBarData, roasBarData, roiBarData, ctrBarData, cvrBarData, effBarData,
            budgetVsSpendData, impVsClicksData, revDoughnutData, ordersDoughnutData,
            channelTable: channels,
        };
    }, [rawMarketing]);

    // -------------------------------------------------------------------------
    // Render states
    // -------------------------------------------------------------------------

    const hasData = derived !== null;

    if (loading && pipelineStatus !== 'loading') {
        return (
            <div className="flex flex-col items-center justify-center min-h-[400px] gap-4">
                <ProgressSpinner />
                <p className="text-gray-500 text-base">Loading marketing channels…</p>
            </div>
        );
    }

    if (fetchError) {
        return (
            <div className="p-6 min-h-[calc(100vh-120px)]">
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
                <div className="flex items-center justify-center min-h-[50vh]">
                    <div className="text-center">
                        <i className="pi pi-exclamation-circle text-5xl text-red-400 mb-3 block" />
                        <p className="text-gray-700 font-medium text-lg">Something went wrong</p>
                        <p className="text-gray-500 text-sm mt-1">Unable to load data. Please try again later.</p>
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
                    quickFilter={quickFilter}
                    dateRange={dateRange}
                    isFiltered={isFiltered}
                    onQuickFilter={applyQuickFilter}
                    onDateChange={setDateRange}
                    onReset={resetFilters}
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
    const { kpis, revenueBarData, roasBarData, roiBarData, ctrBarData, cvrBarData, effBarData,
            budgetVsSpendData, impVsClicksData, revDoughnutData, ordersDoughnutData, channelTable } = derived;

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
                    * All analytics on this page are static aggregates computed over all available data and do not change with the date filter.
                </p>
            )}

            {/* ── KPI Cards ──────────────────────────────────────────────── */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <KPICard icon="pi-sitemap"    iconBg="bg-blue-50"   iconColor="text-blue-600"   value={fmt.number(kpis.totalChannels)}    label="Channel Types" />
                <KPICard icon="pi-eye"        iconBg="bg-green-50"  iconColor="text-green-600"  value={fmt.short(kpis.totalImpressions)}  label="Total Impressions" />
                <KPICard icon="pi-link"       iconBg="bg-orange-50" iconColor="text-orange-600" value={fmt.short(kpis.totalClicks)}       label="Total Clicks" />
                <KPICard icon="pi-trophy"     iconBg="bg-purple-50" iconColor="text-purple-600" value={kpis.bestROIChannel}               label="Best ROI Channel" />
            </div>

            {/* ── Revenue & ROAS ─────────────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-blue-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Revenue & ROAS</h2>
                </div>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {revenueBarData.labels.length > 0 && (
                        <ChartWrapper title="Total Revenue by Channel" height={340}>
                            <Bar data={revenueBarData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                    {roasBarData.labels.length > 0 && (
                        <ChartWrapper title="Avg ROAS by Channel" height={340}>
                            <Bar data={roasBarData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                </div>
            </section>

            {/* ── ROI & CTR ──────────────────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-green-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">ROI & CTR</h2>
                </div>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {roiBarData.labels.length > 0 && (
                        <ChartWrapper title="Avg ROI % by Channel" height={340}>
                            <Bar data={roiBarData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                    {ctrBarData.labels.length > 0 && (
                        <ChartWrapper title="Click-Through Rate % by Channel" height={340}>
                            <Bar data={ctrBarData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                </div>
            </section>

            {/* ── Conversion & Efficiency ────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-purple-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Conversion & Efficiency</h2>
                </div>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {cvrBarData.labels.length > 0 && (
                        <ChartWrapper title="Avg Conversion Rate % by Channel" height={340}>
                            <Bar data={cvrBarData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                    {effBarData.labels.length > 0 && (
                        <ChartWrapper title="Avg Efficiency Score by Channel" height={340}>
                            <Bar data={effBarData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                </div>
            </section>

            {/* ── Budget & Impressions ───────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-orange-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Budget & Impressions</h2>
                </div>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {budgetVsSpendData.labels.length > 0 && (
                        <ChartWrapper title="Budget vs Total Spend by Channel" height={340}>
                            <Bar data={budgetVsSpendData} options={groupedBarOpts()} />
                        </ChartWrapper>
                    )}
                    {impVsClicksData.labels.length > 0 && (
                        <ChartWrapper title="Impressions vs Clicks by Channel" height={340}>
                            <Bar data={impVsClicksData} options={groupedBarOpts()} />
                        </ChartWrapper>
                    )}
                </div>
            </section>

            {/* ── Channel Distribution ───────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-cyan-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Channel Distribution</h2>
                </div>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                    {revDoughnutData.labels.length > 0 && (
                        <ChartWrapper title="Revenue Share by Channel" height={280}>
                            <Doughnut data={revDoughnutData} options={doughnutOpts()} />
                        </ChartWrapper>
                    )}
                    {ordersDoughnutData.labels.length > 0 && (
                        <ChartWrapper title="Orders Share by Channel" height={280}>
                            <Doughnut data={ordersDoughnutData} options={doughnutOpts()} />
                        </ChartWrapper>
                    )}
                </div>
            </section>

            {/* ── Channel Performance Table ──────────────────────────────── */}
            {channelTable.length > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">Channel Performance Summary</h3>
                        <DataTable value={channelTable} scrollable stripedRows
                            emptyMessage="No channel data" className="text-sm">
                            <Column field="channel"           header="Channel Type"   sortable />
                            <Column field="campaigns"         header="Campaigns"      sortable body={(r) => fmt.number(r.campaigns)} />
                            <Column field="total_impressions" header="Impressions"    sortable body={(r) => fmt.short(r.total_impressions)} />
                            <Column field="total_clicks"      header="Clicks"         sortable body={(r) => fmt.short(r.total_clicks)} />
                            <Column field="ctr"               header="CTR %"          sortable body={(r) => fmt.pct(r.ctr)} />
                            <Column field="total_orders"      header="Orders"         sortable body={(r) => fmt.number(r.total_orders)} />
                            <Column field="avg_cvr"           header="Conv. Rate %"   sortable body={(r) => r.avg_cvr != null ? fmt.pct(r.avg_cvr) : '—'} />
                            <Column field="total_revenue"     header="Revenue"        sortable body={(r) => fmt.currency(r.total_revenue)} />
                            <Column field="total_spend"       header="Spend"          sortable body={(r) => fmt.currency(r.total_spend)} />
                            <Column field="total_budget"      header="Budget"         sortable body={(r) => fmt.currency(r.total_budget)} />
                            <Column field="avg_roas"          header="Avg ROAS"       sortable body={(r) => r.avg_roas != null ? fmt.decimal(r.avg_roas) : '—'} />
                            <Column field="avg_roi"           header="Avg ROI %"      sortable body={(r) => r.avg_roi  != null ? fmt.pct(r.avg_roi)  : '—'} />
                            <Column field="avg_aov"           header="Avg AOV"        sortable body={(r) => r.avg_aov  != null ? fmt.currency(r.avg_aov) : '—'} />
                            <Column field="avg_eff"           header="Avg Eff. Score" sortable body={(r) => r.avg_eff  != null ? fmt.decimal(r.avg_eff)  : '—'} />
                        </DataTable>
                    </div>
                </Card>
            )}
        </div>
    );
}
