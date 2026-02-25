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

const STATUS_SEVERITY = { active: 'success', completed: 'secondary', paused: 'warning', cancelled: 'danger' };
const TIER_SEVERITY   = { top: 'success', mid: 'info', low: 'warning', poor: 'danger' };

const camLabel = (r) => r.campaign_name || `Campaign ${r.campaign_id}`;

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

const barOpts = (title, horizontal = false) => ({
    indexAxis: horizontal ? 'y' : 'x',
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { display: false }, title: { display: !!title, text: title } },
    scales: {
        x: { grid: { color: 'rgba(0,0,0,0.05)' } },
        y: { grid: { color: 'rgba(0,0,0,0.05)' } },
    },
});

const groupedBarOpts = (title) => ({
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { position: 'top' }, title: { display: !!title, text: title } },
    scales: {
        x: { grid: { color: 'rgba(0,0,0,0.05)' } },
        y: { grid: { color: 'rgba(0,0,0,0.05)' } },
    },
});

const doughnutOpts = (title) => ({
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
        legend: { position: 'right' },
        title: { display: !!title, text: title },
    },
});

// ---------------------------------------------------------------------------
// Main Component
// ---------------------------------------------------------------------------

export default function MarketingCampaigns() {
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
            console.error('[MarketingCampaigns] fetch error');
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

        const summary   = a.campaign_performance_summary?.data ?? [];
        const perf      = a.campaign_performance?.data ?? [];
        const wasteful  = a.campaign_wasteful_campaigns?.data ?? [];

        if (summary.length === 0 && perf.length === 0) return null;

        // ---- KPIs -----------------------------------------------------------
        const totalCampaigns = summary.length;
        const totalSpend     = summary.reduce((s, r) => s + (+(r.spent_amount ?? 0)), 0);
        const totalRevenue   = summary.reduce((s, r) => s + (+(r.revenue_generated ?? 0)), 0);
        const avgROI         = totalCampaigns > 0
            ? summary.reduce((s, r) => s + (+(r.roi ?? 0)), 0) / totalCampaigns
            : 0;
        const avgROAS        = totalCampaigns > 0
            ? summary.reduce((s, r) => s + (+(r.roas ?? 0)), 0) / totalCampaigns
            : 0;

        // ---- Revenue by campaign (top 12) ------------------------------------
        const revSorted = [...summary]
            .sort((a, b) => (+(b.revenue_generated ?? 0)) - (+(a.revenue_generated ?? 0)))
            .slice(0, 12);
        const revenueBarData = {
            labels: revSorted.map(camLabel),
            datasets: [{ label: 'Revenue ($)', data: revSorted.map((r) => +(r.revenue_generated ?? 0).toFixed(2)), backgroundColor: PALETTE }],
        };

        // ---- ROAS bar (from campaign_performance, top 12) -------------------
        const roasSorted = [...perf]
            .sort((a, b) => (+(b.roas_effective ?? 0)) - (+(a.roas_effective ?? 0)))
            .slice(0, 12);
        const roasBarData = {
            labels: roasSorted.map(camLabel),
            datasets: [{ label: 'ROAS (effective)', data: roasSorted.map((r) => +(r.roas_effective ?? 0).toFixed(2)), backgroundColor: 'rgba(34,197,94,0.82)' }],
        };

        // ---- ROI bar (top 12) -----------------------------------------------
        const roiSorted = [...perf]
            .sort((a, b) => (+(b.roi_effective ?? 0)) - (+(a.roi_effective ?? 0)))
            .slice(0, 12);
        const roiBarData = {
            labels: roiSorted.map(camLabel),
            datasets: [{ label: 'ROI % (effective)', data: roiSorted.map((r) => +(r.roi_effective ?? 0).toFixed(2)), backgroundColor: 'rgba(59,130,246,0.82)' }],
        };

        // ---- Budget vs Spent grouped bar (top 10 by budget) -----------------
        const budgetSorted = [...perf]
            .sort((a, b) => (+(b.budget ?? 0)) - (+(a.budget ?? 0)))
            .slice(0, 10);
        const budgetVsSpentData = {
            labels: budgetSorted.map(camLabel),
            datasets: [
                { label: 'Budget ($)', data: budgetSorted.map((r) => +(r.budget ?? 0).toFixed(2)), backgroundColor: 'rgba(59,130,246,0.7)' },
                { label: 'Spent ($)',  data: budgetSorted.map((r) => +(r.spent_amount ?? 0).toFixed(2)), backgroundColor: 'rgba(249,115,22,0.7)' },
            ],
        };

        // ---- Conversion rate bar (top 12) -----------------------------------
        const convSorted = [...perf]
            .sort((a, b) => (+(b.conversion_rate_effective ?? 0)) - (+(a.conversion_rate_effective ?? 0)))
            .slice(0, 12);
        const convBarData = {
            labels: convSorted.map(camLabel),
            datasets: [{ label: 'Conv. Rate %', data: convSorted.map((r) => +(r.conversion_rate_effective ?? 0).toFixed(2)), backgroundColor: 'rgba(139,92,246,0.82)' }],
        };

        // ---- Efficiency score bar (top 12) ----------------------------------
        const effSorted = [...summary]
            .filter((r) => r.campaign_efficiency_score != null)
            .sort((a, b) => (+(b.campaign_efficiency_score ?? 0)) - (+(a.campaign_efficiency_score ?? 0)))
            .slice(0, 12);
        const effBarData = {
            labels: effSorted.map(camLabel),
            datasets: [{ label: 'Efficiency Score', data: effSorted.map((r) => +(r.campaign_efficiency_score ?? 0).toFixed(2)), backgroundColor: 'rgba(6,182,212,0.82)' }],
        };

        // ---- Status doughnut ------------------------------------------------
        const statusCounts = summary.reduce((acc, r) => {
            const s = (r.campaign_status ?? 'unknown').toLowerCase();
            acc[s] = (acc[s] ?? 0) + 1;
            return acc;
        }, {});
        const statusDoughnutData = {
            labels: Object.keys(statusCounts),
            datasets: [{ data: Object.values(statusCounts), backgroundColor: PALETTE }],
        };

        // ---- Campaign type doughnut -----------------------------------------
        const typeCounts = summary.reduce((acc, r) => {
            const t = r.campaign_type ?? 'unknown';
            acc[t] = (acc[t] ?? 0) + 1;
            return acc;
        }, {});
        const typeDoughnutData = {
            labels: Object.keys(typeCounts),
            datasets: [{ data: Object.values(typeCounts), backgroundColor: PALETTE }],
        };

        // ---- Performance tier doughnut (from campaign_performance) ----------
        const tierCounts = perf.reduce((acc, r) => {
            const t = r.performance_tier ?? 'unknown';
            acc[t] = (acc[t] ?? 0) + 1;
            return acc;
        }, {});
        const tierDoughnutData = {
            labels: Object.keys(tierCounts),
            datasets: [{ data: Object.values(tierCounts), backgroundColor: PALETTE }],
        };

        return {
            kpis: { totalCampaigns, totalSpend, totalRevenue, avgROI, avgROAS },
            revenueBarData, roasBarData, roiBarData, budgetVsSpentData, convBarData, effBarData,
            statusDoughnutData, typeDoughnutData, tierDoughnutData,
            perfTable: perf,
            wastefulTable: wasteful,
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
                <p className="text-gray-500 text-base">Loading marketing campaigns…</p>
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
    const { kpis, revenueBarData, roasBarData, roiBarData, budgetVsSpentData, convBarData, effBarData,
            statusDoughnutData, typeDoughnutData, tierDoughnutData,
            perfTable, wastefulTable } = derived;

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
                <KPICard icon="pi-megaphone"    iconBg="bg-blue-50"   iconColor="text-blue-600"   value={fmt.number(kpis.totalCampaigns)} label="Total Campaigns" />
                <KPICard icon="pi-dollar"       iconBg="bg-red-50"    iconColor="text-red-600"    value={fmt.currency(kpis.totalSpend)}   label="Total Spend" />
                <KPICard icon="pi-chart-line"   iconBg="bg-green-50"  iconColor="text-green-600"  value={fmt.currency(kpis.totalRevenue)} label="Total Revenue" />
                <KPICard icon="pi-percentage"   iconBg="bg-purple-50" iconColor="text-purple-600" value={`${fmt.decimal(kpis.avgROI)}%`}  label="Avg ROI" />
            </div>

            {/* ── Revenue & Budget ───────────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-blue-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Revenue & Budget</h2>
                </div>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {revenueBarData.labels.length > 0 && (
                        <ChartWrapper title="Revenue by Campaign (Top 12)" height={340}>
                            <Bar data={revenueBarData} options={barOpts('', true)} />
                        </ChartWrapper>
                    )}
                    {budgetVsSpentData.labels.length > 0 && (
                        <ChartWrapper title="Budget vs Spent (Top 10)" height={340}>
                            <Bar data={budgetVsSpentData} options={groupedBarOpts('')} />
                        </ChartWrapper>
                    )}
                </div>
            </section>

            {/* ── ROAS & ROI ─────────────────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-green-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">ROAS & ROI</h2>
                </div>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {roasBarData.labels.length > 0 && (
                        <ChartWrapper title="Effective ROAS by Campaign (Top 12)" height={340}>
                            <Bar data={roasBarData} options={barOpts('', true)} />
                        </ChartWrapper>
                    )}
                    {roiBarData.labels.length > 0 && (
                        <ChartWrapper title="Effective ROI % by Campaign (Top 12)" height={340}>
                            <Bar data={roiBarData} options={barOpts('', true)} />
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
                    {convBarData.labels.length > 0 && (
                        <ChartWrapper title="Conversion Rate % (Top 12)" height={340}>
                            <Bar data={convBarData} options={barOpts('', true)} />
                        </ChartWrapper>
                    )}
                    {effBarData.labels.length > 0 && (
                        <ChartWrapper title="Efficiency Score (Top 12)" height={340}>
                            <Bar data={effBarData} options={barOpts('', true)} />
                        </ChartWrapper>
                    )}
                </div>
            </section>

            {/* ── Campaign Distribution ──────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-orange-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Campaign Distribution</h2>
                </div>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                    {statusDoughnutData.labels.length > 0 && (
                        <ChartWrapper title="Campaign Status" height={280}>
                            <Doughnut data={statusDoughnutData} options={doughnutOpts('')} />
                        </ChartWrapper>
                    )}
                    {typeDoughnutData.labels.length > 0 && (
                        <ChartWrapper title="Campaign Types" height={280}>
                            <Doughnut data={typeDoughnutData} options={doughnutOpts('')} />
                        </ChartWrapper>
                    )}
                    {tierDoughnutData.labels.length > 0 && (
                        <ChartWrapper title="Performance Tiers" height={280}>
                            <Doughnut data={tierDoughnutData} options={doughnutOpts('')} />
                        </ChartWrapper>
                    )}
                </div>
            </section>

            {/* ── Performance Tables ─────────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-cyan-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Performance Tables</h2>
                </div>

                {/* Campaign Performance Table */}
                {perfTable.length > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">Campaign Performance Summary</h3>
                            <DataTable value={perfTable} paginator rows={10} scrollable stripedRows
                                emptyMessage="No campaign data" className="text-sm">
                                <Column field="campaign_name"    header="Campaign"   sortable body={(r) => r.campaign_name || `Campaign ${r.campaign_id}`} />
                                <Column field="campaign_type"    header="Type"       sortable />
                                <Column field="campaign_status"  header="Status"     sortable body={(r) => (
                                    <Tag value={r.campaign_status ?? '—'} severity={STATUS_SEVERITY[(r.campaign_status ?? '').toLowerCase()] ?? 'secondary'} />
                                )} />
                                <Column field="performance_tier" header="Tier"       sortable body={(r) => (
                                    <Tag value={r.performance_tier ?? '—'} severity={TIER_SEVERITY[(r.performance_tier ?? '').toLowerCase()] ?? 'secondary'} />
                                )} />
                                <Column field="budget"           header="Budget"     sortable body={(r) => fmt.currency(r.budget)} />
                                <Column field="spent_amount"     header="Spent"      sortable body={(r) => fmt.currency(r.spent_amount)} />
                                <Column field="revenue_generated" header="Revenue"   sortable body={(r) => fmt.currency(r.revenue_generated)} />
                                <Column field="roas_effective"   header="ROAS"       sortable body={(r) => fmt.decimal(r.roas_effective)} />
                                <Column field="roi_effective"    header="ROI %"      sortable body={(r) => fmt.pct(r.roi_effective)} />
                                <Column field="ctr_effective"    header="CTR %"      sortable body={(r) => fmt.pct(r.ctr_effective)} />
                                <Column field="conversion_rate_effective" header="Conv. Rate %" sortable body={(r) => fmt.pct(r.conversion_rate_effective)} />
                                <Column field="avg_order_value"  header="AOV"        sortable body={(r) => fmt.currency(r.avg_order_value)} />
                            </DataTable>
                        </div>
                    </Card>
                )}

                {/* Wasteful Campaigns Table */}
                {wastefulTable.length > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-2 pb-3 border-b border-gray-200">Wasteful / Low-Efficiency Campaigns</h3>
                            <p className="text-xs text-gray-400 mb-4">Campaigns flagged as having low efficiency scores or poor returns.</p>
                            <DataTable value={wastefulTable} paginator rows={10} scrollable stripedRows
                                emptyMessage="No wasteful campaigns found" className="text-sm">
                                <Column field="campaign_name"            header="Campaign"    sortable body={(r) => r.campaign_name || `Campaign ${r.campaign_id}`} />
                                <Column field="campaign_type"            header="Type"        sortable />
                                <Column field="campaign_status"          header="Status"      sortable body={(r) => (
                                    <Tag value={r.campaign_status ?? '—'} severity={STATUS_SEVERITY[(r.campaign_status ?? '').toLowerCase()] ?? 'secondary'} />
                                )} />
                                <Column field="spent_amount"             header="Spent"       sortable body={(r) => fmt.currency(r.spent_amount)} />
                                <Column field="revenue_generated"        header="Revenue"     sortable body={(r) => fmt.currency(r.revenue_generated)} />
                                <Column field="roas"                     header="ROAS"        sortable body={(r) => fmt.decimal(r.roas)} />
                                <Column field="roi"                      header="ROI %"       sortable body={(r) => fmt.pct(r.roi)} />
                                <Column field="campaign_efficiency_score" header="Eff. Score" sortable body={(r) => fmt.decimal(r.campaign_efficiency_score)} />
                                <Column field="revenue_per_click_recalc" header="Rev/Click"  sortable body={(r) => fmt.currency(r.revenue_per_click_recalc)} />
                                <Column field="is_low_efficiency"        header="Low Eff."   sortable body={(r) => (
                                    <Tag value={r.is_low_efficiency ? 'Yes' : 'No'} severity={r.is_low_efficiency ? 'danger' : 'success'} />
                                )} />
                            </DataTable>
                        </div>
                    </Card>
                )}
            </section>
        </div>
    );
}
