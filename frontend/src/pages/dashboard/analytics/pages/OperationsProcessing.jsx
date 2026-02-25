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

const DOW_ORDER = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'];

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

export default function OperationsProcessing() {
    const fmt = useFormatters();
    const { businessId } = useParams();
    const toastRef = useRef(null);
    const { pipelineStatus } = usePipelineProgress();
    const { dateRange, setDateRange, quickFilter, isFiltered, applyQuickFilter, resetFilters } = useAnalyticsDateFilter();
    const { lastUpdate } = useAnalyticsWebSocket(businessId);

    const [rawOps, setRawOps] = useState(null);
    const [loading, setLoading] = useState(true);
    const [fetchError, setFetchError] = useState(false);
    const [dataMode, setDataMode] = useState('unknown');

    // -------------------------------------------------------------------------
    // Fetch
    // -------------------------------------------------------------------------

    const buildUrl = useCallback(() => {
        const base = import.meta.env.VITE_API_URL || 'http://localhost:8000';
        const params = new URLSearchParams({ categories: 'operations_analytics' });
        return `${base}/analytics/data/${businessId}?${params.toString()}`;
    }, [businessId]);

    const fetchData = useCallback(async () => {
        if (!businessId) return;
        try {
            setLoading(true);
            setFetchError(false);
            const res = await fetch(buildUrl());
            if (!res.ok) {
                setRawOps(null);
                return;
            }
            const json = await res.json();
            if (json.mode) setDataMode(json.mode);
            setRawOps(json.categories?.operations_analytics ?? null);
        } catch {
            console.error('[OperationsProcessing] fetch error');
            setFetchError(true);
            setRawOps(null);
        } finally {
            setLoading(false);
        }
    }, [buildUrl, businessId]);

    useEffect(() => { fetchData(); }, [businessId]); // eslint-disable-line
    useEffect(() => {
        if (!lastUpdate) return;
        fetchData();
        toastRef.current?.show({ severity: 'info', summary: 'Data Updated', detail: 'Analytics pipeline completed — refreshing processing data.', life: 3000 });
    }, [lastUpdate]); // eslint-disable-line

    // -------------------------------------------------------------------------
    // Derived data
    // -------------------------------------------------------------------------

    const derived = useMemo(() => {
        if (!rawOps) return null;
        const a = rawOps.analytics ?? {};

        const byCategory    = a.processing_by_category?.data          ?? [];
        const bySubcategory = a.processing_by_subcategory?.data       ?? [];
        const byHour        = a.processing_by_hour?.data              ?? [];
        const byDow         = a.processing_by_day_of_week?.data       ?? [];
        const weekendVsWeekday = a.weekend_vs_weekday?.data           ?? [];
        const bySeason      = a.processing_by_season?.data            ?? [];
        const bySeasonStatus = a.processing_by_season_and_status?.data ?? [];

        if (byCategory.length === 0 && byDow.length === 0 && bySeason.length === 0) return null;

        // ---- KPIs -----------------------------------------------------------
        const totalOrders     = byCategory.reduce((s, r) => s + (+(r.orders ?? 0)), 0);
        const avgProcDays     = byCategory.length > 0
            ? byCategory.reduce((s, r) => s + (+(r.avg_processing_days ?? 0)), 0) / byCategory.length
            : 0;
        const avgDelDays      = byCategory.length > 0
            ? byCategory.reduce((s, r) => s + (+(r.avg_delivery_days ?? 0)), 0) / byCategory.length
            : 0;
        const fastestCategory = [...byCategory].sort((a, b) => (+(a.avg_processing_days ?? 99)) - (+(b.avg_processing_days ?? 99)))[0];
        const slowestCategory = [...byCategory].sort((a, b) => (+(b.avg_processing_days ?? 0)) - (+(a.avg_processing_days ?? 0)))[0];

        // ---- Avg processing days by category (horizontal bar) ---------------
        const catProcSorted = [...byCategory].sort((a, b) => (+(a.avg_processing_days ?? 0)) - (+(b.avg_processing_days ?? 0)));
        const catProcBarData = {
            labels: catProcSorted.map((r) => r.category ?? 'Unknown'),
            datasets: [{
                label: 'Avg Processing Days',
                data: catProcSorted.map((r) => +(r.avg_processing_days ?? 0).toFixed(2)),
                backgroundColor: PALETTE,
            }],
        };

        // ---- Avg delivery days by category ----------------------------------
        const catDelBarData = {
            labels: catProcSorted.map((r) => r.category ?? 'Unknown'),
            datasets: [{
                label: 'Avg Delivery Days',
                data: catProcSorted.map((r) => +(r.avg_delivery_days ?? 0).toFixed(2)),
                backgroundColor: 'rgba(34,197,94,0.82)',
            }],
        };

        // ---- Processing vs Delivery grouped (by category) -------------------
        const catGroupedData = {
            labels: catProcSorted.map((r) => r.category ?? 'Unknown'),
            datasets: [
                { label: 'Avg Processing Days', data: catProcSorted.map((r) => +(r.avg_processing_days ?? 0).toFixed(2)), backgroundColor: 'rgba(59,130,246,0.82)' },
                { label: 'Avg Delivery Days',   data: catProcSorted.map((r) => +(r.avg_delivery_days ?? 0).toFixed(2)),   backgroundColor: 'rgba(34,197,94,0.82)' },
            ],
        };

        // ---- Orders by category (bar) ---------------------------------------
        const catOrdersSorted = [...byCategory].sort((a, b) => (+(b.orders ?? 0)) - (+(a.orders ?? 0)));
        const catOrdersBarData = {
            labels: catOrdersSorted.map((r) => r.category ?? 'Unknown'),
            datasets: [{
                label: 'Orders',
                data: catOrdersSorted.map((r) => +(r.orders ?? 0)),
                backgroundColor: 'rgba(139,92,246,0.82)',
            }],
        };

        // ---- Processing by day of week (sorted Mon–Sun) ---------------------
        const dowSorted = [...byDow].sort((a, b) => {
            const ai = DOW_ORDER.indexOf(a.order_dow_name ?? '');
            const bi = DOW_ORDER.indexOf(b.order_dow_name ?? '');
            return (ai === -1 ? 99 : ai) - (bi === -1 ? 99 : bi);
        });
        const dowBarData = dowSorted.length > 0 ? {
            labels: dowSorted.map((r) => r.order_dow_name ?? `Day ${r.order_dow}`),
            datasets: [
                { label: 'Avg Processing Days', data: dowSorted.map((r) => +(r.avg_processing_days ?? 0).toFixed(2)), backgroundColor: 'rgba(59,130,246,0.82)' },
                { label: 'Avg Delivery Days',   data: dowSorted.map((r) => +(r.avg_delivery_days ?? 0).toFixed(2)),   backgroundColor: 'rgba(34,197,94,0.82)' },
            ],
        } : null;

        // ---- Orders by day of week (doughnut) --------------------------------
        const dowDoughnutData = dowSorted.length > 0 ? {
            labels: dowSorted.map((r) => r.order_dow_name ?? `Day ${r.order_dow}`),
            datasets: [{ data: dowSorted.map((r) => +(r.orders ?? 0)), backgroundColor: PALETTE }],
        } : null;

        // ---- Processing by hour (bar) ----------------------------------------
        const hourSorted = [...byHour].sort((a, b) => (+(a.order_hour ?? 0)) - (+(b.order_hour ?? 0)));
        const hourBarData = hourSorted.length > 0 ? {
            labels: hourSorted.map((r) => `${String(r.order_hour).padStart(2, '0')}:00`),
            datasets: [{
                label: 'Avg Processing Days',
                data: hourSorted.map((r) => +(r.avg_processing_days ?? 0).toFixed(2)),
                backgroundColor: 'rgba(6,182,212,0.82)',
            }],
        } : null;

        // ---- Orders by hour (bar) -------------------------------------------
        const ordersByHourData = hourSorted.length > 0 ? {
            labels: hourSorted.map((r) => `${String(r.order_hour).padStart(2, '0')}:00`),
            datasets: [{
                label: 'Orders',
                data: hourSorted.map((r) => +(r.orders ?? 0)),
                backgroundColor: 'rgba(249,115,22,0.82)',
            }],
        } : null;

        // ---- Weekend vs Weekday (grouped bar) --------------------------------
        const wkdGroupedData = weekendVsWeekday.length > 0 ? {
            labels: weekendVsWeekday.map((r) => r.day_type ?? 'Unknown'),
            datasets: [
                { label: 'Orders',              data: weekendVsWeekday.map((r) => +(r.orders ?? 0)),                backgroundColor: 'rgba(59,130,246,0.82)' },
                { label: 'Avg Processing Days', data: weekendVsWeekday.map((r) => +(r.avg_processing_days ?? 0).toFixed(2)), backgroundColor: 'rgba(249,115,22,0.82)' },
                { label: 'Avg Delivery Days',   data: weekendVsWeekday.map((r) => +(r.avg_delivery_days ?? 0).toFixed(2)),   backgroundColor: 'rgba(34,197,94,0.82)' },
            ],
        } : null;

        // ---- Processing by season -------------------------------------------
        const seasonBarData = bySeason.length > 0 ? {
            labels: bySeason.map((r) => r.season ?? 'Unknown'),
            datasets: [
                { label: 'Avg Processing Days', data: bySeason.map((r) => +(r.avg_processing_days ?? 0).toFixed(2)), backgroundColor: 'rgba(59,130,246,0.82)' },
                { label: 'Median Processing',   data: bySeason.map((r) => +(r.median_processing_days ?? 0).toFixed(2)), backgroundColor: 'rgba(34,197,94,0.82)' },
                { label: 'Max Processing',      data: bySeason.map((r) => +(r.max_processing_days ?? 0).toFixed(2)), backgroundColor: 'rgba(239,68,68,0.82)' },
            ],
        } : null;

        // ---- Orders by season (doughnut) ------------------------------------
        const seasonDoughnutData = bySeason.length > 0 ? {
            labels: bySeason.map((r) => r.season ?? 'Unknown'),
            datasets: [{ data: bySeason.map((r) => +(r.orders ?? 0)), backgroundColor: PALETTE }],
        } : null;

        // ---- Processing by season + status (stacked) ------------------------
        const seasons  = [...new Set(bySeasonStatus.map((r) => r.season ?? 'Unknown'))];
        const statuses = [...new Set(bySeasonStatus.map((r) => r.order_status ?? 'Unknown'))];
        const seasonStatusBarData = (seasons.length > 0 && statuses.length > 0) ? {
            labels: seasons,
            datasets: statuses.map((status, i) => ({
                label: status,
                data: seasons.map((s) => {
                    const row = bySeasonStatus.find((r) => r.season === s && r.order_status === status);
                    return row ? +(row.orders ?? 0) : 0;
                }),
                backgroundColor: PALETTE[i % PALETTE.length],
            })),
        } : null;

        return {
            kpis: { totalOrders, avgProcDays, avgDelDays, fastestCategory, slowestCategory },
            catProcBarData, catDelBarData, catGroupedData, catOrdersBarData,
            dowBarData, dowDoughnutData, hourBarData, ordersByHourData,
            wkdGroupedData, seasonBarData, seasonDoughnutData, seasonStatusBarData,
            byCategory, bySubcategory, weekendVsWeekday, bySeason,
        };
    }, [rawOps]);

    const hasData = derived !== null;

    // -------------------------------------------------------------------------
    // Render states
    // -------------------------------------------------------------------------

    if (loading && pipelineStatus !== 'loading') {
        return (
            <div className="flex flex-col items-center justify-center min-h-[400px] gap-4">
                <ProgressSpinner />
                <p className="text-gray-500 text-base">Loading processing analytics…</p>
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
                        <p className="text-gray-500 text-sm mt-1">Unable to load processing data. Please try again later.</p>
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
                            : 'No processing data to display.'}
                    </p>
                </div>
            </div>
        );
    }
    if (!derived) return null;
    const {
        kpis, catProcBarData, catDelBarData, catGroupedData, catOrdersBarData,
        dowBarData, dowDoughnutData, hourBarData, ordersByHourData,
        wkdGroupedData, seasonBarData, seasonDoughnutData, seasonStatusBarData,
        byCategory, bySubcategory, weekendVsWeekday, bySeason,
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
                    * Operations analytics are static aggregates computed over all available data and do not change with the date filter.
                </p>
            )}

            {/* ── KPI Cards ──────────────────────────────────────────────── */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <KPICard
                    icon="pi-shopping-bag" iconBg="bg-blue-100" iconColor="text-blue-600"
                    value={fmt.number(kpis.totalOrders)}
                    label="Total Orders Analyzed"
                />
                <KPICard
                    icon="pi-clock" iconBg="bg-orange-100" iconColor="text-orange-600"
                    value={`${fmt.decimal(kpis.avgProcDays, 1)}d`}
                    label="Avg Processing Days"
                />
                <KPICard
                    icon="pi-truck" iconBg="bg-green-100" iconColor="text-green-600"
                    value={`${fmt.decimal(kpis.avgDelDays, 1)}d`}
                    label="Avg Delivery Days"
                />
                <KPICard
                    icon="pi-bolt" iconBg="bg-purple-100" iconColor="text-purple-600"
                    value={kpis.fastestCategory?.category ?? '—'}
                    label="Fastest Category"
                />
            </div>

            {/* ── Processing by Category ─────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-blue-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Processing by Category</h2>
                </div>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {catProcBarData.labels.length > 0 && (
                        <ChartWrapper title="Avg Processing Days by Category" height={340}>
                            <Bar data={catProcBarData} options={barOpts(true)} />
                        </ChartWrapper>
                    )}
                    {catGroupedData.labels.length > 0 && (
                        <ChartWrapper title="Processing vs Delivery Days by Category" height={340}>
                            <Bar data={catGroupedData} options={groupedBarOpts()} />
                        </ChartWrapper>
                    )}
                    {catOrdersBarData.labels.length > 0 && (
                        <ChartWrapper title="Order Volume by Category" height={340}>
                            <Bar data={catOrdersBarData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                    {catDelBarData.labels.length > 0 && (
                        <ChartWrapper title="Avg Delivery Days by Category" height={340}>
                            <Bar data={catDelBarData} options={barOpts(true)} />
                        </ChartWrapper>
                    )}
                </div>
            </section>

            {/* ── Day-of-Week Patterns ───────────────────────────────────── */}
            {(dowBarData || dowDoughnutData) && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-purple-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Day-of-Week Patterns</h2>
                    </div>
                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                        {dowBarData && (
                            <ChartWrapper title="Avg Processing & Delivery Days by Day of Week" height={340}>
                                <Bar data={dowBarData} options={groupedBarOpts()} />
                            </ChartWrapper>
                        )}
                        {dowDoughnutData && (
                            <ChartWrapper title="Order Volume by Day of Week" height={280}>
                                <Doughnut data={dowDoughnutData} options={doughnutOpts()} />
                            </ChartWrapper>
                        )}
                    </div>
                </section>
            )}

            {/* ── Hourly Patterns ────────────────────────────────────────── */}
            {(hourBarData || ordersByHourData) && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-cyan-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Hourly Order Patterns</h2>
                    </div>
                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                        {hourBarData && (
                            <ChartWrapper title="Avg Processing Days by Order Hour" height={340}>
                                <Bar data={hourBarData} options={barOpts()} />
                            </ChartWrapper>
                        )}
                        {ordersByHourData && (
                            <ChartWrapper title="Order Volume by Hour of Day" height={340}>
                                <Bar data={ordersByHourData} options={barOpts()} />
                            </ChartWrapper>
                        )}
                    </div>
                </section>
            )}

            {/* ── Weekend vs Weekday ─────────────────────────────────────── */}
            {wkdGroupedData && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-amber-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Weekend vs Weekday</h2>
                    </div>
                    <ChartWrapper title="Orders, Processing & Delivery — Weekend vs Weekday" height={340}>
                        <Bar data={wkdGroupedData} options={groupedBarOpts()} />
                    </ChartWrapper>
                </section>
            )}

            {/* ── Seasonal Analysis ──────────────────────────────────────── */}
            {(seasonBarData || seasonDoughnutData) && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-green-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Seasonal Analysis</h2>
                    </div>
                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                        {seasonBarData && (
                            <ChartWrapper title="Processing Days by Season (Avg / Median / Max)" height={340}>
                                <Bar data={seasonBarData} options={groupedBarOpts()} />
                            </ChartWrapper>
                        )}
                        {seasonDoughnutData && (
                            <ChartWrapper title="Order Volume by Season" height={280}>
                                <Doughnut data={seasonDoughnutData} options={doughnutOpts()} />
                            </ChartWrapper>
                        )}
                    </div>
                    {seasonStatusBarData && (
                        <ChartWrapper title="Order Status Breakdown by Season" height={340}>
                            <Bar data={seasonStatusBarData} options={groupedBarOpts()} />
                        </ChartWrapper>
                    )}
                </section>
            )}

            {/* ── Summary Tables ─────────────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-gray-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Summary Tables</h2>
                </div>

                {/* Category Processing Table */}
                {byCategory.length > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                Processing Performance by Category
                            </h3>
                            <DataTable value={[...byCategory].sort((a, b) => (+(a.avg_processing_days ?? 0)) - (+(b.avg_processing_days ?? 0)))}
                                scrollable stripedRows emptyMessage="No data" className="text-sm">
                                <Column field="category"                 header="Category"              sortable />
                                <Column field="orders"                   header="Orders"                sortable body={(r) => fmt.number(r.orders)} />
                                <Column field="total_units"              header="Total Units"            sortable body={(r) => fmt.number(r.total_units)} />
                                <Column field="avg_processing_days"      header="Avg Processing (d)"    sortable body={(r) => fmt.decimal(r.avg_processing_days, 1)} />
                                <Column field="median_processing_days"   header="Median Processing (d)" sortable body={(r) => fmt.decimal(r.median_processing_days, 1)} />
                                <Column field="max_processing_days"      header="Max Processing (d)"    sortable body={(r) => fmt.decimal(r.max_processing_days, 1)} />
                                <Column field="avg_delivery_days"        header="Avg Delivery (d)"      sortable body={(r) => fmt.decimal(r.avg_delivery_days, 1)} />
                                <Column field="avg_total_fulfillment_days" header="Avg Fulfillment (d)" sortable body={(r) => fmt.decimal(r.avg_total_fulfillment_days, 1)} />
                            </DataTable>
                        </div>
                    </Card>
                )}

                {/* Subcategory Table */}
                {bySubcategory.length > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                Processing Performance by Sub-Category
                            </h3>
                            <DataTable value={bySubcategory} paginator rows={15}
                                scrollable stripedRows emptyMessage="No data" className="text-sm">
                                <Column field="category"               header="Category"              sortable />
                                <Column field="sub_category"           header="Sub-Category"           sortable />
                                <Column field="orders"                 header="Orders"                sortable body={(r) => fmt.number(r.orders)} />
                                <Column field="avg_processing_days"    header="Avg Processing (d)"    sortable body={(r) => fmt.decimal(r.avg_processing_days, 1)} />
                                <Column field="median_processing_days" header="Median Processing (d)" sortable body={(r) => fmt.decimal(r.median_processing_days, 1)} />
                                <Column field="avg_delivery_days"      header="Avg Delivery (d)"      sortable body={(r) => fmt.decimal(r.avg_delivery_days, 1)} />
                            </DataTable>
                        </div>
                    </Card>
                )}

                {/* Weekend vs Weekday Table */}
                {weekendVsWeekday.length > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                Weekend vs Weekday Comparison
                            </h3>
                            <DataTable value={weekendVsWeekday} scrollable stripedRows emptyMessage="No data" className="text-sm">
                                <Column field="day_type"             header="Day Type"             sortable />
                                <Column field="orders"               header="Orders"               sortable body={(r) => fmt.number(r.orders)} />
                                <Column field="avg_processing_days"  header="Avg Processing (d)"   sortable body={(r) => fmt.decimal(r.avg_processing_days, 1)} />
                                <Column field="avg_delivery_days"    header="Avg Delivery (d)"     sortable body={(r) => fmt.decimal(r.avg_delivery_days, 1)} />
                                <Column field="total_revenue"        header="Total Revenue"        sortable body={(r) => fmt.currency(r.total_revenue)} />
                            </DataTable>
                        </div>
                    </Card>
                )}

                {/* Season Table */}
                {bySeason.length > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                Seasonal Processing Summary
                            </h3>
                            <DataTable value={bySeason} scrollable stripedRows emptyMessage="No data" className="text-sm">
                                <Column field="season"                header="Season"               sortable />
                                <Column field="orders"                header="Orders"               sortable body={(r) => fmt.number(r.orders)} />
                                <Column field="avg_processing_days"   header="Avg Processing (d)"   sortable body={(r) => fmt.decimal(r.avg_processing_days, 1)} />
                                <Column field="median_processing_days" header="Median (d)"           sortable body={(r) => fmt.decimal(r.median_processing_days, 1)} />
                                <Column field="max_processing_days"   header="Max (d)"              sortable body={(r) => fmt.decimal(r.max_processing_days, 1)} />
                            </DataTable>
                        </div>
                    </Card>
                )}
            </section>
        </div>
    );
}
