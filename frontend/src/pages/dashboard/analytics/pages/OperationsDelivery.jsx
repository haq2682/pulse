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

export default function OperationsDelivery() {
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
            console.error('[OperationsDelivery] fetch error');
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
        toastRef.current?.show({ severity: 'info', summary: 'Data Updated', detail: 'Analytics pipeline completed — refreshing delivery data.', life: 3000 });
    }, [lastUpdate]); // eslint-disable-line

    // -------------------------------------------------------------------------
    // Derived data
    // -------------------------------------------------------------------------

    const derived = useMemo(() => {
        if (!rawOps) return null;
        const a = rawOps.analytics ?? {};

        const delByCountry   = a.delivery_days_by_country?.data     ?? [];
        const delByState     = a.delivery_days_by_state?.data       ?? [];
        const delByCity      = a.delivery_days_by_city?.data        ?? [];
        const ontimeByCountry = a.ontime_delivery_by_country?.data  ?? [];
        const ontimeByState   = a.ontime_delivery_by_state?.data    ?? [];
        const ontimeByCity    = a.ontime_delivery_by_city?.data     ?? [];

        if (delByCountry.length === 0 && ontimeByCountry.length === 0) return null;

        // ---- KPIs -----------------------------------------------------------
        const totalDelivered  = delByCountry.reduce((s, r) => s + (+(r.delivered_orders ?? 0)), 0);
        const avgDelDays      = delByCountry.length > 0
            ? delByCountry.reduce((s, r) => s + (+(r.avg_delivery_days ?? 0)), 0) / delByCountry.length
            : 0;
        const overallOntime   = (ontimeByCountry.length > 0 && totalDelivered > 0)
            ? ontimeByCountry.reduce((s, r) => s + (+(r.on_time_orders ?? 0)), 0) /
              ontimeByCountry.reduce((s, r) => s + (+(r.delivered_orders ?? 0)), 0) * 100
            : 0;
        const fastestCountry  = [...delByCountry].sort((a, b) => (+(a.avg_delivery_days ?? 99)) - (+(b.avg_delivery_days ?? 99)))[0];
        const topOntimeCountry = [...ontimeByCountry].sort((a, b) => (+(b.on_time_rate ?? 0)) - (+(a.on_time_rate ?? 0)))[0];

        // ---- Avg delivery days by country (top 20, horizontal) ---------------
        const delCountrySorted = [...delByCountry].sort((a, b) => (+(a.avg_delivery_days ?? 0)) - (+(b.avg_delivery_days ?? 0))).slice(0, 20);
        const delCountryBarData = {
            labels: delCountrySorted.map((r) => r.country ?? 'Unknown'),
            datasets: [{
                label: 'Avg Delivery Days',
                data: delCountrySorted.map((r) => +(r.avg_delivery_days ?? 0).toFixed(2)),
                backgroundColor: PALETTE,
            }],
        };

        // ---- Delivered orders by country doughnut (top 10) ------------------
        const delDoughnutData = {
            labels: [...delByCountry].sort((a, b) => (+(b.delivered_orders ?? 0)) - (+(a.delivered_orders ?? 0))).slice(0, 10).map((r) => r.country ?? 'Unknown'),
            datasets: [{
                data: [...delByCountry].sort((a, b) => (+(b.delivered_orders ?? 0)) - (+(a.delivered_orders ?? 0))).slice(0, 10).map((r) => +(r.delivered_orders ?? 0)),
                backgroundColor: PALETTE,
            }],
        };

        // ---- On-time rate by country (horizontal bar, sorted desc) ----------
        const ontimeCountrySorted = [...ontimeByCountry].sort((a, b) => (+(b.on_time_rate ?? 0)) - (+(a.on_time_rate ?? 0))).slice(0, 20);
        const ontimeCountryBarData = ontimeCountrySorted.length > 0 ? {
            labels: ontimeCountrySorted.map((r) => r.country ?? 'Unknown'),
            datasets: [{
                label: 'On-Time Rate %',
                data: ontimeCountrySorted.map((r) => +(r.on_time_rate ?? 0).toFixed(2)),
                backgroundColor: ontimeCountrySorted.map((r) => {
                    const rate = +(r.on_time_rate ?? 0);
                    if (rate >= 90) return 'rgba(34,197,94,0.82)';
                    if (rate >= 70) return 'rgba(234,179,8,0.82)';
                    return 'rgba(239,68,68,0.82)';
                }),
            }],
        } : null;

        // ---- On-time orders vs late orders by country (grouped, top 10) -----
        const ontimeVsLateData = ontimeCountrySorted.length > 0 ? (() => {
            const top10 = ontimeCountrySorted.slice(0, 10);
            return {
                labels: top10.map((r) => r.country ?? 'Unknown'),
                datasets: [
                    { label: 'On-Time Orders', data: top10.map((r) => +(r.on_time_orders ?? 0)), backgroundColor: 'rgba(34,197,94,0.82)' },
                    { label: 'Late Orders',    data: top10.map((r) => Math.max(0, (+(r.delivered_orders ?? 0)) - (+(r.on_time_orders ?? 0)))), backgroundColor: 'rgba(239,68,68,0.82)' },
                ],
            };
        })() : null;

        // ---- Avg delivery days by state (top 20, horizontal) ----------------
        const delStateSorted = [...delByState].sort((a, b) => (+(b.delivered_orders ?? 0)) - (+(a.delivered_orders ?? 0))).slice(0, 20);
        const delStateBarData = delStateSorted.length > 0 ? {
            labels: delStateSorted.map((r) => `${r.state_province ?? '?'}, ${r.country ?? '?'}`),
            datasets: [{
                label: 'Avg Delivery Days',
                data: delStateSorted.map((r) => +(r.avg_delivery_days ?? 0).toFixed(2)),
                backgroundColor: 'rgba(59,130,246,0.82)',
            }],
        } : null;

        // ---- On-time rate by state (top 20, horizontal) ---------------------
        const ontimeStateSorted = [...ontimeByState].sort((a, b) => (+(b.on_time_rate ?? 0)) - (+(a.on_time_rate ?? 0))).slice(0, 20);
        const ontimeStateBarData = ontimeStateSorted.length > 0 ? {
            labels: ontimeStateSorted.map((r) => `${r.state_province ?? '?'}, ${r.country ?? '?'}`),
            datasets: [{
                label: 'On-Time Rate %',
                data: ontimeStateSorted.map((r) => +(r.on_time_rate ?? 0).toFixed(2)),
                backgroundColor: ontimeStateSorted.map((r) => {
                    const rate = +(r.on_time_rate ?? 0);
                    if (rate >= 90) return 'rgba(34,197,94,0.82)';
                    if (rate >= 70) return 'rgba(234,179,8,0.82)';
                    return 'rgba(239,68,68,0.82)';
                }),
            }],
        } : null;

        // ---- Top/bottom cities by on-time rate (top 15) ----------------------
        const cityOntime = [...ontimeByCity].filter((r) => (+(r.delivered_orders ?? 0)) >= 3);
        const topCitiesOntimeData = cityOntime.length > 0 ? (() => {
            const top15 = [...cityOntime].sort((a, b) => (+(b.on_time_rate ?? 0)) - (+(a.on_time_rate ?? 0))).slice(0, 15);
            return {
                labels: top15.map((r) => `${r.city ?? '?'}, ${r.state_province ?? '?'}`),
                datasets: [{
                    label: 'On-Time Rate %',
                    data: top15.map((r) => +(r.on_time_rate ?? 0).toFixed(2)),
                    backgroundColor: 'rgba(34,197,94,0.82)',
                }],
            };
        })() : null;

        return {
            kpis: { totalDelivered, avgDelDays, overallOntime, fastestCountry, topOntimeCountry },
            delCountryBarData, delDoughnutData, ontimeCountryBarData, ontimeVsLateData,
            delStateBarData, ontimeStateBarData, topCitiesOntimeData,
            delByCountry, ontimeByCountry, delByState, ontimeByState, delByCity,
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
                <p className="text-gray-500 text-base">Loading delivery analytics…</p>
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
                        <p className="text-gray-500 text-sm mt-1">Unable to load delivery data. Please try again later.</p>
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
                            : 'No delivery data to display.'}
                    </p>
                </div>
            </div>
        );
    }
    if (!derived) return null;
    const {
        kpis, delCountryBarData, delDoughnutData, ontimeCountryBarData, ontimeVsLateData,
        delStateBarData, ontimeStateBarData, topCitiesOntimeData,
        delByCountry, ontimeByCountry, delByState, ontimeByState,
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
                    icon="pi-check-circle" iconBg="bg-green-100" iconColor="text-green-600"
                    value={fmt.number(kpis.totalDelivered)}
                    label="Total Delivered Orders"
                />
                <KPICard
                    icon="pi-clock" iconBg="bg-blue-100" iconColor="text-blue-600"
                    value={`${fmt.decimal(kpis.avgDelDays, 1)}d`}
                    label="Avg Delivery Days"
                />
                <KPICard
                    icon="pi-calendar-check" iconBg="bg-purple-100" iconColor="text-purple-600"
                    value={fmt.pct(kpis.overallOntime)}
                    label="Overall On-Time Rate"
                />
                <KPICard
                    icon="pi-map-marker" iconBg="bg-amber-100" iconColor="text-amber-600"
                    value={kpis.fastestCountry?.country ?? '—'}
                    label="Fastest Country"
                />
            </div>

            {/* ── Country-Level Delivery ─────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-blue-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Country-Level Delivery</h2>
                </div>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {delCountryBarData.labels.length > 0 && (
                        <ChartWrapper title="Avg Delivery Days by Country (Top 20)" height={420}>
                            <Bar data={delCountryBarData} options={barOpts(true)} />
                        </ChartWrapper>
                    )}
                    {delDoughnutData.labels.length > 0 && (
                        <ChartWrapper title="Delivered Orders Share by Country (Top 10)" height={280}>
                            <Doughnut data={delDoughnutData} options={doughnutOpts()} />
                        </ChartWrapper>
                    )}
                </div>
            </section>

            {/* ── On-Time Delivery ───────────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-green-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">On-Time Delivery Rates</h2>
                </div>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {ontimeCountryBarData && (
                        <ChartWrapper title="On-Time Rate % by Country (Top 20)" height={420}>
                            <Bar data={ontimeCountryBarData} options={barOpts(true)} />
                        </ChartWrapper>
                    )}
                    {ontimeVsLateData && (
                        <ChartWrapper title="On-Time vs Late Orders by Country (Top 10)" height={340}>
                            <Bar data={ontimeVsLateData} options={groupedBarOpts()} />
                        </ChartWrapper>
                    )}
                </div>
            </section>

            {/* ── State-Level Analysis ───────────────────────────────────── */}
            {(delStateBarData || ontimeStateBarData) && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-purple-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">State-Level Analysis</h2>
                    </div>
                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                        {delStateBarData && (
                            <ChartWrapper title="Avg Delivery Days by State (Top 20 by Volume)" height={440}>
                                <Bar data={delStateBarData} options={barOpts(true)} />
                            </ChartWrapper>
                        )}
                        {ontimeStateBarData && (
                            <ChartWrapper title="On-Time Rate % by State (Top 20)" height={440}>
                                <Bar data={ontimeStateBarData} options={barOpts(true)} />
                            </ChartWrapper>
                        )}
                    </div>
                </section>
            )}

            {/* ── City-Level On-Time ─────────────────────────────────────── */}
            {topCitiesOntimeData && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-cyan-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">City-Level On-Time Performance</h2>
                    </div>
                    <ChartWrapper title="Top 15 Cities by On-Time Rate (min 3 orders)" height={420}>
                        <Bar data={topCitiesOntimeData} options={barOpts(true)} />
                    </ChartWrapper>
                </section>
            )}

            {/* ── Summary Tables ─────────────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-gray-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Summary Tables</h2>
                </div>

                {/* Delivery by Country Table */}
                {delByCountry.length > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                Delivery Days by Country
                            </h3>
                            <DataTable value={[...delByCountry].sort((a, b) => (+(a.avg_delivery_days ?? 0)) - (+(b.avg_delivery_days ?? 0)))}
                                scrollable stripedRows emptyMessage="No data" className="text-sm">
                                <Column field="country"             header="Country"               sortable />
                                <Column field="delivered_orders"    header="Delivered Orders"      sortable body={(r) => fmt.number(r.delivered_orders)} />
                                <Column field="avg_delivery_days"   header="Avg Delivery (d)"      sortable body={(r) => fmt.decimal(r.avg_delivery_days, 1)} />
                                <Column field="median_delivery_days" header="Median (d)"           sortable body={(r) => fmt.decimal(r.median_delivery_days, 1)} />
                                <Column field="max_delivery_days"   header="Max (d)"               sortable body={(r) => fmt.decimal(r.max_delivery_days, 1)} />
                            </DataTable>
                        </div>
                    </Card>
                )}

                {/* On-Time by Country Table */}
                {ontimeByCountry.length > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                On-Time Delivery by Country
                            </h3>
                            <DataTable value={[...ontimeByCountry].sort((a, b) => (+(b.on_time_rate ?? 0)) - (+(a.on_time_rate ?? 0)))}
                                scrollable stripedRows emptyMessage="No data" className="text-sm">
                                <Column field="country"          header="Country"            sortable />
                                <Column field="delivered_orders" header="Delivered"           sortable body={(r) => fmt.number(r.delivered_orders)} />
                                <Column field="on_time_orders"   header="On-Time"             sortable body={(r) => fmt.number(r.on_time_orders)} />
                                <Column field="avg_delivery_days" header="Avg Delivery (d)"   sortable body={(r) => fmt.decimal(r.avg_delivery_days, 1)} />
                                <Column field="on_time_rate"     header="On-Time Rate"        sortable body={(r) => (
                                    <Tag value={fmt.pct(r.on_time_rate)}
                                        severity={(+(r.on_time_rate ?? 0)) >= 90 ? 'success' : (+(r.on_time_rate ?? 0)) >= 70 ? 'warning' : 'danger'} />
                                )} />
                            </DataTable>
                        </div>
                    </Card>
                )}

                {/* State Table */}
                {delByState.length > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                Delivery Days by State / Province
                            </h3>
                            <DataTable value={delByState} paginator rows={15}
                                scrollable stripedRows emptyMessage="No data" className="text-sm">
                                <Column field="country"             header="Country"               sortable />
                                <Column field="state_province"      header="State/Province"        sortable />
                                <Column field="delivered_orders"    header="Delivered"             sortable body={(r) => fmt.number(r.delivered_orders)} />
                                <Column field="avg_delivery_days"   header="Avg Delivery (d)"      sortable body={(r) => fmt.decimal(r.avg_delivery_days, 1)} />
                                <Column field="median_delivery_days" header="Median (d)"           sortable body={(r) => fmt.decimal(r.median_delivery_days, 1)} />
                                <Column field="max_delivery_days"   header="Max (d)"               sortable body={(r) => fmt.decimal(r.max_delivery_days, 1)} />
                            </DataTable>
                        </div>
                    </Card>
                )}

                {/* On-Time by State Table */}
                {ontimeByState.length > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                On-Time Delivery by State / Province
                            </h3>
                            <DataTable value={[...ontimeByState].sort((a, b) => (+(b.on_time_rate ?? 0)) - (+(a.on_time_rate ?? 0)))}
                                paginator rows={15} scrollable stripedRows emptyMessage="No data" className="text-sm">
                                <Column field="country"          header="Country"            sortable />
                                <Column field="state_province"   header="State/Province"     sortable />
                                <Column field="delivered_orders" header="Delivered"           sortable body={(r) => fmt.number(r.delivered_orders)} />
                                <Column field="on_time_orders"   header="On-Time"             sortable body={(r) => fmt.number(r.on_time_orders)} />
                                <Column field="avg_delivery_days" header="Avg Delivery (d)"   sortable body={(r) => fmt.decimal(r.avg_delivery_days, 1)} />
                                <Column field="on_time_rate"     header="On-Time Rate"        sortable body={(r) => (
                                    <Tag value={fmt.pct(r.on_time_rate)}
                                        severity={(+(r.on_time_rate ?? 0)) >= 90 ? 'success' : (+(r.on_time_rate ?? 0)) >= 70 ? 'warning' : 'danger'} />
                                )} />
                            </DataTable>
                        </div>
                    </Card>
                )}

                {/* Delivery Days by City Table */}
                {(derived?.delByCity?.length ?? 0) > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                Delivery Days by City
                            </h3>
                            <DataTable
                                value={[...derived.delByCity].sort((a, b) => (+(a.avg_delivery_days ?? 0)) - (+(b.avg_delivery_days ?? 0)))}
                                paginator rows={15} stripedRows emptyMessage="No data" className="text-sm"
                            >
                                <Column field="country"          header="Country"        sortable />
                                <Column field="state_province"   header="State/Province" sortable />
                                <Column field="city"             header="City"           sortable />
                                <Column field="delivered_orders" header="Delivered"      sortable body={(r) => fmt.number(r.delivered_orders)} />
                                <Column field="avg_delivery_days" header="Avg Days"      sortable body={(r) => fmt.decimal(r.avg_delivery_days, 1)} />
                                <Column field="median_delivery_days" header="Median Days" sortable body={(r) => fmt.decimal(r.median_delivery_days, 1)} />
                                <Column field="max_delivery_days"   header="Max Days"    sortable body={(r) => fmt.decimal(r.max_delivery_days, 1)} />
                            </DataTable>
                        </div>
                    </Card>
                )}
            </section>
        </div>
    );
}
