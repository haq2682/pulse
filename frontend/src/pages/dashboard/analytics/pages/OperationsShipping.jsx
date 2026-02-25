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

export default function OperationsShipping() {
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
            console.error('[OperationsShipping] fetch error');
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
        toastRef.current?.show({ severity: 'info', summary: 'Data Updated', detail: 'Analytics pipeline completed — refreshing shipping data.', life: 3000 });
    }, [lastUpdate]); // eslint-disable-line

    // -------------------------------------------------------------------------
    // Derived data
    // -------------------------------------------------------------------------

    const derived = useMemo(() => {
        if (!rawOps) return null;
        const a = rawOps.analytics ?? {};

        const shipByCountry = a.shipping_efficiency_by_country?.data ?? [];
        const shipByState   = a.shipping_efficiency_by_state?.data   ?? [];
        const shipByCity    = a.shipping_efficiency_by_city?.data     ?? [];

        if (shipByCountry.length === 0 && shipByState.length === 0) return null;

        // ---- KPIs -----------------------------------------------------------
        const totalOrders       = shipByCountry.reduce((s, r) => s + (+(r.orders ?? 0)), 0);
        const totalShippingCost = shipByCountry.reduce((s, r) => s + (+(r.total_shipping_cost ?? 0)), 0);
        const totalSubtotal     = shipByCountry.reduce((s, r) => s + (+(r.total_subtotal ?? 0)), 0);
        const overallShipPct    = totalSubtotal > 0 ? (totalShippingCost / totalSubtotal) * 100 : 0;
        const cheapestCountry   = [...shipByCountry].sort((a, b) => (+(a.avg_shipping_pct_of_subtotal ?? 99)) - (+(b.avg_shipping_pct_of_subtotal ?? 99)))[0];
        const mostExpensive     = [...shipByCountry].sort((a, b) => (+(b.avg_shipping_pct_of_subtotal ?? 0)) - (+(a.avg_shipping_pct_of_subtotal ?? 0)))[0];

        // ---- Avg shipping % of subtotal by country (horizontal, top 20) ----
        const countrySorted = [...shipByCountry].sort((a, b) => (+(b.avg_shipping_pct_of_subtotal ?? 0)) - (+(a.avg_shipping_pct_of_subtotal ?? 0))).slice(0, 20);
        const avgShipPctBarData = {
            labels: countrySorted.map((r) => r.country ?? 'Unknown'),
            datasets: [{
                label: 'Avg Shipping % of Subtotal',
                data: countrySorted.map((r) => +(r.avg_shipping_pct_of_subtotal ?? 0).toFixed(2)),
                backgroundColor: countrySorted.map((r) => {
                    const pct = +(r.avg_shipping_pct_of_subtotal ?? 0);
                    if (pct >= 15) return 'rgba(239,68,68,0.82)';
                    if (pct >= 8)  return 'rgba(234,179,8,0.82)';
                    return 'rgba(34,197,94,0.82)';
                }),
            }],
        };

        // ---- Total shipping cost by country (bar, top 15) -------------------
        const shipCostSorted = [...shipByCountry].sort((a, b) => (+(b.total_shipping_cost ?? 0)) - (+(a.total_shipping_cost ?? 0))).slice(0, 15);
        const shipCostBarData = {
            labels: shipCostSorted.map((r) => r.country ?? 'Unknown'),
            datasets: [{
                label: 'Total Shipping Cost ($)',
                data: shipCostSorted.map((r) => +(r.total_shipping_cost ?? 0).toFixed(2)),
                backgroundColor: PALETTE,
            }],
        };

        // ---- Total shipping cost doughnut (top 10) --------------------------
        const shipCostDoughnutData = {
            labels: shipCostSorted.slice(0, 10).map((r) => r.country ?? 'Unknown'),
            datasets: [{ data: shipCostSorted.slice(0, 10).map((r) => +(r.total_shipping_cost ?? 0).toFixed(2)), backgroundColor: PALETTE }],
        };

        // ---- Shipping cost vs order subtotal grouped (top 10) ---------------
        const top10 = [...shipByCountry].sort((a, b) => (+(b.orders ?? 0)) - (+(a.orders ?? 0))).slice(0, 10);
        const shipVsSubtotalGrouped = top10.length > 0 ? {
            labels: top10.map((r) => r.country ?? 'Unknown'),
            datasets: [
                { label: 'Total Shipping Cost ($)', data: top10.map((r) => +(r.total_shipping_cost ?? 0).toFixed(2)),  backgroundColor: 'rgba(239,68,68,0.82)' },
                { label: 'Total Subtotal ($)',       data: top10.map((r) => +(r.total_subtotal ?? 0).toFixed(2)),       backgroundColor: 'rgba(59,130,246,0.82)' },
            ],
        } : null;

        // ---- Avg shipping % by state (top 20, horizontal) ------------------
        const stateSorted = [...shipByState].sort((a, b) => (+(b.avg_shipping_pct_of_subtotal ?? 0)) - (+(a.avg_shipping_pct_of_subtotal ?? 0))).slice(0, 20);
        const stateShipPctBarData = stateSorted.length > 0 ? {
            labels: stateSorted.map((r) => `${r.state_province ?? '?'}, ${r.country ?? '?'}`),
            datasets: [{
                label: 'Avg Shipping % of Subtotal',
                data: stateSorted.map((r) => +(r.avg_shipping_pct_of_subtotal ?? 0).toFixed(2)),
                backgroundColor: stateSorted.map((r) => {
                    const pct = +(r.avg_shipping_pct_of_subtotal ?? 0);
                    if (pct >= 15) return 'rgba(239,68,68,0.82)';
                    if (pct >= 8)  return 'rgba(234,179,8,0.82)';
                    return 'rgba(34,197,94,0.82)';
                }),
            }],
        } : null;

        // ---- Shipping cost by state (top 15) --------------------------------
        const stateShipCostSorted = [...shipByState].sort((a, b) => (+(b.total_shipping_cost ?? 0)) - (+(a.total_shipping_cost ?? 0))).slice(0, 15);
        const stateShipCostBarData = stateShipCostSorted.length > 0 ? {
            labels: stateShipCostSorted.map((r) => `${r.state_province ?? '?'}, ${r.country ?? '?'}`),
            datasets: [{
                label: 'Total Shipping Cost ($)',
                data: stateShipCostSorted.map((r) => +(r.total_shipping_cost ?? 0).toFixed(2)),
                backgroundColor: 'rgba(59,130,246,0.82)',
            }],
        } : null;

        // ---- City shipping efficiency (top 15 most expensive) ---------------
        const cityExpensive = [...shipByCity]
            .filter((r) => (+(r.orders ?? 0)) >= 2)
            .sort((a, b) => (+(b.avg_shipping_pct_of_subtotal ?? 0)) - (+(a.avg_shipping_pct_of_subtotal ?? 0)))
            .slice(0, 15);
        const cityShipPctBarData = cityExpensive.length > 0 ? {
            labels: cityExpensive.map((r) => `${r.city ?? '?'}, ${r.state_province ?? '?'}`),
            datasets: [{
                label: 'Avg Shipping % of Subtotal',
                data: cityExpensive.map((r) => +(r.avg_shipping_pct_of_subtotal ?? 0).toFixed(2)),
                backgroundColor: 'rgba(249,115,22,0.82)',
            }],
        } : null;

        return {
            kpis: { totalOrders, totalShippingCost, overallShipPct, cheapestCountry, mostExpensive },
            avgShipPctBarData, shipCostBarData, shipCostDoughnutData, shipVsSubtotalGrouped,
            stateShipPctBarData, stateShipCostBarData, cityShipPctBarData,
            shipByCountry, shipByState,
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
                <p className="text-gray-500 text-base">Loading shipping analytics…</p>
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
                        <p className="text-gray-500 text-sm mt-1">Unable to load shipping data. Please try again later.</p>
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
                            : 'No shipping data to display.'}
                    </p>
                </div>
            </div>
        );
    }
    if (!derived) return null;
    const {
        kpis, avgShipPctBarData, shipCostBarData, shipCostDoughnutData, shipVsSubtotalGrouped,
        stateShipPctBarData, stateShipCostBarData, cityShipPctBarData,
        shipByCountry, shipByState,
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
                    label="Total Orders"
                />
                <KPICard
                    icon="pi-dollar" iconBg="bg-red-100" iconColor="text-red-600"
                    value={fmt.currency(kpis.totalShippingCost)}
                    label="Total Shipping Cost"
                />
                <KPICard
                    icon="pi-percentage" iconBg="bg-amber-100" iconColor="text-amber-600"
                    value={fmt.pct(kpis.overallShipPct)}
                    label="Shipping % of Revenue"
                />
                <KPICard
                    icon="pi-star" iconBg="bg-green-100" iconColor="text-green-600"
                    value={kpis.cheapestCountry?.country ?? '—'}
                    label="Most Efficient Country"
                />
            </div>

            {/* ── Country Shipping Efficiency ────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-blue-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Country Shipping Efficiency</h2>
                </div>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {avgShipPctBarData.labels.length > 0 && (
                        <ChartWrapper title="Avg Shipping Cost % of Order Value — Country (Top 20)" height={440}>
                            <Bar data={avgShipPctBarData} options={barOpts(true)} />
                        </ChartWrapper>
                    )}
                    {shipCostDoughnutData.labels.length > 0 && (
                        <ChartWrapper title="Shipping Cost Share by Country (Top 10)" height={280}>
                            <Doughnut data={shipCostDoughnutData} options={doughnutOpts()} />
                        </ChartWrapper>
                    )}
                    {shipCostBarData.labels.length > 0 && (
                        <ChartWrapper title="Total Shipping Cost by Country (Top 15)" height={360}>
                            <Bar data={shipCostBarData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                    {shipVsSubtotalGrouped && (
                        <ChartWrapper title="Shipping Cost vs Order Subtotal by Country (Top 10)" height={360}>
                            <Bar data={shipVsSubtotalGrouped} options={groupedBarOpts()} />
                        </ChartWrapper>
                    )}
                </div>
            </section>

            {/* ── State Shipping Efficiency ──────────────────────────────── */}
            {(stateShipPctBarData || stateShipCostBarData) && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-purple-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">State / Province Shipping Efficiency</h2>
                    </div>
                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                        {stateShipPctBarData && (
                            <ChartWrapper title="Avg Shipping % of Order Value — State (Top 20)" height={460}>
                                <Bar data={stateShipPctBarData} options={barOpts(true)} />
                            </ChartWrapper>
                        )}
                        {stateShipCostBarData && (
                            <ChartWrapper title="Total Shipping Cost by State (Top 15)" height={420}>
                                <Bar data={stateShipCostBarData} options={barOpts(true)} />
                            </ChartWrapper>
                        )}
                    </div>
                </section>
            )}

            {/* ── City Shipping Efficiency ───────────────────────────────── */}
            {cityShipPctBarData && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-orange-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">City-Level Shipping Efficiency</h2>
                    </div>
                    <ChartWrapper title="Top 15 Most Expensive Cities (Shipping % of Order Value, min 2 orders)" height={420}>
                        <Bar data={cityShipPctBarData} options={barOpts(true)} />
                    </ChartWrapper>
                </section>
            )}

            {/* ── Summary Tables ─────────────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-gray-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Summary Tables</h2>
                </div>

                {/* Country Table */}
                {shipByCountry.length > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                Shipping Efficiency by Country
                            </h3>
                            <DataTable value={[...shipByCountry].sort((a, b) => (+(a.avg_shipping_pct_of_subtotal ?? 0)) - (+(b.avg_shipping_pct_of_subtotal ?? 0)))}
                                scrollable stripedRows emptyMessage="No data" className="text-sm">
                                <Column field="country"                     header="Country"               sortable />
                                <Column field="orders"                      header="Orders"                sortable body={(r) => fmt.number(r.orders)} />
                                <Column field="total_shipping_cost"         header="Shipping Cost"         sortable body={(r) => fmt.currency(r.total_shipping_cost)} />
                                <Column field="total_subtotal"              header="Total Subtotal"        sortable body={(r) => fmt.currency(r.total_subtotal)} />
                                <Column field="avg_shipping_pct_of_subtotal" header="Avg Shipping %"      sortable body={(r) => (
                                    <Tag value={fmt.pct(r.avg_shipping_pct_of_subtotal)}
                                        severity={(+(r.avg_shipping_pct_of_subtotal ?? 0)) <= 5 ? 'success' : (+(r.avg_shipping_pct_of_subtotal ?? 0)) <= 12 ? 'warning' : 'danger'} />
                                )} />
                                <Column field="median_shipping_pct_of_subtotal" header="Median Shipping %" sortable body={(r) => fmt.pct(r.median_shipping_pct_of_subtotal)} />
                                <Column field="shipping_pct_of_subtotal_overall" header="Overall Shipping %" sortable body={(r) => fmt.pct(r.shipping_pct_of_subtotal_overall)} />
                            </DataTable>
                        </div>
                    </Card>
                )}

                {/* State Table */}
                {shipByState.length > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                Shipping Efficiency by State / Province
                            </h3>
                            <DataTable value={[...shipByState].sort((a, b) => (+(b.total_shipping_cost ?? 0)) - (+(a.total_shipping_cost ?? 0)))}
                                paginator rows={15} scrollable stripedRows emptyMessage="No data" className="text-sm">
                                <Column field="country"                     header="Country"           sortable />
                                <Column field="state_province"              header="State/Province"    sortable />
                                <Column field="orders"                      header="Orders"            sortable body={(r) => fmt.number(r.orders)} />
                                <Column field="total_shipping_cost"         header="Shipping Cost"     sortable body={(r) => fmt.currency(r.total_shipping_cost)} />
                                <Column field="total_subtotal"              header="Total Subtotal"    sortable body={(r) => fmt.currency(r.total_subtotal)} />
                                <Column field="avg_shipping_pct_of_subtotal" header="Avg Shipping %"  sortable body={(r) => (
                                    <Tag value={fmt.pct(r.avg_shipping_pct_of_subtotal)}
                                        severity={(+(r.avg_shipping_pct_of_subtotal ?? 0)) <= 5 ? 'success' : (+(r.avg_shipping_pct_of_subtotal ?? 0)) <= 12 ? 'warning' : 'danger'} />
                                )} />
                                <Column field="median_shipping_pct_of_subtotal" header="Median %" sortable body={(r) => fmt.pct(r.median_shipping_pct_of_subtotal)} />
                            </DataTable>
                        </div>
                    </Card>
                )}
            </section>
        </div>
    );
}
