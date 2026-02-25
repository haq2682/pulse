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

const suppLabel = (id) => `Supplier ${id}`;

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
// Main Component
// ---------------------------------------------------------------------------

export default function SupplierPerformance() {
    const fmt = useFormatters();
    const { businessId } = useParams();
    const toastRef = useRef(null);
    const { pipelineStatus } = usePipelineProgress();
    const { dateRange, setDateRange, quickFilter, isFiltered, applyQuickFilter, resetFilters } = useAnalyticsDateFilter();
    const { lastUpdate } = useAnalyticsWebSocket(businessId);

    const [rawSupplier, setRawSupplier] = useState(null);
    const [loading, setLoading] = useState(true);
    const [fetchError, setFetchError] = useState(false);
    const [dataMode, setDataMode] = useState('unknown');

    // -------------------------------------------------------------------------
    // Fetch
    // -------------------------------------------------------------------------

    const buildUrl = useCallback(() => {
        const base   = import.meta.env.VITE_API_URL || 'http://localhost:8000';
        const params = new URLSearchParams({ categories: 'supplier_analytics' });
        return `${base}/analytics/data/${businessId}?${params.toString()}`;
    }, [businessId]);

    const fetchData = useCallback(async () => {
        if (!businessId) return;
        try {
            setLoading(true);
            setFetchError(false);
            const res = await fetch(buildUrl());
            if (!res.ok) {
                setRawSupplier(null);
                return;
            }
            const json = await res.json();
            if (json.mode) setDataMode(json.mode);
            setRawSupplier(json.categories?.supplier_analytics ?? null);
        } catch {
            console.error('[SupplierPerformance] fetch error');
            setFetchError(true);
            setRawSupplier(null);
        } finally {
            setLoading(false);
        }
    }, [buildUrl, businessId]);

    useEffect(() => { fetchData(); }, [businessId]);           // eslint-disable-line
    useEffect(() => {
        if (!lastUpdate) return;
        fetchData();
        toastRef.current?.show({ severity: 'info', summary: 'Data Updated', detail: 'Analytics pipeline completed — refreshing supplier data.', life: 3000 });
    }, [lastUpdate]); // eslint-disable-line

    // -------------------------------------------------------------------------
    // Derived data
    // -------------------------------------------------------------------------

    const derived = useMemo(() => {
        if (!rawSupplier) return null;
        const a = rawSupplier.analytics ?? {};

        const rankingCore  = a.supplier_ranking_core?.data ?? [];
        const reliability  = a.supplier_reliability?.data  ?? [];
        const fulfillment  = a.supplier_fulfillment_performance?.data ?? [];

        if (rankingCore.length === 0 && reliability.length === 0) return null;

        // ---- KPIs -----------------------------------------------------------
        const totalSuppliers   = rankingCore.length;
        const preferredCount   = rankingCore.filter((r) => r.is_preferred).length;
        const verifiedCount    = rankingCore.filter((r) => r.is_verified).length;
        const avgPerfScore     = totalSuppliers > 0
            ? rankingCore.reduce((s, r) => s + (r.supplier_performance_score ?? 0), 0) / totalSuppliers
            : 0;
        const bestRow          = rankingCore.reduce(
            (best, r) => (!best || (r.supplier_performance_score ?? 0) > (best.supplier_performance_score ?? 0)) ? r : best,
            null,
        );

        // ---- Performance Score (horizontal bar, top 15) ---------------------
        const perfSorted = [...rankingCore]
            .sort((a, b) => (b.supplier_performance_score ?? 0) - (a.supplier_performance_score ?? 0))
            .slice(0, 15);
        const perfBarData = {
            labels: perfSorted.map((r) => suppLabel(r.supplier_id)),
            datasets: [{
                label: 'Performance Score',
                data: perfSorted.map((r) => +(r.supplier_performance_score ?? 0).toFixed(2)),
                backgroundColor: PALETTE,
            }],
        };

        // ---- Reliability Score (bar, top 15) --------------------------------
        const relSorted = [...reliability]
            .sort((a, b) => (b.supplier_reliability_score_effective ?? 0) - (a.supplier_reliability_score_effective ?? 0))
            .slice(0, 15);
        const relBarData = {
            labels: relSorted.map((r) => suppLabel(r.supplier_id)),
            datasets: [{
                label: 'Reliability Score',
                data: relSorted.map((r) => +(r.supplier_reliability_score_effective ?? 0).toFixed(2)),
                backgroundColor: 'rgba(34,197,94,0.82)',
            }],
        };

        // ---- Stock Efficiency Ratio (bar, top 12) ---------------------------
        const effSorted = [...rankingCore]
            .filter((r) => (r.stock_efficiency_ratio ?? 0) > 0)
            .sort((a, b) => (b.stock_efficiency_ratio ?? 0) - (a.stock_efficiency_ratio ?? 0))
            .slice(0, 12);
        const effBarData = {
            labels: effSorted.map((r) => suppLabel(r.supplier_id)),
            datasets: [{
                label: 'Stock Efficiency Ratio',
                data: effSorted.map((r) => +(r.stock_efficiency_ratio ?? 0).toFixed(3)),
                backgroundColor: 'rgba(6,182,212,0.82)',
            }],
        };

        // ---- Inventory Health Score (bar, top 12) ---------------------------
        const healthSorted = [...rankingCore]
            .filter((r) => (r.supplier_inventory_health_score ?? 0) > 0)
            .sort((a, b) => (b.supplier_inventory_health_score ?? 0) - (a.supplier_inventory_health_score ?? 0))
            .slice(0, 12);
        const healthBarData = {
            labels: healthSorted.map((r) => suppLabel(r.supplier_id)),
            datasets: [{
                label: 'Inventory Health Score',
                data: healthSorted.map((r) => +(r.supplier_inventory_health_score ?? 0).toFixed(2)),
                backgroundColor: 'rgba(20,184,166,0.82)',
            }],
        };

        // ---- Preferred vs Not Preferred (doughnut) --------------------------
        const notPreferred = totalSuppliers - preferredCount;
        const preferredDoughnut = {
            labels: ['Preferred', 'Standard'],
            datasets: [{
                data: [preferredCount, notPreferred],
                backgroundColor: ['rgba(34,197,94,0.82)', 'rgba(156,163,175,0.82)'],
                borderWidth: 2,
            }],
        };

        // ---- Supplier Status distribution (doughnut) ------------------------
        const statusCounts = {};
        rankingCore.forEach((r) => {
            const s = r.supplier_status ?? 'Unknown';
            statusCounts[s] = (statusCounts[s] ?? 0) + 1;
        });
        const statusLabels = Object.keys(statusCounts);
        const statusColors = {
            'Active':   'rgba(34,197,94,0.82)',
            'Inactive': 'rgba(239,68,68,0.82)',
            'Pending':  'rgba(234,179,8,0.82)',
        };
        const statusDoughnut = {
            labels: statusLabels,
            datasets: [{
                data: statusLabels.map((s) => statusCounts[s]),
                backgroundColor: statusLabels.map((s) => statusColors[s] ?? 'rgba(156,163,175,0.82)'),
                borderWidth: 2,
            }],
        };

        // ---- Verified vs Unverified (doughnut) ------------------------------
        const notVerified = totalSuppliers - verifiedCount;
        const verifiedDoughnut = {
            labels: ['Verified', 'Unverified'],
            datasets: [{
                data: [verifiedCount, notVerified],
                backgroundColor: ['rgba(59,130,246,0.82)', 'rgba(249,115,22,0.82)'],
                borderWidth: 2,
            }],
        };

        // ---- Merged ranking table -------------------------------------------
        const fulfillMap = Object.fromEntries(fulfillment.map((r) => [r.supplier_id, r]));
        const relMap     = Object.fromEntries(reliability.map((r) => [r.supplier_id, r]));

        const mergedRanking = rankingCore.map((r) => ({
            ...r,
            supplier_reliability_score_effective: relMap[r.supplier_id]?.supplier_reliability_score_effective,
            avg_restock_lead_time:                fulfillMap[r.supplier_id]?.avg_restock_lead_time,
            total_orders_fulfilled:               fulfillMap[r.supplier_id]?.total_orders_fulfilled ?? r.total_orders_fulfilled,
        })).sort((a, b) => (b.supplier_performance_score ?? 0) - (a.supplier_performance_score ?? 0));

        return {
            totalSuppliers, preferredCount, verifiedCount, avgPerfScore, bestRow,
            perfBarData, relBarData, effBarData, healthBarData,
            preferredDoughnut, statusDoughnut, verifiedDoughnut,
            mergedRanking,
        };
    }, [rawSupplier]);

    const hasData = !!(derived && derived.totalSuppliers > 0);

    // -------------------------------------------------------------------------
    // Chart options
    // -------------------------------------------------------------------------

    const hBarOpts = (title) => ({
        responsive: true, maintainAspectRatio: false, indexAxis: 'y',
        plugins: {
            legend: { display: false },
            title: { display: !!title, text: title },
        },
        scales: { x: { beginAtZero: true }, y: { ticks: { font: { size: 11 } } } },
    });

    const barOpts = (title) => ({
        responsive: true, maintainAspectRatio: false,
        plugins: {
            legend: { display: false },
            title: { display: !!title, text: title },
        },
        scales: { x: { ticks: { maxRotation: 45 } }, y: { beginAtZero: true } },
    });

    const doughnutOpts = (title) => ({
        responsive: true, maintainAspectRatio: false,
        plugins: {
            legend: { position: 'right' },
            title: { display: !!title, text: title },
        },
    });

    // -------------------------------------------------------------------------
    // Render states
    // -------------------------------------------------------------------------

    if (loading && pipelineStatus !== 'loading') {
        return (
            <div className="flex flex-col items-center justify-center min-h-[400px] gap-4">
                <ProgressSpinner />
                <p className="text-gray-500 text-base">Loading supplier performance…</p>
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
                        <p className="text-gray-500 text-sm mt-1">Unable to load supplier data. Please try again later.</p>
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
    const {
        totalSuppliers, preferredCount, avgPerfScore, bestRow,
        perfBarData, relBarData, effBarData, healthBarData,
        preferredDoughnut, statusDoughnut, verifiedDoughnut,
        mergedRanking,
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
                    * Supplier analytics are static aggregates computed over all available data and do not change with the date filter.
                </p>
            )}

            {/* ── KPI Cards ──────────────────────────────────────────────── */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <KPICard
                    icon="pi-users" iconBg="bg-blue-100" iconColor="text-blue-600"
                    value={fmt.number(totalSuppliers)}
                    label="Total Suppliers"
                />
                <KPICard
                    icon="pi-star-fill" iconBg="bg-green-100" iconColor="text-green-600"
                    value={fmt.number(preferredCount)}
                    label="Preferred Suppliers"
                />
                <KPICard
                    icon="pi-trophy" iconBg="bg-amber-100" iconColor="text-amber-600"
                    value={bestRow ? suppLabel(bestRow.supplier_id) : '—'}
                    label="Top Performer"
                />
                <KPICard
                    icon="pi-chart-bar" iconBg="bg-purple-100" iconColor="text-purple-600"
                    value={fmt.decimal(avgPerfScore, 1)}
                    label="Avg Performance Score"
                />
            </div>

            {/* ── Performance & Reliability Scores ───────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-blue-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Score Rankings</h2>
                </div>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                    <ChartWrapper title="Performance Score — Top 15 Suppliers" height={380}>
                        <Bar data={perfBarData} options={hBarOpts()} />
                    </ChartWrapper>
                    <ChartWrapper title="Reliability Score — Top 15 Suppliers" height={380}>
                        <Bar data={relBarData} options={hBarOpts()} />
                    </ChartWrapper>
                </div>
            </section>

            {/* ── Efficiency & Health ────────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-cyan-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Efficiency & Health</h2>
                </div>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                    {effBarData.labels.length > 0 && (
                        <ChartWrapper title="Stock Efficiency Ratio — Top 12" height={340}>
                            <Bar data={effBarData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                    {healthBarData.labels.length > 0 && (
                        <ChartWrapper title="Inventory Health Score — Top 12" height={340}>
                            <Bar data={healthBarData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                </div>
            </section>

            {/* ── Supplier Composition ───────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-green-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Supplier Composition</h2>
                </div>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                    <ChartWrapper title="Preferred vs Standard" height={280}>
                        <Doughnut data={preferredDoughnut} options={doughnutOpts()} />
                    </ChartWrapper>
                    <ChartWrapper title="Supplier Status Distribution" height={280}>
                        <Doughnut data={statusDoughnut} options={doughnutOpts()} />
                    </ChartWrapper>
                    <ChartWrapper title="Verified vs Unverified" height={280}>
                        <Doughnut data={verifiedDoughnut} options={doughnutOpts()} />
                    </ChartWrapper>
                </div>
            </section>

            {/* ── Full Supplier Ranking Table ────────────────────────────── */}
            <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                <div className="p-6">
                    <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                        Supplier Performance Rankings
                    </h3>
                    <DataTable
                        value={mergedRanking}
                        paginator rows={15} rowsPerPageOptions={[15, 25, 50]}
                        className="p-datatable-sm" stripedRows sortMode="multiple"
                    >
                        <Column field="supplier_id" header="Supplier" sortable body={(r) => suppLabel(r.supplier_id)} />
                        <Column field="supplier_status" header="Status" sortable
                            body={(r) => <Tag value={r.supplier_status ?? '—'} severity={r.supplier_status === 'Active' ? 'success' : 'warning'} />} />
                        <Column field="is_preferred" header="Preferred"
                            body={(r) => r.is_preferred ? <Tag value="Yes" severity="success" /> : <Tag value="No" severity="secondary" />} />
                        <Column field="is_verified" header="Verified"
                            body={(r) => r.is_verified ? <Tag value="Yes" severity="info" /> : <Tag value="No" severity="secondary" />} />
                        <Column field="supplier_performance_score" header="Perf. Score" sortable
                            body={(r) => fmt.decimal(r.supplier_performance_score, 1)} />
                        <Column field="supplier_reliability_score_effective" header="Reliability" sortable
                            body={(r) => fmt.decimal(r.supplier_reliability_score_effective, 1)} />
                        <Column field="stock_efficiency_ratio" header="Efficiency Ratio" sortable
                            body={(r) => fmt.decimal(r.stock_efficiency_ratio, 3)} />
                        <Column field="supplier_inventory_health_score" header="Inv. Health" sortable
                            body={(r) => fmt.decimal(r.supplier_inventory_health_score, 1)} />
                        <Column field="total_products_supplied" header="Products" sortable
                            body={(r) => fmt.number(r.total_products_supplied)} />
                        <Column field="total_stockouts" header="Stockouts" sortable
                            body={(r) => fmt.number(r.total_stockouts)} />
                        <Column field="stockout_rate" header="Stockout Rate" sortable
                            body={(r) => fmt.pct((r.stockout_rate ?? 0) * 100)} />
                        <Column field="avg_restock_lead_time" header="Lead Time (d)" sortable
                            body={(r) => r.avg_restock_lead_time != null ? fmt.decimal(r.avg_restock_lead_time, 1) : '—'} />
                    </DataTable>
                </div>
            </Card>
        </div>
    );
}
