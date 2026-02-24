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

const CONTRACT_TAG = {
    'Active':   'success',
    'Expiring': 'warning',
    'Expired':  'danger',
    'Critical': 'danger',
};

const CONTRACT_COLOR = {
    'Active':   'rgba(34,197,94,0.82)',
    'Expiring': 'rgba(234,179,8,0.82)',
    'Expired':  'rgba(239,68,68,0.82)',
    'Critical': 'rgba(239,68,68,0.82)',
};

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

export default function SupplierOperations() {
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
            console.error('[SupplierOperations] fetch error');
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

        const fulfillment    = a.supplier_fulfillment_performance?.data ?? [];
        const stockoutBySup  = a.stockout_rate_by_supplier?.data        ?? [];
        const supplierStockouts = a.supplier_stockouts?.data            ?? [];
        const lastRestock    = a.supplier_days_since_last_restock?.data ?? [];
        const contractExp    = a.supplier_contract_expiry?.data         ?? [];

        if (fulfillment.length === 0 && stockoutBySup.length === 0 && contractExp.length === 0) return null;

        // ---- KPIs -----------------------------------------------------------
        const totalOrdersFulfilled = fulfillment.reduce((s, r) => s + (r.total_orders_fulfilled ?? 0), 0);
        const avgLeadTime = fulfillment.length > 0
            ? fulfillment.reduce((s, r) => s + (r.avg_restock_lead_time ?? 0), 0) / fulfillment.length
            : 0;
        const avgStockoutRate = stockoutBySup.length > 0
            ? stockoutBySup.reduce((s, r) => s + (r.supplier_stockout_rate ?? 0), 0) / stockoutBySup.length
            : 0;
        const expiringContracts = contractExp.filter(
            (r) => r.contract_status_flag === 'Expiring' || r.contract_status_flag === 'Critical'
        ).length;

        // ---- Restock Lead Time (bar, sorted desc, top 12) ------------------
        const leadSorted = [...fulfillment]
            .filter((r) => (r.avg_restock_lead_time ?? 0) > 0)
            .sort((a, b) => (b.avg_restock_lead_time ?? 0) - (a.avg_restock_lead_time ?? 0))
            .slice(0, 12);
        const leadTimeBarData = {
            labels: leadSorted.map((r) => suppLabel(r.supplier_id)),
            datasets: [{
                label: 'Avg Restock Lead Time (days)',
                data: leadSorted.map((r) => +(r.avg_restock_lead_time ?? 0).toFixed(1)),
                backgroundColor: 'rgba(249,115,22,0.82)',
            }],
        };

        // ---- Stockout Rate (bar, sorted desc, red gradient) ----------------
        const stockoutSorted = [...stockoutBySup]
            .sort((a, b) => (b.supplier_stockout_rate ?? 0) - (a.supplier_stockout_rate ?? 0))
            .slice(0, 12);
        const stockoutBarData = {
            labels: stockoutSorted.map((r) => suppLabel(r.supplier_id)),
            datasets: [{
                label: 'Stockout Rate',
                data: stockoutSorted.map((r) => +((r.supplier_stockout_rate ?? 0) * 100).toFixed(1)),
                backgroundColor: 'rgba(239,68,68,0.82)',
            }],
        };

        // ---- Total Stockouts Count (bar, sorted desc) ----------------------
        const totalStockoutSorted = [...stockoutBySup]
            .sort((a, b) => (b.total_stockouts ?? 0) - (a.total_stockouts ?? 0))
            .slice(0, 12);
        const totalStockoutsBarData = {
            labels: totalStockoutSorted.map((r) => suppLabel(r.supplier_id)),
            datasets: [{
                label: 'Total Stockouts',
                data: totalStockoutSorted.map((r) => r.total_stockouts ?? 0),
                backgroundColor: 'rgba(239,68,68,0.65)',
            }],
        };

        // ---- Total Orders Fulfilled (bar, sorted desc) ---------------------
        const ordersSorted = [...fulfillment]
            .sort((a, b) => (b.total_orders_fulfilled ?? 0) - (a.total_orders_fulfilled ?? 0))
            .slice(0, 12);
        const ordersBarData = {
            labels: ordersSorted.map((r) => suppLabel(r.supplier_id)),
            datasets: [{
                label: 'Total Orders Fulfilled',
                data: ordersSorted.map((r) => r.total_orders_fulfilled ?? 0),
                backgroundColor: PALETTE,
            }],
        };

        // ---- Days Since Last Restock (bar, sorted desc) --------------------
        const restockSorted = [...lastRestock]
            .filter((r) => (r.days_since_last_restock ?? 0) > 0)
            .sort((a, b) => (b.days_since_last_restock ?? 0) - (a.days_since_last_restock ?? 0))
            .slice(0, 12);
        const restockBarData = {
            labels: restockSorted.map((r) => suppLabel(r.supplier_id)),
            datasets: [{
                label: 'Days Since Last Restock',
                data: restockSorted.map((r) => r.days_since_last_restock ?? 0),
                backgroundColor: restockSorted.map((r) => {
                    const d = r.days_since_last_restock ?? 0;
                    if (d > 90)  return 'rgba(239,68,68,0.82)';
                    if (d > 30)  return 'rgba(234,179,8,0.82)';
                    return 'rgba(34,197,94,0.82)';
                }),
            }],
        };

        // ---- Inventory Health Score (bar, sorted desc from stockout table) -
        const healthSorted = [...stockoutBySup]
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

        // ---- Contract Expiry (next 365 days, sorted by days remaining) -----
        const contractSorted = [...contractExp]
            .filter((r) => (r.days_until_contract_expiry ?? 9999) < 365)
            .sort((a, b) => (a.days_until_contract_expiry ?? 9999) - (b.days_until_contract_expiry ?? 9999))
            .slice(0, 12);
        const contractBarData = {
            labels: contractSorted.map((r) => suppLabel(r.supplier_id)),
            datasets: [{
                label: 'Days Until Expiry',
                data: contractSorted.map((r) => r.days_until_contract_expiry ?? 0),
                backgroundColor: contractSorted.map((r) => CONTRACT_COLOR[r.contract_status_flag] ?? 'rgba(59,130,246,0.82)'),
            }],
        };

        // ---- Merged operational table (fulfillment + stockout + restock) ---
        const stockoutMap = Object.fromEntries(stockoutBySup.map((r) => [r.supplier_id, r]));
        const restockMap  = Object.fromEntries(lastRestock.map((r) => [r.supplier_id, r]));

        const mergedOps = fulfillment.map((r) => ({
            ...r,
            supplier_stockout_rate:        stockoutMap[r.supplier_id]?.supplier_stockout_rate,
            total_stockouts:               stockoutMap[r.supplier_id]?.total_stockouts,
            supplier_inventory_health_score: stockoutMap[r.supplier_id]?.supplier_inventory_health_score,
            days_since_last_restock:       restockMap[r.supplier_id]?.days_since_last_restock,
        })).sort((a, b) => (b.total_orders_fulfilled ?? 0) - (a.total_orders_fulfilled ?? 0));

        return {
            totalOrdersFulfilled, avgLeadTime, avgStockoutRate, expiringContracts,
            leadTimeBarData, stockoutBarData, totalStockoutsBarData, ordersBarData,
            restockBarData, healthBarData, contractBarData,
            mergedOps, contractExp, supplierStockouts,
        };
    }, [rawSupplier]);

    const hasData = !!(derived && (derived.mergedOps?.length > 0 || derived.contractExp?.length > 0));

    // -------------------------------------------------------------------------
    // Chart options
    // -------------------------------------------------------------------------

    const barOpts = (title, yLabel) => ({
        responsive: true, maintainAspectRatio: false,
        plugins: {
            legend: { display: false },
            title: { display: !!title, text: title },
        },
        scales: {
            x: { ticks: { maxRotation: 45 } },
            y: { beginAtZero: true, title: { display: !!yLabel, text: yLabel } },
        },
    });

    const pctBarOpts = (title) => ({
        responsive: true, maintainAspectRatio: false,
        plugins: {
            legend: { display: false },
            title: { display: !!title, text: title },
            tooltip: { callbacks: { label: (ctx) => `${ctx.parsed.y.toFixed(1)}%` } },
        },
        scales: {
            x: { ticks: { maxRotation: 45 } },
            y: { beginAtZero: true, ticks: { callback: (v) => `${v}%` } },
        },
    });

    // -------------------------------------------------------------------------
    // Render states
    // -------------------------------------------------------------------------

    if (loading && pipelineStatus !== 'loading') {
        return (
            <div className="flex flex-col items-center justify-center min-h-[400px] gap-4">
                <ProgressSpinner />
                <p className="text-gray-500 text-base">Loading supplier operations…</p>
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
        totalOrdersFulfilled, avgLeadTime, avgStockoutRate, expiringContracts,
        leadTimeBarData, stockoutBarData, totalStockoutsBarData, ordersBarData,
        restockBarData, healthBarData, contractBarData,
        mergedOps, contractExp,
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
                    icon="pi-shopping-cart" iconBg="bg-blue-100" iconColor="text-blue-600"
                    value={fmt.compact(totalOrdersFulfilled)}
                    label="Total Orders Fulfilled"
                />
                <KPICard
                    icon="pi-clock" iconBg="bg-amber-100" iconColor="text-amber-600"
                    value={`${fmt.decimal(avgLeadTime, 1)}d`}
                    label="Avg Restock Lead Time"
                />
                <KPICard
                    icon="pi-times-circle" iconBg="bg-red-100" iconColor="text-red-600"
                    value={fmt.pct(avgStockoutRate * 100)}
                    label="Avg Stockout Rate"
                />
                <KPICard
                    icon="pi-file-edit" iconBg="bg-orange-100" iconColor="text-orange-600"
                    value={fmt.number(expiringContracts)}
                    label="Contracts Expiring Soon"
                />
            </div>

            {/* ── Fulfillment Capacity ───────────────────────────────────── */}
            {ordersBarData.labels.length > 0 && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-blue-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Fulfillment Capacity</h2>
                    </div>
                    <ChartWrapper title="Total Orders Fulfilled per Supplier" height={340}>
                        <Bar data={ordersBarData} options={barOpts()} />
                    </ChartWrapper>
                </section>
            )}

            {/* ── Lead Time & Restock Timing ─────────────────────────────── */}
            {(leadTimeBarData.labels.length > 0 || restockBarData.labels.length > 0) && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-amber-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Lead Time & Restock Timing</h2>
                    </div>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                        {leadTimeBarData.labels.length > 0 && (
                            <ChartWrapper title="Avg Restock Lead Time — Top 12 (slowest first)" height={340}>
                                <Bar data={leadTimeBarData} options={barOpts(null, 'Days')} />
                            </ChartWrapper>
                        )}
                        {restockBarData.labels.length > 0 && (
                            <ChartWrapper title="Days Since Last Restock (green < 30d, amber 30-90d, red > 90d)" height={340}>
                                <Bar data={restockBarData} options={barOpts(null, 'Days')} />
                            </ChartWrapper>
                        )}
                    </div>
                </section>
            )}

            {/* ── Stockout Analysis ──────────────────────────────────────── */}
            {(stockoutBarData.labels.length > 0 || totalStockoutsBarData.labels.length > 0) && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-red-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Stockout Analysis</h2>
                    </div>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                        {stockoutBarData.labels.length > 0 && (
                            <ChartWrapper title="Stockout Rate by Supplier — Top 12 (highest first)" height={340}>
                                <Bar data={stockoutBarData} options={pctBarOpts()} />
                            </ChartWrapper>
                        )}
                        {totalStockoutsBarData.labels.length > 0 && (
                            <ChartWrapper title="Total Stockouts per Supplier — Top 12" height={340}>
                                <Bar data={totalStockoutsBarData} options={barOpts()} />
                            </ChartWrapper>
                        )}
                    </div>
                </section>
            )}

            {/* ── Inventory Health ───────────────────────────────────────── */}
            {healthBarData.labels.length > 0 && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-teal-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Inventory Health</h2>
                    </div>
                    <ChartWrapper title="Supplier Inventory Health Score — Top 12" height={320}>
                        <Bar data={healthBarData} options={barOpts()} />
                    </ChartWrapper>
                </section>
            )}

            {/* ── Contract Expiry Monitoring ─────────────────────────────── */}
            {contractBarData.labels.length > 0 && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-purple-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Contract Expiry (Next 12 Months)</h2>
                    </div>
                    <ChartWrapper title="Days Until Contract Expiry — Suppliers Expiring Within 365 Days" height={320}>
                        <Bar data={contractBarData} options={barOpts(null, 'Days')} />
                    </ChartWrapper>
                </section>
            )}

            {/* ── Fulfillment Operations Table ───────────────────────────── */}
            {mergedOps.length > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                            Fulfillment Operations Summary
                        </h3>
                        <DataTable
                            value={mergedOps}
                            paginator rows={15} rowsPerPageOptions={[15, 25, 50]}
                            className="p-datatable-sm" stripedRows sortMode="multiple"
                        >
                            <Column field="supplier_id" header="Supplier" sortable body={(r) => suppLabel(r.supplier_id)} />
                            <Column field="supplier_status" header="Status" sortable
                                body={(r) => <Tag value={r.supplier_status ?? '—'} severity={r.supplier_status === 'Active' ? 'success' : 'warning'} />} />
                            <Column field="total_orders_fulfilled" header="Orders Fulfilled" sortable body={(r) => fmt.number(r.total_orders_fulfilled)} />
                            <Column field="total_units_sold" header="Units Sold" sortable body={(r) => fmt.number(r.total_units_sold)} />
                            <Column field="avg_restock_lead_time" header="Lead Time (d)" sortable
                                body={(r) => r.avg_restock_lead_time != null ? fmt.decimal(r.avg_restock_lead_time, 1) : '—'} />
                            <Column field="supplier_stockout_rate" header="Stockout Rate" sortable
                                body={(r) => r.supplier_stockout_rate != null ? fmt.pct(r.supplier_stockout_rate * 100) : '—'} />
                            <Column field="total_stockouts" header="Total Stockouts" sortable
                                body={(r) => r.total_stockouts != null ? fmt.number(r.total_stockouts) : '—'} />
                            <Column field="supplier_inventory_health_score" header="Inv. Health" sortable
                                body={(r) => r.supplier_inventory_health_score != null ? fmt.decimal(r.supplier_inventory_health_score, 1) : '—'} />
                            <Column field="days_since_last_restock" header="Days Since Restock" sortable
                                body={(r) => r.days_since_last_restock != null ? fmt.number(r.days_since_last_restock) : '—'} />
                            <Column field="supplier_performance_score" header="Perf. Score" sortable
                                body={(r) => fmt.decimal(r.supplier_performance_score, 1)} />
                        </DataTable>
                    </div>
                </Card>
            )}

            {/* ── Contract Expiry Table ──────────────────────────────────── */}
            {contractExp.length > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                            Contract Expiry Monitoring
                        </h3>
                        <DataTable
                            value={[...contractExp].sort((a, b) => (a.days_until_contract_expiry ?? 9999) - (b.days_until_contract_expiry ?? 9999))}
                            paginator rows={15} rowsPerPageOptions={[15, 25, 50]}
                            className="p-datatable-sm" stripedRows sortMode="multiple"
                        >
                            <Column field="supplier_id" header="Supplier" sortable body={(r) => suppLabel(r.supplier_id)} />
                            <Column field="supplier_status" header="Status" sortable
                                body={(r) => <Tag value={r.supplier_status ?? '—'} severity={r.supplier_status === 'Active' ? 'success' : 'warning'} />} />
                            <Column field="contract_status_flag" header="Contract Status" sortable
                                body={(r) => <Tag value={r.contract_status_flag ?? '—'} severity={CONTRACT_TAG[r.contract_status_flag] ?? 'info'} />} />
                            <Column field="days_until_contract_expiry" header="Days Until Expiry" sortable
                                body={(r) => fmt.number(r.days_until_contract_expiry)} />
                            <Column field="contract_start_date" header="Start Date" sortable />
                            <Column field="contract_end_date" header="End Date" sortable />
                            <Column field="supplier_performance_score" header="Perf. Score" sortable
                                body={(r) => fmt.decimal(r.supplier_performance_score, 1)} />
                            <Column field="supplier_reliability_score_effective" header="Reliability" sortable
                                body={(r) => fmt.decimal(r.supplier_reliability_score_effective, 1)} />
                        </DataTable>
                    </div>
                </Card>
            )}
            {/* Supplier Stockouts Detail Table */}
            {(derived?.supplierStockouts?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                            Supplier Stockouts Detail
                        </h3>
                        <DataTable
                            value={[...derived.supplierStockouts].sort((a, b) => (b.total_stockouts ?? 0) - (a.total_stockouts ?? 0))}
                            paginator rows={15} rowsPerPageOptions={[15, 25]}
                            className="p-datatable-sm" stripedRows sortMode="multiple"
                        >
                            <Column field="supplier_id" header="Supplier" sortable body={(r) => suppLabel(r.supplier_id)} />
                            <Column field="total_stockouts" header="Total Stockouts" sortable body={(r) => fmt.number(r.total_stockouts)} />
                            <Column field="supplier_stockout_rate" header="Stockout Rate" sortable body={(r) => fmt.pct((r.supplier_stockout_rate ?? 0) * 100)} />
                            <Column field="inv_total_stockouts" header="Inv. Stockouts" sortable body={(r) => fmt.number(r.inv_total_stockouts)} />
                            <Column field="inv_stockout_rate" header="Inv. Stockout Rate" sortable body={(r) => fmt.pct((r.inv_stockout_rate ?? 0) * 100)} />
                            <Column field="inv_total_products" header="Total Products" sortable body={(r) => fmt.number(r.inv_total_products)} />
                            <Column field="inv_total_current_stock" header="Current Stock" sortable body={(r) => fmt.number(r.inv_total_current_stock)} />
                        </DataTable>
                    </div>
                </Card>
            )}
        </div>
    );
}
