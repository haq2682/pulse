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

export default function SupplierEconomics() {
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
            console.error('[SupplierEconomics] fetch error');
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

        const revContrib    = a.supplier_revenue_contribution?.data            ?? [];
        const profitMargin  = a.supplier_profit_margin?.data                   ?? [];
        const carryingCost  = a.inventory_carrying_cost_by_supplier?.data      ?? [];
        const costEff       = a.storage_cost_efficiency_by_supplier?.data      ?? [];

        if (revContrib.length === 0 && profitMargin.length === 0 && carryingCost.length === 0) return null;

        // ---- KPIs -----------------------------------------------------------
        const totalRevenue      = revContrib.reduce((s, r) => s + (r.total_revenue_generated ?? 0), 0);
        const totalCarryingCost = carryingCost.reduce((s, r) => s + (r.total_storage_cost ?? 0), 0);
        const avgProfitMargin   = profitMargin.length > 0
            ? profitMargin.reduce((s, r) => s + (r.avg_profit_margin ?? 0), 0) / profitMargin.length
            : 0;
        const bestRevenueRow    = revContrib.reduce(
            (best, r) => (!best || (r.total_revenue_generated ?? 0) > (best.total_revenue_generated ?? 0)) ? r : best,
            null,
        );

        // ---- Revenue Contribution Doughnut (top 8 by share) ----------------
        const revShareSorted = [...revContrib]
            .sort((a, b) => (b.revenue_contribution_share ?? 0) - (a.revenue_contribution_share ?? 0))
            .slice(0, 8);
        const revDoughnut = {
            labels: revShareSorted.map((r) => suppLabel(r.supplier_id)),
            datasets: [{
                data: revShareSorted.map((r) => +((r.revenue_contribution_share ?? 0) * 100).toFixed(2)),
                backgroundColor: PALETTE,
                borderWidth: 2,
            }],
        };

        // ---- Total Revenue Generated (bar, sorted desc, top 12) ------------
        const revSorted = [...revContrib]
            .sort((a, b) => (b.total_revenue_generated ?? 0) - (a.total_revenue_generated ?? 0))
            .slice(0, 12);
        const revBarData = {
            labels: revSorted.map((r) => suppLabel(r.supplier_id)),
            datasets: [{
                label: 'Total Revenue Generated',
                data: revSorted.map((r) => r.total_revenue_generated ?? 0),
                backgroundColor: PALETTE,
            }],
        };

        // ---- Avg Profit Margin (bar, sorted desc, top 12) ------------------
        const marginSorted = [...profitMargin]
            .sort((a, b) => (b.avg_profit_margin ?? 0) - (a.avg_profit_margin ?? 0))
            .slice(0, 12);
        const marginBarData = {
            labels: marginSorted.map((r) => suppLabel(r.supplier_id)),
            datasets: [{
                label: 'Avg Profit Margin',
                data: marginSorted.map((r) => +((r.avg_profit_margin ?? 0) * 100).toFixed(1)),
                backgroundColor: 'rgba(34,197,94,0.82)',
            }],
        };

        // ---- Carrying / Storage Cost (bar, sorted desc, top 12) ------------
        const costSorted = [...carryingCost]
            .sort((a, b) => (b.total_storage_cost ?? 0) - (a.total_storage_cost ?? 0))
            .slice(0, 12);
        const costBarData = {
            labels: costSorted.map((r) => suppLabel(r.supplier_id)),
            datasets: [{
                label: 'Total Inventory Carrying Cost',
                data: costSorted.map((r) => r.total_storage_cost ?? 0),
                backgroundColor: 'rgba(139,92,246,0.82)',
            }],
        };

        // ---- Avg Storage Cost per Unit (bar, sorted desc, top 12) ----------
        const costPerUnitSorted = [...costEff]
            .filter((r) => (r.avg_storage_cost_per_unit ?? 0) > 0)
            .sort((a, b) => (b.avg_storage_cost_per_unit ?? 0) - (a.avg_storage_cost_per_unit ?? 0))
            .slice(0, 12);
        const costPerUnitBarData = {
            labels: costPerUnitSorted.map((r) => suppLabel(r.supplier_id)),
            datasets: [{
                label: 'Avg Storage Cost per Unit',
                data: costPerUnitSorted.map((r) => +(r.avg_storage_cost_per_unit ?? 0).toFixed(2)),
                backgroundColor: 'rgba(249,115,22,0.82)',
            }],
        };

        // ---- Storage Cost Efficiency Score (bar, sorted desc, top 12) ------
        const effSorted = [...costEff]
            .filter((r) => (r.storage_cost_efficiency ?? 0) > 0)
            .sort((a, b) => (b.storage_cost_efficiency ?? 0) - (a.storage_cost_efficiency ?? 0))
            .slice(0, 12);
        const effBarData = {
            labels: effSorted.map((r) => suppLabel(r.supplier_id)),
            datasets: [{
                label: 'Storage Cost Efficiency Score',
                data: effSorted.map((r) => +(r.storage_cost_efficiency ?? 0).toFixed(2)),
                backgroundColor: 'rgba(6,182,212,0.82)',
            }],
        };

        // ---- Avg Order Value per Supplier (bar, sorted desc) ----------------
        const aovSorted = [...revContrib]
            .filter((r) => (r.avg_order_value ?? 0) > 0)
            .sort((a, b) => (b.avg_order_value ?? 0) - (a.avg_order_value ?? 0))
            .slice(0, 12);
        const aovBarData = {
            labels: aovSorted.map((r) => suppLabel(r.supplier_id)),
            datasets: [{
                label: 'Avg Order Value',
                data: aovSorted.map((r) => +(r.avg_order_value ?? 0).toFixed(0)),
                backgroundColor: 'rgba(234,179,8,0.82)',
            }],
        };

        // ---- Revenue vs Margin dual-column comparison (bar, top 10) --------
        const profitMap = Object.fromEntries(profitMargin.map((r) => [r.supplier_id, r]));
        const costMap   = Object.fromEntries(costEff.map((r) => [r.supplier_id, r]));
        const carryMap  = Object.fromEntries(carryingCost.map((r) => [r.supplier_id, r]));

        const mergedEconomics = revSorted.map((r) => ({
            ...r,
            avg_profit_margin:        profitMap[r.supplier_id]?.avg_profit_margin,
            total_profit_orders:      profitMap[r.supplier_id]?.total_orders_fulfilled,
            total_storage_cost:       carryMap[r.supplier_id]?.total_storage_cost,
            avg_storage_cost_per_unit: costMap[r.supplier_id]?.avg_storage_cost_per_unit,
            storage_cost_efficiency:  costMap[r.supplier_id]?.storage_cost_efficiency,
            supplier_inventory_health_score: costMap[r.supplier_id]?.supplier_inventory_health_score,
        }));

        // Also include suppliers that have cost data but not revenue data
        const inRevSet = new Set(revSorted.map((r) => r.supplier_id));
        const extraCostRows = costEff
            .filter((r) => !inRevSet.has(r.supplier_id))
            .map((r) => ({
                supplier_id: r.supplier_id,
                total_revenue_generated: null,
                revenue_contribution_share: null,
                avg_order_value: null,
                total_orders_fulfilled: null,
                avg_profit_margin: profitMap[r.supplier_id]?.avg_profit_margin,
                total_storage_cost: carryMap[r.supplier_id]?.total_storage_cost,
                avg_storage_cost_per_unit: r.avg_storage_cost_per_unit,
                storage_cost_efficiency: r.storage_cost_efficiency,
                supplier_inventory_health_score: r.supplier_inventory_health_score,
            }));

        const fullEconomics = [...mergedEconomics, ...extraCostRows]
            .sort((a, b) => (b.total_revenue_generated ?? 0) - (a.total_revenue_generated ?? 0));

        return {
            totalRevenue, totalCarryingCost, avgProfitMargin, bestRevenueRow,
            revDoughnut, revBarData, marginBarData, costBarData,
            costPerUnitBarData, effBarData, aovBarData,
            fullEconomics,
        };
    }, [rawSupplier]);

    const hasData = !!(derived && derived.fullEconomics?.length > 0);

    // -------------------------------------------------------------------------
    // Chart options
    // -------------------------------------------------------------------------

    const currencyBarOpts = (title) => ({
        responsive: true, maintainAspectRatio: false,
        plugins: {
            legend: { display: false },
            title: { display: !!title, text: title },
            tooltip: { callbacks: { label: (ctx) => fmt.currency(ctx.raw) } },
        },
        scales: {
            x: { ticks: { maxRotation: 45 } },
            y: { beginAtZero: true, ticks: { callback: (v) => fmt.compact(v) } },
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

    const barOpts = (title) => ({
        responsive: true, maintainAspectRatio: false,
        plugins: {
            legend: { display: false },
            title: { display: !!title, text: title },
        },
        scales: { x: { ticks: { maxRotation: 45 } }, y: { beginAtZero: true } },
    });

    const doughnutOpts = {
        responsive: true, maintainAspectRatio: false,
        plugins: {
            legend: { position: 'right' },
            tooltip: { callbacks: { label: (ctx) => `${ctx.label}: ${fmt.decimal(ctx.raw, 1)}%` } },
        },
    };

    // -------------------------------------------------------------------------
    // Render states
    // -------------------------------------------------------------------------

    if (loading && pipelineStatus !== 'loading') {
        return (
            <div className="flex flex-col items-center justify-center min-h-[400px] gap-4">
                <ProgressSpinner />
                <p className="text-gray-500 text-base">Loading supplier economics…</p>
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
        totalRevenue, totalCarryingCost, avgProfitMargin, bestRevenueRow,
        revDoughnut, revBarData, marginBarData, costBarData,
        costPerUnitBarData, effBarData, aovBarData,
        fullEconomics,
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
                    icon="pi-dollar" iconBg="bg-blue-100" iconColor="text-blue-600"
                    value={fmt.compact(totalRevenue)}
                    label="Total Supplier Revenue"
                />
                <KPICard
                    icon="pi-percentage" iconBg="bg-green-100" iconColor="text-green-600"
                    value={fmt.pct(avgProfitMargin * 100)}
                    label="Avg Profit Margin"
                />
                <KPICard
                    icon="pi-warehouse" iconBg="bg-purple-100" iconColor="text-purple-600"
                    value={fmt.compact(totalCarryingCost)}
                    label="Total Carrying Cost"
                />
                <KPICard
                    icon="pi-trophy" iconBg="bg-amber-100" iconColor="text-amber-600"
                    value={bestRevenueRow ? suppLabel(bestRevenueRow.supplier_id) : '—'}
                    label="Top Revenue Supplier"
                />
            </div>

            {/* ── Revenue Contribution ───────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-blue-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Revenue Contribution</h2>
                </div>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                    {revDoughnut.labels.length > 0 && (
                        <ChartWrapper title="Revenue Share — Top 8 Suppliers" height={320}>
                            <Doughnut data={revDoughnut} options={doughnutOpts} />
                        </ChartWrapper>
                    )}
                    {revBarData.labels.length > 0 && (
                        <ChartWrapper title="Total Revenue Generated — Top 12" height={320}>
                            <Bar data={revBarData} options={currencyBarOpts()} />
                        </ChartWrapper>
                    )}
                </div>

                {aovBarData.labels.length > 0 && (
                    <ChartWrapper title="Avg Order Value per Supplier — Top 12" height={320}>
                        <Bar data={aovBarData} options={currencyBarOpts()} />
                    </ChartWrapper>
                )}
            </section>

            {/* ── Profitability ──────────────────────────────────────────── */}
            {marginBarData.labels.length > 0 && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-green-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Profitability</h2>
                    </div>
                    <ChartWrapper title="Avg Profit Margin by Supplier — Top 12" height={340}>
                        <Bar data={marginBarData} options={pctBarOpts()} />
                    </ChartWrapper>
                </section>
            )}

            {/* ── Inventory Carrying Costs ───────────────────────────────── */}
            {(costBarData.labels.length > 0 || costPerUnitBarData.labels.length > 0 || effBarData.labels.length > 0) && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-purple-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Inventory Carrying Costs</h2>
                    </div>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                        {costBarData.labels.length > 0 && (
                            <ChartWrapper title="Total Inventory Carrying Cost — Top 12" height={340}>
                                <Bar data={costBarData} options={currencyBarOpts()} />
                            </ChartWrapper>
                        )}
                        {costPerUnitBarData.labels.length > 0 && (
                            <ChartWrapper title="Avg Storage Cost per Unit — Top 12 (highest first)" height={340}>
                                <Bar data={costPerUnitBarData} options={currencyBarOpts()} />
                            </ChartWrapper>
                        )}
                    </div>
                    {effBarData.labels.length > 0 && (
                        <ChartWrapper title="Storage Cost Efficiency Score — Top 12" height={320}>
                            <Bar data={effBarData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                </section>
            )}

            {/* ── Full Economics Table ───────────────────────────────────── */}
            <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                <div className="p-6">
                    <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                        Supplier Economics Summary
                    </h3>
                    <DataTable
                        value={fullEconomics}
                        paginator rows={15} rowsPerPageOptions={[15, 25, 50]}
                        className="p-datatable-sm" stripedRows sortMode="multiple"
                    >
                        <Column field="supplier_id" header="Supplier" sortable body={(r) => suppLabel(r.supplier_id)} />
                        <Column field="total_revenue_generated" header="Revenue" sortable
                            body={(r) => r.total_revenue_generated != null ? fmt.currency(r.total_revenue_generated) : '—'} />
                        <Column field="revenue_contribution_share" header="Rev. Share" sortable
                            body={(r) => r.revenue_contribution_share != null ? fmt.pct(r.revenue_contribution_share * 100) : '—'} />
                        <Column field="total_orders_fulfilled" header="Orders" sortable
                            body={(r) => r.total_orders_fulfilled != null ? fmt.number(r.total_orders_fulfilled) : '—'} />
                        <Column field="avg_order_value" header="Avg Order Value" sortable
                            body={(r) => r.avg_order_value != null ? fmt.currency(r.avg_order_value) : '—'} />
                        <Column field="avg_profit_margin" header="Profit Margin" sortable
                            body={(r) => r.avg_profit_margin != null ? fmt.pct(r.avg_profit_margin * 100) : '—'} />
                        <Column field="total_storage_cost" header="Carrying Cost" sortable
                            body={(r) => r.total_storage_cost != null ? fmt.currency(r.total_storage_cost) : '—'} />
                        <Column field="avg_storage_cost_per_unit" header="Cost / Unit" sortable
                            body={(r) => r.avg_storage_cost_per_unit != null ? fmt.currency(r.avg_storage_cost_per_unit) : '—'} />
                        <Column field="storage_cost_efficiency" header="Cost Efficiency" sortable
                            body={(r) => r.storage_cost_efficiency != null ? fmt.decimal(r.storage_cost_efficiency, 2) : '—'} />
                        <Column field="supplier_inventory_health_score" header="Inv. Health" sortable
                            body={(r) => r.supplier_inventory_health_score != null ? fmt.decimal(r.supplier_inventory_health_score, 1) : '—'} />
                    </DataTable>
                </div>
            </Card>
        </div>
    );
}
