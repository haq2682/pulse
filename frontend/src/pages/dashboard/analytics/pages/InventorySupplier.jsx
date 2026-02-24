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

const CONTRACT_COLORS = {
    'Active':   'rgba(34,197,94,0.82)',
    'Expiring': 'rgba(234,179,8,0.82)',
    'Expired':  'rgba(239,68,68,0.82)',
    'Critical': 'rgba(239,68,68,0.82)',
};

const CONTRACT_TAG = {
    'Active':   'success',
    'Expiring': 'warning',
    'Expired':  'danger',
    'Critical': 'danger',
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
// Main component
// ---------------------------------------------------------------------------

export default function InventorySupplier() {
    const fmt = useFormatters();
    const { businessId } = useParams();
    const toastRef = useRef(null);
    const { pipelineStatus } = usePipelineProgress();
    const { dateRange, applyQuickFilter, resetFilters, toISODate } = useAnalyticsDateFilter();
    const { lastUpdate } = useAnalyticsWebSocket(businessId);

    const [rawSupplier, setRawSupplier] = useState(null);
    const [loading, setLoading] = useState(true);

    // -------------------------------------------------------------------------
    // Fetch — supplier_analytics only
    // -------------------------------------------------------------------------

    const buildUrl = useCallback((from, to) => {
        const base   = import.meta.env.VITE_API_URL || 'http://localhost:8000';
        const params = new URLSearchParams({ categories: 'supplier_analytics' });
        if (from) params.set('date_from', toISODate(from));
        if (to)   params.set('date_to',   toISODate(to));
        return `${base}/analytics/data/${businessId}?${params.toString()}`;
    }, [businessId, toISODate]);

    const fetchData = useCallback(async (from, to) => {
        if (!businessId) return;
        setLoading(true);
        try {
            const res = await fetch(buildUrl(from, to));
            if (!res.ok) {
                toastRef.current?.show({ severity: 'warn', summary: 'No Data', detail: 'Run the analytics pipeline first.', life: 5000 });
                setRawSupplier(null);
                return;
            }
            const json = await res.json();
            setRawSupplier(json.categories?.supplier_analytics ?? null);
        } catch (err) {
            console.error('[InventorySupplier] fetch error:', err);
            toastRef.current?.show({ severity: 'error', summary: 'Error', detail: 'Failed to load supplier data', life: 5000 });
        } finally {
            setLoading(false);
        }
    }, [businessId, buildUrl]);

    useEffect(() => { fetchData(null, null); }, [businessId]);           // eslint-disable-line
    useEffect(() => { fetchData(dateRange.from, dateRange.to); }, [dateRange]); // eslint-disable-line
    useEffect(() => {
        if (lastUpdate?.files) {
            toastRef.current?.show({ severity: 'info', summary: 'Data Updated', detail: `${lastUpdate.total_files} metric(s) updated`, life: 3000 });
            fetchData(dateRange.from, dateRange.to);
        }
    }, [lastUpdate]); // eslint-disable-line

    // -------------------------------------------------------------------------
    // Derived data
    // -------------------------------------------------------------------------

    const derived = useMemo(() => {
        if (!rawSupplier) return null;
        const a = rawSupplier.analytics ?? {};

        const rankingCore      = a.supplier_ranking_core?.data                    ?? [];
        const fulfillment      = a.supplier_fulfillment_performance?.data         ?? [];
        const revContrib       = a.supplier_revenue_contribution?.data            ?? [];
        const profitMargin     = a.supplier_profit_margin?.data                   ?? [];
        const stockoutBySup    = a.stockout_rate_by_supplier?.data                ?? [];
        const reliability      = a.supplier_reliability?.data                     ?? [];
        const contractExpiry   = a.supplier_contract_expiry?.data                 ?? [];
        const carryingCost     = a.inventory_carrying_cost_by_supplier?.data      ?? [];

        // ---- KPIs -----------------------------------------------------------
        const totalSuppliers    = rankingCore.length;
        const preferredCount    = rankingCore.filter((r) => r.is_preferred).length;
        const bestSupplierRow   = rankingCore.reduce(
            (best, r) => (!best || (r.supplier_performance_score ?? 0) > (best.supplier_performance_score ?? 0)) ? r : best,
            null,
        );
        const avgStockoutRate   = stockoutBySup.length > 0
            ? stockoutBySup.reduce((s, r) => s + (r.supplier_stockout_rate ?? 0), 0) / stockoutBySup.length
            : 0;

        // ---- Performance score bar ------------------------------------------
        const perfSorted = [...rankingCore].sort((a, b) => (b.supplier_performance_score ?? 0) - (a.supplier_performance_score ?? 0)).slice(0, 12);
        const perfBarData = {
            labels: perfSorted.map((r) => suppLabel(r.supplier_id)),
            datasets: [{
                label: 'Performance Score',
                data: perfSorted.map((r) => r.supplier_performance_score ?? 0),
                backgroundColor: PALETTE,
            }],
        };

        // ---- Reliability score bar ------------------------------------------
        const relSorted = [...reliability].sort((a, b) => (b.supplier_reliability_score_effective ?? 0) - (a.supplier_reliability_score_effective ?? 0)).slice(0, 12);
        const relBarData = {
            labels: relSorted.map((r) => suppLabel(r.supplier_id)),
            datasets: [{
                label: 'Reliability Score',
                data: relSorted.map((r) => r.supplier_reliability_score_effective ?? 0),
                backgroundColor: 'rgba(34,197,94,0.82)',
            }],
        };

        // ---- Revenue contribution doughnut ----------------------------------
        const revSorted = [...revContrib].sort((a, b) => (b.revenue_contribution_share ?? 0) - (a.revenue_contribution_share ?? 0)).slice(0, 8);
        const revDoughnut = {
            labels: revSorted.map((r) => suppLabel(r.supplier_id)),
            datasets: [{
                data: revSorted.map((r) => r.revenue_contribution_share ?? 0),
                backgroundColor: PALETTE,
                borderWidth: 2,
            }],
        };

        // ---- Stockout rate bar ---------------------------------------------
        const stockoutSorted = [...stockoutBySup].sort((a, b) => (b.supplier_stockout_rate ?? 0) - (a.supplier_stockout_rate ?? 0)).slice(0, 12);
        const stockoutBarData = {
            labels: stockoutSorted.map((r) => suppLabel(r.supplier_id)),
            datasets: [{
                label: 'Stockout Rate',
                data: stockoutSorted.map((r) => r.supplier_stockout_rate ?? 0),
                backgroundColor: 'rgba(239,68,68,0.82)',
            }],
        };

        // ---- Avg restock lead time bar -------------------------------------
        const fulfillSorted = [...fulfillment].filter((r) => (r.avg_restock_lead_time ?? 0) > 0)
            .sort((a, b) => (b.avg_restock_lead_time ?? 0) - (a.avg_restock_lead_time ?? 0)).slice(0, 12);
        const leadTimeBarData = {
            labels: fulfillSorted.map((r) => suppLabel(r.supplier_id)),
            datasets: [{
                label: 'Avg Restock Lead Time (days)',
                data: fulfillSorted.map((r) => r.avg_restock_lead_time ?? 0),
                backgroundColor: 'rgba(249,115,22,0.82)',
            }],
        };

        // ---- Contract expiry bar (sorted by days remaining) ----------------
        const contractSorted = [...contractExpiry]
            .filter((r) => (r.days_until_contract_expiry ?? 9999) < 365)
            .sort((a, b) => (a.days_until_contract_expiry ?? 9999) - (b.days_until_contract_expiry ?? 9999))
            .slice(0, 12);

        const contractBarData = {
            labels: contractSorted.map((r) => suppLabel(r.supplier_id)),
            datasets: [{
                label: 'Days Until Expiry',
                data: contractSorted.map((r) => r.days_until_contract_expiry ?? 0),
                backgroundColor: contractSorted.map((r) =>
                    CONTRACT_COLORS[r.contract_status_flag] ?? 'rgba(59,130,246,0.82)'
                ),
            }],
        };

        // ---- Storage cost by supplier bar ----------------------------------
        const carryingSorted = [...carryingCost].sort((a, b) => (b.total_storage_cost ?? 0) - (a.total_storage_cost ?? 0)).slice(0, 10);
        const carryingBarData = {
            labels: carryingSorted.map((r) => suppLabel(r.supplier_id)),
            datasets: [{
                label: 'Total Storage Cost',
                data: carryingSorted.map((r) => r.total_storage_cost ?? 0),
                backgroundColor: 'rgba(139,92,246,0.82)',
            }],
        };

        // ---- Stock efficiency ratio bar ------------------------------------
        const efficiencySorted = [...rankingCore].sort((a, b) => (b.stock_efficiency_ratio ?? 0) - (a.stock_efficiency_ratio ?? 0)).slice(0, 12);
        const efficiencyBarData = {
            labels: efficiencySorted.map((r) => suppLabel(r.supplier_id)),
            datasets: [{
                label: 'Stock Efficiency Ratio',
                data: efficiencySorted.map((r) => r.stock_efficiency_ratio ?? 0),
                backgroundColor: 'rgba(6,182,212,0.82)',
            }],
        };

        // ---- Merged table for supplier ranking ------------------------------
        const profitMap     = Object.fromEntries(profitMargin.map((r) => [r.supplier_id, r]));
        const revMap        = Object.fromEntries(revContrib.map((r) => [r.supplier_id, r]));
        const fulfillMap    = Object.fromEntries(fulfillment.map((r) => [r.supplier_id, r]));
        const stockoutMap   = Object.fromEntries(stockoutBySup.map((r) => [r.supplier_id, r]));

        const mergedRanking = rankingCore.map((r) => ({
            ...r,
            avg_profit_margin:         profitMap[r.supplier_id]?.avg_profit_margin,
            revenue_contribution_share: revMap[r.supplier_id]?.revenue_contribution_share,
            total_revenue_generated:    revMap[r.supplier_id]?.total_revenue_generated,
            avg_restock_lead_time:      fulfillMap[r.supplier_id]?.avg_restock_lead_time,
            supplier_stockout_rate:     stockoutMap[r.supplier_id]?.supplier_stockout_rate,
        })).sort((a, b) => (b.supplier_performance_score ?? 0) - (a.supplier_performance_score ?? 0));

        return {
            totalSuppliers, preferredCount, bestSupplierRow, avgStockoutRate,
            perfBarData, relBarData, revDoughnut, stockoutBarData,
            leadTimeBarData, contractBarData, carryingBarData, efficiencyBarData,
            mergedRanking, contractExpiry,
        };
    }, [rawSupplier]);

    const hasData = !!(derived && derived.totalSuppliers > 0);

    // -------------------------------------------------------------------------
    // Chart options
    // -------------------------------------------------------------------------

    const barOpts = (horizontal = false) => ({
        responsive: true, maintainAspectRatio: false, indexAxis: horizontal ? 'y' : 'x',
        plugins: { legend: { display: !horizontal } },
        scales: { x: { beginAtZero: true }, y: { beginAtZero: !horizontal } },
    });

    const currencyBarOpts = () => ({
        responsive: true, maintainAspectRatio: false,
        plugins: {
            legend: { display: true },
            tooltip: { callbacks: { label: (ctx) => fmt.currency(ctx.raw) } },
        },
        scales: { y: { beginAtZero: true, ticks: { callback: (v) => fmt.compact(v) } } },
    });

    const doughnutOpts = {
        responsive: true, maintainAspectRatio: false,
        plugins: {
            legend: { position: 'right' },
            tooltip: { callbacks: { label: (ctx) => `${ctx.label}: ${fmt.pct((ctx.raw ?? 0) * 100)}` } },
        },
    };

    // -------------------------------------------------------------------------
    // Render
    // -------------------------------------------------------------------------

    if (loading && pipelineStatus !== 'loading') {
        return (
            <div className="flex flex-col items-center justify-center min-h-[400px] gap-4">
                <ProgressSpinner />
                <p className="text-gray-500 text-base">Loading supplier inventory…</p>
            </div>
        );
    }

    if (!hasData) {
        return (
            <div className="flex items-center justify-center min-h-[60vh]">
                <p className="text-gray-500">No supplier data available yet. Run the analytics pipeline first.</p>
            </div>
        );
    }
    if (!derived) return null;
    const {
        totalSuppliers, preferredCount, bestSupplierRow, avgStockoutRate,
        perfBarData, relBarData, revDoughnut, stockoutBarData,
        leadTimeBarData, contractBarData, carryingBarData, efficiencyBarData,
        mergedRanking, contractExpiry,
    } = derived;

    return (
        <div className="p-6 space-y-8">
            <Toast ref={toastRef} />
            <DateFilterBar
                    dateRange={dateRange}
                    onQuickFilter={applyQuickFilter}
                    onReset={resetFilters}
                    toISODate={toISODate}
                />

            {/* ── KPI Cards ──────────────────────────────────────────────── */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <KPICard icon="pi-users"       iconBg="bg-blue-100"   iconColor="text-blue-600"   value={fmt.number(totalSuppliers)}                    label="Total Suppliers" />
                <KPICard icon="pi-star-fill"   iconBg="bg-green-100"  iconColor="text-green-600"  value={fmt.number(preferredCount)}                    label="Preferred Suppliers" />
                <KPICard icon="pi-trophy"      iconBg="bg-amber-100"  iconColor="text-amber-600"  value={bestSupplierRow ? suppLabel(bestSupplierRow.supplier_id) : '—'} label="Best Performing Supplier" />
                <KPICard icon="pi-times-circle" iconBg="bg-red-100"   iconColor="text-red-600"    value={fmt.pct((avgStockoutRate ?? 0) * 100)}         label="Avg Stockout Rate" />
            </div>

            {/* ── Performance & Reliability ──────────────────────────────── */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                <ChartWrapper title="Supplier Performance Score" height={340}>
                    <Bar data={perfBarData} options={barOpts()} />
                </ChartWrapper>

                <ChartWrapper title="Supplier Reliability Score" height={340}>
                    <Bar data={relBarData} options={barOpts()} />
                </ChartWrapper>
            </div>

            {/* ── Revenue & Stockouts ────────────────────────────────────── */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                <ChartWrapper title="Revenue Contribution Share" height={320}>
                    <Doughnut data={revDoughnut} options={doughnutOpts} />
                </ChartWrapper>

                <ChartWrapper title="Stockout Rate by Supplier" height={320}>
                    <Bar data={stockoutBarData} options={barOpts()} />
                </ChartWrapper>
            </div>

            {/* ── Lead Time & Stock Efficiency ───────────────────────────── */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                <ChartWrapper title="Avg Restock Lead Time (days)" height={320}>
                    <Bar data={leadTimeBarData} options={barOpts()} />
                </ChartWrapper>

                <ChartWrapper title="Stock Efficiency Ratio" height={320}>
                    <Bar data={efficiencyBarData} options={barOpts()} />
                </ChartWrapper>
            </div>

            {/* ── Carrying Cost & Contract Expiry ────────────────────────── */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                <ChartWrapper title="Total Storage Cost by Supplier" height={320}>
                    <Bar data={carryingBarData} options={currencyBarOpts()} />
                </ChartWrapper>

                <ChartWrapper title="Contract Expiry (Next 365 Days)" height={320}>
                    <Bar data={contractBarData} options={barOpts()} />
                </ChartWrapper>
            </div>

            {/* ── Full Supplier Ranking Table ────────────────────────────── */}
            <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                <div className="p-6">
                    <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                        Supplier Rankings
                    </h3>
                    <DataTable
                        value={mergedRanking}
                        paginator rows={10} rowsPerPageOptions={[10, 25, 50]}
                        className="p-datatable-sm" stripedRows sortMode="multiple"
                    >
                        <Column field="supplier_id"               header="Supplier ID"       sortable body={(r) => suppLabel(r.supplier_id)} />
                        <Column field="supplier_status"           header="Status"            sortable
                            body={(r) => <Tag value={r.supplier_status ?? '—'} severity={r.supplier_status === 'Active' ? 'success' : 'warning'} />} />
                        <Column field="is_preferred"              header="Preferred"
                            body={(r) => r.is_preferred ? <Tag value="Yes" severity="success" /> : <Tag value="No" severity="secondary" />} />
                        <Column field="supplier_performance_score" header="Perf. Score"      sortable body={(r) => fmt.decimal(r.supplier_performance_score, 1)} />
                        <Column field="supplier_reliability_score" header="Reliability"      sortable body={(r) => fmt.decimal(r.supplier_reliability_score, 1)} />
                        <Column field="stock_efficiency_ratio"    header="Efficiency Ratio"  sortable body={(r) => fmt.decimal(r.stock_efficiency_ratio, 3)} />
                        <Column field="stockout_rate"             header="Stockout Rate"      sortable body={(r) => fmt.pct((r.stockout_rate ?? 0) * 100)} />
                        <Column field="total_revenue_generated"   header="Revenue"           sortable body={(r) => fmt.currency(r.total_revenue_generated)} />
                        <Column field="avg_profit_margin"         header="Profit Margin"     sortable body={(r) => fmt.pct((r.avg_profit_margin ?? 0) * 100)} />
                        <Column field="total_products_supplied"   header="Products"          sortable body={(r) => fmt.number(r.total_products_supplied)} />
                        <Column field="total_stockouts"           header="Stockouts"         sortable body={(r) => fmt.number(r.total_stockouts)} />
                        <Column field="avg_restock_lead_time"     header="Lead Time (days)"  sortable body={(r) => fmt.decimal(r.avg_restock_lead_time, 1)} />
                    </DataTable>
                </div>
            </Card>

            {/* ── Contract Expiry Monitoring Table ───────────────────────── */}
            <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                <div className="p-6">
                    <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                        Contract Expiry Monitoring
                    </h3>
                    <DataTable
                        value={[...contractExpiry].sort((a, b) => (a.days_until_contract_expiry ?? 9999) - (b.days_until_contract_expiry ?? 9999))}
                        paginator rows={10} rowsPerPageOptions={[10, 25]}
                        className="p-datatable-sm" stripedRows sortMode="multiple"
                    >
                        <Column field="supplier_id"                  header="Supplier ID"       sortable body={(r) => suppLabel(r.supplier_id)} />
                        <Column field="supplier_status"              header="Status"            sortable
                            body={(r) => <Tag value={r.supplier_status ?? '—'} severity={r.supplier_status === 'Active' ? 'success' : 'warning'} />} />
                        <Column field="contract_status_flag"         header="Contract Status"   sortable
                            body={(r) => <Tag value={r.contract_status_flag ?? '—'} severity={CONTRACT_TAG[r.contract_status_flag] ?? 'info'} />} />
                        <Column field="days_until_contract_expiry"   header="Days Until Expiry" sortable body={(r) => fmt.number(r.days_until_contract_expiry)} />
                        <Column field="contract_start_date"          header="Start Date"        sortable />
                        <Column field="contract_end_date"            header="End Date"          sortable />
                        <Column field="supplier_performance_score"   header="Perf. Score"       sortable body={(r) => fmt.decimal(r.supplier_performance_score, 1)} />
                        <Column field="supplier_reliability_score_effective" header="Reliability" sortable body={(r) => fmt.decimal(r.supplier_reliability_score_effective, 1)} />
                    </DataTable>
                </div>
            </Card>
        </div>
    );
}
