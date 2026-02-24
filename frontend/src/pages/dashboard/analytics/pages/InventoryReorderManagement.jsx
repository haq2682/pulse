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
];

const URGENCY_TAG = {
    Critical: 'danger',
    Urgent:   'warning',
    High:     'warning',
    Moderate: 'info',
    Medium:   'info',
    Low:      'success',
};

const URGENCY_COLOR = {
    Critical: 'rgba(239,68,68,0.85)',
    Urgent:   'rgba(249,115,22,0.85)',
    High:     'rgba(249,115,22,0.85)',
    Moderate: 'rgba(234,179,8,0.85)',
    Medium:   'rgba(234,179,8,0.85)',
    Low:      'rgba(34,197,94,0.85)',
};

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

export default function InventoryReorderManagement() {
    const fmt = useFormatters();
    const { businessId } = useParams();
    const toastRef = useRef(null);
    const { pipelineStatus } = usePipelineProgress();
    const { dateRange, applyQuickFilter, resetFilters, toISODate } = useAnalyticsDateFilter();
    const { lastUpdate } = useAnalyticsWebSocket(businessId);

    const [rawData, setRawData] = useState(null);
    const [loading, setLoading] = useState(true);

    // -------------------------------------------------------------------------
    // Fetch
    // -------------------------------------------------------------------------

    const buildUrl = useCallback((from, to) => {
        const base   = import.meta.env.VITE_API_URL || 'http://localhost:8000';
        const params = new URLSearchParams({ categories: 'product_analytics' });
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
                setRawData(null);
                return;
            }
            const json = await res.json();
            setRawData(json.categories?.product_analytics ?? null);
        } catch (err) {
            console.error('[InventoryReorder] fetch error:', err);
            toastRef.current?.show({ severity: 'error', summary: 'Error', detail: 'Failed to load reorder data', life: 5000 });
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
        if (!rawData) return null;
        const a = rawData.analytics ?? {};

        const skuReorder       = a.sku_reorder_urgency?.data              ?? [];
        const breachFreq       = a.reorder_point_breach_frequency?.data   ?? [];
        const stockoutRisk     = a.product_stockout_risk?.data            ?? [];
        const replenishment    = a.product_stockout_replenishment?.data   ?? [];

        // ---- KPIs -----------------------------------------------------------
        const criticalUrgent = skuReorder.filter((r) =>
            ['Critical', 'Urgent'].includes(r.reorder_urgency_tier)
        ).length;

        const stockoutRiskCount = stockoutRisk.filter((r) => r.stockout_risk_flag).length;

        const totalStockouts = replenishment.reduce((s, r) => s + (r.stockout_occurrences ?? 0), 0);

        const avgReplenishScore = replenishment.length > 0
            ? replenishment.reduce((s, r) => s + (r.replenishment_priority_score ?? 0), 0) / replenishment.length
            : 0;

        // ---- Urgency tier distribution (doughnut) ----------------------------
        const tierCounts = {};
        skuReorder.forEach((r) => {
            const t = r.reorder_urgency_tier ?? 'Unknown';
            tierCounts[t] = (tierCounts[t] ?? 0) + 1;
        });
        const tierOrder = ['Critical', 'Urgent', 'High', 'Moderate', 'Medium', 'Low'];
        const sortedTiers = tierOrder.filter((t) => tierCounts[t]);

        const tierDoughnutData = {
            labels: sortedTiers,
            datasets: [{
                data: sortedTiers.map((t) => tierCounts[t]),
                backgroundColor: sortedTiers.map((t) => URGENCY_COLOR[t] ?? 'rgba(156,163,175,0.8)'),
                borderWidth: 2,
            }],
        };

        // ---- Top 15 by reorder urgency score (horizontal bar) ----------------
        const top15Urgency = [...skuReorder]
            .sort((a, b) => (b.reorder_urgency_score ?? 0) - (a.reorder_urgency_score ?? 0))
            .slice(0, 15);

        const urgencyScoreBarData = {
            labels: top15Urgency.map((r) => r.product_name ?? r.product_id),
            datasets: [{
                label: 'Reorder Urgency Score',
                data: top15Urgency.map((r) => r.reorder_urgency_score ?? 0),
                backgroundColor: top15Urgency.map((r) =>
                    URGENCY_COLOR[r.reorder_urgency_tier] ?? 'rgba(59,130,246,0.82)'
                ),
            }],
        };

        // ---- Top 10 by replenishment priority score (horizontal bar) ---------
        const top10Replenish = [...replenishment]
            .sort((a, b) => (b.replenishment_priority_score ?? 0) - (a.replenishment_priority_score ?? 0))
            .slice(0, 10);

        const replenishBarData = {
            labels: top10Replenish.map((r) => r.product_name ?? r.product_id),
            datasets: [{
                label: 'Replenishment Priority Score',
                data: top10Replenish.map((r) => r.replenishment_priority_score ?? 0),
                backgroundColor: 'rgba(249,115,22,0.82)',
            }],
        };

        // ---- Stockout risk by category (bar) ---------------------------------
        const riskByCat = {};
        stockoutRisk.filter((r) => r.stockout_risk_flag).forEach((r) => {
            riskByCat[r.category] = (riskByCat[r.category] ?? 0) + 1;
        });
        const riskCatSorted = Object.entries(riskByCat).sort((a, b) => b[1] - a[1]).slice(0, 10);

        const riskByCatData = {
            labels: riskCatSorted.map(([c]) => c),
            datasets: [{
                label: 'At-Risk Products',
                data: riskCatSorted.map(([, v]) => v),
                backgroundColor: PALETTE,
            }],
        };

        // ---- Top 10 breach frequency (horizontal bar) -----------------------
        const top10Breach = [...breachFreq]
            .sort((a, b) => (b.reorder_point_breach_count ?? 0) - (a.reorder_point_breach_count ?? 0))
            .slice(0, 10);

        const breachBarData = {
            labels: top10Breach.map((r) => r.product_name ?? r.product_id),
            datasets: [{
                label: 'Reorder Breach Count',
                data: top10Breach.map((r) => r.reorder_point_breach_count ?? 0),
                backgroundColor: 'rgba(239,68,68,0.82)',
            }],
        };

        // ---- Stockout days distribution (top 10) ----------------------------
        const top10StockoutDays = [...replenishment]
            .sort((a, b) => (b.stockout_days ?? 0) - (a.stockout_days ?? 0))
            .slice(0, 10);

        const stockoutDaysData = {
            labels: top10StockoutDays.map((r) => r.product_name ?? r.product_id),
            datasets: [{
                label: 'Stockout Days',
                data: top10StockoutDays.map((r) => r.stockout_days ?? 0),
                backgroundColor: 'rgba(139,92,246,0.82)',
            }],
        };

        // Table data
        const urgentProducts  = skuReorder.filter((r) => ['Critical', 'Urgent'].includes(r.reorder_urgency_tier))
            .sort((a, b) => (b.reorder_urgency_score ?? 0) - (a.reorder_urgency_score ?? 0));

        return {
            criticalUrgent, stockoutRiskCount, totalStockouts, avgReplenishScore,
            tierDoughnutData, urgencyScoreBarData, replenishBarData,
            riskByCatData, breachBarData, stockoutDaysData,
            urgentProducts, replenishment, stockoutRisk,
        };
    }, [rawData]);

    const hasData = !!(derived && derived.urgentProducts !== undefined);

    // -------------------------------------------------------------------------
    // Chart options
    // -------------------------------------------------------------------------

    const barOpts = (horizontal = false) => ({
        responsive: true, maintainAspectRatio: false, indexAxis: horizontal ? 'y' : 'x',
        plugins: { legend: { display: !horizontal } },
        scales: { x: { beginAtZero: true }, y: { beginAtZero: !horizontal } },
    });

    const doughnutOpts = {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { position: 'right' } },
    };

    // -------------------------------------------------------------------------
    // Render
    // -------------------------------------------------------------------------

    if (loading && pipelineStatus !== 'loading') {
        return (
            <div className="flex flex-col items-center justify-center min-h-[400px] gap-4">
                <ProgressSpinner />
                <p className="text-gray-500 text-base">Loading inventory reorder management…</p>
            </div>
        );
    }

    if (!hasData) {
        return (
            <div className="flex items-center justify-center min-h-[60vh]">
                <p className="text-gray-500">No reorder data available yet. Run the analytics pipeline first.</p>
            </div>
        );
    }
    if (!derived) return null;
    const {
        criticalUrgent, stockoutRiskCount, totalStockouts, avgReplenishScore,
        tierDoughnutData, urgencyScoreBarData, replenishBarData,
        riskByCatData, breachBarData, stockoutDaysData,
        urgentProducts, replenishment, stockoutRisk,
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
                <KPICard icon="pi-exclamation-circle" iconBg="bg-red-100"    iconColor="text-red-600"    value={fmt.number(criticalUrgent)}        label="Critical / Urgent SKUs" />
                <KPICard icon="pi-times-circle"       iconBg="bg-orange-100" iconColor="text-orange-600" value={fmt.number(stockoutRiskCount)}      label="Stockout Risk Products" />
                <KPICard icon="pi-refresh"            iconBg="bg-blue-100"   iconColor="text-blue-600"   value={fmt.number(totalStockouts)}         label="Total Stockout Events" />
                <KPICard icon="pi-arrow-up"           iconBg="bg-purple-100" iconColor="text-purple-600" value={fmt.decimal(avgReplenishScore, 1)}  label="Avg Replenishment Score" />
            </div>

            {/* ── Urgency Overview ───────────────────────────────────────── */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                <ChartWrapper title="Reorder Urgency Tier Distribution" height={300}>
                    <Doughnut data={tierDoughnutData} options={doughnutOpts} />
                </ChartWrapper>

                <ChartWrapper title="At-Risk Products by Category" height={300}>
                    <Bar data={riskByCatData} options={barOpts()} />
                </ChartWrapper>
            </div>

            {/* ── Urgency & Replenishment Scores ─────────────────────────── */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                <ChartWrapper title="Top 15 Products by Reorder Urgency Score" height={400}>
                    <Bar data={urgencyScoreBarData} options={barOpts(true)} />
                </ChartWrapper>

                <ChartWrapper title="Top 10 Products by Replenishment Priority" height={400}>
                    <Bar data={replenishBarData} options={barOpts(true)} />
                </ChartWrapper>
            </div>

            {/* ── Breach & Stockout Days ─────────────────────────────────── */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                <ChartWrapper title="Top 10 Products by Reorder Point Breaches" height={360}>
                    <Bar data={breachBarData} options={barOpts(true)} />
                </ChartWrapper>

                <ChartWrapper title="Top 10 Products by Stockout Days" height={360}>
                    <Bar data={stockoutDaysData} options={barOpts(true)} />
                </ChartWrapper>
            </div>

            {/* ── Urgent Reorder Table ───────────────────────────────────── */}
            <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                <div className="p-6">
                    <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                        SKUs Requiring Immediate Reorder
                    </h3>
                    <DataTable
                        value={urgentProducts}
                        paginator rows={10} rowsPerPageOptions={[10, 25, 50]}
                        className="p-datatable-sm" stripedRows sortMode="multiple"
                    >
                        <Column field="product_name"         header="Product"            sortable style={{ minWidth: '180px' }} />
                        <Column field="category"             header="Category"           sortable />
                        <Column field="reorder_urgency_tier" header="Urgency Tier"       sortable
                            body={(r) => <Tag value={r.reorder_urgency_tier} severity={URGENCY_TAG[r.reorder_urgency_tier] ?? 'info'} />} />
                        <Column field="reorder_urgency_score" header="Urgency Score"     sortable body={(r) => fmt.decimal(r.reorder_urgency_score, 1)} />
                        <Column field="available_stock"      header="Available Stock"    sortable body={(r) => fmt.number(r.available_stock)} />
                        <Column field="current_stock"        header="Current Stock"      sortable body={(r) => fmt.number(r.current_stock)} />
                        <Column field="minimum_stock_level"  header="Min. Stock Level"   sortable body={(r) => fmt.number(r.minimum_stock_level)} />
                        <Column field="reorder_point_breach_count" header="Breaches"     sortable body={(r) => fmt.number(r.reorder_point_breach_count)} />
                        <Column field="days_of_supply"       header="Days of Supply"     sortable body={(r) => fmt.decimal(r.days_of_supply, 1)} />
                    </DataTable>
                </div>
            </Card>

            {/* ── Replenishment Priority Table ───────────────────────────── */}
            <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                <div className="p-6">
                    <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                        Replenishment Priority Queue
                    </h3>
                    <DataTable
                        value={[...replenishment].sort((a, b) => (b.replenishment_priority_score ?? 0) - (a.replenishment_priority_score ?? 0))}
                        paginator rows={10} rowsPerPageOptions={[10, 25, 50]}
                        className="p-datatable-sm" stripedRows sortMode="multiple"
                    >
                        <Column field="product_name"               header="Product"             sortable style={{ minWidth: '180px' }} />
                        <Column field="category"                   header="Category"            sortable />
                        <Column field="replenishment_priority_score" header="Priority Score"    sortable body={(r) => fmt.decimal(r.replenishment_priority_score, 1)} />
                        <Column field="stockout_occurrences"       header="Stockout Events"     sortable body={(r) => fmt.number(r.stockout_occurrences)} />
                        <Column field="stockout_days"              header="Stockout Days"        sortable body={(r) => fmt.number(r.stockout_days)} />
                        <Column field="total_units_sold"           header="Units Sold"          sortable body={(r) => fmt.number(r.total_units_sold)} />
                        <Column field="total_revenue"              header="Revenue"             sortable body={(r) => fmt.currency(r.total_revenue)} />
                    </DataTable>
                </div>
            </Card>

            {/* ── Stockout Risk Table ────────────────────────────────────── */}
            <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                <div className="p-6">
                    <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                        Stockout Risk Products
                    </h3>
                    <DataTable
                        value={stockoutRisk.filter((r) => r.stockout_risk_flag)}
                        paginator rows={10} rowsPerPageOptions={[10, 25]}
                        className="p-datatable-sm" stripedRows sortMode="multiple"
                    >
                        <Column field="product_name"      header="Product"          sortable style={{ minWidth: '180px' }} />
                        <Column field="category"          header="Category"         sortable />
                        <Column field="current_stock_level" header="Stock Level"    sortable body={(r) => fmt.number(r.current_stock_level)} />
                        <Column field="days_of_supply"    header="Days of Supply"   sortable body={(r) => fmt.decimal(r.days_of_supply, 1)} />
                        <Column field="is_high_seller"    header="High Seller"
                            body={(r) => r.is_high_seller ? <Tag value="Yes" severity="success" /> : <Tag value="No" severity="secondary" />} />
                        <Column field="is_low_stock"      header="Low Stock"
                            body={(r) => r.is_low_stock ? <Tag value="Yes" severity="danger" /> : <Tag value="No" severity="secondary" />} />
                        <Column field="total_units_sold"  header="Units Sold"       sortable body={(r) => fmt.number(r.total_units_sold)} />
                        <Column field="total_revenue"     header="Revenue"          sortable body={(r) => fmt.currency(r.total_revenue)} />
                    </DataTable>
                </div>
            </Card>
        </div>
    );
}
