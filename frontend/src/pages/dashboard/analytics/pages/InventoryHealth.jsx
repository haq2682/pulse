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
    CategoryScale, LinearScale, PointElement, LineElement,
    BarElement, ArcElement, Title, Tooltip, Legend,
} from 'chart.js';
import { Bar, Doughnut } from 'react-chartjs-2';
import { useAnalyticsWebSocket } from '../../../../hooks/useAnalyticsWebSocket';
import ChartWrapper from '../components/ChartWrapper';
import { usePipelineProgress } from '@/context/PipelineProgressContext';
import useAnalyticsDateFilter from '@/hooks/useAnalyticsDateFilter';
import DateFilterBar from '../components/DateFilterBar';
import { useFormatters } from '@/hooks/useFormatters';

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, BarElement, ArcElement, Title, Tooltip, Legend);

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PALETTE = [
    'rgba(59,130,246,0.82)', 'rgba(34,197,94,0.82)',  'rgba(249,115,22,0.82)',
    'rgba(239,68,68,0.82)',  'rgba(139,92,246,0.82)', 'rgba(6,182,212,0.82)',
    'rgba(234,179,8,0.82)',  'rgba(236,72,153,0.82)', 'rgba(20,184,166,0.82)',
    'rgba(168,85,247,0.82)',
];

const URGENCY_COLORS = {
    Critical: 'rgba(239,68,68,0.85)',
    High:     'rgba(249,115,22,0.85)',
    Medium:   'rgba(234,179,8,0.85)',
    Low:      'rgba(34,197,94,0.85)',
};

const URGENCY_TAG = {
    Critical: 'danger',
    High:     'warning',
    Medium:   'info',
    Low:      'success',
};

const STATUS_COLORS = {
    'In Stock':       'rgba(34,197,94,0.82)',
    'Low Stock':      'rgba(234,179,8,0.82)',
    'Out of Stock':   'rgba(239,68,68,0.82)',
    'Overstocked':    'rgba(139,92,246,0.82)',
    'Normal':         'rgba(59,130,246,0.82)',
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

export default function InventoryHealth() {
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
            console.error('[InventoryHealth] fetch error:', err);
            toastRef.current?.show({ severity: 'error', summary: 'Error', detail: 'Failed to load inventory health data', life: 5000 });
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

        const healthData    = a.product_inventory_health?.data    ?? [];
        const criticalData  = a.product_inventory_critical?.data  ?? [];
        const deadStockData = a.product_dead_stock?.data          ?? [];
        const stockStatus   = a.inventory_stock_status?.data      ?? [];

        // ---- KPIs -----------------------------------------------------------
        const totalProducts  = healthData.length;
        const criticalCount  = criticalData.length;
        const deadStockCount = deadStockData.length;
        const avgHealthScore = totalProducts > 0
            ? healthData.reduce((s, r) => s + (r.stock_health_score ?? 0), 0) / totalProducts
            : 0;

        // ---- Reorder urgency distribution -----------------------------------
        const urgencyBuckets = { Critical: 0, High: 0, Medium: 0, Low: 0 };
        healthData.forEach((r) => {
            const u = r.reorder_urgency ?? 'Low';
            urgencyBuckets[u] = (urgencyBuckets[u] ?? 0) + 1;
        });

        const urgencyData = {
            labels: Object.keys(urgencyBuckets),
            datasets: [{
                label: 'Products',
                data: Object.values(urgencyBuckets),
                backgroundColor: Object.keys(urgencyBuckets).map((k) => URGENCY_COLORS[k] ?? 'rgba(156,163,175,0.8)'),
            }],
        };

        // ---- Stock status distribution (doughnut) ----------------------------
        const statusCounts = {};
        stockStatus.forEach((r) => {
            const s = r.stock_status_computed ?? 'Unknown';
            statusCounts[s] = (statusCounts[s] ?? 0) + 1;
        });

        const statusDoughnutData = {
            labels: Object.keys(statusCounts),
            datasets: [{
                data: Object.values(statusCounts),
                backgroundColor: Object.keys(statusCounts).map((k) => STATUS_COLORS[k] ?? PALETTE[0]),
                borderWidth: 2,
            }],
        };

        // ---- Health score distribution (bins) --------------------------------
        const scoreBins = { '0–25': 0, '26–50': 0, '51–75': 0, '76–100': 0 };
        healthData.forEach((r) => {
            const s = r.stock_health_score ?? 0;
            if (s <= 25)       scoreBins['0–25']++;
            else if (s <= 50)  scoreBins['26–50']++;
            else if (s <= 75)  scoreBins['51–75']++;
            else               scoreBins['76–100']++;
        });

        const healthScoreBinsData = {
            labels: Object.keys(scoreBins),
            datasets: [{
                label: 'Products',
                data: Object.values(scoreBins),
                backgroundColor: [
                    'rgba(239,68,68,0.82)', 'rgba(249,115,22,0.82)',
                    'rgba(234,179,8,0.82)', 'rgba(34,197,94,0.82)',
                ],
            }],
        };

        // ---- Bottom 10 health score products (horizontal bar) ---------------
        const worst10 = [...healthData]
            .sort((a, b) => (a.stock_health_score ?? 0) - (b.stock_health_score ?? 0))
            .slice(0, 10);

        const worst10BarData = {
            labels: worst10.map((r) => r.product_name ?? r.product_id),
            datasets: [{
                label: 'Health Score',
                data: worst10.map((r) => r.stock_health_score ?? 0),
                backgroundColor: worst10.map((r) => {
                    const s = r.stock_health_score ?? 0;
                    return s <= 25 ? 'rgba(239,68,68,0.82)' : s <= 50 ? 'rgba(249,115,22,0.82)' : 'rgba(234,179,8,0.82)';
                }),
            }],
        };

        // ---- Top 10 dead stock (by dead_stock_score) -----------------------
        const top10DeadStock = [...deadStockData]
            .sort((a, b) => (b.dead_stock_score ?? 0) - (a.dead_stock_score ?? 0))
            .slice(0, 10);

        const deadStockBarData = {
            labels: top10DeadStock.map((r) => r.product_name ?? r.product_id),
            datasets: [{
                label: 'Dead Stock Score',
                data: top10DeadStock.map((r) => r.dead_stock_score ?? 0),
                backgroundColor: 'rgba(139,92,246,0.82)',
            }],
        };

        // ---- Inventory turnover ratio (top 10 by turnover) ------------------
        const top10Turnover = [...healthData]
            .filter((r) => (r.inventory_turnover_ratio ?? 0) > 0)
            .sort((a, b) => (b.inventory_turnover_ratio ?? 0) - (a.inventory_turnover_ratio ?? 0))
            .slice(0, 10);

        const turnoverBarData = {
            labels: top10Turnover.map((r) => r.product_name ?? r.product_id),
            datasets: [{
                label: 'Inventory Turnover Ratio',
                data: top10Turnover.map((r) => r.inventory_turnover_ratio ?? 0),
                backgroundColor: 'rgba(6,182,212,0.82)',
            }],
        };

        return {
            totalProducts, criticalCount, deadStockCount, avgHealthScore,
            urgencyData, statusDoughnutData, healthScoreBinsData,
            worst10BarData, deadStockBarData, turnoverBarData,
            criticalData, deadStockData, healthData,
        };
    }, [rawData]);

    const hasData = !!(derived && derived.totalProducts > 0);

    // -------------------------------------------------------------------------
    // Chart option helpers
    // -------------------------------------------------------------------------

    const barOpts = (title, horizontal = false) => ({
        responsive: true, maintainAspectRatio: false, indexAxis: horizontal ? 'y' : 'x',
        plugins: { legend: { display: !horizontal }, title: { display: false } },
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
                <p className="text-gray-500 text-base">Loading inventory health…</p>
            </div>
        );
    }

    if (!hasData) {
        return (
            <div className="flex items-center justify-center min-h-[60vh]">
                <p className="text-gray-500">No inventory health data available yet. Run the analytics pipeline first.</p>
            </div>
        );
    }
    if (!derived) return null;
    const { totalProducts, criticalCount, deadStockCount, avgHealthScore,
            urgencyData, statusDoughnutData, healthScoreBinsData,
            worst10BarData, deadStockBarData, turnoverBarData,
            criticalData, deadStockData } = derived;

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
                <KPICard icon="pi-box"        iconBg="bg-blue-100"   iconColor="text-blue-600"   value={fmt.number(totalProducts)}          label="Products Tracked" />
                <KPICard icon="pi-exclamation-triangle" iconBg="bg-red-100" iconColor="text-red-600" value={fmt.number(criticalCount)} label="Critical Products" />
                <KPICard icon="pi-ban"         iconBg="bg-purple-100" iconColor="text-purple-600" value={fmt.number(deadStockCount)}         label="Dead Stock Products" />
                <KPICard icon="pi-heart-fill"  iconBg="bg-green-100"  iconColor="text-green-600"  value={fmt.decimal(avgHealthScore, 1)}     label="Avg Health Score" />
            </div>

            {/* ── Overview Charts ────────────────────────────────────────── */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                <ChartWrapper title="Stock Status Distribution" height={300}>
                    <Doughnut data={statusDoughnutData} options={doughnutOpts} />
                </ChartWrapper>

                <ChartWrapper title="Reorder Urgency Distribution" height={300}>
                    <Bar data={urgencyData} options={barOpts('Reorder Urgency Distribution')} />
                </ChartWrapper>

                <ChartWrapper title="Health Score Bands" height={300}>
                    <Bar data={healthScoreBinsData} options={barOpts('Health Score Bands')} />
                </ChartWrapper>
            </div>

            {/* ── Worst Health Score Products ────────────────────────────── */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                <ChartWrapper title="10 Lowest Health Score Products" height={360}>
                    <Bar data={worst10BarData} options={barOpts('', true)} />
                </ChartWrapper>

                <ChartWrapper title="Top 10 Dead Stock Products" height={360}>
                    <Bar data={deadStockBarData} options={barOpts('', true)} />
                </ChartWrapper>
            </div>

            {/* ── Inventory Turnover ─────────────────────────────────────── */}
            <ChartWrapper title="Top 10 Products by Inventory Turnover Ratio" height={340}>
                <Bar data={turnoverBarData} options={barOpts('', true)} />
            </ChartWrapper>

            {/* ── Critical Products Table ────────────────────────────────── */}
            <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                <div className="p-6">
                    <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                        Critical Inventory Products
                    </h3>
                    <DataTable
                        value={criticalData}
                        paginator rows={10} rowsPerPageOptions={[10, 25, 50]}
                        className="p-datatable-sm" stripedRows sortMode="multiple"
                        defaultSortField="criticality_score" defaultSortOrder={-1}
                    >
                        <Column field="product_name"      header="Product"          sortable style={{ minWidth: '180px' }} />
                        <Column field="category"          header="Category"         sortable />
                        <Column field="reorder_urgency"   header="Urgency"          sortable
                            body={(r) => <Tag value={r.reorder_urgency} severity={URGENCY_TAG[r.reorder_urgency] ?? 'info'} />} />
                        <Column field="stock_health_score" header="Health Score"    sortable body={(r) => fmt.decimal(r.stock_health_score, 1)} />
                        <Column field="criticality_score"  header="Criticality"     sortable body={(r) => fmt.decimal(r.criticality_score, 1)} />
                        <Column field="current_stock"      header="Current Stock"   sortable body={(r) => fmt.number(r.current_stock)} />
                        <Column field="available_stock"    header="Available"       sortable body={(r) => fmt.number(r.available_stock)} />
                        <Column field="days_of_supply"     header="Days of Supply"  sortable body={(r) => fmt.decimal(r.days_of_supply, 1)} />
                        <Column field="stockout_frequency" header="Stockouts"       sortable body={(r) => fmt.number(r.stockout_frequency)} />
                    </DataTable>
                </div>
            </Card>

            {/* ── Dead Stock Table ───────────────────────────────────────── */}
            <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                <div className="p-6">
                    <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                        Dead Stock Products
                    </h3>
                    <DataTable
                        value={deadStockData}
                        paginator rows={10} rowsPerPageOptions={[10, 25]}
                        className="p-datatable-sm" stripedRows sortMode="multiple"
                        defaultSortField="dead_stock_score" defaultSortOrder={-1}
                    >
                        <Column field="product_name"         header="Product"         sortable style={{ minWidth: '180px' }} />
                        <Column field="category"             header="Category"        sortable />
                        <Column field="brand"                header="Brand"           sortable />
                        <Column field="dead_stock_score"     header="Dead Stock Score" sortable body={(r) => fmt.decimal(r.dead_stock_score, 1)} />
                        <Column field="inventory_turnover_rate" header="Turnover Rate" sortable body={(r) => fmt.decimal(r.inventory_turnover_rate, 3)} />
                        <Column field="current_stock_level"  header="Stock Level"     sortable body={(r) => fmt.number(r.current_stock_level)} />
                        <Column field="days_of_supply"       header="Days of Supply"  sortable body={(r) => fmt.decimal(r.days_of_supply, 1)} />
                        <Column field="total_units_sold"     header="Units Sold"      sortable body={(r) => fmt.number(r.total_units_sold)} />
                    </DataTable>
                </div>
            </Card>
        </div>
    );
}
