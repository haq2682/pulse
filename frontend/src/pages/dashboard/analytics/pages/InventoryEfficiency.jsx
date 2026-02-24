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
// Main component
// ---------------------------------------------------------------------------

export default function InventoryEfficiency() {
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
            console.error('[InventoryEfficiency] fetch error:', err);
            toastRef.current?.show({ severity: 'error', summary: 'Error', detail: 'Failed to load efficiency data', life: 5000 });
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

        const overstockData   = a.overstock_analysis?.data                ?? [];
        const excessData      = a.excess_inventory_not_selling?.data      ?? [];
        const reservedData    = a.reserved_vs_available?.data             ?? [];
        const carryingData    = a.inventory_carrying_cost_by_product?.data ?? [];
        const marginRisk      = a.margin_erosion_risk?.data               ?? [];

        // ---- KPIs -----------------------------------------------------------
        const totalCarryingCost = carryingData.reduce((s, r) => s + (r.storage_cost ?? 0), 0);
        const overstockCount    = overstockData.length;
        const excessCount       = excessData.length;
        const avgStorageCostPU  = carryingData.length > 0
            ? carryingData.reduce((s, r) => s + (r.storage_cost_per_unit ?? 0), 0) / carryingData.length
            : 0;

        // ---- Top 10 by storage cost (horizontal bar) -------------------------
        const top10Carrying = [...carryingData]
            .sort((a, b) => (b.storage_cost ?? 0) - (a.storage_cost ?? 0))
            .slice(0, 10);

        const carryingBarData = {
            labels: top10Carrying.map((r) => r.product_name ?? r.product_id),
            datasets: [{
                label: 'Storage Cost (USD)',
                data: top10Carrying.map((r) => r.storage_cost ?? 0),
                backgroundColor: 'rgba(139,92,246,0.82)',
            }],
        };

        // ---- Overstock by category (bar) -------------------------------------
        const overstockByCat = {};
        overstockData.forEach((r) => {
            overstockByCat[r.category] = (overstockByCat[r.category] ?? 0) + 1;
        });
        const overstockCatSorted = Object.entries(overstockByCat).sort((a, b) => b[1] - a[1]).slice(0, 10);

        const overstockCatData = {
            labels: overstockCatSorted.map(([c]) => c),
            datasets: [{
                label: 'Overstock Products',
                data: overstockCatSorted.map(([, v]) => v),
                backgroundColor: PALETTE,
            }],
        };

        // ---- Reserved vs available (stacked bar, top 10 by reserved_share) ---
        const top10Reserved = [...reservedData]
            .sort((a, b) => (b.reserved_share ?? 0) - (a.reserved_share ?? 0))
            .slice(0, 10);

        const reservedStackedData = {
            labels: top10Reserved.map((r) => r.product_name ?? r.product_id),
            datasets: [
                {
                    label: 'Available Stock',
                    data: top10Reserved.map((r) => r.available_stock ?? 0),
                    backgroundColor: 'rgba(34,197,94,0.82)',
                    stack: 'stock',
                },
                {
                    label: 'Reserved Stock',
                    data: top10Reserved.map((r) => r.reserved_quantity ?? 0),
                    backgroundColor: 'rgba(239,68,68,0.82)',
                    stack: 'stock',
                },
            ],
        };

        // ---- Reserved share doughnut ----------------------------------------
        const totalAvailable = reservedData.reduce((s, r) => s + (r.available_stock ?? 0), 0);
        const totalReserved  = reservedData.reduce((s, r) => s + (r.reserved_quantity ?? 0), 0);

        const reservedDoughnut = {
            labels: ['Available', 'Reserved'],
            datasets: [{
                data: [totalAvailable, totalReserved],
                backgroundColor: ['rgba(34,197,94,0.82)', 'rgba(239,68,68,0.82)'],
                borderWidth: 2,
            }],
        };

        // ---- Top 10 margin erosion risk (by storage_cost_to_revenue) ---------
        const top10MarginRisk = [...marginRisk]
            .sort((a, b) => (b.storage_cost_to_revenue ?? 0) - (a.storage_cost_to_revenue ?? 0))
            .slice(0, 10);

        const marginRiskBarData = {
            labels: top10MarginRisk.map((r) => r.product_name ?? r.product_id),
            datasets: [{
                label: 'Storage Cost to Revenue Ratio',
                data: top10MarginRisk.map((r) => r.storage_cost_to_revenue ?? 0),
                backgroundColor: 'rgba(249,115,22,0.82)',
            }],
        };

        // ---- Excess inventory: days_of_supply_effective (top 10) ------------
        const top10Excess = [...excessData]
            .sort((a, b) => (b.days_of_supply_effective ?? 0) - (a.days_of_supply_effective ?? 0))
            .slice(0, 10);

        const excessDosData = {
            labels: top10Excess.map((r) => r.product_name ?? r.product_id),
            datasets: [{
                label: 'Days of Supply (Effective)',
                data: top10Excess.map((r) => r.days_of_supply_effective ?? 0),
                backgroundColor: 'rgba(234,179,8,0.82)',
            }],
        };

        return {
            totalCarryingCost, overstockCount, excessCount, avgStorageCostPU,
            carryingBarData, overstockCatData, reservedStackedData, reservedDoughnut,
            marginRiskBarData, excessDosData,
            overstockData, marginRisk, excessData,
        };
    }, [rawData]);

    const hasData = !!(derived && derived.overstockCount !== undefined);

    // -------------------------------------------------------------------------
    // Chart options
    // -------------------------------------------------------------------------

    const barOpts = (horizontal = false, stacked = false) => ({
        responsive: true, maintainAspectRatio: false, indexAxis: horizontal ? 'y' : 'x',
        plugins: { legend: { display: stacked } },
        scales: {
            x: { stacked, beginAtZero: true,
                 ticks: stacked ? {} : {},
               },
            y: { stacked, beginAtZero: !horizontal },
        },
    });

    const currencyBarOpts = (horizontal = false) => ({
        responsive: true, maintainAspectRatio: false, indexAxis: horizontal ? 'y' : 'x',
        plugins: {
            legend: { display: !horizontal },
            tooltip: { callbacks: { label: (ctx) => fmt.currency(ctx.raw) } },
        },
        scales: {
            x: { beginAtZero: true, ticks: { callback: (v) => fmt.compact(v) } },
            y: { beginAtZero: !horizontal },
        },
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
                    <p className="text-gray-500 text-base">Loading inventory efficiency…</p>
                </div>
            );
        }

    if (!hasData) {
        return (
            <div className="flex items-center justify-center min-h-[60vh]">
                <p className="text-gray-500">No efficiency data available yet. Run the analytics pipeline first.</p>
            </div>
        );
    }
    if (!derived) return null;
    const {
        totalCarryingCost, overstockCount, excessCount, avgStorageCostPU,
        carryingBarData, overstockCatData, reservedStackedData, reservedDoughnut,
        marginRiskBarData, excessDosData,
        overstockData, marginRisk, excessData,
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
                <KPICard icon="pi-dollar"        iconBg="bg-purple-100" iconColor="text-purple-600" value={fmt.compact(totalCarryingCost)} label="Total Carrying Cost" />
                <KPICard icon="pi-inbox"         iconBg="bg-orange-100" iconColor="text-orange-600" value={fmt.number(overstockCount)}     label="Overstock Products" />
                <KPICard icon="pi-clock"         iconBg="bg-yellow-100" iconColor="text-yellow-600" value={fmt.number(excessCount)}        label="Excess Inventory SKUs" />
                <KPICard icon="pi-tag"           iconBg="bg-blue-100"   iconColor="text-blue-600"   value={fmt.currency(avgStorageCostPU)} label="Avg Cost Per Unit" />
            </div>

            {/* ── Carrying Costs & Reserved Stock ─────────────────────────── */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                <ChartWrapper title="Top 10 Products by Carrying Cost" height={360}>
                    <Bar data={carryingBarData} options={currencyBarOpts(true)} />
                </ChartWrapper>

                <ChartWrapper title="Overall Reserved vs. Available Stock" height={360}>
                    <Doughnut data={reservedDoughnut} options={doughnutOpts} />
                </ChartWrapper>
            </div>

            {/* ── Overstock & Reserved Breakdown ─────────────────────────── */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                <ChartWrapper title="Overstock Products by Category" height={340}>
                    <Bar data={overstockCatData} options={barOpts()} />
                </ChartWrapper>

                <ChartWrapper title="Top 10 Products — Reserved vs Available Stock" height={340}>
                    <Bar data={reservedStackedData} options={barOpts(false, true)} />
                </ChartWrapper>
            </div>

            {/* ── Margin Risk & Excess DOS ───────────────────────────────── */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                <ChartWrapper title="Top 10 Margin Erosion Risk (Cost-to-Revenue)" height={360}>
                    <Bar data={marginRiskBarData} options={barOpts(true)} />
                </ChartWrapper>

                <ChartWrapper title="Top 10 Excess Inventory by Days of Supply" height={360}>
                    <Bar data={excessDosData} options={barOpts(true)} />
                </ChartWrapper>
            </div>

            {/* ── Overstock Analysis Table ───────────────────────────────── */}
            <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                <div className="p-6">
                    <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                        Overstock Analysis
                    </h3>
                    <DataTable
                        value={overstockData}
                        paginator rows={10} rowsPerPageOptions={[10, 25, 50]}
                        className="p-datatable-sm" stripedRows sortMode="multiple"
                        defaultSortField="storage_cost" defaultSortOrder={-1}
                    >
                        <Column field="product_name"      header="Product"          sortable style={{ minWidth: '180px' }} />
                        <Column field="category"          header="Category"         sortable />
                        <Column field="stock_health_score" header="Health Score"    sortable body={(r) => fmt.decimal(r.stock_health_score, 1)} />
                        <Column field="storage_cost"      header="Storage Cost"     sortable body={(r) => fmt.currency(r.storage_cost)} />
                        <Column field="storage_cost_per_unit" header="Cost / Unit"  sortable body={(r) => fmt.currency(r.storage_cost_per_unit)} />
                        <Column field="current_stock"     header="Current Stock"    sortable body={(r) => fmt.number(r.current_stock)} />
                        <Column field="available_stock"   header="Available"        sortable body={(r) => fmt.number(r.available_stock)} />
                        <Column field="days_of_supply"    header="Days of Supply"   sortable body={(r) => fmt.decimal(r.days_of_supply, 1)} />
                        <Column field="stock_status"      header="Status"
                            body={(r) => <Tag value={r.stock_status} severity={r.stock_status === 'Overstocked' ? 'warning' : 'info'} />} />
                    </DataTable>
                </div>
            </Card>

            {/* ── Margin Erosion Risk Table ──────────────────────────────── */}
            <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                <div className="p-6">
                    <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                        Margin Erosion Risk
                    </h3>
                    <DataTable
                        value={[...marginRisk].sort((a, b) => (b.storage_cost_to_revenue ?? 0) - (a.storage_cost_to_revenue ?? 0))}
                        paginator rows={10} rowsPerPageOptions={[10, 25]}
                        className="p-datatable-sm" stripedRows sortMode="multiple"
                    >
                        <Column field="product_name"               header="Product"               sortable style={{ minWidth: '180px' }} />
                        <Column field="category"                   header="Category"              sortable />
                        <Column field="storage_cost"               header="Storage Cost"          sortable body={(r) => fmt.currency(r.storage_cost)} />
                        <Column field="storage_cost_per_unit"      header="Cost / Unit"           sortable body={(r) => fmt.currency(r.storage_cost_per_unit)} />
                        <Column field="storage_cost_to_revenue"    header="Cost / Revenue"        sortable body={(r) => fmt.pct((r.storage_cost_to_revenue ?? 0) * 100)} />
                        <Column field="storage_cost_per_unit_to_price" header="Cost-to-Price"    sortable body={(r) => fmt.pct((r.storage_cost_per_unit_to_price ?? 0) * 100)} />
                        <Column field="total_revenue"              header="Revenue"               sortable body={(r) => fmt.currency(r.total_revenue)} />
                        <Column field="stock_health_score"         header="Health Score"          sortable body={(r) => fmt.decimal(r.stock_health_score, 1)} />
                    </DataTable>
                </div>
            </Card>

            {/* ── Excess Inventory Table ─────────────────────────────────── */}
            <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                <div className="p-6">
                    <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                        Excess Inventory Not Selling
                    </h3>
                    <DataTable
                        value={[...excessData].sort((a, b) => (b.days_of_supply_effective ?? 0) - (a.days_of_supply_effective ?? 0))}
                        paginator rows={10} rowsPerPageOptions={[10, 25]}
                        className="p-datatable-sm" stripedRows sortMode="multiple"
                    >
                        <Column field="product_name"              header="Product"              sortable style={{ minWidth: '180px' }} />
                        <Column field="category"                  header="Category"             sortable />
                        <Column field="available_stock"           header="Available Stock"      sortable body={(r) => fmt.number(r.available_stock)} />
                        <Column field="current_stock"             header="Current Stock"        sortable body={(r) => fmt.number(r.current_stock)} />
                        <Column field="avg_daily_sales_effective" header="Avg Daily Sales"      sortable body={(r) => fmt.decimal(r.avg_daily_sales_effective, 2)} />
                        <Column field="days_of_supply_effective"  header="Days of Supply"       sortable body={(r) => fmt.decimal(r.days_of_supply_effective, 1)} />
                        <Column field="days_since_launch"         header="Days Since Launch"    sortable body={(r) => fmt.number(r.days_since_launch)} />
                        <Column field="total_units_sold"          header="Units Sold"           sortable body={(r) => fmt.number(r.total_units_sold)} />
                        <Column field="stock_health_score"        header="Health Score"         sortable body={(r) => fmt.decimal(r.stock_health_score, 1)} />
                    </DataTable>
                </div>
            </Card>
        </div>
    );
}
