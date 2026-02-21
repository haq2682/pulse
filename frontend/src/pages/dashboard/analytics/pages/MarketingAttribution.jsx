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
// Formatters
// ---------------------------------------------------------------------------

const fmt = {
    number:   (v) => new Intl.NumberFormat('en-US').format(v ?? 0),
    currency: (v) => `$${new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(v ?? 0)}`,
    decimal:  (v, d = 2) => (+(v ?? 0)).toFixed(d),
    pct:      (v) => `${(+(v ?? 0)).toFixed(1)}%`,
};

const camLabel = (r, idField = 'campaign_id') => r.campaign_name || `Campaign ${r[idField]}`;

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

const barOpts = (horizontal = false) => ({
    indexAxis: horizontal ? 'y' : 'x',
    responsive: true,
    plugins: { legend: { display: false }, title: { display: false } },
    scales: {
        x: { grid: { color: 'rgba(0,0,0,0.05)' } },
        y: { grid: { color: 'rgba(0,0,0,0.05)' } },
    },
});

const groupedBarOpts = () => ({
    responsive: true,
    plugins: { legend: { position: 'top' }, title: { display: false } },
    scales: {
        x: { grid: { color: 'rgba(0,0,0,0.05)' } },
        y: { grid: { color: 'rgba(0,0,0,0.05)' } },
    },
});

const doughnutOpts = () => ({
    responsive: true,
    plugins: { legend: { position: 'right' }, title: { display: false } },
});

// ---------------------------------------------------------------------------
// Main Component
// ---------------------------------------------------------------------------

export default function MarketingAttribution() {
    const { businessId } = useParams();
    const toastRef = useRef(null);
    const { pipelineStatus } = usePipelineProgress();
    const { dateRange, setDateRange, quickFilter, isFiltered, applyQuickFilter, resetFilters } = useAnalyticsDateFilter();
    const { lastUpdate } = useAnalyticsWebSocket(businessId);

    const [rawMarketing, setRawMarketing] = useState(null);
    const [loading, setLoading] = useState(true);
    const [fetchError, setFetchError] = useState(false);
    const [dataMode, setDataMode] = useState('unknown');

    // -------------------------------------------------------------------------
    // Fetch
    // -------------------------------------------------------------------------

    const buildUrl = useCallback(() => {
        const base = import.meta.env.VITE_API_URL || 'http://localhost:8000';
        const params = new URLSearchParams({ categories: 'marketing_analytics' });
        return `${base}/analytics/data/${businessId}?${params.toString()}`;
    }, [businessId]);

    const fetchData = useCallback(async () => {
        if (!businessId) return;
        try {
            setLoading(true);
            setFetchError(false);
            const res = await fetch(buildUrl());
            if (!res.ok) {
                toastRef.current?.show({
                    severity: 'warn', summary: 'No Data',
                    detail: 'Analytics data not available. Run the analytics pipeline first.',
                    life: 5000,
                });
                setRawMarketing(null);
                return;
            }
            const json = await res.json();
            if (json.mode) setDataMode(json.mode);
            setRawMarketing(json.categories?.marketing_analytics ?? null);
        } catch {
            console.error('[MarketingAttribution] fetch error');
            setFetchError(true);
            setRawMarketing(null);
            toastRef.current?.show({ severity: 'error', summary: 'Error', detail: 'Unable to load attribution data.', life: 5000 });
        } finally {
            setLoading(false);
        }
    }, [buildUrl, businessId]);

    useEffect(() => { fetchData(); }, [businessId]); // eslint-disable-line
    useEffect(() => {
        if (!lastUpdate) return;
        fetchData();
        toastRef.current?.show({ severity: 'info', summary: 'Data Updated', detail: 'Analytics pipeline completed — refreshing data.', life: 3000 });
    }, [lastUpdate]); // eslint-disable-line

    // -------------------------------------------------------------------------
    // Derived data
    // -------------------------------------------------------------------------

    const derived = useMemo(() => {
        if (!rawMarketing) return null;
        const a = rawMarketing.analytics ?? {};

        const ltv         = a.campaign_ltv?.data ?? [];
        const ltvSummary  = a.campaign_customer_ltv_summary?.data ?? [];
        const contribution = a.campaign_product_contribution?.data ?? [];
        const margin      = a.campaign_margin_profile?.data ?? [];
        // campaign_performance_summary for campaign names cross-reference
        const summary     = a.campaign_performance_summary?.data ?? [];

        const ltvData = ltv.length > 0 ? ltv : ltvSummary;

        if (ltvData.length === 0 && contribution.length === 0 && margin.length === 0) return null;

        // Build a campaign name map from summary
        const nameMap = {};
        summary.forEach((r) => { nameMap[r.campaign_id] = r.campaign_name; });

        const ltvLabel = (r) => nameMap[r.campaign_id] || `Campaign ${r.campaign_id}`;

        // ---- KPIs -----------------------------------------------------------
        const totalAttributedRevenue = ltvData.reduce((s, r) => s + (+(r.campaign_revenue_from_orders ?? 0)), 0);
        const totalLtvData = ltvData.length;
        const avgCLV = totalLtvData > 0
            ? ltvData.reduce((s, r) => s + (+(r.avg_customer_lifetime_value ?? 0)), 0) / totalLtvData
            : 0;
        const avgHighCLVShare = totalLtvData > 0
            ? ltvData.reduce((s, r) => s + (+(r.high_clv_share ?? 0)), 0) / totalLtvData
            : 0;
        const topContrib = [...contribution]
            .sort((a, b) => (+(b.product_revenue ?? 0)) - (+(a.product_revenue ?? 0)))[0];

        // ---- CLV by campaign (top 12) ---------------------------------------
        const clvSorted = [...ltvData]
            .sort((a, b) => (+(b.avg_customer_lifetime_value ?? 0)) - (+(a.avg_customer_lifetime_value ?? 0)))
            .slice(0, 12);
        const clvBarData = {
            labels: clvSorted.map(ltvLabel),
            datasets: [{ label: 'Avg CLV ($)', data: clvSorted.map((r) => +(r.avg_customer_lifetime_value ?? 0).toFixed(2)), backgroundColor: 'rgba(59,130,246,0.82)' }],
        };

        // ---- High-CLV share bar (top 12) ------------------------------------
        const highClvSorted = [...ltvData]
            .sort((a, b) => (+(b.high_clv_share ?? 0)) - (+(a.high_clv_share ?? 0)))
            .slice(0, 12);
        const highClvBarData = {
            labels: highClvSorted.map(ltvLabel),
            datasets: [{ label: 'High-CLV Share %', data: highClvSorted.map((r) => +(r.high_clv_share ?? 0).toFixed(2)), backgroundColor: 'rgba(34,197,94,0.82)' }],
        };

        // ---- Attributed revenue bar (top 12) --------------------------------
        const revSorted = [...ltvData]
            .sort((a, b) => (+(b.campaign_revenue_from_orders ?? 0)) - (+(a.campaign_revenue_from_orders ?? 0)))
            .slice(0, 12);
        const attributedRevBarData = {
            labels: revSorted.map(ltvLabel),
            datasets: [{ label: 'Attributed Revenue ($)', data: revSorted.map((r) => +(r.campaign_revenue_from_orders ?? 0).toFixed(2)), backgroundColor: 'rgba(249,115,22,0.82)' }],
        };

        // ---- Distinct customers bar (top 12) --------------------------------
        const custSorted = [...ltvData]
            .sort((a, b) => (+(b.distinct_customers ?? 0)) - (+(a.distinct_customers ?? 0)))
            .slice(0, 12);
        const custBarData = {
            labels: custSorted.map(ltvLabel),
            datasets: [{ label: 'Distinct Customers', data: custSorted.map((r) => +(r.distinct_customers ?? 0)), backgroundColor: 'rgba(139,92,246,0.82)' }],
        };

        // ---- Top products horizontal bar (top 12 by product_revenue) --------
        const topProds = [...contribution]
            .sort((a, b) => (+(b.product_revenue ?? 0)) - (+(a.product_revenue ?? 0)))
            .slice(0, 12);
        const topProdsBarData = {
            labels: topProds.map((r) => r.product_name || `Product ${r.product_id}`),
            datasets: [{ label: 'Product Revenue ($)', data: topProds.map((r) => +(r.product_revenue ?? 0).toFixed(2)), backgroundColor: PALETTE }],
        };

        // ---- Product revenue by category (aggregated) -----------------------
        const catRevMap = {};
        contribution.forEach((r) => {
            const c = r.category ?? 'Unknown';
            catRevMap[c] = (catRevMap[c] ?? 0) + (+(r.product_revenue ?? 0));
        });
        const catRevSorted = Object.entries(catRevMap)
            .sort(([, a], [, b]) => b - a)
            .slice(0, 10);
        const catRevBarData = {
            labels: catRevSorted.map(([k]) => k),
            datasets: [{ label: 'Revenue by Category ($)', data: catRevSorted.map(([, v]) => +v.toFixed(2)), backgroundColor: PALETTE }],
        };

        // ---- Revenue share doughnut (by category top 8) --------------------
        const catRevDoughnutData = {
            labels: catRevSorted.slice(0, 8).map(([k]) => k),
            datasets: [{ data: catRevSorted.slice(0, 8).map(([, v]) => +v.toFixed(2)), backgroundColor: PALETTE }],
        };

        // ---- Margin profile bar (top 12 by campaign_avg_product_margin) -----
        const marginSorted = [...margin]
            .sort((a, b) => (+(b.campaign_avg_product_margin ?? 0)) - (+(a.campaign_avg_product_margin ?? 0)))
            .slice(0, 12);
        const marginBarData = {
            labels: marginSorted.map((r) => nameMap[r.campaign_id] || `Campaign ${r.campaign_id}`),
            datasets: [{ label: 'Avg Product Margin %', data: marginSorted.map((r) => +(r.campaign_avg_product_margin ?? 0).toFixed(2)), backgroundColor: 'rgba(6,182,212,0.82)' }],
        };

        // ---- Margin revenue bar (top 12 by campaign_products_revenue) -------
        const marginRevSorted = [...margin]
            .sort((a, b) => (+(b.campaign_products_revenue ?? 0)) - (+(a.campaign_products_revenue ?? 0)))
            .slice(0, 12);
        const marginRevBarData = {
            labels: marginRevSorted.map((r) => nameMap[r.campaign_id] || `Campaign ${r.campaign_id}`),
            datasets: [{ label: 'Products Revenue ($)', data: marginRevSorted.map((r) => +(r.campaign_products_revenue ?? 0).toFixed(2)), backgroundColor: 'rgba(234,179,8,0.82)' }],
        };

        // ---- CLV table (merged with name) -----------------------------------
        const clvTableData = ltvData.map((r) => ({
            ...r,
            _name: nameMap[r.campaign_id] || `Campaign ${r.campaign_id}`,
        }));

        return {
            kpis: { totalAttributedRevenue, avgCLV, avgHighCLVShare, topContribName: topContrib?.product_name ?? '—' },
            clvBarData, highClvBarData, attributedRevBarData, custBarData,
            topProdsBarData, catRevBarData, catRevDoughnutData, marginBarData, marginRevBarData,
            clvTableData, contribution, margin, nameMap,
        };
    }, [rawMarketing]);

    // -------------------------------------------------------------------------
    // Render
    // -------------------------------------------------------------------------

    const hasData = derived !== null;

    if (loading && pipelineStatus !== 'running') {
        return (
            <div className="flex items-center justify-center min-h-[60vh]">
                <ProgressSpinner style={{ width: '48px', height: '48px' }} />
            </div>
        );
    }

    if (fetchError) {
        return (
            <div className="flex items-center justify-center min-h-[60vh]">
                <div className="text-center max-w-md">
                    <i className="pi pi-exclamation-triangle text-5xl text-red-400 mb-4" />
                    <p className="text-gray-600 text-lg font-medium">Something went wrong</p>
                    <p className="text-gray-400 text-sm mt-2">Please try refreshing the page.</p>
                </div>
            </div>
        );
    }

    if (!hasData) {
        return (
            <div className="p-6 space-y-4">
                <Toast ref={toastRef} />
                <DateFilterBar
                    quickFilter={quickFilter} dateRange={dateRange} isFiltered={isFiltered}
                    onQuickFilter={applyQuickFilter} onDateChange={setDateRange} onReset={resetFilters}
                    dataMode={dataMode}
                />
                <div className="flex items-center justify-center min-h-[50vh]">
                    <div className="text-center max-w-md">
                        <i className="pi pi-chart-bar text-5xl text-gray-300 mb-4" />
                        <p className="text-gray-500 text-lg font-medium">No data to display</p>
                        <p className="text-gray-400 text-sm mt-2">Run the analytics pipeline first.</p>
                    </div>
                </div>
            </div>
        );
    }

    const { kpis, clvBarData, highClvBarData, attributedRevBarData, custBarData,
            topProdsBarData, catRevBarData, catRevDoughnutData, marginBarData, marginRevBarData,
            clvTableData, contribution, margin, nameMap } = derived;

    return (
        <div className="p-6 space-y-8">
            <Toast ref={toastRef} />

            {/* Date Filter */}
            <DateFilterBar
                quickFilter={quickFilter} dateRange={dateRange} isFiltered={isFiltered}
                onQuickFilter={applyQuickFilter} onDateChange={setDateRange} onReset={resetFilters}
                dataMode={dataMode}
            />

            {/* KPIs */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <KPICard icon="pi-chart-line"  iconBg="bg-blue-50"   iconColor="text-blue-600"   value={`$${new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 }).format(kpis.totalAttributedRevenue)}`} label="Attributed Revenue" />
                <KPICard icon="pi-users"       iconBg="bg-green-50"  iconColor="text-green-600"  value={`$${(+(kpis.avgCLV ?? 0)).toFixed(2)}`} label="Avg Customer LTV" />
                <KPICard icon="pi-star"        iconBg="bg-purple-50" iconColor="text-purple-600" value={`${(+(kpis.avgHighCLVShare ?? 0)).toFixed(1)}%`} label="Avg High-CLV Share" />
                <KPICard icon="pi-box"         iconBg="bg-orange-50" iconColor="text-orange-600" value={kpis.topContribName} label="Top Contributing Product" />
            </div>

            {/* CLV + High-CLV Share */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {clvBarData.labels.length > 0 && (
                    <Card className="rounded-xl shadow-sm border border-gray-200">
                        <h3 className="text-sm font-semibold text-gray-700 mb-4">Avg Customer LTV by Campaign (Top 12)</h3>
                        <ChartWrapper><Bar data={clvBarData} options={barOpts(true)} /></ChartWrapper>
                    </Card>
                )}
                {highClvBarData.labels.length > 0 && (
                    <Card className="rounded-xl shadow-sm border border-gray-200">
                        <h3 className="text-sm font-semibold text-gray-700 mb-4">High-CLV Customer Share % (Top 12)</h3>
                        <ChartWrapper><Bar data={highClvBarData} options={barOpts(true)} /></ChartWrapper>
                    </Card>
                )}
            </div>

            {/* Attributed Revenue + Customers */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {attributedRevBarData.labels.length > 0 && (
                    <Card className="rounded-xl shadow-sm border border-gray-200">
                        <h3 className="text-sm font-semibold text-gray-700 mb-4">Attributed Revenue by Campaign (Top 12)</h3>
                        <ChartWrapper><Bar data={attributedRevBarData} options={barOpts(true)} /></ChartWrapper>
                    </Card>
                )}
                {custBarData.labels.length > 0 && (
                    <Card className="rounded-xl shadow-sm border border-gray-200">
                        <h3 className="text-sm font-semibold text-gray-700 mb-4">Distinct Customers by Campaign (Top 12)</h3>
                        <ChartWrapper><Bar data={custBarData} options={barOpts(true)} /></ChartWrapper>
                    </Card>
                )}
            </div>

            {/* Product Contribution */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {topProdsBarData.labels.length > 0 && (
                    <Card className="rounded-xl shadow-sm border border-gray-200">
                        <h3 className="text-sm font-semibold text-gray-700 mb-4">Top Products by Revenue (Top 12)</h3>
                        <ChartWrapper><Bar data={topProdsBarData} options={barOpts(true)} /></ChartWrapper>
                    </Card>
                )}
                {catRevDoughnutData.labels.length > 0 && (
                    <Card className="rounded-xl shadow-sm border border-gray-200">
                        <h3 className="text-sm font-semibold text-gray-700 mb-4">Revenue by Category (Top 8)</h3>
                        <ChartWrapper><Doughnut data={catRevDoughnutData} options={doughnutOpts()} /></ChartWrapper>
                    </Card>
                )}
            </div>

            {/* Category Revenue + Margin Profile */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {catRevBarData.labels.length > 0 && (
                    <Card className="rounded-xl shadow-sm border border-gray-200">
                        <h3 className="text-sm font-semibold text-gray-700 mb-4">Revenue by Category (Top 10)</h3>
                        <ChartWrapper><Bar data={catRevBarData} options={barOpts(true)} /></ChartWrapper>
                    </Card>
                )}
                {marginBarData.labels.length > 0 && (
                    <Card className="rounded-xl shadow-sm border border-gray-200">
                        <h3 className="text-sm font-semibold text-gray-700 mb-4">Avg Product Margin % by Campaign (Top 12)</h3>
                        <ChartWrapper><Bar data={marginBarData} options={barOpts(true)} /></ChartWrapper>
                    </Card>
                )}
            </div>

            {/* Margin Revenue */}
            {marginRevBarData.labels.length > 0 && (
                <Card className="rounded-xl shadow-sm border border-gray-200">
                    <h3 className="text-sm font-semibold text-gray-700 mb-4">Campaign Products Revenue (Top 12)</h3>
                    <ChartWrapper><Bar data={marginRevBarData} options={barOpts(true)} /></ChartWrapper>
                </Card>
            )}

            {/* CLV Attribution Table */}
            {clvTableData.length > 0 && (
                <Card className="rounded-xl shadow-sm border border-gray-200">
                    <h3 className="text-sm font-semibold text-gray-700 mb-4">Customer LTV Attribution by Campaign</h3>
                    <DataTable value={clvTableData} paginator rows={10} scrollable stripedRows
                        emptyMessage="No LTV attribution data" className="text-sm">
                        <Column field="_name"                       header="Campaign"           sortable />
                        <Column field="distinct_customers"          header="Customers"          sortable body={(r) => fmt.number(r.distinct_customers)} />
                        <Column field="campaign_revenue_from_orders" header="Revenue"           sortable body={(r) => fmt.currency(r.campaign_revenue_from_orders)} />
                        <Column field="avg_customer_lifetime_value" header="Avg CLV"            sortable body={(r) => fmt.currency(r.avg_customer_lifetime_value)} />
                        <Column field="num_customers_with_clv"      header="Customers w/ CLV"  sortable body={(r) => fmt.number(r.num_customers_with_clv)} />
                        <Column field="high_clv_customers"          header="High-CLV Customers" sortable body={(r) => fmt.number(r.high_clv_customers)} />
                        <Column field="high_clv_share"              header="High-CLV Share %"  sortable body={(r) => fmt.pct(r.high_clv_share)} />
                    </DataTable>
                </Card>
            )}

            {/* Product Contribution Table */}
            {contribution.length > 0 && (
                <Card className="rounded-xl shadow-sm border border-gray-200">
                    <h3 className="text-sm font-semibold text-gray-700 mb-4">Product Contribution by Campaign</h3>
                    <DataTable value={contribution} paginator rows={15} scrollable stripedRows
                        emptyMessage="No product contribution data" className="text-sm">
                        <Column field="campaign_id"          header="Campaign"       sortable body={(r) => nameMap[r.campaign_id] || `Campaign ${r.campaign_id}`} />
                        <Column field="product_name"         header="Product"        sortable body={(r) => r.product_name || `Product ${r.product_id}`} />
                        <Column field="category"             header="Category"       sortable />
                        <Column field="brand"                header="Brand"          sortable />
                        <Column field="units_sold"           header="Units Sold"     sortable body={(r) => fmt.number(r.units_sold)} />
                        <Column field="product_revenue"      header="Revenue"        sortable body={(r) => fmt.currency(r.product_revenue)} />
                        <Column field="orders_count"         header="Orders"         sortable body={(r) => fmt.number(r.orders_count)} />
                        <Column field="avg_product_margin"   header="Avg Margin %"   sortable body={(r) => fmt.pct(r.avg_product_margin)} />
                        <Column field="campaign_revenue"     header="Campaign Rev."  sortable body={(r) => fmt.currency(r.campaign_revenue)} />
                        <Column field="product_revenue_share" header="Rev. Share %"  sortable body={(r) => fmt.pct(r.product_revenue_share)} />
                    </DataTable>
                </Card>
            )}

            {/* Margin Profile Table */}
            {margin.length > 0 && (
                <Card className="rounded-xl shadow-sm border border-gray-200">
                    <h3 className="text-sm font-semibold text-gray-700 mb-4">Campaign Margin Profile</h3>
                    <DataTable value={margin} paginator rows={10} scrollable stripedRows
                        emptyMessage="No margin data" className="text-sm">
                        <Column field="campaign_id"                header="Campaign"       sortable body={(r) => nameMap[r.campaign_id] || `Campaign ${r.campaign_id}`} />
                        <Column field="campaign_avg_product_margin" header="Avg Margin %"  sortable body={(r) => fmt.pct(r.campaign_avg_product_margin)} />
                        <Column field="campaign_products_revenue"  header="Products Rev."  sortable body={(r) => fmt.currency(r.campaign_products_revenue)} />
                        <Column field="campaign_units_sold"        header="Units Sold"     sortable body={(r) => fmt.number(r.campaign_units_sold)} />
                    </DataTable>
                </Card>
            )}
        </div>
    );
}
