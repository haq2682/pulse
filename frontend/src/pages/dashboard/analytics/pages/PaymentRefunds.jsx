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
    CategoryScale, LinearScale, BarElement, PointElement, LineElement,
    ArcElement, Title, Tooltip, Legend,
} from 'chart.js';
import { Bar, Line, Doughnut } from 'react-chartjs-2';
import { useAnalyticsWebSocket } from '../../../../hooks/useAnalyticsWebSocket';
import ChartWrapper from '../components/ChartWrapper';
import { usePipelineProgress } from '@/context/PipelineProgressContext';
import useAnalyticsDateFilter from '@/hooks/useAnalyticsDateFilter';
import DateFilterBar from '../components/DateFilterBar';
import { useFormatters } from '@/hooks/useFormatters';

ChartJS.register(
    CategoryScale, LinearScale, BarElement, PointElement, LineElement,
    ArcElement, Title, Tooltip, Legend,
);

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
        y: { grid: { color: 'rgba(0,0,0,0.05)' } },
    },
});

const lineOpts = () => ({
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { display: true, position: 'top' }, title: { display: false } },
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

export default function PaymentRefunds() {
    const fmt = useFormatters();
    const { businessId } = useParams();
    const toastRef = useRef(null);
    const { pipelineStatus } = usePipelineProgress();
    const { dateRange, setDateRange, quickFilter, isFiltered, applyQuickFilter, resetFilters } = useAnalyticsDateFilter();
    const { lastUpdate } = useAnalyticsWebSocket(businessId);

    const [rawPayment, setRawPayment] = useState(null);
    const [loading, setLoading] = useState(true);
    const [fetchError, setFetchError] = useState(false);
    const [dataMode, setDataMode] = useState('unknown');

    // -------------------------------------------------------------------------
    // Fetch
    // -------------------------------------------------------------------------

    const buildUrl = useCallback(() => {
        const base = import.meta.env.VITE_API_URL || 'http://localhost:8000';
        const params = new URLSearchParams({ categories: 'payment_analytics' });
        return `${base}/analytics/data/${businessId}?${params.toString()}`;
    }, [businessId]);

    const fetchData = useCallback(async () => {
        if (!businessId) return;
        try {
            setLoading(true);
            setFetchError(false);
            const res = await fetch(buildUrl());
            if (!res.ok) {
                setRawPayment(null);
                return;
            }
            const json = await res.json();
            if (json.mode) setDataMode(json.mode);
            setRawPayment(json.categories?.payment_analytics ?? null);
        } catch {
            console.error('[PaymentRefunds] fetch error');
            setFetchError(true);
            setRawPayment(null);
        } finally {
            setLoading(false);
        }
    }, [buildUrl, businessId]);

    useEffect(() => { fetchData(); }, [businessId]); // eslint-disable-line
    useEffect(() => {
        if (!lastUpdate) return;
        fetchData();
        toastRef.current?.show({ severity: 'info', summary: 'Data Updated', detail: 'Analytics pipeline completed — refreshing refund data.', life: 3000 });
    }, [lastUpdate]); // eslint-disable-line

    // -------------------------------------------------------------------------
    // Derived data
    // -------------------------------------------------------------------------

    const derived = useMemo(() => {
        if (!rawPayment) return null;
        const a = rawPayment.analytics ?? {};

        const refundByMethod  = a.refund_rate_by_payment_method?.data  ?? [];
        const refundByProduct = a.refund_rate_by_product?.data         ?? [];
        const refundByMonth   = a.refund_rate_by_month?.data           ?? [];
        const ttpByMethod     = a.time_to_refund_by_payment_method?.data ?? [];

        if (refundByMethod.length === 0 && refundByProduct.length === 0 && refundByMonth.length === 0) return null;

        // ---- KPIs -----------------------------------------------------------
        const totalRefunds      = refundByMethod.reduce((s, r) => s + (+(r.total_refund_amount ?? 0)), 0);
        const overallRefundRate = refundByMethod.length > 0
            ? refundByMethod.reduce((s, r) => s + (+(r.refund_rate_payments ?? 0)), 0) / refundByMethod.length
            : 0;
        const worstMethod       = [...refundByMethod].sort((a, b) => (+(b.refund_rate_payments ?? 0)) - (+(a.refund_rate_payments ?? 0)))[0];
        const fastestMethod     = ttpByMethod.length > 0
            ? [...ttpByMethod].sort((a, b) => (+(a.avg_days_to_refund ?? 999)) - (+(b.avg_days_to_refund ?? 999)))[0]
            : null;
        const topRefundProduct  = [...refundByProduct].sort((a, b) => (+(b.refund_rate_orders ?? 0)) - (+(a.refund_rate_orders ?? 0)))[0];

        // ---- Refund rate by method bar ------------------------------------
        const refundRateSorted = [...refundByMethod].sort((a, b) => (+(b.refund_rate_payments ?? 0)) - (+(a.refund_rate_payments ?? 0)));
        const refundRateBarData = {
            labels: refundRateSorted.map((r) => r.payment_method ?? 'Unknown'),
            datasets: [{
                label: 'Refund Rate %',
                data: refundRateSorted.map((r) => +(r.refund_rate_payments ?? 0).toFixed(2)),
                backgroundColor: refundRateSorted.map((r) => {
                    const rate = +(r.refund_rate_payments ?? 0);
                    if (rate >= 20) return 'rgba(239,68,68,0.82)';
                    if (rate >= 10) return 'rgba(234,179,8,0.82)';
                    return 'rgba(34,197,94,0.82)';
                }),
            }],
        };

        // ---- Total refund amount by method bar ----------------------------
        const refundAmtSorted = [...refundByMethod].sort((a, b) => (+(b.total_refund_amount ?? 0)) - (+(a.total_refund_amount ?? 0)));
        const refundAmtBarData = {
            labels: refundAmtSorted.map((r) => r.payment_method ?? 'Unknown'),
            datasets: [{
                label: 'Total Refund Amount ($)',
                data: refundAmtSorted.map((r) => +(r.total_refund_amount ?? 0).toFixed(2)),
                backgroundColor: 'rgba(239,68,68,0.75)',
            }],
        };

        // ---- Avg refund per payment bar ----------------------------------
        const avgRefundBarData = {
            labels: refundRateSorted.map((r) => r.payment_method ?? 'Unknown'),
            datasets: [{
                label: 'Avg Refund per Payment ($)',
                data: refundRateSorted.map((r) => +(r.avg_refund_per_payment ?? 0).toFixed(2)),
                backgroundColor: 'rgba(249,115,22,0.82)',
            }],
        };

        // ---- Refund amount doughnut --------------------------------------
        const refundDoughnutData = {
            labels: refundAmtSorted.map((r) => r.payment_method ?? 'Unknown'),
            datasets: [{ data: refundAmtSorted.map((r) => +(r.total_refund_amount ?? 0).toFixed(2)), backgroundColor: PALETTE }],
        };

        // ---- Top products by refund rate (top 15, horizontal bar) -------
        const topRefundProdSorted = [...refundByProduct]
            .sort((a, b) => (+(b.refund_rate_orders ?? 0)) - (+(a.refund_rate_orders ?? 0)))
            .slice(0, 15);
        const topProdRefundBarData = topRefundProdSorted.length > 0 ? {
            labels: topRefundProdSorted.map((r) => r.product_name || `Product ${r.product_id}`),
            datasets: [{
                label: 'Refund Rate %',
                data: topRefundProdSorted.map((r) => +(r.refund_rate_orders ?? 0).toFixed(2)),
                backgroundColor: 'rgba(239,68,68,0.82)',
            }],
        } : null;

        // ---- Top products by total refund amount (top 12) ---------------
        const topProdAmtSorted = [...refundByProduct]
            .sort((a, b) => (+(b.total_refund_amount ?? 0)) - (+(a.total_refund_amount ?? 0)))
            .slice(0, 12);
        const topProdAmtBarData = topProdAmtSorted.length > 0 ? {
            labels: topProdAmtSorted.map((r) => r.product_name || `Product ${r.product_id}`),
            datasets: [{
                label: 'Total Refund Amount ($)',
                data: topProdAmtSorted.map((r) => +(r.total_refund_amount ?? 0).toFixed(2)),
                backgroundColor: PALETTE,
            }],
        } : null;

        // ---- Refund trend by month (line) --------------------------------
        const monthsSorted = [...refundByMonth]
            .sort((a, b) => {
                if ((a.order_placed_year ?? 0) !== (b.order_placed_year ?? 0))
                    return (+(a.order_placed_year ?? 0)) - (+(b.order_placed_year ?? 0));
                return (+(a.order_placed_month ?? 0)) - (+(b.order_placed_month ?? 0));
            });
        const monthLabels = monthsSorted.map((r) => `${r.order_placed_year}-${String(r.order_placed_month).padStart(2, '0')}`);
        const refundTrendData = monthsSorted.length > 0 ? {
            labels: monthLabels,
            datasets: [
                {
                    label: 'Refund Rate (Orders) %',
                    data: monthsSorted.map((r) => +(r.refund_rate_orders ?? 0).toFixed(2)),
                    borderColor: 'rgba(239,68,68,0.9)',
                    backgroundColor: 'rgba(239,68,68,0.15)',
                    tension: 0.3, fill: true,
                },
                {
                    label: 'Refund Rate (Amount) %',
                    data: monthsSorted.map((r) => +(r.refund_rate_amount ?? 0).toFixed(2)),
                    borderColor: 'rgba(249,115,22,0.9)',
                    backgroundColor: 'rgba(249,115,22,0.15)',
                    tension: 0.3, fill: true,
                },
            ],
        } : null;

        // ---- Time to refund by method bar --------------------------------
        const ttpSorted = [...ttpByMethod].sort((a, b) => (+(a.avg_days_to_refund ?? 999)) - (+(b.avg_days_to_refund ?? 999)));
        const ttpBarData = ttpSorted.length > 0 ? {
            labels: ttpSorted.map((r) => r.payment_method ?? 'Unknown'),
            datasets: [{
                label: 'Avg Days to Refund',
                data: ttpSorted.map((r) => +(r.avg_days_to_refund ?? 0).toFixed(1)),
                backgroundColor: 'rgba(6,182,212,0.82)',
            }],
        } : null;

        return {
            kpis: { totalRefunds, overallRefundRate, worstMethod, fastestMethod, topRefundProduct },
            refundRateBarData, refundAmtBarData, avgRefundBarData, refundDoughnutData,
            topProdRefundBarData, topProdAmtBarData, refundTrendData, ttpBarData,
            refundByMethod, refundByProduct, ttpSorted,
        };
    }, [rawPayment]);

    const hasData = derived !== null;

    // -------------------------------------------------------------------------
    // Render states
    // -------------------------------------------------------------------------

    if (loading && pipelineStatus !== 'loading') {
        return (
            <div className="flex flex-col items-center justify-center min-h-[400px] gap-4">
                <ProgressSpinner />
                <p className="text-gray-500 text-base">Loading refund analytics…</p>
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
                        <p className="text-gray-500 text-sm mt-1">Unable to load refund data. Please try again later.</p>
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
                            : 'No refund data to display.'}
                    </p>
                </div>
            </div>
        );
    }
    if (!derived) return null;
    const {
        kpis, refundRateBarData, refundAmtBarData, avgRefundBarData, refundDoughnutData,
        topProdRefundBarData, topProdAmtBarData, refundTrendData, ttpBarData,
        refundByMethod, refundByProduct, ttpSorted,
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
                    * Refund analytics are static aggregates computed over all available data and do not change with the date filter.
                </p>
            )}

            {/* ── KPI Cards ──────────────────────────────────────────────── */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <KPICard
                    icon="pi-undo" iconBg="bg-red-100" iconColor="text-red-600"
                    value={fmt.currency(kpis.totalRefunds)}
                    label="Total Refund Amount"
                />
                <KPICard
                    icon="pi-percentage" iconBg="bg-orange-100" iconColor="text-orange-600"
                    value={fmt.pct(kpis.overallRefundRate)}
                    label="Avg Refund Rate"
                />
                <KPICard
                    icon="pi-exclamation-triangle" iconBg="bg-yellow-100" iconColor="text-yellow-600"
                    value={kpis.worstMethod?.payment_method ?? '—'}
                    label="Highest Refund Rate Method"
                />
                <KPICard
                    icon="pi-clock" iconBg="bg-cyan-100" iconColor="text-cyan-600"
                    value={kpis.fastestMethod ? `${fmt.decimal(kpis.fastestMethod.avg_days_to_refund, 1)}d` : '—'}
                    label="Fastest Refund Method"
                />
            </div>

            {/* ── Refund Rate by Method ──────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-red-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Refund Rates by Payment Method</h2>
                </div>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {refundRateBarData.labels.length > 0 && (
                        <ChartWrapper title="Refund Rate % by Payment Method" height={320}>
                            <Bar data={refundRateBarData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                    {refundDoughnutData.labels.length > 0 && (
                        <ChartWrapper title="Total Refund Amount Share by Method" height={280}>
                            <Doughnut data={refundDoughnutData} options={doughnutOpts()} />
                        </ChartWrapper>
                    )}
                    {refundAmtBarData.labels.length > 0 && (
                        <ChartWrapper title="Total Refund Amount by Payment Method" height={320}>
                            <Bar data={refundAmtBarData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                    {avgRefundBarData.labels.length > 0 && (
                        <ChartWrapper title="Avg Refund Amount per Payment" height={320}>
                            <Bar data={avgRefundBarData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                </div>
            </section>

            {/* ── Monthly Trend ─────────────────────────────────────────── */}
            {refundTrendData && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-orange-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Monthly Refund Trend</h2>
                    </div>
                    <ChartWrapper title="Refund Rate % Over Time (Monthly)" height={340}>
                        <Line data={refundTrendData} options={lineOpts()} />
                    </ChartWrapper>
                </section>
            )}

            {/* ── Product Refunds ────────────────────────────────────────── */}
            {(topProdRefundBarData || topProdAmtBarData) && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-purple-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Refunds by Product</h2>
                    </div>
                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                        {topProdRefundBarData && (
                            <ChartWrapper title="Top 15 Products by Refund Rate" height={380}>
                                <Bar data={topProdRefundBarData} options={barOpts(true)} />
                            </ChartWrapper>
                        )}
                        {topProdAmtBarData && (
                            <ChartWrapper title="Top 12 Products by Refund Amount" height={380}>
                                <Bar data={topProdAmtBarData} options={barOpts(true)} />
                            </ChartWrapper>
                        )}
                    </div>
                </section>
            )}

            {/* ── Time to Refund ─────────────────────────────────────────── */}
            {ttpBarData && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-cyan-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Time to Refund</h2>
                    </div>
                    <ChartWrapper title="Avg Days to Process Refund by Payment Method" height={320}>
                        <Bar data={ttpBarData} options={barOpts()} />
                    </ChartWrapper>
                </section>
            )}

            {/* ── Summary Tables ─────────────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-gray-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Summary Tables</h2>
                </div>

                {/* Refund by Method Table */}
                {refundByMethod.length > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                Refund Rate by Payment Method
                            </h3>
                            <DataTable value={[...refundByMethod].sort((a, b) => (+(b.refund_rate_payments ?? 0)) - (+(a.refund_rate_payments ?? 0)))}
                                scrollable stripedRows emptyMessage="No data" className="text-sm">
                                <Column field="payment_method"       header="Payment Method"       sortable />
                                <Column field="total_payments"       header="Total Payments"       sortable body={(r) => fmt.number(r.total_payments)} />
                                <Column field="payments_with_refund" header="Payments w/ Refund"   sortable body={(r) => fmt.number(r.payments_with_refund)} />
                                <Column field="total_refund_amount"  header="Total Refund ($)"     sortable body={(r) => fmt.currency(r.total_refund_amount)} />
                                <Column field="avg_refund_per_payment" header="Avg Refund ($)"     sortable body={(r) => fmt.currency(r.avg_refund_per_payment)} />
                                <Column field="refund_rate_payments" header="Refund Rate"          sortable body={(r) => (
                                    <Tag value={fmt.pct(r.refund_rate_payments)}
                                        severity={(+(r.refund_rate_payments ?? 0)) >= 20 ? 'danger' : (+(r.refund_rate_payments ?? 0)) >= 10 ? 'warning' : 'success'} />
                                )} />
                            </DataTable>
                        </div>
                    </Card>
                )}

                {/* Time to Refund Table */}
                {ttpSorted.length > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                Time to Refund by Payment Method
                            </h3>
                            <DataTable value={ttpSorted} scrollable stripedRows emptyMessage="No data" className="text-sm">
                                <Column field="payment_method"      header="Payment Method"     sortable />
                                <Column field="refunded_payments"   header="Refunded Payments"  sortable body={(r) => fmt.number(r.refunded_payments)} />
                                <Column field="total_refund_amount" header="Total Refund ($)"   sortable body={(r) => fmt.currency(r.total_refund_amount)} />
                                <Column field="avg_days_to_refund"  header="Avg Days"           sortable body={(r) => fmt.decimal(r.avg_days_to_refund, 1)} />
                                <Column field="min_days_to_refund"  header="Min Days"           sortable body={(r) => fmt.decimal(r.min_days_to_refund, 1)} />
                                <Column field="max_days_to_refund"  header="Max Days"           sortable body={(r) => fmt.decimal(r.max_days_to_refund, 1)} />
                            </DataTable>
                        </div>
                    </Card>
                )}

                {/* Top Refunded Products Table */}
                {refundByProduct.length > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                Refund Rate by Product
                            </h3>
                            <DataTable value={[...refundByProduct].sort((a, b) => (+(b.refund_rate_orders ?? 0)) - (+(a.refund_rate_orders ?? 0)))}
                                paginator rows={15} scrollable stripedRows emptyMessage="No data" className="text-sm">
                                <Column field="product_name"        header="Product"            sortable body={(r) => r.product_name || `Product ${r.product_id}`} />
                                <Column field="category"            header="Category"           sortable />
                                <Column field="orders_for_product"  header="Total Orders"       sortable body={(r) => fmt.number(r.orders_for_product)} />
                                <Column field="orders_with_refund"  header="Orders w/ Refund"   sortable body={(r) => fmt.number(r.orders_with_refund)} />
                                <Column field="total_refund_amount" header="Refund Amount ($)"  sortable body={(r) => fmt.currency(r.total_refund_amount)} />
                                <Column field="refund_rate_orders"  header="Refund Rate"        sortable body={(r) => (
                                    <Tag value={fmt.pct(r.refund_rate_orders)}
                                        severity={(+(r.refund_rate_orders ?? 0)) >= 20 ? 'danger' : (+(r.refund_rate_orders ?? 0)) >= 10 ? 'warning' : 'success'} />
                                )} />
                            </DataTable>
                        </div>
                    </Card>
                )}
            </section>
        </div>
    );
}
