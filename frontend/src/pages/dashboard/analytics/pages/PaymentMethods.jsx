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
        y: { grid: { color: 'rgba(0,0,0,0.05)' } },
    },
});

const groupedBarOpts = () => ({
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { position: 'top' }, title: { display: false } },
    scales: {
        x: { grid: { color: 'rgba(0,0,0,0.05)' } },
        y: { grid: { color: 'rgba(0,0,0,0.05)' } },
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

export default function PaymentMethods() {
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
            console.error('[PaymentMethods] fetch error');
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
        toastRef.current?.show({ severity: 'info', summary: 'Data Updated', detail: 'Analytics pipeline completed — refreshing payment data.', life: 3000 });
    }, [lastUpdate]); // eslint-disable-line

    // -------------------------------------------------------------------------
    // Derived data
    // -------------------------------------------------------------------------

    const derived = useMemo(() => {
        if (!rawPayment) return null;
        const a = rawPayment.analytics ?? {};

        const successRates      = a.payment_method_success_rates?.data             ?? [];
        const aovByMethod       = a.payment_method_aov?.data                       ?? [];
        const countsByCountry   = a.payment_counts_by_country_method?.data         ?? [];
        const countsByState     = a.payment_counts_by_state_method?.data           ?? [];
        const successByCountry  = a.payment_method_success_rates_by_country?.data  ?? [];

        if (successRates.length === 0 && aovByMethod.length === 0) return null;

        // ---- KPIs -----------------------------------------------------------
        const totalMethods       = successRates.length;
        const totalPayments      = successRates.reduce((s, r) => s + (+(r.total_payments ?? 0)), 0);
        const overallSuccessRate = totalPayments > 0
            ? successRates.reduce((s, r) => s + (+(r.completed_payments ?? 0)), 0) / totalPayments * 100
            : 0;
        const topMethod          = [...successRates].sort((a, b) => (+(b.success_rate ?? 0)) - (+(a.success_rate ?? 0)))[0];
        const totalRevenue       = aovByMethod.reduce((s, r) => s + (+(r.total_revenue ?? 0)), 0);

        // ---- Success rate bar -----------------------------------------------
        const successSorted = [...successRates].sort((a, b) => (+(b.success_rate ?? 0)) - (+(a.success_rate ?? 0)));
        const successBarData = {
            labels: successSorted.map((r) => r.payment_method ?? 'Unknown'),
            datasets: [{
                label: 'Success Rate %',
                data: successSorted.map((r) => +(r.success_rate ?? 0).toFixed(2)),
                backgroundColor: successSorted.map((r) => {
                    const rate = +(r.success_rate ?? 0);
                    if (rate >= 90) return 'rgba(34,197,94,0.82)';
                    if (rate >= 70) return 'rgba(234,179,8,0.82)';
                    return 'rgba(239,68,68,0.82)';
                }),
            }],
        };

        // ---- Total payments doughnut ----------------------------------------
        const paymentDoughnutData = {
            labels: successSorted.map((r) => r.payment_method ?? 'Unknown'),
            datasets: [{ data: successSorted.map((r) => +(r.total_payments ?? 0)), backgroundColor: PALETTE }],
        };

        // ---- AOV by method bar ---------------------------------------------
        const aovSorted = [...aovByMethod].sort((a, b) => (+(b.avg_order_value_method ?? 0)) - (+(a.avg_order_value_method ?? 0)));
        const aovBarData = {
            labels: aovSorted.map((r) => r.payment_method ?? 'Unknown'),
            datasets: [{
                label: 'Avg Order Value ($)',
                data: aovSorted.map((r) => +(r.avg_order_value_method ?? 0).toFixed(2)),
                backgroundColor: 'rgba(59,130,246,0.82)',
            }],
        };

        // ---- Revenue by method bar -----------------------------------------
        const revSorted = [...aovByMethod].sort((a, b) => (+(b.total_revenue ?? 0)) - (+(a.total_revenue ?? 0)));
        const revenueBarData = {
            labels: revSorted.map((r) => r.payment_method ?? 'Unknown'),
            datasets: [{
                label: 'Total Revenue ($)',
                data: revSorted.map((r) => +(r.total_revenue ?? 0).toFixed(2)),
                backgroundColor: PALETTE,
            }],
        };

        // ---- Payment volume doughnut ----------------------------------------
        const revDoughnutData = {
            labels: revSorted.map((r) => r.payment_method ?? 'Unknown'),
            datasets: [{ data: revSorted.map((r) => +(r.total_revenue ?? 0).toFixed(2)), backgroundColor: PALETTE }],
        };

        // ---- Orders by method bar ------------------------------------------
        const ordersSorted = [...aovByMethod].sort((a, b) => (+(b.order_count ?? 0)) - (+(a.order_count ?? 0)));
        const ordersBarData = {
            labels: ordersSorted.map((r) => r.payment_method ?? 'Unknown'),
            datasets: [{
                label: 'Order Count',
                data: ordersSorted.map((r) => +(r.order_count ?? 0)),
                backgroundColor: 'rgba(139,92,246,0.82)',
            }],
        };

        // ---- Top countries by payment count (top 10 per method) ------------
        const topCountries = [...countsByCountry]
            .sort((a, b) => (+(b.payment_count ?? 0)) - (+(a.payment_count ?? 0)))
            .slice(0, 12);
        const countryLabels = [...new Set(topCountries.map((r) => r.country ?? 'Unknown'))].slice(0, 10);
        const methodsInCountry = [...new Set(topCountries.map((r) => r.payment_method ?? 'Unknown'))];
        const countryBarData = countryLabels.length > 0 ? {
            labels: countryLabels,
            datasets: methodsInCountry.map((method, i) => ({
                label: method,
                data: countryLabels.map((country) => {
                    const row = countsByCountry.find((r) => r.country === country && r.payment_method === method);
                    return row ? +(row.payment_count ?? 0) : 0;
                }),
                backgroundColor: PALETTE[i % PALETTE.length],
            })),
        } : null;

        // ---- Success by country grouped (top 10 countries) -----------------
        const countriesForSuccess = [...new Set(successByCountry.map((r) => r.country ?? 'Unknown'))].slice(0, 10);
        const methodsForSuccess   = [...new Set(successByCountry.map((r) => r.payment_method ?? 'Unknown'))];
        const successByCountryData = countriesForSuccess.length > 0 ? {
            labels: countriesForSuccess,
            datasets: methodsForSuccess.map((method, i) => ({
                label: method,
                data: countriesForSuccess.map((country) => {
                    const row = successByCountry.find((r) => r.country === country && r.payment_method === method);
                    return row ? +(row.success_rate ?? 0).toFixed(2) : 0;
                }),
                backgroundColor: PALETTE[i % PALETTE.length],
            })),
        } : null;

        return {
            kpis: { totalMethods, totalPayments, overallSuccessRate, topMethod, totalRevenue },
            successBarData, paymentDoughnutData, aovBarData, revenueBarData, revDoughnutData,
            ordersBarData, countryBarData, successByCountryData,
            successRates, aovByMethod, countsByState,
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
                <p className="text-gray-500 text-base">Loading payment methods…</p>
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
                        <p className="text-gray-500 text-sm mt-1">Unable to load payment data. Please try again later.</p>
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
                            : 'No payment data to display.'}
                    </p>
                </div>
            </div>
        );
    }
    if (!derived) return null;
    const {
        kpis, successBarData, paymentDoughnutData, aovBarData, revenueBarData, revDoughnutData,
        ordersBarData, countryBarData, successByCountryData,
        successRates, aovByMethod,
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
                    * Payment analytics are static aggregates computed over all available data and do not change with the date filter.
                </p>
            )}

            {/* ── KPI Cards ──────────────────────────────────────────────── */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <KPICard
                    icon="pi-credit-card" iconBg="bg-blue-100" iconColor="text-blue-600"
                    value={fmt.number(kpis.totalMethods)}
                    label="Payment Methods"
                />
                <KPICard
                    icon="pi-check-circle" iconBg="bg-green-100" iconColor="text-green-600"
                    value={fmt.pct(kpis.overallSuccessRate)}
                    label="Overall Success Rate"
                />
                <KPICard
                    icon="pi-dollar" iconBg="bg-amber-100" iconColor="text-amber-600"
                    value={fmt.currency(kpis.totalRevenue)}
                    label="Total Revenue"
                />
                <KPICard
                    icon="pi-trophy" iconBg="bg-purple-100" iconColor="text-purple-600"
                    value={kpis.topMethod?.payment_method ?? '—'}
                    label="Top Method (by Success Rate)"
                />
            </div>

            {/* ── Success Rate & Volume ──────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-green-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Success Rate & Volume</h2>
                </div>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {successBarData.labels.length > 0 && (
                        <ChartWrapper title="Payment Success Rate by Method" height={320}>
                            <Bar data={successBarData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                    {paymentDoughnutData.labels.length > 0 && (
                        <ChartWrapper title="Payment Volume Share by Method" height={280}>
                            <Doughnut data={paymentDoughnutData} options={doughnutOpts()} />
                        </ChartWrapper>
                    )}
                </div>
            </section>

            {/* ── Revenue & Order Value ──────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-blue-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Revenue & Order Value</h2>
                </div>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {revenueBarData.labels.length > 0 && (
                        <ChartWrapper title="Total Revenue by Payment Method" height={320}>
                            <Bar data={revenueBarData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                    {revDoughnutData.labels.length > 0 && (
                        <ChartWrapper title="Revenue Share by Payment Method" height={280}>
                            <Doughnut data={revDoughnutData} options={doughnutOpts()} />
                        </ChartWrapper>
                    )}
                    {aovBarData.labels.length > 0 && (
                        <ChartWrapper title="Avg Order Value by Payment Method" height={320}>
                            <Bar data={aovBarData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                    {ordersBarData.labels.length > 0 && (
                        <ChartWrapper title="Order Count by Payment Method" height={320}>
                            <Bar data={ordersBarData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                </div>
            </section>

            {/* ── Geographic Breakdown ───────────────────────────────────── */}
            {(countryBarData || successByCountryData) && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-amber-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Geographic Breakdown</h2>
                    </div>
                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                        {countryBarData && (
                            <ChartWrapper title="Payment Counts by Country & Method (Top 10)" height={380}>
                                <Bar data={countryBarData} options={groupedBarOpts()} />
                            </ChartWrapper>
                        )}
                        {successByCountryData && (
                            <ChartWrapper title="Success Rate by Country & Method (Top 10)" height={380}>
                                <Bar data={successByCountryData} options={groupedBarOpts()} />
                            </ChartWrapper>
                        )}
                    </div>
                </section>
            )}

            {/* ── Summary Tables ─────────────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-purple-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Summary Tables</h2>
                </div>

                {/* Success Rates Table */}
                {successRates.length > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                Payment Method Success Rates
                            </h3>
                            <DataTable value={[...successRates].sort((a, b) => (+(b.success_rate ?? 0)) - (+(a.success_rate ?? 0)))}
                                scrollable stripedRows emptyMessage="No data" className="text-sm">
                                <Column field="payment_method"     header="Payment Method"    sortable />
                                <Column field="total_payments"     header="Total Payments"    sortable body={(r) => fmt.number(r.total_payments)} />
                                <Column field="completed_payments" header="Completed"         sortable body={(r) => fmt.number(r.completed_payments)} />
                                <Column field="distinct_orders"    header="Distinct Orders"   sortable body={(r) => fmt.number(r.distinct_orders)} />
                                <Column field="success_rate"       header="Success Rate"      sortable body={(r) => (
                                    <Tag value={fmt.pct(r.success_rate)}
                                        severity={(+(r.success_rate ?? 0)) >= 90 ? 'success' : (+(r.success_rate ?? 0)) >= 70 ? 'warning' : 'danger'} />
                                )} />
                            </DataTable>
                        </div>
                    </Card>
                )}

                {/* AOV by Method Table */}
                {aovByMethod.length > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                Revenue & Order Value by Payment Method
                            </h3>
                            <DataTable value={[...aovByMethod].sort((a, b) => (+(b.total_revenue ?? 0)) - (+(a.total_revenue ?? 0)))}
                                scrollable stripedRows emptyMessage="No data" className="text-sm">
                                <Column field="payment_method"          header="Payment Method"    sortable />
                                <Column field="payment_count"           header="Payments"          sortable body={(r) => fmt.number(r.payment_count)} />
                                <Column field="order_count"             header="Orders"            sortable body={(r) => fmt.number(r.order_count)} />
                                <Column field="total_revenue"           header="Total Revenue"     sortable body={(r) => fmt.currency(r.total_revenue)} />
                                <Column field="avg_order_value_method"  header="Avg Order Value"   sortable body={(r) => fmt.currency(r.avg_order_value_method)} />
                            </DataTable>
                        </div>
                    </Card>
                )}

                {/* Payment Counts by State & Method Table */}
                {(derived?.countsByState?.length ?? 0) > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                Payment Counts by State &amp; Method
                            </h3>
                            <DataTable
                                value={[...derived.countsByState].sort((a, b) => (+(b.payment_count ?? 0)) - (+(a.payment_count ?? 0)))}
                                paginator rows={10} stripedRows emptyMessage="No data" className="text-sm"
                            >
                                <Column field="country"        header="Country"         sortable />
                                <Column field="state_province" header="State/Province"  sortable />
                                <Column field="payment_method" header="Payment Method"  sortable />
                                <Column field="payment_count"  header="Payments"        sortable body={(r) => fmt.number(r.payment_count)} />
                                <Column field="order_count"    header="Orders"          sortable body={(r) => fmt.number(r.order_count)} />
                            </DataTable>
                        </div>
                    </Card>
                )}
            </section>
        </div>
    );
}
