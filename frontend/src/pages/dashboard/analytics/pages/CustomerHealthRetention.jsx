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
import { Line, Bar, Doughnut } from 'react-chartjs-2';
import { useAnalyticsWebSocket } from '../../../../hooks/useAnalyticsWebSocket';
import ChartWrapper from '../components/ChartWrapper';
import { usePipelineProgress } from '@/context/PipelineProgressContext';
import useAnalyticsDateFilter from '@/hooks/useAnalyticsDateFilter';
import DateFilterBar from '../components/DateFilterBar';
import { useFormatters } from '@/hooks/useFormatters';

ChartJS.register(
    CategoryScale, LinearScale, PointElement, LineElement,
    BarElement, ArcElement, Title, Tooltip, Legend
);

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PALETTE = [
    'rgba(59,130,246,0.8)',
    'rgba(34,197,94,0.8)',
    'rgba(249,115,22,0.8)',
    'rgba(239,68,68,0.8)',
    'rgba(139,92,246,0.8)',
    'rgba(6,182,212,0.8)',
    'rgba(234,179,8,0.8)',
    'rgba(236,72,153,0.8)',
    'rgba(20,184,166,0.8)',
    'rgba(168,85,247,0.8)',
];

const CHURN_COLORS = {
    'High':   'rgba(239,68,68,0.85)',
    'Medium': 'rgba(234,179,8,0.85)',
    'Low':    'rgba(34,197,94,0.85)',
};

const COHORT_LINE_COLORS = [
    'rgb(59,130,246)',
    'rgb(34,197,94)',
    'rgb(249,115,22)',
    'rgb(139,92,246)',
    'rgb(6,182,212)',
    'rgb(236,72,153)',
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

const MetricRow = ({ label, value }) => (
    <div className="flex justify-between items-center p-3 bg-gray-50 rounded-lg hover:bg-gray-100 transition-colors">
        <span className="text-gray-700 font-medium">{label}</span>
        <span className="text-gray-900 font-semibold text-lg">{value}</span>
    </div>
);

const MetricsCard = ({ title, rows }) => {
    const visible = rows.filter((r) => r.show);
    if (visible.length === 0) return null;
    return (
        <Card className="bg-white border border-gray-200 rounded-xl p-0 shadow-sm">
            <div className="p-6">
                <h3 className="text-xl font-semibold text-gray-900 mb-6 pb-3 border-b-2 border-gray-200">{title}</h3>
                <div className="flex flex-col gap-4">
                    {visible.map((r) => <MetricRow key={r.label} label={r.label} value={r.value} />)}
                </div>
            </div>
        </Card>
    );
};

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

const CustomerHealthRetention = () => {
    const { businessId } = useParams();
    const toastRef = useRef(null);
    const { pipelineStatus } = usePipelineProgress();

    const [loading, setLoading] = useState(true);
    const [fetchError, setFetchError] = useState(false);
    const [rawData, setRawData] = useState(null);
    const [dataMode, setDataMode] = useState('unknown');
    const fmt = useFormatters();
    const {
        dateRange, setDateRange, quickFilter, isFiltered,
        applyQuickFilter, resetFilters, toISODate,
    } = useAnalyticsDateFilter();

    const { lastUpdate } = useAnalyticsWebSocket(businessId);

    // -----------------------------------------------------------------------
    // Fetch
    // -----------------------------------------------------------------------

    const buildUrl = useCallback((from, to) => {
        const base = import.meta.env.VITE_API_URL || 'http://localhost:8000';
        const params = new URLSearchParams({ categories: 'customer_analytics' });
        if (from) params.set('date_from', toISODate(from));
        if (to)   params.set('date_to',   toISODate(to));
        return `${base}/analytics/data/${businessId}?${params.toString()}`;
    }, [businessId, toISODate]);

    const fetchData = useCallback(async (from, to) => {
        if (!businessId) return;
        setLoading(true);
            setFetchError(false);
        try {
            const res = await fetch(buildUrl(from, to));
            if (!res.ok) {
                toastRef.current?.show({
                    severity: 'warn', summary: 'No Data',
                    detail: 'Analytics data not available. Run the analytics pipeline first.',
                    life: 5000,
                });
                setRawData(null);
                return;
            }
            const json = await res.json();
            if (json.mode) setDataMode(json.mode);
            setRawData(json.categories?.customer_analytics ?? null);
        } catch {
            console.error('[fetch] Analytics load error');
            setFetchError(true);
            setRawData(null);
        } finally {
            setLoading(false);
        }
    }, [businessId, buildUrl]);

    useEffect(() => { fetchData(null, null); }, [businessId]); // eslint-disable-line
    useEffect(() => { fetchData(dateRange.from, dateRange.to); }, [dateRange]); // eslint-disable-line
    useEffect(() => {
        if (lastUpdate?.files) {
            toastRef.current?.show({
                severity: 'info', summary: 'Data Updated',
                detail: `${lastUpdate.total_files} metric(s) updated`, life: 3000,
            });
            fetchData(dateRange.from, dateRange.to);
        }
    }, [lastUpdate]); // eslint-disable-line

    // -----------------------------------------------------------------------
    // Derived data — all static aggregates
    // -----------------------------------------------------------------------

    const derived = useMemo(() => {
        if (!rawData) return null;
        const a = rawData.analytics ?? {};

        // churn_risk_summary — static
        const churnRiskSummary = a.churn_risk_summary?.data ?? [];
        const highRiskRow   = churnRiskSummary.find((r) => r.churn_risk === 'High') ?? {};
        const medRiskRow    = churnRiskSummary.find((r) => r.churn_risk === 'Medium') ?? {};
        const lowRiskRow    = churnRiskSummary.find((r) => r.churn_risk === 'Low') ?? {};
        const totalAtRisk   = (highRiskRow.num_customers ?? 0) + (medRiskRow.num_customers ?? 0);

        // customer_cohort_retention — static
        const cohortRetention = a.customer_cohort_retention?.data ?? [];

        // signup_cohort_summary — static
        const signupCohorts = a.signup_cohort_summary?.data ?? [];

        // high_clv_at_risk — static
        const highClvAtRisk = a.high_clv_at_risk?.data ?? [];

        // high_value_abandoners — static
        const highValueAbandoners = a.high_value_abandoners?.data ?? [];

        // cart_behavior_summary — static
        const cartBehavior = (a.cart_behavior_summary?.data ?? [])[0] ?? {};

        // referrer_churn_summary — static
        const referrerChurn = a.referrer_churn_summary?.data ?? [];

        // payment_method_vs_clv_churn — static
        const paymentChurn = a.payment_method_vs_clv_churn?.data ?? [];

        // discount_customers_summary — static
        const discountSummary = a.discount_customers_summary?.data ?? [];

        // high_intent_non_buyers — static
        const highIntentNonBuyers = a.high_intent_non_buyers?.data ?? [];

        return {
            churnRiskSummary, highRiskRow, medRiskRow, lowRiskRow, totalAtRisk,
            cohortRetention, signupCohorts, highClvAtRisk, highValueAbandoners,
            cartBehavior, referrerChurn, paymentChurn, discountSummary, highIntentNonBuyers,
        };
    }, [rawData]);

    // -----------------------------------------------------------------------
    // Chart data
    // -----------------------------------------------------------------------

    // Churn risk summary — bar
    const churnBarData = useMemo(() => {
        const rows = derived?.churnRiskSummary ?? [];
        return {
            labels: rows.map((r) => r.churn_risk ?? ''),
            datasets: [{
                label: 'Customers',
                data: rows.map((r) => r.num_customers ?? 0),
                backgroundColor: rows.map((r) => CHURN_COLORS[r.churn_risk] ?? 'rgba(107,114,128,0.8)'),
            }],
        };
    }, [derived]);

    // Churn risk — avg CLV bar
    const churnClvData = useMemo(() => {
        const rows = derived?.churnRiskSummary ?? [];
        return {
            labels: rows.map((r) => r.churn_risk ?? ''),
            datasets: [{
                label: 'Avg CLV',
                data: rows.map((r) => r.avg_clv ?? 0),
                backgroundColor: rows.map((r) => CHURN_COLORS[r.churn_risk] ?? 'rgba(107,114,128,0.8)'),
            }],
        };
    }, [derived]);

    // Cohort retention curves — multi-line (show last 6 cohorts for clarity)
    const cohortLineData = useMemo(() => {
        const rows = derived?.cohortRetention ?? [];
        if (rows.length === 0) return null;

        // Get unique cohorts, pick most recent 6
        const allCohorts = [...new Set(rows.map((r) => r.signup_cohort_month))].sort().slice(-6);
        const allMonths  = [...new Set(rows.map((r) => r.months_since_signup))].sort((a, b) => a - b);

        return {
            labels: allMonths.map((m) => `Month ${m}`),
            datasets: allCohorts.map((cohort, i) => {
                const cohortRows = rows.filter((r) => r.signup_cohort_month === cohort);
                return {
                    label: cohort,
                    data: allMonths.map((m) => {
                        const row = cohortRows.find((r) => r.months_since_signup === m);
                        return row ? ((row.retention_rate ?? 0) * 100).toFixed(1) : null;
                    }),
                    borderColor: COHORT_LINE_COLORS[i % COHORT_LINE_COLORS.length],
                    backgroundColor: COHORT_LINE_COLORS[i % COHORT_LINE_COLORS.length].replace('rgb', 'rgba').replace(')', ',0.1)'),
                    tension: 0.4,
                    spanGaps: false,
                };
            }),
        };
    }, [derived]);

    // Signup cohort summary — bar
    const signupCohortBarData = useMemo(() => {
        const rows = [...(derived?.signupCohorts ?? [])].sort((a, b) =>
            String(a.signup_cohort_month).localeCompare(String(b.signup_cohort_month))
        );
        return {
            labels: rows.map((r) => r.signup_cohort_month ?? ''),
            datasets: [{
                label: 'Cohort Customers',
                data: rows.map((r) => r.cohort_customers ?? 0),
                backgroundColor: 'rgba(59,130,246,0.8)',
            }],
        };
    }, [derived]);

    // Cart behavior — doughnut
    const cartDoughnutData = useMemo(() => {
        const cb = derived?.cartBehavior ?? {};
        const abandoned  = cb.total_abandoned_carts ?? 0;
        const purchased  = cb.total_purchased_carts ?? 0;
        if (abandoned + purchased === 0) return null;
        return {
            labels: ['Abandoned Carts', 'Purchased Carts'],
            datasets: [{
                data: [abandoned, purchased],
                backgroundColor: ['rgba(239,68,68,0.8)', 'rgba(34,197,94,0.8)'],
            }],
        };
    }, [derived]);

    // Referrer churn — stacked bar
    const referrerChurnData = useMemo(() => {
        const rows = derived?.referrerChurn ?? [];
        const referrers = [...new Set(rows.map((r) => r.preferred_referrer_source ?? ''))];
        const churnLevels = [...new Set(rows.map((r) => r.churn_risk ?? ''))];
        return {
            labels: referrers,
            datasets: churnLevels.map((cl) => ({
                label: cl,
                data: referrers.map((ref) => {
                    const row = rows.find((r) => r.preferred_referrer_source === ref && r.churn_risk === cl);
                    return row?.customer_count ?? 0;
                }),
                backgroundColor: CHURN_COLORS[cl] ?? 'rgba(107,114,128,0.8)',
            })),
        };
    }, [derived]);

    // Payment × Churn — stacked bar
    const paymentChurnData = useMemo(() => {
        const rows = derived?.paymentChurn ?? [];
        const methods = [...new Set(rows.map((r) => r.preferred_payment_method ?? ''))];
        const churnLevels = [...new Set(rows.map((r) => r.churn_risk ?? ''))];
        return {
            labels: methods,
            datasets: churnLevels.map((cl) => ({
                label: cl,
                data: methods.map((m) => {
                    const row = rows.find((r) => r.preferred_payment_method === m && r.churn_risk === cl);
                    return row?.customer_count ?? 0;
                }),
                backgroundColor: CHURN_COLORS[cl] ?? 'rgba(107,114,128,0.8)',
            })),
        };
    }, [derived]);

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    const churnTagSeverity = (risk) => {
        if (!risk) return 'secondary';
        const r = risk.toLowerCase();
        if (r === 'high') return 'danger';
        if (r === 'medium') return 'warning';
        return 'success';
    };

    const hasData = useMemo(() => {
        if (!derived) return false;
        return (
            derived.churnRiskSummary.length > 0 ||
            derived.cohortRetention.length > 0 ||
            derived.highClvAtRisk.length > 0
        );
    }, [derived]);

    // -----------------------------------------------------------------------
    // Render
    // -----------------------------------------------------------------------

    if (fetchError && !loading) {
        return (
            <div className="p-6 min-h-[calc(100vh-120px)]">
                <Toast ref={toastRef} />
                <div className="flex items-center justify-center min-h-[50vh]">
                    <div className="text-center">
                        <i className="pi pi-exclamation-circle text-5xl text-red-400 mb-3 block" />
                        <p className="text-gray-700 font-medium text-lg">Something went wrong</p>
                        <p className="text-gray-500 text-sm mt-1">Unable to load analytics data. Please try again later.</p>
                    </div>
                </div>
            </div>
        );
    }

    if (loading && pipelineStatus !== 'loading') {
        return (
            <div className="flex flex-col items-center justify-center min-h-[400px] gap-4">
                <ProgressSpinner />
                <p className="text-gray-500 text-base">Loading customer health & retention…</p>
            </div>
        );
    }

    if (!hasData && !loading && pipelineStatus !== 'loading') {
        return (
            <div className="p-6 min-h-[calc(100vh-120px)]">
                <Toast ref={toastRef} />
                <DateFilterBar
                    quickFilter={quickFilter} dateRange={dateRange} isFiltered={isFiltered}
                    onQuickFilter={applyQuickFilter} onDateChange={setDateRange}
                    onReset={resetFilters} dataMode={dataMode}
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

    const cb = derived?.cartBehavior ?? {};

    return (
        <div className="p-6 bg-gray-50 min-h-[calc(100vh-120px)]">
            <Toast ref={toastRef} />

            <DateFilterBar
                quickFilter={quickFilter} dateRange={dateRange} isFiltered={isFiltered}
                onQuickFilter={applyQuickFilter} onDateChange={setDateRange}
                onReset={resetFilters} dataMode={dataMode}
                hidden={loading && pipelineStatus === 'loading'}
            />

            {/* All data on this page is static */}
            <p className="mb-6 text-xs text-gray-400 italic">
                * All health and retention metrics are static aggregates computed over all-time records.
                They reflect the full dataset and are not filtered by the date picker.
            </p>

            {/* KPI Cards */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-8">
                {(derived?.highRiskRow?.num_customers ?? 0) > 0 && (
                    <KPICard
                        icon="pi-exclamation-triangle" iconBg="bg-red-50" iconColor="text-red-500"
                        value={fmt.number(derived.highRiskRow.num_customers)}
                        label="High Churn Risk Customers *"
                    />
                )}
                {(derived?.medRiskRow?.num_customers ?? 0) > 0 && (
                    <KPICard
                        icon="pi-info-circle" iconBg="bg-yellow-50" iconColor="text-yellow-500"
                        value={fmt.number(derived.medRiskRow.num_customers)}
                        label="Medium Churn Risk Customers *"
                    />
                )}
                {(derived?.lowRiskRow?.num_customers ?? 0) > 0 && (
                    <KPICard
                        icon="pi-check-circle" iconBg="bg-green-50" iconColor="text-green-500"
                        value={fmt.number(derived.lowRiskRow.num_customers)}
                        label="Low Churn Risk Customers *"
                    />
                )}
                {(derived?.highRiskRow?.avg_clv ?? 0) > 0 && (
                    <KPICard
                        icon="pi-dollar" iconBg="bg-orange-50" iconColor="text-orange-500"
                        value={fmt.currency(derived.highRiskRow.avg_clv)}
                        label="Avg CLV — High Risk *"
                    />
                )}
                {(derived?.highClvAtRisk?.length ?? 0) > 0 && (
                    <KPICard
                        icon="pi-user-minus" iconBg="bg-rose-50" iconColor="text-rose-500"
                        value={fmt.number(derived.highClvAtRisk.length)}
                        label="High-CLV Customers At Risk *"
                    />
                )}
                {(derived?.signupCohorts?.length ?? 0) > 0 && (
                    <KPICard
                        icon="pi-calendar" iconBg="bg-purple-50" iconColor="text-purple-500"
                        value={fmt.number(derived.signupCohorts.length)}
                        label="Customer Cohorts Tracked *"
                    />
                )}
            </div>

            {/* Churn Risk Charts */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
                {(derived?.churnRiskSummary?.length ?? 0) > 0 && (
                    <ChartWrapper title="Churn Risk — Customer Count *" showUpdateBadge={false}>
                        <div className="h-[280px]">
                            <Bar
                                data={churnBarData}
                                options={{
                                    responsive: true, maintainAspectRatio: false,
                                    plugins: { legend: { display: false } },
                                    scales: { y: { beginAtZero: true } },
                                }}
                            />
                        </div>
                    </ChartWrapper>
                )}

                {(derived?.churnRiskSummary?.length ?? 0) > 0 && (
                    <ChartWrapper title="Churn Risk — Average CLV *" showUpdateBadge={false}>
                        <div className="h-[280px]">
                            <Bar
                                data={churnClvData}
                                options={{
                                    responsive: true, maintainAspectRatio: false,
                                    plugins: { legend: { display: false } },
                                    scales: { y: { beginAtZero: true, ticks: { callback: (v) => '$' + v.toLocaleString() } } },
                                }}
                            />
                        </div>
                    </ChartWrapper>
                )}
            </div>

            {/* Churn Risk Summary Metrics */}
            {(derived?.churnRiskSummary?.length ?? 0) > 0 && (
                <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
                    {derived.churnRiskSummary.map((row) => (
                        <MetricsCard
                            key={row.churn_risk}
                            title={`${row.churn_risk} Churn Risk *`}
                            rows={[
                                { label: 'Customers', value: fmt.number(row.num_customers), show: (row.num_customers ?? 0) > 0 },
                                { label: 'Total Revenue', value: fmt.currency(row.total_revenue), show: (row.total_revenue ?? 0) > 0 },
                                { label: 'Avg CLV', value: fmt.currency(row.avg_clv), show: (row.avg_clv ?? 0) > 0 },
                                { label: 'Avg Recency', value: fmt.days(row.avg_recency_days), show: (row.avg_recency_days ?? 0) > 0 },
                            ]}
                        />
                    ))}
                </div>
            )}

            {/* Cohort Retention */}
            {cohortLineData && (
                <div className="mb-8">
                    <ChartWrapper title="Customer Cohort Retention Curves (last 6 cohorts) *" showUpdateBadge={false}>
                        <div className="h-[320px]">
                            <Line
                                data={cohortLineData}
                                options={{
                                    responsive: true, maintainAspectRatio: false,
                                    plugins: { legend: { display: true, position: 'top' } },
                                    scales: {
                                        y: {
                                            beginAtZero: true, min: 0, max: 100,
                                            title: { display: true, text: 'Retention Rate (%)' },
                                            ticks: { callback: (v) => v + '%' },
                                        },
                                        x: { title: { display: true, text: 'Months since Signup' } },
                                    },
                                }}
                            />
                        </div>
                    </ChartWrapper>
                </div>
            )}

            {/* Signup Cohort Summary */}
            {(derived?.signupCohorts?.length ?? 0) > 0 && (
                <div className="mb-8">
                    <ChartWrapper title="Signup Cohort Size by Month *" showUpdateBadge={false}>
                        <div className="h-[280px]">
                            <Bar
                                data={signupCohortBarData}
                                options={{
                                    responsive: true, maintainAspectRatio: false,
                                    plugins: { legend: { display: false } },
                                    scales: { y: { beginAtZero: true } },
                                }}
                            />
                        </div>
                    </ChartWrapper>
                </div>
            )}

            {/* Referrer × Churn + Payment × Churn */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
                {(derived?.referrerChurn?.length ?? 0) > 0 && (
                    <ChartWrapper title="Churn Risk by Referrer Source *" showUpdateBadge={false}>
                        <div className="h-[300px]">
                            <Bar
                                data={referrerChurnData}
                                options={{
                                    responsive: true, maintainAspectRatio: false,
                                    plugins: { legend: { display: true, position: 'top' } },
                                    scales: { x: { stacked: true }, y: { beginAtZero: true, stacked: true } },
                                }}
                            />
                        </div>
                    </ChartWrapper>
                )}

                {(derived?.paymentChurn?.length ?? 0) > 0 && (
                    <ChartWrapper title="Churn Risk by Payment Method *" showUpdateBadge={false}>
                        <div className="h-[300px]">
                            <Bar
                                data={paymentChurnData}
                                options={{
                                    responsive: true, maintainAspectRatio: false,
                                    plugins: { legend: { display: true, position: 'top' } },
                                    scales: { x: { stacked: true }, y: { beginAtZero: true, stacked: true } },
                                }}
                            />
                        </div>
                    </ChartWrapper>
                )}

                {/* Cart Behavior */}
                {cartDoughnutData && (
                    <ChartWrapper title="Cart: Abandoned vs Purchased *" showUpdateBadge={false}>
                        <div className="h-[280px]">
                            <Doughnut
                                data={cartDoughnutData}
                                options={{
                                    responsive: true, maintainAspectRatio: false,
                                    plugins: { legend: { display: true, position: 'right' } },
                                }}
                            />
                        </div>
                    </ChartWrapper>
                )}

                {/* Cart Behavior Metrics */}
                <MetricsCard
                    title="Cart Behavior Summary *"
                    rows={[
                        { label: 'Total Carts Created', value: fmt.number(cb.total_carts_created), show: (cb.total_carts_created ?? 0) > 0 },
                        { label: 'Total Abandoned Carts', value: fmt.number(cb.total_abandoned_carts), show: (cb.total_abandoned_carts ?? 0) > 0 },
                        { label: 'Total Purchased Carts', value: fmt.number(cb.total_purchased_carts), show: (cb.total_purchased_carts ?? 0) > 0 },
                        { label: 'Avg Abandonment Rate', value: fmt.pct(cb.avg_cart_abandonment_rate), show: (cb.avg_cart_abandonment_rate ?? 0) > 0 },
                        { label: 'Total Abandoned Value', value: fmt.currency(cb.total_abandoned_value), show: (cb.total_abandoned_value ?? 0) > 0 },
                        { label: 'Avg Time in Cart', value: `${(cb.avg_time_in_cart_days ?? 0).toFixed(1)} days`, show: (cb.avg_time_in_cart_days ?? 0) > 0 },
                    ]}
                />
            </div>

            {/* High CLV At-Risk Table */}
            {(derived?.highClvAtRisk?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">
                            High-CLV Customers At Risk *
                        </h3>
                        <DataTable
                            value={[...derived.highClvAtRisk].sort((a, b) => (b.customer_lifetime_value ?? 0) - (a.customer_lifetime_value ?? 0))}
                            paginator rows={10} stripedRows size="small"
                        >
                            <Column field="customer_id" header="Customer ID" sortable />
                            <Column field="customer_lifetime_value" header="CLV" sortable body={(r) => fmt.currency(r.customer_lifetime_value)} />
                            <Column field="churn_risk" header="Churn Risk" sortable
                                body={(r) => <Tag value={r.churn_risk} severity={churnTagSeverity(r.churn_risk)} />}
                            />
                            <Column field="customer_activity_score" header="Activity Score" sortable body={(r) => (r.customer_activity_score ?? 0).toFixed(2)} />
                            <Column field="order_recency_days" header="Order Recency (days)" sortable body={(r) => fmt.days(r.order_recency_days)} />
                            <Column field="days_since_last_login" header="Days Since Login" sortable body={(r) => (r.days_since_last_login ?? 0).toFixed(0)} />
                            <Column field="rfm_segment" header="RFM Segment" sortable />
                            <Column field="rfm_category" header="RFM Category" sortable />
                        </DataTable>
                    </div>
                </Card>
            )}

            {/* High Value Abandoners Table */}
            {(derived?.highValueAbandoners?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">
                            High-Value Cart Abandoners *
                        </h3>
                        <DataTable
                            value={[...derived.highValueAbandoners].sort((a, b) => (b.total_abandoned_value ?? 0) - (a.total_abandoned_value ?? 0))}
                            paginator rows={10} stripedRows size="small"
                        >
                            <Column field="customer_id" header="Customer ID" sortable />
                            <Column field="total_abandoned_carts" header="Abandoned Carts" sortable body={(r) => fmt.number(r.total_abandoned_carts)} />
                            <Column field="total_abandoned_value" header="Abandoned Value" sortable body={(r) => fmt.currency(r.total_abandoned_value)} />
                            <Column field="cart_abandonment_rate" header="Abandonment Rate" sortable body={(r) => fmt.pct(r.cart_abandonment_rate)} />
                            <Column field="total_revenue" header="Total Revenue" sortable body={(r) => fmt.currency(r.total_revenue)} />
                            <Column field="customer_lifetime_value" header="CLV" sortable body={(r) => fmt.currency(r.customer_lifetime_value)} />
                        </DataTable>
                    </div>
                </Card>
            )}

            {/* High Intent Non-Buyers Table */}
            {(derived?.highIntentNonBuyers?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">
                            High-Intent Non-Buyers (Conversion Opportunities) *
                        </h3>
                        <DataTable
                            value={[...derived.highIntentNonBuyers].sort((a, b) => (b.total_products_viewed ?? 0) - (a.total_products_viewed ?? 0))}
                            paginator rows={10} stripedRows size="small"
                        >
                            <Column field="customer_id" header="Customer ID" sortable />
                            <Column field="total_products_viewed" header="Products Viewed" sortable body={(r) => fmt.number(r.total_products_viewed)} />
                            <Column field="wishlist_items_count" header="Wishlist Items" sortable body={(r) => fmt.number(r.wishlist_items_count)} />
                            <Column field="total_carts_created" header="Carts Created" sortable body={(r) => fmt.number(r.total_carts_created)} />
                            <Column field="total_purchased_carts" header="Purchased Carts" sortable body={(r) => fmt.number(r.total_purchased_carts)} />
                            <Column field="cart_abandonment_rate" header="Cart Abandon Rate" sortable body={(r) => fmt.pct(r.cart_abandonment_rate)} />
                            <Column field="session_conversion_rate" header="Session Conv. Rate" sortable body={(r) => fmt.pct(r.session_conversion_rate)} />
                        </DataTable>
                    </div>
                </Card>
            )}

            {/* Discount Customers Summary (static) */}
            {(derived?.discountSummary?.length ?? 0) > 0 && (
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-8">
                    {derived.discountSummary.map((row) => (
                        <MetricsCard
                            key={String(row.is_discount_hunter)}
                            title={row.is_discount_hunter ? 'Discount Hunters *' : 'Non-Discount Customers *'}
                            rows={[
                                { label: 'Customer Count', value: fmt.number(row.customer_count), show: (row.customer_count ?? 0) > 0 },
                                { label: 'Avg Discount Share', value: fmt.pct(row.avg_discount_share), show: (row.avg_discount_share ?? 0) > 0 },
                                { label: 'Avg Discount / Order', value: fmt.currency(row.avg_discount_per_order), show: (row.avg_discount_per_order ?? 0) > 0 },
                                { label: 'Avg CLV', value: fmt.currency(row.avg_clv), show: (row.avg_clv ?? 0) > 0 },
                                { label: 'Avg Revenue', value: fmt.currency(row.avg_revenue), show: (row.avg_revenue ?? 0) > 0 },
                            ]}
                        />
                    ))}
                </div>
            )}
        </div>
    );
};

export default CustomerHealthRetention;
