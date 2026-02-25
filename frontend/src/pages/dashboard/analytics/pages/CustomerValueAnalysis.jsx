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

// Correlation display card
const CorrelationCard = ({ label, value, description }) => {
    const absVal = Math.abs(value ?? 0);
    let strength = 'Negligible';
    let color = 'text-gray-500';
    if (absVal >= 0.7) { strength = 'Strong'; color = value > 0 ? 'text-green-600' : 'text-red-600'; }
    else if (absVal >= 0.4) { strength = 'Moderate'; color = value > 0 ? 'text-blue-600' : 'text-orange-600'; }
    else if (absVal >= 0.2) { strength = 'Weak'; color = 'text-yellow-600'; }

    return (
        <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
            <div className="p-6 text-center">
                <p className="text-sm text-gray-500 mb-2">{label}</p>
                <p className={`text-4xl font-bold ${color} mb-2`}>{(value ?? 0).toFixed(4)}</p>
                <span className={`text-sm font-semibold ${color}`}>{strength} {value >= 0 ? 'Positive' : 'Negative'} Correlation</span>
                {description && <p className="text-xs text-gray-400 mt-2">{description}</p>}
            </div>
        </Card>
    );
};

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

const CustomerValueAnalysis = () => {
    const fmt = useFormatters();
    const { businessId } = useParams();
    const toastRef = useRef(null);
    const { pipelineStatus } = usePipelineProgress();

    const [loading, setLoading] = useState(true);
    const [fetchError, setFetchError] = useState(false);
    const [rawData, setRawData] = useState(null);
    const [dataMode, setDataMode] = useState('unknown');

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

        // discount_customers_summary — static
        const discountSummary = a.discount_customers_summary?.data ?? [];
        const hunterRow    = discountSummary.find((r) => r.is_discount_hunter === true) ?? {};
        const nonHunterRow = discountSummary.find((r) => r.is_discount_hunter === false) ?? {};

        // correlation_discount_vs_clv — static
        const corrData = (a.correlation_discount_vs_clv?.data ?? [])[0] ?? {};

        // high_discount_customers — static
        const highDiscountCustomers = a.high_discount_customers?.data ?? [];

        // discount_customers — static (individual customer discount detail)
        const discountCustomers = a.discount_customers?.data ?? [];

        // customer_overall_health_summary — static (comprehensive health per customer)
        const overallHealthRows = a.customer_overall_health_summary?.data ?? [];

        // customers_cohorts — static
        const customerCohorts = a.customers_cohorts?.data ?? [];

        // customer_profit_per_segment — static
        const custProfitSeg = a.customer_profit_per_segment?.data ?? [];

        // top_customers_by_revenue — static
        const topByRevenue = a.top_customers_by_revenue?.data ?? [];

        // top_customers_by_profit — static
        const topByProfit = a.top_customers_by_profit?.data ?? [];

        // payment_method_summary — static
        const paymentSummary = a.payment_method_summary?.data ?? [];

        // referrer_source_summary — static
        const referrerSummary = a.referrer_source_summary?.data ?? [];

        // seg_device_crosstab — static
        const deviceCrosstab = a.seg_device_crosstab?.data ?? [];

        // KPI aggregates
        const totalHunters    = hunterRow.customer_count ?? 0;
        const totalNonHunters = nonHunterRow.customer_count ?? 0;
        const avgHunterCLV    = hunterRow.avg_clv ?? 0;
        const avgNonHunterCLV = nonHunterRow.avg_clv ?? 0;
        const totalSegRevenue = custProfitSeg.reduce((s, r) => s + (r.total_revenue ?? 0), 0);

        return {
            discountSummary, hunterRow, nonHunterRow, corrData,
            highDiscountCustomers, discountCustomers, overallHealthRows, customerCohorts,
            custProfitSeg, topByRevenue, topByProfit,
            paymentSummary, referrerSummary, deviceCrosstab,
            totalHunters, totalNonHunters, avgHunterCLV, avgNonHunterCLV, totalSegRevenue,
        };
    }, [rawData]);

    // -----------------------------------------------------------------------
    // Chart data
    // -----------------------------------------------------------------------

    // Profit per segment — grouped bar (revenue + net profit)
    const profitSegBarData = useMemo(() => {
        const rows = derived?.custProfitSeg ?? [];
        return {
            labels: rows.map((r) => r.customer_segment_label ?? ''),
            datasets: [
                {
                    label: 'Total Revenue',
                    data: rows.map((r) => r.total_revenue ?? 0),
                    backgroundColor: 'rgba(59,130,246,0.8)',
                },
                {
                    label: 'Total Net Profit',
                    data: rows.map((r) => r.total_net_profit ?? 0),
                    backgroundColor: 'rgba(34,197,94,0.8)',
                },
                {
                    label: 'Total Order Profit',
                    data: rows.map((r) => r.total_order_profit ?? 0),
                    backgroundColor: 'rgba(249,115,22,0.8)',
                },
            ],
        };
    }, [derived]);

    // Avg CLV per segment — doughnut
    const clvSegDoughnutData = useMemo(() => {
        const rows = derived?.custProfitSeg ?? [];
        return {
            labels: rows.map((r) => r.customer_segment_label ?? ''),
            datasets: [{
                data: rows.map((r) => r.avg_clv ?? 0),
                backgroundColor: PALETTE,
            }],
        };
    }, [derived]);

    // Referrer source — avg CLV
    const referrerClvData = useMemo(() => {
        const rows = [...(derived?.referrerSummary ?? [])].sort((a, b) => (b.avg_clv ?? 0) - (a.avg_clv ?? 0));
        return {
            labels: rows.map((r) => r.preferred_referrer_source ?? ''),
            datasets: [
                {
                    label: 'Avg CLV',
                    data: rows.map((r) => r.avg_clv ?? 0),
                    backgroundColor: 'rgba(59,130,246,0.8)',
                },
                {
                    label: 'Avg Revenue / Customer',
                    data: rows.map((r) => r.avg_revenue_per_customer ?? 0),
                    backgroundColor: 'rgba(34,197,94,0.8)',
                },
            ],
        };
    }, [derived]);

    // Payment method — avg CLV
    const paymentClvData = useMemo(() => {
        const rows = derived?.paymentSummary ?? [];
        return {
            labels: rows.map((r) => r.preferred_payment_method ?? ''),
            datasets: [
                {
                    label: 'Avg CLV',
                    data: rows.map((r) => r.avg_clv ?? 0),
                    backgroundColor: 'rgba(139,92,246,0.8)',
                },
                {
                    label: 'Total Revenue',
                    data: rows.map((r) => r.total_revenue ?? 0),
                    backgroundColor: 'rgba(6,182,212,0.8)',
                },
            ],
        };
    }, [derived]);

    // Discount hunter comparison — grouped bar
    const discountHunterData = useMemo(() => {
        const h = derived?.hunterRow ?? {};
        const n = derived?.nonHunterRow ?? {};
        return {
            labels: ['Avg CLV', 'Avg Revenue', 'Avg Discount / Order'],
            datasets: [
                {
                    label: 'Discount Hunters',
                    data: [h.avg_clv ?? 0, h.avg_revenue ?? 0, h.avg_discount_per_order ?? 0],
                    backgroundColor: 'rgba(239,68,68,0.8)',
                },
                {
                    label: 'Non-Discount Customers',
                    data: [n.avg_clv ?? 0, n.avg_revenue ?? 0, n.avg_discount_per_order ?? 0],
                    backgroundColor: 'rgba(34,197,94,0.8)',
                },
            ],
        };
    }, [derived]);

    // Device crosstab — stacked bar (segment × device × revenue)
    const deviceData = useMemo(() => {
        const rows = derived?.deviceCrosstab ?? [];
        const segments = [...new Set(rows.map((r) => r.customer_segment_label ?? ''))];
        const devices = [...new Set(rows.map((r) => r.preferred_device_type ?? ''))];
        return {
            labels: segments,
            datasets: devices.map((d, i) => ({
                label: d,
                data: segments.map((seg) => {
                    const row = rows.find((r) => r.customer_segment_label === seg && r.preferred_device_type === d);
                    return row?.segment_revenue ?? 0;
                }),
                backgroundColor: PALETTE[i % PALETTE.length],
            })),
        };
    }, [derived]);

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    const hasData = useMemo(() => {
        if (!derived) return false;
        return (
            derived.custProfitSeg.length > 0 ||
            derived.topByRevenue.length > 0 ||
            derived.paymentSummary.length > 0
        );
    }, [derived]);

    const rfmSeverity = (seg) => {
        if (!seg) return 'secondary';
        const s = seg.toLowerCase();
        if (s.includes('champion') || s.includes('loyal')) return 'success';
        if (s.includes('risk') || s.includes('lost') || s.includes('cannot')) return 'danger';
        if (s.includes('attention') || s.includes('sleep') || s.includes('hiber')) return 'warning';
        return 'info';
    };

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
                <p className="text-gray-500 text-base">Loading customer value analysis…</p>
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

    const h = derived?.hunterRow ?? {};
    const n = derived?.nonHunterRow ?? {};
    const corr = derived?.corrData ?? {};

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
                * All value analysis metrics are static aggregates computed over all-time records.
                They reflect the full dataset and are not filtered by the date picker.
            </p>

            {/* KPI Cards */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-8">
                {(derived?.totalSegRevenue ?? 0) > 0 && (
                    <KPICard
                        icon="pi-dollar" iconBg="bg-blue-50" iconColor="text-blue-500"
                        value={fmt.currency(derived.totalSegRevenue)}
                        label="Total Segmented Revenue *"
                    />
                )}
                {(derived?.avgHunterCLV ?? 0) > 0 && (
                    <KPICard
                        icon="pi-tag" iconBg="bg-red-50" iconColor="text-red-500"
                        value={fmt.currency(derived.avgHunterCLV)}
                        label="Avg CLV — Discount Hunters *"
                    />
                )}
                {(derived?.avgNonHunterCLV ?? 0) > 0 && (
                    <KPICard
                        icon="pi-star" iconBg="bg-green-50" iconColor="text-green-500"
                        value={fmt.currency(derived.avgNonHunterCLV)}
                        label="Avg CLV — Non-Discount Customers *"
                    />
                )}
                {(derived?.totalHunters ?? 0) > 0 && (
                    <KPICard
                        icon="pi-shopping-bag" iconBg="bg-orange-50" iconColor="text-orange-500"
                        value={fmt.number(derived.totalHunters)}
                        label="Discount Hunters *"
                    />
                )}
                {(derived?.totalNonHunters ?? 0) > 0 && (
                    <KPICard
                        icon="pi-user" iconBg="bg-purple-50" iconColor="text-purple-500"
                        value={fmt.number(derived.totalNonHunters)}
                        label="Non-Discount Customers *"
                    />
                )}
                {(derived?.custProfitSeg?.length ?? 0) > 0 && (
                    <KPICard
                        icon="pi-chart-bar" iconBg="bg-cyan-50" iconColor="text-cyan-500"
                        value={fmt.number(derived.custProfitSeg.length)}
                        label="Customer Segments *"
                    />
                )}
            </div>

            {/* Correlation Cards */}
            {(corr.corr_discount_share_clv != null || corr.corr_avg_discount_clv != null) && (
                <div className="mb-8">
                    <h3 className="text-sm font-semibold text-gray-500 uppercase tracking-widest mb-4">
                        Discount vs CLV Correlations *
                    </h3>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                        {corr.corr_discount_share_clv != null && (
                            <CorrelationCard
                                label="Discount Share vs CLV"
                                value={corr.corr_discount_share_clv}
                                description="Pearson correlation between a customer's discount share of revenue and their lifetime value"
                            />
                        )}
                        {corr.corr_avg_discount_clv != null && (
                            <CorrelationCard
                                label="Avg Discount per Order vs CLV"
                                value={corr.corr_avg_discount_clv}
                                description="Pearson correlation between the average discount per order and customer lifetime value"
                            />
                        )}
                    </div>
                </div>
            )}

            {/* Segment Revenue & Profit Charts */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
                {(derived?.custProfitSeg?.length ?? 0) > 0 && (
                    <div className="col-span-1 lg:col-span-2">
                        <ChartWrapper title="Revenue &amp; Profit by Customer Segment *" showUpdateBadge={false}>
                            <div className="h-[300px]">
                                <Bar
                                    data={profitSegBarData}
                                    options={{
                                        responsive: true, maintainAspectRatio: false,
                                        plugins: { legend: { display: true, position: 'top' } },
                                        scales: { y: { beginAtZero: true, ticks: { callback: (v) => '$' + v.toLocaleString() } } },
                                    }}
                                />
                            </div>
                        </ChartWrapper>
                    </div>
                )}

                {(derived?.custProfitSeg?.length ?? 0) > 0 && (
                    <ChartWrapper title="Avg CLV Distribution by Segment *" showUpdateBadge={false}>
                        <div className="h-[300px]">
                            <Doughnut
                                data={clvSegDoughnutData}
                                options={{
                                    responsive: true, maintainAspectRatio: false,
                                    plugins: { legend: { display: true, position: 'right' } },
                                }}
                            />
                        </div>
                    </ChartWrapper>
                )}

                {/* Discount Hunter Comparison */}
                {(h.avg_clv ?? 0) > 0 && (
                    <ChartWrapper title="Discount Hunters vs Non-Discount Customers *" showUpdateBadge={false}>
                        <div className="h-[300px]">
                            <Bar
                                data={discountHunterData}
                                options={{
                                    responsive: true, maintainAspectRatio: false,
                                    plugins: { legend: { display: true, position: 'top' } },
                                    scales: { y: { beginAtZero: true, ticks: { callback: (v) => '$' + v.toLocaleString() } } },
                                }}
                            />
                        </div>
                    </ChartWrapper>
                )}
            </div>

            {/* Referrer & Payment CLV Charts */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
                {(derived?.referrerSummary?.length ?? 0) > 0 && (
                    <ChartWrapper title="Avg CLV &amp; Revenue by Referrer Source *" showUpdateBadge={false}>
                        <div className="h-[300px]">
                            <Bar
                                data={referrerClvData}
                                options={{
                                    responsive: true, maintainAspectRatio: false,
                                    plugins: { legend: { display: true, position: 'top' } },
                                    scales: { y: { beginAtZero: true, ticks: { callback: (v) => '$' + v.toLocaleString() } } },
                                }}
                            />
                        </div>
                    </ChartWrapper>
                )}

                {(derived?.paymentSummary?.length ?? 0) > 0 && (
                    <ChartWrapper title="Avg CLV &amp; Revenue by Payment Method *" showUpdateBadge={false}>
                        <div className="h-[300px]">
                            <Bar
                                data={paymentClvData}
                                options={{
                                    responsive: true, maintainAspectRatio: false,
                                    plugins: { legend: { display: true, position: 'top' } },
                                    scales: { y: { beginAtZero: true, ticks: { callback: (v) => '$' + v.toLocaleString() } } },
                                }}
                            />
                        </div>
                    </ChartWrapper>
                )}

                {(derived?.deviceCrosstab?.length ?? 0) > 0 && (
                    <div className="col-span-1 lg:col-span-2">
                        <ChartWrapper title="Segment Revenue by Device Type *" showUpdateBadge={false}>
                            <div className="h-[300px]">
                                <Bar
                                    data={deviceData}
                                    options={{
                                        responsive: true, maintainAspectRatio: false,
                                        plugins: { legend: { display: true, position: 'top' } },
                                        scales: {
                                            x: { stacked: true },
                                            y: { beginAtZero: true, stacked: true, ticks: { callback: (v) => '$' + v.toLocaleString() } },
                                        },
                                    }}
                                />
                            </div>
                        </ChartWrapper>
                    </div>
                )}
            </div>

            {/* Discount Summary Metrics */}
            {(derived?.discountSummary?.length ?? 0) > 0 && (
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-8">
                    <MetricsCard
                        title="Discount Hunters Summary *"
                        rows={[
                            { label: 'Customer Count',        value: fmt.number(h.customer_count),         show: (h.customer_count ?? 0) > 0 },
                            { label: 'Avg Discount Share',    value: fmt.pct(h.avg_discount_share),         show: (h.avg_discount_share ?? 0) > 0 },
                            { label: 'Avg Discount / Order',  value: fmt.currency(h.avg_discount_per_order), show: (h.avg_discount_per_order ?? 0) > 0 },
                            { label: 'Avg CLV',               value: fmt.currency(h.avg_clv),               show: (h.avg_clv ?? 0) > 0 },
                            { label: 'Avg Revenue',           value: fmt.currency(h.avg_revenue),           show: (h.avg_revenue ?? 0) > 0 },
                        ]}
                    />
                    <MetricsCard
                        title="Non-Discount Customers Summary *"
                        rows={[
                            { label: 'Customer Count',        value: fmt.number(n.customer_count),         show: (n.customer_count ?? 0) > 0 },
                            { label: 'Avg Discount Share',    value: fmt.pct(n.avg_discount_share),         show: true },
                            { label: 'Avg Discount / Order',  value: fmt.currency(n.avg_discount_per_order), show: true },
                            { label: 'Avg CLV',               value: fmt.currency(n.avg_clv),               show: (n.avg_clv ?? 0) > 0 },
                            { label: 'Avg Revenue',           value: fmt.currency(n.avg_revenue),           show: (n.avg_revenue ?? 0) > 0 },
                        ]}
                    />
                </div>
            )}

            {/* Referrer Source Summary Table */}
            {(derived?.referrerSummary?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">
                            Referrer Source Summary *
                        </h3>
                        <DataTable
                            value={[...derived.referrerSummary].sort((a, b) => (b.avg_clv ?? 0) - (a.avg_clv ?? 0))}
                            paginator rows={10} stripedRows size="small"
                        >
                            <Column field="preferred_referrer_source" header="Referrer Source" sortable />
                            <Column field="customer_count" header="Customers" sortable body={(r) => fmt.number(r.customer_count)} />
                            <Column field="avg_clv" header="Avg CLV" sortable body={(r) => fmt.currency(r.avg_clv)} />
                            <Column field="total_revenue" header="Total Revenue" sortable body={(r) => fmt.currency(r.total_revenue)} />
                            <Column field="avg_revenue_per_customer" header="Avg Rev/Customer" sortable body={(r) => fmt.currency(r.avg_revenue_per_customer)} />
                            <Column field="avg_discount_share" header="Avg Discount Share" sortable body={(r) => fmt.pct(r.avg_discount_share)} />
                        </DataTable>
                    </div>
                </Card>
            )}

            {/* Payment Method Summary Table */}
            {(derived?.paymentSummary?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">
                            Payment Method Summary *
                        </h3>
                        <DataTable
                            value={[...derived.paymentSummary].sort((a, b) => (b.avg_clv ?? 0) - (a.avg_clv ?? 0))}
                            paginator rows={10} stripedRows size="small"
                        >
                            <Column field="preferred_payment_method" header="Payment Method" sortable />
                            <Column field="customer_count" header="Customers" sortable body={(r) => fmt.number(r.customer_count)} />
                            <Column field="avg_clv" header="Avg CLV" sortable body={(r) => fmt.currency(r.avg_clv)} />
                            <Column field="avg_revenue_per_customer" header="Avg Rev/Customer" sortable body={(r) => fmt.currency(r.avg_revenue_per_customer)} />
                            <Column field="total_revenue" header="Total Revenue" sortable body={(r) => fmt.currency(r.total_revenue)} />
                        </DataTable>
                    </div>
                </Card>
            )}

            {/* Top Customers by Revenue Table */}
            {(derived?.topByRevenue?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">
                            Top Customers by Revenue *
                        </h3>
                        <DataTable
                            value={derived.topByRevenue} paginator rows={10} stripedRows size="small"
                        >
                            <Column field="customer_id" header="Customer ID" sortable />
                            <Column field="total_revenue" header="Revenue" sortable body={(r) => fmt.currency(r.total_revenue)} />
                            <Column field="customer_lifetime_value" header="CLV" sortable body={(r) => fmt.currency(r.customer_lifetime_value)} />
                            <Column field="customer_segment" header="Segment" sortable />
                            <Column field="rfm_segment" header="RFM" sortable
                                body={(r) => <Tag value={r.rfm_segment} severity={rfmSeverity(r.rfm_segment)} />}
                            />
                        </DataTable>
                    </div>
                </Card>
            )}

            {/* High Discount Customers Table */}
            {(derived?.highDiscountCustomers?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">
                            High Discount Customers *
                        </h3>
                        <DataTable
                            value={[...derived.highDiscountCustomers].sort((a, b) => (b.discount_to_revenue_ratio ?? 0) - (a.discount_to_revenue_ratio ?? 0))}
                            paginator rows={10} stripedRows size="small"
                        >
                            <Column field="customer_id" header="Customer ID" sortable />
                            <Column field="total_revenue" header="Revenue" sortable body={(r) => fmt.currency(r.total_revenue)} />
                            <Column field="total_discount_received" header="Total Discount" sortable body={(r) => fmt.currency(r.total_discount_received)} />
                            <Column field="discount_to_revenue_ratio" header="Discount/Rev Ratio" sortable body={(r) => fmt.pct(r.discount_to_revenue_ratio)} />
                            <Column field="avg_discount_per_order" header="Avg Disc/Order" sortable body={(r) => fmt.currency(r.avg_discount_per_order)} />
                            <Column field="customer_lifetime_value" header="CLV" sortable body={(r) => fmt.currency(r.customer_lifetime_value)} />
                        </DataTable>
                    </div>
                </Card>
            )}

            {/* Customer Profit per Segment Table */}
            {(derived?.custProfitSeg?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">
                            Profit by Customer Segment *
                        </h3>
                        <DataTable
                            value={[...derived.custProfitSeg].sort((a, b) => (b.total_revenue ?? 0) - (a.total_revenue ?? 0))}
                            stripedRows size="small"
                        >
                            <Column field="customer_segment_label" header="Segment" sortable />
                            <Column field="customer_count" header="Customers" sortable body={(r) => fmt.number(r.customer_count)} />
                            <Column field="total_revenue" header="Revenue" sortable body={(r) => fmt.currency(r.total_revenue)} />
                            <Column field="total_order_profit" header="Order Profit" sortable body={(r) => fmt.currency(r.total_order_profit)} />
                            <Column field="total_net_profit" header="Net Profit" sortable body={(r) => fmt.currency(r.total_net_profit)} />
                            <Column field="avg_clv" header="Avg CLV" sortable body={(r) => fmt.currency(r.avg_clv)} />
                            <Column field="avg_profit_per_customer" header="Avg Profit/Customer" sortable body={(r) => fmt.currency(r.avg_profit_per_customer)} />
                        </DataTable>
                    </div>
                </Card>
            )}

            {/* Discount Customers Detail Table */}
            {(derived?.discountCustomers?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">
                            Discount Customers Detail *
                        </h3>
                        <DataTable
                            value={[...derived.discountCustomers].sort((a, b) => (b.total_discount_received ?? 0) - (a.total_discount_received ?? 0))}
                            paginator rows={10} stripedRows size="small" className="text-sm"
                        >
                            <Column field="customer_id" header="Customer ID" sortable />
                            <Column field="total_revenue" header="Revenue" sortable body={(r) => fmt.currency(r.total_revenue)} />
                            <Column field="total_discount_received" header="Total Discount" sortable body={(r) => fmt.currency(r.total_discount_received)} />
                            <Column field="discount_share_of_revenue" header="Discount Share" sortable body={(r) => fmt.pct(r.discount_share_of_revenue)} />
                            <Column field="avg_discount_per_order" header="Avg Disc/Order" sortable body={(r) => fmt.currency(r.avg_discount_per_order)} />
                            <Column field="customer_lifetime_value" header="CLV" sortable body={(r) => fmt.currency(r.customer_lifetime_value)} />
                            <Column field="total_orders" header="Orders" sortable body={(r) => fmt.number(r.total_orders)} />
                            <Column field="is_discount_hunter" header="Discount Hunter" sortable body={(r) => r.is_discount_hunter ? 'Yes' : 'No'} />
                        </DataTable>
                    </div>
                </Card>
            )}

            {/* Customer Overall Health Summary */}
            {(derived?.overallHealthRows?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">
                            Customer Overall Health Summary *
                        </h3>
                        <DataTable
                            value={[...derived.overallHealthRows].sort((a, b) => (b.customer_lifetime_value ?? 0) - (a.customer_lifetime_value ?? 0))}
                            paginator rows={10} stripedRows size="small" className="text-sm"
                            scrollable scrollHeight="400px"
                        >
                            <Column field="customer_id" header="Customer ID" sortable />
                            <Column field="customer_segment_label" header="Segment" sortable />
                            <Column field="rfm_segment" header="RFM" sortable />
                            <Column field="churn_risk" header="Churn Risk" sortable />
                            <Column field="customer_lifetime_value" header="CLV" sortable body={(r) => fmt.currency(r.customer_lifetime_value)} />
                            <Column field="total_revenue" header="Revenue" sortable body={(r) => fmt.currency(r.total_revenue)} />
                            <Column field="total_orders" header="Orders" sortable body={(r) => fmt.number(r.total_orders)} />
                            <Column field="session_conversion_rate" header="Conv. Rate" sortable body={(r) => fmt.pct(r.session_conversion_rate)} />
                            <Column field="cart_abandonment_rate" header="Cart Abandon" sortable body={(r) => fmt.pct(r.cart_abandonment_rate)} />
                            <Column field="customer_activity_score" header="Activity Score" sortable body={(r) => (r.customer_activity_score ?? 0).toFixed(2)} />
                        </DataTable>
                    </div>
                </Card>
            )}

            {/* Customer Cohorts Table */}
            {(derived?.customerCohorts?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">
                            Customer Cohorts *
                        </h3>
                        <DataTable
                            value={[...derived.customerCohorts].sort((a, b) => (a.signup_cohort_month ?? '').localeCompare(b.signup_cohort_month ?? ''))}
                            paginator rows={10} stripedRows size="small" className="text-sm"
                        >
                            <Column field="customer_id" header="Customer ID" sortable />
                            <Column field="signup_cohort_month" header="Signup Cohort" sortable />
                            <Column field="first_order_month" header="First Order Month" sortable />
                            <Column field="signup_date" header="Signup Date" sortable />
                            <Column field="first_order_date" header="First Order Date" sortable />
                        </DataTable>
                    </div>
                </Card>
            )}
        </div>
    );
};

export default CustomerValueAnalysis;
