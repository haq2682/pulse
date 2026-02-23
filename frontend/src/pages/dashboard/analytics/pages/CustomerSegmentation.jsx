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

const RFM_COLORS = {
    'Champions':           'rgba(34,197,94,0.85)',
    'Loyal Customers':     'rgba(59,130,246,0.85)',
    'Potential Loyalist':  'rgba(6,182,212,0.85)',
    'Recent Customers':    'rgba(249,115,22,0.85)',
    'Promising':           'rgba(234,179,8,0.85)',
    'Need Attention':      'rgba(139,92,246,0.85)',
    'About To Sleep':      'rgba(236,72,153,0.85)',
    'At Risk':             'rgba(239,68,68,0.85)',
    'Cannot Lose Them':    'rgba(239,68,68,0.9)',
    'Hibernating':         'rgba(107,114,128,0.85)',
    'Lost':                'rgba(156,163,175,0.85)',
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
// Default chart option factories
// ---------------------------------------------------------------------------

const barOpts = (title, horizontal = false, stacked = false) => ({
    responsive: true,
    maintainAspectRatio: false,
    indexAxis: horizontal ? 'y' : 'x',
    plugins: {
        legend: { display: stacked, position: 'top' },
        title: { display: false },
    },
    scales: {
        x: { stacked },
        y: { beginAtZero: true, stacked },
    },
});

const doughnutOpts = () => ({
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
        legend: { display: true, position: 'right' },
    },
});

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

const CustomerSegmentation = () => {
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
    // Derived data — all static aggregates (no date field in these analytics)
    // -----------------------------------------------------------------------

    const derived = useMemo(() => {
        if (!rawData) return null;
        const a = rawData.analytics ?? {};

        const rfmSummary       = a.rfm_segment_summary?.data ?? [];
        const custProfitSeg    = a.customer_profit_per_segment?.data ?? [];
        const genderCategory   = a.gender_category_preference?.data ?? [];
        const genderProduct    = a.gender_product_preference?.data ?? [];
        const sessCvtDist      = a.session_conversion_distribution?.data ?? [];
        const cartAbandonDist  = a.cart_abandonment_distribution?.data ?? [];
        const referrerSummary  = a.referrer_source_summary?.data ?? [];
        const deviceCrosstab   = a.seg_device_crosstab?.data ?? [];
        const paymentSummary   = a.payment_method_summary?.data ?? [];
        const rfmChurnCrosstab = a.rfm_churn_crosstab?.data ?? [];
        const segRefCrosstab   = a.seg_referrer_crosstab?.data ?? [];
        const topByRevenue     = a.top_customers_by_revenue?.data ?? [];
        const topByProfit      = a.top_customers_by_profit?.data ?? [];

        // KPI: best RFM segment (by total_revenue)
        const bestRfm = [...rfmSummary].sort((a, b) => (b.total_revenue ?? 0) - (a.total_revenue ?? 0))[0];
        const totalSegmentCustomers = rfmSummary.reduce((s, r) => s + (r.num_customers ?? 0), 0);

        return {
            rfmSummary, custProfitSeg, genderCategory, genderProduct, sessCvtDist,
            cartAbandonDist, referrerSummary, deviceCrosstab, paymentSummary,
            rfmChurnCrosstab, segRefCrosstab, topByRevenue, topByProfit,
            bestRfm, totalSegmentCustomers,
        };
    }, [rawData]);

    // -----------------------------------------------------------------------
    // Chart data
    // -----------------------------------------------------------------------

    // RFM Segment — customer count
    const rfmBarData = useMemo(() => {
        const rows = derived?.rfmSummary ?? [];
        return {
            labels: rows.map((r) => r.rfm_segment ?? ''),
            datasets: [{
                label: 'Customers',
                data: rows.map((r) => r.num_customers ?? 0),
                backgroundColor: rows.map((r) => RFM_COLORS[r.rfm_segment] ?? 'rgba(107,114,128,0.8)'),
            }],
        };
    }, [derived]);

    // RFM Segment — revenue doughnut
    const rfmRevDoughnutData = useMemo(() => {
        const rows = derived?.rfmSummary ?? [];
        return {
            labels: rows.map((r) => r.rfm_segment ?? ''),
            datasets: [{
                data: rows.map((r) => r.total_revenue ?? 0),
                backgroundColor: rows.map((r) => RFM_COLORS[r.rfm_segment] ?? 'rgba(107,114,128,0.8)'),
            }],
        };
    }, [derived]);

    // Customer Profit per Segment — bar
    const profitSegData = useMemo(() => {
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
            ],
        };
    }, [derived]);

    // Gender × Category preference
    const genderCatData = useMemo(() => {
        const rows = derived?.genderCategory ?? [];
        const categories = [...new Set(rows.map((r) => r.category ?? ''))];
        const genders = [...new Set(rows.map((r) => r.gender ?? ''))];
        const colorMap = { M: 'rgba(59,130,246,0.8)', F: 'rgba(236,72,153,0.8)', Other: 'rgba(107,114,128,0.8)' };
        return {
            labels: categories,
            datasets: genders.map((g, i) => ({
                label: g,
                data: categories.map((cat) => {
                    const row = rows.find((r) => r.gender === g && r.category === cat);
                    return row?.orders_count ?? 0;
                }),
                backgroundColor: colorMap[g] ?? PALETTE[i],
            })),
        };
    }, [derived]);

    // Session conversion distribution
    const sessConvData = useMemo(() => {
        const rows = derived?.sessCvtDist ?? [];
        return {
            labels: rows.map((r) => `${r.session_conversion_percentage ?? 0}%`),
            datasets: [{
                label: 'Customers',
                data: rows.map((r) => r.customer_count ?? 0),
                backgroundColor: 'rgba(59,130,246,0.7)',
            }],
        };
    }, [derived]);

    // Cart abandonment distribution
    const cartAbandonData = useMemo(() => {
        const rows = derived?.cartAbandonDist ?? [];
        return {
            labels: rows.map((r) => `${r.cart_abandonment_percentage ?? 0}%`),
            datasets: [{
                label: 'Customers',
                data: rows.map((r) => r.customer_count ?? 0),
                backgroundColor: 'rgba(239,68,68,0.7)',
            }],
        };
    }, [derived]);

    // Referrer source — avg CLV
    const referrerData = useMemo(() => {
        const rows = derived?.referrerSummary ?? [];
        return {
            labels: rows.map((r) => r.preferred_referrer_source ?? ''),
            datasets: [{
                label: 'Avg CLV',
                data: rows.map((r) => r.avg_clv ?? 0),
                backgroundColor: PALETTE,
            }],
        };
    }, [derived]);

    // Payment method — avg CLV
    const paymentData = useMemo(() => {
        const rows = derived?.paymentSummary ?? [];
        return {
            labels: rows.map((r) => r.preferred_payment_method ?? ''),
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

    // RFM × Churn crosstab — stacked bar
    const rfmChurnData = useMemo(() => {
        const rows = derived?.rfmChurnCrosstab ?? [];
        const rfmSegments = [...new Set(rows.map((r) => r.rfm_segment ?? ''))];
        const churnLevels = [...new Set(rows.map((r) => r.churn_risk ?? ''))];
        const churnColors = { 'High': 'rgba(239,68,68,0.8)', 'Medium': 'rgba(234,179,8,0.8)', 'Low': 'rgba(34,197,94,0.8)' };
        return {
            labels: rfmSegments,
            datasets: churnLevels.map((cl) => ({
                label: cl,
                data: rfmSegments.map((seg) => {
                    const row = rows.find((r) => r.rfm_segment === seg && r.churn_risk === cl);
                    return row?.customer_count ?? 0;
                }),
                backgroundColor: churnColors[cl] ?? 'rgba(107,114,128,0.8)',
            })),
        };
    }, [derived]);

    // -----------------------------------------------------------------------
    // Visibility
    // -----------------------------------------------------------------------

    const hasData = useMemo(() => {
        if (!derived) return false;
        return (
            derived.rfmSummary.length > 0 ||
            derived.custProfitSeg.length > 0 ||
            derived.topByRevenue.length > 0
        );
    }, [derived]);

    // -----------------------------------------------------------------------
    // Tag severity helper for rfm_segment
    // -----------------------------------------------------------------------

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
                <p className="text-gray-500 text-base">Loading customer segmentation…</p>
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

    return (
        <div className="p-6 bg-gray-50 min-h-[calc(100vh-120px)]">
            <Toast ref={toastRef} />

            <DateFilterBar
                quickFilter={quickFilter} dateRange={dateRange} isFiltered={isFiltered}
                onQuickFilter={applyQuickFilter} onDateChange={setDateRange}
                onReset={resetFilters} dataMode={dataMode}
                hidden={loading && pipelineStatus === 'loading'}
            />

            {/* All data is static aggregates */}
            <p className="mb-6 text-xs text-gray-400 italic">
                * All segmentation data are static aggregates computed over all-time records.
                They reflect the full dataset and are not filtered by the date picker.
            </p>

            {/* KPI Cards */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-8">
                {(derived?.totalSegmentCustomers ?? 0) > 0 && (
                    <KPICard
                        icon="pi-users" iconBg="bg-blue-50" iconColor="text-blue-500"
                        value={fmt.number(derived.totalSegmentCustomers)}
                        label="Segmented Customers *"
                    />
                )}
                {derived?.bestRfm?.rfm_segment && (
                    <KPICard
                        icon="pi-star" iconBg="bg-yellow-50" iconColor="text-yellow-500"
                        value={derived.bestRfm.rfm_segment}
                        label="Top Revenue Segment *"
                    />
                )}
                {derived?.bestRfm?.avg_clv > 0 && (
                    <KPICard
                        icon="pi-chart-line" iconBg="bg-green-50" iconColor="text-green-500"
                        value={fmt.currency(derived.bestRfm.avg_clv)}
                        label="Avg CLV — Top Segment *"
                    />
                )}
                {(derived?.rfmSummary?.length ?? 0) > 0 && (
                    <KPICard
                        icon="pi-th-large" iconBg="bg-purple-50" iconColor="text-purple-500"
                        value={fmt.number(derived.rfmSummary.length)}
                        label="RFM Segments *"
                    />
                )}
                {(derived?.custProfitSeg?.length ?? 0) > 0 && (
                    <KPICard
                        icon="pi-dollar" iconBg="bg-emerald-50" iconColor="text-emerald-500"
                        value={fmt.currency(derived.custProfitSeg.reduce((s, r) => s + (r.total_revenue ?? 0), 0))}
                        label="Total Segmented Revenue *"
                    />
                )}
            </div>

            {/* RFM Charts */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
                {(derived?.rfmSummary?.length ?? 0) > 0 && (
                    <ChartWrapper title="RFM Segment — Customer Count *" showUpdateBadge={false}>
                        <div className="h-[300px]">
                            <Bar
                                data={rfmBarData}
                                options={{
                                    ...barOpts('Customers per RFM Segment'),
                                    scales: { y: { beginAtZero: true } },
                                }}
                            />
                        </div>
                    </ChartWrapper>
                )}

                {(derived?.rfmSummary?.length ?? 0) > 0 && (
                    <ChartWrapper title="RFM Segment — Revenue Share *" showUpdateBadge={false}>
                        <div className="h-[300px]">
                            <Doughnut data={rfmRevDoughnutData} options={doughnutOpts()} />
                        </div>
                    </ChartWrapper>
                )}

                {(derived?.rfmChurnCrosstab?.length ?? 0) > 0 && (
                    <div className="col-span-1 lg:col-span-2">
                        <ChartWrapper title="RFM Segment × Churn Risk *" showUpdateBadge={false}>
                            <div className="h-[300px]">
                                <Bar
                                    data={rfmChurnData}
                                    options={{
                                        ...barOpts('', false, true),
                                        plugins: { legend: { display: true, position: 'top' } },
                                        scales: { x: { stacked: true }, y: { beginAtZero: true, stacked: true } },
                                    }}
                                />
                            </div>
                        </ChartWrapper>
                    </div>
                )}
            </div>

            {/* Profit per Segment */}
            {(derived?.custProfitSeg?.length ?? 0) > 0 && (
                <div className="mb-8">
                    <ChartWrapper title="Revenue &amp; Profit by Customer Segment *" showUpdateBadge={false}>
                        <div className="h-[300px]">
                            <Bar
                                data={profitSegData}
                                options={{
                                    responsive: true,
                                    maintainAspectRatio: false,
                                    plugins: { legend: { display: true, position: 'top' } },
                                    scales: {
                                        y: { beginAtZero: true, ticks: { callback: (v) => '$' + v.toLocaleString() } },
                                    },
                                }}
                            />
                        </div>
                    </ChartWrapper>
                </div>
            )}

            {/* Behavioural Distribution Charts */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
                {(derived?.sessCvtDist?.length ?? 0) > 0 && (
                    <ChartWrapper title="Session Conversion Distribution *" showUpdateBadge={false}>
                        <div className="h-[280px]">
                            <Bar data={sessConvData} options={barOpts('Customers by Session Conversion %')} />
                        </div>
                    </ChartWrapper>
                )}

                {(derived?.cartAbandonDist?.length ?? 0) > 0 && (
                    <ChartWrapper title="Cart Abandonment Distribution *" showUpdateBadge={false}>
                        <div className="h-[280px]">
                            <Bar data={cartAbandonData} options={barOpts('Customers by Cart Abandonment %')} />
                        </div>
                    </ChartWrapper>
                )}

                {(derived?.referrerSummary?.length ?? 0) > 0 && (
                    <ChartWrapper title="Avg CLV by Referrer Source *" showUpdateBadge={false}>
                        <div className="h-[280px]">
                            <Bar
                                data={referrerData}
                                options={{
                                    ...barOpts('Avg CLV by Referrer', false),
                                    scales: { y: { beginAtZero: true, ticks: { callback: (v) => '$' + v.toLocaleString() } } },
                                }}
                            />
                        </div>
                    </ChartWrapper>
                )}

                {(derived?.paymentSummary?.length ?? 0) > 0 && (
                    <ChartWrapper title="CLV &amp; Revenue by Payment Method *" showUpdateBadge={false}>
                        <div className="h-[280px]">
                            <Bar
                                data={paymentData}
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

            {/* Gender × Category preference */}
            {(derived?.genderCategory?.length ?? 0) > 0 && (
                <div className="mb-8">
                    <ChartWrapper title="Gender × Category Preference (Orders) *" showUpdateBadge={false}>
                        <div className="h-[320px]">
                            <Bar
                                data={genderCatData}
                                options={{
                                    responsive: true, maintainAspectRatio: false,
                                    plugins: { legend: { display: true, position: 'top' } },
                                    scales: { y: { beginAtZero: true } },
                                }}
                            />
                        </div>
                    </ChartWrapper>
                </div>
            )}

            {/* Gender × Product Preference Table */}
            {(derived?.genderProduct?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">
                            Gender × Product Preference *
                        </h3>
                        <DataTable
                            value={[...derived.genderProduct].sort((a, b) => (b.total_units ?? 0) - (a.total_units ?? 0))}
                            paginator rows={10} stripedRows size="small" className="text-sm"
                        >
                            <Column field="gender" header="Gender" sortable />
                            <Column field="product_name" header="Product" sortable />
                            <Column field="category" header="Category" sortable />
                            <Column field="total_units" header="Units Sold" sortable body={(r) => fmt.number(r.total_units)} />
                            <Column field="orders_count" header="Orders" sortable body={(r) => fmt.number(r.orders_count)} />
                        </DataTable>
                    </div>
                </Card>
            )}

            {/* RFM Segment Summary Table */}
            {(derived?.rfmSummary?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">
                            RFM Segment Summary *
                        </h3>
                        <DataTable
                            value={[...derived.rfmSummary].sort((a, b) => (b.total_revenue ?? 0) - (a.total_revenue ?? 0))}
                            paginator rows={10} stripedRows size="small"
                        >
                            <Column field="rfm_segment" header="Segment" sortable
                                body={(r) => <Tag value={r.rfm_segment} severity={rfmSeverity(r.rfm_segment)} />}
                            />
                            <Column field="num_customers" header="Customers" sortable body={(r) => fmt.number(r.num_customers)} />
                            <Column field="total_revenue" header="Revenue" sortable body={(r) => fmt.currency(r.total_revenue)} />
                            <Column field="avg_clv" header="Avg CLV" sortable body={(r) => fmt.currency(r.avg_clv)} />
                            <Column field="avg_recency_days" header="Avg Recency (days)" sortable body={(r) => (r.avg_recency_days ?? 0).toFixed(0)} />
                            <Column field="avg_total_orders" header="Avg Orders" sortable body={(r) => (r.avg_total_orders ?? 0).toFixed(1)} />
                            <Column field="avg_aov" header="Avg AOV" sortable body={(r) => fmt.currency(r.avg_aov)} />
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

            {/* Top Customers by Profit Table */}
            {(derived?.topByProfit?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">
                            Top Customers by Profit *
                        </h3>
                        <DataTable
                            value={derived.topByProfit} paginator rows={10} stripedRows size="small"
                        >
                            <Column field="customer_id" header="Customer ID" sortable />
                            <Column field="total_order_profit" header="Order Profit" sortable body={(r) => fmt.currency(r.total_order_profit)} />
                            <Column field="total_net_profit" header="Net Profit" sortable body={(r) => fmt.currency(r.total_net_profit)} />
                            <Column field="total_revenue" header="Revenue" sortable body={(r) => fmt.currency(r.total_revenue)} />
                            <Column field="orders_count" header="Orders" sortable body={(r) => fmt.number(r.orders_count)} />
                            <Column field="customer_segment" header="Segment" sortable />
                            <Column field="rfm_segment" header="RFM" sortable
                                body={(r) => <Tag value={r.rfm_segment} severity={rfmSeverity(r.rfm_segment)} />}
                            />
                        </DataTable>
                    </div>
                </Card>
            )}

            {/* Referrer × Segment Crosstab Table */}
            {(derived?.segRefCrosstab?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">
                            Referrer × Customer Segment Crosstab *
                        </h3>
                        <DataTable
                            value={derived.segRefCrosstab} paginator rows={10} stripedRows size="small"
                        >
                            <Column field="customer_segment_label" header="Segment" sortable />
                            <Column field="preferred_referrer_source" header="Referrer" sortable />
                            <Column field="customer_count" header="Customers" sortable body={(r) => fmt.number(r.customer_count)} />
                            <Column field="segment_revenue" header="Revenue" sortable body={(r) => fmt.currency(r.segment_revenue)} />
                            <Column field="avg_revenue_per_customer" header="Avg Rev/Customer" sortable body={(r) => fmt.currency(r.avg_revenue_per_customer)} />
                        </DataTable>
                    </div>
                </Card>
            )}
        </div>
    );
};

export default CustomerSegmentation;
