import React, { useState, useEffect, useRef, useMemo, useCallback } from 'react';
import { useParams } from 'react-router-dom';
import { Card } from 'primereact/card';
import { ProgressSpinner } from 'primereact/progressspinner';
import { Toast } from 'primereact/toast';
import { Calendar } from 'primereact/calendar';
import {
    Chart as ChartJS,
    CategoryScale,
    LinearScale,
    PointElement,
    LineElement,
    BarElement,
    ArcElement,
    Title,
    Tooltip,
    Legend
} from 'chart.js';
import { Line, Doughnut } from 'react-chartjs-2';
import { useAnalyticsWebSocket } from '../../../../hooks/useAnalyticsWebSocket';
import ChartWrapper from '../components/ChartWrapper';
import { usePipelineProgress } from '@/context/PipelineProgressContext';
import SecondaryButton from '@/components/global/Button/SecondaryButton';

// Register Chart.js components
ChartJS.register(
    CategoryScale,
    LinearScale,
    PointElement,
    LineElement,
    BarElement,
    ArcElement,
    Title,
    Tooltip,
    Legend
);

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const QUICK_FILTERS = [
    { label: '1d', days: 1 },
    { label: '3d', days: 3 },
    { label: '7d', days: 7 },
    { label: '30d', days: 30 },
    { label: '90d', days: 90 },
];

const ANALYTICS_CATEGORIES = [
    'kpis',
    'customer_analytics',
    'product_analytics',
    'operations_analytics',
    'marketing_analytics',
];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const fmt = {
    currency: (v) =>
        new Intl.NumberFormat('en-US', {
            style: 'currency',
            currency: 'USD',
            minimumFractionDigits: 0,
            maximumFractionDigits: 0,
        }).format(v ?? 0),
    number: (v) => new Intl.NumberFormat('en-US').format(v ?? 0),
    pct: (v) => `${(v ?? 0).toFixed(1)}%`,
    hours: (v) => `${(v ?? 0).toFixed(1)} hrs`,
    days: (v) => `${(v ?? 0).toFixed(1)} days`,
};

/**
 * Convert a Date to a yyyy-mm-dd string for API query params.
 */
const toISODate = (d) => {
    if (!d) return null;
    const pad = (n) => String(n).padStart(2, '0');
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
};

/**
 * Client-side filter: keeps array items whose `dateField` falls within [from, to].
 * Works for both array data and passes non-arrays through unchanged.
 *
 * Used as a fallback for BATCH mode or when the API does not support date params.
 */
const filterByDateRange = (data, dateField, from, to) => {
    if (!from && !to) return data;
    if (!Array.isArray(data)) return data;

    const start = from ? new Date(from).setHours(0, 0, 0, 0) : -Infinity;
    const end = to ? new Date(to).setHours(23, 59, 59, 999) : Infinity;

    return data.filter((item) => {
        if (!item[dateField]) return true;
        const t = new Date(item[dateField]).getTime();
        return t >= start && t <= end;
    });
};

/**
 * Aggregate an array of daily rows into a single summary object by summing /
 * averaging relevant numeric fields.  Used for client-side KPI recalculation
 * after date filtering in BATCH mode.
 */
const aggregateDailyRows = (rows) => {
    if (!rows || rows.length === 0) {
        return {
            totalRevenue: 0,
            totalOrders: 0,
            avgOrderValue: 0,
            profitMargin: 0,
            grossProfit: 0,
            netProfit: 0,
        };
    }

    const sum = (field) => rows.reduce((acc, r) => acc + (r[field] ?? 0), 0);
    const avg = (field) => sum(field) / rows.length;

    const totalRevenue = sum('total_revenue');
    const totalOrders = sum('total_orders');

    return {
        totalRevenue,
        totalOrders,
        avgOrderValue: totalOrders > 0 ? totalRevenue / totalOrders : 0,
        profitMargin: avg('margin_pct'),
        grossProfit: sum('gross_profit'),
        netProfit: sum('net_profit'),
    };
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
                <h3 className="text-xl font-semibold text-gray-900 mb-6 pb-3 border-b-2 border-gray-200">
                    {title}
                </h3>
                <div className="flex flex-col gap-4">
                    {visible.map((r) => (
                        <MetricRow key={r.label} label={r.label} value={r.value} />
                    ))}
                </div>
            </div>
        </Card>
    );
};

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

const ExecutiveOverview = () => {
    const { businessId } = useParams();
    const toastRef = useRef(null);
    const { pipelineStatus } = usePipelineProgress();

    // ---- raw (unfiltered) data from API ----
    const [loading, setLoading] = useState(true);
    const [rawCategories, setRawCategories] = useState(null); // full API response
    const [dataMode, setDataMode] = useState('unknown'); // 'batch' | 'db' | 'api' | 'unknown'

    // ---- date filter state ----
    const [dateRange, setDateRange] = useState({ from: null, to: null });
    const [quickFilter, setQuickFilter] = useState(null);

    // ---- websocket for real-time updates ----
    const { lastUpdate } = useAnalyticsWebSocket(businessId);

    // -----------------------------------------------------------------------
    // Fetch
    // -----------------------------------------------------------------------

    /**
     * Build the fetch URL.
     *
     * For DB / API modes we pass date params so the server filters at source.
     * For BATCH mode the server ignores date params, so we filter client-side.
     *
     * We always send the params — the server will use them if it can.
     */
    const buildUrl = useCallback(
        (from, to) => {
            const base = import.meta.env.VITE_API_URL || 'http://localhost:8000';
            const params = new URLSearchParams({
                categories: ANALYTICS_CATEGORIES.join(','),
            });

            if (from) params.set('date_from', toISODate(from));
            if (to) params.set('date_to', toISODate(to));

            return `${base}/analytics/data/${businessId}?${params.toString()}`;
        },
        [businessId]
    );

    const fetchData = useCallback(
        async (from, to) => {
            if (!businessId) return;
            setLoading(true);

            try {
                const url = buildUrl(from, to);
                const res = await fetch(url);

                if (!res.ok) {
                    toastRef.current?.show({
                        severity: 'warn',
                        summary: 'No Data',
                        detail: 'Analytics data not available. Run the analytics pipeline first.',
                        life: 5000,
                    });
                    setRawCategories(null);
                    return;
                }

                const json = await res.json();
                console.log('[ExecutiveOverview] Fetched data:', json);

                // Detect data mode from response metadata if available
                if (json.mode) setDataMode(json.mode);

                setRawCategories(json.categories || {});
            } catch (err) {
                console.error('[ExecutiveOverview] Fetch error:', err);
                toastRef.current?.show({
                    severity: 'error',
                    summary: 'Error',
                    detail: 'Failed to load analytics data',
                    life: 5000,
                });
            } finally {
                setLoading(false);
            }
        },
        [businessId, buildUrl]
    );

    // Initial load
    useEffect(() => {
        fetchData(null, null);
    }, [businessId]); // eslint-disable-line react-hooks/exhaustive-deps

    // Re-fetch when date range changes (sends params to API for db/api modes)
    useEffect(() => {
        fetchData(dateRange.from, dateRange.to);
    }, [dateRange]); // eslint-disable-line react-hooks/exhaustive-deps

    // WebSocket real-time refresh
    useEffect(() => {
        if (lastUpdate?.files) {
            toastRef.current?.show({
                severity: 'info',
                summary: 'Data Updated',
                detail: `${lastUpdate.total_files} metric(s) updated`,
                life: 3000,
            });
            fetchData(dateRange.from, dateRange.to);
        }
    }, [lastUpdate]); // eslint-disable-line react-hooks/exhaustive-deps

    // -----------------------------------------------------------------------
    // Date filter helpers
    // -----------------------------------------------------------------------

    const applyQuickFilter = (days) => {
        const to = new Date();
        const from = new Date();
        from.setDate(from.getDate() - days);
        setQuickFilter(days);
        setDateRange({ from, to });
    };

    const resetFilters = () => {
        setQuickFilter(null);
        setDateRange({ from: null, to: null });
    };

    // -----------------------------------------------------------------------
    // Client-side filtering (BATCH mode fallback)
    //
    // For db/api modes the server already filtered — this is a no-op (from/to
    // are null-ish from the server's perspective, or results are already scoped).
    // For batch mode (or when the server ignores params) we filter here.
    // -----------------------------------------------------------------------

    const clientFilter = useCallback(
        (arr, dateField = 'grain_date') =>
            filterByDateRange(arr, dateField, dateRange.from, dateRange.to),
        [dateRange]
    );

    // -----------------------------------------------------------------------
    // Derived / processed data  (recalculated whenever rawCategories or
    // dateRange changes, so charts always reflect the current filter)
    // -----------------------------------------------------------------------

    const derived = useMemo(() => {
        if (!rawCategories) return null;

        // ---- KPIs ----
        const kpisCategory = rawCategories.kpis ?? {};

        // business_health_daily — apply client filter for BATCH mode
        const bhRaw = kpisCategory?.analytics?.business_health_daily?.data ?? [];
        const bhFiltered = clientFilter(bhRaw, 'grain_date');

        // Aggregate filtered rows for KPI cards
        const agg = aggregateDailyRows(bhFiltered);

        // Revenue trend (already filtered)
        const revenueTrend = bhFiltered.map((item) => ({
            date: item.grain_date,
            revenue: item.total_revenue ?? 0,
        }));

        // CLV summary — not time-series, show as-is
        const clvRaw = kpisCategory?.analytics?.clv_summary?.data ?? [];
        const clvItem = clvRaw[0] ?? {};

        // Funnel summary — not time-series, show as-is
        const funnelRaw = kpisCategory?.analytics?.funnel_summary?.data ?? [];
        const funnelItem = funnelRaw[0] ?? {};

        const kpis = {
            ...agg,
            avgCLV: clvItem.avg_clv ?? 0,
            totalCustomers: clvItem.customers ?? 0,
            conversionRate: funnelItem.overall_conversion_rate ?? 0,
        };

        // ---- Customer analytics ----
        const custCategory = rawCategories.customer_analytics ?? {};

        const newCustRaw = custCategory?.analytics?.new_customers_daily?.data ?? [];
        const newCustFiltered = clientFilter(newCustRaw, 'grain_date').slice(-30);
        const newCustomers =
            newCustFiltered.length > 0
                ? newCustFiltered[newCustFiltered.length - 1].new_customers ?? 0
                : 0;

        const cumCustRaw = custCategory?.analytics?.cumulative_customers_daily?.data ?? [];
        const cumCustFiltered = clientFilter(cumCustRaw, 'grain_date');
        const latestCum =
            cumCustFiltered.length > 0
                ? cumCustFiltered[cumCustFiltered.length - 1]
                : null;

        const customerData = {
            totalCustomers: latestCum?.cumulative_customers ?? kpis.totalCustomers,
            newCustomers,
            newCustomersTrend: newCustFiltered.map((r) => ({
                date: r.grain_date,
                count: r.new_customers ?? 0,
            })),
        };

        // ---- Product analytics ----
        const prodCategory = rawCategories.product_analytics ?? {};
        // Best-selling products is not time-series; filter by date if field exists
        const bspRaw = prodCategory?.analytics?.best_selling_products?.data ?? [];
        const productData = bspRaw.slice(0, 10).map((item) => ({
            name: item.product_name ?? 'Unknown',
            sales: item.total_units_sold ?? 0,
            revenue: item.total_revenue ?? 0,
            category: item.category ?? '',
        }));

        // ---- Operations analytics ----
        const opsCategory = rawCategories.operations_analytics ?? {};

        const procRaw = opsCategory?.analytics?.processing_by_status?.data ?? [];
        const procFiltered = clientFilter(procRaw, 'grain_date');
        const processingTime =
            procFiltered.length > 0
                ? procFiltered.reduce(
                      (s, r) => s + (r.avg_processing_duration_hours ?? 0),
                      0
                  ) / procFiltered.length
                : 0;

        const delivRaw = opsCategory?.analytics?.ontime_delivery_by_country?.data ?? [];
        const delivFiltered = clientFilter(delivRaw, 'grain_date');
        const onTimeDeliveryRate =
            delivFiltered.length > 0
                ? delivFiltered.reduce((s, r) => s + (r.ontime_delivery_rate ?? 0), 0) /
                  delivFiltered.length
                : 0;

        const daysRaw = opsCategory?.analytics?.delivery_days_by_country?.data ?? [];
        const daysFiltered = clientFilter(daysRaw, 'grain_date');
        const deliveryDays =
            daysFiltered.length > 0
                ? daysFiltered.reduce((s, r) => s + (r.avg_delivery_days ?? 0), 0) /
                  daysFiltered.length
                : 0;

        const operationsData = { processingTime, onTimeDeliveryRate, deliveryDays };

        // ---- Marketing analytics ----
        const mktCategory = rawCategories.marketing_analytics ?? {};
        const campRaw = mktCategory?.analytics?.campaign_performance_summary?.data ?? [];
        const campFiltered = clientFilter(campRaw, 'grain_date');

        const marketingData = {
            totalCampaigns: campFiltered.length,
            avgROI:
                campFiltered.length > 0
                    ? campFiltered.reduce((s, r) => s + (r.campaign_roi ?? 0), 0) /
                      campFiltered.length
                    : 0,
            totalCampaignRevenue: campFiltered.reduce(
                (s, r) => s + (r.total_revenue ?? 0),
                0
            ),
            avgCampaignCost:
                campFiltered.length > 0
                    ? campFiltered.reduce((s, r) => s + (r.total_cost ?? 0), 0) /
                      campFiltered.length
                    : 0,
        };

        return { kpis, revenueTrend, customerData, productData, operationsData, marketingData };
    }, [rawCategories, clientFilter]);

    // -----------------------------------------------------------------------
    // Chart configs (built from derived data)
    // -----------------------------------------------------------------------

    const revenueChartData = useMemo(() => {
        const trend = derived?.revenueTrend ?? [];
        return {
            labels: trend.map((d) => d.date),
            datasets: [
                {
                    label: 'Revenue',
                    data: trend.map((d) => d.revenue),
                    borderColor: 'rgb(75, 192, 192)',
                    backgroundColor: 'rgba(75, 192, 192, 0.2)',
                    tension: 0.4,
                },
            ],
        };
    }, [derived]);

    const revenueChartOptions = useMemo(
        () => ({
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: true, position: 'top' },
                title: { display: true, text: 'Revenue Trend' },
            },
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: { callback: (v) => '$' + v.toLocaleString() },
                },
            },
        }),
        []
    );

    const productChartData = useMemo(() => {
        const top5 = (derived?.productData ?? []).slice(0, 5);
        return {
            labels: top5.map((p) => p.name),
            datasets: [
                {
                    label: 'Sales',
                    data: top5.map((p) => p.sales),
                    backgroundColor: [
                        'rgba(255, 99, 132, 0.6)',
                        'rgba(54, 162, 235, 0.6)',
                        'rgba(255, 206, 86, 0.6)',
                        'rgba(75, 192, 192, 0.6)',
                        'rgba(153, 102, 255, 0.6)',
                    ],
                },
            ],
        };
    }, [derived]);

    const productChartOptions = useMemo(
        () => ({
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: true, position: 'right' },
                title: { display: true, text: 'Top 5 Products' },
            },
        }),
        []
    );

    // -----------------------------------------------------------------------
    // Visibility helpers
    // -----------------------------------------------------------------------

    const hasData = useMemo(() => {
        if (!derived) return false;
        const { kpis, revenueTrend, productData, customerData, operationsData, marketingData } =
            derived;
        return (
            kpis.totalRevenue > 0 ||
            kpis.totalOrders > 0 ||
            kpis.avgOrderValue > 0 ||
            kpis.totalCustomers > 0 ||
            kpis.profitMargin > 0 ||
            kpis.avgCLV > 0 ||
            kpis.conversionRate > 0 ||
            revenueTrend.length > 0 ||
            productData.length > 0 ||
            customerData.totalCustomers > 0 ||
            customerData.newCustomers > 0 ||
            operationsData.onTimeDeliveryRate > 0 ||
            operationsData.deliveryDays > 0 ||
            marketingData.totalCampaigns > 0 ||
            marketingData.avgROI > 0 ||
            marketingData.totalCampaignRevenue > 0
        );
    }, [derived]);

    // -----------------------------------------------------------------------
    // Render
    // -----------------------------------------------------------------------

    const isFiltered = !!(dateRange.from || dateRange.to);

    if (loading && pipelineStatus !== 'loading') {
        return (
            <div className="flex flex-col items-center justify-center min-h-[400px] gap-4">
                <ProgressSpinner />
                <p className="text-gray-500 text-base">Loading executive overview…</p>
            </div>
        );
    }

    if (!hasData && !loading && pipelineStatus !== 'loading') {
        return (
            <div className="p-6 min-h-[calc(100vh-120px)]">
                <Toast ref={toastRef} />
                {/* Still show the filter bar even when empty so user can reset */}
                <DateFilterBar
                    quickFilter={quickFilter}
                    dateRange={dateRange}
                    isFiltered={isFiltered}
                    onQuickFilter={applyQuickFilter}
                    onDateChange={setDateRange}
                    onReset={resetFilters}
                />
                <div className="flex items-center justify-center min-h-[50vh]">
                    <p className="text-gray-500 text-lg">
                        {isFiltered
                            ? 'No data found for the selected date range.'
                            : 'No data to display. Run the analytics pipeline first.'}
                    </p>
                </div>
            </div>
        );
    }

    const { kpis, revenueTrend, customerData, productData, operationsData, marketingData } =
        derived ?? {};

    return (
        <div className="p-6 bg-gray-50 min-h-[calc(100vh-120px)]">
            <Toast ref={toastRef} />

            {/* Date Filter Bar */}
            <DateFilterBar
                quickFilter={quickFilter}
                dateRange={dateRange}
                isFiltered={isFiltered}
                onQuickFilter={applyQuickFilter}
                onDateChange={setDateRange}
                onReset={resetFilters}
                dataMode={dataMode}
            />

            {/* ---- KPI Cards ---- */}
            {(kpis.totalRevenue > 0 ||
                kpis.totalOrders > 0 ||
                kpis.avgOrderValue > 0 ||
                kpis.totalCustomers > 0 ||
                kpis.profitMargin > 0 ||
                kpis.avgCLV > 0 ||
                kpis.conversionRate > 0) && (
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-8">
                    {kpis.totalRevenue > 0 && (
                        <KPICard
                            icon="pi-dollar"
                            iconBg="bg-green-50"
                            iconColor="text-green-500"
                            value={fmt.currency(kpis.totalRevenue)}
                            label="Total Revenue"
                        />
                    )}
                    {kpis.totalOrders > 0 && (
                        <KPICard
                            icon="pi-shopping-cart"
                            iconBg="bg-blue-50"
                            iconColor="text-blue-500"
                            value={fmt.number(kpis.totalOrders)}
                            label="Total Orders"
                        />
                    )}
                    {kpis.avgOrderValue > 0 && (
                        <KPICard
                            icon="pi-chart-line"
                            iconBg="bg-orange-50"
                            iconColor="text-orange-500"
                            value={fmt.currency(kpis.avgOrderValue)}
                            label="Average Order Value"
                        />
                    )}
                    {kpis.totalCustomers > 0 && (
                        <KPICard
                            icon="pi-users"
                            iconBg="bg-purple-50"
                            iconColor="text-purple-500"
                            value={fmt.number(kpis.totalCustomers)}
                            label="Total Customers"
                        />
                    )}
                    {kpis.profitMargin > 0 && (
                        <KPICard
                            icon="pi-percentage"
                            iconBg="bg-red-50"
                            iconColor="text-red-500"
                            value={fmt.pct(kpis.profitMargin)}
                            label="Profit Margin"
                        />
                    )}
                    {kpis.avgCLV > 0 && (
                        <KPICard
                            icon="pi-star"
                            iconBg="bg-yellow-50"
                            iconColor="text-yellow-500"
                            value={fmt.currency(kpis.avgCLV)}
                            label="Avg Customer Lifetime Value"
                        />
                    )}
                    {kpis.conversionRate > 0 && (
                        <KPICard
                            icon="pi-chart-bar"
                            iconBg="bg-cyan-50"
                            iconColor="text-cyan-500"
                            value={fmt.pct(kpis.conversionRate)}
                            label="Conversion Rate"
                        />
                    )}
                </div>
            )}

            {/* ---- Charts + Metric Cards ---- */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
                {/* Revenue Trend — full width */}
                {revenueTrend?.length > 0 && (
                    <div className="col-span-1 lg:col-span-2">
                        <ChartWrapper title="Revenue Trend" showUpdateBadge={false}>
                            <div className="h-[300px]">
                                <Line data={revenueChartData} options={revenueChartOptions} />
                            </div>
                        </ChartWrapper>
                    </div>
                )}

                {/* Top Products Doughnut */}
                {productData?.length > 0 && (
                    <ChartWrapper title="Top Products" showUpdateBadge={false}>
                        <div className="h-[300px]">
                            <Doughnut data={productChartData} options={productChartOptions} />
                        </div>
                    </ChartWrapper>
                )}

                {/* Customer Metrics */}
                <MetricsCard
                    title="Customer Metrics"
                    rows={[
                        {
                            label: 'Total Customers',
                            value: fmt.number(customerData?.totalCustomers),
                            show: customerData?.totalCustomers > 0,
                        },
                        {
                            label: 'New Customers (Recent)',
                            value: fmt.number(customerData?.newCustomers),
                            show: customerData?.newCustomers > 0,
                        },
                    ]}
                />

                {/* Operations Metrics */}
                <MetricsCard
                    title="Operations Metrics"
                    rows={[
                        {
                            label: 'Avg Processing Time',
                            value: fmt.hours(operationsData?.processingTime),
                            show: operationsData?.processingTime > 0,
                        },
                        {
                            label: 'On-Time Delivery',
                            value: fmt.pct(operationsData?.onTimeDeliveryRate),
                            show: operationsData?.onTimeDeliveryRate > 0,
                        },
                        {
                            label: 'Avg Delivery Days',
                            value: fmt.days(operationsData?.deliveryDays),
                            show: operationsData?.deliveryDays > 0,
                        },
                    ]}
                />

                {/* Marketing Performance */}
                <MetricsCard
                    title="Marketing Performance"
                    rows={[
                        {
                            label: 'Total Campaigns',
                            value: fmt.number(marketingData?.totalCampaigns),
                            show: marketingData?.totalCampaigns > 0,
                        },
                        {
                            label: 'Average ROI',
                            value: fmt.pct(marketingData?.avgROI),
                            show: marketingData?.avgROI > 0,
                        },
                        {
                            label: 'Campaign Revenue',
                            value: fmt.currency(marketingData?.totalCampaignRevenue),
                            show: marketingData?.totalCampaignRevenue > 0,
                        },
                    ]}
                />
            </div>
        </div>
    );
};

// ---------------------------------------------------------------------------
// DateFilterBar — extracted for reuse in the empty-state path too
// ---------------------------------------------------------------------------

const DateFilterBar = ({
    quickFilter,
    dateRange,
    isFiltered,
    onQuickFilter,
    onDateChange,
    onReset,
    dataMode,
}) => (
    <div className="mb-6 p-4 bg-white rounded-lg shadow-sm">
        <div className="flex flex-wrap items-center gap-3">
            <span className="text-sm font-medium text-gray-700">Filter by:</span>

            {QUICK_FILTERS.map(({ label, days }) => (
                <SecondaryButton
                    key={label}
                    label={label}
                    size="small"
                    outlined
                    onClick={() => onQuickFilter(days)}
                    className={quickFilter === days ? 'p-button-primary' : ''}
                />
            ))}

            <span className="mx-2 text-gray-400">|</span>

            <Calendar
                value={dateRange.from}
                onChange={(e) => onDateChange({ ...dateRange, from: e.value })}
                placeholder="From Date"
                showIcon
                dateFormat="yy-mm-dd"
                className="w-auto"
            />
            <Calendar
                value={dateRange.to}
                onChange={(e) => onDateChange({ ...dateRange, to: e.value })}
                placeholder="To Date"
                showIcon
                dateFormat="yy-mm-dd"
                className="w-auto"
            />

            {isFiltered && (
                <SecondaryButton
                    label="Reset"
                    size="small"
                    severity="secondary"
                    onClick={onReset}
                />
            )}

            {/* Mode badge — helpful for debugging, can be removed in prod */}
            {dataMode && dataMode !== 'unknown' && (
                <span className="ml-auto text-xs px-2 py-1 rounded-full bg-gray-100 text-gray-500 font-mono uppercase">
                    {dataMode} mode
                </span>
            )}
        </div>
    </div>
);

export default ExecutiveOverview;