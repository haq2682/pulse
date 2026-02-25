import React, { useState, useEffect, useRef, useMemo, useCallback } from 'react';
import { useParams } from 'react-router-dom';
import { Card } from 'primereact/card';
import { ProgressSpinner } from 'primereact/progressspinner';
import { Toast } from 'primereact/toast';
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
    Legend,
} from 'chart.js';
import { Line, Bar, Doughnut } from 'react-chartjs-2';
import { useAnalyticsWebSocket } from '../../../../hooks/useAnalyticsWebSocket';
import ChartWrapper from '../components/ChartWrapper';
import { usePipelineProgress } from '@/context/PipelineProgressContext';
import useAnalyticsDateFilter, { aggregateDailyRows } from '@/hooks/useAnalyticsDateFilter';
import DateFilterBar from '../components/DateFilterBar';
import { useFormatters } from '@/hooks/useFormatters';

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

const ANALYTICS_CATEGORIES = [
    'kpis',
    'customer_analytics',
    'product_analytics',
    'operations_analytics',
    'marketing_analytics',
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
    const fmt = useFormatters();
    const { businessId } = useParams();
    const toastRef = useRef(null);
    const { pipelineStatus } = usePipelineProgress();

    // ---- raw (unfiltered) data from API ----
    const [loading, setLoading] = useState(true);
    const [rawCategories, setRawCategories] = useState(null);
    const [dataMode, setDataMode] = useState('unknown');

    // ---- date filter (shared hook) ----
    const {
        dateRange,
        setDateRange,
        quickFilter,
        isFiltered,
        applyQuickFilter,
        resetFilters,
        clientFilter,
        toISODate,
    } = useAnalyticsDateFilter();

    // ---- websocket ----
    const { lastUpdate } = useAnalyticsWebSocket(businessId);

    // -----------------------------------------------------------------------
    // Fetch
    // -----------------------------------------------------------------------

    const buildUrl = useCallback(
        (from, to) => {
            const base = import.meta.env.VITE_API_URL || 'http://localhost:8000';
            const params = new URLSearchParams({
                categories: ANALYTICS_CATEGORIES.join(','),
            });
            if (from) params.set('date_from', toISODate(from));
            if (to)   params.set('date_to',   toISODate(to));
            return `${base}/analytics/data/${businessId}?${params.toString()}`;
        },
        [businessId, toISODate]
    );

    const fetchData = useCallback(
        async (from, to) => {
            if (!businessId) return;
            setLoading(true);
            try {
                const res = await fetch(buildUrl(from, to));

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
                if (json.mode) setDataMode(json.mode);
                setRawCategories(json.categories ?? {});
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

    useEffect(() => { fetchData(null, null); }, [businessId]); // eslint-disable-line

    useEffect(() => { fetchData(dateRange.from, dateRange.to); }, [dateRange]); // eslint-disable-line

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
    }, [lastUpdate]); // eslint-disable-line

    // -----------------------------------------------------------------------
    // Derived data — all client-side filtering happens here
    // -----------------------------------------------------------------------

    const derived = useMemo(() => {
        if (!rawCategories) return null;

        const kpisCategory = rawCategories.kpis ?? {};

        // ---- business_health_daily — daily KPI time-series ----
        const bhRaw      = kpisCategory?.analytics?.business_health_daily?.data ?? [];
        const bhFiltered = clientFilter(bhRaw, 'grain_date');
        const agg        = aggregateDailyRows(bhFiltered);

        const revenueTrend = bhFiltered.map((item) => ({
            date:    item.grain_date,
            revenue: item.total_revenue ?? 0,
        }));

        // ---- Total customers — use cumulative_customers_daily (time-series) ----
        // clv_summary has NO date field so it cannot be filtered client-side.
        // cumulative_customers_daily correctly reflects the customer count up to
        // the selected "to" date and is the right source for filtered views.
        const custCategory    = rawCategories.customer_analytics ?? {};
        const cumCustRaw      = custCategory?.analytics?.cumulative_customers_daily?.data ?? [];
        const cumCustFiltered = clientFilter(cumCustRaw, 'grain_date');
        const latestCumRow    = cumCustFiltered.length > 0
            ? cumCustFiltered[cumCustFiltered.length - 1]
            : null;
        const totalCustomers  = latestCumRow?.cumulative_customers ?? 0;

        // ---- CLV — clv_summary is a static aggregate with no date field ----
        // It cannot be meaningfully filtered by date. We always show the all-time
        // value and make that clear in the label when a filter is active.
        const clvRaw  = kpisCategory?.analytics?.clv_summary?.data ?? [];
        const clvItem = clvRaw[0] ?? {};
        const avgCLV  = clvItem.avg_clv ?? 0;

        // ---- funnel_summary — static aggregate (no date field) ----
        const funnelRaw  = kpisCategory?.analytics?.funnel_summary?.data ?? [];
        const funnelItem = funnelRaw[0] ?? {};

        // ---- cart_abandon_summary — static aggregate (no date field) ----
        const cartRaw  = kpisCategory?.analytics?.cart_abandon_summary?.data ?? [];
        const cartItem = cartRaw[0] ?? {};

        // ---- customer_engagement_summary — static aggregate (no date field) ----
        const engRaw  = kpisCategory?.analytics?.customer_engagement_summary?.data ?? [];
        const engItem = engRaw[0] ?? {};

        // ---- session_to_order_analysis — static aggregate (no date field) ----
        const sessRaw  = kpisCategory?.analytics?.session_to_order_analysis?.data ?? [];
        const sessItem = sessRaw[0] ?? {};

        const kpis = {
            ...agg,
            // Customers — from time-series, responds to date filter
            totalCustomers,
            // CLV & funnel — static aggregates, always all-time
            avgCLV,
            avgTotalRevenue:       clvItem.avg_total_revenue       ?? 0,
            avgOrderValueOverall:  clvItem.avg_order_value_overall ?? 0,
            totalRevenueAllCustomers: clvItem.total_revenue_all_customers ?? 0,
            conversionRate:        funnelItem.overall_conversion_rate  ?? 0,
            viewToCartConversion:  funnelItem.view_to_cart_conversion  ?? 0,
            cartToOrderConversion: funnelItem.cart_to_order_conversion ?? 0,
            totalSessions:         funnelItem.total_sessions            ?? 0,
            avgSessionValue:       funnelItem.avg_session_value         ?? 0,
            // Cart abandonment
            abandonmentRate:       cartItem.abandonment_rate  ?? 0,
            purchaseRate:          cartItem.purchase_rate     ?? 0,
            totalCartsTracked:     cartItem.total_carts_tracked ?? 0,
            abandonedCarts:        cartItem.abandoned_carts    ?? 0,
            convertedCarts:        cartItem.converted_carts   ?? 0,
            // Engagement
            avgSessionsPerCustomer:      engItem.avg_sessions_per_customer       ?? 0,
            avgPagesViewedPerCustomer:   engItem.avg_pages_viewed_per_customer   ?? 0,
            avgProductsViewedPerCustomer: engItem.avg_products_viewed_per_customer ?? 0,
            // Session-to-order
            avgSessionConversionRate: sessItem.avg_session_conversion_rate ?? 0,
            avgCartAbandonmentRate:   sessItem.avg_cart_abandonment_rate   ?? 0,
        };

        // ---- New customers (time-series) ----
        const newCustRaw      = custCategory?.analytics?.new_customers_daily?.data ?? [];
        const newCustFiltered = clientFilter(newCustRaw, 'grain_date').slice(-30);
        const newCustomers    =
            newCustFiltered.length > 0
                ? newCustFiltered[newCustFiltered.length - 1].new_customers ?? 0
                : 0;

        const customerData = {
            totalCustomers,
            newCustomers,
            newCustomersTrend: newCustFiltered.map((r) => ({
                date:  r.grain_date,
                count: r.new_customers ?? 0,
            })),
        };

        // ---- Product analytics ----
        const prodCategory = rawCategories.product_analytics ?? {};
        const bspRaw       = prodCategory?.analytics?.best_selling_products?.data ?? [];
        const productData  = bspRaw.slice(0, 10).map((item) => ({
            name:     item.product_name ?? 'Unknown',
            sales:    item.total_units_sold ?? 0,
            revenue:  item.total_revenue ?? 0,
            category: item.category ?? '',
        }));

        // ---- Operations analytics ----
        const opsCategory = rawCategories.operations_analytics ?? {};

        const procRaw      = opsCategory?.analytics?.processing_by_status?.data ?? [];
        const procFiltered = clientFilter(procRaw, 'grain_date');
        const processingTime =
            procFiltered.length > 0
                ? procFiltered.reduce((s, r) => s + (r.avg_processing_duration_hours ?? 0), 0) /
                  procFiltered.length
                : 0;

        const delivRaw      = opsCategory?.analytics?.ontime_delivery_by_country?.data ?? [];
        const delivFiltered = clientFilter(delivRaw, 'grain_date');
        const onTimeDeliveryRate =
            delivFiltered.length > 0
                ? delivFiltered.reduce((s, r) => s + (r.ontime_delivery_rate ?? 0), 0) /
                  delivFiltered.length
                : 0;

        const daysRaw      = opsCategory?.analytics?.delivery_days_by_country?.data ?? [];
        const daysFiltered = clientFilter(daysRaw, 'grain_date');
        const deliveryDays =
            daysFiltered.length > 0
                ? daysFiltered.reduce((s, r) => s + (r.avg_delivery_days ?? 0), 0) /
                  daysFiltered.length
                : 0;

        const operationsData = { processingTime, onTimeDeliveryRate, deliveryDays };

        // ---- Marketing analytics ----
        const mktCategory        = rawCategories.marketing_analytics ?? {};
        const campaignDateFields = ['grain_date', 'campaign_date', 'start_date', 'created_at', 'date'];
        const campRaw            = mktCategory?.analytics?.campaign_performance_summary?.data ?? [];
        const campFiltered       = clientFilter(campRaw, campaignDateFields);

        const marketingData = {
            totalCampaigns:       campFiltered.length,
            avgROI:
                campFiltered.length > 0
                    ? campFiltered.reduce((s, r) => s + (r.campaign_roi ?? 0), 0) /
                      campFiltered.length
                    : 0,
            totalCampaignRevenue: campFiltered.reduce((s, r) => s + (r.total_revenue ?? 0), 0),
            avgCampaignCost:
                campFiltered.length > 0
                    ? campFiltered.reduce((s, r) => s + (r.total_cost ?? 0), 0) /
                      campFiltered.length
                    : 0,
        };

        return { kpis, revenueTrend, customerData, productData, operationsData, marketingData,
            bhWeeklyRaw: kpisCategory?.analytics?.business_health_weekly?.data ?? [],
            bhMonthlyRaw: kpisCategory?.analytics?.business_health_monthly?.data ?? [],
        };
    }, [rawCategories, clientFilter]);

    // -----------------------------------------------------------------------
    // Chart configs
    // -----------------------------------------------------------------------

    const revenueChartData = useMemo(() => {
        const trend = derived?.revenueTrend ?? [];
        return {
            labels: trend.map((d) => d.date),
            datasets: [
                {
                    label:           'Revenue',
                    data:            trend.map((d) => d.revenue),
                    borderColor:     'rgb(75, 192, 192)',
                    backgroundColor: 'rgba(75, 192, 192, 0.2)',
                    tension:         0.4,
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
                title:  { display: true, text: 'Revenue Trend' },
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
                    data:  top5.map((p) => p.sales),
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
                title:  { display: true, text: 'Top 5 Products' },
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
                <DateFilterBar
                    quickFilter={quickFilter}
                    dateRange={dateRange}
                    isFiltered={isFiltered}
                    onQuickFilter={applyQuickFilter}
                    onDateChange={setDateRange}
                    onReset={resetFilters}
                    dataMode={dataMode}
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

    const { kpis, revenueTrend, customerData, productData, operationsData, marketingData } =
        derived ?? {};

    return (
        <div className="p-6 bg-gray-50 min-h-[calc(100vh-120px)]">
            <Toast ref={toastRef} />

            <DateFilterBar
                quickFilter={quickFilter}
                dateRange={dateRange}
                isFiltered={isFiltered}
                onQuickFilter={applyQuickFilter}
                onDateChange={setDateRange}
                onReset={resetFilters}
                dataMode={dataMode}
                hidden={loading && pipelineStatus === 'loading'}
            />

            {/* ---- Static-data notice when a date filter is active ---- */}
            {isFiltered && (
                <p className="mb-4 text-xs text-gray-400 italic">
                    * CLV, funnel, cart, and engagement metrics are all-time aggregates and do not
                    change with the date filter. Revenue, orders, customers, and operations metrics
                    reflect the selected period.
                </p>
            )}

            {/* ---- KPI Cards ---- */}
            {(kpis?.totalRevenue > 0 ||
                kpis?.totalOrders > 0 ||
                kpis?.avgOrderValue > 0 ||
                kpis?.totalCustomers > 0 ||
                kpis?.profitMargin > 0 ||
                kpis?.avgCLV > 0 ||
                kpis?.conversionRate > 0) && (
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
                            label={`Avg Customer Lifetime Value${isFiltered ? ' *' : ''}`}
                        />
                    )}
                    {kpis.conversionRate > 0 && (
                        <KPICard
                            icon="pi-chart-bar"
                            iconBg="bg-cyan-50"
                            iconColor="text-cyan-500"
                            value={fmt.pct(kpis.conversionRate)}
                            label={`Conversion Rate${isFiltered ? ' *' : ''}`}
                        />
                    )}
                    {kpis.abandonmentRate > 0 && (
                        <KPICard
                            icon="pi-shopping-bag"
                            iconBg="bg-rose-50"
                            iconColor="text-rose-500"
                            value={fmt.pct(kpis.abandonmentRate)}
                            label={`Cart Abandonment Rate${isFiltered ? ' *' : ''}`}
                        />
                    )}
                    {kpis.purchaseRate > 0 && (
                        <KPICard
                            icon="pi-check-circle"
                            iconBg="bg-emerald-50"
                            iconColor="text-emerald-500"
                            value={fmt.pct(kpis.purchaseRate)}
                            label={`Purchase Rate${isFiltered ? ' *' : ''}`}
                        />
                    )}
                    {kpis.avgSessionsPerCustomer > 0 && (
                        <KPICard
                            icon="pi-eye"
                            iconBg="bg-indigo-50"
                            iconColor="text-indigo-500"
                            value={kpis.avgSessionsPerCustomer.toFixed(1)}
                            label={`Avg Sessions / Customer${isFiltered ? ' *' : ''}`}
                        />
                    )}
                </div>
            )}

            {/* ---- Charts + Metric Cards ---- */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
                {revenueTrend?.length > 0 && (
                    <div className="col-span-1 lg:col-span-2">
                        <ChartWrapper title="Revenue Trend" showUpdateBadge={false}>
                            <div className="h-[300px]">
                                <Line data={revenueChartData} options={revenueChartOptions} />
                            </div>
                        </ChartWrapper>
                    </div>
                )}

                {productData?.length > 0 && (
                    <ChartWrapper title="Top Products" showUpdateBadge={false}>
                        <div className="h-[300px]">
                            <Doughnut data={productChartData} options={productChartOptions} />
                        </div>
                    </ChartWrapper>
                )}

                <MetricsCard
                    title="Customer Metrics"
                    rows={[
                        {
                            label: 'Total Customers',
                            value: fmt.number(customerData?.totalCustomers),
                            show:  customerData?.totalCustomers > 0,
                        },
                        {
                            label: 'New Customers (Recent)',
                            value: fmt.number(customerData?.newCustomers),
                            show:  customerData?.newCustomers > 0,
                        },
                    ]}
                />

                <MetricsCard
                    title="Operations Metrics"
                    rows={[
                        {
                            label: 'Avg Processing Time',
                            value: fmt.hours(operationsData?.processingTime),
                            show:  operationsData?.processingTime > 0,
                        },
                        {
                            label: 'On-Time Delivery',
                            value: fmt.pct(operationsData?.onTimeDeliveryRate),
                            show:  operationsData?.onTimeDeliveryRate > 0,
                        },
                        {
                            label: 'Avg Delivery Days',
                            value: fmt.days(operationsData?.deliveryDays),
                            show:  operationsData?.deliveryDays > 0,
                        },
                    ]}
                />

                <MetricsCard
                    title="Marketing Performance"
                    rows={[
                        {
                            label: 'Total Campaigns',
                            value: fmt.number(marketingData?.totalCampaigns),
                            show:  marketingData?.totalCampaigns > 0,
                        },
                        {
                            label: 'Average ROI',
                            value: fmt.pct(marketingData?.avgROI),
                            show:  marketingData?.avgROI > 0,
                        },
                        {
                            label: 'Campaign Revenue',
                            value: fmt.currency(marketingData?.totalCampaignRevenue),
                            show:  marketingData?.totalCampaignRevenue > 0,
                        },
                    ]}
                />

                {/* ---- CLV Details (all-time, static) ---- */}
                <MetricsCard
                    title={`Customer Lifetime Value${isFiltered ? ' (All-time *)' : ''}`}
                    rows={[
                        {
                            label: 'Avg CLV',
                            value: fmt.currency(kpis?.avgCLV),
                            show:  kpis?.avgCLV > 0,
                        },
                        {
                            label: 'Avg Total Revenue / Customer',
                            value: fmt.currency(kpis?.avgTotalRevenue),
                            show:  kpis?.avgTotalRevenue > 0,
                        },
                        {
                            label: 'Avg Order Value (Lifetime)',
                            value: fmt.currency(kpis?.avgOrderValueOverall),
                            show:  kpis?.avgOrderValueOverall > 0,
                        },
                        {
                            label: 'Total Revenue (All Customers)',
                            value: fmt.currency(kpis?.totalRevenueAllCustomers),
                            show:  kpis?.totalRevenueAllCustomers > 0,
                        },
                    ]}
                />

                {/* ---- Funnel Metrics (all-time, static) ---- */}
                <MetricsCard
                    title={`Funnel Metrics${isFiltered ? ' (All-time *)' : ''}`}
                    rows={[
                        {
                            label: 'Overall Conversion Rate',
                            value: fmt.pct(kpis?.conversionRate),
                            show:  kpis?.conversionRate > 0,
                        },
                        {
                            label: 'View → Cart',
                            value: fmt.pct(kpis?.viewToCartConversion),
                            show:  kpis?.viewToCartConversion > 0,
                        },
                        {
                            label: 'Cart → Order',
                            value: fmt.pct(kpis?.cartToOrderConversion),
                            show:  kpis?.cartToOrderConversion > 0,
                        },
                        {
                            label: 'Total Sessions',
                            value: fmt.number(kpis?.totalSessions),
                            show:  kpis?.totalSessions > 0,
                        },
                        {
                            label: 'Avg Session Value',
                            value: fmt.currency(kpis?.avgSessionValue),
                            show:  kpis?.avgSessionValue > 0,
                        },
                    ]}
                />

                {/* ---- Cart Abandonment (all-time, static) ---- */}
                <MetricsCard
                    title={`Cart Abandonment${isFiltered ? ' (All-time *)' : ''}`}
                    rows={[
                        {
                            label: 'Total Carts Tracked',
                            value: fmt.number(kpis?.totalCartsTracked),
                            show:  kpis?.totalCartsTracked > 0,
                        },
                        {
                            label: 'Abandoned Carts',
                            value: fmt.number(kpis?.abandonedCarts),
                            show:  kpis?.abandonedCarts > 0,
                        },
                        {
                            label: 'Converted Carts',
                            value: fmt.number(kpis?.convertedCarts),
                            show:  kpis?.convertedCarts > 0,
                        },
                        {
                            label: 'Abandonment Rate',
                            value: fmt.pct(kpis?.abandonmentRate),
                            show:  kpis?.abandonmentRate > 0,
                        },
                        {
                            label: 'Purchase Rate',
                            value: fmt.pct(kpis?.purchaseRate),
                            show:  kpis?.purchaseRate > 0,
                        },
                    ]}
                />

                {/* ---- Customer Engagement (all-time, static) ---- */}
                <MetricsCard
                    title={`Customer Engagement${isFiltered ? ' (All-time *)' : ''}`}
                    rows={[
                        {
                            label: 'Avg Sessions / Customer',
                            value: (kpis?.avgSessionsPerCustomer ?? 0).toFixed(1),
                            show:  kpis?.avgSessionsPerCustomer > 0,
                        },
                        {
                            label: 'Avg Pages Viewed / Customer',
                            value: (kpis?.avgPagesViewedPerCustomer ?? 0).toFixed(1),
                            show:  kpis?.avgPagesViewedPerCustomer > 0,
                        },
                        {
                            label: 'Avg Products Viewed / Customer',
                            value: (kpis?.avgProductsViewedPerCustomer ?? 0).toFixed(1),
                            show:  kpis?.avgProductsViewedPerCustomer > 0,
                        },
                        {
                            label: 'Session Conversion Rate',
                            value: fmt.pct(kpis?.avgSessionConversionRate),
                            show:  kpis?.avgSessionConversionRate > 0,
                        },
                        {
                            label: 'Session Cart Abandonment',
                            value: fmt.pct(kpis?.avgCartAbandonmentRate),
                            show:  kpis?.avgCartAbandonmentRate > 0,
                        },
                    ]}
                />
            </div>

            {/* Business Health Weekly & Monthly */}
            {(derived?.bhWeeklyRaw?.length ?? 0) > 0 && (
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
                    <div className="bg-white border border-gray-200 rounded-xl shadow-sm p-6">
                        <h3 className="text-lg font-semibold text-gray-900 mb-4">Business Health (Weekly)</h3>
                        <div className="h-[220px]">
                            <Bar
                                data={{
                                    labels: (derived.bhWeeklyRaw).map((r) => `${r.grain_year}-W${String(r.grain_week ?? 0).padStart(2,'0')}`),
                                    datasets: [{ label: 'Revenue', data: (derived.bhWeeklyRaw).map((r) => +(r.total_revenue ?? 0)), backgroundColor: 'rgba(59,130,246,0.8)' }],
                                }}
                                options={{ responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true, ticks: { callback: (v) => '$' + v.toLocaleString() } } } }}
                            />
                        </div>
                    </div>
                    {(derived?.bhMonthlyRaw?.length ?? 0) > 0 && (
                        <div className="bg-white border border-gray-200 rounded-xl shadow-sm p-6">
                            <h3 className="text-lg font-semibold text-gray-900 mb-4">Business Health (Monthly)</h3>
                            <div className="h-[220px]">
                                <Bar
                                    data={{
                                        labels: (derived.bhMonthlyRaw).map((r) => `${r.grain_year}-${String(r.grain_month ?? 0).padStart(2,'0')}`),
                                        datasets: [{ label: 'Revenue', data: (derived.bhMonthlyRaw).map((r) => +(r.total_revenue ?? 0)), backgroundColor: 'rgba(34,197,94,0.8)' }],
                                    }}
                                    options={{ responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true, ticks: { callback: (v) => '$' + v.toLocaleString() } } } }}
                                />
                            </div>
                        </div>
                    )}
                </div>
            )}
        </div>
    );
};

export default ExecutiveOverview;