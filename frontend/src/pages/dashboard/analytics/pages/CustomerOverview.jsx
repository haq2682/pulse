import React, { useState, useEffect, useRef, useMemo, useCallback } from 'react';
import { useParams } from 'react-router-dom';
import { Card } from 'primereact/card';
import { ProgressSpinner } from 'primereact/progressspinner';
import { Toast } from 'primereact/toast';
import { DataTable } from 'primereact/datatable';
import { Column } from 'primereact/column';
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

const ANALYTICS_CATEGORIES = 'customer_analytics,geo_analytics';

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

// ---------------------------------------------------------------------------
// Default chart options factory
// ---------------------------------------------------------------------------

const defaultLineOpts = (title) => ({
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
        legend: { display: true, position: 'top' },
        title: { display: !!title, text: title },
    },
    scales: { y: { beginAtZero: true } },
});

const defaultBarOpts = (title, horizontal = false) => ({
    responsive: true,
    maintainAspectRatio: false,
    indexAxis: horizontal ? 'y' : 'x',
    plugins: {
        legend: { display: false },
        title: { display: !!title, text: title },
    },
    scales: { y: { beginAtZero: true } },
});

const defaultDoughnutOpts = (title) => ({
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
        legend: { display: true, position: 'right' },
        title: { display: !!title, text: title },
    },
});

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

const CustomerOverview = () => {
    const fmt = useFormatters();
    const { businessId } = useParams();
    const toastRef = useRef(null);
    const { pipelineStatus } = usePipelineProgress();

    const [loading, setLoading] = useState(true);
    const [fetchError, setFetchError] = useState(false);
    const [rawData, setRawData] = useState(null);
    const [rawGeo, setRawGeo] = useState(null);
    const [dataMode, setDataMode] = useState('unknown');

    const {
        dateRange, setDateRange, quickFilter, isFiltered,
        applyQuickFilter, resetFilters, clientFilter, toISODate,
    } = useAnalyticsDateFilter();

    const { lastUpdate } = useAnalyticsWebSocket(businessId);

    // -----------------------------------------------------------------------
    // Fetch
    // -----------------------------------------------------------------------

    const buildUrl = useCallback((from, to) => {
        const base = import.meta.env.VITE_API_URL || 'http://localhost:8000';
        const params = new URLSearchParams({ categories: ANALYTICS_CATEGORIES });
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
            setRawGeo(json.categories?.geo_analytics ?? null);
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
    // Derived data
    // -----------------------------------------------------------------------

    const derived = useMemo(() => {
        if (!rawData) return null;
        const a = rawData.analytics ?? {};
        const geoA = rawGeo?.analytics ?? {};

        // --- Time-filtered analytics ---

        // new_customers_daily — grain_date (time-filterable)
        const newCustRaw = a.new_customers_daily?.data ?? [];
        const newCustFiltered = clientFilter(newCustRaw, 'grain_date');
        const totalNewCustomers = newCustFiltered.reduce((s, r) => s + (r.new_customers ?? 0), 0);

        // cumulative_customers_daily — grain_date (time-filterable)
        const cumCustRaw = a.cumulative_customers_daily?.data ?? [];
        const cumCustFiltered = clientFilter(cumCustRaw, 'grain_date');
        const lastCumRow = cumCustFiltered.length > 0 ? cumCustFiltered[cumCustFiltered.length - 1] : null;
        const totalCustomers = lastCumRow?.cumulative_customers ?? 0;

        // customer_account_status_distribution_daily — grain_date (time-filterable)
        // Aggregate by account_status across filtered dates
        const acctStatusRaw = a.customer_account_status_distribution_daily?.data ?? [];
        const acctStatusFiltered = clientFilter(acctStatusRaw, 'grain_date');
        const acctStatusMap = {};
        acctStatusFiltered.forEach((r) => {
            const s = r.account_status ?? 'Unknown';
            acctStatusMap[s] = (acctStatusMap[s] ?? 0) + (r.customer_count ?? 0);
        });

        // new_customers_geo_acquisition_daily — grain_date (time-filterable)
        const geoRaw = a.new_customers_geo_acquisition_daily?.data ?? [];
        const geoFiltered = clientFilter(geoRaw, 'grain_date');
        // Aggregate by country
        const geoByCountry = {};
        geoFiltered.forEach((r) => {
            const c = r.country ?? 'Unknown';
            geoByCountry[c] = (geoByCountry[c] ?? 0) + (r.new_customers ?? 0);
        });

        // --- Static aggregates (no date field) ---

        // customer_age_group_distribution — static
        const ageGroupRaw = a.customer_age_group_distribution?.data ?? [];

        // customer_country_distribution — static
        const countryDistRaw = a.customer_country_distribution?.data ?? [];

        // customer_age_group_spending — static
        const ageSpendingRaw = a.customer_age_group_spending?.data ?? [];

        // new_vs_returning_customer_country — static
        const newVsReturnRaw = a.new_vs_returning_customer_country?.data ?? [];

        // --- Additional static aggregates ---
        // customer_city_distribution — static
        const cityDistRaw = a.customer_city_distribution?.data ?? [];
        // customer_state_distribution — static
        const stateDistRaw = a.customer_state_distribution?.data ?? [];
        // new_vs_returning_customer_state — static
        const newVsReturnStateRaw = a.new_vs_returning_customer_state?.data ?? [];
        // new_vs_returning_customer_city — static
        const newVsReturnCityRaw = a.new_vs_returning_customer_city?.data ?? [];
        // new_customers_geo_acquisition_monthly — static
        const geoMonthlyRaw = a.new_customers_geo_acquisition_monthly?.data ?? [];
        // geo_analytics geo_acquisition — static (country/state/city level)
        const geoAcquisitionRaw = geoA.geo_acquisition?.data ?? [];

        // --- Weekly / Monthly grain variants (static aggregates) ---
        // new_customers_weekly
        const newCustWeeklyRaw = a.new_customers_weekly?.data ?? [];
        // new_customers_monthly
        const newCustMonthlyRaw = a.new_customers_monthly?.data ?? [];
        // cumulative_customers_weekly
        const cumCustWeeklyRaw = a.cumulative_customers_weekly?.data ?? [];
        // cumulative_customers_monthly
        const cumCustMonthlyRaw = a.cumulative_customers_monthly?.data ?? [];
        // customer_account_status_distribution_weekly
        const acctStatusWeeklyRaw = a.customer_account_status_distribution_weekly?.data ?? [];
        // customer_account_status_distribution_monthly
        const acctStatusMonthlyRaw = a.customer_account_status_distribution_monthly?.data ?? [];

        // Aggregate weekly account status by status
        const acctStatusWeeklyMap = {};
        acctStatusWeeklyRaw.forEach((r) => {
            const s = r.account_status ?? 'Unknown';
            acctStatusWeeklyMap[s] = (acctStatusWeeklyMap[s] ?? 0) + (r.customer_count ?? 0);
        });
        // Aggregate monthly account status by status
        const acctStatusMonthlyMap = {};
        acctStatusMonthlyRaw.forEach((r) => {
            const s = r.account_status ?? 'Unknown';
            acctStatusMonthlyMap[s] = (acctStatusMonthlyMap[s] ?? 0) + (r.customer_count ?? 0);
        });

        return {
            // time-filtered
            newCustFiltered,
            cumCustFiltered,
            totalNewCustomers,
            totalCustomers,
            acctStatusMap,
            geoByCountry,
            // static
            ageGroupRaw,
            countryDistRaw,
            ageSpendingRaw,
            newVsReturnRaw,
            // additional static
            cityDistRaw,
            stateDistRaw,
            newVsReturnStateRaw,
            newVsReturnCityRaw,
            geoMonthlyRaw,
            geoAcquisitionRaw,
            // weekly / monthly variants
            newCustWeeklyRaw,
            newCustMonthlyRaw,
            cumCustWeeklyRaw,
            cumCustMonthlyRaw,
            acctStatusWeeklyMap,
            acctStatusMonthlyMap,
        };
    }, [rawData, rawGeo, clientFilter]);

    // -----------------------------------------------------------------------
    // Chart configs
    // -----------------------------------------------------------------------

    // New Customers Trend (time-filtered)
    const newCustChartData = useMemo(() => ({
        labels: (derived?.newCustFiltered ?? []).map((r) => r.grain_date),
        datasets: [{
            label: 'New Customers',
            data: (derived?.newCustFiltered ?? []).map((r) => r.new_customers ?? 0),
            borderColor: 'rgb(59,130,246)',
            backgroundColor: 'rgba(59,130,246,0.2)',
            tension: 0.4,
            fill: true,
        }],
    }), [derived]);

    // Cumulative Growth (time-filtered)
    const cumCustChartData = useMemo(() => ({
        labels: (derived?.cumCustFiltered ?? []).map((r) => r.grain_date),
        datasets: [{
            label: 'Cumulative Customers',
            data: (derived?.cumCustFiltered ?? []).map((r) => r.cumulative_customers ?? 0),
            borderColor: 'rgb(34,197,94)',
            backgroundColor: 'rgba(34,197,94,0.2)',
            tension: 0.4,
            fill: true,
        }],
    }), [derived]);

    // Account Status Distribution (time-filtered, aggregated)
    const acctStatusChartData = useMemo(() => {
        const labels = Object.keys(derived?.acctStatusMap ?? {});
        return {
            labels,
            datasets: [{
                label: 'Customers',
                data: labels.map((l) => derived?.acctStatusMap[l] ?? 0),
                backgroundColor: PALETTE,
            }],
        };
    }, [derived]);

    // Age Group Distribution (static)
    const ageGroupChartData = useMemo(() => {
        const rows = derived?.ageGroupRaw ?? [];
        return {
            labels: rows.map((r) => r.customer_age_group ?? ''),
            datasets: [{
                label: 'Customers',
                data: rows.map((r) => r.customer_count ?? 0),
                backgroundColor: PALETTE,
            }],
        };
    }, [derived]);

    // Country Distribution (static, top 10)
    const countryChartData = useMemo(() => {
        const rows = [...(derived?.countryDistRaw ?? [])]
            .sort((a, b) => (b.customer_count ?? 0) - (a.customer_count ?? 0))
            .slice(0, 10);
        return {
            labels: rows.map((r) => r.country ?? ''),
            datasets: [{
                label: 'Customers',
                data: rows.map((r) => r.customer_count ?? 0),
                backgroundColor: PALETTE,
            }],
        };
    }, [derived]);

    // Age Group Spending (static)
    const ageSpendingChartData = useMemo(() => {
        const rows = derived?.ageSpendingRaw ?? [];
        return {
            labels: rows.map((r) => r.customer_age_group ?? ''),
            datasets: [
                {
                    label: 'Avg CLV',
                    data: rows.map((r) => r.avg_clv ?? 0),
                    backgroundColor: 'rgba(59,130,246,0.8)',
                },
                {
                    label: 'Avg Order Total Spent',
                    data: rows.map((r) => r.avg_order_total_spent ?? 0),
                    backgroundColor: 'rgba(34,197,94,0.8)',
                },
            ],
        };
    }, [derived]);

    // New vs Returning by country — Doughnut per type (static)
    const newVsReturnChartData = useMemo(() => {
        const rows = derived?.newVsReturnRaw ?? [];
        // Aggregate to get new vs returning totals
        const totals = {};
        rows.forEach((r) => {
            const t = r.customer_type ?? 'Unknown';
            totals[t] = (totals[t] ?? 0) + (r.customer_count ?? 0);
        });
        const labels = Object.keys(totals);
        return {
            labels,
            datasets: [{
                data: labels.map((l) => totals[l]),
                backgroundColor: PALETTE,
            }],
        };
    }, [derived]);

    // Geo by country (time-filtered)
    const geoChartData = useMemo(() => {
        const entries = Object.entries(derived?.geoByCountry ?? {})
            .sort((a, b) => b[1] - a[1])
            .slice(0, 10);
        return {
            labels: entries.map(([c]) => c),
            datasets: [{
                label: 'New Customers',
                data: entries.map(([, v]) => v),
                backgroundColor: PALETTE,
            }],
        };
    }, [derived]);

    // -----------------------------------------------------------------------
    // Visibility
    // -----------------------------------------------------------------------

    const hasData = useMemo(() => {
        if (!derived) return false;
        return (
            derived.totalCustomers > 0 ||
            derived.totalNewCustomers > 0 ||
            derived.ageGroupRaw.length > 0 ||
            derived.countryDistRaw.length > 0
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
                <p className="text-gray-500 text-base">Loading customer overview…</p>
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

            {/* Static-data notice */}
            {isFiltered && (
                <p className="mb-4 text-xs text-gray-400 italic">
                    * Age group, country/state/city distributions, and new-vs-returning breakdowns are
                    all-time static aggregates and do not change with the date filter. Customer counts
                    and new-customer trends reflect the selected period.
                </p>
            )}

            {/* KPI Cards */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-8">
                {(derived?.totalCustomers ?? 0) > 0 && (
                    <KPICard
                        icon="pi-users" iconBg="bg-blue-50" iconColor="text-blue-500"
                        value={fmt.number(derived.totalCustomers)}
                        label="Total Customers"
                    />
                )}
                {(derived?.totalNewCustomers ?? 0) > 0 && (
                    <KPICard
                        icon="pi-user-plus" iconBg="bg-green-50" iconColor="text-green-500"
                        value={fmt.number(derived.totalNewCustomers)}
                        label={`New Customers${isFiltered ? ' (Period)' : ''}`}
                    />
                )}
                {(derived?.countryDistRaw?.length ?? 0) > 0 && (
                    <KPICard
                        icon="pi-globe" iconBg="bg-purple-50" iconColor="text-purple-500"
                        value={fmt.number(derived.countryDistRaw.length)}
                        label="Countries Served *"
                    />
                )}
                {(derived?.ageGroupRaw?.length ?? 0) > 0 && (
                    <KPICard
                        icon="pi-chart-bar" iconBg="bg-orange-50" iconColor="text-orange-500"
                        value={fmt.number(derived.ageGroupRaw.length)}
                        label="Age Groups Tracked *"
                    />
                )}
            </div>

            {/* Charts — Time-filtered */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
                {(derived?.newCustFiltered?.length ?? 0) > 0 && (
                    <div className="col-span-1 lg:col-span-2">
                        <ChartWrapper title="New Customers Trend" showUpdateBadge={false}>
                            <div className="h-[280px]">
                                <Line data={newCustChartData} options={defaultLineOpts('New Customers per Day')} />
                            </div>
                        </ChartWrapper>
                    </div>
                )}

                {(derived?.cumCustFiltered?.length ?? 0) > 0 && (
                    <div className="col-span-1 lg:col-span-2">
                        <ChartWrapper title="Cumulative Customer Growth" showUpdateBadge={false}>
                            <div className="h-[280px]">
                                <Line data={cumCustChartData} options={defaultLineOpts('Cumulative Customers')} />
                            </div>
                        </ChartWrapper>
                    </div>
                )}

                {Object.keys(derived?.acctStatusMap ?? {}).length > 0 && (
                    <ChartWrapper title="Account Status Distribution" showUpdateBadge={false}>
                        <div className="h-[280px]">
                            <Bar data={acctStatusChartData} options={defaultBarOpts('Customers by Account Status')} />
                        </div>
                    </ChartWrapper>
                )}

                {Object.keys(derived?.geoByCountry ?? {}).length > 0 && (
                    <ChartWrapper title="New Customer Geo Acquisition" showUpdateBadge={false}>
                        <div className="h-[280px]">
                            <Bar data={geoChartData} options={defaultBarOpts('New Customers by Country (period)')} />
                        </div>
                    </ChartWrapper>
                )}
            </div>

            {/* Charts — Static aggregates */}
            <div className="mb-3">
                <p className="text-xs font-semibold text-gray-400 uppercase tracking-widest">
                    Static Aggregates — these reflect all-time totals and are not affected by the date filter
                </p>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
                {(derived?.ageGroupRaw?.length ?? 0) > 0 && (
                    <ChartWrapper title="Customer Age Group Distribution *" showUpdateBadge={false}>
                        <div className="h-[280px]">
                            <Bar data={ageGroupChartData} options={defaultBarOpts('Customers by Age Group')} />
                        </div>
                    </ChartWrapper>
                )}

                {(derived?.countryDistRaw?.length ?? 0) > 0 && (
                    <ChartWrapper title="Top 10 Countries by Customer Count *" showUpdateBadge={false}>
                        <div className="h-[280px]">
                            <Bar data={countryChartData} options={defaultBarOpts('Customers by Country', true)} />
                        </div>
                    </ChartWrapper>
                )}

                {(derived?.newVsReturnRaw?.length ?? 0) > 0 && (
                    <ChartWrapper title="New vs Returning Customers *" showUpdateBadge={false}>
                        <div className="h-[280px]">
                            <Doughnut data={newVsReturnChartData} options={defaultDoughnutOpts('New vs Returning')} />
                        </div>
                    </ChartWrapper>
                )}

                {(derived?.ageSpendingRaw?.length ?? 0) > 0 && (
                    <ChartWrapper title="Avg CLV &amp; Spend by Age Group *" showUpdateBadge={false}>
                        <div className="h-[280px]">
                            <Bar
                                data={ageSpendingChartData}
                                options={{
                                    ...defaultBarOpts('Avg CLV & Spend by Age Group'),
                                    plugins: {
                                        ...defaultBarOpts().plugins,
                                        legend: { display: true, position: 'top' },
                                    },
                                    scales: { y: { beginAtZero: true, ticks: { callback: (v) => '$' + v.toLocaleString() } } },
                                }}
                            />
                        </div>
                    </ChartWrapper>
                )}
            </div>

            {/* Age Group Spending Detail Table (static) */}
            {(derived?.ageSpendingRaw?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">
                            Age Group Spending Detail *
                        </h3>
                        <DataTable
                            value={derived.ageSpendingRaw}
                            paginator rows={10}
                            stripedRows
                            size="small"
                            className="text-sm"
                        >
                            <Column field="customer_age_group" header="Age Group" sortable />
                            <Column field="customer_count" header="Customers" sortable body={(r) => fmt.number(r.customer_count)} />
                            <Column field="avg_clv" header="Avg CLV" sortable body={(r) => fmt.currency(r.avg_clv)} />
                            <Column field="avg_order_total_spent" header="Avg Order Spent" sortable body={(r) => fmt.currency(r.avg_order_total_spent)} />
                            <Column field="total_spent" header="Total Spent" sortable body={(r) => fmt.currency(r.total_spent)} />
                            <Column field="total_revenue_age_group" header="Total Revenue" sortable body={(r) => fmt.currency(r.total_revenue_age_group)} />
                        </DataTable>
                    </div>
                </Card>
            )}

            {/* Country Distribution Table (static) */}
            {(derived?.countryDistRaw?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">
                            Customer Country Distribution *
                        </h3>
                        <DataTable
                            value={[...derived.countryDistRaw].sort((a, b) => (b.customer_count ?? 0) - (a.customer_count ?? 0))}
                            paginator rows={10}
                            stripedRows
                            size="small"
                            className="text-sm"
                        >
                            <Column field="country" header="Country" sortable />
                            <Column field="customer_count" header="Customers" sortable body={(r) => fmt.number(r.customer_count)} />
                        </DataTable>
                    </div>
                </Card>
            )}

            {/* State Distribution Table (static) */}
            {(derived?.stateDistRaw?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">
                            Customer State/Province Distribution *
                        </h3>
                        <DataTable
                            value={[...derived.stateDistRaw].sort((a, b) => (b.customer_count ?? 0) - (a.customer_count ?? 0))}
                            paginator rows={10} stripedRows size="small" className="text-sm"
                        >
                            <Column field="country" header="Country" sortable />
                            <Column field="state_province" header="State/Province" sortable />
                            <Column field="customer_count" header="Customers" sortable body={(r) => fmt.number(r.customer_count)} />
                        </DataTable>
                    </div>
                </Card>
            )}

            {/* City Distribution Table (static) */}
            {(derived?.cityDistRaw?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">
                            Customer City Distribution *
                        </h3>
                        <DataTable
                            value={[...derived.cityDistRaw].sort((a, b) => (b.customer_count ?? 0) - (a.customer_count ?? 0))}
                            paginator rows={10} stripedRows size="small" className="text-sm"
                        >
                            <Column field="country" header="Country" sortable />
                            <Column field="state_province" header="State/Province" sortable />
                            <Column field="city" header="City" sortable />
                            <Column field="customer_count" header="Customers" sortable body={(r) => fmt.number(r.customer_count)} />
                        </DataTable>
                    </div>
                </Card>
            )}

            {/* New vs Returning by State / City (static) */}
            {(derived?.newVsReturnStateRaw?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">
                            New vs Returning Customers by State *
                        </h3>
                        <DataTable
                            value={derived.newVsReturnStateRaw}
                            paginator rows={10} stripedRows size="small" className="text-sm"
                        >
                            <Column field="country" header="Country" sortable />
                            <Column field="state_province" header="State/Province" sortable />
                            <Column field="customer_type" header="Customer Type" sortable />
                            <Column field="customer_count" header="Customers" sortable body={(r) => fmt.number(r.customer_count)} />
                        </DataTable>
                    </div>
                </Card>
            )}

            {(derived?.newVsReturnCityRaw?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">
                            New vs Returning Customers by City *
                        </h3>
                        <DataTable
                            value={derived.newVsReturnCityRaw}
                            paginator rows={10} stripedRows size="small" className="text-sm"
                        >
                            <Column field="country" header="Country" sortable />
                            <Column field="state_province" header="State/Province" sortable />
                            <Column field="city" header="City" sortable />
                            <Column field="customer_type" header="Customer Type" sortable />
                            <Column field="customer_count" header="Customers" sortable body={(r) => fmt.number(r.customer_count)} />
                        </DataTable>
                    </div>
                </Card>
            )}

            {/* Geo Acquisition Monthly (static) */}
            {(derived?.geoMonthlyRaw?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">
                            New Customer Geo Acquisition (Monthly) *
                        </h3>
                        <DataTable
                            value={[...derived.geoMonthlyRaw].sort((a, b) => (b.new_customers ?? 0) - (a.new_customers ?? 0))}
                            paginator rows={10} stripedRows size="small" className="text-sm"
                        >
                            <Column field="grain_year" header="Year" sortable />
                            <Column field="grain_month" header="Month" sortable />
                            <Column field="country" header="Country" sortable />
                            <Column field="state_province" header="State/Province" sortable />
                            <Column field="city" header="City" sortable />
                            <Column field="new_customers" header="New Customers" sortable body={(r) => fmt.number(r.new_customers)} />
                        </DataTable>
                    </div>
                </Card>
            )}

            {/* Geographic Acquisition (geo_analytics) (static) */}
            {(derived?.geoAcquisitionRaw?.length ?? 0) > 0 && (
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm mb-8">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b-2 border-gray-200">
                            Geographic Customer Acquisition *
                        </h3>
                        <DataTable
                            value={[...derived.geoAcquisitionRaw].sort((a, b) => (b.new_customers ?? 0) - (a.new_customers ?? 0))}
                            paginator rows={10} stripedRows size="small" className="text-sm"
                        >
                            <Column field="country" header="Country" sortable />
                            <Column field="state_province" header="State/Province" sortable />
                            <Column field="city" header="City" sortable />
                            <Column field="new_customers" header="New Customers" sortable body={(r) => fmt.number(r.new_customers)} />
                        </DataTable>
                    </div>
                </Card>
            )}

            {/* Weekly / Monthly Grain Variants */}
            {(derived?.newCustWeeklyRaw?.length ?? 0) > 0 && (
                <ChartWrapper title="New Customers (Weekly Trend) *" showUpdateBadge={false}>
                    <div className="h-[280px] mb-8">
                        <Bar
                            data={{
                                labels: derived.newCustWeeklyRaw.map((r) => `${r.grain_year}-W${String(r.grain_week).padStart(2,'0')}`),
                                datasets: [{ label: 'New Customers', data: derived.newCustWeeklyRaw.map((r) => r.new_customers ?? 0), backgroundColor: 'rgba(59,130,246,0.8)' }],
                            }}
                            options={defaultBarOpts('New Customers per Week')}
                        />
                    </div>
                </ChartWrapper>
            )}

            {(derived?.newCustMonthlyRaw?.length ?? 0) > 0 && (
                <ChartWrapper title="New Customers (Monthly Trend) *" showUpdateBadge={false}>
                    <div className="h-[280px] mb-8">
                        <Bar
                            data={{
                                labels: derived.newCustMonthlyRaw.map((r) => `${r.grain_year}-${String(r.grain_month).padStart(2,'0')}`),
                                datasets: [{ label: 'New Customers', data: derived.newCustMonthlyRaw.map((r) => r.new_customers ?? 0), backgroundColor: 'rgba(34,197,94,0.8)' }],
                            }}
                            options={defaultBarOpts('New Customers per Month')}
                        />
                    </div>
                </ChartWrapper>
            )}

            {(derived?.cumCustWeeklyRaw?.length ?? 0) > 0 && (
                <ChartWrapper title="Cumulative Customer Growth (Weekly) *" showUpdateBadge={false}>
                    <div className="h-[280px] mb-8">
                        <Line
                            data={{
                                labels: derived.cumCustWeeklyRaw.map((r) => `${r.grain_year}-W${String(r.grain_week).padStart(2,'0')}`),
                                datasets: [{ label: 'Cumulative Customers', data: derived.cumCustWeeklyRaw.map((r) => r.cumulative_customers ?? 0), borderColor: 'rgb(139,92,246)', backgroundColor: 'rgba(139,92,246,0.2)', tension: 0.4, fill: true }],
                            }}
                            options={defaultLineOpts('Cumulative Customers (Weekly)')}
                        />
                    </div>
                </ChartWrapper>
            )}

            {(derived?.cumCustMonthlyRaw?.length ?? 0) > 0 && (
                <ChartWrapper title="Cumulative Customer Growth (Monthly) *" showUpdateBadge={false}>
                    <div className="h-[280px] mb-8">
                        <Line
                            data={{
                                labels: derived.cumCustMonthlyRaw.map((r) => `${r.grain_year}-${String(r.grain_month).padStart(2,'0')}`),
                                datasets: [{ label: 'Cumulative Customers', data: derived.cumCustMonthlyRaw.map((r) => r.cumulative_customers ?? 0), borderColor: 'rgb(249,115,22)', backgroundColor: 'rgba(249,115,22,0.2)', tension: 0.4, fill: true }],
                            }}
                            options={defaultLineOpts('Cumulative Customers (Monthly)')}
                        />
                    </div>
                </ChartWrapper>
            )}

            {Object.keys(derived?.acctStatusWeeklyMap ?? {}).length > 0 && (
                <ChartWrapper title="Account Status Distribution (Weekly Aggregate) *" showUpdateBadge={false}>
                    <div className="h-[280px] mb-8">
                        <Bar
                            data={{ labels: Object.keys(derived.acctStatusWeeklyMap), datasets: [{ label: 'Customers', data: Object.values(derived.acctStatusWeeklyMap), backgroundColor: PALETTE }] }}
                            options={defaultBarOpts('Account Status (Weekly)')}
                        />
                    </div>
                </ChartWrapper>
            )}

            {Object.keys(derived?.acctStatusMonthlyMap ?? {}).length > 0 && (
                <ChartWrapper title="Account Status Distribution (Monthly Aggregate) *" showUpdateBadge={false}>
                    <div className="h-[280px] mb-8">
                        <Bar
                            data={{ labels: Object.keys(derived.acctStatusMonthlyMap), datasets: [{ label: 'Customers', data: Object.values(derived.acctStatusMonthlyMap), backgroundColor: PALETTE }] }}
                            options={defaultBarOpts('Account Status (Monthly)')}
                        />
                    </div>
                </ChartWrapper>
            )}
        </div>
    );
};

export default CustomerOverview;
