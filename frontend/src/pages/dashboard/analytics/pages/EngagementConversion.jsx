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
        y: { beginAtZero: true, grid: { color: 'rgba(0,0,0,0.05)' } },
    },
});

const groupedBarOpts = () => ({
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { position: 'top' }, title: { display: false } },
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

export default function EngagementConversion() {
    const fmt = useFormatters();
    const { businessId } = useParams();
    const toastRef = useRef(null);
    const { pipelineStatus } = usePipelineProgress();
    const { dateRange, setDateRange, quickFilter, isFiltered, applyQuickFilter, resetFilters } = useAnalyticsDateFilter();
    const { lastUpdate } = useAnalyticsWebSocket(businessId);

    const [rawCustomer, setRawCustomer] = useState(null);
    const [rawFunnel, setRawFunnel] = useState(null);
    const [loading, setLoading] = useState(true);
    const [fetchError, setFetchError] = useState(false);
    const [dataMode, setDataMode] = useState('unknown');

    // -------------------------------------------------------------------------
    // Fetch
    // -------------------------------------------------------------------------

    const buildUrl = useCallback(() => {
        const base = import.meta.env.VITE_API_URL || 'http://localhost:8000';
        const params = new URLSearchParams({ categories: 'customer_analytics,funnel_analytics' });
        return `${base}/analytics/data/${businessId}?${params.toString()}`;
    }, [businessId]);

    const fetchData = useCallback(async () => {
        if (!businessId) return;
        try {
            setLoading(true);
            setFetchError(false);
            const res = await fetch(buildUrl());
            if (!res.ok) {
                setRawCustomer(null);
                setRawFunnel(null);
                return;
            }
            const json = await res.json();
            if (json.mode) setDataMode(json.mode);
            setRawCustomer(json.categories?.customer_analytics ?? null);
            setRawFunnel(json.categories?.funnel_analytics ?? null);
        } catch {
            console.error('[EngagementConversion] fetch error');
            setFetchError(true);
            setRawCustomer(null);
            setRawFunnel(null);
        } finally {
            setLoading(false);
        }
    }, [buildUrl, businessId]);

    useEffect(() => { fetchData(); }, [businessId]); // eslint-disable-line
    useEffect(() => {
        if (!lastUpdate) return;
        fetchData();
        toastRef.current?.show({ severity: 'info', summary: 'Data Updated', detail: 'Analytics pipeline completed — refreshing conversion data.', life: 3000 });
    }, [lastUpdate]); // eslint-disable-line

    // -------------------------------------------------------------------------
    // Derived data
    // -------------------------------------------------------------------------

    const derived = useMemo(() => {
        const ca = rawCustomer?.analytics ?? {};
        const fa = rawFunnel?.analytics   ?? {};

        const cartBehavior      = ca.cart_behavior_summary?.data?.[0]      ?? null;
        const highAbandoners    = ca.high_value_abandoners?.data            ?? [];

        const abandonedVsConv   = fa.abandoned_vs_converted?.data          ?? [];
        const dropoffReasons    = fa.checkout_dropoff_reasons?.data        ?? [];
        const dropoffBuckets    = fa.checkout_dropoff_buckets?.data        ?? [];
        const dropoffByDevice   = fa.checkout_dropoff_by_device_and_reason?.data ?? [];
        const deviceConvRates   = fa.device_conversion_rates?.data         ?? [];

        if (!cartBehavior && abandonedVsConv.length === 0 && deviceConvRates.length === 0) return null;

        // ---- KPIs -----------------------------------------------------------
        const totalCarts      = +(cartBehavior?.total_carts_created ?? 0);
        const abandRate       = +(cartBehavior?.avg_cart_abandonment_rate ?? 0);
        const abandonedVal    = +(cartBehavior?.total_abandoned_value ?? 0);
        const avgTimeInCart   = +(cartBehavior?.avg_time_in_cart_days ?? 0);

        // ---- Abandoned vs converted session comparison ----------------------
        const abandVsConvGrouped = abandonedVsConv.length > 0 ? {
            labels: abandonedVsConv.map((r) => r.converted ? 'Converted' : 'Abandoned'),
            datasets: [
                { label: 'Session Count',      data: abandonedVsConv.map((r) => +(r.session_count ?? 0)),      backgroundColor: 'rgba(59,130,246,0.82)' },
                { label: 'Avg Products Viewed',data: abandonedVsConv.map((r) => +(r.avg_products_viewed ?? 0).toFixed(2)), backgroundColor: 'rgba(249,115,22,0.82)' },
                { label: 'Avg Cart Value ($)', data: abandonedVsConv.map((r) => +(r.avg_cart_value ?? 0).toFixed(2)),      backgroundColor: 'rgba(34,197,94,0.82)' },
            ],
        } : null;

        // ---- Abandoned vs converted doughnut (by total cart value) ----------
        const abandVsConvDoughnut = abandonedVsConv.length > 0 ? {
            labels: abandonedVsConv.map((r) => r.converted ? 'Converted' : 'Abandoned'),
            datasets: [{
                data: abandonedVsConv.map((r) => +(r.total_cart_value ?? 0).toFixed(0)),
                backgroundColor: ['rgba(34,197,94,0.82)', 'rgba(239,68,68,0.82)'],
                borderWidth: 2,
            }],
        } : null;

        // ---- Checkout dropoff reasons bar -----------------------------------
        const dropoffReasonsBar = dropoffReasons.length > 0 ? {
            labels: dropoffReasons.map((r) => r.cart_abandonment_reason ?? 'Unknown'),
            datasets: [{
                label: 'Drop-off Count',
                data: dropoffReasons.map((r) => +(r.dropoff_count ?? 0)),
                backgroundColor: PALETTE,
            }],
        } : null;

        // ---- Dropoff buckets bar --------------------------------------------
        const dropoffBucketsBar = dropoffBuckets.length > 0 ? {
            labels: dropoffBuckets.map((r) => r.dropoff_bucket ?? 'Unknown'),
            datasets: [{
                label: 'Drop-off Count',
                data: dropoffBuckets.map((r) => +(r.dropoff_count ?? 0)),
                backgroundColor: dropoffBuckets.map((_, i) => PALETTE[i % PALETTE.length]),
            }],
        } : null;

        // ---- Avg abandonment risk score by reason (horizontal bar) ----------
        const riskByReasonBar = dropoffReasons.length > 0 ? {
            labels: dropoffReasons.map((r) => r.cart_abandonment_reason ?? 'Unknown'),
            datasets: [{
                label: 'Avg Abandonment Risk Score',
                data: dropoffReasons.map((r) => +(r.avg_abandonment_risk_score ?? 0).toFixed(3)),
                backgroundColor: dropoffReasons.map((r) => {
                    const v = +(r.avg_abandonment_risk_score ?? 0);
                    if (v >= 0.7) return 'rgba(239,68,68,0.82)';
                    if (v >= 0.4) return 'rgba(234,179,8,0.82)';
                    return 'rgba(34,197,94,0.82)';
                }),
            }],
        } : null;

        // ---- Dropoff by device grouped bar ----------------------------------
        const devices = [...new Set(dropoffByDevice.map((r) => r.device_type ?? 'Unknown'))];
        const buckets = [...new Set(dropoffByDevice.map((r) => r.dropoff_bucket ?? 'Unknown'))];
        const dropoffDeviceGrouped = (devices.length > 0 && buckets.length > 0) ? {
            labels: devices,
            datasets: buckets.map((bucket, i) => ({
                label: bucket,
                data: devices.map((dev) => {
                    const row = dropoffByDevice.find((r) => r.device_type === dev && r.dropoff_bucket === bucket);
                    return row ? +(row.dropoff_count ?? 0) : 0;
                }),
                backgroundColor: PALETTE[i % PALETTE.length],
            })),
        } : null;

        // ---- Device conversion rates grouped bar ----------------------------
        const deviceConvGrouped = deviceConvRates.length > 0 ? {
            labels: deviceConvRates.map((r) => r.device_used ?? 'Unknown'),
            datasets: [
                { label: 'Total Carts',      data: deviceConvRates.map((r) => +(r.carts ?? 0)),            backgroundColor: 'rgba(59,130,246,0.82)' },
                { label: 'Converted Carts',  data: deviceConvRates.map((r) => +(r.converted_carts ?? 0)),  backgroundColor: 'rgba(34,197,94,0.82)' },
                { label: 'Abandoned Carts',  data: deviceConvRates.map((r) => +(r.abandoned_carts ?? 0)),  backgroundColor: 'rgba(239,68,68,0.82)' },
            ],
        } : null;

        // ---- Conversion rate vs abandonment rate by device (grouped bar) ----
        const deviceRateGrouped = deviceConvRates.length > 0 ? {
            labels: deviceConvRates.map((r) => r.device_used ?? 'Unknown'),
            datasets: [
                {
                    label: 'Conversion Rate %',
                    data: deviceConvRates.map((r) => +(r.conversion_rate ?? 0).toFixed(1)),
                    backgroundColor: 'rgba(34,197,94,0.82)',
                },
                {
                    label: 'Abandonment Rate %',
                    data: deviceConvRates.map((r) => +(r.abandonment_rate ?? 0).toFixed(1)),
                    backgroundColor: 'rgba(239,68,68,0.82)',
                },
            ],
        } : null;

        return {
            kpis: { totalCarts, abandRate, abandonedVal, avgTimeInCart },
            abandVsConvGrouped, abandVsConvDoughnut,
            dropoffReasonsBar, dropoffBucketsBar, riskByReasonBar,
            dropoffDeviceGrouped, deviceConvGrouped, deviceRateGrouped,
            cartBehavior, highAbandoners, deviceConvRates,
        };
    }, [rawCustomer, rawFunnel]);

    const hasData = derived !== null;

    // -------------------------------------------------------------------------
    // Render states
    // -------------------------------------------------------------------------

    if (loading && pipelineStatus !== 'loading') {
        return (
            <div className="flex flex-col items-center justify-center min-h-[400px] gap-4">
                <ProgressSpinner />
                <p className="text-gray-500 text-base">Loading conversion analytics…</p>
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
                        <p className="text-gray-500 text-sm mt-1">Unable to load conversion data. Please try again later.</p>
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
                            : 'No conversion data to display.'}
                    </p>
                </div>
            </div>
        );
    }

    if (!derived) return null;
    const {
        kpis, abandVsConvGrouped, abandVsConvDoughnut,
        dropoffReasonsBar, dropoffBucketsBar, riskByReasonBar,
        dropoffDeviceGrouped, deviceConvGrouped, deviceRateGrouped,
        cartBehavior, highAbandoners, deviceConvRates,
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
                    * Engagement conversion analytics are static aggregates computed over all available data and do not change with the date filter.
                </p>
            )}

            {/* ── KPI Cards ──────────────────────────────────────────────── */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <KPICard
                    icon="pi-shopping-cart" iconBg="bg-blue-100" iconColor="text-blue-600"
                    value={fmt.number(kpis.totalCarts)}
                    label="Total Carts Created"
                />
                <KPICard
                    icon="pi-times-circle" iconBg="bg-red-100" iconColor="text-red-600"
                    value={fmt.pct(kpis.abandRate)}
                    label="Avg Cart Abandonment Rate"
                />
                <KPICard
                    icon="pi-dollar" iconBg="bg-amber-100" iconColor="text-amber-600"
                    value={fmt.currency(kpis.abandonedVal)}
                    label="Total Abandoned Cart Value"
                />
                <KPICard
                    icon="pi-clock" iconBg="bg-purple-100" iconColor="text-purple-600"
                    value={`${fmt.decimal(kpis.avgTimeInCart, 1)} days`}
                    label="Avg Time in Cart"
                />
            </div>

            {/* ── Cart Behavior Summary ─────────────────────────────────── */}
            {cartBehavior && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-blue-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Cart Behavior Summary</h2>
                    </div>
                    <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
                        {[
                            { label: 'Total Carts Created',       value: fmt.number(cartBehavior.total_carts_created) },
                            { label: 'Abandoned Carts',            value: fmt.number(cartBehavior.total_abandoned_carts) },
                            { label: 'Purchased Carts',            value: fmt.number(cartBehavior.total_purchased_carts) },
                            { label: 'Avg Abandonment Rate',       value: fmt.pct(cartBehavior.avg_cart_abandonment_rate) },
                            { label: 'Total Abandoned Value',      value: fmt.currency(cartBehavior.total_abandoned_value) },
                            { label: 'Avg Time in Cart',           value: `${fmt.decimal(cartBehavior.avg_time_in_cart_days, 1)} days` },
                        ].map(({ label, value }) => (
                            <Card key={label} className="bg-white border border-gray-200 rounded-xl shadow-sm">
                                <div className="p-6 text-center">
                                    <p className="text-2xl font-bold text-gray-900 mb-1">{value}</p>
                                    <p className="text-xs text-gray-500 uppercase tracking-wider">{label}</p>
                                </div>
                            </Card>
                        ))}
                    </div>
                </section>
            )}

            {/* ── Abandoned vs Converted ────────────────────────────────── */}
            {(abandVsConvGrouped || abandVsConvDoughnut) && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-green-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Abandoned vs Converted Sessions</h2>
                    </div>
                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                        {abandVsConvDoughnut && (
                            <ChartWrapper title="Total Cart Value: Abandoned vs Converted" height={280}>
                                <Doughnut data={abandVsConvDoughnut} options={doughnutOpts()} />
                            </ChartWrapper>
                        )}
                        {abandVsConvGrouped && (
                            <ChartWrapper title="Session Activity: Abandoned vs Converted" height={300}>
                                <Bar data={abandVsConvGrouped} options={groupedBarOpts()} />
                            </ChartWrapper>
                        )}
                    </div>
                </section>
            )}

            {/* ── Checkout Drop-off Analysis ────────────────────────────── */}
            {(dropoffReasonsBar || dropoffBucketsBar || riskByReasonBar) && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-red-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Checkout Drop-off Analysis</h2>
                    </div>
                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                        {dropoffReasonsBar && (
                            <ChartWrapper title="Drop-off Count by Abandonment Reason" height={320}>
                                <Bar data={dropoffReasonsBar} options={barOpts()} />
                            </ChartWrapper>
                        )}
                        {riskByReasonBar && (
                            <ChartWrapper title="Avg Abandonment Risk Score by Reason (color = risk level)" height={320}>
                                <Bar data={riskByReasonBar} options={barOpts()} />
                            </ChartWrapper>
                        )}
                        {dropoffBucketsBar && (
                            <ChartWrapper title="Drop-off Count by Funnel Stage Bucket" height={280}>
                                <Bar data={dropoffBucketsBar} options={barOpts()} />
                            </ChartWrapper>
                        )}
                        {dropoffDeviceGrouped && (
                            <ChartWrapper title="Drop-offs by Device and Funnel Stage" height={320}>
                                <Bar data={dropoffDeviceGrouped} options={groupedBarOpts()} />
                            </ChartWrapper>
                        )}
                    </div>
                </section>
            )}

            {/* ── Device Conversion Rates ───────────────────────────────── */}
            {(deviceConvGrouped || deviceRateGrouped) && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-purple-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Device Conversion Rates</h2>
                    </div>
                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                        {deviceConvGrouped && (
                            <ChartWrapper title="Cart Outcomes by Device" height={300}>
                                <Bar data={deviceConvGrouped} options={groupedBarOpts()} />
                            </ChartWrapper>
                        )}
                        {deviceRateGrouped && (
                            <ChartWrapper title="Conversion Rate % vs Abandonment Rate % by Device" height={280}>
                                <Bar data={deviceRateGrouped} options={groupedBarOpts()} />
                            </ChartWrapper>
                        )}
                    </div>
                    {/* Device table */}
                    {deviceConvRates.length > 0 && (
                        <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                            <div className="p-6">
                                <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                    Cart Conversion by Device
                                </h3>
                                <DataTable value={deviceConvRates} scrollable stripedRows emptyMessage="No data" className="text-sm">
                                    <Column field="device_used"       header="Device"           sortable />
                                    <Column field="carts"             header="Total Carts"      sortable body={(r) => fmt.number(r.carts)} />
                                    <Column field="converted_carts"   header="Converted"        sortable body={(r) => fmt.number(r.converted_carts)} />
                                    <Column field="abandoned_carts"   header="Abandoned"        sortable body={(r) => fmt.number(r.abandoned_carts)} />
                                    <Column field="conversion_rate"   header="Conversion Rate"  sortable body={(r) => fmt.pct(r.conversion_rate)} />
                                    <Column field="abandonment_rate"  header="Abandonment Rate" sortable body={(r) => fmt.pct(r.abandonment_rate)} />
                                </DataTable>
                            </div>
                        </Card>
                    )}
                </section>
            )}

            {/* ── High Value Abandoners ─────────────────────────────────── */}
            {highAbandoners.length > 0 && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-amber-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">High-Value Abandoners</h2>
                    </div>
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                Customers with High Abandoned Cart Value
                            </h3>
                            <DataTable
                                value={[...highAbandoners].sort((a, b) => (+(b.total_abandoned_value ?? 0)) - (+(a.total_abandoned_value ?? 0)))}
                                paginator rows={15}
                                scrollable stripedRows emptyMessage="No data" className="text-sm">
                                <Column field="customer_id"           header="Customer ID"           sortable />
                                <Column field="total_abandoned_carts" header="Abandoned Carts"       sortable body={(r) => fmt.number(r.total_abandoned_carts)} />
                                <Column field="total_abandoned_value" header="Abandoned Value"       sortable body={(r) => fmt.currency(r.total_abandoned_value)} />
                                <Column field="cart_abandonment_rate" header="Abandonment Rate"      sortable body={(r) => fmt.pct(r.cart_abandonment_rate)} />
                                <Column field="total_revenue"         header="Total Revenue"         sortable body={(r) => fmt.currency(r.total_revenue)} />
                                <Column field="customer_lifetime_value" header="Customer LTV"        sortable body={(r) => fmt.currency(r.customer_lifetime_value)} />
                            </DataTable>
                        </div>
                    </Card>
                </section>
            )}
        </div>
    );
}
