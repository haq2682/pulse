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

export default function FunnelCheckout() {
    const fmt = useFormatters();
    const { businessId } = useParams();
    const toastRef = useRef(null);
    const { pipelineStatus } = usePipelineProgress();
    const { dateRange, setDateRange, quickFilter, isFiltered, applyQuickFilter, resetFilters } = useAnalyticsDateFilter();
    const { lastUpdate } = useAnalyticsWebSocket(businessId);

    const [rawFunnel, setRawFunnel] = useState(null);
    const [loading, setLoading] = useState(true);
    const [fetchError, setFetchError] = useState(false);
    const [dataMode, setDataMode] = useState('unknown');

    // -------------------------------------------------------------------------
    // Fetch
    // -------------------------------------------------------------------------

    const buildUrl = useCallback(() => {
        const base = import.meta.env.VITE_API_URL || 'http://localhost:8000';
        const params = new URLSearchParams({ categories: 'funnel_analytics' });
        return `${base}/analytics/data/${businessId}?${params.toString()}`;
    }, [businessId]);

    const fetchData = useCallback(async () => {
        if (!businessId) return;
        try {
            setLoading(true);
            setFetchError(false);
            const res = await fetch(buildUrl());
            if (!res.ok) {
                setRawFunnel(null);
                return;
            }
            const json = await res.json();
            if (json.mode) setDataMode(json.mode);
            setRawFunnel(json.categories?.funnel_analytics ?? null);
        } catch {
            console.error('[FunnelCheckout] fetch error');
            setFetchError(true);
            setRawFunnel(null);
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
        if (!rawFunnel) return null;
        const a = rawFunnel.analytics ?? {};

        const dropoffReasons   = a.checkout_dropoff_reasons?.data                 ?? [];
        const dropoffBuckets   = a.checkout_dropoff_buckets?.data                 ?? [];
        const dropoffByDevice  = a.checkout_dropoff_by_device_and_reason?.data    ?? [];
        const deviceConv       = a.device_conversion_rates?.data                  ?? [];
        const abandonVsConv    = a.abandoned_vs_converted?.data                   ?? [];

        if (dropoffReasons.length === 0 && deviceConv.length === 0 && dropoffBuckets.length === 0) return null;

        // ---- KPIs -----------------------------------------------------------
        const totalDropoffs    = dropoffReasons.reduce((s, r) => s + (+(r.dropoff_count ?? 0)), 0);
        const avgRiskScore     = dropoffReasons.length > 0
            ? dropoffReasons.reduce((s, r) => s + (+(r.avg_abandonment_risk_score ?? 0)), 0) / dropoffReasons.length
            : 0;
        const avgConvAfterAband = dropoffReasons.length > 0
            ? dropoffReasons.reduce((s, r) => s + (+(r.conversion_after_abandonment_rate ?? 0)), 0) / dropoffReasons.length
            : 0;
        const topReason        = dropoffReasons.length > 0
            ? [...dropoffReasons].sort((a, b) => (+(b.dropoff_count ?? 0)) - (+(a.dropoff_count ?? 0)))[0]?.cart_abandonment_reason ?? '—'
            : '—';

        // ---- Dropoff reasons horizontal bar ---------------------------------
        const dropoffReasonsSorted = [...dropoffReasons]
            .sort((a, b) => (+(b.dropoff_count ?? 0)) - (+(a.dropoff_count ?? 0)));
        const dropoffReasonsBarData = {
            labels: dropoffReasonsSorted.map((r) => r.cart_abandonment_reason ?? 'Unknown'),
            datasets: [{
                label: 'Dropoff Count',
                data: dropoffReasonsSorted.map((r) => +(r.dropoff_count ?? 0)),
                backgroundColor: 'rgba(239,68,68,0.82)',
            }],
        };

        // ---- Recovery rate after abandonment bar ----------------------------
        const recoveryRateBarData = {
            labels: dropoffReasonsSorted.map((r) => r.cart_abandonment_reason ?? 'Unknown'),
            datasets: [{
                label: 'Conv. After Abandonment %',
                data: dropoffReasonsSorted.map((r) => +(r.conversion_after_abandonment_rate ?? 0).toFixed(2)),
                backgroundColor: 'rgba(34,197,94,0.82)',
            }],
        };

        // ---- Avg risk score by reason bar -----------------------------------
        const riskByReasonBarData = {
            labels: dropoffReasonsSorted.map((r) => r.cart_abandonment_reason ?? 'Unknown'),
            datasets: [{
                label: 'Avg Abandonment Risk Score',
                data: dropoffReasonsSorted.map((r) => +(r.avg_abandonment_risk_score ?? 0).toFixed(3)),
                backgroundColor: 'rgba(249,115,22,0.82)',
            }],
        };

        // ---- Dropoff buckets bar --------------------------------------------
        const dropoffBucketsBarData = dropoffBuckets.length > 0 ? {
            labels: dropoffBuckets.map((r) => r.dropoff_bucket ?? 'Unknown'),
            datasets: [{
                label: 'Dropoff Count',
                data: dropoffBuckets.map((r) => +(r.dropoff_count ?? 0)),
                backgroundColor: PALETTE,
            }],
        } : null;

        // ---- Dropoff by device doughnut -------------------------------------
        const deviceDropoffMap = {};
        dropoffByDevice.forEach((r) => {
            const d = r.device_type ?? 'Unknown';
            deviceDropoffMap[d] = (deviceDropoffMap[d] ?? 0) + (+(r.dropoff_count ?? 0));
        });
        const deviceDropoffDoughnutData = Object.keys(deviceDropoffMap).length > 0 ? {
            labels: Object.keys(deviceDropoffMap),
            datasets: [{ data: Object.values(deviceDropoffMap), backgroundColor: PALETTE }],
        } : null;

        // ---- Dropoff by device grouped bar (top devices x buckets) ----------
        const devices   = [...new Set(dropoffByDevice.map((r) => r.device_type ?? 'Unknown'))];
        const buckets   = [...new Set(dropoffByDevice.map((r) => r.dropoff_bucket ?? 'Unknown'))];
        const dropoffByDeviceBarData = (devices.length > 0 && buckets.length > 0) ? {
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

        // ---- Device conversion + abandonment rates grouped bar --------------
        const deviceConvBarData = deviceConv.length > 0 ? {
            labels: deviceConv.map((r) => r.device_used ?? 'Unknown'),
            datasets: [
                { label: 'Conversion Rate %',  data: deviceConv.map((r) => +(r.conversion_rate ?? 0).toFixed(2)),  backgroundColor: 'rgba(34,197,94,0.82)' },
                { label: 'Abandonment Rate %', data: deviceConv.map((r) => +(r.abandonment_rate ?? 0).toFixed(2)), backgroundColor: 'rgba(239,68,68,0.82)' },
            ],
        } : null;

        // ---- Device conversion rate doughnut --------------------------------
        const deviceConvDoughnutData = deviceConv.length > 0 ? {
            labels: deviceConv.map((r) => r.device_used ?? 'Unknown'),
            datasets: [{ data: deviceConv.map((r) => +(r.conversion_rate ?? 0).toFixed(2)), backgroundColor: PALETTE }],
        } : null;

        // ---- Abandoned vs Converted bar -------------------------------------
        const abandonBarData = abandonVsConv.length > 0 ? {
            labels: abandonVsConv.map((r) => r.converted ? 'Converted' : 'Abandoned'),
            datasets: [
                { label: 'Session Count',       data: abandonVsConv.map((r) => +(r.session_count ?? 0)),                          backgroundColor: 'rgba(59,130,246,0.82)' },
                { label: 'Avg Products Viewed', data: abandonVsConv.map((r) => +(r.avg_products_viewed ?? 0).toFixed(2)),          backgroundColor: 'rgba(34,197,94,0.82)' },
                { label: 'Avg Items in Cart',   data: abandonVsConv.map((r) => +(r.avg_items_in_cart ?? 0).toFixed(2)),            backgroundColor: 'rgba(249,115,22,0.82)' },
                { label: 'Avg Cart Value ($)',  data: abandonVsConv.map((r) => +(r.avg_cart_value ?? 0).toFixed(2)),               backgroundColor: 'rgba(139,92,246,0.82)' },
            ],
        } : null;

        return {
            kpis: { totalDropoffs, avgRiskScore, avgConvAfterAband, topReason },
            dropoffReasonsBarData, recoveryRateBarData, riskByReasonBarData,
            dropoffBucketsBarData, deviceDropoffDoughnutData, dropoffByDeviceBarData,
            deviceConvBarData, deviceConvDoughnutData, abandonBarData,
            dropoffReasonsSorted, deviceConv,
        };
    }, [rawFunnel]);

    // -------------------------------------------------------------------------
    // Render
    // -------------------------------------------------------------------------

    const hasData = derived !== null;

    if (loading && pipelineStatus !== 'loading') {
        return (
            <div className="flex flex-col items-center justify-center min-h-[400px] gap-4">
                <ProgressSpinner />
                <p className="text-gray-500 text-base">Loading funnel checkout data…</p>
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
                        <p className="text-gray-500 text-sm mt-1">Unable to load checkout data. Please try again later.</p>
                    </div>
                </div>
            </div>
        );
    }

    if (!hasData && !loading && pipelineStatus !== 'loading') {
        return (
            <div className="p-6 space-y-4">
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
                            : 'No data to display.'}
                    </p>
                </div>
            </div>
        );
    }
    if (!derived) return null;
    const { kpis, dropoffReasonsBarData, recoveryRateBarData, riskByReasonBarData,
            dropoffBucketsBarData, deviceDropoffDoughnutData, dropoffByDeviceBarData,
            deviceConvBarData, deviceConvDoughnutData, abandonBarData,
            dropoffReasonsSorted, deviceConv } = derived;

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
                    * Funnel analytics are static aggregates computed over all available data and do not change with the date filter.
                </p>
            )}

            {/* ── KPI Cards ──────────────────────────────────────────────── */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <KPICard icon="pi-times-circle"  iconBg="bg-red-50"    iconColor="text-red-600"    value={fmt.number(kpis.totalDropoffs)}        label="Total Dropoffs" />
                <KPICard icon="pi-exclamation-triangle" iconBg="bg-orange-50" iconColor="text-orange-600" value={fmt.decimal(kpis.avgRiskScore, 3)} label="Avg Abandon Risk" />
                <KPICard icon="pi-refresh"       iconBg="bg-green-50"  iconColor="text-green-600"  value={fmt.pct(kpis.avgConvAfterAband)}        label="Avg Recovery Rate" />
                <KPICard icon="pi-info-circle"   iconBg="bg-blue-50"   iconColor="text-blue-600"   value={kpis.topReason}                         label="Top Dropoff Reason" />
            </div>

            {/* ── Dropoff Analysis ───────────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-red-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Dropoff Analysis</h2>
                </div>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {dropoffReasonsBarData.labels.length > 0 && (
                        <ChartWrapper title="Checkout Dropoff by Reason" height={340}>
                            <Bar data={dropoffReasonsBarData} options={barOpts(true)} />
                        </ChartWrapper>
                    )}
                    {recoveryRateBarData.labels.length > 0 && (
                        <ChartWrapper title="Recovery Rate After Abandonment by Reason" height={340}>
                            <Bar data={recoveryRateBarData} options={barOpts(true)} />
                        </ChartWrapper>
                    )}
                    {riskByReasonBarData.labels.length > 0 && (
                        <ChartWrapper title="Avg Abandonment Risk Score by Reason" height={340}>
                            <Bar data={riskByReasonBarData} options={barOpts(true)} />
                        </ChartWrapper>
                    )}
                    {dropoffBucketsBarData && (
                        <ChartWrapper title="Dropoff Volume by Stage Bucket" height={340}>
                            <Bar data={dropoffBucketsBarData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                </div>
            </section>

            {/* ── Device Analysis ────────────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-blue-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Device Analysis</h2>
                </div>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {deviceDropoffDoughnutData && (
                        <ChartWrapper title="Dropoffs by Device" height={280}>
                            <Doughnut data={deviceDropoffDoughnutData} options={doughnutOpts()} />
                        </ChartWrapper>
                    )}
                    {dropoffByDeviceBarData && (
                        <ChartWrapper title="Dropoff Stage by Device" height={340}>
                            <Bar data={dropoffByDeviceBarData} options={groupedBarOpts()} />
                        </ChartWrapper>
                    )}
                </div>
            </section>

            {/* ── Recovery Metrics ───────────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-green-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Recovery Metrics</h2>
                </div>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {deviceConvBarData && (
                        <ChartWrapper title="Conversion vs Abandonment Rate by Device" height={340}>
                            <Bar data={deviceConvBarData} options={groupedBarOpts()} />
                        </ChartWrapper>
                    )}
                    {deviceConvDoughnutData && (
                        <ChartWrapper title="Conversion Rate Share by Device" height={280}>
                            <Doughnut data={deviceConvDoughnutData} options={doughnutOpts()} />
                        </ChartWrapper>
                    )}
                </div>

                {/* Abandoned vs Converted grouped bar */}
                {abandonBarData && (
                    <ChartWrapper title="Abandoned vs Converted Session Behaviour" height={340}>
                        <Bar data={abandonBarData} options={groupedBarOpts()} />
                    </ChartWrapper>
                )}
            </section>

            {/* ── Performance Tables ─────────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-purple-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Performance Tables</h2>
                </div>

                {/* Dropoff Reasons Table */}
                {dropoffReasonsSorted.length > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">Checkout Dropoff Reasons Detail</h3>
                            <DataTable value={dropoffReasonsSorted} scrollable stripedRows emptyMessage="No data" className="text-sm">
                                <Column field="cart_abandonment_reason"            header="Reason"                sortable />
                                <Column field="dropoff_count"                      header="Dropoff Count"         sortable body={(r) => fmt.number(r.dropoff_count)} />
                                <Column field="avg_abandonment_risk_score"         header="Avg Risk Score"        sortable body={(r) => fmt.decimal(r.avg_abandonment_risk_score, 3)} />
                                <Column field="conversion_after_abandonment_rate"  header="Recovery Rate %"       sortable body={(r) => fmt.pct(r.conversion_after_abandonment_rate)} />
                            </DataTable>
                        </div>
                    </Card>
                )}

                {/* Device Conversion Rates Table */}
                {deviceConv.length > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">Device Conversion Rates</h3>
                            <DataTable value={deviceConv} scrollable stripedRows emptyMessage="No data" className="text-sm">
                                <Column field="device_used"       header="Device"             sortable />
                                <Column field="carts"             header="Total Carts"        sortable body={(r) => fmt.number(r.carts)} />
                                <Column field="converted_carts"   header="Converted"          sortable body={(r) => fmt.number(r.converted_carts)} />
                                <Column field="abandoned_carts"   header="Abandoned"          sortable body={(r) => fmt.number(r.abandoned_carts)} />
                                <Column field="conversion_rate"   header="Conv. Rate %"       sortable body={(r) => (
                                    <Tag value={fmt.pct(r.conversion_rate)}
                                        severity={(+(r.conversion_rate ?? 0)) >= 50 ? 'success' : (+(r.conversion_rate ?? 0)) >= 25 ? 'warning' : 'danger'} />
                                )} />
                                <Column field="abandonment_rate"  header="Abandon Rate %"     sortable body={(r) => (
                                    <Tag value={fmt.pct(r.abandonment_rate)}
                                        severity={(+(r.abandonment_rate ?? 0)) >= 75 ? 'danger' : (+(r.abandonment_rate ?? 0)) >= 50 ? 'warning' : 'success'} />
                                )} />
                            </DataTable>
                        </div>
                    </Card>
                )}
            </section>
        </div>
    );
}
