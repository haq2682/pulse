import React, { useState, useEffect, useRef, useMemo, useCallback } from 'react';
import { useParams } from 'react-router-dom';
import { Card } from 'primereact/card';
import { ProgressSpinner } from 'primereact/progressspinner';
import { Toast } from 'primereact/toast';
import { DataTable } from 'primereact/datatable';
import { Column } from 'primereact/column';
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

export default function ReviewsSentiment() {
    const fmt = useFormatters();
    const { businessId } = useParams();
    const toastRef = useRef(null);
    const { pipelineStatus } = usePipelineProgress();
    const { dateRange, setDateRange, quickFilter, isFiltered, applyQuickFilter, resetFilters } = useAnalyticsDateFilter();
    const { lastUpdate } = useAnalyticsWebSocket(businessId);

    const [rawReview, setRawReview] = useState(null);
    const [loading, setLoading] = useState(true);
    const [fetchError, setFetchError] = useState(false);
    const [dataMode, setDataMode] = useState('unknown');

    // -------------------------------------------------------------------------
    // Fetch
    // -------------------------------------------------------------------------

    const buildUrl = useCallback(() => {
        const base = import.meta.env.VITE_API_URL || 'http://localhost:8000';
        const params = new URLSearchParams({ categories: 'review_analytics' });
        return `${base}/analytics/data/${businessId}?${params.toString()}`;
    }, [businessId]);

    const fetchData = useCallback(async () => {
        if (!businessId) return;
        try {
            setLoading(true);
            setFetchError(false);
            const res = await fetch(buildUrl());
            if (!res.ok) {
                setRawReview(null);
                return;
            }
            const json = await res.json();
            if (json.mode) setDataMode(json.mode);
            setRawReview(json.categories?.review_analytics ?? null);
        } catch {
            console.error('[ReviewsSentiment] fetch error');
            setFetchError(true);
            setRawReview(null);
        } finally {
            setLoading(false);
        }
    }, [buildUrl, businessId]);

    useEffect(() => { fetchData(); }, [businessId]); // eslint-disable-line
    useEffect(() => {
        if (!lastUpdate) return;
        fetchData();
        toastRef.current?.show({ severity: 'info', summary: 'Data Updated', detail: 'Analytics pipeline completed — refreshing sentiment data.', life: 3000 });
    }, [lastUpdate]); // eslint-disable-line

    // -------------------------------------------------------------------------
    // Derived data
    // -------------------------------------------------------------------------

    const derived = useMemo(() => {
        if (!rawReview) return null;
        const a = rawReview.analytics ?? {};

        const sentByCat    = a.sentiment_by_category?.data          ?? [];
        const velMonthly   = a.review_velocity_monthly?.data        ?? [];
        const velWeekly    = a.review_velocity_weekly?.data         ?? [];
        const velDaily     = a.review_velocity_daily?.data          ?? [];

        if (sentByCat.length === 0) return null;

        // ---- KPIs -----------------------------------------------------------
        const totalReviews  = sentByCat.reduce((s, r) => s + (+(r.total_reviews ?? 0)), 0);
        const totalPositive = sentByCat.reduce((s, r) => s + (+(r.positive_reviews ?? 0)), 0);
        const totalNegative = sentByCat.reduce((s, r) => s + (+(r.negative_reviews ?? 0)), 0);
        const totalNeutral  = sentByCat.reduce((s, r) => s + (+(r.neutral_reviews ?? 0)), 0);
        const overallSentScore = sentByCat.length > 0
            ? sentByCat.reduce((s, r) => s + (+(r.avg_sentiment_score ?? 0)), 0) / sentByCat.length
            : 0;

        // ---- Positive sentiment rate by category (bar, color-coded) ---------
        const sentCatSorted = [...sentByCat].sort((a, b) => (+(b.positive_share ?? 0)) - (+(a.positive_share ?? 0)));
        const posSentBarData = {
            labels: sentCatSorted.map((r) => r.category ?? 'Unknown'),
            datasets: [{
                label: 'Positive Share %',
                data: sentCatSorted.map((r) => +(r.positive_share ?? 0).toFixed(1)),
                backgroundColor: sentCatSorted.map((r) => {
                    const v = +(r.positive_share ?? 0);
                    if (v >= 70) return 'rgba(34,197,94,0.82)';
                    if (v >= 50) return 'rgba(59,130,246,0.82)';
                    if (v >= 30) return 'rgba(234,179,8,0.82)';
                    return 'rgba(239,68,68,0.82)';
                }),
            }],
        };

        // ---- Avg sentiment score by category --------------------------------
        const sentScoreBarData = {
            labels: sentCatSorted.map((r) => r.category ?? 'Unknown'),
            datasets: [{
                label: 'Avg Sentiment Score',
                data: sentCatSorted.map((r) => +(r.avg_sentiment_score ?? 0).toFixed(3)),
                backgroundColor: 'rgba(139,92,246,0.82)',
            }],
        };

        // ---- Sentiment breakdown (stacked, by category) ----------------------
        const sentCats = sentByCat.map((r) => r.category ?? 'Unknown');
        const sentStackedData = {
            labels: sentCats,
            datasets: [
                { label: 'Positive', data: sentByCat.map((r) => +(r.positive_reviews ?? 0)), backgroundColor: 'rgba(34,197,94,0.82)' },
                { label: 'Neutral',  data: sentByCat.map((r) => +(r.neutral_reviews ?? 0)),  backgroundColor: 'rgba(234,179,8,0.82)' },
                { label: 'Negative', data: sentByCat.map((r) => +(r.negative_reviews ?? 0)), backgroundColor: 'rgba(239,68,68,0.82)' },
            ],
        };

        // ---- Overall sentiment share doughnut --------------------------------
        const overallSentDoughnut = {
            labels: ['Positive', 'Neutral', 'Negative'],
            datasets: [{
                data: [totalPositive, totalNeutral, totalNegative],
                backgroundColor: ['rgba(34,197,94,0.82)', 'rgba(234,179,8,0.82)', 'rgba(239,68,68,0.82)'],
                borderWidth: 2,
            }],
        };

        // ---- Negative share by category (horizontal bar) ---------------------
        const negByCatSorted = [...sentByCat].sort((a, b) => (+(b.negative_share ?? 0)) - (+(a.negative_share ?? 0)));
        const negShareBarData = {
            labels: negByCatSorted.map((r) => r.category ?? 'Unknown'),
            datasets: [{
                label: 'Negative Share %',
                data: negByCatSorted.map((r) => +(r.negative_share ?? 0).toFixed(1)),
                backgroundColor: negByCatSorted.map((r) => {
                    const v = +(r.negative_share ?? 0);
                    if (v >= 30) return 'rgba(239,68,68,0.82)';
                    if (v >= 15) return 'rgba(234,179,8,0.82)';
                    return 'rgba(34,197,94,0.82)';
                }),
            }],
        };

        // ---- Monthly velocity trend (aggregated across all products) --------
        // Group by year_month, sum monthly_reviews, avg avg_rating_monthly
        const monthMap = {};
        velMonthly.forEach((r) => {
            const key = r.year_month ?? `${r.review_year}-${String(r.review_month ?? 1).padStart(2, '0')}`;
            if (!monthMap[key]) monthMap[key] = { reviews: 0, ratingSum: 0, count: 0 };
            monthMap[key].reviews   += +(r.monthly_reviews ?? 0);
            monthMap[key].ratingSum += +(r.avg_rating_monthly ?? 0);
            monthMap[key].count     += 1;
        });
        const monthKeys = Object.keys(monthMap).sort();
        const monthLineData = monthKeys.length > 0 ? {
            labels: monthKeys,
            datasets: [
                {
                    label: 'Monthly Reviews',
                    data: monthKeys.map((k) => monthMap[k].reviews),
                    borderColor: 'rgba(59,130,246,0.9)',
                    backgroundColor: 'rgba(59,130,246,0.15)',
                    tension: 0.3, fill: true, yAxisID: 'y',
                },
                {
                    label: 'Avg Rating',
                    data: monthKeys.map((k) => +(monthMap[k].ratingSum / monthMap[k].count).toFixed(2)),
                    borderColor: 'rgba(234,179,8,0.9)',
                    backgroundColor: 'rgba(234,179,8,0.15)',
                    tension: 0.3, fill: false, yAxisID: 'y1',
                },
            ],
        } : null;

        const monthLineDualOpts = {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: true, position: 'top' }, title: { display: false } },
            scales: {
                x:  { grid: { color: 'rgba(0,0,0,0.05)' } },
                y:  { beginAtZero: true, grid: { color: 'rgba(0,0,0,0.05)' }, position: 'left', title: { display: true, text: 'Reviews' } },
                y1: { beginAtZero: true, grid: { display: false }, position: 'right', min: 0, max: 5, title: { display: true, text: 'Avg Rating' } },
            },
        };

        return {
            kpis: { totalReviews, totalPositive, totalNegative, totalNeutral, overallSentScore },
            posSentBarData, sentScoreBarData, sentStackedData, overallSentDoughnut,
            negShareBarData, monthLineData, monthLineDualOpts,
            sentByCat, velWeekly, velDaily,
        };
    }, [rawReview]);

    const hasData = derived !== null;

    // -------------------------------------------------------------------------
    // Render states
    // -------------------------------------------------------------------------

    if (loading && pipelineStatus !== 'loading') {
        return (
            <div className="flex flex-col items-center justify-center min-h-[400px] gap-4">
                <ProgressSpinner />
                <p className="text-gray-500 text-base">Loading sentiment analytics…</p>
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
                        <p className="text-gray-500 text-sm mt-1">Unable to load sentiment data. Please try again later.</p>
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
                            : 'No sentiment data to display.'}
                    </p>
                </div>
            </div>
        );
    }
    if (!derived) return null;
    const {
        kpis, posSentBarData, sentScoreBarData, sentStackedData, overallSentDoughnut,
        negShareBarData, monthLineData, monthLineDualOpts,
        sentByCat,
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
                    * Review sentiment analytics are static aggregates computed over all available data and do not change with the date filter.
                    Monthly trend charts reflect the full historical period.
                </p>
            )}

            {/* ── KPI Cards ──────────────────────────────────────────────── */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <KPICard
                    icon="pi-comments" iconBg="bg-blue-100" iconColor="text-blue-600"
                    value={fmt.number(kpis.totalReviews)}
                    label="Total Reviews"
                />
                <KPICard
                    icon="pi-thumbs-up" iconBg="bg-green-100" iconColor="text-green-600"
                    value={fmt.number(kpis.totalPositive)}
                    label="Positive Reviews"
                />
                <KPICard
                    icon="pi-thumbs-down" iconBg="bg-red-100" iconColor="text-red-600"
                    value={fmt.number(kpis.totalNegative)}
                    label="Negative Reviews"
                />
                <KPICard
                    icon="pi-chart-line" iconBg="bg-purple-100" iconColor="text-purple-600"
                    value={fmt.decimal(kpis.overallSentScore, 3)}
                    label="Avg Sentiment Score"
                />
            </div>

            {/* ── Overall Sentiment ─────────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-green-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Overall Sentiment</h2>
                </div>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    <ChartWrapper title="Overall Sentiment Distribution" height={280}>
                        <Doughnut data={overallSentDoughnut} options={doughnutOpts()} />
                    </ChartWrapper>
                    {sentScoreBarData.labels.length > 0 && (
                        <ChartWrapper title="Avg Sentiment Score by Category" height={320}>
                            <Bar data={sentScoreBarData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                </div>
            </section>

            {/* ── Positive vs Negative by Category ──────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-blue-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Sentiment by Category</h2>
                </div>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {posSentBarData.labels.length > 0 && (
                        <ChartWrapper title="Positive Share % by Category (color = level)" height={340}>
                            <Bar data={posSentBarData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                    {negShareBarData.labels.length > 0 && (
                        <ChartWrapper title="Negative Share % by Category (color = alert level)" height={340}>
                            <Bar data={negShareBarData} options={barOpts()} />
                        </ChartWrapper>
                    )}
                </div>
                {sentStackedData.labels.length > 0 && (
                    <ChartWrapper title="Positive / Neutral / Negative Reviews by Category" height={360}>
                        <Bar data={sentStackedData} options={groupedBarOpts()} />
                    </ChartWrapper>
                )}
            </section>

            {/* ── Monthly Trend ─────────────────────────────────────────── */}
            {monthLineData && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-purple-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Monthly Review Trend</h2>
                    </div>
                    <ChartWrapper title="Monthly Review Volume & Avg Rating Over Time" height={360}>
                        <Line data={monthLineData} options={monthLineDualOpts} />
                    </ChartWrapper>
                </section>
            )}

            {/* ── Summary Table ──────────────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-gray-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Summary Table</h2>
                </div>
                {sentByCat.length > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                Sentiment Breakdown by Category
                            </h3>
                            <DataTable
                                value={[...sentByCat].sort((a, b) => (+(b.positive_share ?? 0)) - (+(a.positive_share ?? 0)))}
                                scrollable stripedRows emptyMessage="No data" className="text-sm">
                                <Column field="category"           header="Category"          sortable />
                                <Column field="total_reviews"      header="Total Reviews"     sortable body={(r) => fmt.number(r.total_reviews)} />
                                <Column field="avg_rating"         header="Avg Rating"        sortable body={(r) => fmt.decimal(r.avg_rating, 2)} />
                                <Column field="positive_reviews"   header="Positive"          sortable body={(r) => fmt.number(r.positive_reviews)} />
                                <Column field="neutral_reviews"    header="Neutral"           sortable body={(r) => fmt.number(r.neutral_reviews)} />
                                <Column field="negative_reviews"   header="Negative"          sortable body={(r) => fmt.number(r.negative_reviews)} />
                                <Column field="positive_share"     header="Positive %"        sortable body={(r) => fmt.pct(r.positive_share)} />
                                <Column field="negative_share"     header="Negative %"        sortable body={(r) => fmt.pct(r.negative_share)} />
                                <Column field="avg_sentiment_score" header="Sentiment Score"  sortable body={(r) => fmt.decimal(r.avg_sentiment_score, 3)} />
                            </DataTable>
                        </div>
                    </Card>
                )}

                {/* Review Velocity — Weekly */}
                {(derived?.velWeekly?.length ?? 0) > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                Review Velocity (Weekly) — Aggregated
                            </h3>
                            <div className="h-[240px]">
                                <Line
                                    data={{
                                        labels: derived.velWeekly.map((r) => `${r.review_year}-W${String(r.review_week ?? 0).padStart(2,'0')}`),
                                        datasets: [
                                            { label: 'Weekly Reviews', data: derived.velWeekly.map((r) => +(r.weekly_reviews ?? 0)), borderColor: 'rgb(59,130,246)', backgroundColor: 'rgba(59,130,246,0.15)', tension: 0.4, fill: true, yAxisID: 'y' },
                                            { label: 'Avg Rating', data: derived.velWeekly.map((r) => +(r.avg_rating_weekly ?? 0)), borderColor: 'rgb(249,115,22)', backgroundColor: 'transparent', tension: 0.4, yAxisID: 'y1' },
                                        ],
                                    }}
                                    options={{ responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'top' } }, scales: { y: { beginAtZero: true, position: 'left' }, y1: { beginAtZero: false, position: 'right', min: 0, max: 5, grid: { drawOnChartArea: false } } } }}
                                />
                            </div>
                        </div>
                    </Card>
                )}

                {/* Review Velocity — Daily */}
                {(derived?.velDaily?.length ?? 0) > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                Review Velocity (Daily) — Aggregated across Products
                            </h3>
                            <DataTable
                                value={[...derived.velDaily].sort((a, b) => (a.review_date ?? '').localeCompare(b.review_date ?? '')).slice(-60)}
                                paginator rows={15} stripedRows emptyMessage="No data" className="text-sm"
                            >
                                <Column field="product_id"       header="Product ID"  sortable />
                                <Column field="review_date"      header="Date"        sortable />
                                <Column field="daily_reviews"    header="Daily Reviews" sortable body={(r) => fmt.number(r.daily_reviews)} />
                                <Column field="avg_rating_daily" header="Avg Rating"  sortable body={(r) => fmt.decimal(r.avg_rating_daily, 2)} />
                            </DataTable>
                        </div>
                    </Card>
                )}
            </section>
        </div>
    );
}
