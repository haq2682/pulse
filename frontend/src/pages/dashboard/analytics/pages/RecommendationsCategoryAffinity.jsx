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

export default function RecommendationsCategoryAffinity() {
    const fmt = useFormatters();
    const { businessId } = useParams();
    const toastRef = useRef(null);
    const { pipelineStatus } = usePipelineProgress();
    const { dateRange, setDateRange, quickFilter, isFiltered, applyQuickFilter, resetFilters } = useAnalyticsDateFilter();
    const { lastUpdate } = useAnalyticsWebSocket(businessId);

    const [rawProduct, setRawProduct] = useState(null);
    const [loading, setLoading] = useState(true);
    const [fetchError, setFetchError] = useState(false);
    const [dataMode, setDataMode] = useState('unknown');

    // -------------------------------------------------------------------------
    // Fetch
    // -------------------------------------------------------------------------

    const buildUrl = useCallback(() => {
        const base = import.meta.env.VITE_API_URL || 'http://localhost:8000';
        const params = new URLSearchParams({ categories: 'product_analytics' });
        return `${base}/analytics/data/${businessId}?${params.toString()}`;
    }, [businessId]);

    const fetchData = useCallback(async () => {
        if (!businessId) return;
        try {
            setLoading(true);
            setFetchError(false);
            const res = await fetch(buildUrl());
            if (!res.ok) {
                setRawProduct(null);
                return;
            }
            const json = await res.json();
            if (json.mode) setDataMode(json.mode);
            setRawProduct(json.categories?.product_analytics ?? null);
        } catch {
            console.error('[RecommendationsCategoryAffinity] fetch error');
            setFetchError(true);
            setRawProduct(null);
        } finally {
            setLoading(false);
        }
    }, [buildUrl, businessId]);

    useEffect(() => { fetchData(); }, [businessId]); // eslint-disable-line
    useEffect(() => {
        if (!lastUpdate) return;
        fetchData();
        toastRef.current?.show({ severity: 'info', summary: 'Data Updated', detail: 'Analytics pipeline completed — refreshing category affinity data.', life: 3000 });
    }, [lastUpdate]); // eslint-disable-line

    // -------------------------------------------------------------------------
    // Derived data
    // -------------------------------------------------------------------------

    const derived = useMemo(() => {
        if (!rawProduct) return null;
        const a = rawProduct.analytics ?? {};

        const pairs   = a.category_affinity_pairs?.data           ?? [];
        const topPer  = a.category_affinity_top_per_category?.data ?? [];

        if (pairs.length === 0 && topPer.length === 0) return null;

        // ---- KPIs -----------------------------------------------------------
        const totalPairs   = pairs.length;
        const avgLift      = pairs.length > 0
            ? pairs.reduce((s, r) => s + (+(r.avg_lift_between_categories ?? 0)), 0) / pairs.length
            : 0;
        const topPair      = [...pairs].sort((a, b) => (+(b.avg_lift_between_categories ?? 0)) - (+(a.avg_lift_between_categories ?? 0)))[0];
        const uniqueCats   = new Set([...pairs.map((r) => r.product_a_category), ...pairs.map((r) => r.product_b_category)]).size;

        // ---- Top pairs by avg lift (horizontal bar, top 15) -----------------
        const topLiftSorted = [...pairs].sort((a, b) => (+(b.avg_lift_between_categories ?? 0)) - (+(a.avg_lift_between_categories ?? 0))).slice(0, 15);
        const liftBarData = {
            labels: topLiftSorted.map((r) => `${r.product_a_category ?? '?'} ↔ ${r.product_b_category ?? '?'}`),
            datasets: [{
                label: 'Avg Lift Between Categories',
                data: topLiftSorted.map((r) => +(r.avg_lift_between_categories ?? 0).toFixed(3)),
                backgroundColor: PALETTE,
            }],
        };

        // ---- Top pairs by co-occurrences (horizontal bar, top 15) ----------
        const topCoSorted = [...pairs].sort((a, b) => (+(b.total_co_occurrences ?? 0)) - (+(a.total_co_occurrences ?? 0))).slice(0, 15);
        const coBarData = {
            labels: topCoSorted.map((r) => `${r.product_a_category ?? '?'} ↔ ${r.product_b_category ?? '?'}`),
            datasets: [{
                label: 'Total Co-Occurrences',
                data: topCoSorted.map((r) => +(r.total_co_occurrences ?? 0)),
                backgroundColor: 'rgba(139,92,246,0.82)',
            }],
        };

        // ---- Avg support by pair (top 12) -----------------------------------
        const topSupportSorted = [...pairs].sort((a, b) => (+(b.avg_support ?? 0)) - (+(a.avg_support ?? 0))).slice(0, 12);
        const supportBarData = {
            labels: topSupportSorted.map((r) => `${r.product_a_category ?? '?'} ↔ ${r.product_b_category ?? '?'}`),
            datasets: [{
                label: 'Avg Support',
                data: topSupportSorted.map((r) => +(r.avg_support ?? 0).toFixed(4)),
                backgroundColor: 'rgba(6,182,212,0.82)',
            }],
        };

        // ---- Base category participation (how many top affinities each cat has) ----
        const baseCatCount = {};
        topPer.forEach((r) => {
            const c = r.base_category ?? 'Unknown';
            baseCatCount[c] = (baseCatCount[c] ?? 0) + 1;
        });
        const baseCatLabels = Object.keys(baseCatCount).sort((a, b) => baseCatCount[b] - baseCatCount[a]).slice(0, 12);
        const baseCatBarData = baseCatLabels.length > 0 ? {
            labels: baseCatLabels,
            datasets: [{
                label: 'Affinity Entries',
                data: baseCatLabels.map((c) => baseCatCount[c]),
                backgroundColor: PALETTE,
            }],
        } : null;

        // ---- Top affinity categories doughnut (by co-occurrence volume) -----
        const catVolMap = {};
        pairs.forEach((r) => {
            const ca = r.product_a_category ?? 'Unknown';
            const cb = r.product_b_category ?? 'Unknown';
            catVolMap[ca] = (catVolMap[ca] ?? 0) + (+(r.total_co_occurrences ?? 0));
            catVolMap[cb] = (catVolMap[cb] ?? 0) + (+(r.total_co_occurrences ?? 0));
        });
        const topCatVol = Object.entries(catVolMap).sort((a, b) => b[1] - a[1]).slice(0, 10);
        const catVolDoughnut = topCatVol.length > 0 ? {
            labels: topCatVol.map(([c]) => c),
            datasets: [{ data: topCatVol.map(([, v]) => v), backgroundColor: PALETTE }],
        } : null;

        // ---- Top affinity per category grouped (avg lift by base + affinity cat) --
        const topPerCats = [...new Set(topPer.map((r) => r.base_category ?? 'Unknown'))].slice(0, 8);
        const topPerGrouped = topPerCats.length > 0 ? {
            labels: topPerCats,
            datasets: [
                {
                    label: 'Avg Lift (top affinity pair)',
                    data: topPerCats.map((cat) => {
                        const row = topPer.find((r) => r.base_category === cat);
                        return row ? +(row.avg_lift ?? 0).toFixed(3) : 0;
                    }),
                    backgroundColor: 'rgba(34,197,94,0.82)',
                },
                {
                    label: 'Total Co-Occurrences',
                    data: topPerCats.map((cat) => {
                        const row = topPer.find((r) => r.base_category === cat);
                        return row ? +(row.total_co_occurrences ?? 0) : 0;
                    }),
                    backgroundColor: 'rgba(59,130,246,0.82)',
                },
            ],
        } : null;

        return {
            kpis: { totalPairs, avgLift, topPair, uniqueCats },
            liftBarData, coBarData, supportBarData,
            baseCatBarData, catVolDoughnut, topPerGrouped,
            pairs, topPer,
        };
    }, [rawProduct]);

    const hasData = derived !== null;

    // -------------------------------------------------------------------------
    // Render states
    // -------------------------------------------------------------------------

    if (loading && pipelineStatus !== 'loading') {
        return (
            <div className="flex flex-col items-center justify-center min-h-[400px] gap-4">
                <ProgressSpinner />
                <p className="text-gray-500 text-base">Loading category affinity data…</p>
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
                        <p className="text-gray-500 text-sm mt-1">Unable to load category affinity data. Please try again later.</p>
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
                            : 'No category affinity data to display. Run the analytics pipeline to generate recommendations.'}
                    </p>
                </div>
            </div>
        );
    }
    if (!derived) return null;
    const {
        kpis, liftBarData, coBarData, supportBarData,
        baseCatBarData, catVolDoughnut, topPerGrouped,
        pairs, topPer,
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
                    * Category affinity analytics are static aggregates computed over all available data and do not change with the date filter.
                </p>
            )}

            {/* ── KPI Cards ──────────────────────────────────────────────── */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <KPICard
                    icon="pi-th-large" iconBg="bg-blue-100" iconColor="text-blue-600"
                    value={fmt.number(kpis.uniqueCats)}
                    label="Categories in Pairs"
                />
                <KPICard
                    icon="pi-link" iconBg="bg-purple-100" iconColor="text-purple-600"
                    value={fmt.number(kpis.totalPairs)}
                    label="Category Affinity Pairs"
                />
                <KPICard
                    icon="pi-chart-line" iconBg="bg-green-100" iconColor="text-green-600"
                    value={fmt.decimal(kpis.avgLift, 2)}
                    label="Avg Lift"
                />
                <KPICard
                    icon="pi-arrow-up" iconBg="bg-amber-100" iconColor="text-amber-600"
                    value={kpis.topPair ? (kpis.topPair.product_a_category ?? '—') : '—'}
                    label="Top Affinity Category"
                />
            </div>

            {/* ── Affinity by Co-Occurrence & Volume ─────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-blue-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Co-Occurrence & Volume</h2>
                </div>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {coBarData.labels.length > 0 && (
                        <ChartWrapper title="Top 15 Category Pairs by Co-Occurrence Count" height={400}>
                            <Bar data={coBarData} options={barOpts(true)} />
                        </ChartWrapper>
                    )}
                    {catVolDoughnut && (
                        <ChartWrapper title="Category Participation in Affinity Pairs (Top 10)" height={280}>
                            <Doughnut data={catVolDoughnut} options={doughnutOpts()} />
                        </ChartWrapper>
                    )}
                </div>
            </section>

            {/* ── Lift & Support ─────────────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-green-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Lift & Support</h2>
                </div>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {liftBarData.labels.length > 0 && (
                        <ChartWrapper title="Top 15 Category Pairs by Avg Lift" height={400}>
                            <Bar data={liftBarData} options={barOpts(true)} />
                        </ChartWrapper>
                    )}
                    {supportBarData.labels.length > 0 && (
                        <ChartWrapper title="Top 12 Category Pairs by Avg Support" height={380}>
                            <Bar data={supportBarData} options={barOpts(true)} />
                        </ChartWrapper>
                    )}
                </div>
            </section>

            {/* ── Per-Category Affinity Analysis ────────────────────────── */}
            {(baseCatBarData || topPerGrouped) && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-purple-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Per-Category Affinity</h2>
                    </div>
                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                        {baseCatBarData && (
                            <ChartWrapper title="Affinity Entry Count per Base Category (Top 12)" height={340}>
                                <Bar data={baseCatBarData} options={barOpts()} />
                            </ChartWrapper>
                        )}
                        {topPerGrouped && (
                            <ChartWrapper title="Top Affinity Lift & Co-Occurrences per Category" height={340}>
                                <Bar data={topPerGrouped} options={groupedBarOpts()} />
                            </ChartWrapper>
                        )}
                    </div>
                </section>
            )}

            {/* ── Summary Tables ─────────────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-gray-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Summary Tables</h2>
                </div>

                {/* Category Pairs Table */}
                {pairs.length > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                All Category Affinity Pairs
                            </h3>
                            <DataTable
                                value={[...pairs].sort((a, b) => (+(b.avg_lift_between_categories ?? 0)) - (+(a.avg_lift_between_categories ?? 0)))}
                                paginator rows={15}
                                scrollable stripedRows emptyMessage="No data" className="text-sm">
                                <Column field="product_a_category"          header="Category A"         sortable />
                                <Column field="product_b_category"          header="Category B"         sortable />
                                <Column field="pair_count"                  header="Pair Count"         sortable body={(r) => fmt.number(r.pair_count)} />
                                <Column field="total_co_occurrences"        header="Co-Occurrences"     sortable body={(r) => fmt.number(r.total_co_occurrences)} />
                                <Column field="avg_lift_between_categories" header="Avg Lift"           sortable body={(r) => fmt.decimal(r.avg_lift_between_categories, 3)} />
                                <Column field="avg_support"                 header="Avg Support"        sortable body={(r) => fmt.decimal(r.avg_support, 4)} />
                            </DataTable>
                        </div>
                    </Card>
                )}

                {/* Top Per Category Table */}
                {topPer.length > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                Top Affinity Partner per Category
                            </h3>
                            <DataTable
                                value={[...topPer].sort((a, b) => (+(b.avg_lift ?? 0)) - (+(a.avg_lift ?? 0)))}
                                paginator rows={15}
                                scrollable stripedRows emptyMessage="No data" className="text-sm">
                                <Column field="base_category"     header="Base Category"     sortable />
                                <Column field="affinity_category" header="Affinity Category" sortable />
                                <Column field="pair_count"        header="Pair Count"        sortable body={(r) => fmt.number(r.pair_count)} />
                                <Column field="total_co_occurrences" header="Co-Occurrences" sortable body={(r) => fmt.number(r.total_co_occurrences)} />
                                <Column field="avg_lift"          header="Avg Lift"          sortable body={(r) => fmt.decimal(r.avg_lift, 3)} />
                                <Column field="avg_support"       header="Avg Support"       sortable body={(r) => fmt.decimal(r.avg_support, 4)} />
                            </DataTable>
                        </div>
                    </Card>
                )}
            </section>
        </div>
    );
}
