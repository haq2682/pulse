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

const STRENGTH_COLORS = {
    'Strong':   'rgba(34,197,94,0.82)',
    'Moderate': 'rgba(234,179,8,0.82)',
    'Weak':     'rgba(156,163,175,0.82)',
};

const truncate = (s, n = 22) => (s && s.length > n ? `${s.slice(0, n)}…` : (s ?? '—'));

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

export default function RecommendationsProductAffinity() {
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
            console.error('[RecommendationsProductAffinity] fetch error');
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
        toastRef.current?.show({ severity: 'info', summary: 'Data Updated', detail: 'Analytics pipeline completed — refreshing affinity data.', life: 3000 });
    }, [lastUpdate]); // eslint-disable-line

    // -------------------------------------------------------------------------
    // Derived data
    // -------------------------------------------------------------------------

    const derived = useMemo(() => {
        if (!rawProduct) return null;
        const a = rawProduct.analytics ?? {};

        const pairs   = a.product_affinity_pairs?.data          ?? [];
        const topReco = a.product_affinity_top_per_product?.data ?? [];

        if (pairs.length === 0 && topReco.length === 0) return null;

        // ---- KPIs -----------------------------------------------------------
        const totalPairs    = pairs.length;
        const avgLift       = pairs.length > 0
            ? pairs.reduce((s, r) => s + (+(r.avg_lift ?? 0)), 0) / pairs.length
            : 0;
        const strongPairs   = pairs.filter((r) => r.affinity_strength === 'Strong').length;
        const topPair       = [...pairs].sort((a, b) => (+(b.avg_lift ?? 0)) - (+(a.avg_lift ?? 0)))[0];

        // ---- Top pairs by avg lift (top 15, horizontal bar) ----------------
        const topLiftSorted = [...pairs].sort((a, b) => (+(b.avg_lift ?? 0)) - (+(a.avg_lift ?? 0))).slice(0, 15);
        const liftBarData = {
            labels: topLiftSorted.map((r) => `${truncate(r.product_a_name)} ↔ ${truncate(r.product_b_name)}`),
            datasets: [{
                label: 'Avg Lift',
                data: topLiftSorted.map((r) => +(r.avg_lift ?? 0).toFixed(3)),
                backgroundColor: topLiftSorted.map((r) =>
                    STRENGTH_COLORS[r.affinity_strength] ?? 'rgba(156,163,175,0.82)'
                ),
            }],
        };

        // ---- Top pairs by affinity score (top 15, horizontal bar) ----------
        const topScoreSorted = [...pairs].sort((a, b) => (+(b.affinity_score ?? 0)) - (+(a.affinity_score ?? 0))).slice(0, 15);
        const scoreBarData = {
            labels: topScoreSorted.map((r) => `${truncate(r.product_a_name)} ↔ ${truncate(r.product_b_name)}`),
            datasets: [{
                label: 'Affinity Score',
                data: topScoreSorted.map((r) => +(r.affinity_score ?? 0).toFixed(3)),
                backgroundColor: 'rgba(59,130,246,0.82)',
            }],
        };

        // ---- Top pairs by co-occurrence (top 15) ----------------------------
        const topCoSorted = [...pairs].sort((a, b) => (+(b.co_occurrence_count ?? 0)) - (+(a.co_occurrence_count ?? 0))).slice(0, 15);
        const coBarData = {
            labels: topCoSorted.map((r) => `${truncate(r.product_a_name)} ↔ ${truncate(r.product_b_name)}`),
            datasets: [{
                label: 'Co-Occurrence Count',
                data: topCoSorted.map((r) => +(r.co_occurrence_count ?? 0)),
                backgroundColor: 'rgba(139,92,246,0.82)',
            }],
        };

        // ---- Affinity strength distribution (doughnut) ---------------------
        const strengthCounts = {};
        pairs.forEach((r) => {
            const s = r.affinity_strength ?? 'Unknown';
            strengthCounts[s] = (strengthCounts[s] ?? 0) + 1;
        });
        const strengthDoughnut = Object.keys(strengthCounts).length > 0 ? {
            labels: Object.keys(strengthCounts),
            datasets: [{
                data: Object.values(strengthCounts),
                backgroundColor: Object.keys(strengthCounts).map((s) => STRENGTH_COLORS[s] ?? 'rgba(156,163,175,0.82)'),
            }],
        } : null;

        // ---- Category mix of pairs (doughnut — by product_a_category) ------
        const catCounts = {};
        pairs.forEach((r) => {
            const c = r.product_a_category ?? 'Unknown';
            catCounts[c] = (catCounts[c] ?? 0) + 1;
        });
        const catDoughnut = Object.keys(catCounts).length > 0 ? {
            labels: Object.keys(catCounts),
            datasets: [{ data: Object.values(catCounts), backgroundColor: PALETTE }],
        } : null;

        // ---- Confidence A→B vs B→A grouped (top 12 by avg lift) ----------------
        const topForConf = topLiftSorted.slice(0, 12);
        const confGrouped = topForConf.length > 0 ? {
            labels: topForConf.map((r) => truncate(r.product_a_name, 18)),
            datasets: [
                { label: 'Confidence A→B', data: topForConf.map((r) => +(r.confidence_a_to_b ?? 0).toFixed(3)), backgroundColor: 'rgba(59,130,246,0.82)' },
                { label: 'Confidence B→A', data: topForConf.map((r) => +(r.confidence_b_to_a ?? 0).toFixed(3)), backgroundColor: 'rgba(249,115,22,0.82)' },
            ],
        } : null;

        // ---- Top recommendations per product (top 15 by affinity score) ----
        const topRecoSorted = [...topReco].sort((a, b) => (+(b.affinity_score ?? 0)) - (+(a.affinity_score ?? 0))).slice(0, 15);
        const recoBarData = topRecoSorted.length > 0 ? {
            labels: topRecoSorted.map((r) => truncate(r.product_a_name, 20)),
            datasets: [{
                label: 'Top Recommendation Affinity Score',
                data: topRecoSorted.map((r) => +(r.affinity_score ?? 0).toFixed(3)),
                backgroundColor: PALETTE,
            }],
        } : null;

        return {
            kpis: { totalPairs, avgLift, strongPairs, topPair },
            liftBarData, scoreBarData, coBarData,
            strengthDoughnut, catDoughnut, confGrouped, recoBarData,
            pairs, topReco: topRecoSorted,
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
                <p className="text-gray-500 text-base">Loading product affinity data…</p>
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
                        <p className="text-gray-500 text-sm mt-1">Unable to load product affinity data. Please try again later.</p>
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
                            : 'No product affinity data to display. Run the analytics pipeline to generate recommendations.'}
                    </p>
                </div>
            </div>
        );
    }
    if (!derived) return null;
    const {
        kpis, liftBarData, scoreBarData, coBarData,
        strengthDoughnut, catDoughnut, confGrouped, recoBarData,
        pairs, topReco,
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
                    * Product affinity analytics are static aggregates computed over all available data and do not change with the date filter.
                </p>
            )}

            {/* ── KPI Cards ──────────────────────────────────────────────── */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <KPICard
                    icon="pi-link" iconBg="bg-blue-100" iconColor="text-blue-600"
                    value={fmt.number(kpis.totalPairs)}
                    label="Affinity Pairs"
                />
                <KPICard
                    icon="pi-chart-line" iconBg="bg-purple-100" iconColor="text-purple-600"
                    value={fmt.decimal(kpis.avgLift, 2)}
                    label="Avg Lift"
                />
                <KPICard
                    icon="pi-star-fill" iconBg="bg-green-100" iconColor="text-green-600"
                    value={fmt.number(kpis.strongPairs)}
                    label="Strong Affinity Pairs"
                />
                <KPICard
                    icon="pi-arrow-up" iconBg="bg-amber-100" iconColor="text-amber-600"
                    value={kpis.topPair ? truncate(kpis.topPair.product_a_name, 16) : '—'}
                    label="Top Affinity Product"
                />
            </div>

            {/* ── Affinity Strength & Distribution ──────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-blue-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Affinity Strength & Distribution</h2>
                </div>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {strengthDoughnut && (
                        <ChartWrapper title="Pairs by Affinity Strength" height={280}>
                            <Doughnut data={strengthDoughnut} options={doughnutOpts()} />
                        </ChartWrapper>
                    )}
                    {catDoughnut && (
                        <ChartWrapper title="Affinity Pairs by Category" height={280}>
                            <Doughnut data={catDoughnut} options={doughnutOpts()} />
                        </ChartWrapper>
                    )}
                </div>
            </section>

            {/* ── Top Affinity Pairs ─────────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-green-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Top Affinity Pairs</h2>
                </div>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {liftBarData.labels.length > 0 && (
                        <ChartWrapper title="Top 15 Pairs by Avg Lift (color = strength)" height={420}>
                            <Bar data={liftBarData} options={barOpts(true)} />
                        </ChartWrapper>
                    )}
                    {scoreBarData.labels.length > 0 && (
                        <ChartWrapper title="Top 15 Pairs by Affinity Score" height={420}>
                            <Bar data={scoreBarData} options={barOpts(true)} />
                        </ChartWrapper>
                    )}
                    {coBarData.labels.length > 0 && (
                        <ChartWrapper title="Top 15 Pairs by Co-Occurrence Count" height={420}>
                            <Bar data={coBarData} options={barOpts(true)} />
                        </ChartWrapper>
                    )}
                    {confGrouped && (
                        <ChartWrapper title="Confidence A→B vs B→A (Top 12 by Lift)" height={380}>
                            <Bar data={confGrouped} options={groupedBarOpts()} />
                        </ChartWrapper>
                    )}
                </div>
            </section>

            {/* ── Top Recommendations per Product ───────────────────────── */}
            {recoBarData && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-purple-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Top Recommended Products</h2>
                    </div>
                    <ChartWrapper title="Top 15 Products with Highest Recommendation Affinity Score" height={400}>
                        <Bar data={recoBarData} options={barOpts(true)} />
                    </ChartWrapper>
                </section>
            )}

            {/* ── Summary Tables ─────────────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-gray-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Summary Tables</h2>
                </div>

                {/* All Affinity Pairs Table */}
                {pairs.length > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                All Product Affinity Pairs
                            </h3>
                            <DataTable
                                value={[...pairs].sort((a, b) => (+(b.avg_lift ?? 0)) - (+(a.avg_lift ?? 0)))}
                                paginator rows={15} rowsPerPageOptions={[15, 25, 50]}
                                scrollable stripedRows emptyMessage="No data" className="text-sm">
                                <Column header="Product A" body={(r) => r.product_a_name ?? `ID ${r.product_a_id}`} sortable sortField="product_a_name" />
                                <Column header="Product B" body={(r) => r.product_b_name ?? `ID ${r.product_b_id}`} sortable sortField="product_b_name" />
                                <Column field="product_a_category"   header="Category A"      sortable />
                                <Column field="co_occurrence_count"  header="Co-Occurrences"  sortable body={(r) => fmt.number(r.co_occurrence_count)} />
                                <Column field="support"              header="Support"          sortable body={(r) => fmt.decimal(r.support, 4)} />
                                <Column field="avg_lift"             header="Avg Lift"         sortable body={(r) => fmt.decimal(r.avg_lift, 3)} />
                                <Column field="affinity_score"       header="Affinity Score"   sortable body={(r) => fmt.decimal(r.affinity_score, 3)} />
                                <Column field="affinity_strength"    header="Strength"         sortable body={(r) => (
                                    <Tag value={r.affinity_strength ?? '—'}
                                        severity={r.affinity_strength === 'Strong' ? 'success' : r.affinity_strength === 'Moderate' ? 'warning' : 'secondary'} />
                                )} />
                            </DataTable>
                        </div>
                    </Card>
                )}

                {/* Top Recommendations per Product Table */}
                {topReco.length > 0 && (
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                Top Recommendation per Product
                            </h3>
                            <DataTable
                                value={topReco}
                                paginator rows={15}
                                scrollable stripedRows emptyMessage="No data" className="text-sm">
                                <Column header="Product" body={(r) => r.product_a_name ?? `ID ${r.product_a_id}`} sortable sortField="product_a_name" />
                                <Column field="product_a_category"        header="Category"        sortable />
                                <Column header="Recommended"              body={(r) => r.recommended_product_name ?? `ID ${r.recommended_product_id}`} sortable sortField="recommended_product_name" />
                                <Column field="recommended_product_category" header="Reco Category" sortable />
                                <Column field="co_occurrence_count"       header="Co-Occurrences"  sortable body={(r) => fmt.number(r.co_occurrence_count)} />
                                <Column field="avg_lift"                  header="Avg Lift"         sortable body={(r) => fmt.decimal(r.avg_lift, 3)} />
                                <Column field="affinity_score"            header="Affinity Score"   sortable body={(r) => fmt.decimal(r.affinity_score, 3)} />
                                <Column field="affinity_strength"         header="Strength"         sortable body={(r) => (
                                    <Tag value={r.affinity_strength ?? '—'}
                                        severity={r.affinity_strength === 'Strong' ? 'success' : r.affinity_strength === 'Moderate' ? 'warning' : 'secondary'} />
                                )} />
                            </DataTable>
                        </div>
                    </Card>
                )}
            </section>
        </div>
    );
}
