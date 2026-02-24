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

const truncate = (s, n = 24) => (s && s.length > n ? `${s.slice(0, n)}…` : (s ?? '—'));

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

const doughnutOpts = () => ({
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { position: 'right' }, title: { display: false } },
});

// ---------------------------------------------------------------------------
// Main Component
// ---------------------------------------------------------------------------

export default function RecommendationsCoverage() {
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
            console.error('[RecommendationsCoverage] fetch error');
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
        toastRef.current?.show({ severity: 'info', summary: 'Data Updated', detail: 'Analytics pipeline completed — refreshing coverage data.', life: 3000 });
    }, [lastUpdate]); // eslint-disable-line

    // -------------------------------------------------------------------------
    // Derived data
    // -------------------------------------------------------------------------

    const derived = useMemo(() => {
        if (!rawProduct) return null;
        const a = rawProduct.analytics ?? {};

        const precomputed = a.precomputed_product_recommendations?.data ?? [];
        const coverage    = a.precomputed_reco_coverage?.data            ?? [];
        // Also pull affinity pairs for additional context
        const pairs       = a.product_affinity_pairs?.data               ?? [];

        if (precomputed.length === 0 && coverage.length === 0) return null;

        // ---- Coverage KPIs (from precomputed_reco_coverage) -----------------
        const cov = coverage[0] ?? {};
        const totalProducts      = +(cov.total_products ?? 0);
        const productsWithReco   = +(cov.products_with_recommendations ?? 0);
        const coverageRate       = +(cov.coverage_rate ?? 0);
        const withoutReco        = Math.max(0, totalProducts - productsWithReco);

        // ---- Coverage doughnut -----------------------------------------------
        const coverageDoughnut = totalProducts > 0 ? {
            labels: ['With Recommendations', 'Without Recommendations'],
            datasets: [{
                data: [productsWithReco, withoutReco],
                backgroundColor: ['rgba(34,197,94,0.82)', 'rgba(239,68,68,0.82)'],
                borderWidth: 2,
            }],
        } : null;

        // ---- Products with recommendations (top 20 by avg affinity score) ---
        const withRecoSorted = [...precomputed]
            .filter((r) => r.has_recommendations)
            .sort((a, b) => (+(b.avg_affinity_score ?? 0)) - (+(a.avg_affinity_score ?? 0)))
            .slice(0, 20);

        // Build a product name lookup from affinity pairs (product_a_id → name)
        const nameMap = {};
        pairs.forEach((p) => {
            if (p.product_a_id && p.product_a_name) nameMap[p.product_a_id] = p.product_a_name;
            if (p.product_b_id && p.product_b_name) nameMap[p.product_b_id] = p.product_b_name;
        });

        const topAffinityBarData = withRecoSorted.length > 0 ? {
            labels: withRecoSorted.map((r) => truncate(nameMap[r.product_a_id] ?? `Product ${r.product_a_id}`, 22)),
            datasets: [{
                label: 'Avg Affinity Score',
                data: withRecoSorted.map((r) => +(r.avg_affinity_score ?? 0).toFixed(3)),
                backgroundColor: PALETTE,
            }],
        } : null;

        // ---- Distribution of recommended_products list size -----------------
        // Parse the recommended_products field to count how many each product has
        const recoCounts = {};
        precomputed
            .filter((r) => r.has_recommendations)
            .forEach((r) => {
                const raw = r.recommended_products;
                let count = 0;
                if (Array.isArray(raw)) {
                    count = raw.length;
                } else if (typeof raw === 'string') {
                    try { count = JSON.parse(raw).length; } catch { count = 0; }
                }
                const bucket = count <= 0 ? '0'
                    : count === 1 ? '1'
                    : count <= 3 ? '2-3'
                    : count <= 5 ? '4-5'
                    : '6+';
                recoCounts[bucket] = (recoCounts[bucket] ?? 0) + 1;
            });
        const recoDistOrder = ['1', '2-3', '4-5', '6+'];
        const recoDistLabels = recoDistOrder.filter((k) => recoCounts[k] != null);
        const recoDistData = recoDistLabels.length > 0 ? {
            labels: recoDistLabels,
            datasets: [{
                label: 'Products',
                data: recoDistLabels.map((k) => recoCounts[k] ?? 0),
                backgroundColor: PALETTE,
            }],
        } : null;

        // ---- Affinity score distribution (bar, bucketed) --------------------
        const scoreBuckets = { '0–0.2': 0, '0.2–0.4': 0, '0.4–0.6': 0, '0.6–0.8': 0, '0.8–1.0': 0 };
        precomputed.filter((r) => r.has_recommendations).forEach((r) => {
            const s = +(r.avg_affinity_score ?? 0);
            if (s < 0.2)       scoreBuckets['0–0.2']++;
            else if (s < 0.4)  scoreBuckets['0.2–0.4']++;
            else if (s < 0.6)  scoreBuckets['0.4–0.6']++;
            else if (s < 0.8)  scoreBuckets['0.6–0.8']++;
            else               scoreBuckets['0.8–1.0']++;
        });
        const scoreDistData = {
            labels: Object.keys(scoreBuckets),
            datasets: [{
                label: 'Products',
                data: Object.values(scoreBuckets),
                backgroundColor: [
                    'rgba(239,68,68,0.82)',
                    'rgba(234,179,8,0.82)',
                    'rgba(59,130,246,0.82)',
                    'rgba(34,197,94,0.82)',
                    'rgba(16,185,129,0.82)',
                ],
            }],
        };

        return {
            kpis: { totalProducts, productsWithReco, withoutReco, coverageRate },
            coverageDoughnut, topAffinityBarData, recoDistData, scoreDistData,
            precomputed, withRecoSorted, nameMap,
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
                <p className="text-gray-500 text-base">Loading recommendation coverage…</p>
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
                        <p className="text-gray-500 text-sm mt-1">Unable to load coverage data. Please try again later.</p>
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
                            : 'No recommendation coverage data to display. Run the analytics pipeline to generate recommendations.'}
                    </p>
                </div>
            </div>
        );
    }
    if (!derived) return null;
    const {
        kpis, coverageDoughnut, topAffinityBarData, recoDistData, scoreDistData,
        withRecoSorted, nameMap,
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
                    * Recommendation coverage analytics are static aggregates computed over all available data and do not change with the date filter.
                </p>
            )}

            {/* ── KPI Cards ──────────────────────────────────────────────── */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <KPICard
                    icon="pi-box" iconBg="bg-blue-100" iconColor="text-blue-600"
                    value={fmt.number(kpis.totalProducts)}
                    label="Total Products"
                />
                <KPICard
                    icon="pi-check-circle" iconBg="bg-green-100" iconColor="text-green-600"
                    value={fmt.number(kpis.productsWithReco)}
                    label="Products with Recommendations"
                />
                <KPICard
                    icon="pi-times-circle" iconBg="bg-red-100" iconColor="text-red-600"
                    value={fmt.number(kpis.withoutReco)}
                    label="Products without Recommendations"
                />
                <KPICard
                    icon="pi-percentage" iconBg="bg-purple-100" iconColor="text-purple-600"
                    value={fmt.pct(kpis.coverageRate)}
                    label="Coverage Rate"
                />
            </div>

            {/* ── Coverage Overview ──────────────────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-green-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Coverage Overview</h2>
                </div>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {coverageDoughnut && (
                        <ChartWrapper title="Products With vs Without Recommendations" height={280}>
                            <Doughnut data={coverageDoughnut} options={doughnutOpts()} />
                        </ChartWrapper>
                    )}
                    <ChartWrapper title="Affinity Score Distribution (products with recommendations)" height={280}>
                        <Bar data={scoreDistData} options={barOpts()} />
                    </ChartWrapper>
                </div>
            </section>

            {/* ── Recommendation Depth ───────────────────────────────────── */}
            {recoDistData && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-blue-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Recommendation Depth</h2>
                    </div>
                    <ChartWrapper title="Number of Recommendations per Product (distribution)" height={300}>
                        <Bar data={recoDistData} options={barOpts()} />
                    </ChartWrapper>
                </section>
            )}

            {/* ── Top Products by Affinity Score ─────────────────────────── */}
            {topAffinityBarData && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-purple-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Top Products by Avg Affinity Score</h2>
                    </div>
                    <ChartWrapper title="Top 20 Products with Highest Avg Recommendation Affinity Score" height={460}>
                        <Bar data={topAffinityBarData} options={barOpts(true)} />
                    </ChartWrapper>
                </section>
            )}

            {/* ── Product Recommendations Table ──────────────────────────── */}
            {withRecoSorted.length > 0 && (
                <section className="space-y-6">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-gray-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Product Recommendation Details</h2>
                    </div>
                    <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                                Products with Pre-Computed Recommendations
                            </h3>
                            <DataTable
                                value={withRecoSorted}
                                paginator rows={15}
                                scrollable stripedRows emptyMessage="No data" className="text-sm">
                                <Column header="Product" sortable sortField="product_a_id"
                                    body={(r) => truncate(nameMap[r.product_a_id] ?? `Product ${r.product_a_id}`, 30)} />
                                <Column field="avg_affinity_score" header="Avg Affinity Score" sortable
                                    body={(r) => fmt.decimal(r.avg_affinity_score, 3)} />
                                <Column field="has_recommendations" header="Has Recommendations" sortable
                                    body={(r) => (
                                        <Tag value={r.has_recommendations ? 'Yes' : 'No'}
                                            severity={r.has_recommendations ? 'success' : 'danger'} />
                                    )} />
                            </DataTable>
                        </div>
                    </Card>
                </section>
            )}
        </div>
    );
}
