import React, { useState, useEffect, useRef, useMemo, useCallback } from 'react';
import { useParams } from 'react-router-dom';
import { Card } from 'primereact/card';
import { ProgressSpinner } from 'primereact/progressspinner';
import { Toast } from 'primereact/toast';
import { DataTable } from 'primereact/datatable';
import { Column } from 'primereact/column';
import {
    Chart as ChartJS,
    CategoryScale, LinearScale, PointElement, LineElement,
    BarElement, ArcElement, Title, Tooltip, Legend,
} from 'chart.js';
import { Bar, Line } from 'react-chartjs-2';
import { useAnalyticsWebSocket } from '../../../../hooks/useAnalyticsWebSocket';
import ChartWrapper from '../components/ChartWrapper';
import { usePipelineProgress } from '@/context/PipelineProgressContext';
import useAnalyticsDateFilter from '@/hooks/useAnalyticsDateFilter';
import DateFilterBar from '../components/DateFilterBar';
import { useFormatters } from '@/hooks/useFormatters';

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, BarElement, ArcElement, Title, Tooltip, Legend);

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PALETTE = [
    'rgba(59,130,246,0.82)', 'rgba(34,197,94,0.82)',  'rgba(249,115,22,0.82)',
    'rgba(239,68,68,0.82)',  'rgba(139,92,246,0.82)', 'rgba(6,182,212,0.82)',
    'rgba(234,179,8,0.82)',  'rgba(236,72,153,0.82)', 'rgba(20,184,166,0.82)',
    'rgba(168,85,247,0.82)',
];

const LINE_PALETTE = [
    'rgba(59,130,246,1)', 'rgba(34,197,94,1)',  'rgba(249,115,22,1)',
    'rgba(239,68,68,1)',  'rgba(139,92,246,1)', 'rgba(6,182,212,1)',
    'rgba(234,179,8,1)',  'rgba(236,72,153,1)',
];

const MONTH_NAMES = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

const ANALYTICS_CATEGORIES = [
    'product_analytics',
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
// Main Component
// ---------------------------------------------------------------------------

export default function ProductTrends() {
    const fmt = useFormatters();
    const { businessId } = useParams();
    const toastRef = useRef(null);
    const { pipelineStatus } = usePipelineProgress();
    const { clientFilter, dateRange, setDateRange, quickFilter, isFiltered, applyQuickFilter, resetFilters } = useAnalyticsDateFilter();
    const { lastUpdate } = useAnalyticsWebSocket(businessId);

    const [raw, setRaw] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [dataMode, setDataMode] = useState('unknown');

    // -------------------------------------------------------------------------
    // Fetch
    // -------------------------------------------------------------------------

    const buildUrl = useCallback(() => {
        const base = import.meta.env.VITE_API_URL || 'http://localhost:8000';
        const params = new URLSearchParams({ categories: ANALYTICS_CATEGORIES.join(',') });
        return `${base}/analytics/data/${businessId}?${params.toString()}`;
    }, [businessId]);

    const fetchData = useCallback(async () => {
        if (!businessId) return;
        try {
            setLoading(true);
            setError(null);
            const res = await fetch(buildUrl());
            if (!res.ok) {
                toastRef.current?.show({
                    severity: 'warn', summary: 'No Data',
                    detail: 'Analytics data not available. Run the analytics pipeline first.',
                    life: 5000,
                });
                setRaw(null);
                return;
            }
            const json = await res.json();
            if (json.mode) setDataMode(json.mode);
            setRaw(json.categories?.product_analytics?.analytics ?? {});
        } catch {
            console.error('[ProductTrends] fetch error');
            setError(true);
            setRaw(null);
            toastRef.current?.show({ severity: 'error', summary: 'Error', detail: 'Unable to load analytics data.', life: 5000 });
        } finally {
            setLoading(false);
        }
    }, [buildUrl, businessId]);

    useEffect(() => { fetchData(); }, []); // eslint-disable-line react-hooks/exhaustive-deps
    useEffect(() => { if (raw !== null) fetchData(); }, [dateRange]); // eslint-disable-line react-hooks/exhaustive-deps
    useEffect(() => {
        if (!lastUpdate) return;
        fetchData();
        toastRef.current?.show({ severity: 'info', summary: 'Data Updated', detail: 'Analytics pipeline completed — refreshing trends.', life: 3000 });
    }, [lastUpdate]); // eslint-disable-line react-hooks/exhaustive-deps

    // -------------------------------------------------------------------------
    // Derived data
    // -------------------------------------------------------------------------

    const derived = useMemo(() => {
        if (!raw) return null;

        // Enrich grain_year + grain_month rows with a synthetic grain_date for clientFilter
        const addGrainDate = (arr) =>
            (arr ?? []).map((r) => ({
                ...r,
                grain_date: `${r.grain_year}-${String(r.grain_month).padStart(2, '0')}-01`,
            }));

        // Time-filtered datasets
        const catMonthly  = clientFilter(addGrainDate(raw.category_monthly_trends?.data ?? []), 'grain_date');
        const prodMonthly = clientFilter(addGrainDate(raw.product_monthly_trends?.data  ?? []),  'grain_date');

        // Static aggregates
        const catSeasonal    = raw.category_monthly_seasonality?.data          ?? [];
        const catCalendarSeasonal = raw.category_calendar_month_seasonality?.data ?? [];
        const catPeak        = raw.category_peak_season?.data                   ?? [];
        const prodCalendar   = raw.product_calendar_month_seasonality?.data     ?? [];

        // ---- KPIs -------------------------------------------------------
        const totalUnits  = catMonthly.reduce((s, r) => s + (r.units_sold   ?? 0), 0);
        const totalOrders = catMonthly.reduce((s, r) => s + (r.orders_count ?? 0), 0);
        const peakRow     = catPeak.reduce(
            (best, r) => (!best || (r.peak_season_revenue ?? 0) > (best.peak_season_revenue ?? 0)) ? r : best,
            null,
        );

        // ---- Monthly trend charts (top 5 categories) --------------------
        const catTotalsMap = {};
        catMonthly.forEach((r) => {
            catTotalsMap[r.category] = (catTotalsMap[r.category] ?? 0) + (r.units_sold ?? 0);
        });
        const top5Cats = Object.entries(catTotalsMap)
            .sort((a, b) => b[1] - a[1]).slice(0, 5).map(([c]) => c);

        // Sorted unique month keys e.g. "2024-01"
        const monthKeySet = new Set();
        catMonthly.forEach((r) => monthKeySet.add(`${r.grain_year}-${String(r.grain_month).padStart(2, '0')}`));
        const sortedMonthKeys = Array.from(monthKeySet).sort();
        const monthAxisLabels = sortedMonthKeys.map((key) => {
            const [yr, mo] = key.split('-');
            return `${MONTH_NAMES[parseInt(mo, 10) - 1]} ${yr}`;
        });

        // per-category, per-month unit/order maps
        const catUnitsMap  = {};
        const catOrdersMap = {};
        catMonthly.forEach((r) => {
            const key = `${r.grain_year}-${String(r.grain_month).padStart(2, '0')}`;
            if (!catUnitsMap[r.category])  catUnitsMap[r.category]  = {};
            if (!catOrdersMap[r.category]) catOrdersMap[r.category] = {};
            catUnitsMap[r.category][key]  = (catUnitsMap[r.category][key]  ?? 0) + (r.units_sold   ?? 0);
            catOrdersMap[r.category][key] = (catOrdersMap[r.category][key] ?? 0) + (r.orders_count ?? 0);
        });

        const catUnitsLineData = {
            labels: monthAxisLabels,
            datasets: top5Cats.map((cat, i) => ({
                label: cat,
                data: sortedMonthKeys.map((key) => catUnitsMap[cat]?.[key] ?? 0),
                borderColor: LINE_PALETTE[i % LINE_PALETTE.length],
                backgroundColor: LINE_PALETTE[i % LINE_PALETTE.length].replace(',1)', ',0.12)'),
                tension: 0.4,
                fill: false,
                pointRadius: 3,
                spanGaps: true,
            })),
        };

        const catOrdersBarData = {
            labels: monthAxisLabels,
            datasets: top5Cats.map((cat, i) => ({
                label: cat,
                data: sortedMonthKeys.map((key) => catOrdersMap[cat]?.[key] ?? 0),
                backgroundColor: PALETTE[i % PALETTE.length],
            })),
        };

        // ---- Top 10 products by units (time-filtered) -------------------
        const prodAgg = {};
        prodMonthly.forEach((r) => {
            if (!prodAgg[r.product_name]) {
                prodAgg[r.product_name] = { units: 0, orders: 0, category: r.category, brand: r.brand ?? '' };
            }
            prodAgg[r.product_name].units  += r.units_sold   ?? 0;
            prodAgg[r.product_name].orders += r.orders_count ?? 0;
        });
        const top10Products = Object.entries(prodAgg)
            .sort((a, b) => b[1].units - a[1].units)
            .slice(0, 10)
            .map(([name, d]) => ({ product_name: name, ...d }));

        // ---- Seasonal units stacked bar (static, top 8 categories) -----
        const seasonalCats = [...new Set(catSeasonal.map((r) => r.category))].slice(0, 8);
        const seasonalUnitsMap = {};
        catSeasonal.forEach((r) => {
            if (!seasonalUnitsMap[r.category]) seasonalUnitsMap[r.category] = {};
            seasonalUnitsMap[r.category][r.calendar_month] = r.units_sold ?? 0;
        });

        const catSeasonalUnitsData = {
            labels: MONTH_NAMES,
            datasets: seasonalCats.map((cat, i) => ({
                label: cat,
                data: Array.from({ length: 12 }, (_, j) => seasonalUnitsMap[cat]?.[j + 1] ?? 0),
                backgroundColor: PALETTE[i % PALETTE.length],
                stack: 'seasonal',
            })),
        };

        // ---- Seasonal revenue by calendar month (static) ----------------
        const seasonalRevByMonth = {};
        catSeasonal.forEach((r) => {
            seasonalRevByMonth[r.calendar_month] = (seasonalRevByMonth[r.calendar_month] ?? 0) + (r.total_revenue_month ?? 0);
        });

        const catSeasonalRevData = {
            labels: MONTH_NAMES,
            datasets: [{
                label: 'Total Revenue',
                data: Array.from({ length: 12 }, (_, j) => seasonalRevByMonth[j + 1] ?? 0),
                backgroundColor: PALETTE,
            }],
        };

        // ---- Category peak season (static) ------------------------------
        const peakSorted = [...catPeak].sort((a, b) => (b.peak_season_revenue ?? 0) - (a.peak_season_revenue ?? 0));

        const catPeakBarData = {
            labels: peakSorted.map((r) => r.category),
            datasets: [{
                label: 'Peak Season Revenue',
                data: peakSorted.map((r) => r.peak_season_revenue ?? 0),
                backgroundColor: peakSorted.map((_, i) => PALETTE[i % PALETTE.length]),
            }],
        };

        // ---- Per-category metrics cards (static, top 6 by peak revenue) -
        const top6PeakCats = peakSorted.slice(0, 6);

        // ---- Category Calendar Month Seasonality (units, orders — no revenue) ---
        const calSeasonalCats = [...new Set(catCalendarSeasonal.map((r) => r.category))].slice(0, 8);
        const calSeasonalUnitsMap = {};
        catCalendarSeasonal.forEach((r) => {
            if (!calSeasonalUnitsMap[r.category]) calSeasonalUnitsMap[r.category] = {};
            calSeasonalUnitsMap[r.category][r.calendar_month] = (calSeasonalUnitsMap[r.category][r.calendar_month] ?? 0) + (r.units_sold ?? 0);
        });
        const catCalendarSeasonalData = {
            labels: MONTH_NAMES,
            datasets: calSeasonalCats.map((cat, i) => ({
                label: cat,
                data: Array.from({ length: 12 }, (_, j) => calSeasonalUnitsMap[cat]?.[j + 1] ?? 0),
                backgroundColor: PALETTE[i % PALETTE.length],
                stack: 'calSeasonal',
            })),
        };

        // ---- Product Calendar Month Seasonality (top 10 products by total units) ---
        const prodCalAgg = {};
        prodCalendar.forEach((r) => {
            const k = r.product_name ?? r.product_id ?? 'Unknown';
            if (!prodCalAgg[k]) prodCalAgg[k] = { total: 0, months: {} };
            prodCalAgg[k].total += r.units_sold ?? 0;
            prodCalAgg[k].months[r.calendar_month] = (prodCalAgg[k].months[r.calendar_month] ?? 0) + (r.units_sold ?? 0);
        });
        const top10ProdCalNames = Object.entries(prodCalAgg).sort((a, b) => b[1].total - a[1].total).slice(0, 10).map(([k]) => k);
        const prodCalendarData = {
            labels: MONTH_NAMES,
            datasets: top10ProdCalNames.map((name, i) => ({
                label: name.length > 20 ? name.slice(0, 20) + '…' : name,
                data: Array.from({ length: 12 }, (_, j) => prodCalAgg[name]?.months[j + 1] ?? 0),
                backgroundColor: PALETTE[i % PALETTE.length],
                stack: 'prodCal',
            })),
        };

        return {
            totalUnits, totalOrders, peakRow,
            catUnitsLineData, catOrdersBarData,
            catSeasonalUnitsData, catSeasonalRevData, catPeakBarData,
            catCalendarSeasonalData, prodCalendarData,
            top10Products, peakSorted, top6PeakCats,
            sortedMonthKeys,
        };
    }, [raw, clientFilter]);

    const hasData = !!(derived && (derived.sortedMonthKeys.length > 0 || derived.peakSorted.length > 0));

    // -------------------------------------------------------------------------
    // Chart option helpers
    // -------------------------------------------------------------------------

    const lineOpts = (title) => ({
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { position: 'bottom' }, title: { display: !!title, text: title } },
        scales: { y: { beginAtZero: true } },
    });

    const barOpts = (title, stacked = false) => ({
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { position: 'bottom' }, title: { display: !!title, text: title } },
        scales: { x: { stacked }, y: { stacked, beginAtZero: true } },
    });

    const currencyBarOpts = (title) => ({
        responsive: true, maintainAspectRatio: false,
        plugins: {
            legend: { position: 'bottom' },
            title:  { display: !!title, text: title },
            tooltip: { callbacks: { label: (ctx) => fmt.currency(ctx.raw) } },
        },
        scales: { y: { beginAtZero: true, ticks: { callback: (v) => fmt.compact(v) } } },
    });

    // -------------------------------------------------------------------------
    // Render states
    // -------------------------------------------------------------------------

    if (loading && pipelineStatus !== 'running') {
        return (
            <div className="flex items-center justify-center min-h-[60vh]">
                <ProgressSpinner />
            </div>
        );
    }

    if (error) {
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
                    <div className="text-center">
                        <i className="pi pi-exclamation-circle text-5xl text-red-400 mb-3 block" />
                        <p className="text-gray-700 font-medium text-lg">Something went wrong</p>
                        <p className="text-gray-500 text-sm mt-1">Unable to load analytics data. Please try again later.</p>
                    </div>
                </div>
            </div>
        );
    }

    if (!hasData) {
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
    if (!derived) return null;
    const {
        totalUnits, totalOrders, peakRow,
        catUnitsLineData, catOrdersBarData,
        catSeasonalUnitsData, catSeasonalRevData, catPeakBarData,
        top10Products, peakSorted, top6PeakCats,
    } = derived;

    const staticNote = isFiltered ? (
        <p className="text-xs text-amber-600 italic mb-4">
            * Static aggregates are computed over all historical data and are not affected by the selected date range.
        </p>
    ) : null;

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

            {/* ── KPI Cards ──────────────────────────────────────────────── */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <KPICard
                    icon="pi-chart-line" iconBg="bg-blue-100" iconColor="text-blue-600"
                    value={fmt.number(totalUnits)}
                    label={isFiltered ? 'Units Sold (filtered)' : 'Total Units Sold'}
                />
                <KPICard
                    icon="pi-shopping-cart" iconBg="bg-green-100" iconColor="text-green-600"
                    value={fmt.number(totalOrders)}
                    label={isFiltered ? 'Orders (filtered)' : 'Total Orders'}
                />
                <KPICard
                    icon="pi-star-fill" iconBg="bg-amber-100" iconColor="text-amber-600"
                    value={peakRow?.category ?? '—'}
                    label="Highest Peak Revenue Category *"
                />
                <KPICard
                    icon="pi-calendar" iconBg="bg-purple-100" iconColor="text-purple-600"
                    value={peakRow ? MONTH_NAMES[(peakRow.peak_season_month ?? 1) - 1] : '—'}
                    label="Top Category Peak Month *"
                />
            </div>

            {/* ── Monthly Trends (time-filtered) ─────────────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-blue-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">
                        Monthly Trends{isFiltered ? ' (Date Filtered)' : ''}
                    </h2>
                </div>

                <div className="grid grid-cols-1 gap-6">
                    <ChartWrapper title="Top 5 Categories — Units Sold per Month" height={380}>
                        <Line
                            data={catUnitsLineData}
                            options={lineOpts('Top 5 Categories — Units Sold per Month')}
                        />
                    </ChartWrapper>

                    <ChartWrapper title="Top 5 Categories — Orders per Month" height={380}>
                        <Bar
                            data={catOrdersBarData}
                            options={barOpts('Top 5 Categories — Orders per Month')}
                        />
                    </ChartWrapper>
                </div>

                {/* Top 10 products table */}
                <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                            Top 10 Products by Units Sold{isFiltered ? ' (Date Filtered)' : ''}
                        </h3>
                        <DataTable
                            value={top10Products}
                            paginator rows={10} rowsPerPageOptions={[10, 25]}
                            className="p-datatable-sm" stripedRows sortMode="multiple"
                        >
                            <Column field="product_name" header="Product"    sortable style={{ minWidth: '220px' }} />
                            <Column field="category"     header="Category"   sortable />
                            <Column field="brand"        header="Brand"      sortable />
                            <Column field="units"        header="Units Sold" sortable body={(r) => fmt.number(r.units)} />
                            <Column field="orders"       header="Orders"     sortable body={(r) => fmt.number(r.orders)} />
                        </DataTable>
                    </div>
                </Card>
            </section>

            {/* ── Seasonal Patterns (static aggregates) ─────────────────── */}
            <section className="space-y-6">
                <div className="flex items-center gap-3">
                    <div className="h-1 w-8 bg-purple-500 rounded-full" />
                    <h2 className="text-xl font-bold text-gray-800">Seasonal Patterns *</h2>
                </div>
                {staticNote}

                {/* Stacked bar — category units by calendar month */}
                <ChartWrapper title="Category Units Sold by Calendar Month *" height={420}>
                    <Bar
                        data={catSeasonalUnitsData}
                        options={barOpts('Category Units Sold by Calendar Month — Seasonal Pattern *', true)}
                    />
                </ChartWrapper>

                {/* Revenue by calendar month */}
                <ChartWrapper title="Total Revenue by Calendar Month *" height={360}>
                    <Bar
                        data={catSeasonalRevData}
                        options={currencyBarOpts('Total Revenue by Calendar Month — Seasonal Pattern *')}
                    />
                </ChartWrapper>

                {/* Category peak season revenue */}
                <ChartWrapper title="Category Peak Season Revenue *" height={360}>
                    <Bar
                        data={catPeakBarData}
                        options={currencyBarOpts('Category Peak Season Revenue *')}
                    />
                </ChartWrapper>
            </section>

            {/* ── Category Peak Season Metrics Cards (static) ───────────── */}
            {top6PeakCats.length > 0 && (
                <section className="space-y-4">
                    <div className="flex items-center gap-3">
                        <div className="h-1 w-8 bg-amber-500 rounded-full" />
                        <h2 className="text-xl font-bold text-gray-800">Peak Season by Category *</h2>
                    </div>
                    {staticNote}
                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                        {top6PeakCats.map((r) => (
                            <MetricsCard
                                key={r.category}
                                title={`${r.category} *`}
                                rows={[
                                    { label: 'Peak Month',    value: MONTH_NAMES[(r.peak_season_month ?? 1) - 1],  show: true },
                                    { label: 'Peak Revenue',  value: fmt.currency(r.peak_season_revenue),           show: true },
                                    { label: 'Peak Units',    value: fmt.number(r.units_sold),                      show: true },
                                    { label: 'Peak Orders',   value: fmt.number(r.orders_count),                   show: true },
                                ]}
                            />
                        ))}
                    </div>
                </section>
            )}

            {/* ── Category Peak Season Table (static) ───────────────────── */}
            <Card className="bg-white border border-gray-200 rounded-xl shadow-sm">
                <div className="p-6">
                    <h3 className="text-xl font-semibold text-gray-900 mb-4 pb-3 border-b border-gray-200">
                        Category Peak Season Summary *
                    </h3>
                    {staticNote}
                    <DataTable
                        value={peakSorted}
                        paginator rows={10} rowsPerPageOptions={[10, 25]}
                        className="p-datatable-sm" stripedRows sortMode="multiple"
                    >
                        <Column field="category"             header="Category"        sortable />
                        <Column
                            field="peak_season_month" header="Peak Month" sortable
                            body={(r) => MONTH_NAMES[(r.peak_season_month ?? 1) - 1]}
                        />
                        <Column
                            field="units_sold"  header="Peak Units Sold" sortable
                            body={(r) => fmt.number(r.units_sold)}
                        />
                        <Column
                            field="orders_count" header="Peak Orders" sortable
                            body={(r) => fmt.number(r.orders_count)}
                        />
                        <Column
                            field="peak_season_revenue" header="Peak Revenue" sortable
                            body={(r) => fmt.currency(r.peak_season_revenue)}
                        />
                    </DataTable>
                </div>
            </Card>

            {/* ── Category Calendar Month Seasonality (static) ───────────── */}
            {(derived?.catCalendarSeasonalData?.datasets?.length ?? 0) > 0 && (
                <section className="mb-8">
                    <div className="flex items-center gap-2 mb-4">
                        <span className="text-2xl">📅</span>
                        <h2 className="text-xl font-bold text-gray-800">Category Calendar Seasonality *</h2>
                    </div>
                    {staticNote}
                    <ChartWrapper title="Category Units by Calendar Month (Orders Volume) *" height={360}>
                        <Bar
                            data={derived.catCalendarSeasonalData}
                            options={barOpts('Category Units Sold by Calendar Month (Order Volume) *', true)}
                        />
                    </ChartWrapper>
                </section>
            )}

            {/* ── Product Calendar Month Seasonality (static) ────────────── */}
            {(derived?.prodCalendarData?.datasets?.length ?? 0) > 0 && (
                <section className="mb-8">
                    <div className="flex items-center gap-2 mb-4">
                        <span className="text-2xl">🛍️</span>
                        <h2 className="text-xl font-bold text-gray-800">Product Calendar Seasonality (Top 10) *</h2>
                    </div>
                    {staticNote}
                    <ChartWrapper title="Top 10 Products — Units Sold by Calendar Month *" height={360}>
                        <Bar
                            data={derived.prodCalendarData}
                            options={barOpts('Top 10 Products Units Sold by Calendar Month *', true)}
                        />
                    </ChartWrapper>
                </section>
            )}
        </div>
    );
}
