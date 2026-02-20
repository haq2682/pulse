import { useState, useCallback, useMemo } from 'react';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

export const QUICK_FILTERS = [
    { label: '1d', days: 1 },
    { label: '3d', days: 3 },
    { label: '7d', days: 7 },
    { label: '30d', days: 30 },
    { label: '90d', days: 90 },
];

// ---------------------------------------------------------------------------
// Pure helpers (exported so pages can use them directly if needed)
// ---------------------------------------------------------------------------

/**
 * Convert a Date to a yyyy-mm-dd string for API query params.
 */
export const toISODate = (d) => {
    if (!d) return null;
    const pad = (n) => String(n).padStart(2, '0');
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
};

/**
 * Find the value of the first matching date field on an object.
 * Accepts a single field name string OR an array of candidates tried in order.
 *
 * @param {object} item
 * @param {string | string[]} dateField
 * @returns {string | null}
 */
const resolveDateField = (item, dateField) => {
    if (!item) return null;
    const fields = Array.isArray(dateField) ? dateField : [dateField];
    for (const f of fields) {
        if (item[f] != null) return item[f];
    }
    return null;
};

/**
 * Client-side filter: keeps array items whose resolved date field falls within
 * [from, to].  Non-array values pass through unchanged.
 *
 * @param {any}              data       - Array to filter (non-arrays pass through)
 * @param {string|string[]}  dateField  - Field name(s) to check, tried in order
 * @param {Date|null}        from       - Start of range (inclusive, 00:00:00)
 * @param {Date|null}        to         - End of range (inclusive, 23:59:59)
 */
export const filterByDateRange = (data, dateField, from, to) => {
    if (!from && !to) return data;
    if (!Array.isArray(data)) return data;

    const start = from ? new Date(from).setHours(0, 0, 0, 0) : -Infinity;
    const end   = to   ? new Date(to).setHours(23, 59, 59, 999) : Infinity;

    return data.filter((item) => {
        const raw = resolveDateField(item, dateField);
        // Items with no date field are kept (they are dimension-only rows)
        if (raw == null) return true;
        const t = new Date(raw).getTime();
        return t >= start && t <= end;
    });
};

/**
 * Aggregate an array of daily rows into a single KPI summary object by
 * summing / averaging relevant numeric fields.
 *
 * Used for client-side KPI recalculation after date filtering in BATCH mode.
 */
export const aggregateDailyRows = (rows) => {
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
    const totalOrders  = sum('total_orders');

    return {
        totalRevenue,
        totalOrders,
        avgOrderValue:  totalOrders > 0 ? totalRevenue / totalOrders : 0,
        profitMargin:   avg('margin_pct'),
        grossProfit:    sum('gross_profit'),
        netProfit:      sum('net_profit'),
    };
};

// ---------------------------------------------------------------------------
// Hook
// ---------------------------------------------------------------------------

/**
 * useAnalyticsDateFilter
 *
 * Centralises all date-filter state and helpers so they can be shared across
 * any analytics page without prop-drilling or context.
 *
 * Returns:
 *   dateRange      { from: Date|null, to: Date|null }
 *   quickFilter    number|null  — currently active quick-filter (days)
 *   isFiltered     boolean
 *   applyQuickFilter(days)
 *   setDateRange({ from, to })
 *   resetFilters()
 *   clientFilter(arr, dateField?)  — memoised filtering function
 *   toISODate(date)                — convenience re-export
 */
const useAnalyticsDateFilter = () => {
    const [dateRange, setDateRange]   = useState({ from: null, to: null });
    const [quickFilter, setQuickFilter] = useState(null);

    const applyQuickFilter = useCallback((days) => {
        const to   = new Date();
        const from = new Date();
        from.setDate(from.getDate() - days);
        setQuickFilter(days);
        setDateRange({ from, to });
    }, []);

    const resetFilters = useCallback(() => {
        setQuickFilter(null);
        setDateRange({ from: null, to: null });
    }, []);

    /**
     * Memoised client-side filter bound to the current dateRange.
     *
     * @param {any[]}           arr        - Data array to filter
     * @param {string|string[]} dateField  - Field name(s) to inspect (default: 'grain_date')
     *                                       Pass an array to try multiple candidates in order,
     *                                       e.g. ['grain_date', 'campaign_date', 'start_date']
     */
    const clientFilter = useCallback(
        (arr, dateField = 'grain_date') =>
            filterByDateRange(arr, dateField, dateRange.from, dateRange.to),
        [dateRange]
    );

    const isFiltered = !!(dateRange.from || dateRange.to);

    return useMemo(
        () => ({
            dateRange,
            setDateRange,
            quickFilter,
            isFiltered,
            applyQuickFilter,
            resetFilters,
            clientFilter,
            toISODate,
        }),
        // eslint-disable-next-line react-hooks/exhaustive-deps
        [dateRange, quickFilter, isFiltered, applyQuickFilter, resetFilters, clientFilter]
    );
};

export default useAnalyticsDateFilter;