import React from 'react';
import { Calendar } from 'primereact/calendar';
import SecondaryButton from '@/components/global/Button/SecondaryButton';
import { QUICK_FILTERS } from '@/hooks/useAnalyticsDateFilter';

/**
 * DateFilterBar
 *
 * A fully self-contained filter bar that can be dropped into any analytics
 * page.  All state lives in the parent via useAnalyticsDateFilter().
 *
 * Props:
 *   quickFilter   number|null     — currently active quick filter (days)
 *   dateRange     { from, to }    — current date range
 *   isFiltered    boolean
 *   onQuickFilter (days) => void
 *   onDateChange  ({ from, to }) => void
 *   onReset       () => void
 *   dataMode      string          — optional badge ('batch', 'db', 'api')
 *   hidden        boolean         — hides the bar (e.g. during pipeline loading)
 */
const DateFilterBar = ({
    quickFilter,
    dateRange,
    isFiltered,
    onQuickFilter,
    onDateChange,
    onReset,
    dataMode,
    hidden = false,
}) => {
    if (hidden) return null;

    return (
        <div className="mb-6 p-4 bg-white rounded-lg shadow-sm">
            <div className="flex flex-wrap items-center gap-3">
                <span className="text-sm font-medium text-gray-700">Filter by:</span>

                {QUICK_FILTERS.map(({ label, days }) => (
                    <SecondaryButton
                        key={label}
                        label={label}
                        size="small"
                        outlined
                        onClick={() => onQuickFilter(days)}
                        className={quickFilter === days ? 'p-button-primary' : ''}
                    />
                ))}

                <span className="mx-2 text-gray-400">|</span>

                <Calendar
                    value={dateRange.from}
                    onChange={(e) => onDateChange({ ...dateRange, from: e.value })}
                    placeholder="From Date"
                    showIcon
                    dateFormat="yy-mm-dd"
                    className="w-auto"
                />
                <Calendar
                    value={dateRange.to}
                    onChange={(e) => onDateChange({ ...dateRange, to: e.value })}
                    placeholder="To Date"
                    showIcon
                    dateFormat="yy-mm-dd"
                    className="w-auto"
                />

                {isFiltered && (
                    <SecondaryButton
                        label="Reset"
                        size="small"
                        severity="secondary"
                        onClick={onReset}
                    />
                )}

                {/* Mode badge — helpful for debugging; remove in prod if desired */}
                {dataMode && dataMode !== 'unknown' && (
                    <span className="ml-auto text-xs px-2 py-1 rounded-full bg-gray-100 text-gray-500 font-mono uppercase">
                        {dataMode} mode
                    </span>
                )}
            </div>
        </div>
    );
};

export default DateFilterBar;
