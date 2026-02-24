import { useMemo } from 'react';
import { useCurrency } from '@/context/CurrencyContext';

/**
 * Custom hook that provides formatting utilities including dynamic currency
 *
 * @returns {Object} Formatting functions
 */
export const useFormatters = () => {
    const { formatCurrency } = useCurrency();

    const fmt = useMemo(() => ({
        /**
         * Format a value as currency using the business's configured currency
         * @param {number} v - Value to format
         * @param {Object} options - Formatting options
         * @returns {string} Formatted currency string
         */
        currency: (v, options = {}) => {
            const {
                minimumFractionDigits = 0,
                maximumFractionDigits = 0
            } = options;
            return formatCurrency(v, { minimumFractionDigits, maximumFractionDigits });
        },

        /**
         * Format a number with thousand separators
         * @param {number} v - Value to format
         * @returns {string} Formatted number string
         */
        number: (v) => new Intl.NumberFormat('en-US').format(v ?? 0),

        /**
         * Format a value as percentage
         * @param {number} v - Value to format (e.g., 0.5 for 50%)
         * @param {number} decimals - Number of decimal places
         * @returns {string} Formatted percentage string
         */
        pct: (v, decimals = 2) => `${(v ?? 0).toFixed(decimals)}%`,
        probToPct: (v, decimals = 2) => `${((v ?? 0) * 100).toFixed(decimals)}%`,

        /**
         * Format a value as percentage (raw value, e.g., 50 for 50%)
         * @param {number} v - Value to format
         * @param {number} decimals - Number of decimal places
         * @returns {string} Formatted percentage string
         */
        pctRaw: (v, decimals = 1) => `${(v ?? 0).toFixed(decimals)}%`,
        pct100: (v) => `${(+(v ?? 0)).toFixed(2)}%`,
        probToPct100: (v) => `${((+(v ?? 0)) * 100).toFixed(2)}%`,

        /**
         * Format a decimal number
         * @param {number} v - Value to format
         * @param {number} d - Number of decimal places
         * @returns {string} Formatted decimal string
         */
        decimal: (v, d = 2) => (v ?? 0).toFixed(d),

        /**
         * Format a compact number (e.g., 1.5K, 2.3M)
         * @param {number} v - Value to format
         * @returns {string} Formatted compact number
         */
        compact: (v) => {
            if (v === null || v === undefined || isNaN(v)) return '0';
            return new Intl.NumberFormat('en-US', {
                notation: 'compact',
                compactDisplay: 'short',
                maximumFractionDigits: 1
            }).format(v);
        },

        /**
         * Format a compact currency value (e.g., $1.5K, $2.3M)
         * @param {number} v - Value to format
         * @returns {string} Formatted compact currency
         */
        currencyShort: (v) => {
            if (v === null || v === undefined || isNaN(v)) {
                return formatCurrency(0, { minimumFractionDigits: 0, maximumFractionDigits: 0 });
            }
            // Format the compact number and combine with currency symbol
            const compactNum = new Intl.NumberFormat('en-US', {
                notation: 'compact',
                compactDisplay: 'short',
                maximumFractionDigits: 1
            }).format(Math.abs(v));

            // Get just the symbol
            const currencyStr = formatCurrency(1, { minimumFractionDigits: 0, maximumFractionDigits: 0 });
            const symbol = currencyStr.replace(/[0-9,]/g, '').trim();

            return v < 0 ? `-${symbol}${compactNum}` : `${symbol}${compactNum}`;
        },

        hours: (v) => `${(v ?? 0).toFixed(1)} hrs`,
        days: (v) => `${(v ?? 0).toFixed(1)} days`,
    }), [formatCurrency]);

    return fmt;
};
