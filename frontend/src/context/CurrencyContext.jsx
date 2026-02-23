import React, { createContext, useContext, useState, useEffect } from 'react';

const CurrencyContext = createContext();

export const useCurrency = () => {
    const context = useContext(CurrencyContext);
    if (!context) {
        throw new Error('useCurrency must be used within a CurrencyProvider');
    }
    return context;
};

/**
 * Currency symbols map for common currencies
 */
const CURRENCY_SYMBOLS = {
    'USD': '$',
    'EUR': '€',
    'GBP': '£',
    'JPY': '¥',
    'CNY': '¥',
    'INR': '₹',
    'AUD': 'A$',
    'CAD': 'C$',
    'CHF': 'CHF',
    'SEK': 'kr',
    'NZD': 'NZ$',
    'KRW': '₩',
    'SGD': 'S$',
    'HKD': 'HK$',
    'NOK': 'kr',
    'MXN': 'MX$',
    'ZAR': 'R',
    'BRL': 'R$',
    'RUB': '₽',
    'TRY': '₺',
    'AED': 'د.إ',
    'SAR': '﷼',
    'THB': '฿',
    'IDR': 'Rp',
    'MYR': 'RM',
    'PHP': '₱',
    'PLN': 'zł',
    'DKK': 'kr',
    'CZK': 'Kč',
    'HUF': 'Ft',
    'ILS': '₪',
    'CLP': 'CLP$',
    'PKR': '₨',
    'NGN': '₦',
    'EGP': 'E£',
    'VND': '₫',
};

export const CurrencyProvider = ({ children, businessId }) => {
    const [currency, setCurrency] = useState('USD');
    const [currencySymbol, setCurrencySymbol] = useState('$');
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        const fetchBusinessCurrency = async () => {
            if (!businessId) {
                setLoading(false);
                return;
            }

            try {
                const apiUrl = import.meta.env.VITE_API_URL || 'http://localhost:8000';
                const response = await fetch(`${apiUrl}/analytics/get-business-currency/${businessId}`);

                if (response.ok) {
                    const data = await response.json();
                    const businessCurrency = data.currency || 'USD';
                    setCurrency(businessCurrency);
                    setCurrencySymbol(CURRENCY_SYMBOLS[businessCurrency] || businessCurrency);
                } else {
                    console.error('Failed to fetch business currency, using default USD');
                    setCurrency('USD');
                    setCurrencySymbol('$');
                }
            } catch (err) {
                console.error('Error fetching business currency:', err);
                setError(err.message);
                // Fall back to USD on error
                setCurrency('USD');
                setCurrencySymbol('$');
            } finally {
                setLoading(false);
            }
        };

        fetchBusinessCurrency();
    }, [businessId]);

    /**
     * Format a value as currency
     * @param {number} value - The numeric value to format
     * @param {Object} options - Formatting options
     * @returns {string} Formatted currency string
     */
    const formatCurrency = (value, options = {}) => {
        const {
            minimumFractionDigits = 0,
            maximumFractionDigits = 0,
            showSymbol = true
        } = options;

        if (value === null || value === undefined || isNaN(value)) {
            return showSymbol ? `${currencySymbol}0` : '0';
        }

        const formatter = new Intl.NumberFormat('en-US', {
            style: 'currency',
            currency: currency,
            minimumFractionDigits,
            maximumFractionDigits,
        });

        const formatted = formatter.format(value);

        // If showSymbol is false, remove the currency symbol
        if (!showSymbol) {
            return formatted.replace(/[^0-9,.-]/g, '').trim();
        }

        return formatted;
    };

    /**
     * Get just the currency symbol
     * @returns {string} Currency symbol
     */
    const getSymbol = () => {
        return currencySymbol;
    };

    /**
     * Get the currency code
     * @returns {string} Currency code (e.g., 'USD', 'EUR')
     */
    const getCurrencyCode = () => {
        return currency;
    };

    const value = {
        currency,
        currencySymbol,
        loading,
        error,
        formatCurrency,
        getSymbol,
        getCurrencyCode,
    };

    return (
        <CurrencyContext.Provider value={value}>
            {children}
        </CurrencyContext.Provider>
    );
};
