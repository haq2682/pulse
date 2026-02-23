# Currency Conversion Feature

This document describes the currency conversion implementation in the Pulse analytics engine.

## Overview

The currency conversion feature automatically converts all price-related data from the source currency to the business's target currency. This includes:

1. **Backend**: Converting price columns during the data cleaning phase
2. **Frontend**: Displaying prices in the correct currency throughout the analytics dashboard

## Architecture

### Backend Components

#### 1. Currency Converter Module (`cleaning/currency_converter.py`)

The `CurrencyConverter` class handles:
- Fetching exchange rates from api.exchangerate.host
- Caching rates in Redis for 24 hours
- Retrieving target currency from PostgreSQL based on business_id

**Key Features:**
- **API**: Uses api.exchangerate.host for real-time exchange rates
- **Caching**: Redis caching minimizes API calls (24-hour TTL)
- **Fallback**: Defaults to USD if currency not configured

**Usage Example:**
```python
from currency_converter import CurrencyConverter

# Initialize with business_id (same as bucket name)
converter = CurrencyConverter(business_id="my-business-123")

# Get target currency
target_currency = converter.get_target_currency()  # e.g., "EUR"

# Get exchange rate
rate = converter.get_exchange_rate("USD")  # Returns conversion rate

# Convert a price
converted_price = converter.convert_price(100.0, "USD")  # Converts 100 USD to EUR
```

#### 2. Cleaning Pipeline Integration (`cleaning/cleaning.py`)

The `convert_currency_columns()` function:
1. Detects source currency from the orders table (per-order basis)
2. Fetches target currency from PostgreSQL
3. Converts all price columns using cached exchange rates
4. **Preserves** the original currency column in the orders table for tracking
5. Handles missing currency values gracefully by skipping conversion for those rows

**Important Notes:**
- The `orders.currency` column is **NOT modified** - it remains in its original state
- Conversion is applied per-order based on each order's currency value
- For `order_items` and `payments`, the function joins with orders to get the appropriate exchange rate
- For tables without order linkage (`products`, `inventory`, `marketing_campaigns`, `cart_items`), conversion is only applied if all orders use the same source currency
- Rows with NULL currency values are skipped gracefully

**Price Columns Converted:**
- `orders`: subtotal, tax_amount, shipping_cost, total_discount, total_amount
- `order_items`: discount_amount, product_price (joined by order_id)
- `payments`: processing_fee, refund_amount (joined by order_id)
- `products`: cost_price, sell_price (only if single source currency)
- `inventory`: storage_cost (only if single source currency)
- `marketing_campaigns`: budget, spent_amount (only if single source currency)
- `cart_items`: unit_price, total_price (only if single source currency)

#### 3. API Endpoint (`api/routers/analytics.py`)

**Endpoint:** `GET /analytics/get-business-currency/{business_id}`

Returns the configured currency for a business:
```json
{
  "business_id": "business-123",
  "currency": "EUR"
}
```

### Frontend Components

#### 1. Currency Context (`frontend/src/context/CurrencyContext.jsx`)

The `CurrencyProvider` component:
- Fetches business currency from the API
- Provides currency formatting utilities
- Supports 40+ currencies with appropriate symbols

**Usage:**
```jsx
import { useCurrency } from '@/context/CurrencyContext';

function MyComponent() {
  const { currency, currencySymbol, formatCurrency } = useCurrency();

  return (
    <div>
      <p>Currency: {currency}</p>
      <p>Symbol: {currencySymbol}</p>
      <p>Formatted: {formatCurrency(1234.56)}</p>
    </div>
  );
}
```

#### 2. Formatters Hook (`frontend/src/hooks/useFormatters.js`)

Provides convenient formatting utilities:
- `fmt.currency(value)` - Format as currency
- `fmt.number(value)` - Format with thousand separators
- `fmt.pct(value)` - Format as percentage
- `fmt.currencyCompact(value)` - Format as compact currency (e.g., $1.5K)

**Usage:**
```jsx
import { useFormatters } from '@/hooks/useFormatters';

function ProductCard({ revenue }) {
  const fmt = useFormatters();

  return (
    <div>
      <p>Revenue: {fmt.currency(revenue)}</p>
      <p>Compact: {fmt.currencyCompact(revenue)}</p>
    </div>
  );
}
```

#### 3. Dashboard Integration (`frontend/src/pages/dashboard/index.jsx`)

The main Dashboard component wraps analytics pages with `CurrencyProvider`:
```jsx
<CurrencyProvider businessId={businessId}>
  {renderAnalyticsContent()}
</CurrencyProvider>
```

## Configuration

### Database Schema

The `businesses` table must include the `business_currency` column:
```sql
CREATE TABLE businesses (
    business_id VARCHAR(50) PRIMARY KEY,
    -- ... other columns ...
    business_currency VARCHAR(50),  -- e.g., 'USD', 'EUR', 'GBP'
    -- ... other columns ...
);
```

### Environment Variables

The following environment variables are required:

**For Backend (Python):**
```env
# Redis connection
REDIS_HOST=10.5.0.11
REDIS_PORT=6379

# PostgreSQL connection
POSTGRES_SERVER=postgresql
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
POSTGRES_DB=pulse
```

**For Frontend (React):**
```env
# API URL
VITE_API_URL=http://localhost:8000
```

## Supported Currencies

The system supports 40+ currencies including:
- USD ($), EUR (€), GBP (£)
- JPY (¥), CNY (¥), INR (₹)
- AUD, CAD, CHF, SEK, NZD
- And many more...

See `CurrencyContext.jsx` for the complete list.

## Data Flow

### Cleaning Pipeline Flow

```
1. Load data from MinIO (mapped folder)
   ↓
2. Detect unique currencies from orders.currency column
   ↓
3. Fetch target currency from PostgreSQL (businesses table)
   ↓
4. Get exchange rates for each unique source currency from api.exchangerate.host (or Redis cache)
   ↓
5. Convert price columns in orders table (per-order based on currency value)
   ↓
6. Join order_items and payments with orders to get per-order exchange rates
   ↓
7. Convert prices in order_items and payments (per-order)
   ↓
8. For tables without order linkage (products, inventory, etc.), apply conversion only if single source currency
   ↓
9. PRESERVE orders.currency column (do not modify)
   ↓
10. Skip conversion for rows with NULL currency values
   ↓
11. Save converted data to MinIO (cleaned folder)
```

**Key Behavior:**
- Orders with NULL currency values are skipped gracefully
- Each order's prices are converted based on its own currency value
- Related tables (order_items, payments) inherit the exchange rate from their parent order
- Non-order tables are only converted if all orders use the same source currency

### Frontend Data Flow

```
1. User selects business in dashboard
   ↓
2. CurrencyProvider fetches business currency from API
   ↓
3. Currency and symbol stored in React context
   ↓
4. Analytics components use useFormatters() hook
   ↓
5. All prices displayed in target currency
```

## Performance Considerations

### Caching Strategy

1. **Exchange Rates**: Cached in Redis for 24 hours
   - Key format: `exchange_rate:{target_currency}:{source_currency}`
   - Reduces API calls to exchangerate.host

2. **Currency Lookups**: Single database query per business
   - Cached in React context for session duration

### API Rate Limits

api.exchangerate.host has the following limits:
- Free tier: 250 requests/month
- Paid tier: Unlimited requests

**Recommendation**: With 24-hour Redis caching, free tier is sufficient for most use cases.

## Troubleshooting

### Issue: Currency not converting

**Check:**
1. Is `business_currency` set in the businesses table?
2. Is Redis running and accessible?
3. Check cleaning pipeline logs for errors

### Issue: Frontend showing wrong currency

**Check:**
1. Is the API endpoint `/analytics/get-business-currency/{business_id}` working?
2. Check browser console for CurrencyContext errors
3. Verify the business_id is correct

### Issue: Exchange rate API failing

**Fallback behavior:**
- System will skip conversion and log a warning
- Data will remain in source currency

## Future Enhancements

Potential improvements:
1. Support for cryptocurrency prices
2. Historical exchange rate tracking
3. Multi-currency reports (show both source and target)
4. Custom exchange rate override for specific businesses
5. Real-time currency updates via WebSocket

## References

- Exchange Rate API: https://exchangerate.host
- Redis Documentation: https://redis.io/documentation
- Supported Currency Codes: ISO 4217
