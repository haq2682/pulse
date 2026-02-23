# Scripts Directory

This directory contains utility scripts for the Pulse project.

## Removed Scripts

### update-currency-components.js (REMOVED)

This script was initially created to automatically update all analytics components to use dynamic currency formatting. However, it has been **removed** because:

1. **Better Solution**: We created the `useFormatters` hook (`frontend/src/hooks/useFormatters.js`) which provides a cleaner and more maintainable approach
2. **No Manual Updates Needed**: The `useFormatters` hook can be imported and used directly in any component
3. **Automatic Currency**: All components wrapped in `CurrencyProvider` automatically get access to the business's currency

## Using Currency Formatting in Components

Instead of running a script to update components, simply use the `useFormatters` hook:

```jsx
import { useFormatters } from '@/hooks/useFormatters';

function MyComponent() {
  const fmt = useFormatters();

  return (
    <div>
      <p>Revenue: {fmt.currency(revenue)}</p>
      <p>Count: {fmt.number(count)}</p>
      <p>Growth: {fmt.pct(growth)}</p>
    </div>
  );
}
```

All analytics pages are automatically wrapped with `CurrencyProvider` in the Dashboard component, so currency formatting will work correctly without any additional setup.

See `docs/CURRENCY_CONVERSION.md` for complete documentation.
