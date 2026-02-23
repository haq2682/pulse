#!/usr/bin/env node
/**
 * Script to update all analytics components to use dynamic currency from CurrencyContext
 *
 * This script:
 * 1. Finds all analytics page components
 * 2. Adds the useCurrency import
 * 3. Updates the fmt currency formatter to use dynamic currency
 */

const fs = require('fs');
const path = require('path');

const ANALYTICS_PAGES_DIR = path.join(__dirname, '../../frontend/src/pages/dashboard/analytics/pages');

// List of files to update
const filesToUpdate = [
    'CustomerValueAnalysis.jsx',
    'CustomerOverview.jsx',
    'CustomerHealthRetention.jsx',
    'CustomerSegmentation.jsx',
    'ExecutiveOverview.jsx',
    'EngagementMetrics.jsx',
    'EngagementBehavior.jsx',
    'EngagementConversion.jsx',
    'FunnelCheckout.jsx',
    'FunnelCart.jsx',
    'FunnelOverview.jsx',
    'FunnelWishlist.jsx',
    'InventoryEfficiency.jsx',
    'InventoryHealth.jsx',
    'InventorySupplier.jsx',
    'InventoryReorderManagement.jsx',
    'OperationsProcessing.jsx',
    'OperationsShipping.jsx',
    'OperationsDelivery.jsx',
    'MarketingCampaigns.jsx',
    'MarketingChannels.jsx',
    'MarketingAttribution.jsx',
    'PaymentFinancialMetrics.jsx',
    'PaymentMethods.jsx',
    'PaymentRefunds.jsx',
    'ProductEngagement.jsx',
    // 'ProductPerformance.jsx', // Already updated
    'ProductProfitability.jsx',
    'ProductTrends.jsx',
    'SupplierEconomics.jsx',
    'SupplierOperations.jsx',
    'SupplierPerformance.jsx',
    'ReviewsOverview.jsx',
    'ReviewsImpact.jsx',
    'ReviewsSentiment.jsx',
    'RecommendationsProductAffinity.jsx',
    'RecommendationsCategoryAffinity.jsx',
    'RecommendationsCoverage.jsx',
    'Forecasts.jsx',
];

function updateFile(filePath) {
    console.log(`Updating ${path.basename(filePath)}...`);

    let content = fs.readFileSync(filePath, 'utf8');

    // Check if file has currency formatting
    if (!content.includes('currency:') && !content.includes('Currency') && !content.includes('\\$')) {
        console.log(`  Skipped: No currency formatting found`);
        return false;
    }

    // Check if already updated
    if (content.includes('useCurrency')) {
        console.log(`  Skipped: Already updated`);
        return false;
    }

    // 1. Add useCurrency import
    const currencyContextImport = "import { useCurrency } from '@/context/CurrencyContext';";

    // Find where to insert the import (after other context imports or before the first non-import line)
    if (content.includes("from '@/context/")) {
        // Insert after existing context imports
        content = content.replace(
            /(import.*from '@\/context\/.*';)/,
            `$1\n${currencyContextImport}`
        );
    } else if (content.includes("from '@/")) {
        // Insert before other @ imports
        content = content.replace(
            /(import.*from '@\/)/,
            `${currencyContextImport}\n$1`
        );
    }

    // 2. Find the component function and add useCurrency hook
    // Look for common patterns like: const ComponentName = () => {
    const componentMatch = content.match(/const (\w+) = \(\) => \{[\s\S]*?const.*?useParams/);
    if (componentMatch) {
        const hookInsertPoint = content.indexOf('useParams');
        if (hookInsertPoint !== -1) {
            // Find the end of the hooks section (look for first useState or other pattern)
            const hooksEndMatch = content.slice(hookInsertPoint).match(/\n\s+const \[/);
            if (hooksEndMatch) {
                const insertPos = hookInsertPoint + hooksEndMatch.index;
                content = content.slice(0, insertPos) +
                          '\n    const { formatCurrency } = useCurrency();\n' +
                          content.slice(insertPos);
            }
        }
    }

    // 3. Update fmt object to use dynamic currency
    // Pattern 1: const fmt = { currency: (v) => ... }
    const fmtPattern = /const fmt = \{[\s\S]*?currency: \(v\) => new Intl\.NumberFormat\([^}]+\}\.format\(v \?\? 0\)/;
    if (fmtPattern.test(content)) {
        // Move fmt inside component and make it use formatCurrency
        content = content.replace(
            fmtPattern,
            `const fmt = useMemo(() => ({\n        currency: (v) => formatCurrency(v, { minimumFractionDigits: 0, maximumFractionDigits: 0 })`
        );

        // Close the useMemo
        const fmtEndPattern = /decimal:\s+\(v, d = 2\) => \(v \?\? 0\)\.toFixed\(d\),?\n\};/;
        if (fmtEndPattern.test(content)) {
            content = content.replace(
                fmtEndPattern,
                `decimal:  (v, d = 2) => (v ?? 0).toFixed(d),\n    }), [formatCurrency]);`
            );
        }
    }

    // Pattern 2: Direct currency formatting without fmt object
    content = content.replace(
        /new Intl\.NumberFormat\('en-US', \{ style: 'currency', currency: 'USD'/g,
        "formatCurrency(v, { minimumFractionDigits: 0, maximumFractionDigits: 0 }) // "
    );

    // 4. Replace hardcoded $ symbols in template strings
    // This is trickier, only replace in specific contexts
    // Skip this for now as it's error-prone

    fs.writeFileSync(filePath, content, 'utf8');
    console.log(`  Updated successfully`);
    return true;
}

console.log('Starting to update analytics components...\n');

let updatedCount = 0;
let skippedCount = 0;

for (const fileName of filesToUpdate) {
    const filePath = path.join(ANALYTICS_PAGES_DIR, fileName);

    if (!fs.existsSync(filePath)) {
        console.log(`Warning: ${fileName} not found`);
        continue;
    }

    try {
        if (updateFile(filePath)) {
            updatedCount++;
        } else {
            skippedCount++;
        }
    } catch (error) {
        console.error(`Error updating ${fileName}:`, error.message);
    }
}

console.log(`\nDone! Updated: ${updatedCount}, Skipped: ${skippedCount}`);
