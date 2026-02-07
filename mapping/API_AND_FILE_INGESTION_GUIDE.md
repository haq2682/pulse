# API Format Validation and Multi-Table File Ingestion

## Overview

This document describes the API format validation and multi-table file ingestion enhancements implemented in the Pulse mapping system.

## 1. API Format Validation

### What Changed

The API ingestion service now validates incoming data to ensure it follows the correct format. This prevents malformed data from entering the pipeline.

### Expected API Format

The API endpoint must return data in the following format:

```json
{
  "tables": [
    {
      "table_name": "users",
      "data": [
        {
          "id": 1,
          "name": "Alice",
          "email": "alice@example.com"
        },
        {
          "id": 2,
          "name": "Bob",
          "email": "bob@example.com"
        }
      ]
    },
    {
      "table_name": "orders",
      "data": [
        {
          "order_id": 101,
          "user_id": 1,
          "amount": 250
        },
        {
          "order_id": 102,
          "user_id": 2,
          "amount": 180
        }
      ]
    }
  ]
}
```

### Key Requirements

1. **Root object must have a `tables` key** containing an array
2. **Each table must have:**
   - `table_name` (string): Name of the table
   - `data` (array): Array of record objects
3. **Table names:**
   - Must not be empty
   - Can only contain alphanumeric characters, underscores, or hyphens
   - Will be automatically converted to lowercase

### Error Messages

If the API data doesn't match the expected format, you'll see an error message like:

```
API data validation error: Invalid API data format: ...
Expected format: {... example format ...}
```

The error message includes:
- Specific validation errors
- An example of the correct format

## 2. Multi-Table File Ingestion

### What Changed

The file loader now supports multiple tables in a single file for certain formats.

### Supported Scenarios

#### Scenario 1: Excel Files with Multiple Sheets ✅

If you upload an Excel file with multiple sheets, each sheet will be treated as a separate table:

```
customers.xlsx:
  - Sheet "customers" → customers table
  - Sheet "orders" → orders table
```

**How it works:**
1. Each sheet name is normalized to match canonical table names
2. Empty sheets are automatically skipped
3. Each sheet becomes a separate DataFrame in the pipeline

**Example:**
```python
# Excel file with sheets: "Customers", "Orders", "Products"
# Results in:
# - customers_df
# - orders_df  
# - products_df
```

#### Scenario 2: JSON Files with Table Structure ✅

**Format A: API-style format**
```json
{
  "tables": [
    {
      "table_name": "customers",
      "data": [{"id": 1, "name": "Alice"}]
    },
    {
      "table_name": "orders",
      "data": [{"order_id": 101}]
    }
  ]
}
```

**Format B: Nested table structure**
```json
{
  "customers": [
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"}
  ],
  "orders": [
    {"order_id": 101, "customer_id": 1},
    {"order_id": 102, "customer_id": 2}
  ]
}
```

**Format C: Traditional single-table array**
```json
[
  {"id": 1, "name": "Alice"},
  {"id": 2, "name": "Bob"}
]
```

#### Scenario 3: CSV/Parquet Files

**Single table per file** (recommended):
```
customers.csv - contains only customer data
orders.csv - contains only order data
```

**Unified format** (advanced users):
```csv
customers__customer_id,customers__name,orders__order_id,orders__amount
1,Alice,101,250
2,Bob,102,180
```

Columns must follow the `table__column` naming convention.

## 3. Usage Examples

### API Ingestion

**Setting up your API endpoint:**

```python
from flask import Flask, jsonify

app = Flask(__name__)

@app.route('/api/data')
def get_data():
    return jsonify({
        "tables": [
            {
                "table_name": "customers",
                "data": [
                    {"customer_id": "1", "name": "Alice"},
                    {"customer_id": "2", "name": "Bob"}
                ]
            },
            {
                "table_name": "orders",
                "data": [
                    {"order_id": "101", "customer_id": "1", "amount": 250}
                ]
            }
        ]
    })
```

**Running the API ingestion:**

```bash
cd mapping
# Edit run_mapping.py CONFIG section:
# - Set mode to "api"
# - Set api_url to your endpoint
python run_mapping.py
```

### File Ingestion

**Option 1: Separate files (recommended)**

```
bucket/ingested/
  ├── customers.csv
  ├── orders.csv
  └── products.csv
```

**Option 2: Excel with multiple sheets**

```
bucket/ingested/
  └── ecommerce_data.xlsx
      ├── Sheet: customers
      ├── Sheet: orders
      └── Sheet: products
```

**Option 3: JSON with nested tables**

```json
// ecommerce_data.json
{
  "customers": [...],
  "orders": [...],
  "products": [...]
}
```

**Running batch ingestion:**

```bash
cd mapping
# Edit run_mapping.py CONFIG section:
# - Set mode to "batch"
python run_mapping.py
```

## 4. Validation and Testing

### Running Validation Tests

```bash
cd mapping
python test_validation.py
```

This will test:
- API format validation with various valid/invalid inputs
- Multi-table JSON structure detection
- Error handling

### Expected Output

```
============================================================
Testing API Data Validation
============================================================

1. Testing valid API data...
✅ Valid data accepted
   Tables: ['users', 'orders']

2. Testing invalid data (missing 'tables' key)...
✅ Correctly rejected: ...

[Additional test results]

============================================================
✅ All tests completed
============================================================
```

## 5. Migration Guide

### If you were using the old API format

**Old format (deprecated):**
```json
{
  "tables": [
    {
      "name": "users",  // ❌ Wrong field name
      "data": [...]
    }
  ]
}
```

**New format (required):**
```json
{
  "tables": [
    {
      "table_name": "users",  // ✅ Correct field name
      "data": [...]
    }
  ]
}
```

**Action required:** Update your API endpoint to use `table_name` instead of `name`.

### If you were uploading Excel files

No changes required! The system will now automatically handle multiple sheets if present.

### If you were uploading JSON files

No changes required! The system will automatically detect:
- API-style format with `tables` key
- Nested table structure
- Traditional single-table arrays

## 6. Troubleshooting

### "API data validation error"

**Problem:** Your API response doesn't match the expected format.

**Solution:** 
1. Check the error message for specific validation issues
2. Compare your response to the expected format shown in the error
3. Ensure you're using `table_name` (not `name`)
4. Ensure `tables` is an array with at least one table

### "No match found for [filename], skipping"

**Problem:** The file name doesn't map to a canonical table name.

**Solution:**
1. Rename your file to match a known table name (e.g., `customers.csv`)
2. For Excel: Rename sheets to match table names
3. For JSON: Use the API format with explicit `table_name` fields

### "Unknown table '[name]' in column"

**Problem:** Using unified format with invalid table names in column prefixes.

**Solution:** Ensure all columns follow the `table__column` format with valid table names.

## 7. Best Practices

1. **For API ingestion:** Always use the validated format with `table_name` field
2. **For file ingestion:** Use separate files per table when possible
3. **For Excel files:** Name sheets after canonical table names
4. **For JSON files:** Use the API format or nested structure for multi-table data
5. **Testing:** Run validation tests after making changes to your data format

## 8. Technical Details

### Validation Implementation

- Uses Pydantic v2 for schema validation
- Validates data structure before processing
- Provides detailed error messages with examples
- Normalizes table names to lowercase

### File Loading Implementation

- Excel: Uses pandas to read all sheets
- JSON: Auto-detects structure (API format, nested, or array)
- CSV/Parquet: Single table per file
- Multi-table files return dict of DataFrames

### Performance Considerations

- Validation adds minimal overhead (< 1ms per request)
- Multi-sheet Excel files are processed sequentially
- Empty sheets/tables are automatically skipped
- DataFrames are cached after creation
