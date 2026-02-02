# Quick Start Guide: Using the New Features

## What Was Implemented

This PR adds two major features to the Pulse mapping system:

1. **API Format Validation** - Ensures API data follows the correct structure
2. **Multi-Table File Support** - Handles multiple tables in a single file

## For Users: Quick Examples

### Example 1: Setting Up Your API Endpoint

Your API should return data in this format:

```python
from flask import Flask, jsonify

@app.route('/api/data')
def get_data():
    return jsonify({
        "tables": [
            {
                "table_name": "customers",  # Must use "table_name", not "name"
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

### Example 2: Uploading an Excel File with Multiple Tables

Create an Excel file `data.xlsx` with multiple sheets:

```
Sheet "customers":
customer_id, name, email
1, Alice, alice@example.com
2, Bob, bob@example.com

Sheet "orders":
order_id, customer_id, amount
101, 1, 250
102, 2, 180
```

Upload to MinIO bucket under `ingested/data.xlsx` - both tables will be automatically detected and mapped!

### Example 3: Using JSON with Multiple Tables

Create a JSON file with nested structure:

```json
{
  "customers": [
    {"customer_id": "1", "name": "Alice"},
    {"customer_id": "2", "name": "Bob"}
  ],
  "orders": [
    {"order_id": "101", "customer_id": "1", "amount": 250}
  ]
}
```

Upload to MinIO - both tables will be automatically split and processed!

## Running the System

### Mode 1: Batch (File Processing)

```bash
cd mapping

# Edit run_mapping.py:
# CONFIG = {
#     "mode": "batch",
#     "bucket_name": "pulse-bucket-1"
# }

python run_mapping.py
```

### Mode 2: API Ingestion

```bash
cd mapping

# Edit run_mapping.py:
# CONFIG = {
#     "mode": "api",
#     "api_url": "http://localhost:5000/api/data",
#     "bucket_name": "pulse-bucket-1"
# }

python run_mapping.py
```

## Testing Your Implementation

Run the validation tests:

```bash
cd mapping
python test_validation.py
```

Expected output:
```
============================================================
Testing API Data Validation
============================================================
1. Testing valid API data...
✅ Valid data accepted
...
============================================================
✅ All tests completed
============================================================
```

## Common Issues and Solutions

### Issue: "API data validation error"

**Cause:** Your API response doesn't match the expected format.

**Solution:** Check the error message. It will show you exactly what's wrong and provide an example of the correct format. Most common issue: using "name" instead of "table_name".

### Issue: "No match found for [filename]"

**Cause:** File name doesn't match a known table name.

**Solution:** 
- For CSV/Parquet: Rename file to match table name (e.g., `customers.csv`)
- For Excel: Rename sheets to match table names
- For JSON: Use the structured format with explicit table names

### Issue: File uploaded but no data in mapped folder

**Cause:** Either table name not recognized or data format issue.

**Solution:** 
1. Check the console output for errors
2. Verify file format matches one of the supported formats
3. For Excel: Make sure sheet names match table names
4. For JSON: Use one of the supported structures (see examples)

## File Structure Reference

```
pulse/
├── mapping/
│   ├── streaming/
│   │   └── ingestion/
│   │       ├── api_validation.py          # NEW: Validation models
│   │       └── api_ingest_service.py      # UPDATED: Uses validation
│   ├── utils/
│   │   └── file_loader.py                 # UPDATED: Multi-table support
│   ├── test_validation.py                 # NEW: Test suite
│   ├── API_AND_FILE_INGESTION_GUIDE.md    # NEW: Detailed guide
│   ├── MULTI_TABLE_INGESTION_ANALYSIS.md  # NEW: Technical analysis
│   └── IMPLEMENTATION_SUMMARY.md          # NEW: Summary
└── requirements.txt                       # UPDATED: Added pydantic
```

## What to Read Next

1. **Quick start:** This file (you're reading it!)
2. **Detailed usage:** `API_AND_FILE_INGESTION_GUIDE.md`
3. **Technical details:** `MULTI_TABLE_INGESTION_ANALYSIS.md`
4. **Summary:** `IMPLEMENTATION_SUMMARY.md`

## Support

If you encounter issues:
1. Check the error messages - they're designed to be helpful
2. Run `test_validation.py` to verify the system is working
3. Review the documentation files
4. Check console output for specific error details

## Migration Checklist

If you have existing API integrations:

- [ ] Update API endpoint to use `table_name` instead of `name`
- [ ] Test with validation test suite
- [ ] Verify data flows through to mapped folder

If you upload files:

- [ ] No changes required! 
- [ ] Optional: Try uploading Excel with multiple sheets
- [ ] Optional: Try uploading JSON with nested tables

## Quick Reference: Supported File Formats

| Format | Single Table | Multiple Tables | Notes |
|--------|--------------|-----------------|-------|
| CSV | ✅ | ⚠️ | Multi-table requires `table__column` format |
| Excel | ✅ | ✅ | Each sheet = one table |
| Parquet | ✅ | ⚠️ | Multi-table requires `table__column` format |
| JSON | ✅ | ✅ | Auto-detects structure |
| API | ✅ | ✅ | Validated format required |

Legend:
- ✅ Fully supported, automatic
- ⚠️ Supported with special format
