# Multi-Table File Ingestion Analysis

## Question 2: Can the system handle multiple tables in a single file?

### Current Capabilities

**YES, with limitations.** The system CAN handle multiple tables in a single file, but requires specific data formats:

#### 1. **Unified DataFrame Format (Currently Supported)**
If your file uses the `table__column` naming convention:
```csv
customers__customer_id,customers__name,orders__order_id,orders__amount
1,Alice,101,250
2,Bob,102,180
```

The system automatically splits this into separate tables using `split_unified_dataframe()` function.

#### 2. **JSON with Table Structure (Partially Supported)**
JSON files can contain arrays of objects, but currently each JSON file is treated as a single table. The file loader would need enhancement to support:
```json
{
  "customers": [...],
  "orders": [...]
}
```

#### 3. **Single File, Single Table (Currently Supported)**
CSV, XLSX, Parquet, JSON files where all columns belong to one table - this works perfectly with automatic table detection.

### Limitations

**What DOESN'T work currently:**
- CSV/Excel files with mixed table columns (e.g., both `customer_id` and `order_id` in same row) WITHOUT the `table__column` prefix
- Multiple sheets in Excel where each sheet is a different table
- JSON files with nested table structure (as shown above)
- Parquet files with mixed schemas

### Recommended Approach: Standardize Ingestion Format

**Option A: Standard API Format (Recommended for API ingestion)**
```json
{
  "tables": [
    {
      "table_name": "users",
      "data": [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"}
      ]
    },
    {
      "table_name": "orders",
      "data": [
        {"order_id": 101, "user_id": 1, "amount": 250}
      ]
    }
  ]
}
```

**Option B: Unified Column Naming (Recommended for file ingestion)**
```csv
customers__customer_id,customers__name,orders__order_id,orders__user_id
1,Alice,101,1
2,Bob,102,2
```

**Option C: Separate Files (Simplest approach)**
- `customers.csv` with customer data
- `orders.csv` with order data
- System automatically detects table type by filename or column matching

### Recommendation for Implementation

**For API Ingestion:**
- ✅ Use the standardized format (Option A) - already partially implemented
- ✅ Add validation to ensure format compliance
- ✅ This is the cleanest approach for API data

**For File Ingestion:**
- ✅ Option C (separate files) - works perfectly now
- ⚠️ Option B (unified format) - works but requires preprocessing
- ❌ Option A (JSON structure) - would require significant enhancement

### Proposed Enhancements (If Multi-Table Single File Support is Required)

If you need to support multiple tables in a single file WITHOUT the `table__column` prefix:

1. **Intelligent Column-Based Splitting**
   - Analyze all columns in the file
   - Group columns by their table affinity using the mapping algorithms
   - Split the dataframe into multiple tables
   - Risk: Ambiguous columns that could belong to multiple tables

2. **Excel Multi-Sheet Support**
   - Read each sheet as a separate table
   - Use sheet name to infer table type
   - This is straightforward to implement

3. **JSON Nested Structure Support**
   - Parse JSON with table keys
   - Each key becomes a separate table
   - Easy to implement for JSON files

### Conclusion

**The system CAN handle multiple tables in files, but the approach depends on the file format:**
- **API**: Use the standardized JSON format with `tables` array ✅ (needs validation)
- **Files**: Use separate files per table ✅ (already works)
- **Advanced**: Implement intelligent splitting for mixed-column files ⚠️ (complex, error-prone)

**My recommendation:** 
1. Implement API format validation (straightforward)
2. Add Excel multi-sheet support (simple enhancement)
3. Add JSON nested structure support (simple enhancement)
4. Document the unified `table__column` format for advanced users
5. Keep separate files as the primary recommendation for file ingestion
