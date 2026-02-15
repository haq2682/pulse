# Phase 1 Quick Reference Card

## 🚀 Installation (One-time setup)

```bash
# 1. Install dependencies
pip install sqlalchemy psycopg2-binary

# 2. Configure .env file
cat >> .env << EOF
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
POSTGRES_SERVER=localhost
POSTGRES_DATABASE_NAME=pulse_db
POSTGRES_PORT=5432
EOF

# 3. Run test suite
python test_phase1.py
```

## 💻 Daily Usage

```bash
# Default: Incremental mode (process only new files)
python cleaning/cleaning.py

# Expected first run: 5-8 minutes
# Expected subsequent runs: 30-90 seconds (or 2 sec if no new files)
```

## 🔧 Command Options

```bash
# Process only new files (default)
python cleaning/cleaning.py

# Process all files (ignore state)
python cleaning/cleaning.py --full

# Reset state and reprocess everything
python cleaning/cleaning.py --force-full

# Use custom bucket
python cleaning/cleaning.py --bucket-name my-bucket
```

## 📊 Monitor State

```sql
-- How many files processed?
SELECT COUNT(*) FROM cleaning_state;

-- What was processed today?
SELECT file_path, processed_at 
FROM cleaning_state 
WHERE processed_at::date = CURRENT_DATE
ORDER BY processed_at DESC;

-- When was last run?
SELECT MAX(processed_at) FROM cleaning_state;

-- Total records processed
SELECT SUM(record_count) FROM cleaning_state;
```

## ⚡ Performance Expectations

| Scenario | Time | Improvement |
|----------|------|-------------|
| First run | 5-8 min | N/A |
| No new files | ~2 sec | 99% ✅ |
| 2 new files | 30-60 sec | 85-90% ✅ |
| 5 new files | 60-90 sec | 80-85% ✅ |

## 🔍 Troubleshooting

### Dependencies missing?
```bash
pip install sqlalchemy psycopg2-binary
```

### PostgreSQL connection error?
```bash
# Check environment variables
echo $POSTGRES_SERVER
echo $POSTGRES_USER

# Test connection
psql -h $POSTGRES_SERVER -U $POSTGRES_USER -d $POSTGRES_DATABASE_NAME
```

### State table missing?
```bash
# Create manually
psql -f sql/create_cleaning_state_table.sql

# Or let system create automatically on first run
python cleaning/cleaning.py
```

### All files reprocessed every time?
```sql
-- Check if state has records
SELECT COUNT(*) FROM cleaning_state;

-- If 0, check logs for errors
```

## 📁 Key Files

- `cleaning/cleaning.py` - Main pipeline with incremental support
- `cleaning/incremental_cleaner.py` - State tracking class
- `cleaning/cleaning_utils.py` - File path utilities
- `sql/create_cleaning_state_table.sql` - State table schema
- `test_phase1.py` - Test suite
- `PHASE1_IMPLEMENTATION_COMPLETE.md` - Full documentation

## ✅ Verify It's Working

```bash
# Test 1: First run (should process all files)
python cleaning/cleaning.py
# Expected: "Processing 15 tables", takes 5-8 min

# Test 2: Immediate second run (should skip all)
python cleaning/cleaning.py
# Expected: "No new files to process", takes ~2 sec

# Test 3: Check state
psql -c "SELECT COUNT(*) FROM cleaning_state;"
# Expected: 15 (or number of files processed)
```

## 🎯 Success = Second run completes in ~2 seconds!

---

**Need help?** See `PHASE1_IMPLEMENTATION_COMPLETE.md` for full documentation.
