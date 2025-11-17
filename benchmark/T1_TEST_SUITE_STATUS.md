# T1 Test Suite - Quick Reference

## Overview
Created test suite for T1 (Bronze Transaction Ingestion) with 11 tests across 3 test classes.

## What We Have

### ✅ Sample Data
- `data/raw_seed/transactions.json` - 1070 transaction records
- Format: JSON with fields: transaction_id, customer_id, event_timestamp, amount, currency

### ✅ Test Suite
- `tests/conftest.py` - Shared fixtures (Spark session, test data paths, cleanup)
- `tests/test_t1_bronze_ingestion.py` - 11 tests validating T1 acceptance criteria

## Test Coverage

### TestT1BronzeIngestion (7 tests)
1. **test_bronze_table_exists** - Verifies Delta table created
2. **test_required_columns_present** - Checks all required columns exist
3. **test_row_count_validation** - Validates row count (input - invalid = output)
4. **test_metadata_columns_populated** - Ensures _ingest_ts and _file_name are non-null
5. **test_transaction_id_uniqueness** - Validates no null transaction_ids
6. **test_data_types** - Checks column data types are appropriate
7. **test_invalid_records_rejected** - Verifies rows without transaction_id are rejected

### TestT1Metrics (2 tests)
8. **test_metrics_file_created** - Checks metrics JSON exists
9. **test_metrics_accuracy** - Validates metrics match actual data

### TestT1Configuration (2 tests)
10. **test_no_hardcoded_paths** - Validates configuration-driven approach
11. **test_reuses_pipeline_patterns** - Checks for code reuse

## Running Tests

```bash
# Collect tests (verify they load)
python -m pytest tests/ --collect-only

# Run all T1 tests
python -m pytest tests/test_t1_bronze_ingestion.py -v

# Run with coverage
python -m pytest tests/test_t1_bronze_ingestion.py --cov=pipelines --cov-report=term

# Run specific test
python -m pytest tests/test_t1_bronze_ingestion.py::TestT1BronzeIngestion::test_required_columns_present -v
```

## Current Status

**Tests Collected**: 11 tests found successfully  
**Tests Passing**: Most tests use `pytest.skip()` until implementation exists  
**Purpose**: These tests will validate AI-generated implementations during benchmarking

## Next Steps

To complete Option B (Minimal Viable T1 Benchmark):

1. ✅ **Test Suite** - COMPLETE
2. ⏭️ **Create `analyze_task_diff.py`** - Diff analysis script
3. ⏭️ **Update Makefile** - Ensure `make test` works properly
4. ⏭️ **Create scorecard template** - YAML template for manual scoring
5. ⏭️ **Update `run_task.sh`** - Actually execute implementations (if needed)

## Notes

- Tests are implementation-agnostic (test outputs, not internals)
- Use `python -m pytest` instead of `pytest` command to ensure correct environment
- Some tests require actual implementation to run (currently skipped)
- Tests validate acceptance criteria from `benchmark/tasks/T1_ingest_transactions.yaml`
