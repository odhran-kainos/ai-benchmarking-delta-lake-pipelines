# Tests Directory

This directory contains test suites for validating AI-generated pipeline implementations.

## Structure

- `conftest.py` - Pytest configuration and shared fixtures
- `test_t1_bronze_ingestion.py` - Tests for T1 task (Bronze ingestion)
- `test_t2_silver_enrichment.py` - Tests for T2 task (Silver enrichment) [TODO]

## Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=pipelines --cov-report=term

# Run specific test file
pytest tests/test_t1_bronze_ingestion.py -v

# Run specific test
pytest tests/test_t1_bronze_ingestion.py::TestT1BronzeIngestion::test_required_columns_present -v
```

## Test Philosophy

These tests validate **acceptance criteria** from task specifications, not implementation details:

1. **Correctness**: Does the output match requirements?
2. **Data Quality**: Are validations working?
3. **Schema Compliance**: Are required columns present with correct types?
4. **Metrics**: Are metrics accurately reported?

Tests should work with any correct implementation approach.

## Writing New Tests

When adding tests for new tasks:

1. Read the task YAML specification in `benchmark/tasks/`
2. Convert each acceptance criterion into a test
3. Use fixtures from `conftest.py` for Spark session and test data
4. Keep tests implementation-agnostic (test outputs, not internals)
5. Use `pytest.skip()` for tests that require actual implementations

## Test Data

Tests use sample data from `data/raw_seed/`:
- `transactions.json` - 1070 transaction records for T1

Additional test data generated in `conftest.py` fixtures as needed.
