# Code Review: Bronze Transactions Pipeline

**File:** `pipelines/bronze_transactions_pipeline.py`  
**Reviewer:** Senior Engineer  
**Date:** 17 November 2025

---

## Executive Summary

This bronze layer ingestion pipeline has **multiple critical issues** that make it unsuitable for production use. Primary concerns include inefficient Spark operations (3x performance penalty), data loss risks from overwrite mode, and lack of idempotency. Significant refactoring is required.

---

## Critical Issues

### 1. **Inefficient DataFrame Operations** ⚠️ HIGH PRIORITY

**Lines:** 54, 66, 77

**Problem:**
```python
self.metrics["rows_raw"] = raw_df.count()          # Line 54 - Full Spark job #1
self.metrics["rows_invalid"] = self.metrics["rows_raw"] - valid_df.count()  # Line 66 - Full Spark job #2
self.metrics["rows_loaded"] = df.count()           # Line 77 - Full Spark job #3
```

Three separate `.count()` operations trigger three complete Spark jobs to scan the entire dataset.

**Impact:**
- 3x execution time on large datasets
- Unnecessary cluster resource consumption
- Poor pipeline performance

**Solution:**
- Cache the DataFrame after read and reuse it
- Collect metrics in a single pass using Spark's built-in statistics
- Use write callbacks or post-write table statistics

---

### 2. **Dangerous Write Mode** ⚠️ CRITICAL

**Line:** 75

**Problem:**
```python
self.delta_ops.write_delta_table(df, bronze_path, mode="overwrite")
```

Overwrite mode destroys all historical data on every pipeline run.

**Impact:**
- **Complete data loss** of previous ingestions
- No audit trail
- Cannot reprocess or investigate historical issues
- Violates bronze layer principles (immutable raw data storage)

**Solution:**
```python
# Option 1: Append mode with deduplication
self.delta_ops.write_delta_table(df, bronze_path, mode="append")

# Option 2: Merge/upsert based on transaction_id
self.delta_ops.merge_delta_table(
    source_df=df,
    target_path=bronze_path,
    merge_condition="source.transaction_id = target.transaction_id",
    update_set={"_ingest_ts": "source._ingest_ts"},
    insert_values=None  # Insert all columns
)
```

---

### 3. **Inadequate Data Quality Handling** ⚠️ HIGH PRIORITY

**Lines:** 64-69

**Problem:**
```python
valid_df = transformed_df.filter(col("transaction_id").isNotNull())
self.metrics["rows_invalid"] = self.metrics["rows_raw"] - valid_df.count()

if self.metrics["rows_invalid"] > 0:
    self.logger.warning(f"Rejected {self.metrics['rows_invalid']} rows...")
```

Invalid rows are silently dropped without any persistence or detailed tracking.

**Impact:**
- **Data loss** without recovery mechanism
- No way to investigate or fix bad records
- Compliance/audit issues
- Poor data lineage

**Solution:**
```python
# Separate invalid records
invalid_df = transformed_df.filter(col("transaction_id").isNull())

# Write to quarantine table
if invalid_df.count() > 0:
    quarantine_path = self.config['database']['bronze_path'] + "/transactions_quarantine"
    invalid_df.withColumn("rejection_reason", lit("Missing transaction_id")) \
              .withColumn("quarantine_ts", current_timestamp()) \
              .write.format("delta").mode("append").save(quarantine_path)
```

---

### 4. **No Idempotency** ⚠️ HIGH PRIORITY

**Problem:**
Pipeline can be run multiple times on the same data, creating duplicates if mode is changed to append.

**Impact:**
- Duplicate records in production
- Incorrect analytics and reporting
- Issues with scheduled/retry jobs

**Solution:**
```python
# Add deduplication based on transaction_id and event_timestamp
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("transaction_id").orderBy(col("_ingest_ts").desc())
deduplicated_df = transformed_df.withColumn("row_num", row_number().over(window_spec)) \
                                .filter(col("row_num") == 1) \
                                .drop("row_num")
```

---

## Design Issues

### 5. **Schema Enforcement Configuration Is Misleading**

**Lines:** 47-50

**Problem:**
```python
if schema_enforcement:
    schema = self.get_explicit_schema()
    raw_df = self.spark.read.format(file_format).schema(schema).load(source_path)
else:
    raw_df = self.spark.read.format(file_format).load(source_path)
```

Delta tables enforce schema at write time regardless of read-time schema. This toggle provides false flexibility.

**Solution:**
- Always use explicit schema for data governance
- Remove misleading configuration option
- Add schema evolution strategy if needed (`mergeSchema` option)

---

### 6. **Poor Error Handling**

**Problem:**
No try-catch blocks, no validation of file existence, no handling of schema mismatches.

**Impact:**
- Cryptic error messages
- Pipeline crashes without cleanup
- Difficult troubleshooting

**Solution:**
```python
def extract(self) -> DataFrame:
    try:
        # Validate source exists
        if not Path(source_path).exists():
            raise FileNotFoundError(f"Source file not found: {source_path}")
        
        # Read with error handling
        raw_df = self.spark.read.format(file_format).schema(schema).load(source_path)
        
    except Exception as e:
        self.logger.error(f"Failed to extract data: {str(e)}")
        raise
```

---

### 7. **Metrics Timing Inaccuracy**

**Lines:** 34, 87

**Problem:**
```python
def extract(self) -> DataFrame:
    start_time = time.time()  # Line 34 - Never used
    # ...

def run(self) -> dict:
    start_time = time.time()  # Line 87 - But extract already did work
```

Duration metric doesn't capture extraction count operation.

**Solution:**
Remove start_time from extract method, keep only in run method.

---

### 8. **Violates Base Class Contract**

**Problem:**
- Base class `run()` returns `None`
- This implementation returns `dict`
- Creates inconsistent interface

**Solution:**
Either update base class to support metrics return, or store metrics as instance variable and provide separate getter method.

---

## Code Quality Issues

### 9. **Hardcoded String Concatenation**

**Line:** 74

**Problem:**
```python
bronze_path = self.config['database']['bronze_path'] + "/transactions"
```

**Solution:**
```python
from pathlib import Path
bronze_path = str(Path(self.config['database']['bronze_path']) / "transactions")
```

---

### 10. **Missing Validation**

**Problem:**
- No check that config keys exist
- No DataFrame schema validation
- No write verification

**Solution:**
```python
# Validate config
required_keys = ['database.bronze_path', 'data_sources.transactions.path']
# Add validation logic

# Validate schema before write
expected_columns = set(['transaction_id', 'customer_id', 'event_timestamp', 'amount', 'currency'])
actual_columns = set(df.columns)
if not expected_columns.issubset(actual_columns):
    raise ValueError(f"Missing columns: {expected_columns - actual_columns}")
```

---

### 11. **Unused/Misleading Metadata Column**

**Line:** 63

**Problem:**
```python
.withColumn("_file_name", input_file_name())
```

For JSON sources, this returns the directory path, not individual file names.

**Solution:**
Remove if not useful, or add clarifying comment about expected behavior.

---

## Missing Best Practices

### 12. **No Data Partitioning Strategy**

**Problem:**
Bronze tables written without partitioning.

**Impact:**
Poor query performance, slow reads, expensive scans.

**Solution:**
```python
df.withColumn("_ingest_date", current_date())
self.delta_ops.write_delta_table(
    df, 
    bronze_path, 
    mode="append",
    partition_by=["_ingest_date"]
)
```

---

### 13. **No Post-Write Optimization**

**Problem:**
No OPTIMIZE or VACUUM operations.

**Impact:**
Degraded performance over time, small file proliferation.

**Solution:**
```python
# After write
self.delta_ops.optimize_table(bronze_path, z_order_by=["transaction_id"])
```

---

### 14. **Incomplete Metrics**

**Problem:**
Missing important metrics:
- Schema validation errors
- File count processed
- Data volume (MB/GB)
- Duplicate count
- Processing rate (rows/sec)

**Solution:**
Expand metrics dictionary with comprehensive monitoring data.

---

## Recommendations Priority Order

### Must Fix (P0)
1. ✅ Change from overwrite to append/merge mode
2. ✅ Implement deduplication/idempotency
3. ✅ Fix inefficient count operations
4. ✅ Add quarantine table for invalid records

### Should Fix (P1)
5. ✅ Add comprehensive error handling
6. ✅ Add data partitioning
7. ✅ Fix base class contract violation
8. ✅ Add path handling with Path objects

### Nice to Have (P2)
9. ✅ Add post-write optimization
10. ✅ Expand metrics collection
11. ✅ Add schema validation
12. ✅ Remove misleading schema_enforcement config

---

## Conclusion

This code demonstrates understanding of basic Spark/Delta operations but lacks production-ready robustness. The most critical issue is the **overwrite mode causing data loss**. Combined with performance issues from multiple counts and lack of idempotency, this pipeline would fail in any production environment.

**Estimated refactoring effort:** 4-6 hours  
**Risk level if deployed:** **CRITICAL** ⚠️

**Recommendation:** **Do not merge. Requires significant refactoring before production use.**
