# Best Practices: PySpark Delta Lake Bronze Layer Pipelines

This document provides production-ready guidelines for building robust, performant, and maintainable bronze layer data ingestion pipelines using PySpark and Delta Lake.

---

## 1. Performance Optimization

### Minimize DataFrame Materialization

**❌ AVOID: Multiple count() operations**
```python
# BAD - Triggers 3 separate Spark jobs
rows_raw = df.count()
rows_valid = valid_df.count()
rows_loaded = final_df.count()
```

**✅ DO: Collect metrics efficiently**
```python
# GOOD - Cache and reuse, or collect stats in single pass
df.cache()
rows_raw = df.count()

# Or use write callbacks and Delta table statistics
df.write.format("delta").save(path)
loaded_count = spark.read.format("delta").load(path).count()

# Or calculate metrics during transformation
df.groupBy().agg(
    count("*").alias("total"),
    count(when(col("id").isNull(), 1)).alias("invalid")
)
```

**Rationale:** Each `.count()` triggers a full dataset scan. Cache DataFrames if you need multiple actions, or design transformations to collect all metrics in one pass.

### Leverage DataFrame Caching Strategically

```python
# Cache after expensive operations, before multiple actions
raw_df = spark.read.format("json").schema(schema).load(path)
raw_df.cache()

# Now multiple operations use cached data
row_count = raw_df.count()
sample_data = raw_df.limit(100).collect()

# Unpersist when done
raw_df.unpersist()
```

---

## 2. Data Persistence and Write Modes

### Bronze Layer Write Strategy

**❌ NEVER: Use overwrite mode for bronze layer**
```python
# BAD - Destroys all historical data!
df.write.format("delta").mode("overwrite").save(bronze_path)
```

**✅ DO: Use append mode with deduplication**
```python
# GOOD - Preserves history, implements idempotency
from delta.tables import DeltaTable

if DeltaTable.isDeltaTable(spark, bronze_path):
    delta_table = DeltaTable.forPath(spark, bronze_path)
    
    # Merge with upsert logic
    delta_table.alias("target").merge(
        df.alias("source"),
        "target.transaction_id = source.transaction_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
else:
    # First write - create table
    df.write.format("delta").mode("append").save(bronze_path)
```

**Rationale:** Bronze layer represents immutable raw data. Use append mode to maintain audit trail and enable reprocessing. Implement merge/upsert for idempotency.

### Implement Idempotency

**✅ DO: Deduplicate within batch**
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

# Remove duplicates within the batch
window_spec = Window.partitionBy("transaction_id").orderBy(desc("event_timestamp"))
deduplicated_df = df.withColumn("row_num", row_number().over(window_spec)) \
                    .filter(col("row_num") == 1) \
                    .drop("row_num")
```

**✅ DO: Track processed sources**
```python
# Add batch/run tracking
df.withColumn("batch_id", lit(batch_id)) \
  .withColumn("_ingest_ts", current_timestamp())
```

---

## 3. Data Quality and Validation

### Handle Invalid Records with Quarantine Tables

**❌ AVOID: Silently dropping invalid data**
```python
# BAD - Data loss without traceability
valid_df = df.filter(col("id").isNotNull())
# Invalid records are lost forever
```

**✅ DO: Persist invalid records to quarantine**
```python
from pyspark.sql.functions import lit, current_timestamp

# Separate valid and invalid records
valid_df = df.filter(col("transaction_id").isNotNull())
invalid_df = df.filter(col("transaction_id").isNull())

# Write valid data to bronze
valid_df.write.format("delta").mode("append").save(bronze_path)

# Write invalid data to quarantine with metadata
if invalid_df.count() > 0:
    quarantine_path = f"{bronze_path}_quarantine"
    invalid_df.withColumn("rejection_reason", lit("Missing transaction_id")) \
              .withColumn("quarantine_ts", current_timestamp()) \
              .withColumn("pipeline_run_id", lit(run_id)) \
              .write.format("delta").mode("append").save(quarantine_path)
    
    logger.warning(f"Quarantined {invalid_df.count()} invalid records")
```

### Validate Schema Before Write

```python
def validate_schema(df: DataFrame, expected_columns: set) -> None:
    """Validate DataFrame has required columns"""
    actual_columns = set(df.columns)
    
    missing = expected_columns - actual_columns
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    
    # Optional: Check data types
    expected_schema = {...}  # Define expected types
    for field in df.schema.fields:
        if field.name in expected_schema:
            if field.dataType != expected_schema[field.name]:
                raise TypeError(f"Column {field.name} has wrong type")
```

### Implement Comprehensive Validation

```python
# Define data quality rules
quality_checks = [
    ("transaction_id", col("transaction_id").isNotNull()),
    ("amount", col("amount") > 0),
    ("currency", col("currency").isin(["USD", "EUR", "GBP"])),
    ("event_timestamp", col("event_timestamp").isNotNull())
]

# Add validation flags
for check_name, condition in quality_checks:
    df = df.withColumn(f"_valid_{check_name}", condition)

# Aggregate validation results
df = df.withColumn(
    "_is_valid",
    reduce(lambda a, b: a & b, [col(f"_valid_{c[0]}") for c in quality_checks])
)

# Split valid/invalid
valid_df = df.filter(col("_is_valid"))
invalid_df = df.filter(~col("_is_valid"))
```

---

## 4. Error Handling and Resilience

### Implement Robust Error Handling

**✅ DO: Wrap I/O operations in try-except**
```python
from pathlib import Path
from pyspark.sql.utils import AnalysisException

def extract(self, source_path: str) -> DataFrame:
    """Extract with comprehensive error handling"""
    try:
        # Validate source exists
        path = Path(source_path)
        if not path.exists():
            raise FileNotFoundError(f"Source not found: {source_path}")
        
        # Read with schema validation
        df = self.spark.read \
            .format("json") \
            .schema(self.get_schema()) \
            .load(source_path)
        
        # Validate non-empty
        if df.rdd.isEmpty():
            raise ValueError(f"Source file is empty: {source_path}")
        
        return df
        
    except AnalysisException as e:
        self.logger.error(f"Spark analysis error: {e}")
        raise
    except Exception as e:
        self.logger.error(f"Failed to extract data from {source_path}: {e}")
        raise
```

### Validate Configuration

```python
def __init__(self, spark, config_path: str):
    super().__init__(spark, config_path)
    self._validate_config()

def _validate_config(self):
    """Validate required configuration keys exist"""
    required_keys = [
        ("database", "bronze_path"),
        ("data_sources", "transactions", "path"),
        ("data_sources", "transactions", "format")
    ]
    
    for keys in required_keys:
        config_section = self.config
        for key in keys:
            if key not in config_section:
                raise ValueError(f"Missing config key: {'.'.join(keys)}")
            config_section = config_section[key]
```

---

## 5. Path Handling

### Use Pathlib for Cross-Platform Compatibility

**❌ AVOID: String concatenation**
```python
# BAD - May fail on Windows
bronze_path = self.config['database']['bronze_path'] + "/transactions"
```

**✅ DO: Use pathlib.Path**
```python
from pathlib import Path

# GOOD - OS-agnostic path handling
bronze_path = str(Path(self.config['database']['bronze_path']) / "transactions")
```

---

## 6. Delta Lake Optimizations

### Implement Partitioning Strategy

**✅ DO: Partition by date for time-series data**
```python
from pyspark.sql.functions import current_date

# Add partition column
df = df.withColumn("_ingest_date", current_date())

# Write with partitioning
df.write.format("delta") \
  .mode("append") \
  .partitionBy("_ingest_date") \
  .save(bronze_path)
```

**Rationale:** Partitioning improves query performance by enabling partition pruning. Common partition strategies:
- Bronze layer: `_ingest_date` or `_ingest_year_month`
- Event data: `event_date` or `year/month/day`

### Optimize Tables Post-Write

**✅ DO: Run OPTIMIZE and Z-ORDER**
```python
from delta.tables import DeltaTable

# After write, optimize the table
delta_table = DeltaTable.forPath(spark, bronze_path)

# Compact small files
delta_table.optimize().executeCompaction()

# Z-order by commonly filtered columns
delta_table.optimize().executeZOrderBy("transaction_id", "event_timestamp")

logger.info(f"Optimized table at {bronze_path}")
```

### Implement Vacuum Strategy

```python
# Clean up old files (carefully!)
delta_table.vacuum(retentionHours=168)  # 7 days

# Note: Ensure retention period allows for:
# - Long-running queries
# - Time travel requirements
# - Disaster recovery needs
```

---

## 7. Metadata and Audit Columns

### Add Standard Metadata Columns

**✅ DO: Include ingestion metadata**
```python
from pyspark.sql.functions import current_timestamp, lit, input_file_name

df = df.withColumn("_ingest_ts", current_timestamp()) \
      .withColumn("_pipeline_run_id", lit(run_id)) \
      .withColumn("_source_file", input_file_name()) \
      .withColumn("_ingest_date", current_date())
```

**Standard bronze layer metadata:**
- `_ingest_ts`: Timestamp when record was ingested
- `_ingest_date`: Date for partitioning
- `_pipeline_run_id`: Unique identifier for pipeline execution
- `_source_file`: Origin file path (use cautiously with JSON)
- `_schema_version`: Track schema evolution

---

## 8. Schema Management

### Always Define Explicit Schemas

**❌ AVOID: Schema inference**
```python
# BAD - Schema may drift, poor performance
df = spark.read.format("json").load(path)
```

**✅ DO: Define explicit schemas**
```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

def get_schema(self) -> StructType:
    """Define explicit schema for data governance"""
    return StructType([
        StructField("transaction_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=True),
        StructField("event_timestamp", TimestampType(), nullable=True),
        StructField("amount", DoubleType(), nullable=True),
        StructField("currency", StringType(), nullable=True)
    ])

# Use explicit schema
df = spark.read.format("json").schema(self.get_schema()).load(path)
```

**Rationale:** Explicit schemas provide:
- Better performance (no inference overhead)
- Data governance and validation
- Protection against schema drift
- Clear documentation of expected structure

### Handle Schema Evolution

```python
# Enable schema evolution when needed
df.write.format("delta") \
  .mode("append") \
  .option("mergeSchema", "true") \
  .save(bronze_path)

# Track schema versions
df.withColumn("_schema_version", lit("v1.2.0"))
```

---

## 9. Metrics and Observability

### Collect Comprehensive Metrics

**✅ DO: Track detailed pipeline metrics**
```python
class BronzePipeline:
    def __init__(self, spark, config_path):
        self.metrics = {
            "pipeline_start_ts": None,
            "pipeline_end_ts": None,
            "duration_seconds": 0.0,
            
            # Data metrics
            "rows_read": 0,
            "rows_valid": 0,
            "rows_invalid": 0,
            "rows_duplicate": 0,
            "rows_loaded": 0,
            
            # File metrics
            "source_file_count": 0,
            "source_size_mb": 0.0,
            "output_size_mb": 0.0,
            
            # Quality metrics
            "validation_errors": {},
            "schema_violations": 0,
            
            # Performance metrics
            "read_duration_seconds": 0.0,
            "transform_duration_seconds": 0.0,
            "write_duration_seconds": 0.0,
            "rows_per_second": 0.0
        }
```

### Log Structured Metrics

```python
import json
import time

def run(self) -> dict:
    """Execute pipeline with comprehensive metrics"""
    start_time = time.time()
    self.metrics["pipeline_start_ts"] = time.strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        # Pipeline execution...
        
        # Calculate final metrics
        self.metrics["duration_seconds"] = round(time.time() - start_time, 2)
        self.metrics["rows_per_second"] = round(
            self.metrics["rows_loaded"] / self.metrics["duration_seconds"], 2
        ) if self.metrics["duration_seconds"] > 0 else 0
        
        # Log as structured JSON for parsing by monitoring tools
        self.logger.info(f"Pipeline metrics: {json.dumps(self.metrics)}")
        
        return self.metrics
        
    except Exception as e:
        self.metrics["error"] = str(e)
        self.logger.error(f"Pipeline failed: {json.dumps(self.metrics)}")
        raise
```

---

## 10. Interface Design

### Respect Base Class Contracts

**❌ AVOID: Violating parent class signatures**
```python
class BasePipeline:
    def run(self) -> None:
        pass

class BronzePipeline(BasePipeline):
    def run(self) -> dict:  # BAD - Changes return type
        return self.metrics
```

**✅ DO: Maintain consistent interfaces**
```python
class BronzePipeline(BasePipeline):
    def run(self) -> None:
        """Execute pipeline per base class contract"""
        # Execute pipeline...
        self.logger.info(f"Metrics: {self.metrics}")
    
    def get_metrics(self) -> dict:
        """Separate method for metrics access"""
        return self.metrics
```

**Alternative:** Update base class to support metrics:
```python
class BasePipeline(ABC):
    @abstractmethod
    def run(self) -> Optional[dict]:
        """Execute pipeline, optionally return metrics"""
        pass
```

---

## 11. Configuration Management

### Use Safe Configuration Access

**✅ DO: Use .get() with defaults**
```python
# Safe access with defaults
transactions_config = self.config.get('data_sources', {}).get('transactions', {})
source_path = transactions_config.get('path', 'data/raw_seed/transactions.json')
file_format = transactions_config.get('format', 'json')
```

### Avoid Misleading Configuration Options

**❌ AVOID: Configuration that doesn't deliver promised behavior**
```python
# BAD - schema_enforcement at read time doesn't prevent write-time schema enforcement
schema_enforcement = config.get('schema_enforcement', True)
if schema_enforcement:
    df = spark.read.schema(schema).load(path)
else:
    df = spark.read.load(path)  # Delta write will still enforce schema!
```

**✅ DO: Configuration that clearly states behavior**
```python
# GOOD - Clear about what schema validation means
use_explicit_schema = config.get('use_explicit_schema', True)
allow_schema_evolution = config.get('allow_schema_evolution', False)

if use_explicit_schema:
    df = spark.read.schema(schema).load(path)
else:
    df = spark.read.load(path)

# Schema evolution handled at write time
write_options = {"mergeSchema": "true"} if allow_schema_evolution else {}
df.write.format("delta").options(**write_options).save(path)
```

---

## 12. Testing Considerations

### Design for Testability

```python
class BronzePipeline:
    def __init__(self, spark, config_path: str = None, config: dict = None):
        """Accept either config path or config dict for testing"""
        self.spark = spark
        
        if config:
            self.config = config
        elif config_path:
            self.config = self.load_config(config_path)
        else:
            raise ValueError("Must provide config_path or config")
```

### Write Testable Methods

```python
# Small, focused methods are easier to test
def validate_required_fields(self, df: DataFrame) -> DataFrame:
    """Validate required fields - easily testable"""
    return df.filter(col("transaction_id").isNotNull())

def add_metadata_columns(self, df: DataFrame, run_id: str) -> DataFrame:
    """Add metadata - easily testable in isolation"""
    return df.withColumn("_ingest_ts", current_timestamp()) \
             .withColumn("_run_id", lit(run_id))
```

---

## Summary Checklist

Before deploying a bronze layer pipeline, ensure:

**Performance:**
- [ ] Minimized DataFrame materializations (count, collect, etc.)
- [ ] Strategic use of caching for multi-action DataFrames
- [ ] Metrics collected efficiently in single pass when possible

**Data Persistence:**
- [ ] Using append/merge mode (NOT overwrite) for bronze layer
- [ ] Implemented idempotency (deduplication logic)
- [ ] Partitioning strategy defined and implemented

**Data Quality:**
- [ ] Invalid records written to quarantine table
- [ ] Comprehensive validation rules defined
- [ ] Explicit schema defined and enforced

**Error Handling:**
- [ ] Try-except blocks around I/O operations
- [ ] Configuration validation at startup
- [ ] Informative error messages and logging

**Optimizations:**
- [ ] Post-write OPTIMIZE and Z-ORDER configured
- [ ] VACUUM strategy defined
- [ ] Appropriate retention periods set

**Observability:**
- [ ] Comprehensive metrics collection
- [ ] Structured logging (JSON format)
- [ ] Performance metrics (rows/sec, duration)

**Code Quality:**
- [ ] Path handling uses pathlib.Path
- [ ] Base class contracts respected
- [ ] Clear, testable method signatures
- [ ] Configuration clearly documents behavior

---

## Additional Resources

- [Delta Lake Best Practices](https://docs.delta.io/latest/best-practices.html)
- [PySpark Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- [Medallion Architecture Pattern](https://www.databricks.com/glossary/medallion-architecture)
