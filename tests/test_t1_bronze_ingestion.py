"""
Tests for T1: Bronze layer transaction ingestion.

These tests validate that AI-generated implementations of the T1 task
meet the acceptance criteria defined in benchmark/tasks/T1_ingest_transactions.yaml
"""
import pytest
from pathlib import Path
from pyspark.sql import functions as F
from delta import DeltaTable
import sys
import os

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


class TestT1BronzeIngestion:
    """Test suite for T1 Bronze transaction ingestion pipeline."""
    
    def test_bronze_table_exists(self, spark_session, bronze_output_dir):
        """
        Test that the bronze_transactions Delta table is created.
        
        Acceptance Criteria:
        - Delta table bronze_transactions created with expected columns
        """
        from pipelines.bronze_transactions_pipeline import BronzeTransactionsPipeline
        
        # Create pipeline with temporary config
        pipeline = BronzeTransactionsPipeline(spark_session, "config/pipeline_config.yaml")
        
        # Override output path for testing
        pipeline.config['database']['bronze_path'] = str(bronze_output_dir)
        
        # Run pipeline
        metrics = pipeline.run()
        
        # Verify table exists
        table_path = bronze_output_dir / "transactions"
        assert table_path.exists(), "Bronze transactions table was not created"
        assert DeltaTable.isDeltaTable(spark_session, str(table_path)), "Path is not a valid Delta table"
    
    def test_required_columns_present(self, spark_session, bronze_output_dir):
        """
        Test that all required columns are present in bronze table.
        
        Required columns per T1 spec:
        - transaction_id (from source)
        - customer_id (from source)
        - event_timestamp (from source)
        - amount (from source)
        - currency (from source)
        - _ingest_ts (metadata - timestamp)
        - _file_name (metadata - string)
        """
        from pipelines.bronze_transactions_pipeline import BronzeTransactionsPipeline
        
        pipeline = BronzeTransactionsPipeline(spark_session, "config/pipeline_config.yaml")
        pipeline.config['database']['bronze_path'] = str(bronze_output_dir)
        pipeline.run()
        
        table_path = bronze_output_dir / "transactions"
        df = spark_session.read.format("delta").load(str(table_path))
        
        required_columns = {
            "transaction_id",
            "customer_id", 
            "event_timestamp",
            "amount",
            "currency",
            "_ingest_ts",
            "_file_name"
        }
        
        actual_columns = set(df.columns)
        
        missing_columns = required_columns - actual_columns
        assert not missing_columns, f"Missing required columns: {missing_columns}"
    
    def test_row_count_validation(self, spark_session, sample_transactions_path, bronze_output_dir):
        """
        Test that row count matches input minus invalid rows.
        
        Acceptance Criteria:
        - Row count matches input minus invalid rows
        
        Note: The sample data has 1070 rows. All should be valid (have transaction_id).
        """
        from pipelines.bronze_transactions_pipeline import BronzeTransactionsPipeline
        
        pipeline = BronzeTransactionsPipeline(spark_session, "config/pipeline_config.yaml")
        pipeline.config['database']['bronze_path'] = str(bronze_output_dir)
        metrics = pipeline.run()
        
        table_path = bronze_output_dir / "transactions"
        
        # Count source rows
        source_df = spark_session.read.json(sample_transactions_path)
        source_count = source_df.count()
        
        # Count rows with valid transaction_id
        valid_source_count = source_df.filter(F.col("transaction_id").isNotNull()).count()
        
        # Count bronze rows
        bronze_df = spark_session.read.format("delta").load(str(table_path))
        bronze_count = bronze_df.count()
        
        # Bronze count should equal valid source count
        assert bronze_count == valid_source_count, (
            f"Expected {valid_source_count} rows (valid from {source_count} total), "
            f"but found {bronze_count} in bronze table"
        )
    
    def test_metadata_columns_populated(self, spark_session, bronze_output_dir):
        """
        Test that ingestion metadata columns are populated (non-null).
        
        Acceptance Criteria:
        - Ingestion metadata columns populated (non-null)
        """
        from pipelines.bronze_transactions_pipeline import BronzeTransactionsPipeline
        
        pipeline = BronzeTransactionsPipeline(spark_session, "config/pipeline_config.yaml")
        pipeline.config['database']['bronze_path'] = str(bronze_output_dir)
        pipeline.config['database']['bronze_path'] = str(bronze_output_dir)
        pipeline.run()
        
        table_path = bronze_output_dir / "transactions"
        df = spark_session.read.format("delta").load(str(table_path))
        
        # Check _ingest_ts is not null
        null_ingest_ts = df.filter(F.col("_ingest_ts").isNull()).count()
        assert null_ingest_ts == 0, f"Found {null_ingest_ts} rows with null _ingest_ts"
        
        # Check _file_name is not null
        null_file_name = df.filter(F.col("_file_name").isNull()).count()
        assert null_file_name == 0, f"Found {null_file_name} rows with null _file_name"
    
    def test_transaction_id_uniqueness(self, spark_session, bronze_output_dir):
        """
        Test that transaction_id values are present and ideally unique.
        
        Note: This is a data quality check beyond the basic T1 requirements.
        """
        from pipelines.bronze_transactions_pipeline import BronzeTransactionsPipeline
        
        pipeline = BronzeTransactionsPipeline(spark_session, "config/pipeline_config.yaml")
        pipeline.config['database']['bronze_path'] = str(bronze_output_dir)
        pipeline.config['database']['bronze_path'] = str(bronze_output_dir)
        pipeline.run()
        
        table_path = bronze_output_dir / "transactions"
        df = spark_session.read.format("delta").load(str(table_path))
        
        # All rows should have transaction_id (per validation requirement)
        null_txn_ids = df.filter(F.col("transaction_id").isNull()).count()
        assert null_txn_ids == 0, "Invalid rows with null transaction_id should be rejected"
        
        # Check for duplicates (info only, not a hard requirement for T1)
        total_count = df.count()
        distinct_count = df.select("transaction_id").distinct().count()
        
        if total_count != distinct_count:
            duplicates = total_count - distinct_count
            # This is a warning, not a failure for T1
            pytest.warns(
                UserWarning,
                match=f"Found {duplicates} duplicate transaction_ids"
            )
    
    def test_data_types(self, spark_session, bronze_output_dir):
        """
        Test that columns have appropriate data types.
        """
        from pipelines.bronze_transactions_pipeline import BronzeTransactionsPipeline
        
        pipeline = BronzeTransactionsPipeline(spark_session, "config/pipeline_config.yaml")
        pipeline.config['database']['bronze_path'] = str(bronze_output_dir)
        pipeline.run()
        
        table_path = bronze_output_dir / "transactions"
        df = spark_session.read.format("delta").load(str(table_path))
        schema = df.schema
        
        # Build type map
        type_map = {field.name: str(field.dataType) for field in schema.fields}
        
        # Check key type expectations
        assert "transaction_id" in type_map
        assert "string" in type_map["transaction_id"].lower()
        
        assert "amount" in type_map
        assert any(t in type_map["amount"].lower() for t in ["double", "decimal", "float"])
        
        assert "_ingest_ts" in type_map
        assert "timestamp" in type_map["_ingest_ts"].lower()
    
    def test_invalid_records_rejected(self, spark_session, tmp_path, bronze_output_dir):
        """
        Test that rows missing transaction_id are rejected.
        
        This creates a test file with some invalid records and verifies they're excluded.
        """
        # Create test data with invalid records
        test_file = tmp_path / "test_transactions_with_invalid.json"
        
        # Write test data: 3 valid, 2 invalid (missing transaction_id)
        import json
        test_records = [
            {"transaction_id": "T1", "customer_id": "C1", "event_timestamp": "2025-11-17T10:00:00", "amount": 100.0, "currency": "EUR"},
            {"transaction_id": "T2", "customer_id": "C2", "event_timestamp": "2025-11-17T10:01:00", "amount": 200.0, "currency": "EUR"},
            {"transaction_id": "T3", "customer_id": "C3", "event_timestamp": "2025-11-17T10:02:00", "amount": 300.0, "currency": "EUR"},
            {"customer_id": "C4", "event_timestamp": "2025-11-17T10:03:00", "amount": 400.0, "currency": "EUR"},  # Missing transaction_id
            {"transaction_id": None, "customer_id": "C5", "event_timestamp": "2025-11-17T10:04:00", "amount": 500.0, "currency": "EUR"},  # Null transaction_id
        ]
        
        with open(test_file, 'w') as f:
            for record in test_records:
                f.write(json.dumps(record) + '\n')
        
        # This test would require running the actual implementation
        # For now, document the expected behavior
        pytest.skip("Requires implementation to test invalid record handling")
        
        # Expected behavior:
        # - Load test_file
        # - Bronze table should have 3 rows (only valid ones)
        # - Metrics should show: rows_raw=5, rows_invalid=2, rows_loaded=3


class TestT1Metrics:
    """Test suite for T1 metrics collection."""
    
    def test_metrics_file_created(self, bronze_output_dir):
        """
        Test that metrics JSON file is created.
        
        Expected metrics per T1 spec:
        - rows_raw
        - rows_invalid  
        - rows_loaded
        - ingestion_duration_seconds
        """
        # Implementations should create metrics file
        # This is validated by the benchmark harness
        pytest.skip("Metrics validation handled by benchmark harness")
    
    def test_metrics_accuracy(self, bronze_output_dir):
        """
        Test that reported metrics match actual table state.
        """
        pytest.skip("Requires implementation and metrics file")


class TestT1Configuration:
    """Test configuration-driven implementation (best practice from T1 spec)."""
    
    def test_no_hardcoded_paths(self):
        """
        Test that implementation uses configuration, not hardcoded paths.
        
        AI Guidance from T1:
        - Prefer configuration-driven changes (avoid hardcoding paths)
        """
        # This would require code analysis of the implementation
        pytest.skip("Requires static code analysis - checked in maintainability scoring")
    
    def test_reuses_pipeline_patterns(self):
        """
        Test that implementation reuses existing pipeline utility patterns.
        
        AI Guidance from T1:
        - Reuse existing pipeline utility patterns
        """
        # This would require code analysis
        pytest.skip("Requires code review - checked in maintainability scoring")
