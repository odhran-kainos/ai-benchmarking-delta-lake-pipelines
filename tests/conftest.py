"""
Pytest configuration and shared fixtures for Delta Lake pipeline tests.
"""
import os
import shutil
from pathlib import Path
import pytest
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


@pytest.fixture(scope="session")
def spark_session():
    """
    Create a shared Spark session for all tests with Delta Lake support.
    """
    builder = (
        SparkSession.builder
        .appName("DeltaLakePipelineTests")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", "spark-warehouse-test")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    )
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    yield spark
    
    spark.stop()
    
    # Cleanup test warehouse
    if os.path.exists("spark-warehouse-test"):
        shutil.rmtree("spark-warehouse-test")


@pytest.fixture
def test_data_dir():
    """
    Path to the test data directory.
    """
    return Path(__file__).parent.parent / "data" / "raw_seed"


@pytest.fixture
def bronze_output_dir(tmp_path):
    """
    Temporary directory for bronze layer outputs during testing.
    """
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir(exist_ok=True)
    return bronze_dir


@pytest.fixture
def sample_transactions_path(test_data_dir):
    """
    Path to sample transaction JSON file.
    """
    transactions_file = test_data_dir / "transactions.json"
    if not transactions_file.exists():
        pytest.skip(f"Sample data not found at {transactions_file}")
    return str(transactions_file)


@pytest.fixture
def cleanup_delta_tables():
    """
    Cleanup Delta tables after tests.
    """
    tables_to_clean = []
    
    def register_table(table_path):
        tables_to_clean.append(table_path)
    
    yield register_table
    
    # Cleanup after test
    for table_path in tables_to_clean:
        if os.path.exists(table_path):
            shutil.rmtree(table_path)


@pytest.fixture
def expected_transaction_schema():
    """
    Expected schema for bronze transactions table.
    """
    return {
        "transaction_id": "string",
        "customer_id": "string", 
        "event_timestamp": "string",  # or timestamp depending on implementation
        "amount": "double",
        "currency": "string",
        "_ingest_ts": "timestamp",
        "_file_name": "string"
    }
