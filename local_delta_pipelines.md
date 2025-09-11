

## Prerequisites Setup

First, let's ensure you have the necessary tools installed:

```bash
# Install pyenv if you haven't already
brew install pyenv

# Install Java (required for Spark)
brew install openjdk@11

# Add Java to your path
echo 'export PATH="/opt/homebrew/opt/openjdk@11/bin:$PATH"' >> ~/.zshrc
echo 'export JAVA_HOME="/opt/homebrew/opt/openjdk@11"' >> ~/.zshrc
source ~/.zshrc
```

## Python Environment Setup

Create and configure a dedicated Python environment:

```bash
# Install Python 3.10 (good compatibility with Spark/Delta)
pyenv install 3.10.12
pyenv virtualenv 3.10.12 delta-lakehouse
pyenv activate delta-lakehouse

# Verify Java is accessible
java -version
echo $JAVA_HOME
```

## Core Dependencies Installation

Install the essential packages for your delta lakehouse:

```bash
# Core Spark and Delta Lake
pip install pyspark==3.5.0
pip install delta-spark==3.0.0

# Data processing and utilities
pip install pandas numpy pyarrow
pip install sqlalchemy psycopg2-binary

# Workflow orchestration (choose one or both)
pip install prefect  # Modern Python-first orchestrator
pip install apache-airflow==2.7.1  # Traditional option

# Data validation and quality
pip install great-expectations
pip install pydantic

# Configuration management
pip install python-decouple pyyaml

# Development tools
pip install jupyter notebook pytest
```

## Directory Structure

Create a project structure for your lakehouse:

```bash
mkdir delta-lakehouse-project
cd delta-lakehouse-project

# Create directory structure
mkdir -p {data/{raw,bronze,silver,gold},pipelines,config,notebooks,tests,logs}

# Project structure:
# ├── data/
# │   ├── raw/      # Landing zone for raw data
# │   ├── bronze/   # Raw data in Delta format
# │   ├── silver/   # Cleaned, validated data
# │   └── gold/     # Business-ready aggregated data
# ├── pipelines/    # Pipeline definitions
# ├── config/       # Configuration files
# ├── notebooks/    # Jupyter notebooks for exploration
# ├── tests/        # Unit tests
# └── logs/         # Application logs
```

## Configuration Setup

Create configuration files:

**config/spark_config.yaml**:

```yaml
spark:
  app_name: "Delta Lakehouse Local"
  master: "local[*]"
  config:
    spark.sql.extensions: "io.delta.sql.DeltaSparkSessionExtension"
    spark.sql.catalog.spark_catalog: "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    spark.sql.adaptive.enabled: "true"
    spark.sql.adaptive.coalescePartitions.enabled: "true"
    spark.serializer: "org.apache.spark.serializer.KryoSerializer"

delta:
  data_path: "./data"
  checkpoint_location: "./data/checkpoints"
```

**config/pipeline_config.yaml**:

```yaml
database:
  bronze_path: "data/bronze"
  silver_path: "data/silver"
  gold_path: "data/gold"

data_quality:
  enable_validation: true
  fail_on_error: false
  
logging:
  level: "INFO"
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
```

## Core Utilities

Create utility modules:

**pipelines/utils/spark_session.py**:

```python
from pyspark.sql import SparkSession
import yaml
from pathlib import Path

def create_spark_session():
    """Create Spark session with Delta Lake configuration"""
    
    # Load configuration
    config_path = Path("config/spark_config.yaml")
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    
    builder = SparkSession.builder \
        .appName(config['spark']['app_name']) \
        .master(config['spark']['master'])
    
    # Add Spark configurations
    for key, value in config['spark']['config'].items():
        builder = builder.config(key, value)
    
    # Add Delta Lake packages
    builder = builder.config(
        "spark.jars.packages", 
        "io.delta:delta-core_2.12:3.0.0,io.delta:delta-storage:3.0.0"
    )
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    return spark
```

**pipelines/utils/delta_operations.py**:

```python
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
import logging

logger = logging.getLogger(__name__)

class DeltaOperations:
    def __init__(self, spark):
        self.spark = spark
    
    def write_delta_table(self, df: DataFrame, path: str, mode: str = "append", 
                         partition_by: list = None, merge_key: str = None):
        """Write DataFrame to Delta table with various options"""
        
        writer = df.write.format("delta").mode(mode)
        
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        
        writer.save(path)
        logger.info(f"Successfully wrote data to {path}")
        
        return path
    
    def merge_delta_table(self, source_df: DataFrame, target_path: str, 
                         merge_condition: str, update_set: dict = None, 
                         insert_values: dict = None):
        """Perform merge (upsert) operation on Delta table"""
        
        if not DeltaTable.isDeltaTable(self.spark, target_path):
            # If table doesn't exist, create it
            source_df.write.format("delta").save(target_path)
            return
        
        delta_table = DeltaTable.forPath(self.spark, target_path)
        
        merge_builder = delta_table.alias("target").merge(
            source_df.alias("source"), 
            merge_condition
        )
        
        if update_set:
            merge_builder = merge_builder.whenMatchedUpdate(set=update_set)
        else:
            merge_builder = merge_builder.whenMatchedUpdateAll()
        
        if insert_values:
            merge_builder = merge_builder.whenNotMatchedInsert(values=insert_values)
        else:
            merge_builder = merge_builder.whenNotMatchedInsertAll()
        
        merge_builder.execute()
        logger.info(f"Successfully merged data into {target_path}")
```

## Declarative Pipeline Framework

**pipelines/base_pipeline.py**:

```python
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from typing import Dict, Any
import yaml
import logging
from pathlib import Path

class BasePipeline(ABC):
    """Abstract base class for declarative pipelines"""
    
    def __init__(self, spark, config_path: str = None):
        self.spark = spark
        self.config = self.load_config(config_path) if config_path else {}
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def load_config(self, config_path: str) -> Dict[Any, Any]:
        """Load pipeline configuration from YAML file"""
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    
    @abstractmethod
    def extract(self) -> DataFrame:
        """Extract data from source"""
        pass
    
    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """Transform the data"""
        pass
    
    @abstractmethod
    def load(self, df: DataFrame) -> None:
        """Load data to destination"""
        pass
    
    def run(self) -> None:
        """Execute the complete pipeline"""
        self.logger.info(f"Starting pipeline: {self.__class__.__name__}")
        
        # Extract
        raw_df = self.extract()
        self.logger.info("Data extraction completed")
        
        # Transform
        transformed_df = self.transform(raw_df)
        self.logger.info("Data transformation completed")
        
        # Load
        self.load(transformed_df)
        self.logger.info("Data loading completed")
        
        self.logger.info(f"Pipeline {self.__class__.__name__} completed successfully")
```

## Sample Declarative Pipeline

**pipelines/sample_etl_pipeline.py**:

```python
from pipelines.base_pipeline import BasePipeline
from pipelines.utils.delta_operations import DeltaOperations
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

class SampleETLPipeline(BasePipeline):
    """Sample pipeline demonstrating bronze -> silver -> gold pattern"""
    
    def __init__(self, spark, config_path: str = "config/pipeline_config.yaml"):
        super().__init__(spark, config_path)
        self.delta_ops = DeltaOperations(spark)
    
    def extract(self) -> DataFrame:
        """Extract sample data (replace with your data source)"""
        # Sample data - replace with your actual data source
        sample_data = [
            (1, "Alice", "Engineering", 75000, "2023-01-15"),
            (2, "Bob", "Marketing", 65000, "2023-02-20"),
            (3, "Charlie", "Engineering", 80000, "2023-01-10"),
            (4, "Diana", "Sales", 70000, "2023-03-05")
        ]
        
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("department", StringType(), True),
            StructField("salary", IntegerType(), True),
            StructField("hire_date", StringType(), True)
        ])
        
        df = self.spark.createDataFrame(sample_data, schema)
        
        # Write to bronze layer (raw data)
        bronze_path = self.config['database']['bronze_path'] + "/employees"
        self.delta_ops.write_delta_table(df, bronze_path, mode="overwrite")
        
        return df
    
    def transform(self, df: DataFrame) -> DataFrame:
        """Transform data for silver layer"""
        # Data cleaning and transformation
        silver_df = df.select(
            col("id"),
            upper(col("name")).alias("name"),
            col("department"),
            col("salary"),
            to_date(col("hire_date"), "yyyy-MM-dd").alias("hire_date"),
            current_timestamp().alias("processed_at")
        ).filter(col("salary") > 0)
        
        # Write to silver layer
        silver_path = self.config['database']['silver_path'] + "/employees"
        self.delta_ops.write_delta_table(silver_df, silver_path, mode="overwrite")
        
        return silver_df
    
    def load(self, df: DataFrame) -> None:
        """Create gold layer aggregations"""
        # Department salary statistics
        dept_stats = df.groupBy("department").agg(
            count("*").alias("employee_count"),
            avg("salary").alias("avg_salary"),
            max("salary").alias("max_salary"),
            min("salary").alias("min_salary")
        ).withColumn("analysis_date", current_date())
        
        # Write to gold layer
        gold_path = self.config['database']['gold_path'] + "/department_stats"
        self.delta_ops.write_delta_table(dept_stats, gold_path, mode="overwrite")
        
        self.logger.info("Gold layer analytics tables created")
```

## Workflow Orchestration with Prefect

**pipelines/orchestration/prefect_flows.py**:

```python
from prefect import flow, task
from pipelines.utils.spark_session import create_spark_session
from pipelines.sample_etl_pipeline import SampleETLPipeline
import logging

@task
def run_etl_pipeline():
    """Task to run the ETL pipeline"""
    spark = create_spark_session()
    
    try:
        pipeline = SampleETLPipeline(spark)
        pipeline.run()
        return "Pipeline completed successfully"
    finally:
        spark.stop()

@flow(name="daily-lakehouse-pipeline")
def daily_lakehouse_flow():
    """Daily lakehouse processing flow"""
    logging.info("Starting daily lakehouse processing")
    
    result = run_etl_pipeline()
    
    logging.info("Daily lakehouse processing completed")
    return result
```

## Testing Your Setup

Create a test script to verify everything works:

**test_setup.py**:

```python
from pipelines.utils.spark_session import create_spark_session
from pipelines.sample_etl_pipeline import SampleETLPipeline
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

def test_lakehouse_setup():
    """Test the complete lakehouse setup"""
    
    print("Testing Delta Lakehouse setup...")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Run sample pipeline
        pipeline = SampleETLPipeline(spark)
        pipeline.run()
        
        # Verify data in each layer
        print("\n=== Bronze Layer ===")
        bronze_df = spark.read.format("delta").load("data/bronze/employees")
        bronze_df.show()
        
        print("\n=== Silver Layer ===")
        silver_df = spark.read.format("delta").load("data/silver/employees")
        silver_df.show()
        
        print("\n=== Gold Layer ===")
        gold_df = spark.read.format("delta").load("data/gold/department_stats")
        gold_df.show()
        
        print("\n✅ Lakehouse setup test completed successfully!")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    test_lakehouse_setup()
```

## Running Your Setup

1. **Activate your environment**:

```bash
pyenv activate delta-lakehouse
cd delta-lakehouse-project
```

2. **Test the setup**:

```bash
python test_setup.py
```

3. **Run with Prefect** (optional):

```bash
# Start Prefect server
prefect server start

# In another terminal, run the flow
python -c "from pipelines.orchestration.prefect_flows import daily_lakehouse_flow; daily_lakehouse_flow()"
```

4. **Explore with Jupyter**:

```bash
jupyter notebook
```

## Key Features You Now Have

- **Delta Lake**: ACID transactions, time travel, schema evolution
- **Medallion Architecture**: Bronze (raw) → Silver (cleaned) → Gold (aggregated)
- **Declarative Pipelines**: Reusable, configurable pipeline framework
- **Workflow Orchestration**: Prefect integration for scheduling and monitoring
- **Local Development**: No Docker dependencies, pure Python environment

This setup gives you a production-ready foundation that can scale from local development to cloud deployment while maintaining the same codebase and patterns.