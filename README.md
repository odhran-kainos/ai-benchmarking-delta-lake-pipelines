# Delta Lake Pipelines - Local Lakehouse

A Delta Lake pipeline framework for building declarative data pipelines with Spark, featuring the medallion architecture (Bronze → Silver → Gold) and local development capabilities.

## Features

- **Delta Lake Integration**: ACID transactions, time travel, schema evolution
- **Medallion Architecture**: Bronze (raw) → Silver (cleaned) → Gold (aggregated) data layers
- **Declarative Pipelines**: Reusable, configurable pipeline framework
- **Workflow Orchestration**: Prefect integration for scheduling and monitoring
- **Local Development**: No Docker dependencies, pure Python environment
- **Production Ready**: Scalable from local development to cloud deployment

## Prerequisites

### System Requirements

- **Java 11 or higher**: Required for Spark
- **Python 3.10.x**: Recommended for compatibility with Spark/Delta
- **pyenv**: For Python version management

### macOS Setup

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

### Linux/Ubuntu Setup

```bash
# Install Java
sudo apt update
sudo apt install openjdk-11-jdk

# Install pyenv
curl https://pyenv.run | bash

# Add to your shell profile
echo 'export PATH="$HOME/.pyenv/bin:$PATH"' >> ~/.bashrc
echo 'eval "$(pyenv init --path)"' >> ~/.bashrc
echo 'eval "$(pyenv virtualenv-init -)"' >> ~/.bashrc
source ~/.bashrc
```

## Quick Start

### 1. Python Environment Setup

```bash
# Install Python 3.10 (good compatibility with Spark/Delta)
pyenv install 3.10.12
pyenv virtualenv 3.10.12 delta-lakehouse
pyenv activate delta-lakehouse

# Verify Java is accessible
java -version
echo $JAVA_HOME
```

### 2. Install Dependencies

```bash
# Clone/navigate to project directory
cd delta-lake-pipelines

# Install all dependencies
pip install -r requirements.txt
```

### 3. Verify Installation

```bash
# Test the complete setup
python test_setup.py
```

You should see output showing data processed through Bronze → Silver → Gold layers.

## Project Structure

```
delta-lake-pipelines/
├── data/
│   ├── raw/          # Landing zone for raw data
│   ├── bronze/       # Raw data in Delta format
│   ├── silver/       # Cleaned, validated data
│   └── gold/         # Business-ready aggregated data
├── pipelines/
│   ├── utils/        # Utility modules
│   ├── orchestration/ # Workflow orchestration
│   ├── base_pipeline.py # Abstract pipeline framework
│   └── sample_etl_pipeline.py # Example pipeline
├── config/           # Configuration files
├── notebooks/        # Jupyter notebooks for exploration
├── tests/           # Unit tests
├── logs/            # Application logs
└── requirements.txt  # Python dependencies
```

## Core Components

### Spark Session with Delta Lake

The framework automatically configures Spark with Delta Lake extensions:

- **Delta Core**: 3.0.0 (compatible with Spark 3.5.0)
- **Delta Storage**: 3.0.0
- **Adaptive Query Execution**: Enabled for performance
- **Kryo Serializer**: For better performance

### Medallion Architecture

- **Bronze Layer**: Raw data ingestion with minimal processing
- **Silver Layer**: Cleaned, validated, and transformed data
- **Gold Layer**: Business-ready aggregated data and analytics

### Declarative Pipeline Framework

All pipelines extend the `BasePipeline` class providing:

- **Extract**: Data ingestion from various sources
- **Transform**: Data cleaning and business logic
- **Load**: Writing to Delta Lake tables
- **Configuration**: YAML-based pipeline configuration

## Usage Examples

### Running the Sample Pipeline

```bash
# Activate environment
pyenv activate delta-lakehouse

# Run the sample ETL pipeline
python -c "
from pipelines.utils.spark_session import create_spark_session
from pipelines.sample_etl_pipeline import SampleETLPipeline

spark = create_spark_session()
try:
    pipeline = SampleETLPipeline(spark)
    pipeline.run()
finally:
    spark.stop()
"
```

### Using Prefect Orchestration

```bash
# Start Prefect server (optional)
prefect server start

# Run orchestrated pipeline
python -c "from pipelines.orchestration.prefect_flows import daily_lakehouse_flow; daily_lakehouse_flow()"
```

### Exploring Data with Jupyter

```bash
jupyter notebook notebooks/
```

## Configuration

### Spark Configuration (`config/spark_config.yaml`)

```yaml
spark:
  app_name: "Delta Lakehouse Local"
  master: "local[*]"
  config:
    spark.sql.extensions: "io.delta.sql.DeltaSparkSessionExtension"
    spark.sql.catalog.spark_catalog: "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    spark.sql.adaptive.enabled: "true"
```

### Pipeline Configuration (`config/pipeline_config.yaml`)

```yaml
database:
  bronze_path: "data/bronze"
  silver_path: "data/silver" 
  gold_path: "data/gold"

data_quality:
  enable_validation: true
  fail_on_error: false
```

## Creating Custom Pipelines

1. **Extend BasePipeline**:

```python
from pipelines.base_pipeline import BasePipeline

class MyCustomPipeline(BasePipeline):
    def extract(self):
        # Your data extraction logic
        pass
    
    def transform(self, df):
        # Your transformation logic
        pass
    
    def load(self, df):
        # Your loading logic
        pass
```

2. **Configure in YAML**:

```yaml
# config/my_pipeline_config.yaml
source:
  type: "parquet"
  path: "/path/to/source"

target:
  bronze_path: "data/bronze/my_table"
  silver_path: "data/silver/my_table"
  gold_path: "data/gold/my_aggregations"
```

## Delta Lake Features

### Time Travel Queries

```python
# Query historical versions
df = spark.read.format("delta").option("versionAsOf", 0).load("data/silver/employees")

# Query data as of timestamp
df = spark.read.format("delta").option("timestampAsOf", "2023-01-01").load("data/silver/employees")
```

### Schema Evolution

```python
# Delta automatically handles schema changes
df_with_new_column = df.withColumn("new_field", lit("default_value"))
df_with_new_column.write.format("delta").mode("append").save("data/silver/employees")
```

### ACID Transactions

```python
# Merge (upsert) operations
from pipelines.utils.delta_operations import DeltaOperations

delta_ops = DeltaOperations(spark)
delta_ops.merge_delta_table(
    source_df=new_data,
    target_path="data/silver/employees", 
    merge_condition="target.id = source.id"
)
```

## Monitoring and Logging

- **Application Logs**: Stored in `logs/` directory
- **Spark UI**: Available at `http://localhost:4040` during execution
- **Prefect UI**: Available at `http://localhost:4200` when server is running

## Troubleshooting

### Common Issues

1. **Java not found**: Ensure JAVA_HOME is set correctly
2. **Permission errors**: Check write permissions on data directories
3. **Memory errors**: Adjust Spark memory settings in config
4. **Package conflicts**: Use fresh virtual environment

### Memory Configuration

For larger datasets, adjust Spark memory settings:

```yaml
spark:
  config:
    spark.driver.memory: "4g"
    spark.executor.memory: "4g"
    spark.sql.execution.arrow.maxRecordsPerBatch: "10000"
```

## Version Compatibility

- **Spark**: 3.5.0
- **Delta Lake**: 3.0.0 
- **Python**: 3.10.x
- **Java**: 11 or higher
- **Scala**: 2.12 (included with Spark)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## License

MIT License - see LICENSE file for details.