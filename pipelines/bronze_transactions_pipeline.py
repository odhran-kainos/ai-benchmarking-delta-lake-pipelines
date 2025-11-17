from pipelines.base_pipeline import BasePipeline
from pipelines.utils.delta_operations import DeltaOperations
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, input_file_name, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import time


class BronzeTransactionsPipeline(BasePipeline):
    """Bronze layer ingestion pipeline for transactions data"""
    
    def __init__(self, spark, config_path: str = "config/pipeline_config.yaml"):
        super().__init__(spark, config_path)
        self.delta_ops = DeltaOperations(spark)
        self.metrics = {
            "rows_raw": 0,
            "rows_invalid": 0,
            "rows_loaded": 0,
            "ingestion_duration_seconds": 0.0
        }
    
    def get_explicit_schema(self) -> StructType:
        """Define explicit schema for transactions data"""
        return StructType([
            StructField("transaction_id", StringType(), False),
            StructField("customer_id", StringType(), True),
            StructField("event_timestamp", TimestampType(), True),
            StructField("amount", DoubleType(), True),
            StructField("currency", StringType(), True)
        ])
    
    def extract(self) -> DataFrame:
        """Extract transaction data from JSON source"""
        start_time = time.time()
        
        # Get configuration
        transactions_config = self.config.get('data_sources', {}).get('transactions', {})
        source_path = transactions_config.get('path', 'data/raw_seed/transactions.json')
        file_format = transactions_config.get('format', 'json')
        schema_enforcement = transactions_config.get('schema_enforcement', True)
        
        self.logger.info(f"Reading transactions from {source_path}")
        
        # Read data with explicit schema if enforcement is enabled
        if schema_enforcement:
            schema = self.get_explicit_schema()
            raw_df = self.spark.read.format(file_format).schema(schema).load(source_path)
        else:
            raw_df = self.spark.read.format(file_format).load(source_path)
        
        # Track raw row count
        self.metrics["rows_raw"] = raw_df.count()
        self.logger.info(f"Read {self.metrics['rows_raw']} raw transactions")
        
        return raw_df
    
    def transform(self, df: DataFrame) -> DataFrame:
        """Transform data with schema normalization and metadata"""
        # Add ingestion metadata columns
        transformed_df = df.withColumn("_ingest_ts", current_timestamp()) \
                          .withColumn("_file_name", input_file_name())
        
        # Identify invalid rows (missing transaction_id)
        valid_df = transformed_df.filter(col("transaction_id").isNotNull())
        
        # Calculate invalid row count
        self.metrics["rows_invalid"] = self.metrics["rows_raw"] - valid_df.count()
        
        if self.metrics["rows_invalid"] > 0:
            self.logger.warning(f"Rejected {self.metrics['rows_invalid']} rows with missing transaction_id")
        
        return valid_df
    
    def load(self, df: DataFrame) -> None:
        """Load data to bronze_transactions Delta table"""
        bronze_path = self.config['database']['bronze_path'] + "/transactions"
        
        # Write to Delta table
        self.delta_ops.write_delta_table(df, bronze_path, mode="overwrite")
        
        # Track loaded row count
        self.metrics["rows_loaded"] = df.count()
        self.logger.info(f"Loaded {self.metrics['rows_loaded']} transactions to {bronze_path}")
    
    def run(self) -> dict:
        """Execute the complete pipeline and return metrics"""
        start_time = time.time()
        
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
        
        # Calculate duration
        self.metrics["ingestion_duration_seconds"] = round(time.time() - start_time, 2)
        
        self.logger.info(f"Pipeline {self.__class__.__name__} completed successfully")
        self.logger.info(f"Metrics: {self.metrics}")
        
        return self.metrics
