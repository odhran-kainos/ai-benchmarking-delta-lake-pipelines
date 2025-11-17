#!/usr/bin/env python3
"""
Script to run the Bronze Transactions ingestion pipeline.
Outputs metrics to benchmark/metrics/T1/run_metrics.json
"""
import json
import logging
from pathlib import Path
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pipelines.bronze_transactions_pipeline import BronzeTransactionsPipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_spark_session():
    """Create Spark session with Delta Lake support"""
    builder = (
        SparkSession.builder
        .appName("BronzeTransactionIngestion")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    )
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def main():
    """Run the bronze ingestion pipeline and save metrics"""
    logger.info("Starting Bronze Transactions ingestion")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Create and run pipeline
        pipeline = BronzeTransactionsPipeline(spark, "config/pipeline_config.yaml")
        metrics = pipeline.run()
        
        # Save metrics to expected location
        metrics_dir = Path("benchmark/metrics/T1")
        metrics_dir.mkdir(parents=True, exist_ok=True)
        
        metrics_file = metrics_dir / "run_metrics.json"
        with open(metrics_file, 'w') as f:
            json.dump(metrics, f, indent=2)
        
        logger.info(f"Metrics saved to {metrics_file}")
        logger.info(f"Pipeline completed successfully: {metrics}")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
