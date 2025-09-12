from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import yaml
from pathlib import Path
import os

def create_spark_session():
    """Create Spark session with Delta Lake configuration
    
    Note: This configuration works with Java 18+ by avoiding the 
    getSubject compatibility issues that occur during package resolution.
    """
    
    # Load configuration
    config_path = Path("config/spark_config.yaml")
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    
    builder = SparkSession.builder \
        .appName(config['spark']['app_name']) \
        .master(config['spark']['master'])

    builder = configure_spark_with_delta_pip(builder)

    # Add Spark configurations (excluding Delta-specific ones for now)
    for key, value in config['spark']['config'].items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    return spark




