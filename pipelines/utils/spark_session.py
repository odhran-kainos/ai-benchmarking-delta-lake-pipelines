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