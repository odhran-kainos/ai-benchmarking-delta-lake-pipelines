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

@task
def validate_pipeline_output():
    """Task to validate pipeline output"""
    spark = create_spark_session()
    
    try:
        # Check if all layers exist and have data
        bronze_df = spark.read.format("delta").load("data/bronze/employees")
        silver_df = spark.read.format("delta").load("data/silver/employees") 
        gold_df = spark.read.format("delta").load("data/gold/department_stats")
        
        bronze_count = bronze_df.count()
        silver_count = silver_df.count()
        gold_count = gold_df.count()
        
        logging.info(f"Bronze layer: {bronze_count} records")
        logging.info(f"Silver layer: {silver_count} records") 
        logging.info(f"Gold layer: {gold_count} records")
        
        if bronze_count > 0 and silver_count > 0 and gold_count > 0:
            return "Validation successful"
        else:
            raise ValueError("Validation failed - empty tables detected")
            
    finally:
        spark.stop()

@flow(name="daily-lakehouse-pipeline")
def daily_lakehouse_flow():
    """Daily lakehouse processing flow"""
    logging.info("Starting daily lakehouse processing")
    
    # Run the ETL pipeline
    pipeline_result = run_etl_pipeline()
    
    # Validate the output
    validation_result = validate_pipeline_output()
    
    logging.info("Daily lakehouse processing completed")
    return {
        "pipeline_result": pipeline_result,
        "validation_result": validation_result
    }

@flow(name="incremental-pipeline")  
def incremental_flow():
    """Incremental processing flow for real-time data"""
    logging.info("Starting incremental processing")
    
    spark = create_spark_session()
    
    try:
        # This would typically read from a streaming source
        # For demo purposes, we'll just run the batch pipeline
        pipeline = SampleETLPipeline(spark)
        pipeline.run()
        
        logging.info("Incremental processing completed")
        return "Incremental pipeline completed successfully"
        
    finally:
        spark.stop()

if __name__ == "__main__":
    # Run the daily flow
    daily_lakehouse_flow()