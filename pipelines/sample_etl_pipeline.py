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
            (4, "Diana", "Sales", 70000, "2023-03-05"),
            (5, "Eve", "Engineering", 85000, "2023-01-20"),
            (6, "Frank", "Marketing", 60000, "2023-02-15"),
            (7, "Grace", "Sales", 72000, "2023-03-10")
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
        
        # Validate extraction
        if not self.validate_data(df, "extraction"):
            raise ValueError("Data extraction validation failed")
        
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
        
        # Add derived columns
        silver_df = silver_df.withColumn(
            "salary_band",
            when(col("salary") < 65000, "Low")
            .when(col("salary") < 75000, "Medium")
            .otherwise("High")
        ).withColumn(
            "years_employed",
            months_between(current_date(), col("hire_date")) / 12
        )
        
        # Write to silver layer
        silver_path = self.config['database']['silver_path'] + "/employees"
        self.delta_ops.write_delta_table(silver_df, silver_path, mode="overwrite")
        
        # Validate transformation
        if not self.validate_data(silver_df, "transformation"):
            raise ValueError("Data transformation validation failed")
        
        return silver_df
    
    def load(self, df: DataFrame) -> None:
        """Create gold layer aggregations"""
        # Department salary statistics
        dept_stats = df.groupBy("department").agg(
            count("*").alias("employee_count"),
            avg("salary").alias("avg_salary"),
            max("salary").alias("max_salary"),
            min("salary").alias("min_salary"),
            sum("salary").alias("total_salary")
        ).withColumn("analysis_date", current_date())
        
        # Salary band distribution
        salary_band_stats = df.groupBy("salary_band").agg(
            count("*").alias("employee_count"),
            avg("salary").alias("avg_salary")
        ).withColumn("analysis_date", current_date())
        
        # Overall company statistics
        company_stats = df.agg(
            count("*").alias("total_employees"),
            avg("salary").alias("avg_company_salary"),
            avg("years_employed").alias("avg_years_employed")
        ).withColumn("analysis_date", current_date())
        
        # Write to gold layer
        gold_base_path = self.config['database']['gold_path']
        
        self.delta_ops.write_delta_table(
            dept_stats, 
            f"{gold_base_path}/department_stats", 
            mode="overwrite"
        )
        
        self.delta_ops.write_delta_table(
            salary_band_stats, 
            f"{gold_base_path}/salary_band_stats", 
            mode="overwrite"
        )
        
        self.delta_ops.write_delta_table(
            company_stats, 
            f"{gold_base_path}/company_stats", 
            mode="overwrite"
        )
        
        # Validate load
        if not self.validate_data(dept_stats, "gold layer loading"):
            raise ValueError("Gold layer loading validation failed")
        
        self.logger.info("Gold layer analytics tables created successfully")
    
    def get_sample_analytics(self) -> dict:
        """Get sample analytics from gold layer"""
        gold_base_path = self.config['database']['gold_path']
        
        # Read gold layer tables
        dept_stats = self.spark.read.format("delta").load(f"{gold_base_path}/department_stats")
        company_stats = self.spark.read.format("delta").load(f"{gold_base_path}/company_stats")
        
        return {
            "department_stats": dept_stats.collect(),
            "company_stats": company_stats.collect()
        }