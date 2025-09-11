from pipelines.utils.spark_session import create_spark_session
from pipelines.sample_etl_pipeline import SampleETLPipeline
import logging
import sys
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def test_lakehouse_setup():
    """Test the complete lakehouse setup"""
    
    print("🚀 Testing Delta Lakehouse setup...")
    print("=" * 50)
    
    # Create Spark session
    try:
        spark = create_spark_session()
        print("✅ Spark session created successfully")
    except Exception as e:
        print(f"❌ Failed to create Spark session: {e}")
        return False
    
    try:
        # Run sample pipeline
        print("\n📊 Running sample ETL pipeline...")
        pipeline = SampleETLPipeline(spark)
        pipeline.run()
        print("✅ Pipeline execution completed")
        
        # Verify data in each layer
        print("\n" + "=" * 50)
        print("🔍 VERIFYING DATA LAYERS")
        print("=" * 50)
        
        print("\n📥 BRONZE LAYER (Raw Data)")
        print("-" * 30)
        bronze_df = spark.read.format("delta").load("data/bronze/employees")
        bronze_df.show()
        print(f"Records: {bronze_df.count()}")
        
        print("\n🔄 SILVER LAYER (Cleaned Data)")
        print("-" * 35)
        silver_df = spark.read.format("delta").load("data/silver/employees")
        silver_df.show()
        print(f"Records: {silver_df.count()}")
        
        print("\n🏆 GOLD LAYER (Analytics)")
        print("-" * 25)
        
        # Department statistics
        print("Department Statistics:")
        dept_stats_df = spark.read.format("delta").load("data/gold/department_stats")
        dept_stats_df.show()
        
        # Salary band statistics
        print("Salary Band Statistics:")
        salary_stats_df = spark.read.format("delta").load("data/gold/salary_band_stats")
        salary_stats_df.show()
        
        # Company statistics
        print("Company Statistics:")
        company_stats_df = spark.read.format("delta").load("data/gold/company_stats")
        company_stats_df.show()
        
        # Test Delta Lake features
        print("\n" + "=" * 50)
        print("🔬 TESTING DELTA LAKE FEATURES")
        print("=" * 50)
        
        # Table history
        print("\n📜 Table History (Bronze Layer):")
        from delta.tables import DeltaTable
        bronze_table = DeltaTable.forPath(spark, "data/bronze/employees")
        history_df = bronze_table.history(5)
        history_df.select("version", "timestamp", "operation", "operationMetrics").show()
        
        # Schema information
        print("\n📋 Schema Information (Silver Layer):")
        silver_df.printSchema()
        
        print("\n" + "=" * 50)
        print("✅ LAKEHOUSE SETUP TEST COMPLETED SUCCESSFULLY!")
        print("=" * 50)
        
        print("\n🎯 Next Steps:")
        print("1. Explore data with Jupyter: jupyter notebook")
        print("2. Run Prefect workflows: python pipelines/orchestration/prefect_flows.py")  
        print("3. Create your own custom pipelines extending BasePipeline")
        print("4. Experiment with Delta Lake time travel and merge operations")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        spark.stop()

def check_prerequisites():
    """Check if all prerequisites are met"""
    print("🔍 Checking prerequisites...")
    
    # Check Java
    import subprocess
    try:
        result = subprocess.run(['java', '-version'], capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ Java is installed")
        else:
            print("❌ Java is not installed or not in PATH")
            return False
    except FileNotFoundError:
        print("❌ Java is not installed or not in PATH")
        return False
    
    # Check Python packages
    required_packages = ['pyspark', 'delta', 'pandas', 'yaml', 'prefect']
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"✅ {package} is installed")
        except ImportError:
            print(f"❌ {package} is not installed")
            missing_packages.append(package)
    
    if missing_packages:
        print(f"\n❌ Missing packages: {', '.join(missing_packages)}")
        print("Run: pip install -r requirements.txt")
        return False
    
    print("✅ All prerequisites are met")
    return True

if __name__ == "__main__":
    print("🏗️  Delta Lakehouse Setup Verification")
    print("=" * 50)
    
    # Check prerequisites first
    if not check_prerequisites():
        print("\n❌ Prerequisites not met. Please install missing dependencies.")
        sys.exit(1)
    
    # Run the test
    success = test_lakehouse_setup()
    
    if success:
        print("\n🎉 Setup verification completed successfully!")
        sys.exit(0)
    else:
        print("\n💥 Setup verification failed!")
        sys.exit(1)