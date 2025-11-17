#!/usr/bin/env python3
"""Quick verification script to check the bronze transactions table"""
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Create Spark session
builder = (
    SparkSession.builder
    .appName("VerifyBronzeTable")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Read the bronze table
df = spark.read.format("delta").load("data/bronze/transactions")

print("=" * 80)
print("BRONZE TRANSACTIONS TABLE VERIFICATION")
print("=" * 80)

# Show schema
print("\nSchema:")
df.printSchema()

# Show sample data
print("\nSample Data (5 rows):")
df.show(5, truncate=False)

# Show row count
print(f"\nTotal Rows: {df.count()}")

# Verify metadata columns
print("\nMetadata Columns Verification:")
print(f"  - Rows with null _ingest_ts: {df.filter(df._ingest_ts.isNull()).count()}")
print(f"  - Rows with null _file_name: {df.filter(df._file_name.isNull()).count()}")
print(f"  - Rows with null transaction_id: {df.filter(df.transaction_id.isNull()).count()}")

# Show column list
print(f"\nColumns ({len(df.columns)}):")
for col in df.columns:
    print(f"  - {col}")

spark.stop()
print("\n" + "=" * 80)
