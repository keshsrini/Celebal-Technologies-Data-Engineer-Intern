# Databricks Setup and Data Loading Commands
# Run these commands in separate Databricks notebook cells

# Cell 1: Download NYC Taxi Data to DBFS
"""
%sh
# Create directory in DBFS
mkdir -p /tmp/nyc_taxi

# Download the dataset
wget -O /tmp/yellow_tripdata_2020-01.csv https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2020-01.csv

# Check file size
ls -lh /tmp/yellow_tripdata_2020-01.csv
"""

# Cell 2: Copy to DBFS
"""
# Copy file to DBFS
dbutils.fs.mkdirs("/FileStore/shared_uploads/nyc_taxi/")
dbutils.fs.cp("file:/tmp/yellow_tripdata_2020-01.csv", "/FileStore/shared_uploads/nyc_taxi/yellow_tripdata_2020-01.csv")

# Verify file in DBFS
dbutils.fs.ls("/FileStore/shared_uploads/nyc_taxi/")
"""

# Cell 3: Alternative - Direct load from S3
"""
# Direct load from S3 (if above doesn't work)
df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2020-01.csv")

# Save to DBFS
df_raw.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("/FileStore/shared_uploads/nyc_taxi/")
"""

# Cell 4: JSON Flattening Example (if needed)
from pyspark.sql.functions import *
from pyspark.sql.types import *

def flatten_json_data(df):
    """Flatten JSON fields if present in the dataset"""
    
    # Example for nested JSON structure
    # This would be used if your data has nested JSON fields
    
    # Get all column names
    columns = df.columns
    
    # Identify JSON columns (assuming they contain struct types)
    json_columns = [col_name for col_name, col_type in df.dtypes if 'struct' in col_type.lower()]
    
    if json_columns:
        # Flatten each JSON column
        for json_col in json_columns:
            # Get nested field names
            nested_fields = df.select(f"{json_col}.*").columns
            
            # Add flattened columns
            for field in nested_fields:
                df = df.withColumn(f"{json_col}_{field}", col(f"{json_col}.{field}"))
            
            # Drop original JSON column
            df = df.drop(json_col)
    
    return df

# Cell 5: Create External Parquet Table
def create_external_parquet_table():
    """Create external parquet table in Databricks"""
    
    # Load the processed data
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("/FileStore/shared_uploads/nyc_taxi/yellow_tripdata_2020-01.csv")
    
    # Clean and process data
    df_clean = df.filter(
        (col("fare_amount") >= 0) & 
        (col("passenger_count") > 0) & 
        (col("trip_distance") > 0)
    )
    
    # Add Revenue column
    df_processed = df_clean.withColumn(
        "Revenue",
        col("fare_amount") + col("extra") + col("mta_tax") + 
        col("improvement_surcharge") + col("tip_amount") + 
        col("tolls_amount") + col("total_amount")
    )
    
    # Write as external parquet table
    table_location = "/FileStore/tables/nyc_taxi_external"
    
    df_processed.write \
        .mode("overwrite") \
        .option("path", table_location) \
        .saveAsTable("nyc_taxi_external")
    
    print(f"External table created at: {table_location}")
    
    # Verify table creation
    spark.sql("DESCRIBE EXTENDED nyc_taxi_external").show(truncate=False)
    
    return df_processed

# Cell 6: SQL Queries for verification
sql_queries = """
-- Query to check table structure
DESCRIBE nyc_taxi_external;

-- Query to check data quality
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT VendorID) as unique_vendors,
    MIN(tpep_pickup_datetime) as earliest_trip,
    MAX(tpep_pickup_datetime) as latest_trip,
    AVG(Revenue) as avg_revenue
FROM nyc_taxi_external;

-- Sample data preview
SELECT * FROM nyc_taxi_external LIMIT 10;
"""