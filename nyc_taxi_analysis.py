# NYC Taxi Data Analysis with PySpark in Databricks
# Load data, perform transformations, and execute analytical queries

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import requests

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("NYC Taxi Data Analysis") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Step 1: Load NYC Taxi Data to DBFS
def load_data_to_dbfs():
    """Download and load NYC taxi data to DBFS"""
    
    # Download data using wget or curl (run in Databricks cell)
    # %sh wget -O /tmp/yellow_tripdata_2020-01.csv https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2020-01.csv
    # dbutils.fs.cp("file:/tmp/yellow_tripdata_2020-01.csv", "/FileStore/shared_uploads/nyc_taxi/yellow_tripdata_2020-01.csv")
    
    # Load data from DBFS
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("/FileStore/shared_uploads/nyc_taxi/yellow_tripdata_2020-01.csv")
    
    return df

# Step 2: Load and prepare the dataset
def prepare_dataset():
    """Load and clean the NYC taxi dataset"""
    
    # Define schema for better performance
    schema = StructType([
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("RatecodeID", IntegerType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True)
    ])
    
    # Load data with schema
    df = spark.read \
        .option("header", "true") \
        .schema(schema) \
        .csv("/FileStore/shared_uploads/nyc_taxi/yellow_tripdata_2020-01.csv")
    
    # Clean data - remove nulls and invalid records
    df_clean = df.filter(
        (col("fare_amount") >= 0) & 
        (col("passenger_count") > 0) & 
        (col("trip_distance") > 0) &
        (col("tpep_pickup_datetime").isNotNull()) &
        (col("tpep_dropoff_datetime").isNotNull())
    )
    
    return df_clean

# Query 1: Add Revenue column
def add_revenue_column(df):
    """Add Revenue column as sum of fare components"""
    
    df_with_revenue = df.withColumn(
        "Revenue",
        col("fare_amount") + 
        col("extra") + 
        col("mta_tax") + 
        col("improvement_surcharge") + 
        col("tip_amount") + 
        col("tolls_amount") + 
        col("total_amount")
    )
    
    return df_with_revenue

# Query 2: Passenger count by area (Location ID)
def passenger_count_by_area(df):
    """Count total passengers by pickup location"""
    
    result = df.groupBy("PULocationID") \
        .agg(sum("passenger_count").alias("total_passengers")) \
        .orderBy(desc("total_passengers"))
    
    return result

# Query 3: Average fare by vendor
def average_fare_by_vendor(df):
    """Calculate real-time average fare by vendor"""
    
    result = df.groupBy("VendorID") \
        .agg(
            avg("fare_amount").alias("avg_fare"),
            avg("total_amount").alias("avg_total_earning"),
            count("*").alias("trip_count")
        ) \
        .orderBy("VendorID")
    
    return result

# Query 4: Payment mode analysis
def payment_mode_analysis(df):
    """Moving count of payments by payment type"""
    
    # Add window for moving count
    window_spec = Window.partitionBy("payment_type").orderBy("tpep_pickup_datetime")
    
    result = df.withColumn(
        "moving_payment_count",
        row_number().over(window_spec)
    ).groupBy("payment_type") \
     .agg(
         count("*").alias("total_payments"),
         max("moving_payment_count").alias("max_moving_count")
     ) \
     .orderBy(desc("total_payments"))
    
    return result

# Query 5: Top vendors by date with passengers and distance
def top_vendors_by_date(df, target_date="2020-01-15"):
    """Find highest gaining vendors on specific date"""
    
    result = df.filter(date_format("tpep_pickup_datetime", "yyyy-MM-dd") == target_date) \
        .groupBy("VendorID", date_format("tpep_pickup_datetime", "yyyy-MM-dd").alias("date")) \
        .agg(
            sum("total_amount").alias("total_revenue"),
            sum("passenger_count").alias("total_passengers"),
            sum("trip_distance").alias("total_distance"),
            count("*").alias("trip_count")
        ) \
        .orderBy(desc("total_revenue")) \
        .limit(2)
    
    return result

# Query 6: Most passengers between route locations
def most_passengers_by_route(df):
    """Find routes with most passengers"""
    
    result = df.groupBy("PULocationID", "DOLocationID") \
        .agg(
            sum("passenger_count").alias("total_passengers"),
            count("*").alias("trip_count"),
            avg("trip_distance").alias("avg_distance")
        ) \
        .orderBy(desc("total_passengers")) \
        .limit(10)
    
    return result

# Query 7: Top pickup locations in recent time window
def top_pickup_locations_recent(df, seconds=10):
    """Get top pickup locations with most passengers in last N seconds"""
    
    # Get max timestamp and create time window
    max_time = df.agg(max("tpep_pickup_datetime")).collect()[0][0]
    time_threshold = max_time - expr(f"INTERVAL {seconds} SECONDS")
    
    result = df.filter(col("tpep_pickup_datetime") >= time_threshold) \
        .groupBy("PULocationID") \
        .agg(
            sum("passenger_count").alias("recent_passengers"),
            count("*").alias("recent_trips")
        ) \
        .orderBy(desc("recent_passengers")) \
        .limit(10)
    
    return result

# Step 3: Create external parquet table
def create_parquet_table(df, table_name="nyc_taxi_data"):
    """Write data as external parquet table"""
    
    # Write to parquet format
    parquet_path = f"/FileStore/tables/{table_name}"
    
    df.write \
        .mode("overwrite") \
        .option("path", parquet_path) \
        .saveAsTable(table_name)
    
    print(f"Table {table_name} created at {parquet_path}")
    return parquet_path

# Main execution function
def main():
    """Execute all queries and analysis"""
    
    print("Loading NYC Taxi Data...")
    df = prepare_dataset()
    
    print(f"Dataset loaded with {df.count()} records")
    df.printSchema()
    
    # Query 1: Add Revenue column
    print("\n=== Query 1: Adding Revenue Column ===")
    df_with_revenue = add_revenue_column(df)
    df_with_revenue.select("fare_amount", "extra", "mta_tax", "tip_amount", "Revenue").show(5)
    
    # Query 2: Passenger count by area
    print("\n=== Query 2: Passenger Count by Area ===")
    passenger_by_area = passenger_count_by_area(df_with_revenue)
    passenger_by_area.show(10)
    
    # Query 3: Average fare by vendor
    print("\n=== Query 3: Average Fare by Vendor ===")
    avg_fare_vendor = average_fare_by_vendor(df_with_revenue)
    avg_fare_vendor.show()
    
    # Query 4: Payment mode analysis
    print("\n=== Query 4: Payment Mode Analysis ===")
    payment_analysis = payment_mode_analysis(df_with_revenue)
    payment_analysis.show()
    
    # Query 5: Top vendors by date
    print("\n=== Query 5: Top Vendors by Date ===")
    top_vendors = top_vendors_by_date(df_with_revenue)
    top_vendors.show()
    
    # Query 6: Most passengers by route
    print("\n=== Query 6: Most Passengers by Route ===")
    route_analysis = most_passengers_by_route(df_with_revenue)
    route_analysis.show()
    
    # Query 7: Recent pickup locations
    print("\n=== Query 7: Top Recent Pickup Locations ===")
    recent_pickups = top_pickup_locations_recent(df_with_revenue, 10)
    recent_pickups.show()
    
    # Create parquet table
    print("\n=== Creating Parquet Table ===")
    parquet_path = create_parquet_table(df_with_revenue)
    
    # Register as temp view for SQL queries
    df_with_revenue.createOrReplaceTempView("taxi_data")
    
    print("\nAnalysis completed successfully!")
    return df_with_revenue

# Execute main function
if __name__ == "__main__":
    taxi_df = main()