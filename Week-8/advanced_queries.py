# Advanced PySpark Queries for NYC Taxi Data Analysis
# Additional analytical queries and optimizations

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

# Advanced Query Implementations

def revenue_trend_analysis(df):
    """Analyze revenue trends over time"""
    
    result = df.withColumn("pickup_hour", hour("tpep_pickup_datetime")) \
        .withColumn("pickup_date", date_format("tpep_pickup_datetime", "yyyy-MM-dd")) \
        .groupBy("pickup_date", "pickup_hour") \
        .agg(
            sum("Revenue").alias("hourly_revenue"),
            count("*").alias("trip_count"),
            avg("Revenue").alias("avg_revenue_per_trip")
        ) \
        .orderBy("pickup_date", "pickup_hour")
    
    return result

def vendor_performance_comparison(df):
    """Compare vendor performance metrics"""
    
    vendor_stats = df.groupBy("VendorID") \
        .agg(
            count("*").alias("total_trips"),
            sum("Revenue").alias("total_revenue"),
            avg("Revenue").alias("avg_revenue"),
            avg("trip_distance").alias("avg_distance"),
            avg("passenger_count").alias("avg_passengers"),
            stddev("Revenue").alias("revenue_stddev")
        )
    
    # Calculate market share
    total_trips = df.count()
    total_revenue = df.agg(sum("Revenue")).collect()[0][0]
    
    result = vendor_stats.withColumn(
        "market_share_trips", 
        round((col("total_trips") / total_trips) * 100, 2)
    ).withColumn(
        "market_share_revenue",
        round((col("total_revenue") / total_revenue) * 100, 2)
    )
    
    return result

def peak_hours_analysis(df):
    """Identify peak hours and demand patterns"""
    
    hourly_demand = df.withColumn("pickup_hour", hour("tpep_pickup_datetime")) \
        .groupBy("pickup_hour") \
        .agg(
            count("*").alias("trip_count"),
            sum("passenger_count").alias("total_passengers"),
            avg("Revenue").alias("avg_revenue"),
            avg("trip_distance").alias("avg_distance")
        ) \
        .orderBy("pickup_hour")
    
    return hourly_demand

def location_hotspots(df):
    """Identify pickup and dropoff hotspots"""
    
    pickup_hotspots = df.groupBy("PULocationID") \
        .agg(
            count("*").alias("pickup_count"),
            sum("passenger_count").alias("total_passengers_pickup"),
            avg("Revenue").alias("avg_revenue_from_location")
        ) \
        .orderBy(desc("pickup_count")) \
        .limit(20)
    
    dropoff_hotspots = df.groupBy("DOLocationID") \
        .agg(
            count("*").alias("dropoff_count"),
            sum("passenger_count").alias("total_passengers_dropoff"),
            avg("Revenue").alias("avg_revenue_to_location")
        ) \
        .orderBy(desc("dropoff_count")) \
        .limit(20)
    
    return pickup_hotspots, dropoff_hotspots

def payment_type_analysis(df):
    """Analyze payment patterns and preferences"""
    
    # Payment type mapping (common NYC taxi payment types)
    payment_mapping = {
        1: "Credit Card",
        2: "Cash", 
        3: "No Charge",
        4: "Dispute",
        5: "Unknown",
        6: "Voided Trip"
    }
    
    # Create payment type description
    payment_df = df.withColumn(
        "payment_description",
        when(col("payment_type") == 1, "Credit Card")
        .when(col("payment_type") == 2, "Cash")
        .when(col("payment_type") == 3, "No Charge")
        .when(col("payment_type") == 4, "Dispute")
        .otherwise("Other")
    )
    
    result = payment_df.groupBy("payment_type", "payment_description") \
        .agg(
            count("*").alias("transaction_count"),
            sum("Revenue").alias("total_revenue"),
            avg("Revenue").alias("avg_revenue"),
            avg("tip_amount").alias("avg_tip"),
            (avg("tip_amount") / avg("fare_amount") * 100).alias("avg_tip_percentage")
        ) \
        .orderBy(desc("transaction_count"))
    
    return result

def distance_revenue_correlation(df):
    """Analyze correlation between distance and revenue"""
    
    # Create distance buckets
    distance_buckets = df.withColumn(
        "distance_bucket",
        when(col("trip_distance") <= 1, "0-1 miles")
        .when(col("trip_distance") <= 3, "1-3 miles")
        .when(col("trip_distance") <= 5, "3-5 miles")
        .when(col("trip_distance") <= 10, "5-10 miles")
        .otherwise("10+ miles")
    )
    
    result = distance_buckets.groupBy("distance_bucket") \
        .agg(
            count("*").alias("trip_count"),
            avg("Revenue").alias("avg_revenue"),
            avg("fare_amount").alias("avg_fare"),
            avg("tip_amount").alias("avg_tip"),
            min("trip_distance").alias("min_distance"),
            max("trip_distance").alias("max_distance")
        ) \
        .orderBy("avg_revenue")
    
    return result

def time_based_surge_analysis(df):
    """Analyze surge pricing patterns by time"""
    
    # Calculate revenue per mile as proxy for surge
    surge_df = df.withColumn(
        "revenue_per_mile",
        when(col("trip_distance") > 0, col("Revenue") / col("trip_distance"))
        .otherwise(0)
    ).withColumn("pickup_hour", hour("tpep_pickup_datetime")) \
     .withColumn("day_of_week", dayofweek("tpep_pickup_datetime"))
    
    result = surge_df.groupBy("pickup_hour", "day_of_week") \
        .agg(
            avg("revenue_per_mile").alias("avg_revenue_per_mile"),
            count("*").alias("trip_count"),
            stddev("revenue_per_mile").alias("revenue_per_mile_stddev")
        ) \
        .orderBy("day_of_week", "pickup_hour")
    
    return result

def passenger_group_analysis(df):
    """Analyze passenger group patterns"""
    
    result = df.groupBy("passenger_count") \
        .agg(
            count("*").alias("trip_count"),
            sum("Revenue").alias("total_revenue"),
            avg("Revenue").alias("avg_revenue"),
            avg("trip_distance").alias("avg_distance"),
            avg("tip_amount").alias("avg_tip")
        ) \
        .withColumn(
            "revenue_per_passenger",
            col("avg_revenue") / col("passenger_count")
        ) \
        .orderBy("passenger_count")
    
    return result

# Optimization functions
def optimize_queries(df):
    """Apply optimizations for better performance"""
    
    # Cache frequently used dataframe
    df.cache()
    
    # Repartition by date for time-based queries
    df_optimized = df.repartition(col("tpep_pickup_datetime"))
    
    # Create broadcast variables for small lookup tables if needed
    # broadcast_var = spark.sparkContext.broadcast(lookup_dict)
    
    return df_optimized

def create_summary_statistics(df):
    """Generate comprehensive summary statistics"""
    
    summary_stats = df.select(
        count("*").alias("total_records"),
        countDistinct("VendorID").alias("unique_vendors"),
        countDistinct("PULocationID").alias("unique_pickup_locations"),
        countDistinct("DOLocationID").alias("unique_dropoff_locations"),
        min("tpep_pickup_datetime").alias("earliest_trip"),
        max("tpep_pickup_datetime").alias("latest_trip"),
        sum("Revenue").alias("total_revenue"),
        avg("Revenue").alias("avg_revenue"),
        sum("passenger_count").alias("total_passengers"),
        avg("passenger_count").alias("avg_passengers"),
        sum("trip_distance").alias("total_distance"),
        avg("trip_distance").alias("avg_distance")
    )
    
    return summary_stats

# Main function to run all advanced queries
def run_advanced_analysis(df):
    """Execute all advanced analytical queries"""
    
    print("=== Advanced NYC Taxi Data Analysis ===\n")
    
    # Revenue trend analysis
    print("1. Revenue Trend Analysis")
    revenue_trends = revenue_trend_analysis(df)
    revenue_trends.show(10)
    
    # Vendor performance comparison
    print("\n2. Vendor Performance Comparison")
    vendor_performance = vendor_performance_comparison(df)
    vendor_performance.show()
    
    # Peak hours analysis
    print("\n3. Peak Hours Analysis")
    peak_hours = peak_hours_analysis(df)
    peak_hours.show(24)
    
    # Location hotspots
    print("\n4. Location Hotspots")
    pickup_spots, dropoff_spots = location_hotspots(df)
    print("Top Pickup Locations:")
    pickup_spots.show(10)
    print("Top Dropoff Locations:")
    dropoff_spots.show(10)
    
    # Payment analysis
    print("\n5. Payment Type Analysis")
    payment_analysis = payment_type_analysis(df)
    payment_analysis.show()
    
    # Distance-revenue correlation
    print("\n6. Distance-Revenue Analysis")
    distance_analysis = distance_revenue_correlation(df)
    distance_analysis.show()
    
    # Passenger group analysis
    print("\n7. Passenger Group Analysis")
    passenger_analysis = passenger_group_analysis(df)
    passenger_analysis.show()
    
    # Summary statistics
    print("\n8. Summary Statistics")
    summary = create_summary_statistics(df)
    summary.show()
    
    print("\nAdvanced analysis completed!")