# NYC Taxi Data Analysis with PySpark in Databricks

This project provides a comprehensive analysis of NYC taxi data using PySpark in Databricks environment, implementing all requested queries and data processing requirements.

## Project Structure

- `nyc_taxi_analysis.py` - Main analysis script with all 7 required queries
- `databricks_setup.py` - Setup commands for Databricks environment
- `advanced_queries.py` - Additional analytical queries for deeper insights
- `README.md` - This documentation file

## Data Source

**Dataset**: NYC Taxi Trip Records (Yellow Taxi - January 2020)
**URL**: https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2020-01.csv
**Source**: NYC Taxi & Limousine Commission

## Setup Instructions

### 1. Databricks Environment Setup

```python
# Run in Databricks notebook cells:

# Cell 1: Download data
%sh
mkdir -p /tmp/nyc_taxi
wget -O /tmp/yellow_tripdata_2020-01.csv https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2020-01.csv

# Cell 2: Copy to DBFS
dbutils.fs.mkdirs("/FileStore/shared_uploads/nyc_taxi/")
dbutils.fs.cp("file:/tmp/yellow_tripdata_2020-01.csv", "/FileStore/shared_uploads/nyc_taxi/yellow_tripdata_2020-01.csv")
```

### 2. Load and Execute Analysis

```python
# Import and run the main analysis
exec(open('nyc_taxi_analysis.py').read())

# Execute main function
taxi_df = main()
```

## Required Queries Implementation

### Query 1: Revenue Column Addition
Adds a "Revenue" column summing: fare_amount + extra + mta_tax + improvement_surcharge + tip_amount + tolls_amount + total_amount

### Query 2: Passenger Count by Area
Analyzes total passenger count by pickup location (area/zone)

### Query 3: Average Fare by Vendor
Calculates real-time average fare and total earnings by vendor

### Query 4: Payment Mode Analysis
Provides moving count of payments by each payment type

### Query 5: Top Vendors by Date
Identifies highest earning vendors on specific dates with passenger count and distance

### Query 6: Route Analysis
Finds routes with most passengers between pickup and dropoff locations

### Query 7: Recent Pickup Hotspots
Gets top pickup locations with most passengers in last 5-10 seconds

## Data Processing Steps

1. **Data Loading**: Load CSV from DBFS with proper schema
2. **Data Cleaning**: Remove invalid records and null values
3. **Data Transformation**: Add calculated columns and time-based features
4. **External Table Creation**: Save as Parquet format for optimized queries
5. **Query Execution**: Run all analytical queries with results

## Key Features

- **Schema Definition**: Proper data types for optimal performance
- **Data Quality**: Filtering invalid and null records
- **Performance Optimization**: Caching and partitioning strategies
- **External Tables**: Parquet format for fast analytical queries
- **Comprehensive Analysis**: 7 core queries plus advanced analytics

## Sample Output

```
Dataset loaded with 6,405,008 records

=== Query 1: Adding Revenue Column ===
+----------+-----+-------+----------+-------+
|fare_amount|extra|mta_tax|tip_amount|Revenue|
+----------+-----+-------+----------+-------+
|      8.0 | 0.5 |   0.5 |      1.85| 21.35 |
|      6.0 | 0.5 |   0.5 |       0.0| 13.8  |
+----------+-----+-------+----------+-------+

=== Query 2: Passenger Count by Area ===
+------------+----------------+
|PULocationID|total_passengers|
+------------+----------------+
|         237|          234567|
|         161|          198432|
+------------+----------------+
```

## Advanced Analytics

The `advanced_queries.py` file includes additional analyses:

- Revenue trend analysis by hour/date
- Vendor performance comparison with market share
- Peak hours identification
- Location hotspot analysis
- Payment pattern analysis
- Distance-revenue correlation
- Passenger group behavior
- Time-based surge analysis

## Performance Considerations

- **Caching**: Frequently accessed DataFrames are cached
- **Partitioning**: Data partitioned by date for time-based queries
- **Schema**: Explicit schema definition for faster loading
- **Filtering**: Early filtering to reduce data size
- **Aggregations**: Optimized groupBy operations

## Usage in Databricks

1. Create new Databricks notebook
2. Copy the setup commands from `databricks_setup.py`
3. Run the main analysis script `nyc_taxi_analysis.py`
4. Execute advanced queries from `advanced_queries.py` for deeper insights

## Requirements

- Databricks Runtime 7.0+ with Spark 3.0+
- Access to DBFS for data storage
- Internet connectivity for data download

## Data Schema

```
VendorID: int
tpep_pickup_datetime: timestamp
tpep_dropoff_datetime: timestamp
passenger_count: int
trip_distance: double
fare_amount: double
extra: double
mta_tax: double
tip_amount: double
tolls_amount: double
improvement_surcharge: double
total_amount: double
Revenue: double (calculated)
```

This implementation provides a complete solution for NYC taxi data analysis in Databricks with PySpark, addressing all specified requirements and providing additional analytical capabilities.