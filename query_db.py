# PostgreSQL Database Query & Analysis - Serves as observation layer
# that data is collected, transformed, and stored correctly
# check data integrity and quality
# verify schedular ETL pipeline runs as expected and keeping historical record
# as a Pipeline health check
# for weather data stored in PostgreSQL
# using pandas for data retrieval and analysis

# Pandas for data manipulation and analysis
import pandas as pd
# SQLAlchemy for database connection
from sqlalchemy import create_engine
import os
# dotenv for loading environment variable management
# reading Database credentials from .env file
# Same as in load.py module for consistency and security best practices
# keep sensitive credentials out of source codebase
from dotenv import load_dotenv

load_dotenv()

# function creates SQLAlchemy engine using environment variables with connection pooling capability
# retrieve PostgreSQL connection parameters configuration value from environment variable with sensible defaults
def get_db_engine():
    """Create and return SQLAlchemy engine for PostgreSQL."""
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "weather_db")
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "postgres")
    
    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}" # standard format for PostgreSQL connection string
    engine = create_engine(connection_string) # create SQLAlchemy engine abstracts database connection details
    return engine

# Connection Management & Query Execution
# function to query and display weather data summary statistics from PostgreSQL
# connects object serves as gateway to database, executes SQL queries to retrieve summary statistics
# that SQLAlchemy use connection pooling manages efficient reuse of database connections
# for scalability and performance in production when multiple queries executed in database concurrently
def query_database():
    """Query and display weather data from PostgreSQL database."""
    engine = get_db_engine()
    
    # Use connection context manager to ensure proper resource management
    # finally block to ensure engine disposed after use to free up resources
    try:
        with engine.connect() as conn:
            print("=" * 60)
            print("WEATHER DATA SUMMARY")
            print("=" * 60)
            
            # Total records
            # perform SQL query using panda to count total records in weather_data table
            # display dataset size for validation
            total = pd.read_sql_query("SELECT COUNT(*) as count FROM weather_data", conn)
            print(f"\nTotal Records: {total['count'][0]}")
            
            # Date range
            # PostgreSQL MIN(), MAX() aggregate function for temporal span of data
            # to get earliest and latest timestamps in the dataset
            # useful to verify scheduled pipeline is collecting data as expected consistently over time
            date_range = pd.read_sql_query(
                "SELECT MIN(timestamp) as earliest, MAX(timestamp) as latest FROM weather_data", 
                conn
            )
            print(f"Date Range: {date_range['earliest'][0]} to {date_range['latest'][0]}")
            
            # Temperature stats
            # SQL aggregate functions: AVG(), MIN(), MAX() for temperature statistics
            # Wrap values with ROUND(temperature_c, 2) for better readability
            # [0] to access first row of result set for single-row result to get scalar values
            temp_stats = pd.read_sql_query(
                """SELECT 
                    ROUND(AVG(temperature_c), 2) as avg_temp,
                    ROUND(MIN(temperature_c), 2) as min_temp,
                    ROUND(MAX(temperature_c), 2) as max_temp
                FROM weather_data""",
                conn
            )
            print(f"\nTemperature (°C):")
            print(f"  Average: {temp_stats['avg_temp'][0]}°C")
            print(f"  Min: {temp_stats['min_temp'][0]}°C")
            print(f"  Max: {temp_stats['max_temp'][0]}°C")
            
            # Precipitation stats
            # with conditional aggregation using COUNT with CASE statement
            # sum(precip_mm) for total precipitation
            # COUNT(CASE WHEN precip_mm > 0 THEN 1 END) conditional aggregation
            # for counting rainy hours: increments the count when precipitation exceeds zero
            # rather than just total number of records of precipitation, it shows frequency of rain events vs intensity
            precip_stats = pd.read_sql_query(
                """SELECT 
                    ROUND(SUM(precip_mm), 2) as total_precip,
                    COUNT(CASE WHEN precip_mm > 0 THEN 1 END) as rainy_hours
                FROM weather_data""",
                conn
            )
            print(f"\nPrecipitation:")
            print(f"  Total: {precip_stats['total_precip'][0]} mm")
            print(f"  Rainy Hours: {precip_stats['rainy_hours'][0]}")
            
            # Latest 10 records
            print("\n" + "=" * 60)
            print("LATEST 10 RECORDS")
            print("=" * 60)
            # finally, retrieve and display the latest 10 records from weather_data table
            # for scheduler.py verification purpose and recent pipeline runs
            latest = pd.read_sql_query(
                "SELECT * FROM weather_data ORDER BY timestamp DESC LIMIT 10",
                conn
            )
            print(latest.to_string(index=False)) # format DataFrame as string for better console output readability for logs
    
    finally:
        engine.dispose()

# standalone execution for quick querying
# instant database connection and runs query_database function
# for quick summary statistics retrieval without needing to run the full ETL pipeline
# make it easy access and inspect the stored weather data as reporting dashboard
# for verification ETL pipeline runs and data quality checks

if __name__ == "__main__":
    query_database()
