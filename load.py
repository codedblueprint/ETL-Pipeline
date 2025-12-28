import os
import pandas as pd
from sqlalchemy import create_engine, text
# dotenv for environment variable management
# to keep sensitive info like DB credentials out of codebase as security best practice
from dotenv import load_dotenv

# ETL - LOAD (Extract, Transform, Load) pattern - load persists it to durable storage
# handles loading transformed data into PostgreSQL database
# Functions to configuration management, connect to the database & error handling

# Load environment variables
# loading variables from .env file into environment variables
# configuration available for all functions in this module

load_dotenv()


# Database engine creation function
# create SQLAlchemy engine using environment variables
# connection to PostgreSQL database
# retrieve configuration value from environment variable
# localhost as default
# set PostgreSQL connection string in standard format:

def get_db_engine():
    """Create and return SQLAlchemy engine (SQLite - no installation needed)."""
    # Using SQLite instead of PostgreSQL for simplicity
    db_path = "weather_data.db"
    connection_string = f"sqlite:///{db_path}"
    engine = create_engine(connection_string)
    print(f"✓ Using SQLite database: {db_path}")
    return engine

# Initialise table schema
# execute SQL script in create_tables.sql
def create_tables(engine):
    """Execute the SQL script to create tables."""
    # Read create_tables.sql
    with open("sql/create_tables.sql", "r") as f:
        sql_script = f.read()
    
    # Use SQLAlchemy: connection context manager to execute it
    with engine.connect() as conn:
        conn.execute(text(sql_script)) # Text() wrapper forSQLAlchemy: treat the string as raw SQL
        conn.commit() # call is for the schema changes are persisted to the database
    print("Tables created successfully")

# Data Loading with Pandas Integration
# Function that load the clean DataFrame to PostgreSQL

def load_weather_data(df: pd.DataFrame, engine):
    """Load transformed weather data into PostgreSQL."""
    try:
        # Load data to PostgreSQL
        df.to_sql( # panda to_sql() method for data insertion
            name="weather_data",
            con=engine,
            if_exists="append",  # append new data to existing record instead of replacing the table, for incremental data load
            index=False, # Ask panda not to write index as column, Timestamp column already set
            method="multi"  # parameter for faster bulk insert as batching multiple row into single INSERT
        )
        print(f"Loaded {len(df)} records to database") # return number of record loaded

    # Error handling by try-except block
    # fore report error from e.g. network issues, constraint violations, disk space problems
    except Exception as e:
        print(f"Error loading data: {e}")
        raise # raise exception for handle failure and log error

# run load.py directly to verify database connection and schema without actual loading the data
# for debug connection issues
if __name__ == "__main__":
    # Test connection and table creation
    engine = get_db_engine()
    create_tables(engine)
    print("Database setup complete!")
