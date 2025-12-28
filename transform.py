import pandas as pd

# Function to transform raw weather data DataFrame
# It cleans and standardises the weather data in the Panda DataFrame.
# ETL - TRANSFORM (Extract, Transform, Load) pattern
# for data quality and consistency for further analysis.
# transformation handles data cleaning and standardisation.

def transform_weather(df: pd.DataFrame) -> pd.DataFrame:
    # 1) Begin with rename columns (clean + consistent)
    # Purpose id to follow a descriptive and consistent naming convention
    # That original (time, temperature_2m, precipitation) 
    # are replaced  (timestamp, temperature_c, precip_mm)
    # Unit: Celsius and millimeters added in columns
    df = df.rename(columns={
        "time": "timestamp",
        "temperature_2m": "temperature_c",
        "precipitation": "precip_mm"
    })

    # 2) Parse timestamp & Type conversion
    # Convert 'timestamp' from string to datetime objects
    # using pd.to_datetime
    # Handle malformed dates by error="coerce" by converting them to NaT
    # Not-a-Time, pandas' equivalent of null for datetime
    # best practice for production data pipelines for error handling
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    # 3) Basic data quality checks
    # remove rows with missing timestamps
    # remove duplicate entries based on timestamp
    # as time-series data should have unique timestamps and no duplicates
    # when data collection error and feed issues
    df = df.dropna(subset=["timestamp"])
    df = df.drop_duplicates(subset=["timestamp"])

    # 4) temperature and precipitation: ensure numeric types
    # Errors during conversion are coerced to NaN
    # error handling prevent break of pipeline
    df["temperature_c"] = pd.to_numeric(df["temperature_c"], errors="coerce")
    df["precip_mm"] = pd.to_numeric(df["precip_mm"], errors="coerce")

    return df

# hook for local testing
# maintainable purpose
if __name__ == "__main__":
    # quick local test (assumes you saved raw extract to CSV, optional)
    pass # run script for manual testing without affect code that import this function as module

