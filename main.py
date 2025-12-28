# Import extract and transform functions from respective modules
# separation of concerns for modularity and maintainability
from extract import extract_weather_data #  extract_weather_data from the extract module
from transform import transform_weather # transform_weather from the transform module
# for keeping each phase of the ETL pipeline in its own file, making the codebase more maintainable and testable

# Main ETL (Extract, Transform, Load) pipeline process: entry point for 
# orchestrates the weather data processing workflow:
# orchestration pattern: raw data in, cleaned, inspect, error handling

# pipeline only run: flexibility for different use cases like testing or integration with other systems
if __name__ == "__main__": # pipeline only runs when execute script directly and not by import module elsewhere
    raw = extract_weather_data() # handles API calls and raw data retrieval store them as raw variable
    clean = transform_weather(raw) # handles data cleaning and standardisation store them as clean variable
    print(clean.head()) # 5 row of output for quick visual check
    print(clean.dtypes) # for data validation
