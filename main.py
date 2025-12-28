import sys
import logging # logging for monitoring and debugging

# Import extract and transform functions from respective modules
# separation of concerns for modularity and maintainability
from extract import extract_weather_data #  extract_weather_data from the extract module
from transform import transform_weather # transform_weather from the transform module
from load import get_db_engine, load_weather_data # load functions for database persistence
# for keeping each phase of the ETL pipeline in its own file, making the codebase more maintainable and testable

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Main ETL (Extract, Transform, Load) pipeline process: entry point for 
# orchestrates the weather data processing workflow:
# orchestration pattern: raw data in, cleaned, inspect, error handling
def main():
    """Main ETL pipeline process with error handling."""
    try:
        logger.info("Starting ETL pipeline...")
        
        # Extract
        logger.info("Extracting weather data...")
        raw = extract_weather_data()
        logger.info(f"Extracted {len(raw)} records")
        
        # Transform
        logger.info("Transforming data...")
        clean = transform_weather(raw)
        logger.info(f"Cleaned to {len(clean)} records")
        
        # Load
        logger.info("Loading to database...")
        engine = get_db_engine()
        load_weather_data(clean, engine)
        
        logger.info("ETL pipeline completed successfully!")
        return 0
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        return 1



# pipeline only run: flexibility for different use cases like testing or integration with other systems
if __name__ == "__main__": # pipeline only runs when execute script directly and not by import module elsewhere
    exit_code = main()
    sys.exit(exit_code)
