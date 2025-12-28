# Weather ETL Pipeline Scheduler

# This scheduler runs the ETL pipeline every 6 hours to ensure
# that the weather data in the database is regularly updated.

# It is for automated scheduling of the ETL pipeline using APScheduler,
# OS-native a cross-platform Python scheduling library.
# production-ready scheduling process with logging and error handling.


# logging for scheduler for monitoring and debugging
import logging
# APScheduler for scheduling the ETL pipeline for standard interval execution
# blocking scheduler for main thread execution and not return until interrupted
# while background scheduler runs in separate thread
# allow main program to continue running other tasks if needed
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
import subprocess
import sys
import os

# Configure logging
# call for dual logging to file - etl_scheduler.log and console output
logging.basicConfig(
    level=logging.INFO,
    # format string for log messages with timestamp, logger name, log level and message
    # for troubleshooting scheduler runs unattended
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    # the log create permanent audit trail of scheduler activity for offline analysis
    handlers=[
        logging.FileHandler('etl_scheduler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Pipeline execution function
# run_pipeline() use subprocess.run() to call main.py to run the ETL pipeline
# create an isolated process for the ETL pipeline from crashes, hangs, or consumes excessive memory
# that would not affect the scheduler's stability
def run_pipeline():
    """Execute the ETL pipeline."""
    logger.info("="*60)
    logger.info(f"Running ETL Pipeline at {datetime.now()}")
    logger.info("="*60)
    
    # path resolution for main.py and environment safeguard
    try:
        # construct absolute path to main.py using os.path.dirname(os.path.abspath(__file__))
        # to locate the scheduler directory and build path to main.py
        script_dir = os.path.dirname(os.path.abspath(__file__))
        main_py = os.path.join(script_dir, "main.py")
        # use sys.executable to get the current Python interpreter path
        python_exe = sys.executable
        
        result = subprocess.run(
            [python_exe, main_py],
            cwd=script_dir,
            capture_output=True, # parameter to capture stdout and stderr and redirect them to result object for scheduler to log
            text=True, # parameter to get output as string instead of bytes for better logging
            timeout=300  # 5 minute timeout, protect against hanging processes
        )
        
        if result.stdout:
            logger.info(result.stdout)
        if result.stderr:
            logger.warning(f"Errors: {result.stderr}")

        # 0 set to success execution check return code to determine success or failure
        if result.returncode == 0:
            logger.info("Pipeline completed successfully!")
        else:
            logger.error(f"Pipeline failed with exit code {result.returncode}")
    
    # when ETL pipeline encounters unexpected issues and does not complete in 5 mins
    # log timeout error for investigation
    except subprocess.TimeoutExpired:
        logger.error("Pipeline execution timed out")
    # catch-all for any other exceptions during pipeline execution
    except Exception as e:
        logger.exception(f"Error running pipeline: {e}")
    # otherwise the stuck pipeline will block scheduler from running future jobs

# Exit point for scheduler
if __name__ == "__main__":
    scheduler = BlockingScheduler()
    
    # Run every 6 hours
    # Execute run_pipeline function at 6-hour intervals
    # since first run on startup, so database has fresh data immediately for recent weather data
    scheduler.add_job(run_pipeline, 'interval', hours=6, id='weather_etl')
    
    logger.info("Weather ETL Scheduler Started")
    logger.info("="*60)
    logger.info("Schedule: Every 6 hours")
    logger.info("Press Ctrl+C to exit")
    logger.info("="*60)
    
    # Run once immediately on startup
    run_pipeline()
    
    # Start the scheduler
    # handling KeyboardInterrupt and SystemExit for graceful shutdown
    # blockscheduler.start() runs the scheduler in the main thread
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")
