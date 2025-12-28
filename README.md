# Weather ETL Pipeline

A production-ready ETL (Extract, Transform, Load) pipeline that collects hourly weather data from the Open-Meteo API, transforms it, and stores it in a PostgreSQL database for analysis.

## Features

-  **Data Extraction**: Fetches hourly weather forecasts from Open-Meteo public API
-  **Data Transformation**: Cleans, validates, and standardizes weather data
-  **Data Loading**: Stores processed data in PostgreSQL database
-  **Analytics**: Pre-built SQL queries for weather analysis
-  **Automated Scheduling**: Run pipeline periodically with APScheduler
-  **Query Tool**: Python script to view database statistics and recent records

## Tech Stack

- **Python 3.14**
- **pandas** - Data manipulation
- **SQLAlchemy** - Database ORM
- **requests** - HTTP client
- **APScheduler** - Job scheduling
- **PostgreSQL** - Database

## Installation

### 1. Clone Repository
bash
git clone https://github.com/codedblueprint/ETL-Pipeline.git
cd ETL-Pipeline


### 2. Set Up Python
bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt


### 3. Configure Database
bash
cp .env.example .env
# Edit .env with your database credentials


## Usage

bash
# Run complete pipeline
python main.py

# Query database
python query_db.py

# Start scheduler
python scheduler.py


## Author

**codedblueprint**  
December 28, 2025
