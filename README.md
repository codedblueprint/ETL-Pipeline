# ETL Pipeline — Weather Data

An end-to-end **ETL (Extract, Transform, Load) pipeline** built with Python, SQL, and PostgreSQL.  
The pipeline collects hourly weather data from the **Open-Meteo API**, cleans and validates it, and stores it in a relational database for analysis.

---

## Project Overview

This project demonstrates a **production-style data engineering workflow**:
- External API ingestion
- Data cleaning and validation
- Relational data modeling
- Idempotent data loading
- Local orchestration and scheduling

The pipeline is designed to be **re-runnable, testable, and extensible**.

---

## Architecture

```
Open-Meteo API
      ↓
Extract (Python + requests)
      ↓
Transform (pandas)
      ↓
Load (PostgreSQL + SQLAlchemy)
```


---

## Features

- **ETL Architecture**: Clear separation of Extract → Transform → Load
- **Data Extraction**: Pulls hourly weather data from the Open-Meteo public API
- **Data Transformation**:
  - Timestamp normalization
  - Type casting and validation
  - Deduplication
- **Data Loading**:
  - PostgreSQL persistence
  - Primary key enforcement for idempotency
- **Scheduling**: Optional periodic execution using APScheduler
- **Query Utilities**: SQL scripts and Python helpers for data validation

---

## Tech Stack

- **Python 3**
- **pandas** — data transformation
- **SQLAlchemy** — database connection & transactions
- **requests** — API ingestion
- **APScheduler** — scheduling
- **PostgreSQL** — analytical storage
- **Docker** — local database environment

---

## How to Run

### 1. Clone the Repository

```bash
git clone https://github.com/codedblueprint/ETL-Pipeline.git
cd ETL-Pipeline
```

### 2. Set Up Python Environment

```bash
python -m venv .venv
source .venv/bin/activate   # macOS / Linux
# .venv\Scripts\activate    # Windows

pip install -r requirements.txt
```

### 3. Configure Environment Variables

```bash
cp .env.example .env
```

Edit `.env` with your database credentials:
```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=weather_db
DB_USER=postgres
DB_PASSWORD=postgres
```

### 4. Start PostgreSQL (Docker)

```bash
docker compose up -d
```

### 5. Run the Pipeline

```bash
python main.py
```

---

## Usage Examples

### Query the Database

```bash
python query_db.py
```

### Schedule Periodic Runs

```bash
python scheduler.py
```

### Run Individual Phases

```bash
# Extract only
python extract.py

# Transform only
python transform.py

# Test database connection
python load.py
```

---

## Database Schema

**Table: `weather_data`**

| Column          | Type           | Description              |
|-----------------|----------------|--------------------------|
| `id`            | SERIAL (PK)    | Auto-incrementing ID     |
| `timestamp`     | TIMESTAMP      | Observation time (unique)|
| `temperature_c` | NUMERIC(5,2)   | Temperature (°C)         |
| `precip_mm`     | NUMERIC(5,2)   | Precipitation (mm)       |
| `created_at`    | TIMESTAMP      | Record creation time     |

---

## Verification Queries

```sql
-- Count total records
SELECT COUNT(*) FROM weather_data;

-- View latest records
SELECT *
FROM weather_data
ORDER BY timestamp DESC
LIMIT 5;

-- Get temperature statistics
SELECT 
    ROUND(AVG(temperature_c), 2) as avg_temp,
    ROUND(MIN(temperature_c), 2) as min_temp,
    ROUND(MAX(temperature_c), 2) as max_temp
FROM weather_data;
```

For more analytics queries, see [`sql/queries.sql`](sql/queries.sql).


---

## Future Improvements

- [ ] Cloud storage for raw data (AWS S3)
- [ ] Data quality checks & alerts
- [ ] Centralized orchestration (Apache Airflow)
- [ ] Monitoring and logging (Grafana/Prometheus)
- [ ] Unit and integration tests
- [ ] CI/CD pipeline (GitHub Actions)

---


## Author

**codedblueprint**  
_December 28, 2025_
