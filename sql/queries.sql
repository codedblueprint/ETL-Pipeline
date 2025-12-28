-- Weather Data Analysis Queries
-- Run these queries using: sqlite3 weather_data.db < sql/queries.sql

-- 1. View all records
SELECT * FROM weather_data 
ORDER BY timestamp DESC;

-- 2. Get temperature statistics
SELECT 
    ROUND(AVG(temperature_c), 2) as avg_temp,
    ROUND(MIN(temperature_c), 2) as min_temp,
    ROUND(MAX(temperature_c), 2) as max_temp,
    ROUND(MAX(temperature_c) - MIN(temperature_c), 2) as temp_range
FROM weather_data;

-- 3. Find coldest and warmest hours
SELECT timestamp, temperature_c
FROM weather_data
ORDER BY temperature_c DESC
LIMIT 5;

-- 4. Precipitation analysis
SELECT 
    ROUND(SUM(precip_mm), 2) as total_precipitation,
    ROUND(AVG(precip_mm), 2) as avg_precipitation,
    COUNT(CASE WHEN precip_mm > 0 THEN 1 END) as rainy_hours,
    ROUND(COUNT(CASE WHEN precip_mm > 0 THEN 1 END) * 100.0 / COUNT(*), 2) as rainy_percentage
FROM weather_data;

-- 5. Hourly breakdown - find rainy hours
SELECT timestamp, temperature_c, precip_mm
FROM weather_data
WHERE precip_mm > 0
ORDER BY precip_mm DESC;

-- 6. Daily aggregates (group by date)
SELECT 
    DATE(timestamp) as date,
    ROUND(AVG(temperature_c), 2) as avg_temp,
    ROUND(MIN(temperature_c), 2) as min_temp,
    ROUND(MAX(temperature_c), 2) as max_temp,
    ROUND(SUM(precip_mm), 2) as total_precip
FROM weather_data
GROUP BY DATE(timestamp)
ORDER BY date;

-- 7. Recent 24 hours forecast
SELECT 
    timestamp,
    temperature_c,
    precip_mm,
    CASE 
        WHEN precip_mm > 0 THEN '🌧️ Rainy'
        WHEN temperature_c < 5 THEN '❄️ Cold'
        WHEN temperature_c > 15 THEN '☀️ Warm'
        ELSE '⛅ Mild'
    END as conditions
FROM weather_data
ORDER BY timestamp ASC
LIMIT 24;

-- 8. Count records by creation date
SELECT 
    DATE(created_at) as load_date,
    COUNT(*) as records_loaded
FROM weather_data
GROUP BY DATE(created_at)
ORDER BY load_date DESC;
