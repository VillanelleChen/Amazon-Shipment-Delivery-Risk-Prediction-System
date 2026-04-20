-- ============================================================
-- Create database
-- ============================================================
CREATE DATABASE IF NOT EXISTS amazon;
USE amazon;

-- ============================================================
-- Drop existing tables if needed (optional)
-- ============================================================
DROP TABLE IF EXISTS dim_environment;
DROP TABLE IF EXISTS dim_operational;

-- ============================================================
-- Table: dim_environment
-- Matches dim_environment.csv exactly
-- ============================================================
CREATE TABLE dim_environment (
    date_key DATE NOT NULL,
    region VARCHAR(10) NOT NULL,
    precipitation_mm DECIMAL(10,5) NOT NULL,
    wind_speed_kmh DECIMAL(10,5) NOT NULL,
    traffic_congestion_score DECIMAL(10,5) NOT NULL,
    is_holiday TINYINT(1) NOT NULL,
    
    PRIMARY KEY (date_key, region)
);

-- ============================================================
-- Table: dim_operational
-- Matches dim_operational.csv exactly
-- Cross join expands carriers × warehouses
-- ============================================================
CREATE TABLE dim_operational (
    carrier VARCHAR(50) NOT NULL,
    warehouse_id VARCHAR(10) NOT NULL,
    carrier_delay_rate_30d DECIMAL(10,5) NOT NULL,
    carrier_fail_rate_30d DECIMAL(10,5) NOT NULL,
    carrier_lost_rate_30d DECIMAL(10,5) NOT NULL,
    warehouse_load_index DECIMAL(10,5) NOT NULL,
    route_difficulty_score INT NOT NULL
);
