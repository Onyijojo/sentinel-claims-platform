-- Sentinel Claims Platform — Schema Definitions

-- Staging: temporary landing area — data is COPYed here from S3 landing zone each run
CREATE SCHEMA IF NOT EXISTS staging;

-- Warehouse: permanent dimensional model (facts + dimensions + SCD2)
CREATE SCHEMA IF NOT EXISTS warehouse;

-- Analytics: views and aggregations for reporting consumers
CREATE SCHEMA IF NOT EXISTS analytics;
