# Databricks notebook source
# MAGIC %md
# MAGIC # Setup tables
# MAGIC Creates the schema and every Delta table used by the pipeline. Safe to run more than once.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath("../src"))

from flight_tracker import config

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.CATALOG}.{config.SCHEMA}")

# COMMAND ----------

# Bronze: raw API payloads, stored as received so any run can be reprocessed.

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {config.SCHEDULES_RAW} (
    run_id STRING,
    airport_iata STRING,
    window_start_local TIMESTAMP,
    payload STRING,
    http_flights INT,
    ingested_at TIMESTAMP
) USING DELTA
COMMENT 'Raw AeroDataBox departures responses'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {config.FARES_RAW} (
    run_id STRING,
    provider STRING,
    payload STRING,
    quote_count INT,
    ingested_at TIMESTAMP
) USING DELTA
COMMENT 'Raw fare quotes from the configured fare provider'
""")

# COMMAND ----------

# Silver: cleaned, typed and deduplicated.

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {config.FLIGHTS} (
    flight_key STRING,
    flight_number STRING,
    airline_iata STRING,
    airline_name STRING,
    origin STRING,
    destination STRING,
    route STRING,
    departure_date DATE,
    scheduled_departure_utc TIMESTAMP,
    revised_departure_utc TIMESTAMP,
    scheduled_arrival_utc TIMESTAMP,
    status STRING,
    aircraft_model STRING,
    terminal STRING,
    run_id STRING,
    updated_at TIMESTAMP
) USING DELTA
COMMENT 'One row per flight per departure date (latest status wins)'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {config.FARE_QUOTES} (
    quote_key STRING,
    flight_number STRING,
    airline_iata STRING,
    origin STRING,
    destination STRING,
    route STRING,
    departure_date DATE,
    quote_date DATE,
    quoted_at_utc TIMESTAMP,
    days_to_departure INT,
    fare_inr DOUBLE,
    cabin STRING,
    seats_left INT,
    provider STRING,
    run_id STRING
) USING DELTA
PARTITIONED BY (quote_date)
COMMENT 'One row per fare quote; quote_key makes reloads idempotent'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {config.QUARANTINE} (
    run_id STRING,
    source STRING,
    record STRING,
    dq_reasons STRING,
    quarantined_at TIMESTAMP
) USING DELTA
COMMENT 'Records that failed data quality rules, with the reasons'
""")

# COMMAND ----------

# Gold: business-ready aggregates.

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {config.ROUTE_DAILY_PRICES} (
    route STRING,
    departure_date DATE,
    quote_date DATE,
    min_fare_inr DOUBLE,
    avg_fare_inr DOUBLE,
    max_fare_inr DOUBLE,
    quotes INT,
    airlines INT,
    cheapest_flight STRING,
    provider STRING
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {config.AIRLINE_ROUTE_SUMMARY} (
    route STRING,
    airline_iata STRING,
    flights INT,
    quotes INT,
    avg_fare_inr DOUBLE,
    min_fare_inr DOUBLE,
    on_time_share DOUBLE,
    provider STRING,
    refreshed_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {config.PRICE_BY_LEAD_TIME} (
    route STRING,
    lead_time_bucket STRING,
    bucket_order INT,
    avg_fare_inr DOUBLE,
    median_fare_inr DOUBLE,
    quotes INT,
    provider STRING,
    refreshed_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {config.PRICE_ALERTS} (
    route STRING,
    departure_date DATE,
    quote_date DATE,
    current_min_fare_inr DOUBLE,
    baseline_avg_fare_inr DOUBLE,
    change_pct DOUBLE,
    provider STRING,
    run_id STRING,
    notified BOOLEAN,
    created_at TIMESTAMP
) USING DELTA
""")

# COMMAND ----------

# Audit: one row per task per run.

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {config.PIPELINE_RUNS} (
    run_id STRING,
    task STRING,
    status STRING,
    rows_in BIGINT,
    rows_out BIGINT,
    rows_rejected BIGINT,
    message STRING,
    started_at TIMESTAMP,
    finished_at TIMESTAMP
) USING DELTA
""")

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {config.CATALOG}.{config.SCHEMA}"))
