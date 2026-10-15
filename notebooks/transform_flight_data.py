# Databricks notebook source
# MAGIC %md
# MAGIC # Transform flight data (Silver)
# MAGIC Parses the bronze payloads for this run, applies data quality rules, quarantines bad records,
# MAGIC and MERGEs good records into the silver tables so a rerun never creates duplicates.

# COMMAND ----------

# MAGIC %run ./_audit

# COMMAND ----------

import json
import os
import sys
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import functions as F

sys.path.insert(0, os.path.abspath("../src"))

from flight_tracker import config
from flight_tracker.aerodatabox import parse_departures
from flight_tracker.quality import FARE_RULES, FLIGHT_RULES, validate

# COMMAND ----------

run_id = dbutils.jobs.taskValues.get(taskKey="ingest_flight_schedules", key="run_id", debugValue="")
if not run_id:
    run_id = spark.sql(f"SELECT max(run_id) FROM {config.SCHEDULES_RAW}").first()[0]
print(f"run_id = {run_id}")
started_at = datetime.now(timezone.utc).replace(tzinfo=None)
now = datetime.now(timezone.utc).replace(tzinfo=None)

# COMMAND ----------

def quarantine(records, source):
    if not records:
        return
    spark.createDataFrame(
        [(run_id, source, json.dumps(r, default=str), r["dq_reasons"], now) for r in records],
        "run_id string, source string, record string, dq_reasons string, quarantined_at timestamp",
    ).write.mode("append").saveAsTable(config.QUARANTINE)

# COMMAND ----------

# MAGIC %md ## Flights

# COMMAND ----------

payloads = spark.table(config.SCHEDULES_RAW).where(f"run_id = '{run_id}'").select("airport_iata", "payload").collect()
raw_flights = [f for p in payloads for f in parse_departures(json.loads(p.payload), p.airport_iata)]
good_flights, bad_flights = validate(raw_flights, FLIGHT_RULES)
quarantine(bad_flights, "flights")

FLIGHT_COLS = [
    "flight_number", "airline_iata", "airline_name", "origin", "destination", "scheduled_departure_utc",
    "revised_departure_utc", "scheduled_arrival_utc", "status", "aircraft_model", "terminal",
]
flight_schema = (
    "flight_number string, airline_iata string, airline_name string, origin string, destination string, "
    "scheduled_departure_utc string, revised_departure_utc string, scheduled_arrival_utc string, "
    "status string, aircraft_model string, terminal string"
)
flights_df = (
    spark.createDataFrame([tuple(f[c] for c in FLIGHT_COLS) for f in good_flights], flight_schema)
    .withColumn("scheduled_departure_utc", F.to_timestamp("scheduled_departure_utc", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    .withColumn("revised_departure_utc", F.to_timestamp("revised_departure_utc", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    .withColumn("scheduled_arrival_utc", F.to_timestamp("scheduled_arrival_utc", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    .withColumn("departure_date", F.to_date(F.from_utc_timestamp("scheduled_departure_utc", "Asia/Kolkata")))
    .withColumn("route", F.concat_ws("-", "origin", "destination"))
    .withColumn("flight_key", F.sha2(F.concat_ws("|", "flight_number", "departure_date"), 256))
    .withColumn("run_id", F.lit(run_id))
    .withColumn("updated_at", F.lit(now).cast("timestamp"))
    .dropDuplicates(["flight_key"])
)

(
    DeltaTable.forName(spark, config.FLIGHTS).alias("t")
    .merge(flights_df.alias("s"), "t.flight_key = s.flight_key")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
print(f"flights: {len(good_flights)} good, {len(bad_flights)} quarantined")

# COMMAND ----------

# MAGIC %md ## Fare quotes

# COMMAND ----------

fare_payloads = spark.table(config.FARES_RAW).where(f"run_id = '{run_id}'").select("payload").collect()
raw_quotes = [q for p in fare_payloads for q in json.loads(p.payload)]
good_quotes, bad_quotes = validate(raw_quotes, FARE_RULES)
quarantine(bad_quotes, "fare_quotes")

quote_schema = (
    "flight_number string, origin string, destination string, departure_date string, fare_inr double, "
    "currency string, cabin string, seats_left int, provider string, quoted_at_utc string"
)
airlines = spark.table(config.FLIGHTS).select("flight_number", "departure_date", "airline_iata")

quotes_df = (
    spark.createDataFrame(
        [
            (q["flight_number"], q["origin"], q["destination"], q["departure_date"], float(q["fare_inr"]),
             q.get("currency"), q.get("cabin"), q.get("seats_left"), q.get("provider"), q["quoted_at_utc"])
            for q in good_quotes
        ],
        quote_schema,
    )
    .withColumn("departure_date", F.to_date("departure_date"))
    .withColumn("quoted_at_utc", F.to_timestamp("quoted_at_utc", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    .withColumn("quote_date", F.to_date(F.from_utc_timestamp("quoted_at_utc", "Asia/Kolkata")))
    .withColumn("days_to_departure", F.datediff("departure_date", "quote_date"))
    .withColumn("route", F.concat_ws("-", "origin", "destination"))
    .withColumn(
        "quote_key",
        F.sha2(F.concat_ws("|", "flight_number", "departure_date", "cabin", "provider", "quoted_at_utc"), 256),
    )
    .withColumn("run_id", F.lit(run_id))
    .join(airlines, ["flight_number", "departure_date"], "left")
    .where(F.col("days_to_departure") >= 0)
    .dropDuplicates(["quote_key"])
    .drop("currency")
)

(
    DeltaTable.forName(spark, config.FARE_QUOTES).alias("t")
    .merge(quotes_df.alias("s"), "t.quote_key = s.quote_key AND t.quote_date = s.quote_date")
    .whenNotMatchedInsertAll()
    .execute()
)
print(f"fare quotes: {len(good_quotes)} good, {len(bad_quotes)} quarantined")

# COMMAND ----------

write_audit(
    config.PIPELINE_RUNS, run_id, "transform_flight_data", "SUCCESS",
    rows_in=len(raw_flights) + len(raw_quotes),
    rows_out=len(good_flights) + len(good_quotes),
    rows_rejected=len(bad_flights) + len(bad_quotes),
    started_at=started_at,
)
