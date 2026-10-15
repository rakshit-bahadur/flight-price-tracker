# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest fare quotes (Bronze)
# MAGIC Gets fare quotes for the flights loaded in this run from the configured fare provider and stores
# MAGIC them as-is in the bronze table.
# MAGIC
# MAGIC - `simulated` (default): realistic fares for the real AeroDataBox flights, marked `provider = 'simulated'`.
# MAGIC - `json_file`: fares exported from a real fare source, read from the folder in `fare_path`.

# COMMAND ----------

# MAGIC %run ./_audit

# COMMAND ----------

import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.abspath("../src"))

from flight_tracker import config
from flight_tracker.aerodatabox import parse_departures
from flight_tracker.fares import JsonFileFareProvider, SimulatedFareProvider

# COMMAND ----------

dbutils.widgets.dropdown("fare_provider", "simulated", ["simulated", "json_file"], "Fare provider")
dbutils.widgets.text("fare_path", "/Volumes/workspace/flight_tracker/fare_exports", "Folder for json_file provider")

run_id = dbutils.jobs.taskValues.get(taskKey="ingest_flight_schedules", key="run_id", debugValue="")
if not run_id:
    run_id = spark.sql(f"SELECT max(run_id) FROM {config.SCHEDULES_RAW}").first()[0]
print(f"run_id = {run_id}")
started_at = datetime.now(timezone.utc).replace(tzinfo=None)

# COMMAND ----------

payloads = spark.table(config.SCHEDULES_RAW).where(f"run_id = '{run_id}'").select("airport_iata", "payload").collect()

tracked = {r.code for r in config.ROUTES}
flights = [
    f
    for p in payloads
    for f in parse_departures(json.loads(p.payload), p.airport_iata)
    if f"{f['origin']}-{f['destination']}" in tracked
]
print(f"{len(flights)} flights on tracked routes")

# COMMAND ----------

provider_name = dbutils.widgets.get("fare_provider")
provider = (
    SimulatedFareProvider()
    if provider_name == "simulated"
    else JsonFileFareProvider(dbutils.widgets.get("fare_path"))
)

quoted_at = datetime.now(timezone.utc).replace(tzinfo=None)
quotes = provider.quotes(flights, quoted_at)
print(f"{len(quotes)} quotes from provider '{provider.name}'")

# COMMAND ----------

spark.createDataFrame(
    [(run_id, provider.name, json.dumps(quotes), len(quotes), datetime.now(timezone.utc).replace(tzinfo=None))],
    "run_id string, provider string, payload string, quote_count int, ingested_at timestamp",
).write.mode("append").saveAsTable(config.FARES_RAW)

write_audit(
    config.PIPELINE_RUNS, run_id, "ingest_fare_quotes", "SUCCESS" if quotes else "EMPTY",
    rows_in=len(flights), rows_out=len(quotes), started_at=started_at,
    message=f"provider={provider.name}",
)
