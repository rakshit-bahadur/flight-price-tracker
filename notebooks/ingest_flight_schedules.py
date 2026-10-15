# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest flight schedules (Bronze)
# MAGIC Calls AeroDataBox for departures from every origin airport in the next window and stores each
# MAGIC response as-is in the bronze table. Creates the `run_id` that every later task uses.

# COMMAND ----------

# MAGIC %run ./_audit

# COMMAND ----------

import json
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.abspath("../src"))

from flight_tracker import config
from flight_tracker.aerodatabox import AeroDataBoxClient

# COMMAND ----------

dbutils.widgets.text("windows", "2", "12-hour windows to fetch per airport")
windows = int(dbutils.widgets.get("windows"))

run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M") + "-" + uuid.uuid4().hex[:6]
started_at = datetime.now(timezone.utc).replace(tzinfo=None)
dbutils.jobs.taskValues.set(key="run_id", value=run_id)
print(f"run_id = {run_id}")

# COMMAND ----------

api_key = dbutils.secrets.get(scope=config.SECRET_SCOPE, key="aerodatabox_api_key")
client = AeroDataBoxClient(api_key)

ist = timezone(timedelta(hours=5, minutes=30))
first_window = datetime.now(ist).replace(minute=0, second=0, microsecond=0, tzinfo=None)

rows, errors = [], []
for airport in config.ORIGIN_AIRPORTS:
    for w in range(windows):
        start_local = first_window + timedelta(hours=config.SCHEDULE_WINDOW_HOURS * w)
        try:
            payload = client.departures(airport, start_local, hours=config.SCHEDULE_WINDOW_HOURS)
            rows.append(
                (run_id, airport, start_local, json.dumps(payload), len(payload.get("departures", [])),
                 datetime.now(timezone.utc).replace(tzinfo=None))
            )
            print(f"{airport} {start_local:%Y-%m-%d %H:%M}: {len(payload.get('departures', []))} flights")
        except Exception as exc:
            errors.append(f"{airport} {start_local:%Y-%m-%d %H:%M}: {exc}")
            print(f"FAILED {errors[-1]}")

# COMMAND ----------

schema = "run_id string, airport_iata string, window_start_local timestamp, payload string, http_flights int, ingested_at timestamp"
if rows:
    spark.createDataFrame(rows, schema).write.mode("append").saveAsTable(config.SCHEDULES_RAW)

status = "SUCCESS" if not errors else ("PARTIAL" if rows else "FAILED")
write_audit(
    config.PIPELINE_RUNS, run_id, "ingest_flight_schedules", status,
    rows_in=len(rows) + len(errors), rows_out=sum(r[4] for r in rows), rows_rejected=len(errors),
    message="; ".join(errors), started_at=started_at,
)

if not rows:
    raise RuntimeError("No schedule data was ingested: " + "; ".join(errors))
