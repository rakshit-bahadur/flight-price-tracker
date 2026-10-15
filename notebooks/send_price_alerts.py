# Databricks notebook source
# MAGIC %md
# MAGIC # Send price alerts
# MAGIC Compares today's lowest fare for each route and departure date with the average of the previous
# MAGIC quote days. Drops of 15% or more are saved to `gold_price_alerts` and emailed.
# MAGIC Email is optional: if the SMTP secrets are not set, alerts are only saved.

# COMMAND ----------

# MAGIC %run ./_audit

# COMMAND ----------

import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.abspath("../src"))

from flight_tracker import config
from flight_tracker.alerts import detect_price_drops, format_alert_email, send_email

# COMMAND ----------

run_id = dbutils.jobs.taskValues.get(taskKey="ingest_flight_schedules", key="run_id", debugValue="manual")
started_at = datetime.now(timezone.utc).replace(tzinfo=None)

daily = [
    {
        "route": r.route,
        "departure_date": r.departure_date.isoformat(),
        "quote_date": r.quote_date.isoformat(),
        "min_fare_inr": r.min_fare_inr,
        "provider": r.provider,
    }
    for r in spark.table(config.ROUTE_DAILY_PRICES)
    .where("departure_date >= current_date()")
    .select("route", "departure_date", "quote_date", "min_fare_inr", "provider")
    .collect()
]

alerts = []
for provider in sorted({d["provider"] for d in daily}):
    for a in detect_price_drops([d for d in daily if d["provider"] == provider]):
        alerts.append({**a, "provider": provider})
print(f"{len(alerts)} price drop(s) found")

# COMMAND ----------

def secret_or_none(key):
    try:
        return dbutils.secrets.get(scope=config.SECRET_SCOPE, key=key)
    except Exception:
        return None


sender = secret_or_none("smtp_sender")
password = secret_or_none("smtp_app_password")
recipients = [r.strip() for r in (secret_or_none("alert_recipients") or "").split(",") if r.strip()]

notified = False
if alerts and sender and password and recipients:
    subject, body = format_alert_email(alerts)
    send_email(subject, body, sender, password, recipients)
    notified = True
    print(f"Email sent to {len(recipients)} recipient(s)")
elif alerts:
    print("SMTP secrets not set; alerts saved without email")

# COMMAND ----------

if alerts:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    spark.createDataFrame(
        [
            (a["route"], a["departure_date"], a["quote_date"], a["current_min_fare_inr"], a["baseline_avg_fare_inr"],
             a["change_pct"], a["provider"], run_id, notified, now)
            for a in alerts
        ],
        "route string, departure_date string, quote_date string, current_min_fare_inr double, "
        "baseline_avg_fare_inr double, change_pct double, provider string, run_id string, notified boolean, "
        "created_at timestamp",
    ).selectExpr(
        "route", "to_date(departure_date) as departure_date", "to_date(quote_date) as quote_date",
        "current_min_fare_inr", "baseline_avg_fare_inr", "change_pct", "provider", "run_id", "notified", "created_at",
    ).write.mode("append").saveAsTable(config.PRICE_ALERTS)

write_audit(
    config.PIPELINE_RUNS, run_id, "send_price_alerts", "SUCCESS",
    rows_in=len(daily), rows_out=len(alerts), started_at=started_at,
    message=f"notified={notified}",
)
