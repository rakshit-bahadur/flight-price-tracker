# Databricks notebook source
# MAGIC %md
# MAGIC Shared helper, loaded with `%run ./_audit` from the other notebooks.

# COMMAND ----------

from datetime import datetime, timezone

from pyspark.sql import Row


def write_audit(table, run_id, task, status, rows_in=0, rows_out=0, rows_rejected=0, message=None, started_at=None):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    spark.createDataFrame(
        [
            Row(
                run_id=run_id,
                task=task,
                status=status,
                rows_in=int(rows_in),
                rows_out=int(rows_out),
                rows_rejected=int(rows_rejected),
                message=(message or "")[:1000] or None,
                started_at=started_at or now,
                finished_at=now,
            )
        ],
        "run_id string, task string, status string, rows_in long, rows_out long, rows_rejected long, "
        "message string, started_at timestamp, finished_at timestamp",
    ).write.mode("append").saveAsTable(table)
