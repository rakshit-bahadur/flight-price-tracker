# Databricks notebook source
# MAGIC %md
# MAGIC # Aggregate flight prices (Gold)
# MAGIC Builds the business tables from silver:
# MAGIC - `gold_route_daily_prices`: lowest, average and highest fare per route, departure date and quote date
# MAGIC - `gold_airline_route_summary`: price level and on-time share per airline and route
# MAGIC - `gold_price_by_lead_time`: how fares change as the departure date gets closer

# COMMAND ----------

# MAGIC %run ./_audit

# COMMAND ----------

import os
import sys
from datetime import datetime, timezone

from pyspark.sql import Window
from pyspark.sql import functions as F

sys.path.insert(0, os.path.abspath("../src"))

from flight_tracker import config

# COMMAND ----------

run_id = dbutils.jobs.taskValues.get(taskKey="ingest_flight_schedules", key="run_id", debugValue="manual")
started_at = datetime.now(timezone.utc).replace(tzinfo=None)
quotes = spark.table(config.FARE_QUOTES)

# COMMAND ----------

# Lowest fare per route/departure/quote date, plus the flight that offered it.
cheapest = Window.partitionBy("route", "departure_date", "quote_date", "provider").orderBy("fare_inr")

route_daily = (
    quotes.withColumn("rank", F.row_number().over(cheapest))
    .groupBy("route", "departure_date", "quote_date", "provider")
    .agg(
        F.min("fare_inr").alias("min_fare_inr"),
        F.round(F.avg("fare_inr"), 2).alias("avg_fare_inr"),
        F.max("fare_inr").alias("max_fare_inr"),
        F.count("*").cast("int").alias("quotes"),
        F.countDistinct("airline_iata").cast("int").alias("airlines"),
        F.max(F.when(F.col("rank") == 1, F.col("flight_number"))).alias("cheapest_flight"),
    )
    .select("route", "departure_date", "quote_date", "min_fare_inr", "avg_fare_inr", "max_fare_inr",
            "quotes", "airlines", "cheapest_flight", "provider")
)
route_daily.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(config.ROUTE_DAILY_PRICES)

# COMMAND ----------

flights = spark.table(config.FLIGHTS)
on_time = (
    flights.where(F.col("status").isin("Departed", "Arrived", "Expected", "Delayed", "EnRoute", "Landed"))
    .withColumn(
        "is_on_time",
        F.when(F.col("revised_departure_utc").isNull(), 1)
        .when((F.unix_timestamp("revised_departure_utc") - F.unix_timestamp("scheduled_departure_utc")) <= 15 * 60, 1)
        .otherwise(0),
    )
    .groupBy("route", "airline_iata")
    .agg(F.count("*").cast("int").alias("flights"), F.round(F.avg("is_on_time"), 3).alias("on_time_share"))
)

airline_summary = (
    quotes.groupBy("route", "airline_iata", "provider")
    .agg(
        F.count("*").cast("int").alias("quotes"),
        F.round(F.avg("fare_inr"), 2).alias("avg_fare_inr"),
        F.min("fare_inr").alias("min_fare_inr"),
    )
    .join(on_time, ["route", "airline_iata"], "left")
    .withColumn("refreshed_at", F.current_timestamp())
    .select("route", "airline_iata", "flights", "quotes", "avg_fare_inr", "min_fare_inr",
            "on_time_share", "provider", "refreshed_at")
)
airline_summary.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(config.AIRLINE_ROUTE_SUMMARY)

# COMMAND ----------

bucket = (
    F.when(F.col("days_to_departure") <= 1, F.struct(F.lit("0-1 days").alias("name"), F.lit(1).alias("o")))
    .when(F.col("days_to_departure") <= 7, F.struct(F.lit("2-7 days").alias("name"), F.lit(2).alias("o")))
    .when(F.col("days_to_departure") <= 21, F.struct(F.lit("8-21 days").alias("name"), F.lit(3).alias("o")))
    .when(F.col("days_to_departure") <= 45, F.struct(F.lit("22-45 days").alias("name"), F.lit(4).alias("o")))
    .otherwise(F.struct(F.lit("46+ days").alias("name"), F.lit(5).alias("o")))
)

lead_time = (
    quotes.withColumn("b", bucket)
    .groupBy("route", F.col("b.name").alias("lead_time_bucket"), F.col("b.o").alias("bucket_order"), "provider")
    .agg(
        F.round(F.avg("fare_inr"), 2).alias("avg_fare_inr"),
        F.round(F.percentile_approx("fare_inr", 0.5), 2).alias("median_fare_inr"),
        F.count("*").cast("int").alias("quotes"),
    )
    .withColumn("refreshed_at", F.current_timestamp())
    .select("route", "lead_time_bucket", "bucket_order", "avg_fare_inr", "median_fare_inr",
            "quotes", "provider", "refreshed_at")
)
lead_time.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(config.PRICE_BY_LEAD_TIME)

# COMMAND ----------

write_audit(
    config.PIPELINE_RUNS, run_id, "aggregate_flight_prices", "SUCCESS",
    rows_in=quotes.count(), rows_out=route_daily.count(), started_at=started_at,
)
display(spark.table(config.ROUTE_DAILY_PRICES).orderBy(F.desc("quote_date"), "route").limit(50))
