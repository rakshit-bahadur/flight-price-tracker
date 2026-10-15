# FlightPriceTracker

A batch data pipeline on Databricks that tracks flights and fares on 8 busy routes out of Indian hubs. It runs every 3 hours, follows the Medallion (bronze, silver, gold) design on Delta Lake, quarantines bad records, keeps an audit log of every task, and emails an alert when a fare drops sharply.

## Routes tracked

DEL-BOM, BOM-DEL, DEL-BLR, BLR-DEL, BOM-BLR, DEL-CCU, DEL-DXB, BOM-SIN. Edit `src/flight_tracker/config.py` to change them.

## Architecture

```
AeroDataBox API (RapidAPI)          Fare provider
real flight schedules and status    simulated or JSON export
        |                                  |
        v                                  v
bronze_flight_schedules_raw         bronze_fare_quotes_raw          raw payloads, stored as received
        \                                  /
         \                                /
          v                              v
     transform_flight_data: parse, data quality rules, MERGE
          |                    |                    |
          v                    v                    v
   silver_flights     silver_fare_quotes     silver_quarantine     clean, typed, deduplicated
          \                    /
           v                  v
     aggregate_flight_prices
          |                    |                       |
          v                    v                       v
gold_route_daily_prices  gold_airline_route_summary  gold_price_by_lead_time
          |
          v
     send_price_alerts --> gold_price_alerts + email

audit_pipeline_runs: one row per task per run (rows in, rows out, rows rejected, status)
```

The Databricks Job runs five notebook tasks in order. The first task creates a `run_id` and passes it to the others with task values, so every task works on exactly the same batch.

## About the fare data

AeroDataBox gives real flight schedules, status and aircraft, but not fares. Free public fare APIs are now rare: Amadeus Self-Service, the usual free option, shut down in July 2026. So fares come in through a small provider interface in `src/flight_tracker/fares.py`:

- **`simulated`** (default): realistic fares for the real flights returned by AeroDataBox. Fares depend on route, airline, time of day, day of week and days to departure. Every row is stored with `provider = 'simulated'`, so simulated prices are never mixed up with real ones.
- **`json_file`**: reads real fare quotes exported from any fare API or GDS feed. Put JSON files in a Unity Catalog volume and set the `fare_provider` and `fare_path` parameters.

```json
[{"flight_number": "6E 2134", "origin": "DEL", "destination": "BOM",
  "departure_date": "2026-11-01", "fare": 5450, "currency": "INR", "cabin": "ECONOMY"}]
```

Adding a new live source means writing one class with a `quotes()` method.

## Key design choices

- **Bronze keeps raw payloads.** Any run can be reprocessed from bronze without calling the APIs again.
- **Idempotent silver.** Flights and fare quotes are written with Delta `MERGE` on hashed business keys, so rerunning a task never creates duplicates.
- **Quarantine, not silent drops.** Records that fail a rule (bad airport code, fare out of range, missing fields) go to `silver_quarantine` with the reason.
- **Logic outside notebooks.** Parsing, data quality rules, fare providers and alert logic live in the `flight_tracker` Python package with no Spark dependency, so they are unit tested with pytest. Notebooks only read and write tables.
- **No secrets in code.** API keys and SMTP credentials are read from a Databricks secret scope.

## Project structure

```
src/flight_tracker/
    config.py           routes, table names, thresholds
    aerodatabox.py      API client with retry on rate limits, response parser
    fares.py            fare provider interface: simulated and JSON file
    quality.py          data quality rules and validation
    alerts.py           price-drop detection and email
notebooks/
    setup_tables.py             creates schema and all Delta tables
    ingest_flight_schedules.py  bronze: AeroDataBox departures
    ingest_fare_quotes.py       bronze: fare quotes
    transform_flight_data.py    silver: parse, validate, quarantine, MERGE
    aggregate_flight_prices.py  gold: daily prices, airline summary, lead-time curve
    send_price_alerts.py        price-drop alerts and email
    _audit.py                   shared audit helper
jobs/flight_price_tracker_job.json   Databricks Job definition (every 3 hours)
tests/                               pytest unit tests
```

## Setup on Databricks

1. **Add the repo.** In your workspace, go to Workspace > Create > Git folder and paste this repo's URL.
2. **Get an AeroDataBox key.** Subscribe to the free plan of AeroDataBox on RapidAPI and copy your key.
3. **Create the secret scope.** Using the Databricks CLI:
   ```bash
   databricks secrets create-scope flight_tracker
   databricks secrets put-secret flight_tracker aerodatabox_api_key
   # Optional, for email alerts (Gmail needs an app password, not your normal password)
   databricks secrets put-secret flight_tracker smtp_sender
   databricks secrets put-secret flight_tracker smtp_app_password
   databricks secrets put-secret flight_tracker alert_recipients   # comma-separated emails
   ```
4. **Create the tables.** Run `notebooks/setup_tables` once.
5. **Create the job.** Go to Jobs & Pipelines > Create > Job, switch to the JSON editor, and paste `jobs/flight_price_tracker_job.json`. Or run each notebook by hand in order.

The default catalog and schema are `workspace.flight_tracker`, which works on Databricks Free Edition. Change `CATALOG` and `SCHEMA` in `config.py` for other workspaces.

## Run the tests

```bash
pip install -e ".[dev]"
pytest
```

## Example queries

```sql
-- Cheapest fare per route for the next 30 days, from the latest quote day
SELECT route, departure_date, min_fare_inr, cheapest_flight
FROM workspace.flight_tracker.gold_route_daily_prices
WHERE quote_date = (SELECT max(quote_date) FROM workspace.flight_tracker.gold_route_daily_prices)
  AND departure_date <= date_add(current_date(), 30)
ORDER BY route, departure_date;

-- How fares change as departure gets closer
SELECT route, lead_time_bucket, avg_fare_inr, quotes
FROM workspace.flight_tracker.gold_price_by_lead_time
ORDER BY route, bucket_order;

-- Health of the last run
SELECT task, status, rows_in, rows_out, rows_rejected, message
FROM workspace.flight_tracker.audit_pipeline_runs
WHERE run_id = (SELECT max(run_id) FROM workspace.flight_tracker.audit_pipeline_runs)
ORDER BY started_at;
```

## Tech stack

Databricks (Jobs, Unity Catalog, serverless compute), PySpark, Delta Lake, Python, AeroDataBox API, pytest.

## Author

Rakshit Bahadur, Senior Data Engineer. [GitHub](https://github.com/rakshit-bahadur) · [LinkedIn](https://www.linkedin.com/in/rakshit-bahadur) · [Website](https://rakshit-bahadur.github.io)
