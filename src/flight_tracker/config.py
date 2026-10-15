"""Pipeline configuration: tracked routes, table names and thresholds."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Route:
    origin: str
    destination: str

    @property
    def code(self) -> str:
        return f"{self.origin}-{self.destination}"


# High-traffic domestic and international routes out of Indian hubs.
ROUTES: tuple[Route, ...] = (
    Route("DEL", "BOM"),
    Route("BOM", "DEL"),
    Route("DEL", "BLR"),
    Route("BLR", "DEL"),
    Route("BOM", "BLR"),
    Route("DEL", "CCU"),
    Route("DEL", "DXB"),
    Route("BOM", "SIN"),
)

ORIGIN_AIRPORTS: tuple[str, ...] = tuple(sorted({r.origin for r in ROUTES}))

CATALOG = "workspace"
SCHEMA = "flight_tracker"


def table(name: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{name}"


# Bronze
SCHEDULES_RAW = table("bronze_flight_schedules_raw")
FARES_RAW = table("bronze_fare_quotes_raw")
# Silver
FLIGHTS = table("silver_flights")
FARE_QUOTES = table("silver_fare_quotes")
QUARANTINE = table("silver_quarantine")
# Gold
ROUTE_DAILY_PRICES = table("gold_route_daily_prices")
AIRLINE_ROUTE_SUMMARY = table("gold_airline_route_summary")
PRICE_BY_LEAD_TIME = table("gold_price_by_lead_time")
PRICE_ALERTS = table("gold_price_alerts")
# Audit
PIPELINE_RUNS = table("audit_pipeline_runs")

# Databricks secret scope holding API keys and SMTP credentials.
SECRET_SCOPE = "flight_tracker"

# AeroDataBox allows at most a 12-hour window per departures call.
SCHEDULE_WINDOW_HOURS = 12

# Fares outside this range (INR) are treated as bad data and quarantined.
MIN_VALID_FARE_INR = 500
MAX_VALID_FARE_INR = 500_000

# A price alert fires when today's lowest fare is this far below the trailing average.
ALERT_DROP_PCT = 15.0
ALERT_BASELINE_DAYS = 7
