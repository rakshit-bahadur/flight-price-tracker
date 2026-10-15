"""Row-level data quality rules for the silver layer.

Each rule returns a reason string when a record fails, or None when it passes.
Failed records go to the quarantine table with their reasons instead of being
dropped silently.
"""

from __future__ import annotations

import re
from datetime import date
from typing import Any, Callable

from . import config

IATA = re.compile(r"^[A-Z]{3}$")
FLIGHT_NUMBER = re.compile(r"^[A-Z0-9]{2}\d{1,4}[A-Z]?$")

Rule = Callable[[dict[str, Any]], "str | None"]


def _required(*fields: str) -> Rule:
    def check(r: dict[str, Any]) -> str | None:
        missing = [f for f in fields if r.get(f) in (None, "")]
        return f"missing {', '.join(missing)}" if missing else None

    return check


def _airports(r: dict[str, Any]) -> str | None:
    if not IATA.match(r.get("origin") or "") or not IATA.match(r.get("destination") or ""):
        return "invalid airport code"
    if r["origin"] == r["destination"]:
        return "origin equals destination"
    return None


def _flight_number(r: dict[str, Any]) -> str | None:
    return None if FLIGHT_NUMBER.match(r.get("flight_number") or "") else "invalid flight number"


def _fare_range(r: dict[str, Any]) -> str | None:
    fare = r.get("fare_inr")
    if not isinstance(fare, (int, float)):
        return "fare is not numeric"
    if not config.MIN_VALID_FARE_INR <= fare <= config.MAX_VALID_FARE_INR:
        return f"fare {fare} outside {config.MIN_VALID_FARE_INR}-{config.MAX_VALID_FARE_INR}"
    return None


def _departure_date(r: dict[str, Any]) -> str | None:
    try:
        date.fromisoformat(r["departure_date"])
    except (KeyError, TypeError, ValueError):
        return "invalid departure_date"
    return None


FLIGHT_RULES: list[Rule] = [
    _required("flight_number", "origin", "destination", "scheduled_departure_utc"),
    _airports,
    _flight_number,
]

FARE_RULES: list[Rule] = [
    _required("flight_number", "origin", "destination", "departure_date", "fare_inr", "quoted_at_utc"),
    _airports,
    _flight_number,
    _fare_range,
    _departure_date,
]


def validate(records: list[dict[str, Any]], rules: list[Rule]) -> tuple[list[dict], list[dict]]:
    """Split records into (valid, rejected). Rejected records carry dq_reasons."""
    valid, rejected = [], []
    for r in records:
        reasons = [msg for rule in rules if (msg := rule(r))]
        if reasons:
            rejected.append({**r, "dq_reasons": "; ".join(reasons)})
        else:
            valid.append(r)
    return valid, rejected
