"""Fare providers.

Public, free flight fare APIs are rare (Amadeus Self-Service, the common free
choice, shut down in July 2026). The pipeline therefore reads fares through a
small provider interface:

- JsonFileFareProvider: reads fare quotes exported from any fare API or GDS
  feed in a simple JSON format. Use this for real data.
- SimulatedFareProvider: generates realistic fares for the real flights that
  AeroDataBox returns. Every quote it produces is marked provider='simulated'
  so it can never be mistaken for a real market price.
"""

from __future__ import annotations

import hashlib
import json
import math
import random
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Protocol

# Typical one-way economy base fares (INR) used by the simulator.
ROUTE_BASE_FARE_INR = {
    "DEL-BOM": 5200,
    "BOM-DEL": 5300,
    "DEL-BLR": 6100,
    "BLR-DEL": 6000,
    "BOM-BLR": 3900,
    "DEL-CCU": 5600,
    "DEL-DXB": 14500,
    "BOM-SIN": 17800,
}

# Rough relative price level by airline (1.0 = market average).
AIRLINE_FACTOR = {"AI": 1.12, "UK": 1.18, "6E": 0.96, "QP": 0.92, "IX": 0.9, "SG": 0.94, "EK": 1.35, "SQ": 1.4}


class FareProvider(Protocol):
    name: str

    def quotes(self, flights: Iterable[dict[str, Any]], quoted_at: datetime) -> list[dict[str, Any]]:
        ...


def _stable_seed(*parts: Any) -> int:
    digest = hashlib.sha256("|".join(str(p) for p in parts).encode()).hexdigest()
    return int(digest[:12], 16)


def lead_time_multiplier(days_to_departure: int) -> float:
    """Fares rise steeply in the last two weeks and are lowest 3-6 weeks out."""
    d = max(days_to_departure, 0)
    if d <= 1:
        return 1.85
    if d <= 7:
        return 1.45 - 0.04 * d
    if d <= 21:
        return 1.18 - 0.01 * (d - 7)
    if d <= 45:
        return 1.0
    return 1.05


class SimulatedFareProvider:
    name = "simulated"

    def __init__(self, seed: int = 7):
        self._seed = seed

    def quotes(self, flights: Iterable[dict[str, Any]], quoted_at: datetime) -> list[dict[str, Any]]:
        out = []
        for f in flights:
            route = f"{f['origin']}-{f['destination']}"
            base = ROUTE_BASE_FARE_INR.get(route)
            dep = f.get("scheduled_departure_utc")
            if base is None or not dep:
                continue
            dep_dt = datetime.strptime(dep, "%Y-%m-%dT%H:%M:%SZ")
            days = (dep_dt.date() - quoted_at.date()).days
            if days < 0:
                continue
            rng = random.Random(_stable_seed(self._seed, f["flight_number"], dep, quoted_at.strftime("%Y-%m-%d %H")))
            hour = dep_dt.hour + 5.5  # IST
            peak = 1.08 if 6 <= hour % 24 <= 10 or 17 <= hour % 24 <= 21 else 0.97
            weekly = 1 + 0.06 * math.sin(2 * math.pi * dep_dt.weekday() / 7)
            fare = (
                base
                * AIRLINE_FACTOR.get(f.get("airline_iata") or "", 1.0)
                * lead_time_multiplier(days)
                * peak
                * weekly
                * rng.uniform(0.9, 1.12)
            )
            out.append(
                {
                    "flight_number": f["flight_number"],
                    "origin": f["origin"],
                    "destination": f["destination"],
                    "departure_date": dep_dt.date().isoformat(),
                    "fare_inr": round(fare / 10) * 10,
                    "currency": "INR",
                    "cabin": "ECONOMY",
                    "seats_left": rng.randint(1, 9),
                    "provider": self.name,
                    "quoted_at_utc": quoted_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                }
            )
        return out


class JsonFileFareProvider:
    """Reads fare quotes from JSON files.

    Expected format: a list of objects with flight_number, origin, destination,
    departure_date (YYYY-MM-DD), fare (number), currency, and optional cabin
    and seats_left.
    """

    name = "json_file"

    def __init__(self, path: str | Path, inr_rates: dict[str, float] | None = None):
        self._path = Path(path)
        self._rates = {"INR": 1.0, **(inr_rates or {})}

    def quotes(self, flights: Iterable[dict[str, Any]], quoted_at: datetime) -> list[dict[str, Any]]:
        files = sorted(self._path.glob("*.json")) if self._path.is_dir() else [self._path]
        out = []
        for file in files:
            for q in json.loads(file.read_text(encoding="utf-8")):
                currency = (q.get("currency") or "INR").upper()
                rate = self._rates.get(currency)
                if rate is None:
                    continue
                out.append(
                    {
                        "flight_number": str(q["flight_number"]).replace(" ", "").upper(),
                        "origin": q["origin"].upper(),
                        "destination": q["destination"].upper(),
                        "departure_date": q["departure_date"],
                        "fare_inr": round(float(q["fare"]) * rate, 2),
                        "currency": "INR",
                        "cabin": (q.get("cabin") or "ECONOMY").upper(),
                        "seats_left": q.get("seats_left"),
                        "provider": self.name,
                        "quoted_at_utc": q.get("quoted_at_utc") or quoted_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    }
                )
        return out


def days_to_departure(departure_date: str, quoted_at_utc: str) -> int:
    dep = date.fromisoformat(departure_date)
    quoted = datetime.strptime(quoted_at_utc, "%Y-%m-%dT%H:%M:%SZ").date()
    return (dep - quoted).days
