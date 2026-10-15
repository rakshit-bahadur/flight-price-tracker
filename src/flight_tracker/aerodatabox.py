"""Client and parser for AeroDataBox airport departures (via RapidAPI).

AeroDataBox returns flight schedules and status (airline, flight number,
times, aircraft). It does not return fares; fares come from a fare provider
(see fares.py).
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta
from typing import Any

import requests

HOST = "aerodatabox.p.rapidapi.com"
BASE_URL = f"https://{HOST}/flights/airports/iata"


class AeroDataBoxClient:
    def __init__(self, api_key: str, session: requests.Session | None = None, max_retries: int = 3):
        if not api_key:
            raise ValueError("AeroDataBox API key is empty; set it in the Databricks secret scope")
        self._session = session or requests.Session()
        self._session.headers.update({"X-RapidAPI-Key": api_key, "X-RapidAPI-Host": HOST})
        self._max_retries = max_retries

    def departures(self, airport_iata: str, start_local: datetime, hours: int = 12) -> dict[str, Any]:
        """Departures from an airport in a local-time window of up to 12 hours."""
        if not 1 <= hours <= 12:
            raise ValueError("AeroDataBox windows must be between 1 and 12 hours")
        end_local = start_local + timedelta(hours=hours) - timedelta(minutes=1)
        url = f"{BASE_URL}/{airport_iata}/{start_local:%Y-%m-%dT%H:%M}/{end_local:%Y-%m-%dT%H:%M}"
        params = {
            "direction": "Departure",
            "withLeg": "true",
            "withCancelled": "true",
            "withCodeshared": "false",
            "withCargo": "false",
            "withPrivate": "false",
        }
        for attempt in range(1, self._max_retries + 1):
            resp = self._session.get(url, params=params, timeout=30)
            if resp.status_code == 429 and attempt < self._max_retries:
                time.sleep(2 ** attempt * 5)
                continue
            if resp.status_code == 204:
                return {"departures": []}
            resp.raise_for_status()
            return resp.json()
        raise RuntimeError(f"AeroDataBox rate limit not cleared for {airport_iata}")


def _parse_time(block: dict | None, key: str = "scheduledTime") -> str | None:
    """Return the UTC timestamp as ISO text ('2026-10-15T04:30:00Z'), or None."""
    if not block:
        return None
    value = (block.get(key) or {}).get("utc")
    if not value:
        return None
    # AeroDataBox formats UTC as '2026-10-15 04:30Z'.
    return datetime.strptime(value.replace("Z", ""), "%Y-%m-%d %H:%M").strftime("%Y-%m-%dT%H:%M:00Z")


def normalise_flight_number(number: str | None) -> str | None:
    if not number:
        return None
    return number.replace(" ", "").upper()


def parse_departures(payload: dict[str, Any], origin_iata: str) -> list[dict[str, Any]]:
    """Flatten a departures response into one record per flight.

    With withLeg=true each item has 'departure' and 'arrival' blocks. Older
    responses use 'movement' for the destination; both shapes are handled.
    Items without a flight number or destination are skipped.
    """
    records = []
    for item in payload.get("departures", []):
        arrival = item.get("arrival") or item.get("movement") or {}
        departure = item.get("departure") or {}
        destination = (arrival.get("airport") or {}).get("iata")
        flight_number = normalise_flight_number(item.get("number"))
        if not destination or not flight_number:
            continue
        sched_dep = _parse_time(departure) if departure else None
        airline = item.get("airline") or {}
        records.append(
            {
                "flight_number": flight_number,
                "airline_iata": airline.get("iata"),
                "airline_name": airline.get("name"),
                "origin": origin_iata.upper(),
                "destination": destination.upper(),
                "scheduled_departure_utc": sched_dep,
                "revised_departure_utc": _parse_time(departure, "revisedTime") if departure else None,
                "scheduled_arrival_utc": _parse_time(arrival),
                "status": item.get("status"),
                "aircraft_model": (item.get("aircraft") or {}).get("model"),
                "terminal": departure.get("terminal"),
            }
        )
    return records
