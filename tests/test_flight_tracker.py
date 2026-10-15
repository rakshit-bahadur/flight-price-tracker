import json
from datetime import datetime

import pytest

from flight_tracker import config
from flight_tracker.aerodatabox import AeroDataBoxClient, normalise_flight_number, parse_departures
from flight_tracker.alerts import detect_price_drops, format_alert_email
from flight_tracker.fares import (
    JsonFileFareProvider,
    SimulatedFareProvider,
    days_to_departure,
    lead_time_multiplier,
)
from flight_tracker.quality import FARE_RULES, FLIGHT_RULES, validate

QUOTED_AT = datetime(2026, 10, 15, 6, 0, 0)

DEPARTURES = {
    "departures": [
        {
            "departure": {
                "scheduledTime": {"utc": "2026-10-20 03:30Z", "local": "2026-10-20 09:00+05:30"},
                "revisedTime": {"utc": "2026-10-20 03:50Z"},
                "terminal": "3",
            },
            "arrival": {
                "airport": {"iata": "BOM", "name": "Mumbai"},
                "scheduledTime": {"utc": "2026-10-20 05:45Z"},
            },
            "number": "6E 2134",
            "status": "Expected",
            "aircraft": {"model": "Airbus A320 NEO"},
            "airline": {"name": "IndiGo", "iata": "6E"},
        },
        {
            "movement": {"airport": {"iata": "DXB"}, "scheduledTime": {"utc": "2026-10-20 08:10Z"}},
            "number": "AI 995",
            "status": "Expected",
            "airline": {"name": "Air India", "iata": "AI"},
        },
        {"number": "XX 1", "arrival": {}},
        {"arrival": {"airport": {"iata": "BLR"}}},
    ]
}


def good_quote(**overrides):
    q = {
        "flight_number": "6E2134",
        "origin": "DEL",
        "destination": "BOM",
        "departure_date": "2026-10-20",
        "fare_inr": 5400.0,
        "quoted_at_utc": "2026-10-15T06:00:00Z",
    }
    q.update(overrides)
    return q


# AeroDataBox parsing

def test_parse_departures_handles_both_response_shapes_and_skips_incomplete():
    flights = parse_departures(DEPARTURES, "del")
    assert [f["flight_number"] for f in flights] == ["6E2134", "AI995"]
    first = flights[0]
    assert first["origin"] == "DEL" and first["destination"] == "BOM"
    assert first["scheduled_departure_utc"] == "2026-10-20T03:30:00Z"
    assert first["revised_departure_utc"] == "2026-10-20T03:50:00Z"
    assert first["scheduled_arrival_utc"] == "2026-10-20T05:45:00Z"
    assert flights[1]["destination"] == "DXB"
    assert flights[1]["scheduled_departure_utc"] is None


def test_normalise_flight_number():
    assert normalise_flight_number("6e 2134") == "6E2134"
    assert normalise_flight_number(None) is None


def test_client_requires_key_and_valid_window():
    with pytest.raises(ValueError):
        AeroDataBoxClient("")
    client = AeroDataBoxClient("dummy")
    with pytest.raises(ValueError):
        client.departures("DEL", QUOTED_AT, hours=13)


# Fares

def test_simulated_fares_are_labelled_deterministic_and_skip_unknown_routes():
    flights = parse_departures(DEPARTURES, "DEL")
    flights[1]["scheduled_departure_utc"] = "2026-10-20T08:10:00Z"
    unknown = {**flights[0], "destination": "XYZ"}
    provider = SimulatedFareProvider()
    first = provider.quotes(flights + [unknown], QUOTED_AT)
    second = provider.quotes(flights + [unknown], QUOTED_AT)
    assert first == second
    assert len(first) == 2
    assert all(q["provider"] == "simulated" for q in first)
    assert all(q["fare_inr"] % 10 == 0 for q in first)
    international = next(q for q in first if q["destination"] == "DXB")
    domestic = next(q for q in first if q["destination"] == "BOM")
    assert international["fare_inr"] > domestic["fare_inr"]


def test_simulated_fares_skip_flights_that_already_left():
    flights = parse_departures(DEPARTURES, "DEL")[:1]
    assert SimulatedFareProvider().quotes(flights, datetime(2026, 10, 25)) == []


def test_lead_time_multiplier_is_highest_close_to_departure():
    assert lead_time_multiplier(0) > lead_time_multiplier(5) > lead_time_multiplier(14) > lead_time_multiplier(30)


def test_json_file_provider_converts_currency_and_drops_unknown(tmp_path):
    (tmp_path / "export.json").write_text(
        json.dumps(
            [
                {"flight_number": "ek 511", "origin": "del", "destination": "dxb",
                 "departure_date": "2026-11-01", "fare": 200, "currency": "USD"},
                {"flight_number": "6E1", "origin": "DEL", "destination": "BOM",
                 "departure_date": "2026-11-01", "fare": 10, "currency": "JPY"},
            ]
        )
    )
    quotes = JsonFileFareProvider(tmp_path, inr_rates={"USD": 84.0}).quotes([], QUOTED_AT)
    assert len(quotes) == 1
    assert quotes[0]["fare_inr"] == 16800.0
    assert quotes[0]["flight_number"] == "EK511"
    assert quotes[0]["origin"] == "DEL"


def test_days_to_departure():
    assert days_to_departure("2026-10-20", "2026-10-15T06:00:00Z") == 5


# Data quality

def test_fare_rules_accept_good_and_explain_bad():
    bad = [
        good_quote(fare_inr=50),
        good_quote(origin="DEL", destination="DEL"),
        good_quote(flight_number="??"),
        good_quote(departure_date="20-10-2026"),
        good_quote(fare_inr=None),
    ]
    valid, rejected = validate([good_quote()] + bad, FARE_RULES)
    assert len(valid) == 1
    assert len(rejected) == 5
    reasons = [r["dq_reasons"] for r in rejected]
    assert "outside" in reasons[0]
    assert "origin equals destination" in reasons[1]
    assert "invalid flight number" in reasons[2]
    assert "invalid departure_date" in reasons[3]
    assert "missing fare_inr" in reasons[4]


def test_flight_rules_require_departure_time():
    flights = parse_departures(DEPARTURES, "DEL")
    valid, rejected = validate(flights, FLIGHT_RULES)
    assert [f["flight_number"] for f in valid] == ["6E2134"]
    assert "scheduled_departure_utc" in rejected[0]["dq_reasons"]


# Alerts

def _daily(route, dep, prices):
    return [
        {"route": route, "departure_date": dep, "quote_date": f"2026-10-{10 + i:02d}", "min_fare_inr": p}
        for i, p in enumerate(prices)
    ]


def test_detect_price_drops_flags_only_large_drops():
    rows = _daily("DEL-BOM", "2026-11-01", [6000, 6100, 5900, 4800])  # -20%
    rows += _daily("BOM-DEL", "2026-11-01", [6000, 6000, 5700])  # -5%
    rows += _daily("DEL-DXB", "2026-11-01", [15000, 9000])  # not enough history
    alerts = detect_price_drops(rows)
    assert len(alerts) == 1
    assert alerts[0]["route"] == "DEL-BOM"
    assert alerts[0]["change_pct"] == -20.0
    assert alerts[0]["quote_date"] == "2026-10-13"


def test_detect_price_drops_uses_only_recent_baseline():
    prices = [20000] * 10 + [6000] * 7 + [5000]
    alerts = detect_price_drops(_daily("DEL-BLR", "2026-12-01", prices), drop_pct=15, baseline_days=7)
    assert alerts[0]["baseline_avg_fare_inr"] == 6000
    assert alerts[0]["change_pct"] == -16.7


def test_alert_email_lists_every_alert():
    alerts = detect_price_drops(_daily("DEL-BOM", "2026-11-01", [6000, 6000, 4500]))
    subject, body = format_alert_email(alerts)
    assert "1 price drop" in subject
    assert "DEL-BOM" in body and "4,500" in body


def test_config_routes_are_unique_and_tables_are_qualified():
    codes = [r.code for r in config.ROUTES]
    assert len(codes) == len(set(codes))
    assert config.FARE_QUOTES == "workspace.flight_tracker.silver_fare_quotes"
