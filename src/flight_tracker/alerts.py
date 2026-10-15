"""Price-drop alert logic and email formatting."""

from __future__ import annotations

import smtplib
from email.message import EmailMessage
from typing import Any

from . import config


def detect_price_drops(
    daily_rows: list[dict[str, Any]],
    drop_pct: float = config.ALERT_DROP_PCT,
    baseline_days: int = config.ALERT_BASELINE_DAYS,
) -> list[dict[str, Any]]:
    """Find route/departure-date pairs whose latest lowest fare fell sharply.

    daily_rows: one row per (route, departure_date, quote_date) with min_fare_inr.
    For each route and departure date, the latest quote_date is compared with the
    average of the previous `baseline_days` quote dates. An alert is raised when
    the drop is at least `drop_pct` percent.
    """
    by_key: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in daily_rows:
        by_key.setdefault((row["route"], row["departure_date"]), []).append(row)

    alerts = []
    for (route, departure_date), rows in by_key.items():
        rows = sorted(rows, key=lambda r: r["quote_date"])
        latest, history = rows[-1], rows[:-1][-baseline_days:]
        if len(history) < 2:
            continue
        baseline = sum(r["min_fare_inr"] for r in history) / len(history)
        if baseline <= 0:
            continue
        change_pct = (latest["min_fare_inr"] - baseline) / baseline * 100
        if change_pct <= -drop_pct:
            alerts.append(
                {
                    "route": route,
                    "departure_date": departure_date,
                    "quote_date": latest["quote_date"],
                    "current_min_fare_inr": float(latest["min_fare_inr"]),
                    "baseline_avg_fare_inr": round(baseline, 2),
                    "change_pct": round(change_pct, 1),
                }
            )
    return sorted(alerts, key=lambda a: a["change_pct"])


def format_alert_email(alerts: list[dict[str, Any]]) -> tuple[str, str]:
    subject = f"FlightPriceTracker: {len(alerts)} price drop(s) found"
    lines = ["Fares dropped on these routes compared with the recent average:", ""]
    for a in alerts:
        lines.append(
            f"{a['route']}  departing {a['departure_date']}: "
            f"INR {a['current_min_fare_inr']:,.0f} (avg INR {a['baseline_avg_fare_inr']:,.0f}, {a['change_pct']}%)"
        )
    lines += ["", "Sent by the FlightPriceTracker Databricks job."]
    return subject, "\n".join(lines)


def send_email(subject: str, body: str, sender: str, password: str, recipients: list[str],
               host: str = "smtp.gmail.com", port: int = 465) -> None:
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)
    with smtplib.SMTP_SSL(host, port, timeout=30) as smtp:
        smtp.login(sender, password)
        smtp.send_message(msg)
