"""Hourly rainfall over Central Park, cached to disk.

Open-Meteo, which is free, needs no key, and publishes hourly precipitation for
an arbitrary point. The point is Central Park (40.7823, -73.9654) because that
is where the National Weather Service's own New York City record is taken, so
the series here lines up with the one every other account of New York weather
uses.

    python -m analysis.weather --days 3

## Why it is cached

The regression is re-run constantly while it is being written, and a script
that re-fetches an external API on every run is a script that is slow, that
fails when the wifi does, and that quietly produces different numbers on
Tuesday than it did on Monday because the provider revised its history. The
response is written to `data/weather/` and read from there unless `--refresh`
is passed. Committed for the same reason the GTFS seeds are: a repository that
cannot be rebuilt without three external services being up cannot be checked.

## One thing worth knowing about the data

Precipitation is reported per hour in millimetres, and it is overwhelmingly
zero. New York gets rain on roughly one hour in ten, so a day of observations
is a regressor that is almost entirely a single value -- which is not a
problem with the fetch, it is the reason the regression needs weeks rather
than hours, and `rain_regression.py` refuses rather than pretending otherwise.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

import requests

# Central Park, where the NWS takes the New York City record.
LATITUDE = 40.7823
LONGITUDE = -73.9654

ROOT = Path(__file__).resolve().parent.parent
CACHE = ROOT / "data" / "weather"
ENDPOINT = "https://api.open-meteo.com/v1/forecast"


def cache_path(days: int) -> Path:
    return CACHE / f"central-park-hourly-{days}d.json"


def fetch(days: int = 3, refresh: bool = False) -> dict:
    """Hourly precipitation for the last `days` days, plus today so far."""
    path = cache_path(days)
    if path.exists() and not refresh:
        return json.loads(path.read_text(encoding="utf-8"))

    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "hourly": "precipitation,temperature_2m,wind_speed_10m",
        "past_days": days,
        "forecast_days": 1,
        # Asked for in New York time, so the hour labels join straight to
        # `service_hour` without a conversion that could be got wrong twice.
        "timezone": "America/New_York",
    }
    response = requests.get(ENDPOINT, params=params, timeout=60)
    response.raise_for_status()
    payload = response.json()
    payload["_fetched_at"] = datetime.now().astimezone().isoformat()
    payload["_source"] = response.url

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=1), encoding="utf-8")
    return payload


def to_rows(payload: dict) -> list[dict]:
    """The API's column-oriented response, as rows ready to join on the hour."""
    hourly = payload["hourly"]
    times = hourly["time"]
    return [
        {
            # "2026-09-07T04:00" -- a naive local hour, which is exactly what
            # `service_hour` is in the warehouse.
            "service_hour": times[i],
            "precip_mm": hourly["precipitation"][i],
            "temperature_c": hourly["temperature_2m"][i],
            "wind_kmh": hourly["wind_speed_10m"][i],
            "is_wet": bool((hourly["precipitation"][i] or 0) > 0),
        }
        for i in range(len(times))
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args(argv)

    payload = fetch(days=args.days, refresh=args.refresh)
    rows = to_rows(payload)
    wet = sum(1 for row in rows if row["is_wet"])
    total_mm = sum(row["precip_mm"] or 0 for row in rows)

    print(f"{len(rows)} hourly observations, Central Park")
    print(f"  {wet} wet hours ({wet / max(len(rows), 1):.0%})")
    print(f"  {total_mm:.1f} mm total")
    print(f"  cached at {cache_path(args.days).relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
