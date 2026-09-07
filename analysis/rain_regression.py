"""What rain costs a rider, per line, with the interval attached.

    python -m analysis.rain_regression

Joins hourly Central Park precipitation to `fct_excess_wait` and fits, for each
route:

    excess_wait_minutes ~ precip_mm + C(hour_of_day)

with HC3 standard errors, and reports the coefficient on `precip_mm` -- extra
minutes of excess wait per millimetre of rain in that hour -- next to its 95%
confidence interval and its sample size.

## Why the controls are there

Hour of day is in the model because both sides of the question move with it.
Service is worse at some hours regardless of weather, and rain is not evenly
distributed across the day either. Without the control, an afternoon
thunderstorm season would show up as "rain makes trains late" when what it
actually shows is "afternoons".

HC3 rather than the default standard errors because the residuals are
heteroskedastic by construction: excess wait is a variance, its own variability
grows with its level, and assuming otherwise would report intervals narrower
than the data supports. Narrow intervals are how a null result gets published
as a finding.

## Why this will usually refuse to answer

It reports a coefficient only when the data can support one, and says which
test it failed when it cannot:

  * fewer than `MIN_OBSERVATIONS` route-hours
  * fewer than `MIN_WET_HOURS` hours with any rain in them
  * no variation in the regressor at all

That is the entire point of the script. A pipeline that has been running for an
afternoon has seen no rain, and fitting a slope through a regressor that is
zero everywhere produces a number, an interval, and no information. The failure
mode this guards against is not a crash -- it is a plausible-looking table.

Running it against weeks of history is what makes it say something. Running it
against a morning is what makes it say so.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd

from analysis.weather import fetch, to_rows

ROOT = Path(__file__).resolve().parent.parent
WAREHOUSE = ROOT / "data" / "warehouse.duckdb"

# An excess wait computed from a couple of gaps is arithmetic, not a
# measurement. Hours thinner than this are dropped before anything is fitted.
MIN_HEADWAYS_PER_HOUR = 3
# Below these the script declines to report rather than reporting noise.
MIN_OBSERVATIONS = 60
MIN_WET_HOURS = 8


def load_excess_wait(warehouse: Path) -> pd.DataFrame:
    if not warehouse.exists():
        raise SystemExit(
            f"No warehouse at {warehouse}.\n"
            "Run the poller, then `dbt build --profiles-dir .` in transform/."
        )
    con = duckdb.connect(str(warehouse), read_only=True)
    try:
        return con.execute(
            f"""
            select
                route_id,
                stop_id,
                service_hour,
                hour_of_day,
                day_of_week,
                headways,
                mean_headway_minutes,
                excess_wait_minutes
            from fct_excess_wait
            where headways >= {MIN_HEADWAYS_PER_HOUR}
            """
        ).df()
    finally:
        con.close()


def join_weather(waits: pd.DataFrame, days: int, refresh: bool) -> pd.DataFrame:
    weather = pd.DataFrame(to_rows(fetch(days=days, refresh=refresh)))
    weather["service_hour"] = pd.to_datetime(weather["service_hour"])
    waits = waits.copy()
    # Both sides are naive New York wall-clock hours; see weather.py.
    waits["service_hour"] = pd.to_datetime(waits["service_hour"]).dt.floor("h")
    return waits.merge(weather, on="service_hour", how="inner")


def fit_route(frame: pd.DataFrame) -> dict:
    """One route. Returns either a fitted result or the reason there is none."""
    import statsmodels.formula.api as smf

    observations = len(frame)
    wet_hours = int(frame["is_wet"].sum())

    if observations < MIN_OBSERVATIONS:
        return {"status": "insufficient", "why": f"{observations} route-hours", "n": observations}
    if wet_hours < MIN_WET_HOURS:
        return {"status": "insufficient", "why": f"{wet_hours} wet hours", "n": observations}
    if frame["precip_mm"].nunique() < 2:
        return {"status": "insufficient", "why": "no variation in rainfall", "n": observations}

    # Hour of day only enters as a control where there is more than one of
    # them; with a single hour the dummy set is collinear with the intercept.
    formula = "excess_wait_minutes ~ precip_mm"
    if frame["hour_of_day"].nunique() > 1:
        formula += " + C(hour_of_day)"

    model = smf.ols(formula, data=frame).fit(cov_type="HC3")
    low, high = model.conf_int().loc["precip_mm"]
    return {
        "status": "fitted",
        "minutes_per_mm": float(model.params["precip_mm"]),
        "ci_low": float(low),
        "ci_high": float(high),
        "p_value": float(model.pvalues["precip_mm"]),
        "n": observations,
        "wet_hours": wet_hours,
        "r_squared": float(model.rsquared),
        # An interval that contains zero is a result. It is reported as one.
        "straddles_zero": bool(low <= 0 <= high),
    }


def report(results: dict[str, dict]) -> None:
    fitted = {k: v for k, v in results.items() if v["status"] == "fitted"}
    refused = {k: v for k, v in results.items() if v["status"] != "fitted"}

    if fitted:
        print("\n  RAIN AGAINST EXCESS WAIT, BY ROUTE")
        print("  extra minutes of excess wait per mm of rain in the hour\n")
        print(f"  {'route':>6}  {'coef':>8}  {'95% interval':>20}  {'n':>6}  {'wet':>4}")
        print(f"  {'-'*6}  {'-'*8}  {'-'*20}  {'-'*6}  {'-'*4}")
        for route, r in sorted(fitted.items()):
            interval = f"[{r['ci_low']:+.3f}, {r['ci_high']:+.3f}]"
            mark = "" if r["straddles_zero"] else "  *"
            print(
                f"  {route:>6}  {r['minutes_per_mm']:+8.3f}  {interval:>20}  "
                f"{r['n']:>6,}  {r['wet_hours']:>4}{mark}"
            )
        straddling = sum(1 for r in fitted.values() if r["straddles_zero"])
        print(
            f"\n  {straddling} of {len(fitted)} intervals contain zero. "
            "An interval containing zero is a result, not a missing one:\n"
            "  it says this data cannot distinguish the effect from nothing."
        )

    if refused:
        print("\n  NOT REPORTED")
        print("  These routes do not have enough data to support a coefficient.\n")
        for route, r in sorted(refused.items()):
            print(f"  {route:>6}  {r['why']}")
        print(
            "\n  Nothing is printed for these on purpose. A slope fitted through\n"
            "  a regressor that barely varies is a number with no information in\n"
            "  it, and it would look exactly like a finding."
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--warehouse", type=Path, default=WAREHOUSE)
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args(argv)

    waits = load_excess_wait(args.warehouse)
    print(f"{len(waits):,} route-hours with >= {MIN_HEADWAYS_PER_HOUR} headways")

    joined = join_weather(waits, days=args.days, refresh=args.refresh)
    print(f"{len(joined):,} of them joined to an hour of weather")
    if joined.empty:
        print("\nNothing to fit. The warehouse and the weather cache do not overlap.")
        return 0

    wet = int(joined["is_wet"].sum())
    print(f"{wet:,} of those hours had rain in them\n")

    results = {
        str(route): fit_route(frame)
        for route, frame in joined.groupby("route_id")
    }
    report(results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
