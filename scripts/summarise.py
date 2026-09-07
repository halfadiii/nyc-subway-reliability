"""What is actually in the warehouse, printed.

    python scripts/summarise.py data/warehouse.duckdb

Run at the end of `make build` and `make demo`, so that building the project
tells you what it built rather than leaving you to go and look. Everything here
is a count or an aggregate straight out of the marts -- there is no
interpretation and nothing is rounded up.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

TABLES = [
    ("stg_stop_time_updates", "raw observations"),
    ("int_inferred_arrivals", "inferred arrivals"),
    ("fct_arrivals", "arrivals (fact)"),
    ("fct_headways", "headways"),
    ("fct_excess_wait", "route-hours"),
    ("dim_stations", "stations"),
    ("dim_routes", "routes"),
]


def main(argv: list[str]) -> int:
    path = Path(argv[1] if len(argv) > 1 else "data/warehouse.duckdb")
    if not path.exists():
        print(f"No warehouse at {path}. Run `make build` (or `make demo`).")
        return 1

    con = duckdb.connect(str(path), read_only=True)
    try:
        print(f"\n  {path}")
        print("  " + "-" * 52)
        for table, label in TABLES:
            try:
                count = con.execute(f"select count(*) from {table}").fetchone()[0]
            except duckdb.CatalogException:
                continue
            print(f"  {label:<22} {count:>12,}")

        span = con.execute(
            """
            select min(observed_at), max(observed_at),
                   count(distinct feed_id)
            from stg_stop_time_updates
            """
        ).fetchone()
        if span[0]:
            minutes = (span[1] - span[0]).total_seconds() / 60
            print(f"\n  observed        {span[0]:%Y-%m-%d %H:%M} to {span[1]:%H:%M}"
                  f"  ({minutes:.0f} min, {span[2]} feeds)")

        unpublished = con.execute(
            "select count(*) from dim_stations where not is_published"
        ).fetchone()[0]
        print(f"  stations the timetable does not publish: {unpublished}")

        rows = con.execute(
            """
            select route_id, count(*) as arrivals
            from fct_arrivals group by 1 order by 2 desc limit 6
            """
        ).fetchall()
        if rows:
            print("\n  most arrivals inferred")
            for route, arrivals in rows:
                print(f"    {route:<4} {arrivals:>7,}")

        worst = con.execute(
            """
            select w.route_id, s.station_name, w.headways,
                   w.mean_headway_minutes, w.excess_wait_minutes
            from fct_excess_wait w
            left join dim_stations s using (station_id)
            where w.headways >= 3
            order by w.excess_wait_minutes desc
            limit 5
            """
        ).fetchall()
        if worst:
            print("\n  worst excess wait (>= 3 gaps in the hour)")
            print(f"    {'rt':<3} {'station':<28} {'gaps':>5} {'mean':>7} {'excess':>7}")
            for route, station, gaps, mean, excess in worst:
                name = (station or "(unpublished)")[:28]
                print(f"    {route:<3} {name:<28} {gaps:>5} {mean:>7.2f} {excess:>7.2f}")
        else:
            print(
                "\n  No hour yet has three gaps at one platform, which is what an\n"
                "  excess wait needs. Keep the poller running: headways only appear\n"
                "  once two trains have been seen to arrive at the same stop."
            )
        print()
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
