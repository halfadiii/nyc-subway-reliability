"""How much does the answer depend on the threshold nobody can justify?

    python -m analysis.sensitivity
    python -m analysis.sensitivity --values 30 60 120 300 600

The cancellation rule -- a record that vanishes more than
`arrival_grace_seconds` before it was due was not an arrival -- is the weakest
link in this pipeline. It is defensible, it is documented, and it is still a
number somebody chose. The honest thing to do with a threshold like that is not
to defend it harder; it is to measure what it does.

So this rebuilds the marts at a range of values and reports how far the
headline figures move. If the answer barely changes across an order of
magnitude, the threshold is not doing the work and the result stands on the
data. If it swings, that is worth knowing before anyone quotes the number, and
worth saying out loud in the write-up rather than leaving for a reader to find.

It writes to a scratch database so the real warehouse is not disturbed, and it
restores nothing afterwards because it never touched anything.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
TRANSFORM = ROOT / "transform"
SCRATCH = ROOT / "data" / "sensitivity.duckdb"

DEFAULT_VALUES = [30, 60, 120, 240, 480]


def rebuild(grace: int, database: Path) -> None:
    """Run the marts with one value of the threshold, into a scratch database."""
    environment = {**os.environ, "MTA_DUCKDB": str(database).replace("\\", "/")}
    result = subprocess.run(
        [
            sys.executable, "-m", "dbt.cli.main", "build",
            "--profiles-dir", ".",
            "--target", "duckdb",
            "--vars", f"{{arrival_grace_seconds: {grace}}}",
            # Tests are asserted once, by `make test`. Here they would only
            # slow down a sweep whose whole purpose is to vary the thing some
            # of them assert on.
            "--exclude", "test_type:singular",
        ],
        cwd=TRANSFORM,
        env=environment,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise SystemExit(
            f"dbt build failed at grace={grace}:\n{result.stdout[-4000:]}"
        )


def measure(database: Path) -> dict:
    con = duckdb.connect(str(database), read_only=True)
    try:
        arrivals, headways = con.execute(
            "select (select count(*) from fct_arrivals),"
            "       (select count(*) from fct_headways)"
        ).fetchone()
        row = con.execute(
            """
            select
                count(*),
                avg(mean_headway_minutes),
                avg(excess_wait_minutes)
            from fct_excess_wait
            where headways >= 3
            """
        ).fetchone()
        return {
            "arrivals": arrivals,
            "headways": headways,
            "route_hours": row[0],
            "mean_headway": row[1],
            "excess_wait": row[2],
        }
    finally:
        con.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--values", type=int, nargs="+", default=DEFAULT_VALUES)
    parser.add_argument("--database", type=Path, default=SCRATCH)
    args = parser.parse_args(argv)

    print("Rebuilding the marts at each threshold. This takes a moment each.\n")
    results: dict[int, dict] = {}
    for grace in args.values:
        rebuild(grace, args.database)
        results[grace] = measure(args.database)
        print(f"  grace={grace:4d}s done")

    print("\n  SENSITIVITY TO THE CANCELLATION THRESHOLD\n")
    print(
        f"  {'grace':>7}  {'arrivals':>9}  {'headways':>9}  "
        f"{'route-hrs':>9}  {'mean hw':>8}  {'excess':>8}"
    )
    print(f"  {'-'*7}  {'-'*9}  {'-'*9}  {'-'*9}  {'-'*8}  {'-'*8}")
    for grace, r in sorted(results.items()):
        mean_hw = f"{r['mean_headway']:.2f}" if r["mean_headway"] is not None else "-"
        excess = f"{r['excess_wait']:.2f}" if r["excess_wait"] is not None else "-"
        print(
            f"  {grace:>6d}s  {r['arrivals']:>9,d}  {r['headways']:>9,d}  "
            f"{r['route_hours']:>9,d}  {mean_hw:>8}  {excess:>8}"
        )

    baseline = results.get(120) or next(iter(results.values()))
    if baseline["excess_wait"]:
        spread = [
            r["excess_wait"] for r in results.values() if r["excess_wait"] is not None
        ]
        if len(spread) > 1:
            swing = (max(spread) - min(spread)) / baseline["excess_wait"]
            print(
                f"\n  Excess wait moves {swing:.1%} across the whole range of "
                f"thresholds tried.\n"
                "  Read that as: how much of the headline number is the data, and\n"
                "  how much is the judgement call in the middle of the pipeline."
            )
    else:
        print(
            "\n  Not enough history yet for the headline figures to be worth\n"
            "  comparing. Run this again once the poller has a few days behind it."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
