"""An incremental build must be the same warehouse as a full rebuild.

Two models only read what is new: `stg_stop_time_updates` and
`int_last_sightings`. That is a claim about speed, and it is worthless unless a
second claim holds with it: that the tables they leave behind are row-for-row
the tables a rebuild from every raw file would have produced. An incremental
model that is fast and slightly wrong is worse than a slow one, because nothing
about it looks wrong.

So every test here builds the same raw files twice -- once in several sittings,
the way the pipeline really runs, and once in a single pass -- and compares the
results as sets. The sittings are arranged to hit the cases where an incremental
build goes wrong:

  * a train in the newest snapshot of one sitting that is gone in the next
    (it must become an arrival without ever being observed again)
  * a pair that vanishes, is recorded as an arrival, and then comes back
    (the arrival must be withdrawn)
  * a feed that stops for a while and resumes
  * a file that lands late, and a file that lands twice

And one test states the limit instead of hiding it: a file that lands later
than the lookback is missed by an incremental run, and found by a refresh.
"""

from __future__ import annotations

import gzip
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parent.parent
TRANSFORM = ROOT / "transform"
SAMPLE = ROOT / "data" / "sample"

# The same instant test_pipeline.py uses, for the same reasons.
BASE = datetime(2026, 3, 10, 14, 0, 0, tzinfo=timezone.utc)
SERVICE_DATE = "20260310"

# Every table an incremental model feeds, down to the last mart.
COMPARED = [
    "stg_stop_time_updates",
    "int_last_sightings",
    "fct_arrivals",
    "fct_headways",
    "fct_excess_wait",
]

Snapshots = dict[tuple[str, int], list[dict]]


def epoch(offset_seconds: int) -> int:
    return int((BASE + timedelta(seconds=offset_seconds)).timestamp())


def tick(index: int) -> int:
    """The observation time of the poller's `index`-th round, 30s apart."""
    return epoch(index * 30)


def row(*, feed: str, route: str, observed: int, trip: str, stop: str,
        arrival: int, fetched: int | None = None) -> dict:
    """One flattened stop-time update, in the shape `ingest/schema.py` emits."""
    return {
        "feed_id": feed,
        "observed_at": observed,
        "fetched_at": observed + 3 if fetched is None else fetched,
        "trip_id": trip,
        "route_id": route,
        "start_date": SERVICE_DATE,
        "direction": "NORTH",
        "train_id": f"train-{trip}",
        "is_assigned": True,
        "stop_id": stop,
        "stop_sequence": 0,
        "predicted_arrival": arrival,
        "predicted_departure": arrival + 30,
        "scheduled_track": "1",
        "actual_track": "1",
        "schedule_relationship": "SCHEDULED",
    }


def write(raw_dir: Path, snapshots: Snapshots, suffix: str = "") -> None:
    """One gzipped NDJSON file per feed per snapshot, as the poller lays them out."""
    for (feed, observed_at), rows in snapshots.items():
        when = datetime.fromtimestamp(observed_at, tz=timezone.utc)
        directory = raw_dir / f"dt={when:%Y-%m-%d}" / f"hour={when:%H}"
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{feed}-{observed_at}{suffix}.ndjson.gz"
        with gzip.open(path, "wt", encoding="utf-8") as handle:
            for r in rows:
                handle.write(json.dumps(r) + "\n")


def run_dbt(raw_dir: Path, warehouse: Path, *, full_refresh: bool = False,
            variables: dict | None = None) -> None:
    """Run the real project. Incremental unless told otherwise, as a normal build is."""
    environment = {
        **os.environ,
        "MTA_RAW_GLOB": str(raw_dir / "**" / "*.ndjson.gz").replace("\\", "/"),
        "MTA_DUCKDB": str(warehouse).replace("\\", "/"),
    }
    command = [
        sys.executable, "-m", "dbt.cli.main", "build",
        "--profiles-dir", ".",
        "--target", "duckdb",
    ]
    if full_refresh:
        command.append("--full-refresh")
    if variables:
        command += ["--vars", json.dumps(variables)]
    result = subprocess.run(
        command, cwd=TRANSFORM, env=environment, capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise AssertionError(
            "dbt build failed:\n" + result.stdout[-6000:] + result.stderr[-2000:]
        )


def differences(staged: Path, rebuilt: Path) -> dict[str, tuple[int, int]]:
    """Rows in one warehouse and not the other, per table. Empty means identical."""
    connection = duckdb.connect()
    connection.execute(f"attach '{staged.as_posix()}' as staged (read_only)")
    connection.execute(f"attach '{rebuilt.as_posix()}' as rebuilt (read_only)")
    found: dict[str, tuple[int, int]] = {}
    try:
        for table in COMPARED:
            # Floating-point columns are compared to nine decimal places. An
            # average is a sum, DuckDB adds in parallel, and the order it adds
            # in is not fixed -- so the same rows give 20.055555555555554 in
            # one build and ...557 in the next. That is not a difference in the
            # data, and a week-sized run showed it in three rows of
            # `fct_excess_wait`. Everything else is compared exactly.
            columns = ", ".join(
                f"round({name}, 9) as {name}" if kind in ("DOUBLE", "FLOAT") else name
                for name, kind in connection.execute(
                    "select column_name, data_type from information_schema.columns"
                    " where table_catalog = 'staged' and table_name = ?"
                    " order by ordinal_position",
                    [table],
                ).fetchall()
            )
            only_staged = connection.execute(
                f"select count(*) from (select {columns} from staged.{table}"
                f" except select {columns} from rebuilt.{table})"
            ).fetchone()[0]
            only_rebuilt = connection.execute(
                f"select count(*) from (select {columns} from rebuilt.{table}"
                f" except select {columns} from staged.{table})"
            ).fetchone()[0]
            if only_staged or only_rebuilt:
                found[table] = (only_staged, only_rebuilt)
    finally:
        connection.close()
    return found


def scalar(warehouse: Path, sql: str):
    connection = duckdb.connect(str(warehouse), read_only=True)
    try:
        return connection.execute(sql).fetchone()[0]
    finally:
        connection.close()


# -- the scenario ------------------------------------------------------------

# Three sittings of the `ace` feed, a hundred rounds in all.
SITTINGS = [range(0, 20), range(20, 60), range(60, 100)]
# The `g` feed is polled in the first and third only: an outage in the middle.
G_ROUNDS = set(SITTINGS[0]) | set(SITTINGS[2])


def scenario(rounds: range) -> Snapshots:
    """What the poller would have written during one sitting."""
    snapshots: Snapshots = {}

    for index in rounds:
        observed = tick(index)
        ace: list[dict] = []

        def add(trip: str, stop: str, arrival: int) -> None:
            ace.append(row(feed="ace", route="A", observed=observed,
                           trip=trip, stop=stop, arrival=arrival))

        # Arrives early in the first sitting, and is seen to.
        if index <= 3:
            add("arrives", "T01N", epoch(90))

        # Pulled while still fifteen minutes out.
        if index <= 1:
            add("cancelled", "T02N", epoch(900))

        # In every snapshot there is. Never an arrival.
        add("enroute", "T03N", epoch(7200))

        # In the *last* snapshot of the first sitting, and in nothing after.
        # At the end of that sitting it has not vanished; nobody has looked
        # again. The second sitting never observes it, and has to conclude it
        # arrived anyway.
        if index <= 19:
            add("straddles", "T06N", tick(19) + 20)

        # Vanishes on time in the first sitting, so it is recorded as an
        # arrival. Then it turns up again in the second, fifteen minutes out,
        # and is pulled. The recorded arrival has to be withdrawn.
        if index <= 3:
            add("returns", "T05N", epoch(90))
        if 30 <= index <= 32:
            add("returns", "T05N", tick(32) + 900)

        # Six trains at one platform, their gaps crossing all three sittings.
        for train, minutes in enumerate([0, 2, 20, 22, 40, 42]):
            arrival = epoch(minutes * 60)
            if observed <= arrival:
                add(f"gap{train}", "T04N", arrival)

        snapshots[("ace", observed)] = ace

        if index in G_ROUNDS:
            g: list[dict] = []

            def add_g(trip: str, stop: str, arrival: int) -> None:
                g.append(row(feed="g", route="G", observed=observed,
                             trip=trip, stop=stop, arrival=arrival))

            # Arrives and is seen to, before the outage.
            if index <= 10:
                add_g("g-arrives", "G01N", tick(10) + 15)
            # In the last snapshot before the outage; gone when the feed is
            # back, forty rounds later.
            if index <= 19:
                add_g("g-straddles", "G02N", tick(19) + 10)
            # Only exists after the outage.
            if 60 <= index <= 70:
                add_g("g-later", "G03N", tick(70) + 15)
            # Keeps the feed's watermark moving after everything else is gone.
            add_g("g-enroute", "G04N", epoch(7200))

            snapshots[("g", observed)] = g

    return snapshots


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    """The scenario built in three sittings, and again in one pass.

    Lookback is zero for the staged build. With the default three hours every
    sitting would simply re-read the whole fixture, and the test would pass
    without the watermark logic ever being asked a question.
    """
    workspace = tmp_path_factory.mktemp("incremental")
    raw_dir = workspace / "raw"
    staged = workspace / "staged.duckdb"
    rebuilt = workspace / "rebuilt.duckdb"
    no_overlap = {"incremental_lookback_hours": 0}

    after: list[dict] = []
    for rounds in SITTINGS:
        write(raw_dir, scenario(rounds))
        run_dbt(raw_dir, staged, variables=no_overlap)
        after.append({
            trip: scalar(
                staged,
                f"select count(*) from fct_arrivals where trip_id = '{trip}'",
            )
            for trip in ["arrives", "straddles", "returns", "g-straddles"]
        })

    run_dbt(raw_dir, rebuilt, full_refresh=True)
    return {
        "raw": raw_dir, "staged": staged, "rebuilt": rebuilt,
        "after": after, "no_overlap": no_overlap,
    }


# -- equal to a rebuild ------------------------------------------------------

def test_three_sittings_build_the_same_warehouse_as_one_pass(built):
    assert differences(built["staged"], built["rebuilt"]) == {}


def test_a_train_in_the_newest_snapshot_arrives_once_somebody_looks_again(built):
    """Not an arrival at the end of the first sitting; an arrival after the second.

    The second sitting contains no observation of this train at all. It becomes
    an arrival only because the feed's watermark moved past its last sighting,
    which is the case an incremental model that only looks at new rows misses.
    """
    first, second, _third = built["after"]
    assert first["straddles"] == 0
    assert second["straddles"] == 1


def test_an_arrival_is_withdrawn_when_the_train_comes_back(built):
    first, second, third = built["after"]
    assert first["returns"] == 1
    assert second["returns"] == 0
    assert third["returns"] == 0


def test_a_feed_that_stopped_is_picked_up_where_it_left_off(built):
    """`g` was not polled during the second sitting. Its watermark must not move
    just because another feed's did."""
    first, second, third = built["after"]
    assert first["g-straddles"] == 0
    assert second["g-straddles"] == 0
    assert third["g-straddles"] == 1


def test_running_again_with_nothing_new_changes_nothing(built):
    run_dbt(built["raw"], built["staged"], variables=built["no_overlap"])
    assert differences(built["staged"], built["rebuilt"]) == {}


def test_the_threshold_can_be_varied_without_a_rebuild(built):
    """The sensitivity sweep changes `arrival_grace_seconds` between runs.

    If the threshold were baked into a stored row, an incremental run with a
    new value would leave every older row judged by the old one. It is applied
    at read time instead, so the staged warehouse must agree with a full
    rebuild at the new value, and the pulled trip must now count.
    """
    generous = {"arrival_grace_seconds": 1200}
    run_dbt(built["raw"], built["staged"],
            variables={**built["no_overlap"], **generous})
    run_dbt(built["raw"], built["rebuilt"], full_refresh=True, variables=generous)

    assert differences(built["staged"], built["rebuilt"]) == {}
    assert scalar(
        built["staged"],
        "select count(*) from fct_arrivals where trip_id = 'cancelled'",
    ) == 1


# -- files that land late, twice, or too late --------------------------------

def test_late_and_replayed_files_within_the_lookback(tmp_path):
    raw_dir = tmp_path / "raw"
    staged = tmp_path / "staged.duckdb"
    rebuilt = tmp_path / "rebuilt.duckdb"
    hour = {"incremental_lookback_hours": 1}

    def snapshot(index: int, suffix: str = "", fetched_late: int = 0) -> None:
        observed = tick(index)
        write(raw_dir, {("ace", observed): [
            row(feed="ace", route="A", observed=observed, trip="enroute",
                stop="T03N", arrival=epoch(7200),
                fetched=observed + 3 + fetched_late),
            row(feed="ace", route="A", observed=observed, trip=f"late{index}",
                stop="T07N", arrival=observed + 20),
        ]}, suffix)

    # Two hours of rounds, with round 200 missing from the first sitting.
    for index in range(0, 240, 4):
        if index != 200:
            snapshot(index)
    run_dbt(raw_dir, staged, variables=hour)
    rows_before = scalar(staged, "select count(*) from stg_stop_time_updates")

    # Round 200 lands now, twenty minutes behind the newest row held and
    # inside the hour. Round 236 lands a second time, as a replay would.
    snapshot(200)
    snapshot(236, suffix="-replay", fetched_late=60)
    snapshot(240)
    run_dbt(raw_dir, staged, variables=hour)

    # Two new snapshots of two rows each; the replay added nothing.
    assert scalar(staged, "select count(*) from stg_stop_time_updates") == rows_before + 4
    assert scalar(
        staged, "select count(*) from fct_arrivals where trip_id = 'late200'"
    ) == 1

    run_dbt(raw_dir, rebuilt, full_refresh=True)
    assert differences(staged, rebuilt) == {}


def test_a_file_later_than_the_lookback_needs_a_refresh(tmp_path):
    """The limit, stated. An incremental run does not find this file.

    That is the trade the lookback makes, and it is better written down as a
    test than discovered: a backfill of anything older than the lookback is
    followed by `python run.py refresh`.
    """
    raw_dir = tmp_path / "raw"
    warehouse = tmp_path / "warehouse.duckdb"
    hour = {"incremental_lookback_hours": 1}

    def snapshot(index: int) -> None:
        observed = tick(index)
        write(raw_dir, {("ace", observed): [
            row(feed="ace", route="A", observed=observed, trip="enroute",
                stop="T03N", arrival=epoch(7200)),
            row(feed="ace", route="A", observed=observed, trip=f"late{index}",
                stop="T07N", arrival=observed + 20),
        ]})

    for index in range(0, 240, 4):
        if index != 20:
            snapshot(index)
    run_dbt(raw_dir, warehouse, variables=hour)

    # Round 20 is ten minutes into a two-hour run: well outside the hour.
    snapshot(20)
    snapshot(240)
    run_dbt(raw_dir, warehouse, variables=hour)
    missed = "select count(*) from fct_arrivals where trip_id = 'late20'"
    assert scalar(warehouse, missed) == 0

    run_dbt(raw_dir, warehouse, full_refresh=True, variables=hour)
    assert scalar(warehouse, missed) == 1


# -- and on real observations ------------------------------------------------

def test_the_committed_sample_in_three_sittings_equals_one_pass(tmp_path):
    """The same claim on what the MTA actually sent, at the default lookback.

    The fixtures above are built to hit particular cases. This one has no
    opinion: it deals the 150-minute sample out in three sittings in the order
    it was collected, and the result must be the warehouse one pass builds.
    """
    files = sorted(
        SAMPLE.rglob("*.ndjson.gz"),
        key=lambda path: int(path.name.split("-")[-1].split(".")[0]),
    )
    assert len(files) > 300, "the committed sample is missing"

    raw_dir = tmp_path / "raw"
    staged = tmp_path / "staged.duckdb"
    rebuilt = tmp_path / "rebuilt.duckdb"

    third = len(files) // 3
    for sitting in (files[:third], files[third:2 * third], files[2 * third:]):
        for source in sitting:
            target = raw_dir / source.relative_to(SAMPLE)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)
        run_dbt(raw_dir, staged)

    run_dbt(raw_dir, rebuilt, full_refresh=True)

    assert differences(staged, rebuilt) == {}
    assert scalar(staged, "select count(*) from fct_arrivals") > 1000
