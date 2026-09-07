"""The inference rules, proved against the real SQL on data with a known answer.

These are integration tests, not unit tests, and that is deliberate. The three
rules that decide what counts as an arrival live in `int_inferred_arrivals.sql`;
a Python reimplementation of them would test the reimplementation. So each test
writes synthetic snapshots in the poller's own output format, points dbt at
them, runs the actual project, and asserts on the marts that come out.

The fixtures are built so the right answer is known by construction:

  * a train that arrives           -> its prediction converges and the row goes
  * a train that is cancelled      -> the row goes while still ten minutes out
  * a train still en route         -> the row is in the last snapshot we took
  * six trains with known gaps     -> an excess wait that can be worked by hand

If the SQL stops distinguishing the first three, or the excess-wait formula
drifts, these fail. Nothing else in the repository can catch that: on real feed
data there is no ground truth to check against, which is the whole reason the
project exists.
"""

from __future__ import annotations

import gzip
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parent.parent
TRANSFORM = ROOT / "transform"

# A Tuesday, in the middle of a service day, and not on a DST transition --
# so the local-time columns are unambiguous and the test does not start failing
# on one weekend a year.
BASE = datetime(2026, 3, 10, 14, 0, 0, tzinfo=timezone.utc)
SERVICE_DATE = "20260310"
FEED = "ace"
ROUTE = "A"  # A real route id, so the dim_routes relationship test has a target.


def epoch(offset_seconds: int) -> int:
    return int((BASE + timedelta(seconds=offset_seconds)).timestamp())


def row(*, observed: int, trip: str, stop: str, arrival: int) -> dict:
    """One flattened stop-time update, in the shape `ingest/schema.py` emits."""
    return {
        "feed_id": FEED,
        "observed_at": observed,
        "fetched_at": observed + 3,
        "trip_id": trip,
        "route_id": ROUTE,
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


def write_snapshots(raw_dir: Path, snapshots: dict[int, list[dict]]) -> None:
    """One gzipped NDJSON file per snapshot, in the poller's partition layout."""
    for observed_at, rows in snapshots.items():
        when = datetime.fromtimestamp(observed_at, tz=timezone.utc)
        directory = raw_dir / f"dt={when:%Y-%m-%d}" / f"hour={when:%H}"
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{FEED}-{observed_at}.ndjson.gz"
        with gzip.open(path, "wt", encoding="utf-8") as handle:
            for r in rows:
                handle.write(json.dumps(r) + "\n")


def run_dbt(raw_dir: Path, warehouse: Path) -> None:
    """Run the real project against the fixture, and fail loudly if it fails."""
    environment = {
        **os.environ,
        "MTA_RAW_GLOB": str(raw_dir / "**" / "*.ndjson.gz").replace("\\", "/"),
        "MTA_DUCKDB": str(warehouse).replace("\\", "/"),
    }
    result = subprocess.run(
        [
            sys.executable, "-m", "dbt.cli.main", "build",
            "--profiles-dir", ".",
            "--target", "duckdb",
        ],
        cwd=TRANSFORM,
        env=environment,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise AssertionError(
            "dbt build failed on the fixture:\n"
            + result.stdout[-6000:]
            + result.stderr[-2000:]
        )


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory) -> duckdb.DuckDBPyConnection:
    """Build the whole project once, against one fixture, for every test below."""
    workspace = tmp_path_factory.mktemp("pipeline")
    raw_dir = workspace / "raw"
    database = workspace / "test.duckdb"

    # -- the scenario ----------------------------------------------------
    # Six snapshots, thirty seconds apart, exactly as the poller would write.
    snapshots: dict[int, list[dict]] = {}

    for index in range(6):
        observed = epoch(index * 30)
        rows: list[dict] = []

        # 1. ARRIVES. Predicted for T+90s, and the prediction converges on it.
        #    Present for the first four snapshots (through T+90), gone after --
        #    which is what a train pulling into a platform looks like.
        if index <= 3:
            rows.append(row(observed=observed, trip="arrives", stop="T01N",
                            arrival=epoch(90)))

        # 2. CANCELLED. Predicted for T+900s, ten minutes out, and the record
        #    disappears after the second snapshot. Nothing arrived; the trip was
        #    pulled. The grace rule has to reject this.
        if index <= 1:
            rows.append(row(observed=observed, trip="cancelled", stop="T02N",
                            arrival=epoch(900)))

        # 3. STILL EN ROUTE. Present in every snapshot including the last one.
        #    It has not vanished; we simply stopped watching. The watermark
        #    rule has to reject this, or every train in the newest snapshot
        #    becomes an arrival.
        rows.append(row(observed=observed, trip="enroute", stop="T03N",
                        arrival=epoch(3600)))

        # 4. SIX TRAINS AT ONE PLATFORM, with gaps of 2, 18, 2, 18 and 2
        #    minutes. Worked by hand in the assertions below.
        for train, minutes in enumerate([0, 2, 20, 22, 40, 42]):
            arrival = epoch(minutes * 60)
            # Each is visible until the snapshot after it is due, then gone.
            if observed <= arrival:
                rows.append(row(observed=observed, trip=f"gap{train}",
                                stop="T04N", arrival=arrival))

        snapshots[observed] = rows

    # The gap trains are due up to 42 minutes out, so the six snapshots above
    # never see most of them vanish. Extend the watch to an hour and a half of
    # snapshots carrying only the trains that are still ahead.
    for index in range(6, 190):
        observed = epoch(index * 30)
        rows = [row(observed=observed, trip="enroute", stop="T03N",
                    arrival=epoch(3600))]
        for train, minutes in enumerate([0, 2, 20, 22, 40, 42]):
            arrival = epoch(minutes * 60)
            if observed <= arrival:
                rows.append(row(observed=observed, trip=f"gap{train}",
                                stop="T04N", arrival=arrival))
        snapshots[observed] = rows

    write_snapshots(raw_dir, snapshots)
    run_dbt(raw_dir, database)

    connection = duckdb.connect(str(database), read_only=True)
    yield connection
    connection.close()


# -- the three rules -------------------------------------------------------

def test_a_train_that_arrives_is_an_arrival(warehouse):
    trips = {r[0] for r in warehouse.execute(
        "select trip_id from fct_arrivals where stop_id = 'T01N'"
    ).fetchall()}
    assert trips == {"arrives"}


def test_a_cancelled_trip_is_not_an_arrival(warehouse):
    """It vanished while still ten minutes from being due. Nothing arrived."""
    count = warehouse.execute(
        "select count(*) from fct_arrivals where trip_id = 'cancelled'"
    ).fetchone()[0]
    assert count == 0


def test_a_train_still_en_route_is_not_an_arrival(warehouse):
    """Present in the newest snapshot: it has not disappeared, we stopped looking.

    This is the rule that is easy to leave out, and leaving it out produces
    arrivals for every train in the latest snapshot -- worst exactly at the
    leading edge of the data, which is where anyone reading a dashboard looks.
    """
    count = warehouse.execute(
        "select count(*) from fct_arrivals where trip_id = 'enroute'"
    ).fetchone()[0]
    assert count == 0


def test_the_arrival_time_is_the_last_prediction_before_it_vanished(warehouse):
    inferred, last_seen = warehouse.execute(
        """
        select inferred_arrival, last_seen_at
        from fct_arrivals where trip_id = 'arrives'
        """
    ).fetchone()
    # Both come back timezone-aware, in New York local time -- `.timestamp()`
    # converts. Replacing the tzinfo instead would silently shift them by the
    # UTC offset and this assertion would be checking the wrong instant.
    assert int(inferred.timestamp()) == epoch(90)
    # Last seen in the T+90 snapshot, which is the last one that carried it.
    assert int(last_seen.timestamp()) == epoch(90)


# -- the metric ------------------------------------------------------------

def test_headways_are_the_gaps_between_consecutive_trains(warehouse):
    gaps = [r[0] for r in warehouse.execute(
        """
        select headway_minutes from fct_headways
        where stop_id = 'T04N' order by inferred_arrival
        """
    ).fetchall()]
    assert gaps == pytest.approx([2.0, 18.0, 2.0, 18.0, 2.0])


def test_excess_wait_matches_the_hand_worked_figure(warehouse):
    """Gaps of 2, 18, 2, 18, 2 minutes.

        sum(h)  = 42
        sum(h2) = 4 + 324 + 4 + 324 + 4 = 660
        rider wait = 660 / (2 x 42)      = 7.857 min
        even wait  = (42 / 5) / 2        = 4.200 min
        excess     = 7.857 - 4.200       = 3.657 min

    Which is the point of the metric: those six trains are a perfectly
    respectable 8.4-minute mean headway, and riders wait nearly four minutes
    longer than that implies because half of them walk into an 18-minute hole.
    """
    headways, mean_headway, rider, even, excess = warehouse.execute(
        """
        select headways, mean_headway_minutes, rider_wait_minutes,
               even_wait_minutes, excess_wait_minutes
        from fct_excess_wait where stop_id = 'T04N'
        """
    ).fetchone()

    assert headways == 5
    assert mean_headway == pytest.approx(8.4)
    assert rider == pytest.approx(7.857142857, rel=1e-6)
    assert even == pytest.approx(4.2)
    assert excess == pytest.approx(3.657142857, rel=1e-6)


def test_an_even_service_has_no_excess_wait(warehouse):
    """The control. Excess wait is a variance term; on regular gaps it is zero.

    Asserted against the maths rather than the data: `even_wait_minutes` is
    half the mean headway by definition, so a service whose gaps are all equal
    must produce exactly zero, and any drift means the formula has changed.
    """
    rows = warehouse.execute(
        """
        select excess_wait_minutes
        from fct_excess_wait
        where min_headway_minutes = max_headway_minutes
        """
    ).fetchall()
    for (excess,) in rows:
        assert excess == pytest.approx(0.0, abs=1e-9)


def test_unpublished_stations_are_kept_and_flagged(warehouse):
    """The fixture's stops are not in the static bundle. They must still exist.

    Dropping a fact because a *different* published file is incomplete would be
    deleting a measurement to protect a reference. The dimension carries them
    with `is_published = false` instead, so the join holds and the gap is
    countable.
    """
    published, name = warehouse.execute(
        "select is_published, station_name from dim_stations where station_id = 'T01'"
    ).fetchone()
    assert published is False
    assert name is None

    orphans = warehouse.execute(
        """
        select count(*) from fct_arrivals a
        left join dim_stations s using (station_id)
        where s.station_id is null
        """
    ).fetchone()[0]
    assert orphans == 0
