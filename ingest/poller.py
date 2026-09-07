"""The poller: eight feeds, every thirty seconds, appended and never touched.

`explore.py` fetches one feed and prints it. This is the same fetch with the
printing replaced by a file, the one feed replaced by all eight, and every way
it can fail handled -- which is most of the code.

    python -m ingest.poller                    # run until Ctrl-C
    python -m ingest.poller --minutes 30       # run for half an hour
    python -m ingest.poller --once             # a single round, for testing

## Why thirty seconds

The MTA regenerates each feed at roughly that cadence, so polling faster buys
duplicate snapshots and polling slower loses arrivals: a train that stops at a
station and leaves inside one poll interval never appears as a change. Thirty
seconds is the source's own rate, which is the fastest rate at which the
question can be answered honestly.

## Why the eight fetches are concurrent

Eight sequential fetches at a second or two each is most of the interval spent
waiting on sockets, and the cadence would drift out with every slow response.
Run together, a round costs about as long as its slowest feed, and the schedule
is kept against a fixed clock rather than by sleeping thirty seconds after
whatever the last round happened to take -- so a slow round is absorbed instead
of accumulating.

## What a failure does

Nothing, deliberately. Each feed is fetched, retried a few times with a backoff,
and if it still fails it is skipped and recorded in the run log. One endpoint
having a bad afternoon costs that feed's rows for that round, not the round, and
certainly not the process: this is meant to run for weeks, and history only
accumulates while it is up.

## Where the rows go

`data/raw/dt=YYYY-MM-DD/hour=HH/{feed}-{observed_at}.ndjson.gz`, one file per
feed per round, gzipped newline-delimited JSON.

The layout is Hive-style partitioning, which is not decoration: `bq load` reads
`dt=` and `hour=` as columns, DuckDB's `read_json_auto` does the same with
`hive_partitioning=1`, and a day's reconstruction reads a day's directory rather
than the whole history. Gzip because these are text and compress about ten to
one, and a month of this is otherwise tens of gigabytes of mostly-repeated
station ids.

The partition is taken from the **feed's** timestamp, not ours, so a row is
filed under the hour the MTA believed it -- which is the hour every query is
about.
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

from com.google.transit.realtime import gtfs_realtime_pb2 as rt
from feeds import FEEDS, feed_url
from schema import flatten

NY = ZoneInfo("America/New_York")
ROOT = Path(__file__).resolve().parent.parent
# Overridable by environment so the container can be pointed at a mounted
# volume without an argument; see deploy/Dockerfile.
DEFAULT_OUT = Path(os.environ.get("MTA_OUT") or (ROOT / "data" / "raw"))

INTERVAL_SECONDS = 30
FETCH_TIMEOUT = 20
RETRIES = 3
BACKOFF_SECONDS = 1.5

# Set by the signal handler so a round finishes writing before we exit. Killing
# the process mid-write is how you get a half-written gzip member that every
# later read has to cope with.
_stopping = False


def _stop(signum, frame) -> None:  # noqa: ARG001
    global _stopping
    _stopping = True
    print("\n  stopping after this round...", flush=True)


class Poller:
    def __init__(self, out_dir: Path = DEFAULT_OUT, session=None):
        self.out_dir = out_dir
        # One session for the whole run: connection reuse turns eight TLS
        # handshakes every thirty seconds into eight the first time only.
        self.session = session or requests.Session()
        self.session.headers["User-Agent"] = "mta-reliability/1.0 (portfolio project)"
        self.rounds = 0
        self.rows_written = 0
        self.failures: dict[str, int] = {}
        # Feed header timestamps already on disk, per feed. The MTA answers
        # every request whether or not it has regenerated, so consecutive polls
        # often return the same snapshot. Writing it twice is not wrong -- the
        # inference takes the last observation and duplicates do not change it
        # -- but it is bytes for nothing, so a repeat is counted and skipped.
        self.last_seen: dict[str, int] = {}
        self.duplicates = 0

    # -- one feed ---------------------------------------------------------

    def fetch(self, feed_id: str) -> tuple[str, bytes | None, float]:
        """Bytes for one feed, or None if it could not be had. Never raises."""
        url = feed_url(feed_id)
        for attempt in range(1, RETRIES + 1):
            try:
                fetched_at = time.time()
                response = self.session.get(url, timeout=FETCH_TIMEOUT)
                response.raise_for_status()
                return feed_id, response.content, fetched_at
            except Exception as exc:  # noqa: BLE001 - any failure is the same failure
                if attempt == RETRIES:
                    self.failures[feed_id] = self.failures.get(feed_id, 0) + 1
                    print(f"    {feed_id:9s} FAILED after {RETRIES}: {exc}", flush=True)
                    return feed_id, None, 0.0
                time.sleep(BACKOFF_SECONDS * attempt)
        return feed_id, None, 0.0

    def write(self, feed_id: str, payload: bytes, fetched_at: float) -> int:
        """Decode, flatten, and append one file. Returns rows written."""
        message = rt.FeedMessage()
        message.ParseFromString(payload)
        observed_at = int(message.header.timestamp)

        # A feed with no header timestamp is a feed we cannot place in time.
        if not observed_at:
            print(f"    {feed_id:9s} no header timestamp, skipped", flush=True)
            return 0

        if self.last_seen.get(feed_id) == observed_at:
            self.duplicates += 1
            return 0
        self.last_seen[feed_id] = observed_at

        rows = list(flatten(feed_id, message, fetched_at))
        if not rows:
            return 0

        when = datetime.fromtimestamp(observed_at, tz=timezone.utc).astimezone(NY)
        directory = self.out_dir / f"dt={when:%Y-%m-%d}" / f"hour={when:%H}"
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{feed_id}-{observed_at}.ndjson.gz"

        # Written to a temporary name and moved into place, so a reader can
        # never open a file that is still being written to. os.replace is
        # atomic on the same filesystem, on Windows as well as POSIX.
        staging = path.with_suffix(".tmp")
        with gzip.open(staging, "wt", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, separators=(",", ":")) + "\n")
        staging.replace(path)
        return len(rows)

    # -- one round --------------------------------------------------------

    def round_once(self) -> int:
        started = time.time()
        with ThreadPoolExecutor(max_workers=len(FEEDS)) as pool:
            results = list(pool.map(self.fetch, FEEDS))

        written = 0
        lag_max = 0
        for feed_id, payload, fetched_at in results:
            if payload is None:
                continue
            try:
                count = self.write(feed_id, payload, fetched_at)
                written += count
                if count:
                    lag = int(fetched_at) - self.last_seen[feed_id]
                    lag_max = max(lag_max, lag)
            except Exception as exc:  # noqa: BLE001
                self.failures[feed_id] = self.failures.get(feed_id, 0) + 1
                print(f"    {feed_id:9s} decode/write failed: {exc}", flush=True)

        self.rounds += 1
        self.rows_written += written
        elapsed = time.time() - started
        print(
            f"  round {self.rounds:4d}  {written:6,d} rows  "
            f"{elapsed:4.1f}s  feed lag {lag_max:3d}s  "
            f"total {self.rows_written:,d}",
            flush=True,
        )
        return written

    def run(self, minutes: float | None = None) -> None:
        signal.signal(signal.SIGINT, _stop)
        signal.signal(signal.SIGTERM, _stop)

        deadline = time.time() + minutes * 60 if minutes else None
        print(
            f"Polling {len(FEEDS)} feeds every {INTERVAL_SECONDS}s into "
            f"{self.out_dir}\n"
            + (f"Running for {minutes:g} minutes.\n" if minutes else "Ctrl-C to stop.\n")
        )

        # A fixed schedule rather than sleep-after-work, so a slow round is
        # absorbed instead of pushing every later round further out.
        next_at = time.time()
        while not _stopping and (deadline is None or time.time() < deadline):
            self.round_once()
            next_at += INTERVAL_SECONDS
            sleep_for = next_at - time.time()
            if sleep_for < 0:
                # Fell behind: give up the missed slots rather than sprinting to
                # catch up, which would only hammer an endpoint already slow.
                next_at = time.time()
                sleep_for = 0
            end_by = time.time() + sleep_for
            while not _stopping and time.time() < end_by:
                time.sleep(min(0.5, end_by - time.time()))

        self.report()

    def report(self) -> None:
        print("\n" + "=" * 58)
        print(f"  rounds        {self.rounds:,d}")
        print(f"  rows written  {self.rows_written:,d}")
        print(f"  duplicates    {self.duplicates:,d} (feed had not regenerated)")
        if self.failures:
            print(f"  failures      {self.failures}")
        else:
            print("  failures      none")
        print("=" * 58)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--minutes", type=float, default=None)
    parser.add_argument("--once", action="store_true", help="one round, then exit")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args(argv)

    poller = Poller(out_dir=args.out)
    if args.once:
        poller.round_once()
        poller.report()
    else:
        poller.run(minutes=args.minutes)
    return 0


if __name__ == "__main__":
    sys.exit(main())
