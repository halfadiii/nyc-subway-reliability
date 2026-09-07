"""Load the poller's files into the BigQuery landing table.

    python -m load.bigquery.load_bq --dry-run           # print the plan
    python -m load.bigquery.load_bq --date 2026-09-07   # load one day

The poller writes gzipped newline-delimited JSON, which is a format BigQuery
loads natively -- so this is a job configuration and a file list, not a
transformation. Nothing is reshaped on the way in; that is the point of the
landing table.

## The epoch columns

The files carry epoch seconds because that is what the feed emits and the
landing files are meant to be a faithful copy of it. The table declares those
columns as TIMESTAMP, and BigQuery will read an integer into a TIMESTAMP column
as seconds since the epoch, so the cast happens in the load rather than in a
staging step that could be skipped.

## Idempotence

Loading the same day twice would double it, so each load writes to a *partition
decorator* -- `table$20260907` -- with WRITE_TRUNCATE. That replaces exactly
that day and leaves every other partition alone, which makes a re-run after a
failure safe and makes a backfill a loop rather than a special case.

## Credentials

Application Default Credentials: `gcloud auth application-default login`
locally, or the service account attached to the job in production. Nothing here
reads a key file, and no key file belongs in this repository.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
RAW = ROOT / "data" / "raw"


def partitions(raw_dir: Path, only: str | None) -> dict[str, list[Path]]:
    """Files on disk, grouped by the date partition they belong to."""
    found: dict[str, list[Path]] = {}
    for path in sorted(raw_dir.glob("dt=*/hour=*/*.ndjson.gz")):
        day = path.parent.parent.name.removeprefix("dt=")
        if only and day != only:
            continue
        found.setdefault(day, []).append(path)
    return found


def load(
    project: str,
    dataset: str,
    day: str,
    files: list[Path],
    location: str,
) -> int:
    """Replace one day's partition from a list of local files."""
    from google.cloud import bigquery  # imported here so --dry-run needs no SDK

    client = bigquery.Client(project=project, location=location)
    suffix = day.replace("-", "")
    table = f"{project}.{dataset}_raw.stop_time_updates${suffix}"

    config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        # The table already exists with the right partitioning and clustering;
        # letting a load job infer a schema is how a column quietly becomes a
        # STRING one afternoon and every downstream cast starts failing.
        autodetect=False,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        # A single malformed line should not lose a day. It should be visible.
        max_bad_records=0,
        ignore_unknown_values=False,
    )

    total = 0
    for path in files:
        with path.open("rb") as handle:
            job = client.load_table_from_file(handle, table, job_config=config)
        job.result()
        total += job.output_rows or 0
        # Only the first file of a day may truncate; the rest append into the
        # partition it just replaced.
        config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    return total


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--raw", type=Path, default=RAW)
    parser.add_argument("--date", help="one YYYY-MM-DD partition; default all")
    parser.add_argument("--project", default=os.environ.get("BQ_PROJECT", ""))
    parser.add_argument("--dataset", default=os.environ.get("BQ_DATASET", "mta"))
    parser.add_argument("--location", default=os.environ.get("BQ_LOCATION", "US"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    if args.date:
        try:
            datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            parser.error("--date must be YYYY-MM-DD")

    found = partitions(args.raw, args.date)
    if not found:
        print(f"No files under {args.raw}. Has the poller run?")
        return 1

    print(f"{sum(len(v) for v in found.values()):,} files in {len(found)} partitions")
    for day, files in sorted(found.items()):
        size = sum(f.stat().st_size for f in files)
        print(f"  {day}  {len(files):5,d} files  {size / 1e6:7.1f} MB compressed")

    if args.dry_run:
        print(
            f"\nDry run. Would load into "
            f"{args.project or '${BQ_PROJECT}'}.{args.dataset}_raw.stop_time_updates,\n"
            "one WRITE_TRUNCATE per partition decorator, then appends."
        )
        return 0

    if not args.project:
        parser.error("--project (or BQ_PROJECT) is required for a real load")

    grand_total = 0
    for day, files in sorted(found.items()):
        rows = load(args.project, args.dataset, day, files, args.location)
        grand_total += rows
        print(f"  loaded {day}: {rows:,} rows")
    print(f"\n{grand_total:,} rows loaded.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
