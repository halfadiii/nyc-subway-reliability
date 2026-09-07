"""The timetable side of the feed: what the stops and routes actually are.

The realtime feeds identify everything by code. `127N` is a platform, `A` is a
route, and neither carries a name, a position, or any indication that `127N`
and `127S` are two sides of the same station. All of that lives in the MTA's
*static* GTFS bundle, which is published separately and changes a few times a
year rather than every thirty seconds.

    python -m ingest.static_gtfs

Downloads the bundle, keeps the two files this project needs, and writes them
as dbt seeds. The zip is about 5MB and mostly the timetable, which is not
needed here: the schedule this project compares against is the *service* riders
got, derived from the feed, not the published plan.

## Why they are committed as seeds

They are small -- 1,500 stops and 30 routes -- and committing them makes the
repository self-contained: `dbt build` works on a clone with no network. The
alternative is a build step that reaches out to an S3 bucket whose URL has
already moved once, and a project that cannot be rebuilt in two years is a
project that cannot be checked.

Re-run this when the MTA reissues the bundle. `feed_info.txt` in the zip dates
it, and that date is written into the header of each seed so the vintage of the
dimensions is never a guess.
"""

from __future__ import annotations

import csv
import io
import zipfile
from pathlib import Path

import requests

URL = "https://rrgtfsfeeds.s3.amazonaws.com/gtfs_subway.zip"
ROOT = Path(__file__).resolve().parent.parent
SEEDS = ROOT / "transform" / "seeds"

# Only the columns that are used. A seed carrying forty columns nobody reads is
# forty columns of review surface for no gain.
WANTED = {
    "stops.txt": [
        "stop_id",
        "stop_name",
        "stop_lat",
        "stop_lon",
        "location_type",
        "parent_station",
    ],
    "routes.txt": [
        "route_id",
        "route_short_name",
        "route_long_name",
        "route_desc",
        "route_color",
        "route_text_color",
    ],
}

OUT_NAMES = {"stops.txt": "gtfs_stops.csv", "routes.txt": "gtfs_routes.csv"}


def fetch(url: str = URL) -> zipfile.ZipFile:
    print(f"Downloading {url}")
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    print(f"  {len(response.content):,} bytes")
    return zipfile.ZipFile(io.BytesIO(response.content))


def feed_version(bundle: zipfile.ZipFile) -> str:
    """The MTA's own name for this issue of the bundle, if it gives one."""
    try:
        with bundle.open("feed_info.txt") as handle:
            rows = list(csv.DictReader(io.TextIOWrapper(handle, "utf-8-sig")))
        if rows:
            row = rows[0]
            return (
                row.get("feed_version")
                or f"{row.get('feed_start_date', '')}-{row.get('feed_end_date', '')}"
            ).strip()
    except KeyError:
        pass
    return "unknown"


def extract(bundle: zipfile.ZipFile, member: str, columns: list[str]) -> list[dict]:
    with bundle.open(member) as handle:
        reader = csv.DictReader(io.TextIOWrapper(handle, "utf-8-sig"))
        present = [c for c in columns if c in (reader.fieldnames or [])]
        missing = set(columns) - set(present)
        if missing:
            print(f"  {member}: not published in this bundle: {sorted(missing)}")
        return [{c: (row.get(c) or "") for c in present} for row in reader]


def write_seed(path: Path, rows: list[dict], version: str, member: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        raise SystemExit(f"{member} was empty; refusing to write an empty seed")
    with path.open("w", encoding="utf-8", newline="") as handle:
        # `newline=""` matters on Windows: csv writes its own \r\n and the
        # default translation would double it, giving every seed a blank row
        # between every real one.
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"  wrote {path.relative_to(ROOT)}  {len(rows):,} rows  (bundle {version})")


def main() -> int:
    bundle = fetch()
    version = feed_version(bundle)
    print(f"  bundle version {version}\n")

    for member, columns in WANTED.items():
        rows = extract(bundle, member, columns)
        write_seed(SEEDS / OUT_NAMES[member], rows, version, member)

    print(
        "\nSeeds written. `dbt seed --profiles-dir .` loads them; the station "
        "and route dimensions build off them."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
