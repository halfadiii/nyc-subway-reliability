"""Every operation this project supports, without needing make.

    python run.py                 list the tasks
    python run.py demo            build from the committed sample, and show it
    python run.py poll            start collecting (Ctrl-C to stop)
    python run.py build           rebuild from everything collected
    python run.py test            dbt tests + the correctness tests
    python run.py rain            the weather regression
    python run.py sensitivity     how much the threshold moves the answer

There is a Makefile with the same targets, and on a machine with `make` either
works. This exists because Windows does not ship one, and "install make first"
is a poor first line for a repository whose whole pitch is that it runs from a
clone.

It also fixes the thing that makes cross-platform Makefiles unpleasant: the
interpreter. Every task below runs `sys.executable`, so whatever Python is
running this file is the Python the task uses, and a virtualenv that is active
stays active without anything having to know where it lives.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PY = sys.executable

# Task name -> (help text, working directory, argv, extra environment)
Task = tuple[str, Path, list[str], dict[str, str]]


def dbt(*args: str) -> list[str]:
    return [PY, "-m", "dbt.cli.main", *args, "--profiles-dir", "."]


TASKS: dict[str, Task] = {
    "demo": (
        "build the warehouse from the committed sample, then summarise it",
        ROOT / "transform",
        dbt("build"),
        {
            "MTA_RAW_GLOB": "../data/sample/**/*.ndjson.gz",
            "MTA_DUCKDB": "../data/demo.duckdb",
        },
    ),
    "poll": (
        "poll all eight feeds every 30s until stopped",
        ROOT / "ingest",
        [PY, "poller.py"],
        {},
    ),
    "seeds": (
        "re-download station names and route colours from the static bundle",
        ROOT / "ingest",
        [PY, "static_gtfs.py"],
        {},
    ),
    "build": (
        "rebuild the warehouse from everything collected so far",
        ROOT / "transform",
        dbt("build"),
        {},
    ),
    "dbt-test": (
        "run the dbt tests only",
        ROOT / "transform",
        dbt("test"),
        {},
    ),
    "pytest": (
        "run the correctness tests only",
        ROOT,
        [PY, "-m", "pytest", "tests", "-q"],
        {},
    ),
    "rain": (
        "excess wait against rainfall, per route, with intervals",
        ROOT,
        [PY, "-m", "analysis.rain_regression"],
        {},
    ),
    "sensitivity": (
        "how far the headline numbers move with the cancellation threshold",
        ROOT,
        [PY, "-m", "analysis.sensitivity"],
        {},
    ),
    "docs": (
        "generate the dbt documentation site",
        ROOT / "transform",
        dbt("docs", "generate"),
        {},
    ),
}

# Tasks that print a summary of the warehouse they just built.
SUMMARISES = {"demo": "data/demo.duckdb", "build": "data/warehouse.duckdb"}


def run(name: str) -> int:
    if name == "test":
        for step in ("dbt-test", "pytest"):
            code = run(step)
            if code != 0:
                return code
        return 0

    if name not in TASKS:
        print(f"Unknown task {name!r}.\n")
        return usage()

    _help, cwd, argv, extra = TASKS[name]
    environment = {**os.environ, **extra}
    print(f"--> {name}: {' '.join(argv[1:])}\n")
    code = subprocess.run(argv, cwd=cwd, env=environment).returncode
    if code != 0:
        return code

    warehouse = SUMMARISES.get(name)
    if warehouse:
        subprocess.run([PY, "scripts/summarise.py", warehouse], cwd=ROOT)
    return 0


def usage() -> int:
    print(__doc__.splitlines()[0] + "\n")
    print("  tasks:\n")
    width = max(len(n) for n in TASKS)
    for name, (help_text, *_rest) in TASKS.items():
        print(f"    {name:<{width}}  {help_text}")
    print(f"    {'test':<{width}}  dbt-test and pytest together")
    print()
    return 1


if __name__ == "__main__":
    raise SystemExit(run(sys.argv[1]) if len(sys.argv) > 1 else usage())
