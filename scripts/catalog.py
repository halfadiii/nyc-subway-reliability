"""The data catalog, written from the project itself.

    python scripts/catalog.py data/demo.duckdb            # write docs/catalog.md
    python scripts/catalog.py data/demo.duckdb --check    # fail if anything is undocumented

A catalog typed by hand is out of date the first time somebody adds a column.
This one is assembled from two things that cannot drift from the pipeline,
because they *are* the pipeline:

  * `transform/target/manifest.json`, which dbt writes on every build and which
    carries every description in the `.yml` files, how each model is
    materialised, what it reads from, and which tests guard it;
  * the warehouse, for the columns that actually exist, their types, and how
    many rows are in each table.

The columns come from the warehouse and the descriptions from the manifest, on
purpose. Done the other way round, a column nobody described would simply not
appear, and the catalog would look complete. This way it appears with a gap
next to it, and `--check` turns that gap into a failing build.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
MANIFEST = ROOT / "transform" / "target" / "manifest.json"
OUTPUT = ROOT / "docs" / "catalog.md"

# Reading order: the way the data flows.
LAYERS = [
    ("Staging", "The landing rows, made usable.", ["stg_stop_time_updates"]),
    (
        "Intermediate",
        "Where an arrival is decided.",
        ["int_last_sightings", "int_inferred_arrivals"],
    ),
    (
        "Marts",
        "What analysis reads. A star: three facts, two dimensions.",
        ["fct_arrivals", "fct_headways", "fct_excess_wait", "dim_stations", "dim_routes"],
    ),
]

MATERIALISED = {
    "incremental": "Incremental table",
    "table": "Table",
    "view": "View",
}


def one_line(text: str) -> str:
    return " ".join((text or "").split())


def cell(text: str) -> str:
    return one_line(text).replace("|", "\\|")


def load_manifest() -> dict:
    if not MANIFEST.exists():
        raise SystemExit(
            f"No manifest at {MANIFEST}. Build first: `python run.py demo`."
        )
    return json.loads(MANIFEST.read_text(encoding="utf-8"))


def tests_by_model(manifest: dict) -> dict[str, int]:
    """How many dbt tests guard each model."""
    counts: dict[str, int] = {}
    for node in manifest["nodes"].values():
        if node["resource_type"] != "test":
            continue
        for parent in node.get("depends_on", {}).get("nodes", []):
            name = parent.split(".")[-1]
            counts[name] = counts.get(name, 0) + 1
    return counts


def build(warehouse: Path) -> tuple[str, list[str]]:
    """The catalog as markdown, and every gap found while writing it."""
    manifest = load_manifest()
    models = {
        node["name"]: node
        for node in manifest["nodes"].values()
        if node["resource_type"] == "model"
    }
    tests = tests_by_model(manifest)
    source = next(iter(manifest["sources"].values()))
    gaps: list[str] = []

    connection = duckdb.connect(str(warehouse), read_only=True)
    lines: list[str] = [
        "# Data catalog",
        "",
        "Every table in the warehouse: what one row is, how it is kept up to",
        "date, what it is built from, and what each column means.",
        "",
        "**Generated, not written.** `python run.py catalog` assembles this from",
        "the dbt project and the warehouse it built, so it cannot describe a",
        "column that does not exist or miss one that does. CI fails if any",
        "column is left without a description. Row counts are from the",
        "committed sample (`python run.py demo`): three feeds, 150 minutes.",
        "",
        "## At a glance",
        "",
        "| Table | Layer | Kept up to date by | One row is | Rows in the sample |",
        "| --- | --- | --- | --- | --- |",
    ]

    counts: dict[str, int] = {}
    for layer, _blurb, names in LAYERS:
        for name in names:
            counts[name] = connection.execute(
                f"select count(*) from {name}"
            ).fetchone()[0]
            node = models[name]
            meta = node.get("meta") or node.get("config", {}).get("meta", {})
            if not meta.get("grain"):
                gaps.append(f"{name}: no grain")
            lines.append(
                f"| [`{name}`](#{name}) | {layer} "
                f"| {MATERIALISED[node['config']['materialized']]} "
                f"| {cell(meta.get('grain', ''))} | {counts[name]:,} |"
            )

    lines += [
        "",
        "## Where it comes from",
        "",
        f"**`{source['source_name']}.{source['name']}`** is the landing zone.",
        one_line(source["description"]),
        "",
        "On a laptop it is the poller's files, read straight off the disk:",
        "`data/raw/dt=YYYY-MM-DD/hour=HH/{feed}-{observed_at}.ndjson.gz`. On",
        "BigQuery it is a table partitioned by observation date.",
        "",
        "| Column | Meaning |",
        "| --- | --- |",
    ]
    for name, column in source["columns"].items():
        lines.append(f"| `{name}` | {cell(column['description'])} |")

    for layer, blurb, names in LAYERS:
        lines += ["", f"## {layer}", "", blurb]
        for name in names:
            node = models[name]
            meta = node.get("meta") or node.get("config", {}).get("meta", {})
            parents = sorted(
                parent.split(".")[-1]
                for parent in node["depends_on"]["nodes"]
                if parent.split(".")[0] in ("model", "seed", "source")
            )
            if not one_line(node["description"]):
                gaps.append(f"{name}: no description")

            lines += [
                "",
                f"### {name}",
                "",
                one_line(node["description"]),
                "",
                f"- **One row is:** {one_line(meta.get('grain', ''))}",
                f"- **Kept up to date:** {one_line(meta.get('refresh', ''))}",
                "- **Built from:** " + ", ".join(f"`{p}`" for p in parents),
                f"- **Guarded by:** {tests.get(name, 0)} dbt tests",
                f"- **Rows in the sample:** {counts[name]:,}",
                "",
                "| Column | Type | Meaning |",
                "| --- | --- | --- |",
            ]

            described = node["columns"]
            for column, kind in connection.execute(
                "select column_name, data_type from information_schema.columns"
                " where table_name = ? order by ordinal_position",
                [name],
            ).fetchall():
                description = one_line(described.get(column, {}).get("description", ""))
                if not description:
                    gaps.append(f"{name}.{column}: no description")
                lines.append(f"| `{column}` | {kind.lower()} | {cell(description)} |")

            for column in described:
                exists = connection.execute(
                    "select count(*) from information_schema.columns"
                    " where table_name = ? and column_name = ?",
                    [name, column],
                ).fetchone()[0]
                if not exists:
                    gaps.append(f"{name}.{column}: described, but not in the table")

    connection.close()
    return "\n".join(lines) + "\n", gaps


def main(argv: list[str]) -> int:
    arguments = [a for a in argv[1:] if not a.startswith("--")]
    check = "--check" in argv
    warehouse = Path(arguments[0] if arguments else "data/demo.duckdb")
    if not warehouse.exists():
        print(f"No warehouse at {warehouse}. Run `python run.py demo` first.")
        return 1

    text, gaps = build(warehouse)

    if gaps:
        print("The catalog has gaps:\n")
        for gap in gaps:
            print(f"  {gap}")
        print("\nDescribe them in the model's .yml file.")
        return 1

    if check:
        print("Catalog complete: every table and every column is described.")
        return 0

    OUTPUT.write_text(text, encoding="utf-8", newline="\n")
    print(f"Wrote {OUTPUT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
