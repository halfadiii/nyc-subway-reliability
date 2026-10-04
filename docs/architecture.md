# Architecture

The shape of the thing, and the handful of decisions that were not obvious.

```
  MTA GTFS-realtime  ──►  poller.py  ──►  data/raw/dt=…/hour=…/*.ndjson.gz
   8 endpoints, ~30s        8 threads          append-only, Hive-partitioned
                                                        │
                        ┌───────────────────────────────┴─────────┐
                        ▼                                         ▼
                 DuckDB (local)                            BigQuery (cloud)
                 read_json_auto                            bq load, partitioned
                        │                                         │
                        └───────────────► dbt ◄───────────────────┘
                                           │
     stg_stop_time_updates  →  int_last_sightings  →  int_inferred_arrivals
          (incremental)          (incremental)              (view)
                                           │
                    fct_arrivals  →  fct_headways  →  fct_excess_wait
                          ▲                                │
                dim_stations, dim_routes                   ▼
                 (static GTFS seeds)              analysis/rain_regression.py
                                                   + hourly Central Park rain
```

## The two timestamps

Every landed row carries both `observed_at` and `fetched_at`, and they are not
interchangeable.

`observed_at` is the **feed header's** timestamp — the instant the MTA says it
generated this picture. It is the event time. Every partition, every window
function and every hour bucket uses it.

`fetched_at` is **our** clock, when the bytes arrived. It is never the event
time: our clock can drift, a poll can be late, a retry can put minutes between
the two. It is kept for exactly one job, which is measuring the gap. A GTFS
endpoint that has silently stopped regenerating keeps answering requests with a
stale snapshot and nothing in the response says so — the only evidence is
`fetched_at - observed_at`, which is landed as `feed_lag_seconds` and asserted
on by `tests/assert_feed_lag_is_sane.sql`.

## Why the landing zone is append-only

Nothing is deduplicated, filtered or corrected on the way in. A transformation
bug downstream is a query to rewrite; an observation dropped on the way in is
gone for good. Every decision about what counts is made in dbt, where it is
version-controlled, tested, and visible in a diff.

The one exception is at the edge of that principle and worth naming: the poller
skips writing a snapshot whose feed header timestamp it has already written.
The MTA answers every request whether or not it has regenerated, so consecutive
polls frequently return an identical snapshot. That is bytes for nothing rather
than information, and duplicates would not change the inference anyway — it
takes the *last* observation. The count is reported at the end of every run.

## Why staging is a table and everything else in that layer would be a view

Staging is materialised, which contradicts the usual advice, for two reasons
that both come from the source being files rather than a table.

A view over a glob re-reads and re-decompresses every file on every downstream
query. And its relative path is resolved by whichever process opens the
database, not by dbt — so a view works while dbt's working directory is
`transform/` and fails for every script in `analysis/` and every ad-hoc query
anyone ever runs. Materialising reads the files once, at build time, in the one
place the relative path is known to be right.

## What is incremental, and what is not

Two models read only what is new: `stg_stop_time_updates` and
`int_last_sightings`. They are the two that touch every observation. Staging
decompresses and deduplicates the landing files; the last-sightings model sorts
every observation of every trip-stop pair to find the final one. Both of those
costs grow with the whole history when rebuilt, and with the last few hours
when not.

Everything downstream is still rebuilt in full, on purpose. `fct_arrivals`,
`fct_headways` and `fct_excess_wait` read the arrivals, not the observations,
and there are about 170 observations for every arrival. Making them incremental
would add state to three more models to save a fraction of a second.

The rule that makes the incremental model correct is which pairs it revisits,
and the model's own comment sets it out. The short version: a pair's last
sighting is final once it has been absent from a later snapshot, so only pairs
seen at or after the previous run's watermark can change. That includes pairs
in the newest snapshot of the previous run, which become arrivals without being
observed again.

The cancellation threshold stays out of the stored table and is applied in the
`int_inferred_arrivals` view. It is the one number in the pipeline that is
varied on purpose, and a stored verdict would be stale the moment it was.

`tests/test_incremental.py` holds the claim that matters: built in several
sittings or in one pass, the warehouse is the same.

## The protobuf version trap

This one will cost an hour if nobody writes it down.

`dbt-adapters` and `dbt-common` both pin `protobuf<7`. `grpcio-tools` 1.83 —
which is what `pip install grpcio-tools` gives you — requires `protobuf>=7.35`.
They cannot coexist in one environment.

Generated protobuf bindings carry the version of the compiler that made them and
refuse to load on an older runtime:

```
VersionError: Detected incompatible Protobuf Gencode/Runtime versions …
gencode 7.35.1 runtime 6.33.6
```

So the bindings in `ingest/com/` are compiled with **grpcio-tools 1.76.0**,
whose `protoc` emits gencode the 6.x runtime accepts, and `requirements.txt`
pins it. If you raise either half, regenerate the bindings in the same commit.

The alternative — two virtualenvs, one for ingest and one for transform — is
defensible and was rejected: the bindings are build output, regenerating them is
one documented command, and one environment is one thing for a reader to set up.

There is a second, older trap that `docs/STEP1.md` already describes: do not
`pip install gtfs-realtime-bindings`. It registers the same protobuf schema
under a different module path and importing both crashes on a duplicate symbol.

## Why the models are dialect-portable

Everything is written with dbt's cross-database macros (`dbt.datediff`,
`dbt.dateadd`, `dbt.date_trunc`, `dbt.safe_cast`) plus a handful of dispatched
macros in `transform/macros/portable.sql` for the places BigQuery and DuckDB
genuinely disagree — epoch conversion, timezone conversion, and `split_part`,
which BigQuery does not have.

The alternative is two copies of the SQL, and two copies of the SQL means two
definitions of an arrival, and the second one is wrong within a month. It also
means the local target is a real test of the production logic rather than an
approximation of it.

## The star schema

`fct_arrivals` is the grain everything aggregates from: one row per trip, per
stop, per service date. `fct_headways` and `fct_excess_wait` are derived from it
rather than re-deriving arrivals, so there is one place an arrival is decided.

Dimensions come from the MTA's published static bundle, committed as dbt seeds
(`transform/seeds/`) so a clone can build with no network. `dim_stations` is a
union of the published stations and the ones the realtime feed actually
referenced — see the model, and the README, for why.

Platforms and stations are deliberately different things. `127N` and `127S` are
the two directions of Times Square; a headway is per *platform*, because a
northbound gap and a southbound gap are two different waits for two different
riders, and interleaving them produces a headway nobody experienced.
