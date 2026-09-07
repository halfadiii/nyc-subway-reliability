# NYC subway reliability pipeline

**The MTA never publishes an arrival. This pipeline infers one.**

The agency's realtime feeds say what it currently *believes*: for every trip in
service, the stops ahead of it and the times it expects to reach them. There is
no event anywhere in that stream saying *train A-1042 reached Jay St at
08:14:32*. A train that arrives simply stops appearing, and the next snapshot is
quietly shorter than the last.

Every reliability question a rider actually has — how long will I wait, is this
line worse in the rain, is the evening worse than the morning — sits downstream
of an event the source never emits.

So: poll all eight feeds every thirty seconds, keep every snapshot, and read the
arrival out of what disappeared between them.

---

## Run it in five minutes

A sample of real observations is committed, so a clone builds the whole
warehouse with no cloud account, no credentials and no waiting.

```bash
python -m venv .venv
.venv/Scripts/activate          # Windows;  source .venv/bin/activate elsewhere
pip install -r requirements.txt

python run.py demo
```

That decodes the sample, runs every model, runs every test, and prints what it
built. Then, to collect your own:

```bash
python run.py poll        # eight feeds, every 30s, Ctrl-C when you have had enough
python run.py build       # rebuild the warehouse from everything collected
python run.py test        # 56 dbt checks + 8 correctness tests
```

`python run.py` on its own lists every task. There is a `Makefile` with the same
targets for anyone who prefers it; `run.py` exists because Windows does not ship
`make`, and "install make first" is a poor first line for a repository whose
pitch is that it runs from a clone.

There is no API key. The subway realtime feeds are open.

---

## What it does, in order

| Stage | Where | What happens |
| --- | --- | --- |
| **Poll** | `ingest/poller.py` | Eight endpoints, concurrently, every 30s. Retries, per-feed isolation, atomic writes. |
| **Land** | `data/raw/dt=…/hour=…` | Gzipped NDJSON, Hive-partitioned. Append-only: nothing deduplicated or corrected on the way in. |
| **Stage** | `stg_stop_time_updates` | Types, names, and the two identifiers the feed hides inside strings. |
| **Infer** | `int_inferred_arrivals` | The arrival, reconstructed from an absence. Three rules; see below. |
| **Model** | `fct_arrivals` → `fct_headways` → `fct_excess_wait` | Star schema, with `dim_stations` and `dim_routes` from the published static bundle. |
| **Analyse** | `analysis/rain_regression.py` | Excess wait against hourly rainfall, per route, with confidence intervals. |

The same models run on **DuckDB** (locally, off the files, no credentials) and
on **BigQuery** (`--target bigquery`). Every one of them is written with dbt's
cross-database macros rather than either dialect's own functions, so there is one
definition of an arrival and not two.

---

## The inference, which is the whole project

An arrival is *the last prediction that existed before the record vanished*.
Three rules decide whether a disappearance counts, and the middle one is the one
that is easy to leave out.

**1. Take the last observation of each trip–stop pair.** A window function,
partitioned by service date, trip and stop.

**2. Require that it actually vanished.** In the most recent snapshot, every
prediction is trivially its own last observation. Without this rule the pipeline
reports arrivals for trains that have not got there yet — and does it hardest at
the leading edge of the data, which is exactly where anyone reading a dashboard
is looking. The watermark is per feed, because the eight endpoints regenerate
independently.

**3. Require that it was about to happen.** A pair also disappears when a trip is
cancelled, re-routed or short-turned, and counting those as arrivals flatters
the service picture in precisely the situations riders care about most. A
prediction that vanishes while still more than `arrival_grace_seconds` (120s) in
the future was not an arrival.

That third rule is a threshold, and thresholds are the weakest thing in any
pipeline. So it is a project variable, and `run.py sensitivity` rebuilds the
marts across a range of it and reports how far the headline numbers move.
Measured on a 150-minute run of all eight feeds — 1.6M observations:

| grace | arrivals | headways | route-hours | mean headway | excess wait |
| --- | --- | --- | --- | --- | --- |
| 30s | 9,019 | 7,475 | 1,644 | 19.21 | 0.55 |
| 60s | 9,224 | 7,673 | 1,698 | 19.16 | 0.54 |
| **120s** | **9,344** | **7,789** | **1,728** | **19.13** | **0.54** |
| 240s | 9,401 | 7,845 | 1,744 | 19.09 | 0.54 |
| 480s | 9,441 | 7,884 | 1,749 | 19.05 | 0.53 |

**A sixteenfold change in the threshold moves excess wait by 3.8%.** The rule is
not doing much work, and the answer rests on the data rather than on the
judgement call in the middle of the pipeline. That is the sort of thing worth
knowing before quoting a number, and it is why the sweep exists.

It also earned its keep immediately: the sweep failed at 240s because a dbt test
had `120` typed into it rather than reading the variable. A test that hardcodes
the thing it is checking only passes on the default.

---

## Why excess wait, and not the average gap

The intuitive metric is the mean headway, and it is the wrong one. Riders do not
turn up at evenly spaced moments chosen to match the timetable; they arrive
roughly at random, so they are more likely to walk into a long gap than a short
one — a twelve-minute gap catches three times as many people as a four-minute
gap, just by being three times as long.

The wait a rider actually experiences is therefore the length-weighted mean:

```
E[wait] = E[H²] / (2·E[H])        against E[H]/2 for an evenly spread service
```

The difference is **excess wait time**. Worked, because the formula is easy to
nod along to and not see:

| Six trains an hour | Gaps | Mean headway | Rider waits | Excess |
| --- | --- | --- | --- | --- |
| Evenly | 10, 10, 10, 10, 10, 10 | 10.0 | 5.0 | **0.0** |
| Bunched | 2, 18, 2, 18, 2, 18 | 10.0 | 8.2 | **3.2** |

The timetable is being met in both rows. Only the second metric notices that
half the riders are standing on a platform for eighteen minutes. `tests/` asserts
this arithmetic against a hand-worked fixture, so the formula cannot drift.

---

## What the tests actually prove

On real feed data there is no ground truth: nobody publishes when the train
arrived, which is the reason this project exists. So the correctness tests build
synthetic snapshots where the answer is known by construction, point dbt at
them, run the real models, and assert on the marts.

```
tests/test_pipeline.py
  a train that arrives                    -> is an arrival
  a train cancelled ten minutes out       -> is not
  a train still in the newest snapshot    -> is not
  the arrival time is the last prediction before it vanished
  gaps of 2, 18, 2, 18, 2 minutes         -> excess wait of 3.657 min, by hand
  an even service                         -> excess wait of exactly zero
  stops the timetable omits               -> kept, flagged, join still holds
```

Plus 56 dbt checks on every run: uniqueness on the real compound grain,
not-null, referential integrity from every fact to both dimensions, and four
singular tests including one asserting that excess wait can never be negative
— which is a statement about arithmetic (it is a variance) rather than a hope
about the data.

---

## A thing the data turned out to say

The realtime feed refers to places the published timetable has never heard of:
ids like `H05`, `H17`, `H18`, `R60`, `R65`. The static bundle carries `H01`–`H15`
and stops at `R45`. These are yard leads, relay tracks and non-revenue movements
— real places a train genuinely goes, and places no rider can board.

That is a choice, not a nuisance. Dropping those arrivals would mean deleting
observations because a *different* published file is incomplete, which is
backwards: the feed is the measurement and the bundle is the reference.
Downgrading the referential test to a warning would leave the facts pointing at
a dimension that does not describe them, and a foreign key allowed to dangle is
not a foreign key.

So `dim_stations` is the union of both, with `is_published` saying which.
Referential integrity holds by construction and the gap between the two sources
is visible *as data* — countable, joinable — rather than hidden behind a
silenced test. Anything reporting to riders filters on `is_published`; anything
measuring the fleet does not.

---

## The rain question, and why it usually refuses to answer

`analysis/rain_regression.py` joins hourly Central Park precipitation to excess
wait and fits, per route:

```
excess_wait_minutes ~ precip_mm + C(hour_of_day)
```

with HC3 standard errors, reporting the coefficient next to its 95% interval and
its sample size. Hour of day is controlled for because both sides move with it,
and without it an afternoon storm season shows up as "rain makes trains late"
when it actually shows "afternoons".

**It declines to report** when there are fewer than 60 route-hours, fewer than 8
wet hours, or no variation in rainfall — and says which test failed. That is the
point of the script rather than a limitation of it. New York gets rain about one
hour in ten, so a pipeline that has been running for an afternoon has seen none,
and a slope fitted through a regressor that is zero everywhere produces a number,
an interval, and no information. The failure mode being guarded against is not a
crash; it is a plausible-looking table.

**An interval that contains zero is reported as a result**, not as a missing one.

---

## Layout

```
ingest/
  feeds.py          the eight endpoints
  explore.py        fetch one feed and print it            (start here)
  watch.py          watch one train's predictions vanish   (then here)
  poller.py         the real thing: 8 feeds, 30s, resilient
  schema.py         the one flat row this pipeline lands
  static_gtfs.py    station names and route colours, as dbt seeds
  com/              generated protobuf bindings (build output — see below)
transform/          the dbt project: staging → intermediate → marts
analysis/
  weather.py        hourly Central Park rainfall, cached
  rain_regression.py
  sensitivity.py    how much the cancellation threshold moves the answer
load/bigquery/      DDL and a loader for the BigQuery path
tests/              the correctness tests described above
deploy/             Dockerfile for the poller
docs/               architecture, deployment, and the original step-by-step
data/sample/        real observations, committed, so `make demo` works
```

`run.py` and the `Makefile` are the two entry points; everything else is
below.

`ingest/com/` is generated by `protoc` from the `.proto` files in
`ingest/proto/`. Never edit it by hand; regenerate with:

```bash
cd ingest && python -m grpc_tools.protoc -I=proto --python_out=. \
  proto/com/google/transit/realtime/gtfs-realtime.proto \
  proto/com/google/transit/realtime/gtfs-realtime-NYCT.proto
```

There is a version trap there worth knowing about before it costs you an hour;
`docs/architecture.md` has it.

---

## State of things, honestly

**Working and verified here.** A 150-minute continuous run of all eight feeds:
300 rounds, **1,600,014 rows, zero failures** across 2,400 fetches and zero
duplicate snapshots. That built to 9,344 inferred arrivals, 7,789 headways and
3,408 route-hours of excess wait, with all 56 dbt checks and all 8 correctness
tests passing, in 15 seconds.

The worst hour it found, on overnight service: the A at 59 St–Columbus Circle,
mean headway 20.6 minutes and **8.9 minutes of excess wait** — riders waiting
half again as long as an evenly spread service of the same frequency would ask
of them.

**Written but not exercised against a live project:** the BigQuery path
(`load/bigquery/`). The DDL and the loader are here and `--dry-run` works, but
they have not been run against a real dataset, because doing that needs a GCP
project and credentials that do not belong in this repository. The dbt models
themselves are dialect-portable and tested on DuckDB.

**Needs time rather than code:** the rain regression. It is finished; it is
waiting on history. It needs weeks of observations before it can say anything,
and it will keep refusing until it has them.

**Not yet running continuously.** The poller is built to run for weeks and
containerised for it (`deploy/`), but nothing is hosting it yet, and history
only accumulates while it is up. That is the next thing to do, and
`docs/deploying.md` sets out the options and the one real constraint: a
thirty-second cadence rules out Cloud Scheduler, whose floor is a minute.
