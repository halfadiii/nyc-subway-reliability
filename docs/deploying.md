# Running it somewhere that is not a laptop

History only accumulates while the poller is up, so this is the difference
between a project that works and a project that has data.

## The one real constraint

**A thirty-second cadence rules out Cloud Scheduler.** Its floor is one minute,
and so is cron's. Halving the poll rate is not a free trade: a train that stops
at a platform and pulls out inside one interval never appears as a change
between snapshots, and its arrival is simply not observed. Thirty seconds is the
rate the MTA regenerates at, which is the fastest rate at which the question can
be answered honestly, and the slowest at which it can be answered completely.

So the poller is a **long-running process**, not a scheduled job. Everything
below follows from that.

## Option A — a small always-on VM (recommended)

The cheapest and least surprising thing that works. An `e2-micro` sits inside
GCP's free tier in most regions and is far more machine than this needs: the
poller uses a few MB of memory and is idle 95% of the time.

```bash
gcloud compute instances create mta-poller \
  --machine-type=e2-micro --boot-disk-size=30GB \
  --image-family=debian-12 --image-project=debian-cloud \
  --scopes=storage-rw
```

Run it under systemd so it comes back after a reboot and restarts if it dies:

```ini
# /etc/systemd/system/mta-poller.service
[Unit]
Description=MTA GTFS-realtime poller
After=network-online.target

[Service]
Type=simple
User=mta
WorkingDirectory=/opt/mta-reliability/ingest
Environment=MTA_OUT=/var/lib/mta/raw
ExecStart=/opt/mta-reliability/.venv/bin/python poller.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

`Restart=always` matters more than it looks. The failure this guards against is
not a crash — the poller catches its own network errors — it is the process
being killed by something outside it, and a poller that is down is a gap in the
history that can never be backfilled, because the feed only ever publishes
*now*.

## Option B — Cloud Run service with a minimum instance

Works, and costs more than the VM for this workload, because a Cloud Run
instance that never scales to zero is a VM you are renting through a more
expensive abstraction. Worth it only if everything else you run is already
there.

```bash
gcloud run deploy mta-poller --source . \
  --min-instances=1 --max-instances=1 --no-cpu-throttling
```

`--no-cpu-throttling` is not optional. Cloud Run throttles CPU outside a request
by default, and a background loop with no inbound traffic simply stops between
polls.

## Option C — Cloud Run *job*, every ten minutes, polling for ten

A job triggered by Scheduler every ten minutes that polls internally for ten
minutes (`poller.py --minutes 10`). It keeps the thirty-second cadence inside
each run and scales to zero between them.

The catch is the seam: two runs overlap or leave a gap depending on how long the
container takes to start, so a few snapshots are lost or duplicated at every
boundary. Duplicates are harmless — the inference takes the last observation —
but the gaps are not, and there are 144 of them a day.

## Getting the rows somewhere durable

Whichever option, the container's own filesystem is not storage. Either mount a
disk, or sync to object storage on a timer:

```bash
gsutil -m rsync -r /var/lib/mta/raw gs://$BUCKET/raw
```

Then load into BigQuery from there. `load/bigquery/load_bq.py` reads local
files; pointing it at a GCS prefix instead is a small change and the natural
next step.

## The daily transform

Unlike the poller, this genuinely is a scheduled job — once a day, after
midnight New York time, over the service day that just closed.

```bash
cd transform && dbt build --profiles-dir . --target bigquery
```

Cloud Run job plus Cloud Scheduler, or GitHub Actions on a cron. A failing test
stops the run rather than publishing a quietly wrong number, which is the whole
reason the tests are in the build rather than beside it.

## Costs, roughly

At eight feeds every thirty seconds the poller lands on the order of 100M rows a
month, which is a few GB compressed. BigQuery storage on that is small; the
thing to watch is query cost, which is why the landing table is partitioned by
date, clustered on route and stop, and declared `require_partition_filter =
true` — so a query that would scan the whole history fails instead of quietly
costing money.
