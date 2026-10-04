# Data catalog

Every table in the warehouse: what one row is, how it is kept up to
date, what it is built from, and what each column means.

**Generated, not written.** `python run.py catalog` assembles this from
the dbt project and the warehouse it built, so it cannot describe a
column that does not exist or miss one that does. CI fails if any
column is left without a description. Row counts are from the
committed sample (`python run.py demo`): three feeds, 150 minutes.

## At a glance

| Table | Layer | Kept up to date by | One row is | Rows in the sample |
| --- | --- | --- | --- | --- |
| [`stg_stop_time_updates`](#stg_stop_time_updates) | Staging | Incremental table | One row per feed, per snapshot, per trip, per stop still ahead of it. | 385,275 |
| [`int_last_sightings`](#int_last_sightings) | Intermediate | Incremental table | One row per service date, per trip, per stop. | 3,407 |
| [`int_inferred_arrivals`](#int_inferred_arrivals) | Intermediate | View | One row per service date, per trip, per stop that the train reached. | 2,075 |
| [`fct_arrivals`](#fct_arrivals) | Marts | Table | One row per service date, per trip, per stop. | 2,075 |
| [`fct_headways`](#fct_headways) | Marts | Table | One row per arrival that has a previous arrival on the same route and platform. | 1,710 |
| [`fct_excess_wait`](#fct_excess_wait) | Marts | Table | One row per service date, per route, per platform, per hour. | 773 |
| [`dim_stations`](#dim_stations) | Marts | Table | One row per station. | 501 |
| [`dim_routes`](#dim_routes) | Marts | Table | One row per route. | 29 |

## Where it comes from

**`raw.stop_time_updates`** is the landing zone.
Flattened GTFS-realtime stop-time updates from all eight feeds.

On a laptop it is the poller's files, read straight off the disk:
`data/raw/dt=YYYY-MM-DD/hour=HH/{feed}-{observed_at}.ndjson.gz`. On
BigQuery it is a table partitioned by observation date.

| Column | Meaning |
| --- | --- |
| `feed_id` | Which of the eight endpoints this snapshot came from. |
| `observed_at` | The feed header's own timestamp, in epoch seconds. The event time. Every question this warehouse answers is asked in this clock. |
| `fetched_at` | Our clock, when the bytes landed. Never the event time; kept only so the gap between the two can be monitored, because a feed that has stopped being refreshed still answers requests. |
| `trip_id` | MTA trip identifier, unique within a service date. |
| `stop_id` | A single platform, e.g. 127N. Not a station. |
| `predicted_arrival` | What the MTA believed, at observed_at, about when this trip would reach this stop. A prediction, never an arrival. |

## Staging

The landing rows, made usable.

### stg_stop_time_updates

The landing rows with their columns made usable: types cast, the two identifiers the feed hides inside strings pulled out, and exact duplicates removed on the natural key. Nothing is filtered -- what counts as an arrival is decided one layer up, on a stated rule, where the decision is visible in a diff. Incremental: a run reads only the landing files newer than what it already holds, less a lookback for late files.

- **One row is:** One row per feed, per snapshot, per trip, per stop still ahead of it.
- **Kept up to date:** Incremental. Every build adds what landed since the last one.
- **Built from:** `stop_time_updates`
- **Guarded by:** 13 dbt tests
- **Rows in the sample:** 385,275

| Column | Type | Meaning |
| --- | --- | --- |
| `feed_id` | varchar | Which of the eight MTA endpoints this snapshot came from. |
| `trip_id` | varchar | MTA trip identifier, unique within a service date. Part of the natural key the incremental load replaces rows on, which is why it may not be null: a null never equals itself, so a row without one would be loaded again on every run. |
| `stop_id` | varchar | A single platform, e.g. 127N. Not a station. |
| `station_id` | varchar | The stop id with its direction letter removed. |
| `platform_direction` | varchar | N or S, from the last letter of the stop id. Null where it carries none. |
| `route_id` | varchar | The route, e.g. A. Joins to dim_routes. |
| `service_date_raw` | varchar | The service date the trip belongs to, YYYYMMDD, as the feed states it. Not the calendar date of the observation: a train leaving at 00:40 belongs to the previous service day. |
| `observed_at` | timestamp with time zone | The feed's own clock. The event time. |
| `fetched_at` | timestamp with time zone | Our clock. Never the event time. |
| `predicted_arrival` | timestamp with time zone | What the MTA believed, at observed_at, about when this trip would reach this stop. A prediction, never an arrival. |
| `predicted_departure` | timestamp with time zone | The same, for leaving the stop. |
| `feed_lag_seconds` | bigint | fetched_at minus observed_at. The only evidence that a feed has stopped regenerating, since a stale endpoint still answers. |
| `direction` | varchar | NORTH, SOUTH, EAST or WEST, from the MTA's extension to the feed. |
| `train_id` | varchar | The MTA's internal identifier for the physical train. |
| `is_assigned` | boolean | Whether the trip has been assigned to physical equipment yet. |
| `stop_sequence` | bigint | Position in the trip's list of remaining stops at this observation. |
| `scheduled_track` | varchar | The track the timetable has the train on at this stop. |
| `actual_track` | varchar | The track it is actually routed to, where the feed says. |
| `schedule_relationship` | varchar | SCHEDULED, SKIPPED, NO_DATA or UNSCHEDULED. Only SCHEDULED rows can become arrivals. |
| `trip_origin_minutes` | double | The trip's departure from its origin, in minutes after midnight, decoded from the trip id. The only human-stable name a trip has. |

## Intermediate

Where an arrival is decided.

### int_last_sightings

The last time each trip-stop pair was seen, and whether it has since gone. One row per pair whatever became of it: arrived, cancelled, or still on its way. Incremental: a run recomputes only the pairs seen since the previous run had watched their feed to, and tests/test_incremental.py proves the result equals a full rebuild.

- **One row is:** One row per service date, per trip, per stop.
- **Kept up to date:** Incremental. Only pairs whose answer could have changed are recomputed.
- **Built from:** `stg_stop_time_updates`
- **Guarded by:** 10 dbt tests
- **Rows in the sample:** 3,407

| Column | Type | Meaning |
| --- | --- | --- |
| `service_date_raw` | varchar | The service date the trip belongs to, YYYYMMDD. |
| `feed_id` | varchar | The endpoint the pair was observed on. |
| `route_id` | varchar | The route, e.g. A. |
| `trip_id` | varchar | MTA trip identifier, unique within a service date. |
| `train_id` | varchar | The MTA's internal identifier for the physical train. |
| `stop_id` | varchar | A single platform, e.g. 127N. |
| `station_id` | varchar | The stop id with its direction letter removed. |
| `platform_direction` | varchar | N or S, from the stop id. |
| `direction` | varchar | NORTH, SOUTH, EAST or WEST, from the MTA's extension. |
| `last_predicted_arrival` | timestamp with time zone | The prediction carried by the last observation of the pair. If the pair went on to vanish close to this time, this is the arrival. |
| `last_seen_at` | timestamp with time zone | When that last observation was made, on the feed's clock. |
| `lead_seconds` | bigint | How far ahead the prediction still was at the last sighting. Not bounded here: a pulled trip can be many minutes out. The cancellation threshold is applied in int_inferred_arrivals. |
| `observations_of_pair` | bigint | How many snapshots carried this pair, over its whole history. |
| `scheduled_track` | varchar | The track the timetable had the train on. |
| `actual_track` | varchar | The track it was actually routed to. |
| `has_vanished` | boolean | True once the pair has been absent from a later snapshot of its own feed. False means it was in the newest one: it has not disappeared, we stopped looking. |

### int_inferred_arrivals

The measurement the project exists for: an arrival reconstructed from the absence of a record. Three rules decide whether a disappearance counts; they are set out in the model and proved in tests/test_pipeline.py against fixtures whose answer is known by construction.

- **One row is:** One row per service date, per trip, per stop that the train reached.
- **Kept up to date:** A view. Always current with int_last_sightings.
- **Built from:** `int_last_sightings`
- **Guarded by:** 6 dbt tests
- **Rows in the sample:** 2,075

| Column | Type | Meaning |
| --- | --- | --- |
| `service_date_raw` | varchar | The service date the trip belongs to, YYYYMMDD. |
| `feed_id` | varchar | The endpoint the arrival was observed on. |
| `route_id` | varchar | The route, e.g. A. |
| `trip_id` | varchar | MTA trip identifier, unique within a service date. |
| `train_id` | varchar | The MTA's internal identifier for the physical train. |
| `stop_id` | varchar | A single platform, e.g. 127N. |
| `station_id` | varchar | The stop id with its direction letter removed. |
| `platform_direction` | varchar | N or S, from the stop id. |
| `direction` | varchar | NORTH, SOUTH, EAST or WEST, from the MTA's extension. |
| `inferred_arrival` | timestamp with time zone | The last prediction that existed before the record vanished. |
| `last_seen_at` | timestamp with time zone | The observation that prediction came from. |
| `lead_seconds` | bigint | How far ahead the arrival still was when the record went. Near zero is a train pulling in; negative is one already overdue, which still arrived. Bounded above by the cancellation rule. |
| `observations_of_pair` | bigint | How many snapshots carried this pair before it went. One is a weaker observation than twenty and worth being able to filter on. |
| `scheduled_track` | varchar | The track the timetable had the train on. |
| `actual_track` | varchar | The track it was actually routed to. |

## Marts

What analysis reads. A star: three facts, two dimensions.

### fct_arrivals

Every arrival the pipeline inferred. One row per trip, per stop, per service date. The grain everything downstream aggregates from.

- **One row is:** One row per service date, per trip, per stop.
- **Kept up to date:** Rebuilt in full from int_inferred_arrivals, which is a view over an incremental table.
- **Built from:** `int_inferred_arrivals`
- **Guarded by:** 11 dbt tests
- **Rows in the sample:** 2,075

| Column | Type | Meaning |
| --- | --- | --- |
| `service_date_raw` | varchar | The service date the trip belongs to, YYYYMMDD. |
| `feed_id` | varchar | The endpoint the arrival was observed on. |
| `route_id` | varchar | The route. Joins to dim_routes. |
| `trip_id` | varchar | MTA trip identifier, unique within a service date. |
| `train_id` | varchar | The MTA's internal identifier for the physical train. |
| `stop_id` | varchar | The platform the train arrived at. |
| `station_id` | varchar | Every arrival must land at a station that exists. |
| `platform_direction` | varchar | N or S, from the stop id. |
| `direction` | varchar | NORTH, SOUTH, EAST or WEST, from the MTA's extension. |
| `inferred_arrival` | timestamp with time zone | When the train arrived, as inferred. An instant, in UTC. |
| `arrival_local` | timestamp | The same instant as New York wall-clock time. |
| `service_hour` | timestamp | arrival_local truncated to the hour. What excess wait is grouped by. |
| `hour_of_day` | bigint | 0 to 23, New York time. |
| `day_of_week` | bigint | Day of the week of arrival_local, as the warehouse numbers it. |
| `last_seen_at` | timestamp with time zone | The observation the arrival was read from. |
| `lead_seconds` | bigint | How far ahead the arrival still was when the record vanished. |
| `observations_of_pair` | bigint | How many snapshots carried this trip at this stop before it went. |
| `scheduled_track` | varchar | The track the timetable had the train on. |
| `actual_track` | varchar | The track it was actually routed to. |

### fct_headways

The gap between consecutive trains at one platform on one route.

- **One row is:** One row per arrival that has a previous arrival on the same route and platform.
- **Kept up to date:** Rebuilt in full from fct_arrivals.
- **Built from:** `fct_arrivals`
- **Guarded by:** 5 dbt tests
- **Rows in the sample:** 1,710

| Column | Type | Meaning |
| --- | --- | --- |
| `service_date_raw` | varchar | The service date, YYYYMMDD. |
| `route_id` | varchar | The route. |
| `stop_id` | varchar | The platform. |
| `station_id` | varchar | The station the platform belongs to. |
| `platform_direction` | varchar | N or S. |
| `trip_id` | varchar | The trip that closed the gap. |
| `previous_trip_id` | varchar | The trip that opened it. |
| `previous_arrival` | timestamp with time zone | When the train before this one arrived. |
| `inferred_arrival` | timestamp with time zone | When this one arrived. |
| `arrival_local` | timestamp | The same instant as New York wall-clock time. |
| `service_hour` | timestamp | arrival_local truncated to the hour. |
| `hour_of_day` | bigint | 0 to 23, New York time. |
| `day_of_week` | bigint | Day of the week of arrival_local, as the warehouse numbers it. |
| `headway_minutes` | double | Bounded by the project variables. Outside them it is not a headway; see the model. |

### fct_excess_wait

Excess wait time by route, platform and hour. The metric that describes a bad commute; the mean headway does not.

- **One row is:** One row per service date, per route, per platform, per hour.
- **Kept up to date:** Rebuilt in full from fct_headways.
- **Built from:** `fct_headways`
- **Guarded by:** 6 dbt tests
- **Rows in the sample:** 773

| Column | Type | Meaning |
| --- | --- | --- |
| `service_date_raw` | varchar | The service date, YYYYMMDD. |
| `route_id` | varchar | The route. Joins to dim_routes. |
| `stop_id` | varchar | The platform. |
| `station_id` | varchar | The station the platform belongs to. |
| `platform_direction` | varchar | N or S. |
| `service_hour` | timestamp | The hour, New York time. |
| `hour_of_day` | bigint | 0 to 23, New York time. |
| `day_of_week` | bigint | Day of the week of the hour, as the warehouse numbers it. |
| `headways` | bigint | How many gaps the hour's figure is built from. An excess wait from two gaps is arithmetic, not a measurement -- filter on this. |
| `mean_headway_minutes` | double | The average gap between trains in the hour. |
| `min_headway_minutes` | double | The shortest gap in the hour. |
| `max_headway_minutes` | double | The longest gap in the hour. |
| `rider_wait_minutes` | double | The wait a rider turning up at random actually experiences: long gaps weighted by their length, because more people walk into them. |
| `even_wait_minutes` | double | What that wait would be if the same trains were evenly spread. |
| `excess_wait_minutes` | double | rider_wait_minutes minus even_wait_minutes. Zero on a regular service. |

### dim_stations

One row per station: everything the static bundle publishes, plus every station the realtime feed actually referenced. See the model for why the two are unioned rather than the second being dropped.

- **One row is:** One row per station.
- **Kept up to date:** Rebuilt in full on every build.
- **Built from:** `gtfs_stops`, `stg_stop_time_updates`
- **Guarded by:** 8 dbt tests
- **Rows in the sample:** 501

| Column | Type | Meaning |
| --- | --- | --- |
| `station_id` | varchar | Parent station id. Joins to `station_id` on every fact. |
| `station_name` | varchar | Null where `is_published` is false. Deliberately not filled in from the id: a label that looks like a station name and is not one is worse than a blank. |
| `latitude` | float | From the static bundle. Null where `is_published` is false. |
| `longitude` | float | From the static bundle. Null where `is_published` is false. |
| `is_published` | boolean | True where the static bundle carries this station. False for yard leads, relay tracks and other non-revenue places the feed moves trains through and the timetable does not name. Anything reported to riders should filter on this; anything measuring the fleet should not. |
| `platform_count` | bigint | How many platforms the static bundle lists under this station. |
| `observations` | bigint | How many landed rows referenced this station. |
| `routes_observed` | bigint | How many distinct routes were seen calling here. |

### dim_routes

One row per route, named and coloured from the static bundle.

- **One row is:** One row per route.
- **Kept up to date:** Rebuilt in full on every build.
- **Built from:** `gtfs_routes`, `stg_stop_time_updates`
- **Guarded by:** 5 dbt tests
- **Rows in the sample:** 29

| Column | Type | Meaning |
| --- | --- | --- |
| `route_id` | varchar | The route as the feed names it, e.g. A. |
| `route_short_name` | varchar | The letter or number on the front of the train. |
| `route_long_name` | varchar | The published name, e.g. 8 Avenue Express. |
| `route_color` | varchar | The MTA's colour for the line, as a hex code with its hash. |
| `route_text_color` | varchar | The colour of the text set on it, as a hex code with its hash. |
| `feed_id` | varchar | The endpoint the route was actually observed on. Derived from the data, not declared, so it reports a move rather than hiding one. |
| `trips_observed` | bigint | Distinct trips seen on the route. Zero if it was never observed. |
| `first_observed_at` | timestamp with time zone | The earliest observation of the route. |
| `last_observed_at` | timestamp with time zone | The latest observation of the route. |
