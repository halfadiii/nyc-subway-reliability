-- The landing table.
--
-- Append-only, and shaped so that the two things this warehouse does often are
-- the two things it does cheaply.
--
-- PARTITION BY the observation date, because every query is about a day or a
-- range of days and partition pruning is what stops a reconstruction reading
-- the entire history. At roughly five thousand rows per feed per snapshot,
-- eight feeds, every thirty seconds, this table gains something like a hundred
-- million rows a month; a full scan of it to rebuild one afternoon is the
-- difference between a query that costs nothing and a query that costs real
-- money.
--
-- CLUSTER BY route and stop, because the inference and every mart downstream
-- group by exactly those. Clustering co-locates them inside the partition, so
-- the window function reads the blocks it needs instead of the day.
--
-- The partition column is derived from `observed_at`, the *feed's* clock, not
-- from `fetched_at`. A row belongs to the hour the MTA believed it, which is
-- the hour every question is about. Filing by our own clock would smear a
-- retry across a partition boundary.

create schema if not exists `${BQ_PROJECT}.${BQ_DATASET}_raw`
  options (location = '${BQ_LOCATION}');

create table if not exists `${BQ_PROJECT}.${BQ_DATASET}_raw.stop_time_updates`
(
  feed_id               string    not null options (description = 'Which of the eight endpoints this came from.'),
  observed_at           timestamp not null options (description = 'Feed header timestamp. The event time.'),
  fetched_at            timestamp not null options (description = 'Our clock when the bytes landed. Never the event time; used only to monitor staleness.'),
  trip_id               string             options (description = 'MTA trip identifier, unique within a service date.'),
  route_id              string             options (description = 'Route, e.g. A. Joins to dim_routes.'),
  start_date            string             options (description = 'Service date the trip belongs to, YYYYMMDD.'),
  direction             string             options (description = 'NORTH/EAST/SOUTH/WEST, from the NYCT extension.'),
  train_id              string             options (description = 'NYCT internal train identifier.'),
  is_assigned           bool               options (description = 'Whether the trip has been assigned to physical equipment.'),
  stop_id               string             options (description = 'A single platform, e.g. 127N. Not a station.'),
  stop_sequence         int64              options (description = 'Position in the remaining-stops list at this observation.'),
  predicted_arrival     timestamp          options (description = 'What the MTA believed, at observed_at, about reaching this stop.'),
  predicted_departure   timestamp,
  scheduled_track       string,
  actual_track          string,
  schedule_relationship string             options (description = 'SCHEDULED / SKIPPED / NO_DATA / UNSCHEDULED.')
)
partition by date(observed_at)
cluster by route_id, stop_id
options (
  description = 'Flattened GTFS-realtime stop-time updates, append-only. Nothing here is deduplicated or corrected; that happens in dbt, where it is visible.',
  -- Nothing. The whole point of an append-only landing table is that it can be
  -- replayed, and an expiry silently turns "replay from the beginning" into
  -- "replay from ninety days ago" the first time anyone needs it.
  require_partition_filter = true
);
