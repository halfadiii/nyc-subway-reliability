"""The one flat row this pipeline lands, and the code that makes one.

A GTFS-realtime feed is a tree: a snapshot holds entities, an entity holds a
trip update, a trip update holds a list of stop-time updates. Warehouses are
happier with rectangles, so this flattens the tree to its leaves -- one row per
*predicted stop*, carrying its trip and its snapshot down with it.

Nothing is corrected, filtered or deduplicated here. That is deliberate and it
is the whole reason the landing table can be trusted: a transformation bug
downstream is a query to rewrite, but an observation dropped on the way in is
gone for good. Everything that arrives is written.

## The two timestamps, and why there are two

`observed_at` is the feed header's own timestamp -- the instant the MTA says it
generated this picture of the system. It is the event time, and it is the one
the inference uses.

`fetched_at` is our clock, when the bytes actually landed here. It is not the
event time and must never be substituted for it: our clock can drift, our poll
can be late, and a retry can put minutes between the two. It is kept for one
job, which is measuring the gap between them. A feed that stops being refreshed
still answers requests, so `fetched_at - observed_at` is the only way to notice
that the MTA has quietly gone stale -- see `feed_lag_seconds`.
"""

from __future__ import annotations

from typing import Any, Iterator

from com.google.transit.realtime import gtfs_realtime_NYCT_pb2 as nyct

# `direction` in the NYCT extension is an enum; these are its four values.
DIRECTIONS = {1: "NORTH", 2: "EAST", 3: "SOUTH", 4: "WEST"}

# `schedule_relationship` on a stop-time update. SKIPPED and NO_DATA both mean
# "there will be no arrival here", which the inference has to be able to see.
STOP_RELATIONSHIP = {0: "SCHEDULED", 1: "SKIPPED", 2: "NO_DATA", 3: "UNSCHEDULED"}

# One row per predicted stop. Order matters: it is the column order of the
# BigQuery table in `load/bigquery/ddl.sql` and of the DuckDB view.
COLUMNS = (
    "feed_id",
    "observed_at",
    "fetched_at",
    "trip_id",
    "route_id",
    "start_date",
    "direction",
    "train_id",
    "is_assigned",
    "stop_id",
    "stop_sequence",
    "predicted_arrival",
    "predicted_departure",
    "scheduled_track",
    "actual_track",
    "schedule_relationship",
)


def _seconds(value: int) -> int | None:
    """Protobuf gives 0 for an absent int64. That is a real time, so map it out.

    Epoch 0 is 1970, which no train is arriving at. Left as 0 it would sail
    through every null check downstream and quietly become the earliest arrival
    in the warehouse.
    """
    return int(value) if value else None


def flatten(feed_id: str, message: Any, fetched_at: float) -> Iterator[dict]:
    """One decoded feed message -> one dict per predicted stop.

    A generator rather than a list: the numbered feed carries a few thousand of
    these per snapshot and they are written straight out to a file, so there is
    no reason for the whole snapshot to exist twice in memory.
    """
    observed_at = int(message.header.timestamp)
    fetched = int(fetched_at)

    for entity in message.entity:
        if not entity.HasField("trip_update"):
            continue

        update = entity.trip_update
        trip = update.trip
        # Every NYCT trip carries the extension. Guard anyway: a feed that
        # changes shape should cost us the extra columns, not the whole poll.
        try:
            ext = trip.Extensions[nyct.nyct_trip_descriptor]
            direction = DIRECTIONS.get(ext.direction)
            train_id = ext.train_id or None
            is_assigned = bool(ext.is_assigned)
        except Exception:  # noqa: BLE001 - see above
            direction, train_id, is_assigned = None, None, None

        for sequence, stop in enumerate(update.stop_time_update):
            try:
                stop_ext = stop.Extensions[nyct.nyct_stop_time_update]
                scheduled_track = stop_ext.scheduled_track or None
                actual_track = stop_ext.actual_track or None
            except Exception:  # noqa: BLE001
                scheduled_track, actual_track = None, None

            yield {
                "feed_id": feed_id,
                "observed_at": observed_at,
                "fetched_at": fetched,
                "trip_id": trip.trip_id or None,
                "route_id": trip.route_id or None,
                "start_date": trip.start_date or None,
                "direction": direction,
                "train_id": train_id,
                "is_assigned": is_assigned,
                "stop_id": stop.stop_id or None,
                # The feed's own `stop_sequence` is optional and usually unset
                # on NYCT, so this is the position in the remaining-stops list.
                # It is only ever used to keep a snapshot's stops in order.
                "stop_sequence": sequence,
                "predicted_arrival": _seconds(stop.arrival.time),
                "predicted_departure": _seconds(stop.departure.time),
                "scheduled_track": scheduled_track,
                "actual_track": actual_track,
                "schedule_relationship": STOP_RELATIONSHIP.get(
                    stop.schedule_relationship, "SCHEDULED"
                ),
            }


def feed_lag_seconds(row: dict) -> int:
    """How far behind our clock the MTA's picture was. See the module docstring."""
    return int(row["fetched_at"]) - int(row["observed_at"])
