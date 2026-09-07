"""STEP 1: first contact with the MTA realtime feed.

Fetches ONE feed, decodes the protobuf, and prints what's inside --
first a summary of the whole snapshot, then one train in detail.

Run it:      python explore.py ace
             python explore.py numbered
"""

import sys
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests

from feeds import feed_url
from com.google.transit.realtime import gtfs_realtime_pb2 as rt
from com.google.transit.realtime import gtfs_realtime_NYCT_pb2 as nyct

NY = ZoneInfo("America/New_York")
DIRECTION_NAMES = {1: "NORTH", 2: "EAST", 3: "SOUTH", 4: "WEST"}


def ts(epoch_seconds: int) -> str:
    """Epoch seconds -> readable New York local time."""
    if not epoch_seconds:
        return "(none)"
    return (
        datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)
        .astimezone(NY)
        .strftime("%H:%M:%S")
    )


def main(feed_id: str = "ace") -> None:
    url = feed_url(feed_id)
    print(f"Fetching {feed_id} feed...\n  {url}\n")

    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    print(f"Got {len(resp.content):,} bytes of protobuf.\n")

    # Decode the binary blob into a structured message.
    msg = rt.FeedMessage()
    msg.ParseFromString(resp.content)

    # ---- 1. THE HEADER -------------------------------------------------
    # This timestamp is when the MTA generated the snapshot. It is your
    # EVENT TIME. Never substitute your own clock for it.
    feed_ts = msg.header.timestamp
    age = int(datetime.now(tz=timezone.utc).timestamp()) - feed_ts
    print("=" * 62)
    print(f"FEED HEADER   generated {ts(feed_ts)} NY  ({age}s ago)")
    print("=" * 62)

    # ---- 2. WHAT'S IN THE SNAPSHOT -------------------------------------
    trip_updates, vehicles, alerts = [], [], []
    for entity in msg.entity:
        if entity.HasField("trip_update"):
            trip_updates.append(entity.trip_update)
        elif entity.HasField("vehicle"):
            vehicles.append(entity.vehicle)
        elif entity.HasField("alert"):
            alerts.append(entity.alert)

    print(f"\n{len(msg.entity)} entities:")
    print(f"  {len(trip_updates):4d} TripUpdate      (a train + its predicted stops)")
    print(f"  {len(vehicles):4d} VehiclePosition (where a train is right now)")
    print(f"  {len(alerts):4d} Alert")

    if not trip_updates:
        print("\nNo trips in this feed right now (late night?). Try another feed.")
        return

    # ---- 3. ONE TRAIN, IN FULL -----------------------------------------
    # Pick a train that still has several stops ahead of it, so the output
    # is interesting.
    tu = max(trip_updates, key=lambda t: len(t.stop_time_update))
    trip = tu.trip

    # NYCT packs extra fields into a protobuf "extension".
    ext = trip.Extensions[nyct.nyct_trip_descriptor]

    print("\n" + "=" * 62)
    print("ONE TRAIN, IN DETAIL")
    print("=" * 62)
    print(f"  trip_id      {trip.trip_id}")
    print(f"  route_id     {trip.route_id}")
    print(f"  start_date   {trip.start_date}")
    print(f"  train_id     {ext.train_id}          <- NYCT extension")
    print(f"  direction    {DIRECTION_NAMES.get(ext.direction, '?')}")
    print(f"  is_assigned  {ext.is_assigned}")

    # trip_id format: 064750_1..S03R
    #   064750 = origin time, hundredths of a minute after midnight
    #   1      = route
    #   S      = direction (S=south/downtown, N=north/uptown)
    if "_" in trip.trip_id:
        origin_raw = trip.trip_id.split("_")[0]
        if origin_raw.isdigit():
            mins = int(origin_raw) / 100
            print(
                f"\n  decoded from trip_id: left its origin terminal at "
                f"{int(mins // 60):02d}:{int(mins % 60):02d}"
            )

    # ---- 4. THE PREDICTIONS --------------------------------------------
    print(f"\n  NEXT {min(5, len(tu.stop_time_update))} STOPS "
          f"(of {len(tu.stop_time_update)} remaining):\n")
    print(f"    {'stop_id':>8}  {'arrive':>8}  {'depart':>8}   track")
    print(f"    {'-'*8}  {'-'*8}  {'-'*8}   {'-'*12}")
    for stu in tu.stop_time_update[:5]:
        stu_ext = stu.Extensions[nyct.nyct_stop_time_update]
        track = stu_ext.actual_track or stu_ext.scheduled_track or ""
        print(
            f"    {stu.stop_id:>8}  {ts(stu.arrival.time):>8}  "
            f"{ts(stu.departure.time):>8}   {track}"
        )

    print("""
    ^ These are PREDICTIONS, not arrivals. Poll again in 30 seconds and
      these times will have shifted. When the train passes a stop, that
      row DISAPPEARS from the list. The last prediction you ever see for
      a stop is your best estimate of when the train actually got there.

      stop_id '127N' = static stop 127 (Times Sq) + N for northbound.
""")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "ace")
