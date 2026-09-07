"""STEP 1, part 2: watch a prediction converge, then vanish.

Polls one feed every 30 seconds and tracks a single train. Each poll it
prints what that train currently predicts for its next few stops, so you
can watch the numbers get revised -- and watch a stop DROP OFF the list
the moment the train passes it.

That drop-off is the whole project. Sit and watch it happen once.

Run it:   python watch.py ace 20      (feed id, number of polls)
Ctrl-C to stop early.
"""

import sys
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests

from feeds import feed_url
from com.google.transit.realtime import gtfs_realtime_pb2 as rt

NY = ZoneInfo("America/New_York")


def hhmmss(epoch: int) -> str:
    if not epoch:
        return "--:--:--"
    return (
        datetime.fromtimestamp(epoch, tz=timezone.utc)
        .astimezone(NY)
        .strftime("%H:%M:%S")
    )


def get_trips(feed_id: str) -> tuple[int, dict]:
    """Fetch feed -> (header_timestamp, {trip_id: {stop_id: predicted_arrival}})."""
    msg = rt.FeedMessage()
    msg.ParseFromString(requests.get(feed_url(feed_id), timeout=30).content)
    trips = {}
    for entity in msg.entity:
        if not entity.HasField("trip_update"):
            continue
        tu = entity.trip_update
        trips[tu.trip.trip_id] = {
            stu.stop_id: stu.arrival.time
            for stu in tu.stop_time_update
            if stu.arrival.time
        }
    return msg.header.timestamp, trips


def main(feed_id: str = "ace", n_polls: int = 20) -> None:
    # Poll once to choose a train worth following: one with a decent number
    # of stops left, so we'll see several of them disappear.
    _, trips = get_trips(feed_id)
    if not trips:
        print("No trips right now. Try a busier feed or time of day.")
        return

    target = max(trips, key=lambda t: len(trips[t]))
    watched = list(trips[target])[:6]  # follow its next 6 stops
    print(f"Following trip {target} on the {feed_id} feed.")
    print(f"Watching its next {len(watched)} stops. {n_polls} polls, 30s apart.\n")

    header = f"{'poll':>4} {'feed time':>9} " + " ".join(f"{s:>9}" for s in watched)
    print(header)
    print("-" * len(header))

    last_seen: dict[str, tuple[int, int]] = {}  # stop -> (feed_ts, predicted)

    for i in range(1, n_polls + 1):
        try:
            feed_ts, trips = get_trips(feed_id)
        except Exception as exc:                     # network hiccups happen
            print(f"{i:>4} fetch failed: {exc}")
            time.sleep(30)
            continue

        preds = trips.get(target, {})
        cells = []
        for stop in watched:
            if stop in preds:
                last_seen[stop] = (feed_ts, preds[stop])
                cells.append(f"{hhmmss(preds[stop])[3:]:>9}")   # MM:SS
            else:
                cells.append(f"{'  --  ':>9}")
        print(f"{i:>4} {hhmmss(feed_ts)[3:]:>9} " + " ".join(cells))

        if target not in trips:
            print("\n  Trip gone from feed entirely -- it finished its run.")
            break
        time.sleep(30)

    # ---- THE PAYOFF ----------------------------------------------------
    print("\n" + "=" * 60)
    print("DERIVED ARRIVALS  (last prediction before the stop vanished)")
    print("=" * 60)
    for stop in watched:
        if stop in last_seen:
            seen_at, predicted = last_seen[stop]
            gone = stop not in trips.get(target, {})
            mark = "passed" if gone else "still ahead"
            print(
                f"  {stop:>6}  arrival ~{hhmmss(predicted)}   "
                f"(last seen in feed at {hhmmss(seen_at)}, {mark})"
            )
    print("""
  The 'passed' rows are observed arrivals -- and no column in the feed
  ever contained them. You just built the project's central measurement
  by hand. Step 8 does exactly this in SQL, for every train at once.
""")


if __name__ == "__main__":
    feed = sys.argv[1] if len(sys.argv) > 1 else "ace"
    polls = int(sys.argv[2]) if len(sys.argv) > 2 else 20
    main(feed, polls)
