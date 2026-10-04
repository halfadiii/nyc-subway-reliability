{{
    config(
        materialized = 'view',
    )
}}

/*
    The measurement the whole project exists for.

    The MTA never publishes an arrival. There is no event that says train
    G-1042 reached Bedford-Nostrand at 08:14:32. What happens instead is that
    the trip-stop pair stops appearing, and the next snapshot is quietly
    shorter than the last. So the arrival is the *last prediction that existed
    before the record vanished*, and finding it is a window function.

    Three conditions, and the second one is the one that is easy to miss. The
    first two are computed in `int_last_sightings`, which stores them so they
    are not redone over the whole history every run; the third is applied
    here.

    1. The final observation of that trip at that stop. One row per pair in
       `int_last_sightings`.

    2. `has_vanished` -- the pair was genuinely absent from a later snapshot of
       the same feed. Without this, every prediction in the most recent
       snapshot becomes an "arrival", because it is trivially the last
       observation of itself. The pipeline would report a full set of arrivals
       for trains that have not got there yet, and it would do it most
       severely at the exact edge of the data, which is where anybody looking
       at a dashboard is looking. The watermark is per feed, because the eight
       endpoints regenerate independently.

    3. The cancellation rule. A pair also vanishes when a trip is cancelled,
       re-routed or short-turned, and counting those as arrivals flatters the
       service picture in precisely the situations riders care about most. The
       separation is whether the prediction was ever about to come true: gone
       while still more than `arrival_grace_seconds` in the future, and it was
       not an arrival.

    That third rule is a threshold, and thresholds are the weakest thing in any
    pipeline. It is a project variable rather than a literal so its influence is
    measurable rather than asserted -- `analysis/sensitivity.py` re-runs the
    marts across a range of it and reports how far the headline numbers move.
    It is applied in this view, at read time, and not stored: a sweep changes
    the answer without rebuilding anything underneath.
*/

select
    service_date_raw,
    feed_id,
    route_id,
    trip_id,
    train_id,
    stop_id,
    station_id,
    platform_direction,
    direction,

    last_predicted_arrival as inferred_arrival,
    last_seen_at,

    -- How far ahead the arrival still was when the record vanished. Near zero
    -- is a train pulling in; strongly negative is a train that was already
    -- overdue, which is still an arrival and still counts.
    lead_seconds,

    -- How many snapshots this pair appeared in before it went. One is a pair
    -- seen exactly once, which is a weaker observation than one tracked across
    -- twenty polls, and worth being able to filter on.
    observations_of_pair,

    scheduled_track,
    actual_track

from {{ ref('int_last_sightings') }}

-- It vanished, rather than us stopping looking. See (2) above.
where has_vanished
  -- It was about to happen. See (3) above.
  and last_predicted_arrival <= {{
        dbt.dateadd('second', var('arrival_grace_seconds'), 'last_seen_at')
      }}
