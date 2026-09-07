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

    Three conditions, and the second one is the one that is easy to miss.

    1. `recency = 1` -- the final observation of that trip at that stop.

    2. `last_seen_at < observed_through` -- the pair was genuinely absent from a
       later snapshot of the same feed. Without this, every prediction in the
       most recent snapshot becomes an "arrival", because it is trivially the
       last observation of itself. The pipeline would report a full set of
       arrivals for trains that have not got there yet, and it would do it
       most severely at the exact edge of the data, which is where anybody
       looking at a dashboard is looking. The watermark is per feed, because
       the eight endpoints regenerate independently.

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
*/

with observations as (

    select *
    from {{ ref('stg_stop_time_updates') }}
    where predicted_arrival is not null
      -- SKIPPED and NO_DATA are the feed telling us there will be no arrival
      -- here. Believe it: that is not an inference, it is a statement.
      and schedule_relationship = 'SCHEDULED'

),

-- How far each feed has actually been watched. Anything last seen at this
-- instant has not been observed to disappear; we simply stopped looking.
watermark as (

    select
        feed_id,
        max(observed_at) as observed_through
    from {{ ref('stg_stop_time_updates') }}
    group by 1

),

ranked as (

    select
        observations.*,
        row_number() over (
            partition by service_date_raw, trip_id, stop_id
            order by observed_at desc
        ) as recency,
        count(*) over (
            partition by service_date_raw, trip_id, stop_id
        ) as observations_of_pair
    from observations

)

select
    ranked.service_date_raw,
    ranked.feed_id,
    ranked.route_id,
    ranked.trip_id,
    ranked.train_id,
    ranked.stop_id,
    ranked.station_id,
    ranked.platform_direction,
    ranked.direction,

    ranked.predicted_arrival as inferred_arrival,
    ranked.observed_at       as last_seen_at,

    -- How far ahead the arrival still was when the record vanished. Near zero
    -- is a train pulling in; strongly negative is a train that was already
    -- overdue, which is still an arrival and still counts.
    {{ dbt.datediff('ranked.observed_at', 'ranked.predicted_arrival', 'second') }}
        as lead_seconds,

    -- How many snapshots this pair appeared in before it went. One is a pair
    -- seen exactly once, which is a weaker observation than one tracked across
    -- twenty polls, and worth being able to filter on.
    ranked.observations_of_pair,

    ranked.scheduled_track,
    ranked.actual_track

from ranked
inner join watermark
    on watermark.feed_id = ranked.feed_id

where ranked.recency = 1
  -- It vanished, rather than us stopping looking. See (2) above.
  and ranked.observed_at < watermark.observed_through
  -- It was about to happen. See (3) above.
  and ranked.predicted_arrival <= {{
        dbt.dateadd('second', var('arrival_grace_seconds'), 'ranked.observed_at')
      }}
