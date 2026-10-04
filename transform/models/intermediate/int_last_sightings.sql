{{
    config(
        materialized = 'incremental',
        unique_key = ['service_date_raw', 'trip_id', 'stop_id'],
        on_schema_change = 'fail',
    )
}}

/*
    The last time each trip-stop pair was seen, and whether it has gone.

    This is the first two of the three rules in `int_inferred_arrivals`, kept
    as a table so they do not have to be recomputed over the whole history on
    every run. One row per pair, whatever became of it: arrived, cancelled, or
    still on its way.

    ## Why this is the thing to keep

    Finding the last observation of every pair is a sort over every
    observation ever landed, and it was being done from scratch each run. But
    almost all of that work has an answer that can no longer change. Once a
    pair has been absent from a later snapshot, its last sighting is final --
    unless it turns up again, which is a new observation and is handled below.

    So a run recomputes only the pairs that have been *touched*: seen at or
    after the point the previous run had watched their feed to. That set is
    exactly the pairs whose answer could have moved:

      * pairs with a new observation, including one that had vanished and has
        come back;
      * pairs that were in the newest snapshot last time. They had not
        vanished, only because nobody had looked again. Now somebody has.

    For those pairs it reads their *whole* history, not just the new part, so
    `observations_of_pair` is the true count and the row written is the row a
    full rebuild would write. The unique key replaces the old row with it.

    The step back from the previous run's watermark is per feed, because the
    eight endpoints regenerate independently and one of them can be hours
    behind the others. It is widened by `incremental_lookback_hours` for the
    same reason staging is: a file can land late.

    ## What is deliberately not in here

    The third rule, the cancellation threshold. It is a judgement call that
    `analysis/sensitivity.py` varies on purpose, and a threshold baked into a
    stored row would mean every stored row is wrong the moment it is varied.
    This table records what was observed -- how far ahead the prediction still
    was when the pair was last seen -- and the view above it applies the
    threshold at read time.

    `tests/test_incremental.py` asserts that building this in stages gives the
    same rows as building it once, including a pair that vanishes and returns.
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

{% if is_incremental() -%}

-- How far the previous run had watched each feed. Pairs still pending are
-- stored too, so the newest sighting held is that run's watermark.
previous as (

    select
        feed_id,
        max(last_seen_at) as seen_through
    from {{ this }}
    group by 1

),

touched as (

    select distinct
        observations.service_date_raw,
        observations.trip_id,
        observations.stop_id
    from observations
    left join previous
        on previous.feed_id = observations.feed_id
    -- A feed with nothing stored yet is new: take all of it.
    where previous.seen_through is null
       or observations.observed_at >= {{
            dbt.dateadd('hour', -1 * var('incremental_lookback_hours'), 'previous.seen_through')
          }}

),

scoped as (

    select observations.*
    from observations
    inner join touched
        on  touched.service_date_raw = observations.service_date_raw
        and touched.trip_id = observations.trip_id
        and touched.stop_id = observations.stop_id

),

{%- else -%}

scoped as (

    select * from observations

),

{%- endif %}

ranked as (

    select
        scoped.*,
        row_number() over (
            partition by service_date_raw, trip_id, stop_id
            order by observed_at desc
        ) as recency,
        count(*) over (
            partition by service_date_raw, trip_id, stop_id
        ) as observations_of_pair
    from scoped

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

    ranked.predicted_arrival as last_predicted_arrival,
    ranked.observed_at       as last_seen_at,

    -- How far ahead the arrival still was at the last sighting. Near zero is
    -- a train pulling in; strongly negative is a train that was already
    -- overdue; ten minutes is a trip that was pulled.
    {{ dbt.datediff('ranked.observed_at', 'ranked.predicted_arrival', 'second') }}
        as lead_seconds,

    -- How many snapshots this pair appeared in. One is a pair seen exactly
    -- once, which is a weaker observation than one tracked across twenty
    -- polls, and worth being able to filter on.
    ranked.observations_of_pair,

    ranked.scheduled_track,
    ranked.actual_track,

    -- Absent from a later snapshot of its own feed. False means it was in the
    -- newest one: it has not disappeared, we stopped looking.
    ranked.observed_at < watermark.observed_through as has_vanished

from ranked
inner join watermark
    on watermark.feed_id = ranked.feed_id

where ranked.recency = 1
