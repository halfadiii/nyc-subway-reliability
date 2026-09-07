/*
    No arrival may be inferred from the newest snapshot of its own feed.

    The inference reads an arrival off the *last* observation of a trip-stop
    pair. In the most recent snapshot every pair is trivially its own last
    observation, so without the watermark condition in
    `int_inferred_arrivals` the pipeline reports arrivals for trains that have
    not got there yet -- and does it hardest at the leading edge of the data,
    which is exactly where anyone looking at a dashboard is looking.

    This asserts the condition holds after the fact, against the marts, rather
    than trusting that the join in the model still says what it said.
*/

with watermark as (

    select
        feed_id,
        max(observed_at) as observed_through
    from {{ ref('stg_stop_time_updates') }}
    group by 1

)

select
    arrivals.feed_id,
    arrivals.trip_id,
    arrivals.stop_id,
    arrivals.last_seen_at,
    watermark.observed_through
from {{ ref('fct_arrivals') }} as arrivals
inner join watermark using (feed_id)
where arrivals.last_seen_at >= watermark.observed_through
