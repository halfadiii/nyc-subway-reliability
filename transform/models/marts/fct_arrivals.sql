{{ config(materialized = 'table') }}

/*
    Every arrival the pipeline was able to infer, with the local clock attached.

    This is the grain everything else is aggregated from: one row per trip, per
    stop, per service date. It is deliberately a fact table rather than the last
    step of a single enormous query -- headways, excess wait and the weather
    join all read it, and none of them should be re-deriving an arrival.

    Local time is added here and nowhere else. Every question about service is
    asked in New York wall-clock terms ("the evening peak", "the 8am hour") and
    those do not survive being asked in UTC, because the offset moves twice a
    year and a chart of the morning rush would slide an hour every spring.
*/

with arrivals as (

    select * from {{ ref('int_inferred_arrivals') }}

),

local_clock as (

    select
        arrivals.*,
        {{ to_ny('inferred_arrival') }} as arrival_local
    from arrivals

)

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

    inferred_arrival,
    arrival_local,
    {{ dbt.date_trunc('hour', 'arrival_local') }}  as service_hour,
    extract(hour from arrival_local)               as hour_of_day,
    extract(dayofweek from arrival_local)          as day_of_week,

    last_seen_at,
    lead_seconds,
    observations_of_pair,
    scheduled_track,
    actual_track
from local_clock
