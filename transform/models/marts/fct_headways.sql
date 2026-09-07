{{ config(materialized = 'table') }}

/*
    The gap between one train and the next, at a platform, on a route.

    Partitioned by `stop_id` and not by `station_id`: a stop id already carries
    its direction, and a northbound gap and a southbound gap at the same
    station are two different waits for two different riders. Merging them
    would interleave two sequences of arrivals and produce a headway nobody
    experienced.

    Two filters, both of which exist to stop the aggregate lying.

    A gap under `min_headway_minutes` is almost never two trains. It is one
    train counted twice -- the same arrival inferred from two feeds, or a
    prediction that flickered back into the snapshot after appearing to vanish.
    Left in, it drags the mean headway down and makes service look better than
    it was.

    A gap over `max_headway_minutes` is usually not a wait either. It is the
    overnight service change, or a hole in *our own polling*, and that is the
    important one: if the poller was down for two hours, the arrival either
    side of the outage form a "gap" of two hours that no rider stood through.
    Averaging across a hole in the observations invents a wait, and it invents
    it in the direction that makes the service look worst, which is the
    direction a portfolio project is most tempted to believe.

    Both are project variables so their effect is measurable rather than
    assumed.
*/

with arrivals as (

    select
        service_date_raw,
        route_id,
        stop_id,
        station_id,
        platform_direction,
        trip_id,
        inferred_arrival,
        arrival_local,
        service_hour,
        hour_of_day,
        day_of_week
    from {{ ref('fct_arrivals') }}

),

sequenced as (

    select
        arrivals.*,
        lag(inferred_arrival) over (
            partition by service_date_raw, route_id, stop_id
            order by inferred_arrival
        ) as previous_arrival,
        lag(trip_id) over (
            partition by service_date_raw, route_id, stop_id
            order by inferred_arrival
        ) as previous_trip_id
    from arrivals

),

gaps as (

    select
        sequenced.*,
        {{ dbt.datediff('previous_arrival', 'inferred_arrival', 'second') }} / 60.0
            as headway_minutes
    from sequenced
    where previous_arrival is not null
      -- The same trip cannot be the train in front of itself. It can appear
      -- twice at one stop when a trip id is reused across a service change.
      and previous_trip_id <> trip_id

)

select
    service_date_raw,
    route_id,
    stop_id,
    station_id,
    platform_direction,
    trip_id,
    previous_trip_id,
    previous_arrival,
    inferred_arrival,
    arrival_local,
    service_hour,
    hour_of_day,
    day_of_week,
    headway_minutes
from gaps
where headway_minutes >= {{ var('min_headway_minutes') }}
  and headway_minutes <= {{ var('max_headway_minutes') }}
