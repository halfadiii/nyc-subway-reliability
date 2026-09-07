{{ config(materialized = 'table') }}

/*
    One row per station, which is not the same thing as one row per stop.

    In GTFS every platform is a stop: `127N` and `127S` are the northbound and
    southbound sides of Times Square, separate rows with separate ids. A rider
    does not think that way, and neither does any question worth asking -- "how
    long do you wait at Times Square" is about the station.

    The static bundle already models this: platforms carry a `parent_station`,
    and the parent is a row of its own with `location_type = 1`. That parent id
    is exactly what `station_of()` produces by lopping the direction letter off
    a stop id, which is what lets the realtime feed join to the timetable.

    ## The stops the timetable does not publish

    The realtime feed refers to places the static bundle has never heard of.
    Measured on the observations in this warehouse, they are ids like `H05`,
    `H17`, `H18`, `R60` and `R65`: the published bundle carries `H01`-`H15` and
    stops at `R45`, and these sit outside or inside those ranges without
    appearing at all. They are yard leads, relay tracks and non-revenue
    movements -- real places a train genuinely goes, and places no rider can
    board.

    That leaves a choice, and it is the kind of choice worth stating rather
    than making quietly.

    Dropping those arrivals would be deleting observations because a *different*
    published file is incomplete, which is backwards: the feed is the
    measurement and the bundle is the reference.

    Downgrading the referential test to a warning would leave the facts
    pointing at a dimension that does not describe them, and a foreign key that
    is allowed to dangle is not a foreign key.

    So the dimension is the union: every station the bundle publishes, plus
    every station the feed actually referenced, with `is_published` saying
    which. Referential integrity then holds by construction, and the gap
    between the two sources is visible *as data* -- countable, joinable,
    trendable -- instead of hidden behind a silenced test. Anything reporting
    to riders filters on `is_published`; anything measuring the fleet does not.
*/

with stops as (

    select * from {{ ref('gtfs_stops') }}

),

published as (

    select
        stop_id                                                   as station_id,
        stop_name                                                 as station_name,
        {{ dbt.safe_cast('stop_lat', api.Column.translate_type('float')) }} as latitude,
        {{ dbt.safe_cast('stop_lon', api.Column.translate_type('float')) }} as longitude
    from stops
    -- `1` is a station; blank is a platform inside one.
    where {{ dbt.safe_cast('location_type', api.Column.translate_type('integer')) }} = 1

),

platform_counts as (

    select
        parent_station    as station_id,
        count(*)          as platform_count
    from stops
    where parent_station is not null
      and parent_station <> ''
    group by 1

),

observed as (

    select
        station_id,
        count(*)                    as observations,
        count(distinct route_id)    as routes_observed
    from {{ ref('stg_stop_time_updates') }}
    where station_id is not null
    group by 1

),

unioned as (

    select
        station_id,
        station_name,
        latitude,
        longitude,
        true as is_published
    from published

    union all

    -- Referenced by the feed, absent from the bundle. No name and no position,
    -- because there is no honest source for either -- naming it after its id
    -- would put a label on a chart that looks like a station and is not one.
    select
        observed.station_id,
        cast(null as {{ api.Column.translate_type('string') }})  as station_name,
        cast(null as {{ api.Column.translate_type('float') }})   as latitude,
        cast(null as {{ api.Column.translate_type('float') }})   as longitude,
        false as is_published
    from observed
    left join published using (station_id)
    where published.station_id is null

)

select
    unioned.station_id,
    unioned.station_name,
    unioned.latitude,
    unioned.longitude,
    unioned.is_published,
    coalesce(platform_counts.platform_count, 0) as platform_count,
    coalesce(observed.observations, 0)          as observations,
    coalesce(observed.routes_observed, 0)       as routes_observed
from unioned
left join platform_counts using (station_id)
left join observed using (station_id)
