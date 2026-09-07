{{ config(materialized = 'table') }}

/*
    One row per route, named and coloured from the published bundle.

    The realtime feed says `A`. Everything a human reads says "8 Avenue
    Express" in MTA blue, and the two are joined here rather than in whatever
    is drawing the chart, so a colour is never typed into a stylesheet.

    `feed_id` is derived from what was actually observed rather than declared
    from `ingest/feeds.py`. Those two should agree, and a test asserts that
    every observed route resolves to a known route -- but if the MTA moves a
    line between endpoints, this reports what happened rather than what the
    lookup table still believes.
*/

with routes as (

    select * from {{ ref('gtfs_routes') }}

),

observed as (

    select
        route_id,
        min(feed_id)               as feed_id,
        count(distinct trip_id)    as trips_observed,
        min(observed_at)           as first_observed_at,
        max(observed_at)           as last_observed_at
    from {{ ref('stg_stop_time_updates') }}
    where route_id is not null
    group by 1

)

select
    routes.route_id,
    routes.route_short_name,
    routes.route_long_name,
    -- The bundle stores these bare; a leading hash is what every consumer
    -- wants and prepending it in six places downstream is six chances to
    -- forget.
    case
        when routes.route_color is null or routes.route_color = '' then null
        else '#' || routes.route_color
    end                              as route_color,
    case
        when routes.route_text_color is null or routes.route_text_color = '' then null
        else '#' || routes.route_text_color
    end                              as route_text_color,

    observed.feed_id,
    coalesce(observed.trips_observed, 0) as trips_observed,
    observed.first_observed_at,
    observed.last_observed_at
from routes
left join observed using (route_id)
