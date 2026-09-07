{{
    config(
        materialized = 'table',
    )
}}

/*
    A table, and the one place in this project that overrides the layer default.

    Staging is a view everywhere else because it only renames and casts, and a
    second copy of the landing table would earn nothing. Here the "landing
    table" is a glob of gzipped files on disk, and that changes two things. A
    view re-reads and re-decompresses every one of them on every query
    downstream, which is most of the runtime of everything. And its path is
    resolved by whichever process opens the database, not by dbt -- so a view
    built here works while dbt's working directory is this folder and fails for
    anything that opens the warehouse from anywhere else, which is every script
    in `analysis/` and every ad-hoc query anyone will ever run.

    Materialising reads the files once, at build time, in the one place the
    relative path is known to be right. After that the warehouse stands alone.

    Types, names, and the two identifiers the feed encodes in strings. No
    filtering: this is the landing table with its columns made usable, and a
    row that looks like rubbish here is a row the intermediate layer gets to
    decide about, on a stated rule, where the decision is visible.

    The one thing it does remove is an exact duplicate. The poller skips a
    snapshot whose header timestamp it has already written, but a restart, a
    replay, or a backfill run twice can all put the same observation on disk
    more than once, and every downstream count would carry it. Deduplicated on
    the natural key rather than on the whole row, so a re-poll that differs
    only in `fetched_at` collapses too.
*/

with source as (

    select * from {{ source('raw', 'stop_time_updates') }}

),

deduplicated as (

    select *
    from source
    qualify row_number() over (
        partition by feed_id, observed_at, trip_id, stop_id
        order by fetched_at
    ) = 1

)

select
    -- Identity -------------------------------------------------------------
    feed_id,
    trip_id,
    stop_id,
    {{ station_of('stop_id') }}                       as station_id,
    {{ direction_of('stop_id') }}                     as platform_direction,
    route_id,

    -- The service date the trip belongs to. Not the calendar date of the
    -- observation: a train that leaves at 00:40 belongs to the previous
    -- service day, and the feed says so in start_date.
    {{ dbt.safe_cast('start_date', api.Column.translate_type('string')) }} as service_date_raw,

    -- Time -----------------------------------------------------------------
    {{ epoch_to_timestamp('observed_at') }}           as observed_at,
    {{ epoch_to_timestamp('fetched_at') }}            as fetched_at,
    {{ epoch_to_timestamp('predicted_arrival') }}     as predicted_arrival,
    {{ epoch_to_timestamp('predicted_departure') }}   as predicted_departure,

    -- How far behind our clock the MTA's picture was, in seconds. A feed that
    -- silently stops regenerating still answers every request, so this is the
    -- only signal that the source has gone stale; `tests/` asserts on it.
    fetched_at - observed_at                          as feed_lag_seconds,

    -- Trip detail ----------------------------------------------------------
    direction,
    train_id,
    is_assigned,
    stop_sequence,
    scheduled_track,
    actual_track,
    schedule_relationship,

    -- The departure time packed into the trip id, in minutes after midnight.
    -- `064750_A..N00R` is the 10:47 A train, and that is the only human-stable
    -- name a trip has.
    {{ trip_origin_minutes('trip_id') }}              as trip_origin_minutes

from deduplicated
