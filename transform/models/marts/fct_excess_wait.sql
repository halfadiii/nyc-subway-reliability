{{ config(materialized = 'table') }}

/*
    Excess wait time: how much longer a rider waits than the service implies.

    The intuitive metric is the mean headway, and it is the wrong one. Riders
    do not arrive on the platform at evenly spaced moments chosen to match the
    timetable; they turn up roughly at random, which means they are more likely
    to walk into a long gap than a short one -- a twelve-minute gap catches
    three times as many people as a four-minute gap, simply by being three
    times as long.

    So the wait a rider actually experiences is the *length-weighted* mean of
    the gaps, which for random arrivals is

        E[wait] = E[H squared] / (2 E[H])

    against the E[H]/2 that an evenly spread service would give. The difference
    between those two is excess wait time, and it is the number that describes
    a bad commute: bunching can leave the mean headway completely untouched
    while making the wait materially worse, and only this metric notices.

    Worked, because the formula is easy to nod along to and not see.

        Six trains an hour, evenly: gaps of 10, 10, 10, 10, 10, 10.
            mean headway 10, E[H2]/(2 E[H]) = 100/20 = 5, half the mean is 5.
            Excess wait: 0. Nobody is worse off than the service promises.

        Six trains an hour, bunched: gaps of 2, 18, 2, 18, 2, 18.
            mean headway is *still* 10 -- the timetable is being met.
            E[H2] = (4+324+4+324+4+324)/6 = 164, so E[H2]/(2 E[H]) = 8.2.
            Excess wait: 3.2 minutes. Which is the complaint riders actually
            have, and which the mean headway reports as no problem at all.

    Grouped by hour because that is the finest slice with enough gaps in it to
    mean anything, and because "is the evening worse than the morning" is the
    question. `headways` ships with every row: an excess wait computed from two
    gaps is arithmetic, not a measurement, and anything reading this should be
    filtering on it. `analysis/rain_regression.py` does.
*/

with headways as (

    select * from {{ ref('fct_headways') }}

)

select
    service_date_raw,
    route_id,
    stop_id,
    station_id,
    platform_direction,
    service_hour,
    hour_of_day,
    day_of_week,

    count(*)                                       as headways,
    avg(headway_minutes)                           as mean_headway_minutes,
    min(headway_minutes)                           as min_headway_minutes,
    max(headway_minutes)                           as max_headway_minutes,

    -- The wait a rider turning up at random actually experiences.
    sum(headway_minutes * headway_minutes) / (2 * sum(headway_minutes))
                                                   as rider_wait_minutes,
    -- What that wait would be if the same number of trains were evenly spread.
    avg(headway_minutes) / 2                       as even_wait_minutes,
    -- The gap between those two. Zero on a perfectly regular service, and it
    -- cannot be negative: E[H2] >= E[H]^2 for any distribution, which makes
    -- this the variance term and is why `tests/assert_excess_wait_non_negative`
    -- is a real assertion about the maths rather than a guess about the data.
    sum(headway_minutes * headway_minutes) / (2 * sum(headway_minutes))
        - avg(headway_minutes) / 2                 as excess_wait_minutes

from headways
group by
    service_date_raw,
    route_id,
    stop_id,
    station_id,
    platform_direction,
    service_hour,
    hour_of_day,
    day_of_week
