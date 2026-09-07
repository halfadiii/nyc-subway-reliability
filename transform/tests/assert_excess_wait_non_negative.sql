/*
    Excess wait cannot be negative, and this is a statement about arithmetic
    rather than a hope about the data.

    Excess wait is E[H2]/(2 E[H]) - E[H]/2, and multiplying through by 2 E[H]
    turns it into E[H2] - E[H]^2, which is the variance of the headways. A
    variance is non-negative for every distribution that has ever existed, so a
    negative row here is not a bad day on the subway -- it is a bug in the
    aggregation, an overflow, or a headway that got through the filter with the
    wrong sign.

    The tolerance is for floating point, not for physics.
*/

select
    service_date_raw,
    route_id,
    stop_id,
    service_hour,
    headways,
    mean_headway_minutes,
    excess_wait_minutes
from {{ ref('fct_excess_wait') }}
where excess_wait_minutes < -1e-9
