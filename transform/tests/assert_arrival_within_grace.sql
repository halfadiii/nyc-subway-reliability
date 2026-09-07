/*
    The cancellation rule, asserted where it can be seen.

    An arrival is the last prediction before a record vanished, and a record
    that vanished while still comfortably in the future was a cancellation or a
    re-route rather than a train pulling in. The model enforces that with
    `arrival_grace_seconds`; this checks the marts still obey it, so that
    changing the variable and forgetting to rebuild is a failing test rather
    than a quietly different set of numbers.
*/

select
    trip_id,
    stop_id,
    last_seen_at,
    inferred_arrival,
    lead_seconds
from {{ ref('fct_arrivals') }}
where lead_seconds > {{ var('arrival_grace_seconds') }}
