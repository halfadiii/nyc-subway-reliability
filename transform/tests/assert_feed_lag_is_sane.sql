/*
    The feed was actually live when it was polled.

    A GTFS endpoint that has stopped regenerating keeps answering requests with
    a stale snapshot, and nothing about the response says so. The only evidence
    is the gap between the header's timestamp and our clock, so it is landed as
    a column and asserted on here.

    Ten minutes is loose on purpose: the MTA's own refresh wanders, and a
    tightly-drawn bound would fail on ordinary afternoons and teach everyone to
    ignore the test. This catches a feed that has genuinely stopped, and a
    negative lag, which would mean our clock is behind the MTA's and every
    freshness figure derived from it is nonsense.
*/

select
    feed_id,
    observed_at,
    fetched_at,
    feed_lag_seconds
from {{ ref('stg_stop_time_updates') }}
where feed_lag_seconds > 600
   or feed_lag_seconds < -60
