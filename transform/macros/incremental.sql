{#-
    Where an incremental run starts reading the landing zone from.

    "Everything newer than the newest row already loaded" is the obvious rule
    and it loses data. A file can land late -- a backfill, a replay, a copy
    from another machine -- carrying a timestamp older than rows that are
    already in. So the run steps back `incremental_lookback_hours` from the
    newest row it has and reads from there, and the model's unique key throws
    away what it re-reads. The price of the overlap is re-reading a few hours;
    the price of no overlap is a hole nobody notices.

    Returned as epoch seconds, as a literal, because that is the clock the
    landing files are in and because a literal is what lets DuckDB skip whole
    directories instead of opening every file to compare.

    Call it only inside `is_incremental()`: on a first build or a full refresh
    there is no table to ask.
-#}
{% macro landing_cutoff_epoch(relation) -%}
    {%- if not execute -%}
        {{ return(0) }}
    {%- endif -%}
    {%- set query -%}
        select {{ timestamp_to_epoch('max(observed_at)') }} from {{ relation }}
    {%- endset -%}
    {%- set newest = run_query(query).columns[0].values()[0] -%}
    {%- if newest is none -%}
        {{ return(0) }}
    {%- endif -%}
    {{ return((newest | int) - (var('incremental_lookback_hours') | int) * 3600) }}
{%- endmacro %}


{#-
    The landing rows at or after a cutoff, cheaply.

    The exact filter is on `observed_at`. The second condition is the one that
    saves the time: the poller files by date (`dt=2026-09-07/`), DuckDB exposes
    that directory name as a column, and a filter on it removes whole
    directories before a single file is opened. The poller names those
    directories in New York time and the cutoff is in UTC, so the date is taken
    a day early -- a day too many files opened, never one too few.

    BigQuery's landing table is partitioned on `observed_at` itself, so there
    the one filter does both jobs.
-#}
{% macro landed_since(cutoff_epoch) -%}
    {{ return(adapter.dispatch('landed_since', 'mta_reliability')(cutoff_epoch)) }}
{%- endmacro %}

{% macro default__landed_since(cutoff_epoch) -%}
    {%- set day = modules.datetime.datetime.fromtimestamp(cutoff_epoch, modules.pytz.utc)
                  - modules.datetime.timedelta(days=1) -%}
    observed_at >= {{ cutoff_epoch }}
    and dt >= cast('{{ day.strftime("%Y-%m-%d") }}' as date)
{%- endmacro %}

{% macro bigquery__landed_since(cutoff_epoch) -%}
    observed_at >= timestamp_seconds({{ cutoff_epoch }})
{%- endmacro %}
