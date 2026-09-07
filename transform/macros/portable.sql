{#-
    The handful of places where BigQuery and DuckDB genuinely disagree.

    dbt ships cross-database macros for most of this (`dbt.datediff`,
    `dbt.date_trunc`, `dbt.safe_cast`) and those are used directly in the
    models. What is left is here, dispatched per adapter, so that a model never
    contains a branch and there is exactly one definition of every derived
    column regardless of where it runs.
-#}

{#- Epoch seconds -> timestamp. The feed speaks in epoch seconds throughout. -#}
{% macro epoch_to_timestamp(column) -%}
    {{ return(adapter.dispatch('epoch_to_timestamp', 'mta_reliability')(column)) }}
{%- endmacro %}

{% macro default__epoch_to_timestamp(column) -%}
    to_timestamp({{ column }})
{%- endmacro %}

{% macro bigquery__epoch_to_timestamp(column) -%}
    timestamp_seconds({{ column }})
{%- endmacro %}


{#-
    The station a platform belongs to.

    Every physical platform is its own stop: `127N` and `127S` are the two
    directions of Times Square, and a question about a *station* has to add
    them back together. The direction letter is the last character, but only
    when it is one -- a few stop ids in the feed carry no suffix at all, and
    lopping a character off those would silently merge them into a station
    that does not exist.
-#}
{% macro station_of(column) -%}
    {{ return(adapter.dispatch('station_of', 'mta_reliability')(column)) }}
{%- endmacro %}

{% macro default__station_of(column) -%}
    case
        when right({{ column }}, 1) in ('N', 'S') then left({{ column }}, length({{ column }}) - 1)
        else {{ column }}
    end
{%- endmacro %}

{% macro bigquery__station_of(column) -%}
    case
        when right({{ column }}, 1) in ('N', 'S') then left({{ column }}, length({{ column }}) - 1)
        else {{ column }}
    end
{%- endmacro %}


{#- The direction letter, where the stop id carries one. -#}
{% macro direction_of(column) -%}
    case
        when right({{ column }}, 1) = 'N' then 'N'
        when right({{ column }}, 1) = 'S' then 'S'
        else null
    end
{%- endmacro %}


{#-
    The origin departure time packed into a trip id.

    `064750_A..N00R` -> `064750` is hundredths of a minute after midnight, so
    647.5 minutes, so 10:47. It is the only stable identity a trip has across
    snapshots on the same service date, and it is worth pulling out as a column
    because it is the difference between "the 10:47 A train" and a string.
-#}
{% macro trip_origin_minutes(column) -%}
    {{ return(adapter.dispatch('trip_origin_minutes', 'mta_reliability')(column)) }}
{%- endmacro %}

{% macro default__trip_origin_minutes(column) -%}
    try_cast(split_part({{ column }}, '_', 1) as integer) / 100.0
{%- endmacro %}

{#- BigQuery has no split_part; `split` returns an array. -#}
{% macro bigquery__trip_origin_minutes(column) -%}
    safe_cast(split({{ column }}, '_')[safe_offset(0)] as int64) / 100.0
{%- endmacro %}


{#-
    A UTC instant as New York wall-clock time.

    Every question here is asked in local terms -- "the evening peak", "the
    8am hour" -- and those do not survive being asked in UTC, because the
    offset changes twice a year. Converted once, here, so no model has to
    remember to.
-#}
{% macro to_ny(column) -%}
    {{ return(adapter.dispatch('to_ny', 'mta_reliability')(column)) }}
{%- endmacro %}

{% macro default__to_ny(column) -%}
    ({{ column }} at time zone 'America/New_York')
{%- endmacro %}

{% macro bigquery__to_ny(column) -%}
    datetime({{ column }}, 'America/New_York')
{%- endmacro %}
