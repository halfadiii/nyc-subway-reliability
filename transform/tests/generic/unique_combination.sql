{#-
    A uniqueness test over several columns at once.

    dbt ships `unique` for a single column and dbt_utils ships this, but the
    package is a dependency for one macro and the macro is eight lines. The
    grain of every fact table here is compound -- a trip, a stop and a service
    date -- and asserting on a concatenated surrogate key would only prove that
    the concatenation is unique, which is not the same claim.
-#}
{% test unique_combination(model, columns) %}

select
    {{ columns | join(', ') }},
    count(*) as records
from {{ model }}
group by {{ range(1, columns | length + 1) | join(', ') }}
having count(*) > 1

{% endtest %}
