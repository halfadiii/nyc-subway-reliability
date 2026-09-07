{#-
    A range assertion, inclusive at both ends.

    Nulls pass: whether a column is allowed to be null is a separate question
    with its own test, and folding the two together makes a failure ambiguous.
-#}
{% test between(model, column_name, lower, upper) %}

select
    {{ column_name }} as value
from {{ model }}
where {{ column_name }} is not null
  and ({{ column_name }} < {{ lower }} or {{ column_name }} > {{ upper }})

{% endtest %}
