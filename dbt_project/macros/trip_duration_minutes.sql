{% macro trip_duration_minutes(pickup_col, dropoff_col) %}
    {#- Minutes between dropoff and pickup, as a float, via DuckDB's datediff. -#}
    (date_diff('second', {{ pickup_col }}, {{ dropoff_col }}) / 60.0)
{% endmacro %}
