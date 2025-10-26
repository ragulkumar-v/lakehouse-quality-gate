{% macro safe_divide(numerator, denominator) %}
    {#-
        Divide two expressions, returning NULL instead of a divide-by-zero
        error when the denominator is zero or null. Exercised by the dbt
        unit test in models/intermediate/_intermediate__models.yml, which
        pins down exactly this edge case.
    -#}
    (case
        when {{ denominator }} is null or {{ denominator }} = 0 then null
        else {{ numerator }} * 1.0 / {{ denominator }}
    end)
{% endmacro %}
