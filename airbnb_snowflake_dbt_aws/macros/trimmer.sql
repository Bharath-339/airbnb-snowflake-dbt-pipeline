{% macro trimmer(column) %}
    {{column | trim | upper}}
{% endmacro %}