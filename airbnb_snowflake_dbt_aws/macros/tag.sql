{% macro tag(col) %}
    case 
        when {{col}} < 100 then 'low'
        when {{col}} < 150 then 'MID-RANGE'
        else 'LUXURY'
    end
{% endmacro %}