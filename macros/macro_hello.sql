{% macro hello() %}

{% if execute %}
    {% set results = run_query("select 'hello'; ").columns[0].values()[0] %}

{% endif %}

    {{ print( results ) }}

{% endmacro %}