{% materialization custom_materialized, default %}

    {%- set target_relation = this.incorporate(type='table') %}

    -- build model
    {% call statement('main') -%}
        create or replace transient table {{this.database}}.{{this.schema}}.{{this.identifier}} as (
            {{ sql }}
        )
    {%- endcall %}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}