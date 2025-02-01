{% macro stage_customer_truncate() %}

{{ log(database, info=True )}}
{{ log(schema, info=True) }}

{% if adapter.get_relation(database=database,
                            schema=schema,
                            identifier='stage_customer') is none %}
                        
    {{ log('Create table stage_customer', info=True)}}

    create table if not exists {{database}}.{{schema}}.stage_customer
    (
    CustomerID varchar(256),
    CustomerName varchar(256),
    Segment varchar(256),
    Country varchar(256),
    State varchar(256),
    load_dt timestamp
    );

{% else %}
    {{ log('Get the max load_dt from stage_customer', info=True ) }}
    {% set query_max_load_dt %}
        select coalesce(max(load_dt), '1900-01-01') as max_load_dt from {{database}}.{{schema}}.stage_customer
    {% endset %}
    {% if execute %}
        {% set max_load_dt_val = run_query(query_max_load_dt).columns[0].values()[0] %}
    {% endif %}
    {{ print( max_load_dt_val ) }}
    {{ log('Truncate table stage_customer', info=True ) }}
    {% set truncate_query %}
        delete from {{database}}.{{schema}}.stage_customer
    {% endset %}
    {% if execute %}
        {% do run_query(truncate_query) %}
    {% endif %}
{% endif %}

{{ return(max_load_dt_val) }}

{% endmacro %}