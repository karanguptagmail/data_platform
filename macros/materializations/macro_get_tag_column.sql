-- macros/my_macro_with_where_clause.sql
-- macro to get tag references associated with { this } object --
{% macro macro_get_tag_column(database_name, schema_name, table_name) %}
    {% set table_name = table_name | upper %}
    {% set schema_name = schema_name | upper %}
    {% set database_name = database_name | upper %}

    -- get existing tag_references for this object, if any --
    {% set query %}
        select column_name, tag_name
        from snowflake.account_usage.tag_references
        where true
        and object_database = '{{ database_name }}'
        and object_schema = '{{ schema_name }}'
        and object_name = '{{ table_name }}'  
    {% endset %}

    {% set results = run_query(query) %}

    {%- set dynamic_tag_policy = {} -%}

    -- Process the results (if any) --
    {% if results %}
        {% for row in results %}
            -- Log each row or specific values from each row --
            {% set column_name = row[0] %}
            {% set tag_name = row[1] %}
            {% do dynamic_tag_policy.update({column_name : tag_name}) %}
        {% endfor %}
        {% do log("The column_name : tag py_dict is : " ~ dynamic_tag_policy, info=True) %}
        {{ return(dynamic_tag_policy) }}
    {% else %}
        {% do log("No existing tag references found. If required, check the snowflake.account_usage.tag_references latency", info=True) %}
        {%- set dynamic_tag_policy = '' -%}
        -- return dynamic_tag_policy as an empty string --
        {{ return(dynamic_tag_policy) }}
    {% endif %}
{% endmacro %}
