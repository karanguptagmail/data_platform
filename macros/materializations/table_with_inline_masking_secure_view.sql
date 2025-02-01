{% materialization table_with_inline_masking_secure_view, adapter='snowflake', supported_languages=['sql', 'python'] %}

    -- materialization is a combination of --
    -- https://github.com/dbt-labs/dbt-snowflake/blob/v1.8.0b1/dbt/include/snowflake/macros/materializations/table.sql --
    -- To generate SQL code, snowflake materialization calls create_table_as macro, which is below --
    -- implemented create_table_as functionality directly into the main materialization --
    -- https://github.com/dbt-labs/dbt-snowflake/blob/v1.8.0b1/dbt/include/snowflake/macros/relations/table/create.sql --

    {% set original_query_tag = set_query_tag() %}
    -- set identifier as the dbt model file name excluding .sql --
    {%- set identifier = model['alias'] -%}
    -- currently set to support only sql --
    {%- set language = model['language'] -%}
    {% set grant_config = config.get('grants') %}
    -- old_relation --> get metadata, if { this } exists in the snowflake, else returns none --
    {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
    -- target_relation --> metadata for the new relation <table/view> to be created in snowflake --
    {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database, type='table') -%}
    -- run pre hook sql --
    {{ run_hooks(pre_hooks) }}

    {#-- Drop the relation if it was a view to "convert" it in a table. This may lead to
    -- downtime, but it should be a relatively infrequent occurrence  #}
    {% if old_relation is not none and not old_relation.is_table %}
        {{ log("Dropping relation " ~ old_relation ~ " because it is of type " ~ old_relation.type) }}
        {{ drop_relation_if_exists(old_relation) }}
    {% endif %}

    {% set database_name = this.database %}
    {% do log("The database name is: " ~ database_name, info=True) %}

    {% set schema_name = this.schema %}
    {% do log("The schema name is: " ~ schema_name, info=True) %}

    {% set table_name = this.identifier %}
    {% do log("The view name is: " ~ table_name, info=True) %}

    -- call macro to get similar py_dict to dynamic_tag_policy below --
    -- if existing tag_references are present, it will return in a py_dict --
    {%- set dynamic_tag_policy = macro_get_tag_column(database_name = this.database, schema_name = this.schema, table_name = this.identifier) -%}

    -- for new tables/views, where the tags need to be set --
    -- set it via config --
    -- the first time, there will be no entry in the metadata tables --
    -- so it picks from the config --
    -- going forward, it will pick from the config --

    {% if dynamic_tag_policy is none or dynamic_tag_policy == '' %}
        {% do log("Set from config if available: ", info=True) %}
        {%- set dynamic_tag_policy = config.get('dynamic_tag_policy', {}) -%}
    {% endif %}

    {# Prepare the list of columns with masking policies applied if provided #}
    {%- set column_definitions = [] -%}

    -- if the model is executed for the first time --
    -- create an empty table with only the column definition --
    {%- if adapter.get_columns_in_relation(this) | length == 0 -%}
    -- create or replace table <table_name> as select col1, col2 from source where false --
    -- in case of any schema change, we need to drop the existing table/ view --
        {% call statement('create_view', fetch_result=True, language = language) -%}
            create view if not exists {{ target_relation }}
            as
            (
            {{ sql }}
            where false
            )
        {% endcall %}
    {%- endif -%}

    -- setup from --
    -- https://github.com/dbt-labs/dbt-snowflake/blob/v1.8.0b1/dbt/include/snowflake/macros/relations/table/create.sql --

    -- default temporary = false --

    {%- set temporary = false -%}

    {%- set transient = config.get('transient', default=true) -%}

    {% if temporary -%}
        {%- set table_type = "temporary" -%}
    {%- elif transient -%}
        {%- set table_type = "transient" -%}
    {%- else -%}
        {%- set table_type = "" -%}
    {%- endif %}

    -- cluster_by_key configuration setup --
    -- automatic_clustering configuration setup --
    -- copy_grants setup --
   
    -- get cluster_by_keys from the config 'cluster_by' --
    {%- set cluster_by_keys = config.get('cluster_by', default=none) -%}
    {%- set enable_automatic_clustering = config.get('automatic_clustering', default=false) -%}
    {%- set copy_grants = config.get('copy_grants', default=false) -%}
    -- if cluster_by is passed as string instead of a list --
    -- example --
    -- cluster_by = 'column_1' --
    {%- if cluster_by_keys is not none and cluster_by_keys is string -%}
        -- convert string to list --
        {%- set cluster_by_keys = [cluster_by_keys] -%}
    {%- endif -%}
    {%- if cluster_by_keys is not none -%}
        -- convert cluster_by_keys list to cluster_by_string, using join --
        -- similar to the following example --
        -- column_name = ', '.join(cluster_by_keys)
        {%- set cluster_by_string = cluster_by_keys|join(", ")-%}
    {% else %}
        {%- set cluster_by_string = none -%}
    {%- endif -%}


    {# Extract the column names and types from the model SQL #}
    -- for a new relation, we need to first create an empty table or a view to get column definition --
    {%- for column in adapter.get_columns_in_relation(this) -%}
        {%- set column_name = column.name -%}
        -- column_datatype not applicable for views

        -- Check if this column has a masking policy defined --
        {%- if column_name in dynamic_tag_policy.keys() -%}
            -- Include the MASKING POLICY clause in the column definition --
            {%- set column_def = column_name ~ " " ~ " with tag (" ~ dynamic_tag_policy[column_name] ~ " = 'PII')"-%}
        {%- else -%}
            -- Regular column definition without masking policy --
            {%- set column_def = column_name ~ " " -%}
        {%- endif -%}

        {%- do column_definitions.append(column_def) -%}
    {%- endfor -%}

    -- Join all column definitions with commas for use in the CREATE TABLE statement --
    {%- set column_definitions_sql = column_definitions | join(', ') -%}

    {%- set sql_header = config.get('sql_header', none) -%}

    {{ sql_header if sql_header is not none }}

    {% call statement('main', fetch_result=True, language = language) -%}
        create or replace view {{ target_relation }} 
        {%- set contract_config = config.get('contract') -%}
        {%- if contract_config.enforced -%}
          {{ get_assert_columns_equivalent(sql) }}
          {{ get_table_columns_and_constraints() }}
        {% endif %}
        -- copy grants not applicable for views --
        (
            {{ column_definitions_sql }}
        ) 
        as
        (
        {%- if cluster_by_string is not none -%}
            select * from (
                {{ sql }}
            ) order by ({{ cluster_by_string }})
        {%- else -%}
            {{ sql }}
        {%- endif -%}
        );    
    {% endcall %}
    
    -- clustering not applicable for views --

    -- run post hook --
    {{ run_hooks(post_hooks) }}
    -- old_relation: the current state of the target relation (e.g., a table). Could be None if the table does not exist yet --
    -- full_refresh_mode=True: A flag indicating whether a full refresh is being performed --
    -- If full_refresh_mode is set to True, the table is being rebuilt from scratch --
    -- which requires revoking and reapplying grants --
    {% set should_revoke = should_revoke(old_relation, full_refresh_mode=True) %}
    -- re apply grants --
    -- flag determines whether existing grants should first be revoked before applying the new ones --
    -- The value of should_revoke was set in the previous step --
    {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}
    -- document target_relation --
    {% do persist_docs(target_relation, model) %}
    -- unset query tag --
    {% do unset_query_tag(original_query_tag) %}

    {# Return the relation for dbt to track #}
    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}