{% macro ingest_metadata(results) -%}
    {%- set metadata_type = 'model_executions' -%}

    {{ create_empty_table_if_not_exists(get_metadata_database(), get_metadata_schema(), metadata_type) }}

{%- endmacro %}

{% macro snowflake__get_create_table_statement(database_name, schema_name, table_name) -%}
    create table {{database_name}}.{{schema_name}}.{{table_name}} (
        model_execution_id STRING,
        node_id STRING,
        name STRING,
        thread_id STRING,
        was_full_refresh BOOLEAN,
        status STRING,
        compile_started_at TIMESTAMP,
        query_completed_at TIMESTAMP,
        total_node_runtime INTEGER,
        rows_affected INTEGER,
        materialization STRING,
        schema STRING
    )
{%- endmacro %}

{% macro databricks__get_create_table_statement(database_name, schema_name, table_name) -%}
    create table {{database_name}}.{{schema_name}}.{{table_name}} (
        model_execution_id STRING,
        node_id STRING,
        name STRING,
        thread_id STRING,
        was_full_refresh BOOLEAN,
        status STRING,
        compile_started_at TIMESTAMP,
        query_completed_at TIMESTAMP,
        total_node_runtime TIMESTAMP,
        rows_affected INTEGER,
        materialization STRING,
        schema STRING
    )
    using delta
{%- endmacro %}

{% macro create_empty_table_if_not_exists(database_name, schema_name, table_name) -%}

    {%- do adapter.create_schema(api.Relation.create(target=database_name, schema=schema_name)) -%}

    {%- if adapter.get_relation(database=database_name, schema=schema_name, identifier=table_name) is none -%}
        {{ log("Creating table - "~adapter.quote(database_name~"."~schema_name~"."~table_name), info=true) }}
        {%- set query -%}
            {{ adapter.dispatch('get_create_table_statement')(database_name, schema_name, table_name) }}
        {% endset %}
        {%- call statement(auto_begin=True) -%}
            {{query}}
        {%- endcall -%}
    {%- endif -%}

{%- endmacro %}

{% macro get_metadata_database() -%}
    {{ var('dbt_artifacts_database', 'ANALYTICS_DEV') }}
{%- endmacro %}

{% macro get_metadata_schema() -%}
    {{ var('dbt_artifacts_schema', 'BDC_DANI') }}
{%- endmacro %}
