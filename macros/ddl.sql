{% macro parse_results_models(results) %}
 {% if execute %}
  {% for model in results if model.node.resource_type == "model" -%}
    {% set line -%}
        command_invocation_id: {{ invocation_id }},
        node_id: {{ model.node.unique_id }},
        was_full_refresh_flags: {{ flags.FULL_REFRESH }},
        was_full_refresh_results: {{ model.node.config.full_refresh }},
        thread_id: {{ model.thread_id }},
        status: {{ model.status }},
        {% for stage in model.timing if stage.name == "compile" %}
          compile_started_at: {{ stage.started_at }},
        {% endfor %}
        {% for stage in model.timing if stage.name == "execute" %}
          query_completed_at: {{ stage.completed_at }},
        {% endfor %}
        total_node_runtime: {{ model.execution_time }},
        rows_affected: {{ model.adapter_response.rows_affected }},
        model_materialization: {{ model.node.config.materialized }},
        model_schema: {{ model.node.schema }},
        name: {{ model.node.name }}
    {%- endset %}
    {{ log(line, info=True) }}
  {% endfor %}
 {% endif %}
{% endmacro %}

{% macro ingest_metadata(results) -%}
    {%- set metadata_type = 'model_executions' -%} -- TODO: Iterate through the various metadata types

    {# If table does not already exists, make it #}
    {{ adapter.dispatch('create_metadata_table')(metadata_type) }}

    {# Insert into #}
    {{ log('Inserting into metadata table', info=True) }}

    {# Get model_execution values #}
    {% set model_execution_values %}
        {% for model in results if model.node.resource_type == "model" -%}
                {% set was_full_refresh = model.node.config.full_refresh if model.node.config.full_refresh == true else flags.FULL_REFRESH %}
                (
                    '{{ invocation_id }}',
                    '{{ model.node.unique_id }}',
                    '{{ model.node.name }}',
                    '{{ model.thread_id }}',
                    {{ was_full_refresh }},
                    '{{ model.status }}',
                    'started_at_hardcoded',
                    'completed_at_hardcoded',
                    {{ model.execution_time }},
                    {% if not model.adapter_response.rows_affected %}
                        NULL,
                    {% else %}
                        {{ model.adapter_response.rows_affected }},
                    {% endif %}
                    '{{ model.node.config.materialized }}',
                    '{{ model.node.schema }}'
                )
            {%- if not loop.last %}
                ,
            {%- endif %}
        {%- endfor %}
    {% endset %}
    {{ adapter.dispatch('insert_into_metadata_table')(metadata_type, model_execution_values) }}

{%- endmacro %}

{% macro snowflake__create_metadata_table(metadata_type) -%}
    {% set ddl %}
    {% if flags.FULL_REFRESH %}
        create or replace table
    {% else %}
        create table if not exists
    {% endif %}

    {{get_metadata_database()}}.{{get_metadata_schema()}}.{{metadata_type}} (
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
    {% endset %}
    {{ log(ddl, info=True) }}
{%- endmacro %}

{% macro databricks__create_metadata_table(metadata_type) -%}
    {% if flags.FULL_REFRESH %}
        create or replace table
    {% else %}
        create table if not exists
    {% endif %}

    {{get_metadata_database()}}.{{get_metadata_schema()}}.{{metadata_type}} (
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

{% macro default__insert_into_metadata_table(metadata_type, content) -%}
    insert into {{get_metadata_database()}}.{{get_metadata_schema()}}.{{metadata_type}}(model_execution_id, node_id, name, thread_id, was_full_refresh, status, compile_started_at, query_completed_at, total_node_runtime, rows_affected, materialization, schema)
    values {{content}}
    {{ log(metadata_type, info=True) }}
    {{ log(content, info=True) }}
{%- endmacro %}

{% macro get_metadata_database() -%}
    {{ var('dbt_artifacts_database', 'ANALYTICS_DEV') }}
{%- endmacro %}

{% macro get_metadata_schema() -%}
    {{ var('dbt_artifacts_schema', 'BDC_DANI') }}
{%- endmacro %}
