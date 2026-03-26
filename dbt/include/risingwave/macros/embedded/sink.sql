{% macro risingwave__get_embedded_sink_name(relation) %}
  {%- set sink_name = '__dbt_sink_' ~ relation.identifier -%}
  {{ return(sink_name) }}
{% endmacro %}

{% macro risingwave__check_embedded_sink_exists(relation) %}
  {%- set sink_name = risingwave__get_embedded_sink_name(relation) -%}
  {%- set exists = false -%}
  {% if execute %}
    {% set _sql %}
      select 1 from rw_catalog.rw_sinks s
      join rw_catalog.rw_schemas sc on s.schema_id = sc.id
      where s.name = '{{ sink_name }}'
      and sc.name = '{{ relation.schema }}'
      limit 1
    {% endset %}
    {% set result = run_query(_sql) %}
    {% if result is not none and (result.rows | length) > 0 %}
      {%- set exists = true -%}
    {% endif %}
  {% endif %}
  {{ return(exists) }}
{% endmacro %}

{% macro risingwave__drop_embedded_sink_if_exists(relation) %}
  {%- set sink_name = risingwave__get_embedded_sink_name(relation) -%}
  {% if execute %}
    {% if risingwave__check_embedded_sink_exists(relation) %}
      {{- log("Dropping embedded sink: " ~ relation.schema ~ "." ~ sink_name) -}}
      {% call statement('drop_embedded_sink') -%}
        drop sink "{{ relation.schema }}"."{{ sink_name }}"
      {%- endcall %}
    {% endif %}
  {% endif %}
{% endmacro %}

{# Look up a connection model's node config by model name from the dbt graph. #}
{% macro risingwave__get_connection_node(connection_ref) %}
  {%- set ns = namespace(conn_node=none) -%}
  {% for node_id, node in graph.nodes.items() %}
    {% if node.name == connection_ref and node.config.get('materialized') == 'connection' %}
      {%- set ns.conn_node = node -%}
    {% endif %}
  {% endfor %}
  {% if ns.conn_node is none %}
    {{ exceptions.raise_compiler_error(
      "connection_ref '" ~ connection_ref ~ "' not found in the dbt graph. "
      ~ "Ensure a model with materialized='connection' and name='" ~ connection_ref ~ "' exists."
    ) }}
  {% endif %}
  {{ return(ns.conn_node) }}
{% endmacro %}

{# Resolve with_properties by applying auto_* flags. Returns a new dict. #}
{% macro risingwave__resolve_with_properties(relation, sink_config) %}
  {%- set with_properties = sink_config.get('with_properties', {}).copy() -%}
  {%- set auto_primary_key = sink_config.get('auto_primary_key', false) -%}
  {% if auto_primary_key and execute %}
    {%- set pk = risingwave__get_primary_keys(relation) -%}
    {% if pk %}
      {% do with_properties.update({'primary_key': pk}) %}
    {% endif %}
  {% endif %}
  {%- set auto_table_name = sink_config.get('auto_table_name', false) -%}
  {% if auto_table_name %}
    {% do with_properties.update({'table.name': relation.identifier}) %}
  {% endif %}
  {%- set auto_database_name = sink_config.get('auto_database_name', false) -%}
  {% if auto_database_name %}
    {% do with_properties.update({'database.name': relation.schema}) %}
  {% endif %}
  {{ return(with_properties) }}
{% endmacro %}

{% macro risingwave__create_embedded_sink(relation, sink_config, with_properties) %}
  {%- set sink_name = risingwave__get_embedded_sink_name(relation) -%}
  {%- set connection_ref = sink_config.get('connection_ref', none) -%}
  {%- set connection = sink_config.get('connection', none) -%}

  {# Validate: connection_ref and connection are mutually exclusive #}
  {% if connection_ref and connection %}
    {{ exceptions.raise_compiler_error(
      "Embedded sink '" ~ sink_name ~ "': 'connection_ref' and 'connection' cannot both be set. "
      ~ "Use 'connection_ref' for dbt-managed connections or 'connection' for plain RisingWave connection names."
    ) }}
  {% endif %}

  {# Validate: connector/connection must not appear in with_properties #}
  {% if connection_ref and (with_properties.get('connector') or with_properties.get('connection')) %}
    {{ exceptions.raise_compiler_error(
      "Embedded sink '" ~ sink_name ~ "': do not set 'connector' or 'connection' in with_properties "
      ~ "when using 'connection_ref'. The connector type comes from the connection model."
    ) }}
  {% endif %}

  {# Resolve the connection clause for SQL #}
  {%- set connection_sql = none -%}
  {%- set connector_type = none -%}
  {% if connection_ref %}
    {# ref() registers the DAG dependency; output schema-qualified name for RisingWave #}
    {%- set connection_relation = ref(connection_ref) -%}
    {%- set connection_sql = '"' ~ connection_relation.schema ~ '"."' ~ connection_relation.identifier ~ '"' -%}
    {# Auto-derive connector type from the connection model's config #}
    {%- set conn_node = risingwave__get_connection_node(connection_ref) -%}
    {%- set connector_type = conn_node.config.get('connector', none) -%}
  {% elif connection %}
    {# Plain RisingWave connection name — injected without quotes #}
    {%- set connection_sql = connection -%}
  {% endif %}

  {%- set data_format = sink_config.get('format', none) -%}
  {%- set data_encode = sink_config.get('encode', none) -%}
  {%- set format_properties = sink_config.get('format_properties', {}) -%}
  {% if (data_format and not data_encode) or (data_encode and not data_format) %}
    {{ exceptions.raise_compiler_error(
      "Embedded sink '" ~ sink_name ~ "': 'format' and 'encode' must be specified together. "
      ~ "Got format=" ~ data_format ~ ", encode=" ~ data_encode ~ "."
    ) }}
  {% endif %}
  {% if execute %}
    {% if risingwave__check_embedded_sink_exists(relation) %}
      {{- log("Embedded sink already exists, skipping: " ~ relation.schema ~ "." ~ sink_name) -}}
    {% else %}
      {{- log("Creating embedded sink: " ~ relation.schema ~ "." ~ sink_name) -}}
      {% call statement('create_embedded_sink') -%}
        {{ risingwave__render_sql_header() }}
        create sink "{{ relation.schema }}"."{{ sink_name }}" from {{ relation }}
        with (
          {% if connection_sql %}
          connection = {{ connection_sql }},
          {% endif %}
          {% if connector_type %}
          connector = '{{ connector_type }}',
          {% endif %}
          {% for key, value in with_properties.items() %}
          {{ key }} = {{ risingwave__render_with_value(value) }}
            {%- if not loop.last -%},{%- endif %}
          {% endfor %}
        )
          {% if data_format and data_encode %}
        format {{ data_format }} encode {{ data_encode }}
            {% if format_properties %}
        (
            {% for key, value in format_properties.items() %}
          {{ key }} = {{ risingwave__render_with_value(value) }}
              {%- if not loop.last -%},{%- endif %}
            {% endfor %}
        )
            {% endif %}
          {% endif %}
      {%- endcall %}
    {% endif %}
  {% endif %}
{% endmacro %}

{% macro risingwave__manage_embedded_sink(relation, full_refresh_mode) %}
  {%- set embedded_sink = config.get('embedded_sink', {}) -%}
  {% if embedded_sink and embedded_sink.get('enabled', false) %}
    {%- set resolved_props = risingwave__resolve_with_properties(relation, embedded_sink) -%}
    {{ risingwave__create_embedded_sink(relation, embedded_sink, resolved_props) }}
    {{ risingwave__iceberg_persist_docs(relation, model, embedded_sink, resolved_props) }}
  {% endif %}
{% endmacro %}

{% macro risingwave__iceberg_persist_docs(relation, model, embedded_sink, resolved_props) -%}
  {# Sync docs to the Iceberg table that an embedded sink writes to. #}
  {# Only supported with connection_ref (not plain connection) since we need #}
  {# the connection model's connector_properties for PyIceberg catalog config. #}
  {% if embedded_sink.get('iceberg_persist_docs', false) %}
    {%- set connection_ref = embedded_sink.get('connection_ref') -%}
    {% if not connection_ref %}
      {{ exceptions.raise_compiler_error(
        "iceberg_persist_docs requires 'connection_ref' (not 'connection'). "
        ~ "Set connection_ref to a connection materialization model with connector='iceberg'."
      ) }}
    {% endif %}
    {%- set conn_node = risingwave__get_connection_node(connection_ref) -%}
    {%- set connector = (conn_node.config.get('connector', '') or '') | lower -%}
    {% if connector == 'iceberg' %}
      {# Check persist_docs config - skip Iceberg connection if nothing to sync #}
      {%- set persist_docs_config = model.get('config', {}).get('persist_docs', {}) -%}
      {%- set for_relation = persist_docs_config.get('relation', false) -%}
      {%- set for_columns = persist_docs_config.get('columns', false) -%}
      {% if for_relation or for_columns %}
        {%- set connector_properties = conn_node.config.get('connector_parameters', {}) -%}
        {% do adapter.iceberg_persist_docs(relation, model, connector_properties, resolved_props) %}
      {% else %}
        {{ log("iceberg_persist_docs is enabled but persist_docs.relation and persist_docs.columns are both false. Skipping Iceberg doc sync.") }}
      {% endif %}
    {% else %}
      {{ log("iceberg_persist_docs is enabled but connection '" ~ connection_ref ~ "' connector is '" ~ connector ~ "', not 'iceberg'. Skipping.") }}
    {% endif %}
  {% endif %}
{%- endmacro %}
