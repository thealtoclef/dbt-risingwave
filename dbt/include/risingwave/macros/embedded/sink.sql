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
  {# Resolve connection_ref to a dbt relation for the SQL connection clause #}
  {%- set connection_ref = sink_config.get('connection_ref', none) -%}
  {%- set connection_relation = none -%}
  {% if connection_ref %}
    {%- set connection_relation = ref(connection_ref) -%}
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
        create sink "{{ relation.schema }}"."{{ sink_name }}" from {{ relation }}
        with (
          {% if connection_relation %}
          connection = {{ connection_relation }},
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
  {%- set embedded_sink = config.get('embedded_sink', none) -%}
  {% if embedded_sink is not none and embedded_sink.get('enabled', false) %}
    {%- set resolved_props = risingwave__resolve_with_properties(relation, embedded_sink) -%}
    {{ risingwave__create_embedded_sink(relation, embedded_sink, resolved_props) }}
  {% endif %}
{% endmacro %}
