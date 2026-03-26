{% macro risingwave__get_subscription_name(relation) %}
  {%- set sub_name = '__dbt_sub_' ~ relation.identifier -%}
  {{ return(sub_name) }}
{% endmacro %}

{% macro risingwave__check_subscription_exists(relation) %}
  {%- set sub_name = risingwave__get_subscription_name(relation) -%}
  {%- set exists = false -%}
  {% if execute %}
    {% set _sql %}
      select 1 from rw_catalog.rw_subscriptions s
      join rw_catalog.rw_schemas sc on s.schema_id = sc.id
      where s.name = '{{ sub_name }}'
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

{% macro risingwave__drop_subscription_if_exists(relation) %}
  {%- set sub_name = risingwave__get_subscription_name(relation) -%}
  {% if execute %}
    {% if risingwave__check_subscription_exists(relation) %}
      {{- log("Dropping embedded subscription: " ~ relation.schema ~ "." ~ sub_name) -}}
      {% call statement('drop_subscription') -%}
        drop subscription "{{ relation.schema }}"."{{ sub_name }}"
      {%- endcall %}
    {% endif %}
  {% endif %}
{% endmacro %}

{% macro risingwave__create_subscription(relation, sub_config) %}
  {%- set sub_name = risingwave__get_subscription_name(relation) -%}
  {%- set with_properties = sub_config.get('with_properties', {}) -%}
  {% if execute %}
    {% if risingwave__check_subscription_exists(relation) %}
      {{- log("Embedded subscription already exists, skipping: " ~ relation.schema ~ "." ~ sub_name) -}}
    {% else %}
      {{- log("Creating embedded subscription: " ~ relation.schema ~ "." ~ sub_name) -}}
      {% call statement('create_subscription') -%}
        {{ risingwave__render_sql_header() }}
        create subscription "{{ relation.schema }}"."{{ sub_name }}" from {{ relation }} with (
          {% for key, value in with_properties.items() %}
          {{ key }} = {{ risingwave__render_with_value(value) }}
            {%- if not loop.last -%},{%- endif %}
          {% endfor %}
        )
      {%- endcall %}
    {% endif %}
  {% endif %}
{% endmacro %}

{% macro risingwave__manage_subscription(relation, full_refresh_mode) %}
  {%- set embedded_sub = config.get('embedded_sub', none) -%}
  {% if embedded_sub is not none and embedded_sub.get('enabled', false) %}
    {{ risingwave__create_subscription(relation, embedded_sub) }}
  {% endif %}
{% endmacro %}
