{% macro risingwave__render_with_value(value) %}
  {%- if value is number -%}
    {{ value }}
  {%- elif value is sameas true -%}
    'true'
  {%- elif value is sameas false -%}
    'false'
  {%- else -%}
    '{{ value }}'
  {%- endif -%}
{% endmacro %}
