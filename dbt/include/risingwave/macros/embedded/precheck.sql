{% macro risingwave__embedded_precheck(relation) %}
  {%- set embedded_sub = config.get('embedded_sub', none) -%}
  {%- set sub_enabled = embedded_sub is not none and embedded_sub.get('enabled', false) -%}
  {%- set embedded_sink = config.get('embedded_sink', none) -%}
  {%- set sink_enabled = embedded_sink is not none and embedded_sink.get('enabled', false) -%}

  {% if execute %}
    {%- set collisions = [] -%}

    {% if sub_enabled and risingwave__check_subscription_exists(relation) %}
      {% do collisions.append(risingwave__get_subscription_name(relation)) %}
    {% endif %}

    {% if sink_enabled and risingwave__check_embedded_sink_exists(relation) %}
      {% do collisions.append(risingwave__get_embedded_sink_name(relation)) %}
    {% endif %}

    {% if collisions | length > 0 %}
      {{ exceptions.raise_compiler_error(
        "Name collision detected: relation " ~ relation ~ " does not exist, "
        ~ "but the following embedded objects already exist: " ~ collisions | join(", ")
        ~ ". This indicates a naming conflict with manually created objects. "
        ~ "Please drop them before proceeding."
      ) }}
    {% endif %}
  {% endif %}
{% endmacro %}
