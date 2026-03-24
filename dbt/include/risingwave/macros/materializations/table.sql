{% materialization table, adapter='risingwave' %}
  {%- set identifier = model['alias'] -%}
  {%- set full_refresh_mode = should_full_refresh() -%}
  {%- set old_relation = adapter.get_relation(identifier=identifier,
                                              schema=schema,
                                              database=database) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}

  {% if full_refresh_mode and old_relation %}
    {{ risingwave__drop_embedded_sink_if_exists(target_relation) }}
    {{ risingwave__drop_subscription_if_exists(target_relation) }}
    {{ adapter.drop_relation(old_relation) }}
  {% endif %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if old_relation is none %}
    {{ risingwave__embedded_precheck(target_relation) }}
  {% endif %}

  {% if old_relation is none or (full_refresh_mode and old_relation) %}
    {% call statement('main') -%}
      {{ risingwave__create_table_as(False, target_relation, sql) }}
    {%- endcall %}
    {{ risingwave__wait_for_background_ddl(target_relation, 'table') }}

    {{ create_indexes(target_relation) }}
    {{ risingwave__wait_for_background_indexes(target_relation) }}
  {% else %}
    {{ risingwave__execute_no_op(target_relation) }}
  {% endif %}

  {{ risingwave__manage_subscription(target_relation, full_refresh_mode) }}
  {{ risingwave__manage_embedded_sink(target_relation, full_refresh_mode) }}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}
  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
