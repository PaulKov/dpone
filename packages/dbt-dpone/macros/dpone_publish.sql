{% macro dpone_publish(
    profile,
    workflow,
    strategy='auto',
    unique_key=none,
    partition_key=none,
    window_days=none,
    target_schema=none,
    target_table=none,
    physical_profile=none,
    order_by=none,
    execution_profile=none,
    max_parallelism=none,
    quality='standard',
    lineage=true
) %}
  {% set publish = {
    'enabled': true,
    'profile': profile,
    'workflow': workflow,
    'strategy': {'mode': strategy},
    'quality': {'preset': quality},
    'lineage': {'enabled': lineage}
  } %}
  {% if unique_key is not none %}{% do publish['strategy'].update({'unique_key': unique_key}) %}{% endif %}
  {% if partition_key is not none %}{% do publish['strategy'].update({'partition_key': partition_key}) %}{% endif %}
  {% if window_days is not none %}{% do publish['strategy'].update({'window_days': window_days}) %}{% endif %}
  {% if target_schema is not none or target_table is not none %}
    {% do publish.update({'target': {'schema': target_schema, 'table': target_table}}) %}
  {% endif %}
  {% if physical_profile is not none or order_by is not none %}
    {% do publish.update({'physical_design': {'profile': physical_profile, 'order_by': order_by}}) %}
  {% endif %}
  {% if execution_profile is not none or max_parallelism is not none %}
    {% do publish.update({'execution': {'profile': execution_profile, 'max_parallelism': max_parallelism}}) %}
  {% endif %}
  {% do return({'dpone': {'publish': publish}}) %}
{% endmacro %}
