{# Transport validation only. Protected SQL authenticates original bytes and plans. #}
{% macro dpone_managed_check(condition, field) %}
  {% if not condition %}{% do exceptions.raise_compiler_error('DPONE_MANAGED_INPUT_INVALID: ' ~ field) %}{% endif %}
{% endmacro %}

{% macro dpone_managed_shape(value, fields, field) %}
  {% do dpone_managed_check(value is mapping, field) %}
  {% do dpone_managed_check(value.keys() | list | sort == fields | sort, field) %}
{% endmacro %}

{% macro dpone_managed_text(value, field, maximum=4096, empty=false) %}
  {% do dpone_managed_check(value is string, field) %}
  {% do dpone_managed_check((empty or value | length > 0) and value | length <= maximum, field) %}
  {% do dpone_managed_check(not modules.re.search('[\ud800-\udfff]', value), field) %}
  {% do dpone_managed_check(value.encode('utf-8') | length <= maximum, field) %}
{% endmacro %}

{% macro dpone_managed_uuid(value, field) %}
  {% do dpone_managed_text(value, field, 36) %}
  {% do dpone_managed_check(modules.re.fullmatch('[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', value), field) %}
{% endmacro %}

{% macro dpone_managed_digest(value, field) %}
  {% do dpone_managed_text(value, field, 71) %}
  {% do dpone_managed_check(modules.re.fullmatch('sha256:[0-9a-f]{64}', value), field) %}
{% endmacro %}

{% macro dpone_managed_integer(value, field, minimum=1, maximum=9223372036854775807) %}
  {# agate returns Decimal for SQL integers; bool/string/fractional are forbidden. #}
  {% do dpone_managed_check(value is number and value is not boolean, field) %}
  {% do dpone_managed_check(modules.re.fullmatch('[0-9]+', value | string), field) %}
  {% do dpone_managed_check(value >= minimum and value <= maximum and value == value | int, field) %}
{% endmacro %}

{% macro dpone_managed_identifier(value, field) %}
  {% do dpone_managed_text(value, field) %}
  {% do dpone_managed_check(value.encode('utf-16-le') | length <= 256 and not modules.re.search('[\x00-\x1f\x7f-\x9f]', value), field) %}
{% endmacro %}

{% macro dpone_managed_reference(value, field) %}
  {% do dpone_managed_shape(value, ['locator', 'sha256'], field) %}
  {% do dpone_managed_text(value.locator, field) %}
  {% do dpone_managed_digest(value.sha256, field) %}
  {% do dpone_managed_check(not modules.re.search('[\\\\\x00-\x1f\x7f-\x9f]', value.locator), field) %}
  {% for part in value.locator.split('/') %}
    {% do dpone_managed_check(part not in ['', '.', '..'], field) %}
  {% endfor %}
{% endmacro %}

{% macro dpone_managed_literal(value) %}
  {% do dpone_managed_text(value, 'SQL literal') %}
  {% do return("N'" ~ value.replace("'", "''") ~ "'") %}
{% endmacro %}

{% macro dpone_managed_envelope() %}
  {% set value = var('__dpone_managed') %}
  {% do dpone_managed_shape(value, ['generation_id', 'invocation_id', 'plan_set', 'runtime_registration_id'], '__dpone_managed') %}
  {% for field in ['generation_id', 'invocation_id', 'runtime_registration_id'] %}{% do dpone_managed_uuid(value[field], field) %}{% endfor %}
  {% do dpone_managed_reference(value.plan_set, 'plan_set') %}
  {% do return(value) %}
{% endmacro %}

{% macro dpone_managed_json(text, field) %}
  {# Bound lexical work BEFORE fromjson; reject duplicate keys at every depth.
     This is a transport parser, not the canonical codec or a digest verifier. #}
  {% do dpone_managed_text(text, field, 1048576) %}
  {% set tokens = modules.re.findall('"(?:[^"\\\\\x00-\x1f]|\\\\(?:["\\\\/bfnrt]|u[0-9a-fA-F]{4}))*"|-?(?:0|[1-9][0-9]*)|true|false|null|[{}\\[\\],:]|[ \t\r\n]+', text) %}
  {% do dpone_managed_check(tokens | join == text, field) %}
  {% set state = namespace(count=0) %}{% set stack = [] %}
  {% for token in tokens if token.strip() %}
    {% set state.count = state.count + 1 %}
    {% do dpone_managed_check(state.count <= 65536, field) %}
    {% if token in ['{', '['] %}
      {% do stack.append({'kind': token, 'keys': [], 'key': true}) %}
      {% do dpone_managed_check(stack | length <= 32, field) %}
    {% elif token in ['}', ']'] %}
      {% do dpone_managed_check(stack | length > 0, field) %}
      {% do dpone_managed_check(stack[-1].kind == ('{' if token == '}' else '['), field) %}
      {% do stack.pop() %}
    {% elif token == ',' and stack and stack[-1].kind == '{' %}
      {% do stack[-1].update({'key': true}) %}
    {% elif token[0] == '"' %}
      {% do dpone_managed_check(token | length <= 24578, field) %}
      {% set scalar = fromjson(token) %}
      {% do dpone_managed_text(scalar, field, empty=true) %}
      {% if stack and stack[-1].kind == '{' and stack[-1].key %}
        {% do dpone_managed_check(scalar not in stack[-1]['keys'], field) %}
        {% do stack[-1]['keys'].append(scalar) %}{% do stack[-1].update({'key': false}) %}
      {% endif %}
    {% elif token not in [':', ',', 'true', 'false', 'null'] %}
      {% do dpone_managed_check(token.replace('-', '') | length <= 128, field) %}
    {% endif %}
  {% endfor %}
  {% do dpone_managed_check(not stack, field) %}
  {% set value = fromjson(text) %}
  {% do dpone_managed_check(value is mapping, field) %}
  {% do return(value) %}
{% endmacro %}

{% macro dpone_managed_columns(kind) %}
  {% set common = ['wire_version', 'registration_id', 'registration_digest', 'generation_id', 'executor_invocation_id'] %}
  {% if kind == 'attach' %}
    {% do return(common + ['plan_set_sha256', 'model_unique_id', 'model_plan_sha256', 'session_registration_id', 'session_id', 'guard_epoch', 'source_revision', 'control_program_sha256', 'executor_json', 'plan_json']) %}
  {% endif %}
  {% do dpone_managed_check(kind == 'transaction', 'wire kind') %}
  {% do return(common + ['model_unique_id', 'model_plan_sha256', 'session_registration_id', 'session_id', 'transaction_id', 'transaction_count', 'transaction_state']) %}
{% endmacro %}

{% macro dpone_managed_row(table, kind) %}
  {% set fields = dpone_managed_columns(kind) %}
  {% do dpone_managed_check(table is not none and table.column_names | list == fields and table.rows | length == 1, 'result shape') %}
  {% do dpone_managed_check(table.rows[0] | length == fields | length, 'row width') %}
  {% set value = {} %}
  {% for field in fields %}
    {% set item = table.rows[0][loop.index0] %}
    {% do dpone_managed_check(item is not none, field) %}{% do value.update({field: item}) %}
  {% endfor %}
  {% do dpone_managed_validate_row(value, kind) %}
  {% do return(value) %}
{% endmacro %}

{% macro dpone_managed_validate_row(value, kind) %}
  {% do dpone_managed_shape(value, dpone_managed_columns(kind), 'observation') %}
  {% do dpone_managed_integer(value.wire_version, 'wire_version', 1, 1) %}
  {% for field in ['registration_id', 'generation_id', 'executor_invocation_id', 'session_registration_id'] %}{% do dpone_managed_uuid(value[field], field) %}{% endfor %}
  {% for field in ['registration_digest', 'model_plan_sha256'] %}{% do dpone_managed_digest(value[field], field) %}{% endfor %}
  {% do dpone_managed_integer(value.session_id, 'session_id', 1, 2147483647) %}
  {% do dpone_managed_text(value.model_unique_id, 'model_unique_id') %}
  {% if kind == 'attach' %}
    {% for field in ['plan_set_sha256', 'control_program_sha256'] %}{% do dpone_managed_digest(value[field], field) %}{% endfor %}
    {% do dpone_managed_integer(value.guard_epoch, 'guard_epoch') %}
    {% do dpone_managed_integer(value.source_revision, 'source_revision', 2) %}
    {% do dpone_managed_payloads(value) %}
  {% else %}
    {% do dpone_managed_integer(value.transaction_id, 'transaction_id') %}
    {% do dpone_managed_integer(value.transaction_count, 'transaction_count', 1, 1) %}
    {% do dpone_managed_integer(value.transaction_state, 'transaction_state', 1, 1) %}
  {% endif %}
{% endmacro %}

{% macro dpone_managed_payloads(row) %}
  {% set executor = dpone_managed_json(row.executor_json, 'executor_json') %}
  {% do dpone_managed_shape(executor, ['schema', 'generation_id', 'guard_epoch', 'invocation_id', 'reservation', 'profile', 'command'], 'executor') %}
  {% do dpone_managed_check(executor.schema == 'dpone.native-source-executor-binding.v1', 'executor schema') %}
  {% do dpone_managed_check(executor.guard_epoch is integer and executor.guard_epoch == row.guard_epoch and executor.generation_id == row.generation_id and executor.invocation_id == row.executor_invocation_id, 'executor identity') %}
  {% for field in ['reservation', 'profile', 'command'] %}{% do dpone_managed_reference(executor[field], field) %}{% endfor %}
  {% set plan = dpone_managed_json(row.plan_json, 'plan_json') %}
  {% do dpone_managed_shape(plan, ['schema', 'generation_id', 'spec', 'predecessor', 'candidate_name', 'helper_name', 'backup_name', 'columnstore_index_name', 'model_plan_sha256'], 'plan') %}
  {% do dpone_managed_check(plan.schema == 'dpone.mssql-physical-model-plan.v1' and plan.generation_id == row.generation_id and plan.model_plan_sha256 == row.model_plan_sha256, 'plan identity') %}
  {% set spec = plan.spec %}
  {% do dpone_managed_shape(spec, ['schema', 'model_unique_id', 'source_graph_sha256', 'relation', 'columns', 'layout', 'physical_policy', 'filegroup', 'resource_bounds', 'model_spec_sha256'], 'spec') %}
  {% do dpone_managed_check(spec.schema == 'dpone.mssql-physical-model-spec.v1' and spec.model_unique_id == row.model_unique_id and spec.physical_policy == 'sqlserver-table-physical-v1', 'spec identity') %}
  {% for field in ['source_graph_sha256', 'model_spec_sha256'] %}{% do dpone_managed_digest(spec[field], field) %}{% endfor %}
  {% do dpone_managed_reference(spec.resource_bounds, 'resource_bounds') %}
  {% do dpone_managed_shape(spec.relation, ['database', 'schema', 'table'], 'relation') %}
  {% for name in spec.relation.values() %}{% do dpone_managed_identifier(name, 'relation identifier') %}{% endfor %}
  {% do dpone_managed_shape(spec.filegroup, ['data_space_id', 'name'], 'filegroup') %}
  {% do dpone_managed_check(spec.filegroup.data_space_id is integer, 'data_space_id') %}
  {% do dpone_managed_integer(spec.filegroup.data_space_id, 'data_space_id', 1, 2147483647) %}
  {% do dpone_managed_identifier(spec.filegroup.name, 'filegroup name') %}
  {% do dpone_managed_check(spec.layout in ['rowstore_none', 'rowstore_row', 'rowstore_page', 'columnstore'], 'layout') %}
  {% do dpone_managed_check(spec.columns is sequence and spec.columns is not string and spec.columns is not mapping and spec.columns | length > 0, 'columns') %}
  {% for column in spec.columns %}
    {% do dpone_managed_shape(column, ['name', 'dtype', 'nullable', 'collation'], 'column') %}
    {% do dpone_managed_identifier(column.name, 'column name') %}
    {% do dpone_managed_text(column.dtype, 'column dtype') %}
    {% do dpone_managed_check(column.nullable is boolean, 'column nullable') %}
    {% if column.collation is not none %}{% do dpone_managed_identifier(column.collation, 'column collation') %}{% endif %}
  {% endfor %}
  {% do dpone_managed_check(plan.predecessor is mapping and plan.predecessor.kind in ['ABSENT', 'MANAGED'], 'predecessor') %}
  {% if plan.predecessor.kind == 'ABSENT' %}
    {% do dpone_managed_shape(plan.predecessor, ['kind'], 'predecessor') %}
    {% do dpone_managed_check(plan.backup_name is none, 'backup_name') %}
  {% else %}
    {% do dpone_managed_shape(plan.predecessor, ['kind', 'object_id', 'object_create_time', 'local_receipt'], 'predecessor') %}
    {% do dpone_managed_check(plan.predecessor.object_id is integer, 'predecessor object_id') %}
    {% do dpone_managed_integer(plan.predecessor.object_id, 'predecessor object_id', 1, 2147483647) %}
    {% do dpone_managed_text(plan.predecessor.object_create_time, 'object_create_time', 27) %}
    {% do dpone_managed_check(modules.re.fullmatch('[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{7}', plan.predecessor.object_create_time), 'object_create_time') %}
    {% do dpone_managed_reference(plan.predecessor.local_receipt, 'local_receipt') %}
    {% do dpone_managed_identifier(plan.backup_name, 'backup_name') %}
  {% endif %}
  {% for field in ['candidate_name', 'helper_name'] %}{% do dpone_managed_identifier(plan[field], field) %}{% endfor %}
  {% if spec.layout == 'columnstore' %}{% do dpone_managed_identifier(plan.columnstore_index_name, 'columnstore_index_name') %}
  {% else %}{% do dpone_managed_check(plan.columnstore_index_name is none, 'columnstore_index_name') %}{% endif %}
{% endmacro %}

{% macro dpone_managed_compare(value, expected, fields) %}
  {% for field in fields %}{% do dpone_managed_check(value[field] == expected[field], field ~ ' mismatch') %}{% endfor %}
{% endmacro %}
