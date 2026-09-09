{% macro get_incremental_dpone_scope_merge_sql(arg_dict) %}
  {% set scope_map = var('dpone_semantic_refresh_scope_map', none) %}
  {% if scope_map is not mapping %}
    {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_SCOPE_MAP_REQUIRED: signed scope map is absent') }}
  {% endif %}
  {% set scope_map_fields = ['schema', 'scope_map_sha256', 'signature_sha256', 'verification_status', 'models', 'authorities'] | sort %}
  {% if scope_map.keys() | list | sort != scope_map_fields %}
    {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_SCOPE_MAP_INVALID: signed scope map is not closed') }}
  {% endif %}
  {% if scope_map['schema'] != 'dpone.semantic-refresh-mssql-scope-map.v1' or scope_map['verification_status'] != 'VERIFIED' %}
    {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_SCOPE_MAP_UNVERIFIED: signed scope map is not verified') }}
  {% endif %}
  {% do dpone_semantic_refresh_require_digest(scope_map['scope_map_sha256'], 'scope_map_sha256') %}
  {% do dpone_semantic_refresh_require_digest(scope_map['signature_sha256'], 'signature_sha256') %}
  {% if scope_map['models'] is not mapping or scope_map['authorities'] is not mapping or model.unique_id not in scope_map['models'] or model.unique_id not in scope_map['authorities'] %}
    {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_SCOPE_MAP_MODEL_MISSING: current model.unique_id is not authorized') }}
  {% endif %}
  {% if scope_map['models'].keys() | list | sort != scope_map['authorities'].keys() | list | sort %}
    {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_SCOPE_MAP_INVALID: authority closure differs from model closure') }}
  {% endif %}
  {% set attempt = scope_map['models'][model.unique_id] %}
  {% set strategy_authority_json = scope_map['authorities'][model.unique_id] %}
  {% if strategy_authority_json is not string or fromjson(strategy_authority_json) != attempt %}
    {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_SCOPE_MAP_INVALID: model binding differs from its canonical authority payload') }}
  {% endif %}
  {% if attempt is not mapping %}
    {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_ATTEMPT_REQUIRED: current model binding is absent') }}
  {% endif %}
  {% set required = [
      'after_image_relation', 'artifact_authority', 'attempt_binding_sha256',
      'before_image_relation', 'clickhouse_cluster_authority_id',
      'ddl_epoch', 'ddl_freeze_assurance_receipt_sha256', 'effective_key_mapping_sha256',
      'effective_key_template_sha256', 'effective_keys', 'event_time_column',
      'fencing_epoch', 'guard_relation', 'guard_resource_id',
      'model_unique_id', 'mssql_connection_authority_id',
      'mssql_target_authority_id', 'operation_id', 'operation_plan_sha256',
      'ordered_writable_schema_sha256', 'owner_id', 'receipt_relation',
      'replacement_action', 'resource_policy', 'route_certification_receipt_sha256',
      'schema', 'scope_end_utc', 'scope_image_namespace_policy_sha256',
      'scope_start_utc', 'strategy_template_sha256', 'target_relation',
      'writable_columns', 'writer_exclusivity_assurance_receipt_sha256',
      'workflow_execution_binding_sha256', 'workflow_execution_id'
  ] %}
  {% if 'utc_semantics_assurance_receipt_sha256' in attempt %}
    {% do required.append('utc_semantics_assurance_receipt_sha256') %}
  {% endif %}
  {% if attempt['replacement_action'] | default('BLOCK') == 'RESTORE_THEN_REBUILD' %}
    {% do required.extend([
        'predecessor_receipt_relation', 'predecessor_operation_id',
        'predecessor_operation_plan_sha256', 'predecessor_attempt_binding_sha256',
        'predecessor_fencing_epoch', 'predecessor_before_image_relation',
        'predecessor_before_image_sha256', 'predecessor_after_image_relation',
        'predecessor_after_image_sha256'
    ]) %}
  {% endif %}
  {% if attempt.keys() | list | sort != required | sort %}
    {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_ATTEMPT_INVALID: current model binding is not closed') }}
  {% endif %}
  {% if attempt['schema'] != 'dpone.semantic-refresh-mssql-attempt-strategy-authority.v1' %}
    {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_ATTEMPT_INVALID: strategy authority schema is unsupported') }}
  {% endif %}
  {% if attempt['replacement_action'] not in ['BUILD_FRESH', 'RESTORE_THEN_REBUILD'] %}
    {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_ATTEMPT_INVALID: replacement action is blocked or unsupported') }}
  {% endif %}
  {% if attempt['model_unique_id'] != model.unique_id %}
    {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_SCOPE_MAP_MODEL_MISSING: current model identity differs from its authority payload') }}
  {% endif %}
  {% for field in [
      'workflow_execution_id', 'owner_id', 'guard_resource_id',
      'scope_start_utc', 'scope_end_utc', 'event_time_column',
      'mssql_connection_authority_id', 'mssql_target_authority_id',
      'clickhouse_cluster_authority_id'
  ] %}
    {% if attempt[field] is not string or attempt[field] | length == 0 %}
      {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_ATTEMPT_INVALID: ' ~ field ~ ' must be a non-empty string') }}
    {% endif %}
  {% endfor %}
  {% do dpone_semantic_refresh_require_digest(attempt['operation_plan_sha256'], 'operation_plan_sha256') %}
  {% do dpone_semantic_refresh_require_digest(attempt['operation_id'], 'operation_id') %}
  {% do dpone_semantic_refresh_require_digest(attempt['attempt_binding_sha256'], 'attempt_binding_sha256') %}
  {% for field in [
      'workflow_execution_binding_sha256', 'effective_key_template_sha256',
      'effective_key_mapping_sha256', 'ordered_writable_schema_sha256',
      'route_certification_receipt_sha256',
      'writer_exclusivity_assurance_receipt_sha256',
      'ddl_freeze_assurance_receipt_sha256', 'scope_image_namespace_policy_sha256',
      'strategy_template_sha256'
  ] %}
    {% do dpone_semantic_refresh_require_digest(attempt[field], field) %}
  {% endfor %}
  {% if attempt['fencing_epoch'] is boolean or attempt['fencing_epoch'] is not integer or attempt['fencing_epoch'] <= 0 %}
    {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_ATTEMPT_INVALID: fencing_epoch must be a positive integer') }}
  {% endif %}
  {% if attempt['ddl_epoch'] is boolean or attempt['ddl_epoch'] is not integer or attempt['ddl_epoch'] <= 0 %}
    {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_ATTEMPT_INVALID: ddl_epoch must be a positive integer') }}
  {% endif %}
  {% set guard_relation = dpone_semantic_refresh_relation(attempt['guard_relation'], 'guard_relation') %}
  {% set ddl_epoch_relation = dpone_semantic_refresh_relation({
      'database': attempt['guard_relation']['database'],
      'schema': attempt['guard_relation']['schema'],
      'identifier': 'semantic_refresh_ddl_epoch'
  }, 'ddl_epoch_relation') %}
  {% set receipt_relation = dpone_semantic_refresh_relation(attempt['receipt_relation'], 'receipt_relation') %}
  {% set before_image_relation = dpone_semantic_refresh_relation(attempt['before_image_relation'], 'before_image_relation') %}
  {% set after_image_relation = dpone_semantic_refresh_relation(attempt['after_image_relation'], 'after_image_relation') %}
  {% set authorized_target_relation = dpone_semantic_refresh_relation(attempt['target_relation'], 'target_relation') %}
  {% set target_resource_id = attempt['target_relation']['database'] ~ '.' ~ attempt['target_relation']['schema'] ~ '.' ~ attempt['target_relation']['identifier'] %}
  {% set mssql_target_authority_id = 'mssql://' ~ attempt['mssql_connection_authority_id'] ~ '/' ~ target_resource_id %}
  {% if attempt['guard_resource_id'] != target_resource_id
        or attempt['mssql_target_authority_id'] != mssql_target_authority_id %}
    {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_ATTEMPT_INVALID: workflow or physical MSSQL target authority differs') }}
  {% endif %}
  {% do dpone_semantic_refresh_require_resource_policy(attempt['resource_policy']) %}
  {% do dpone_semantic_refresh_require_artifact_authority(attempt['artifact_authority']) %}
  {% set runtime_statement_timeout_seconds = var('dpone_semantic_refresh_statement_timeout_seconds', none) %}
  {% if runtime_statement_timeout_seconds is boolean
        or runtime_statement_timeout_seconds is not integer
        or runtime_statement_timeout_seconds != attempt['resource_policy']['max_statement_seconds'] %}
    {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_RESOURCE_BUDGET_UNVERIFIED: ODBC query timeout differs from governed statement budget') }}
  {% endif %}

  {% set unique_key = arg_dict['unique_key'] %}
  {% if unique_key is string %}
    {% set unique_keys = [unique_key] %}
  {% elif unique_key is sequence and unique_key | length > 0 %}
    {% set unique_keys = unique_key %}
  {% else %}
    {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_KEY_REQUIRED: unique_key must be a non-empty string or list') }}
  {% endif %}
  {% set key_specs = attempt['effective_keys'] %}
  {% if key_specs is not sequence or key_specs | length != unique_keys | length %}
    {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_KEY_INVALID: effective-key metadata differs from unique_key') }}
  {% endif %}
  {% for index in range(unique_keys | length) %}
    {% if key_specs[index] is not mapping %}
      {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_KEY_INVALID: effective-key entry is not closed') }}
    {% endif %}
    {% if key_specs[index]['data_type'] | default('') == 'decimal' %}
      {% if key_specs[index].keys() | list | sort != ['data_type', 'name', 'precision', 'scale'] %}
        {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_KEY_INVALID: decimal key entry is not closed') }}
      {% endif %}
      {% if key_specs[index]['precision'] is boolean or key_specs[index]['precision'] is not integer or key_specs[index]['precision'] < 1 or key_specs[index]['precision'] > 38 %}
        {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_KEY_INVALID: decimal precision must be from 1 through 38') }}
      {% endif %}
      {% if key_specs[index]['scale'] is boolean or key_specs[index]['scale'] is not integer or key_specs[index]['scale'] < 0 or key_specs[index]['scale'] > key_specs[index]['precision'] %}
        {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_KEY_INVALID: decimal scale must be from 0 through precision') }}
      {% endif %}
    {% elif key_specs[index]['data_type'] | default('') == 'date' %}
      {% if key_specs[index].keys() | list | sort != ['data_type', 'domain_max', 'domain_min', 'name']
            or key_specs[index]['domain_min'] != '1970-01-01'
            or key_specs[index]['domain_max'] != '2149-06-06' %}
        {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_KEY_INVALID: date key domain differs from protected policy') }}
      {% endif %}
    {% elif key_specs[index]['data_type'] | default('') == 'datetime2(6)' %}
      {% if key_specs[index].keys() | list | sort != ['data_type', 'domain_max', 'domain_min', 'name', 'utc_assurance_sha256']
            or key_specs[index]['domain_min'] != '1900-01-01T00:00:00.000000Z'
            or key_specs[index]['domain_max'] != '2299-12-31T23:59:59.999999Z' %}
        {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_KEY_INVALID: datetime key domain differs from protected policy') }}
      {% endif %}
      {% do dpone_semantic_refresh_require_digest(key_specs[index]['utc_assurance_sha256'], 'utc_assurance_sha256') %}
    {% elif key_specs[index].keys() | list | sort != ['data_type', 'name'] %}
      {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_KEY_INVALID: effective-key entry is not closed') }}
    {% endif %}
    {% if key_specs[index]['name'] != unique_keys[index] %}
      {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_KEY_INVALID: effective-key order differs from unique_key') }}
    {% endif %}
    {% if key_specs[index]['data_type'] not in ['bit', 'tinyint', 'smallint', 'int', 'bigint', 'uniqueidentifier', 'date', 'datetime2(6)', 'decimal'] %}
      {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_KEY_INVALID: effective-key type is unsupported') }}
    {% endif %}
  {% endfor %}

  {% if 'utc_semantics_assurance_receipt_sha256' in attempt %}
    {% do dpone_semantic_refresh_require_digest(attempt['utc_semantics_assurance_receipt_sha256'], 'utc_semantics_assurance_receipt_sha256') %}
  {% endif %}
  {% set datetime_keys = key_specs | selectattr('data_type', 'equalto', 'datetime2(6)') | list %}
  {% if (datetime_keys | length > 0) != ('utc_semantics_assurance_receipt_sha256' in attempt) %}
    {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_KEY_INVALID: UTC assurance closure differs from effective keys') }}
  {% endif %}
  {% for spec in datetime_keys %}
    {% if spec['utc_assurance_sha256'] != attempt['utc_semantics_assurance_receipt_sha256'] %}
      {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_KEY_INVALID: UTC key assurance differs from protected authority') }}
    {% endif %}
  {% endfor %}

  {% set destination_columns = arg_dict['dest_columns'] | map(attribute='name') | list %}
  {% do dpone_semantic_refresh_require_writable_columns(attempt['writable_columns'], destination_columns) %}
  {% if attempt['event_time_column'] not in destination_columns %}
    {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_SCOPE_INVALID: event-time column is absent from the enforced contract') }}
  {% endif %}
  {% if attempt['event_time_column'] not in unique_keys %}
    {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_EVENT_TIME_KEY_REQUIRED: event time must be an effective-key component') }}
  {% endif %}
  {% for spec in key_specs %}
    {% if spec['name'] == attempt['event_time_column'] and spec['data_type'] not in ['date', 'datetime2(6)'] %}
      {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_EVENT_TIME_KEY_REQUIRED: event time must use date or datetime2(6)') }}
    {% endif %}
  {% endfor %}
  {% for key in unique_keys %}
    {% if key not in destination_columns %}
      {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_KEY_INVALID: effective key is absent from the enforced contract') }}
    {% endif %}
  {% endfor %}

  {% set quoted_columns = [] %}
  {% set update_assignments = [] %}
  {% set bounded_row_byte_terms = [] %}
  {% for column in destination_columns %}
    {% do quoted_columns.append(adapter.quote(column)) %}
    {% do bounded_row_byte_terms.append(
        '(CONVERT(bigint, 512) + (CONVERT(bigint, COALESCE(DATALENGTH(' ~ adapter.quote(column) ~ '), 0)) * 6))'
    ) %}
    {% if column not in unique_keys %}
      {% do update_assignments.append('target.' ~ adapter.quote(column) ~ ' = source.' ~ adapter.quote(column)) %}
    {% endif %}
  {% endfor %}
  {% if update_assignments | length == 0 %}
    {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_PAYLOAD_REQUIRED: at least one non-key column is required') }}
  {% endif %}

  {% set join_parts = [] %}
  {% set source_group = [] %}
  {% set key_order = [] %}
  {% for key in unique_keys %}
    {% do join_parts.append('target.' ~ adapter.quote(key) ~ ' = source.' ~ adapter.quote(key)) %}
    {% do source_group.append(adapter.quote(key)) %}
    {% set spec = key_specs[loop.index0] %}
    {% if spec['data_type'] == 'uniqueidentifier' %}
      {% do key_order.append('CONVERT(char(36), ' ~ adapter.quote(key) ~ ')') %}
    {% else %}
      {% do key_order.append(adapter.quote(key)) %}
    {% endif %}
  {% endfor %}

  {% set target_relation = arg_dict['target_relation'] %}
  {% if target_relation | string != authorized_target_relation | string %}
    {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_ATTEMPT_INVALID: dbt target differs from protected target relation') }}
  {% endif %}
  {% set temp_relation = arg_dict['temp_relation'] %}
  {% set event_time = adapter.quote(attempt['event_time_column']) %}
  {% set column_csv = quoted_columns | join(', ') %}
  {% set key_csv = source_group | join(', ') %}
  {% set join_sql = join_parts | join(' AND ') %}
  {% set update_sql = update_assignments | join(', ') %}
  {% set bounded_row_bytes = bounded_row_byte_terms | join(' + ') %}
  {% set scope_start = dpone_semantic_refresh_literal(attempt['scope_start_utc']) %}
  {% set scope_end = dpone_semantic_refresh_literal(attempt['scope_end_utc']) %}

  {% if attempt['replacement_action'] == 'RESTORE_THEN_REBUILD' %}
    {% set restore_sql = semantic_refresh_restore_predecessor_scope(
        target_relation,
        destination_columns,
        unique_keys,
        attempt
    ) %}
  {% else %}
    {% set restore_sql = '' %}
  {% endif %}

  {% set sql %}
SET NOCOUNT ON;
SET XACT_ABORT ON;
SET LOCK_TIMEOUT {{ attempt['resource_policy']['max_statement_seconds'] * 1000 }};
IF @@TRANCOUNT = 0
    THROW 51000, 'DPONE_SEMANTIC_REFRESH_TRANSACTION_REQUIRED', 1;
DECLARE @dpone_attempt_context_info varbinary(128) = HASHBYTES(
    'SHA2_256',
    CONVERT(varbinary(max), {{ dpone_semantic_refresh_literal(attempt['attempt_binding_sha256']) }})
);
SET CONTEXT_INFO @dpone_attempt_context_info;
IF TRY_CONVERT(datetime2(6), {{ scope_start }}, 126) IS NULL
   OR TRY_CONVERT(datetime2(6), {{ scope_end }}, 126) IS NULL
   OR CONVERT(time(6), TRY_CONVERT(datetime2(6), {{ scope_start }}, 126)) <> CONVERT(time(6), '00:00:00')
   OR CONVERT(time(6), TRY_CONVERT(datetime2(6), {{ scope_end }}, 126)) <> CONVERT(time(6), '00:00:00')
   OR DATEDIFF(day, TRY_CONVERT(datetime2(6), {{ scope_start }}, 126), TRY_CONVERT(datetime2(6), {{ scope_end }}, 126)) <> 1
    THROW 51014, 'DPONE_SEMANTIC_REFRESH_SCOPE_INVALID', 1;

DECLARE @dpone_lock_result int;
EXEC @dpone_lock_result = sys.sp_getapplock
    @Resource = {{ dpone_semantic_refresh_literal('dpone:semantic-refresh:' ~ attempt['guard_resource_id']) }},
    @LockMode = N'Exclusive', @LockOwner = N'Transaction', @LockTimeout = 0;
IF @dpone_lock_result < 0
    THROW 51001, 'DPONE_SEMANTIC_REFRESH_FENCE_LOCK_FAILED', 1;
DECLARE @dpone_ddl_lock_result int;
EXEC @dpone_ddl_lock_result = sys.sp_getapplock
    @Resource = N'dpone:semantic-refresh:ddl-freeze',
    @LockMode = N'Shared', @LockOwner = N'Transaction', @LockTimeout = 0;
IF @dpone_ddl_lock_result < 0
    THROW 51025, 'DPONE_SEMANTIC_REFRESH_DDL_FREEZE_LOCK_FAILED', 1;
DECLARE @dpone_current_ddl_epoch bigint;
SELECT @dpone_current_ddl_epoch = current_epoch
FROM {{ ddl_epoch_relation }} WITH (UPDLOCK, HOLDLOCK)
WHERE singleton_id = 1;
IF ISNULL(@dpone_current_ddl_epoch, -1) <> {{ attempt['ddl_epoch'] }}
    THROW 51026, 'DPONE_SEMANTIC_REFRESH_DDL_EPOCH_DRIFT', 1;
DECLARE @dpone_scope_lock_started_at datetime2(6) = SYSUTCDATETIME();
DECLARE @dpone_statement_started_at datetime2(6);
DECLARE @dpone_log_free_bytes bigint;
DECLARE @dpone_transaction_log_bytes bigint;
DECLARE @dpone_version_store_bytes bigint;

DECLARE @dpone_actual_resource_id nvarchar(512);
DECLARE @dpone_actual_workflow_id nvarchar(512);
DECLARE @dpone_actual_owner_id nvarchar(512);
DECLARE @dpone_actual_operation_id nvarchar(512);
DECLARE @dpone_actual_operation_plan_sha256 varchar(71);
DECLARE @dpone_actual_attempt_binding_sha256 varchar(71);
DECLARE @dpone_actual_strategy_authority_sha256 varchar(71);
DECLARE @dpone_actual_fencing_epoch bigint;
DECLARE @dpone_actual_guard_status nvarchar(32);
SELECT
    @dpone_actual_resource_id = resource_id,
    @dpone_actual_workflow_id = workflow_id,
    @dpone_actual_owner_id = owner_id,
    @dpone_actual_operation_id = operation_id,
    @dpone_actual_operation_plan_sha256 = operation_plan_sha256,
    @dpone_actual_attempt_binding_sha256 = attempt_binding_sha256,
    @dpone_actual_strategy_authority_sha256 = strategy_authority_sha256,
    @dpone_actual_fencing_epoch = fencing_epoch,
    @dpone_actual_guard_status = status
FROM {{ guard_relation }} WITH (UPDLOCK, HOLDLOCK)
WHERE resource_id = {{ dpone_semantic_refresh_literal(attempt['guard_resource_id']) }};
IF @dpone_actual_resource_id IS NULL
    THROW 51002, 'DPONE_SEMANTIC_REFRESH_FENCE_REJECTED', 1;
IF ISNULL(@dpone_actual_workflow_id, N'') COLLATE Latin1_General_100_BIN2 <> {{ dpone_semantic_refresh_literal(attempt['workflow_execution_id']) }} COLLATE Latin1_General_100_BIN2
   OR ISNULL(@dpone_actual_owner_id, N'') COLLATE Latin1_General_100_BIN2 <> {{ dpone_semantic_refresh_literal(attempt['owner_id']) }} COLLATE Latin1_General_100_BIN2
   OR ISNULL(@dpone_actual_operation_id, N'') COLLATE Latin1_General_100_BIN2 <> {{ dpone_semantic_refresh_literal(attempt['operation_id']) }} COLLATE Latin1_General_100_BIN2
   OR ISNULL(@dpone_actual_operation_plan_sha256, '') COLLATE Latin1_General_100_BIN2 <> {{ dpone_semantic_refresh_literal(attempt['operation_plan_sha256']) }} COLLATE Latin1_General_100_BIN2
   OR ISNULL(@dpone_actual_attempt_binding_sha256, '') COLLATE Latin1_General_100_BIN2 <> {{ dpone_semantic_refresh_literal(attempt['attempt_binding_sha256']) }} COLLATE Latin1_General_100_BIN2
   OR ISNULL(@dpone_actual_strategy_authority_sha256, '') COLLATE Latin1_General_100_BIN2 <> ('sha256:' + LOWER(CONVERT(varchar(64), HASHBYTES('SHA2_256', {{ dpone_semantic_refresh_literal(strategy_authority_json) }}), 2))) COLLATE Latin1_General_100_BIN2
   OR ISNULL(@dpone_actual_fencing_epoch, -1) <> {{ attempt['fencing_epoch'] }}
   OR ISNULL(@dpone_actual_guard_status, N'') <> N'HELD'
    THROW 51002, 'DPONE_SEMANTIC_REFRESH_FENCE_REJECTED', 1;

IF EXISTS (
    SELECT 1 FROM {{ receipt_relation }} WITH (UPDLOCK, HOLDLOCK)
    WHERE operation_id COLLATE Latin1_General_100_BIN2 = {{ dpone_semantic_refresh_literal(attempt['operation_id']) }} COLLATE Latin1_General_100_BIN2
      AND (
          operation_plan_sha256 COLLATE Latin1_General_100_BIN2 <> {{ dpone_semantic_refresh_literal(attempt['operation_plan_sha256']) }} COLLATE Latin1_General_100_BIN2
          OR attempt_binding_sha256 COLLATE Latin1_General_100_BIN2 <> {{ dpone_semantic_refresh_literal(attempt['attempt_binding_sha256']) }} COLLATE Latin1_General_100_BIN2
          OR fencing_epoch <> {{ attempt['fencing_epoch'] }}
      )
)
    THROW 51003, 'DPONE_SEMANTIC_REFRESH_RECEIPT_CONFLICT', 1;

DECLARE @dpone_existing_receipt_count bigint;
SELECT @dpone_existing_receipt_count = COUNT_BIG(*)
FROM {{ receipt_relation }} WITH (UPDLOCK, HOLDLOCK)
WHERE operation_id COLLATE Latin1_General_100_BIN2 = {{ dpone_semantic_refresh_literal(attempt['operation_id']) }} COLLATE Latin1_General_100_BIN2
  AND operation_plan_sha256 COLLATE Latin1_General_100_BIN2 = {{ dpone_semantic_refresh_literal(attempt['operation_plan_sha256']) }} COLLATE Latin1_General_100_BIN2
  AND attempt_binding_sha256 COLLATE Latin1_General_100_BIN2 = {{ dpone_semantic_refresh_literal(attempt['attempt_binding_sha256']) }} COLLATE Latin1_General_100_BIN2
  AND fencing_epoch = {{ attempt['fencing_epoch'] }};
IF @dpone_existing_receipt_count > 1
    THROW 51003, 'DPONE_SEMANTIC_REFRESH_RECEIPT_CONFLICT', 1;

IF @dpone_existing_receipt_count = 1
BEGIN
    DECLARE @dpone_existing_before_relation nvarchar(776);
    DECLARE @dpone_existing_after_relation nvarchar(776);
    DECLARE @dpone_existing_before_sha varchar(71);
    DECLARE @dpone_existing_after_sha varchar(71);
    DECLARE @dpone_existing_updated_count bigint;
    DECLARE @dpone_existing_inserted_count bigint;
    DECLARE @dpone_existing_build_receipt_sha varchar(71);
    DECLARE @dpone_reconciled_before_json nvarchar(max);
    DECLARE @dpone_reconciled_after_json nvarchar(max);
    DECLARE @dpone_reconciled_before_rows bigint;
    DECLARE @dpone_reconciled_after_rows bigint;
    DECLARE @dpone_reconciled_before_preflight_bytes bigint;
    DECLARE @dpone_reconciled_after_preflight_bytes bigint;
    DECLARE @dpone_current_target_rows bigint;
    DECLARE @dpone_current_target_preflight_bytes bigint;
    DECLARE @dpone_reconciled_build_receipt_json nvarchar(max);
    SELECT
        @dpone_existing_before_relation = before_image_relation,
        @dpone_existing_before_sha = before_image_sha256,
        @dpone_existing_after_relation = after_image_relation,
        @dpone_existing_after_sha = after_image_sha256,
        @dpone_existing_updated_count = updated_count,
        @dpone_existing_inserted_count = inserted_count,
        @dpone_existing_build_receipt_sha = build_receipt_sha256
    FROM {{ receipt_relation }} WITH (UPDLOCK, HOLDLOCK)
    WHERE operation_id COLLATE Latin1_General_100_BIN2 = {{ dpone_semantic_refresh_literal(attempt['operation_id']) }} COLLATE Latin1_General_100_BIN2
      AND operation_plan_sha256 COLLATE Latin1_General_100_BIN2 = {{ dpone_semantic_refresh_literal(attempt['operation_plan_sha256']) }} COLLATE Latin1_General_100_BIN2
      AND attempt_binding_sha256 COLLATE Latin1_General_100_BIN2 = {{ dpone_semantic_refresh_literal(attempt['attempt_binding_sha256']) }} COLLATE Latin1_General_100_BIN2
      AND fencing_epoch = {{ attempt['fencing_epoch'] }};
    IF @dpone_existing_before_relation <> {{ dpone_semantic_refresh_literal(before_image_relation | string) }}
       OR @dpone_existing_after_relation <> {{ dpone_semantic_refresh_literal(after_image_relation | string) }}
       OR OBJECT_ID({{ dpone_semantic_refresh_literal(before_image_relation | string) }}, N'U') IS NULL
       OR OBJECT_ID({{ dpone_semantic_refresh_literal(after_image_relation | string) }}, N'U') IS NULL
        THROW 51013, 'DPONE_SEMANTIC_REFRESH_RECEIPT_IMAGE_CONFLICT', 1;
    SELECT @dpone_reconciled_before_rows = COUNT_BIG(*),
           @dpone_reconciled_before_preflight_bytes = COALESCE(SUM({{ bounded_row_bytes }}), 2)
    FROM {{ before_image_relation }};
    SELECT @dpone_reconciled_after_rows = COUNT_BIG(*),
           @dpone_reconciled_after_preflight_bytes = COALESCE(SUM({{ bounded_row_bytes }}), 2)
    FROM {{ after_image_relation }};
    IF @dpone_reconciled_before_rows > {{ attempt['resource_policy']['max_before_image_rows'] }}
       OR @dpone_reconciled_after_rows > {{ attempt['resource_policy']['max_after_image_rows'] }}
       OR @dpone_reconciled_before_preflight_bytes > {{ attempt['resource_policy']['max_before_image_bytes'] }}
       OR @dpone_reconciled_after_preflight_bytes > {{ attempt['resource_policy']['max_after_image_bytes'] }}
       OR @dpone_reconciled_before_preflight_bytes > 67108864
       OR @dpone_reconciled_after_preflight_bytes > 67108864
        THROW 51022, 'DPONE_SEMANTIC_REFRESH_RESOURCE_BUDGET_EXCEEDED', 1;
    SELECT @dpone_reconciled_before_json = (
        SELECT {{ column_csv }} FROM {{ before_image_relation }}
        ORDER BY {{ key_order | join(', ') }} FOR JSON PATH, INCLUDE_NULL_VALUES
    );
    SELECT @dpone_reconciled_after_json = (
        SELECT {{ column_csv }} FROM {{ after_image_relation }}
        ORDER BY {{ key_order | join(', ') }} FOR JSON PATH, INCLUDE_NULL_VALUES
    );
    IF @dpone_existing_before_sha COLLATE Latin1_General_100_BIN2 <> ('sha256:' + LOWER(CONVERT(varchar(64), HASHBYTES('SHA2_256', COALESCE(@dpone_reconciled_before_json, N'[]')), 2))) COLLATE Latin1_General_100_BIN2
       OR @dpone_existing_after_sha COLLATE Latin1_General_100_BIN2 <> ('sha256:' + LOWER(CONVERT(varchar(64), HASHBYTES('SHA2_256', COALESCE(@dpone_reconciled_after_json, N'[]')), 2))) COLLATE Latin1_General_100_BIN2
        THROW 51013, 'DPONE_SEMANTIC_REFRESH_RECEIPT_IMAGE_CONFLICT', 1;
    SET @dpone_statement_started_at = SYSUTCDATETIME();
    SELECT @dpone_current_target_rows = COUNT_BIG(*),
           @dpone_current_target_preflight_bytes = COALESCE(SUM({{ bounded_row_bytes }}), 2)
    FROM {{ target_relation }} WITH (UPDLOCK, HOLDLOCK)
    WHERE {{ event_time }} >= {{ scope_start }} AND {{ event_time }} < {{ scope_end }};
    IF @dpone_current_target_rows > {{ attempt['resource_policy']['max_target_scope_rows'] }}
       OR @dpone_current_target_preflight_bytes > {{ attempt['resource_policy']['max_after_image_bytes'] }}
       OR @dpone_current_target_preflight_bytes > 67108864
        THROW 51022, 'DPONE_SEMANTIC_REFRESH_RESOURCE_BUDGET_EXCEEDED', 1;
    IF EXISTS (
        SELECT {{ column_csv }}, COUNT_BIG(*) AS dpone_multiplicity
        FROM {{ target_relation }} WITH (UPDLOCK, HOLDLOCK)
        WHERE {{ event_time }} >= {{ scope_start }} AND {{ event_time }} < {{ scope_end }}
        GROUP BY {{ column_csv }}
        EXCEPT
        SELECT {{ column_csv }}, COUNT_BIG(*) AS dpone_multiplicity
        FROM {{ after_image_relation }} GROUP BY {{ column_csv }}
    ) OR EXISTS (
        SELECT {{ column_csv }}, COUNT_BIG(*) AS dpone_multiplicity
        FROM {{ after_image_relation }} GROUP BY {{ column_csv }}
        EXCEPT
        SELECT {{ column_csv }}, COUNT_BIG(*) AS dpone_multiplicity
        FROM {{ target_relation }} WITH (UPDLOCK, HOLDLOCK)
        WHERE {{ event_time }} >= {{ scope_start }} AND {{ event_time }} < {{ scope_end }}
        GROUP BY {{ column_csv }}
    )
        THROW 51024, 'DPONE_DBT_MSSQL_RECEIPT_TARGET_DRIFT', 1;
    {{ dpone_semantic_refresh_statement_budget_gate(attempt['resource_policy']) }}
    {{ dpone_semantic_refresh_runtime_budget_gate(attempt['resource_policy']) }}
    SELECT @dpone_reconciled_build_receipt_json = (
        SELECT
            {{ dpone_semantic_refresh_literal(after_image_relation | string) }} AS [after_image_relation],
            @dpone_existing_after_sha AS [after_image_sha256],
            {{ dpone_semantic_refresh_literal(attempt['attempt_binding_sha256']) }} AS [attempt_binding_sha256],
            {{ dpone_semantic_refresh_literal(before_image_relation | string) }} AS [before_image_relation],
            @dpone_existing_before_sha AS [before_image_sha256],
            {{ attempt['fencing_epoch'] }} AS [fencing_epoch],
            @dpone_existing_inserted_count AS [inserted_count],
            {{ dpone_semantic_refresh_literal(attempt['model_unique_id']) }} AS [model_unique_id],
            {{ dpone_semantic_refresh_literal(attempt['operation_id']) }} AS [operation_id],
            {{ dpone_semantic_refresh_literal(attempt['operation_plan_sha256']) }} AS [operation_plan_sha256],
            N'dpone.semantic-refresh-model-build-receipt.v1' AS [schema],
            @dpone_actual_strategy_authority_sha256 AS [strategy_authority_sha256],
            @dpone_existing_updated_count AS [updated_count]
        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    );
    IF @dpone_existing_build_receipt_sha COLLATE Latin1_General_100_BIN2 <> ('sha256:' + LOWER(CONVERT(varchar(64), HASHBYTES('SHA2_256', @dpone_reconciled_build_receipt_json), 2))) COLLATE Latin1_General_100_BIN2
        THROW 51013, 'DPONE_SEMANTIC_REFRESH_RECEIPT_IMAGE_CONFLICT', 1;
END;

IF NOT EXISTS (
    SELECT 1 FROM {{ receipt_relation }} WITH (UPDLOCK, HOLDLOCK)
    WHERE operation_id COLLATE Latin1_General_100_BIN2 = {{ dpone_semantic_refresh_literal(attempt['operation_id']) }} COLLATE Latin1_General_100_BIN2
      AND operation_plan_sha256 COLLATE Latin1_General_100_BIN2 = {{ dpone_semantic_refresh_literal(attempt['operation_plan_sha256']) }} COLLATE Latin1_General_100_BIN2
      AND attempt_binding_sha256 COLLATE Latin1_General_100_BIN2 = {{ dpone_semantic_refresh_literal(attempt['attempt_binding_sha256']) }} COLLATE Latin1_General_100_BIN2
      AND fencing_epoch = {{ attempt['fencing_epoch'] }}
)
BEGIN
    {{ restore_sql }}

    DECLARE @dpone_dbt_temp_rows bigint;
    DECLARE @dpone_source_scope_rows bigint;
    DECLARE @dpone_target_scope_rows bigint;
    DECLARE @dpone_before_image_rows bigint;
    DECLARE @dpone_after_image_rows bigint;
    DECLARE @dpone_before_json nvarchar(max);
    DECLARE @dpone_after_json nvarchar(max);
    DECLARE @dpone_dbt_temp_bytes bigint;
    DECLARE @dpone_before_image_bytes bigint;
    DECLARE @dpone_after_image_bytes bigint;
    DECLARE @dpone_before_sha varchar(71);
    DECLARE @dpone_after_sha varchar(71);
    DECLARE @dpone_build_receipt_json nvarchar(max);
    DECLARE @dpone_build_receipt_sha varchar(71);

    SET @dpone_statement_started_at = SYSUTCDATETIME();
    SELECT @dpone_dbt_temp_rows = COUNT_BIG(*) FROM {{ temp_relation }};
    SELECT @dpone_source_scope_rows = COUNT_BIG(*) FROM {{ temp_relation }}
    WHERE {{ event_time }} >= {{ scope_start }} AND {{ event_time }} < {{ scope_end }};
    SELECT @dpone_target_scope_rows = COUNT_BIG(*) FROM {{ target_relation }} WITH (UPDLOCK, HOLDLOCK)
    WHERE {{ event_time }} >= {{ scope_start }} AND {{ event_time }} < {{ scope_end }};
    SELECT @dpone_dbt_temp_bytes = COALESCE(SUM({{ bounded_row_bytes }}), 2)
    FROM {{ temp_relation }};
    IF @dpone_dbt_temp_rows > {{ attempt['resource_policy']['max_dbt_temp_rows'] }}
       OR @dpone_source_scope_rows > {{ attempt['resource_policy']['max_source_scope_rows'] }}
       OR @dpone_target_scope_rows > {{ attempt['resource_policy']['max_target_scope_rows'] }}
       OR @dpone_dbt_temp_bytes > {{ attempt['resource_policy']['max_dbt_temp_bytes'] }}
       OR @dpone_dbt_temp_bytes > 67108864
        THROW 51022, 'DPONE_SEMANTIC_REFRESH_RESOURCE_BUDGET_EXCEEDED', 1;
    {{ dpone_semantic_refresh_statement_budget_gate(attempt['resource_policy']) }}
    {{ dpone_semantic_refresh_runtime_budget_gate(attempt['resource_policy']) }}

    -- DPONE_BEFORE_IMAGE: the full current logical scope is captured atomically.
    SET @dpone_statement_started_at = SYSUTCDATETIME();
    IF OBJECT_ID({{ dpone_semantic_refresh_literal(before_image_relation | string) }}, N'U') IS NOT NULL
        THROW 51004, 'DPONE_SEMANTIC_REFRESH_BEFORE_IMAGE_CONFLICT', 1;
    SELECT {{ column_csv }}
    INTO {{ before_image_relation }}
    FROM {{ target_relation }} WITH (UPDLOCK, HOLDLOCK)
    WHERE {{ event_time }} >= {{ scope_start }} AND {{ event_time }} < {{ scope_end }};
    SET @dpone_before_image_rows = @@ROWCOUNT;
    SELECT @dpone_before_image_bytes = COALESCE(SUM({{ bounded_row_bytes }}), 2)
    FROM {{ before_image_relation }};
    IF @dpone_before_image_rows > {{ attempt['resource_policy']['max_before_image_rows'] }}
       OR @dpone_before_image_bytes > {{ attempt['resource_policy']['max_before_image_bytes'] }}
       OR @dpone_before_image_bytes > 67108864
        THROW 51022, 'DPONE_SEMANTIC_REFRESH_RESOURCE_BUDGET_EXCEEDED', 1;
    SELECT @dpone_before_json = (
        SELECT {{ column_csv }} FROM {{ before_image_relation }}
        ORDER BY {{ key_order | join(', ') }} FOR JSON PATH, INCLUDE_NULL_VALUES
    );
    SET @dpone_before_image_bytes = DATALENGTH(COALESCE(@dpone_before_json, N'[]'));
    IF @dpone_before_image_rows > {{ attempt['resource_policy']['max_before_image_rows'] }}
       OR @dpone_before_image_bytes > {{ attempt['resource_policy']['max_before_image_bytes'] }}
       OR @dpone_before_image_bytes > 67108864
        THROW 51022, 'DPONE_SEMANTIC_REFRESH_RESOURCE_BUDGET_EXCEEDED', 1;
    {{ dpone_semantic_refresh_statement_budget_gate(attempt['resource_policy']) }}
    {{ dpone_semantic_refresh_runtime_budget_gate(attempt['resource_policy']) }}

    SET @dpone_statement_started_at = SYSUTCDATETIME();
    SELECT {{ column_csv }}
    INTO #dpone_semantic_refresh_source
    FROM {{ temp_relation }}
    WHERE {{ event_time }} >= {{ scope_start }} AND {{ event_time }} < {{ scope_end }};
    IF @@ROWCOUNT > {{ attempt['resource_policy']['max_source_scope_rows'] }}
        THROW 51022, 'DPONE_SEMANTIC_REFRESH_RESOURCE_BUDGET_EXCEEDED', 1;
    {{ dpone_semantic_refresh_statement_budget_gate(attempt['resource_policy']) }}
    {{ dpone_semantic_refresh_runtime_budget_gate(attempt['resource_policy']) }}

    -- DPONE_SOURCE_GATES: null, duplicate, domain, and out-of-scope collisions fail closed.
    IF EXISTS (
        SELECT 1 FROM #dpone_semantic_refresh_source
        WHERE {{ dpone_semantic_refresh_invalid_key_predicate('', key_specs) }}
    )
        THROW 51005, 'DPONE_SEMANTIC_REFRESH_SOURCE_KEY_INVALID', 1;
    IF EXISTS (
        SELECT 1 FROM #dpone_semantic_refresh_source
        GROUP BY {{ key_csv }} HAVING COUNT_BIG(*) <> 1
    )
        THROW 51006, 'DPONE_SEMANTIC_REFRESH_SOURCE_KEY_DUPLICATE', 1;
    IF EXISTS (
        SELECT 1
        FROM {{ target_relation }} AS target WITH (UPDLOCK, HOLDLOCK)
        INNER JOIN #dpone_semantic_refresh_source AS source ON {{ join_sql }}
        WHERE target.{{ event_time }} < {{ scope_start }} OR target.{{ event_time }} >= {{ scope_end }}
    )
        THROW 51007, 'DPONE_SEMANTIC_REFRESH_KEY_OUTSIDE_SCOPE', 1;

    -- DPONE_TARGET_PRE_GATES: current target null, duplicate, and domain gates precede DML.
    IF EXISTS (
        SELECT 1 FROM {{ target_relation }}
        WHERE {{ event_time }} >= {{ scope_start }} AND {{ event_time }} < {{ scope_end }}
          AND ({{ dpone_semantic_refresh_invalid_key_predicate('', key_specs) }})
    )
        THROW 51011, 'DPONE_SEMANTIC_REFRESH_CURRENT_TARGET_KEY_INVALID', 1;
    IF EXISTS (
        SELECT 1 FROM {{ target_relation }}
        WHERE {{ event_time }} >= {{ scope_start }} AND {{ event_time }} < {{ scope_end }}
        GROUP BY {{ key_csv }} HAVING COUNT_BIG(*) <> 1
    )
        THROW 51012, 'DPONE_SEMANTIC_REFRESH_CURRENT_TARGET_KEY_DUPLICATE', 1;

    DECLARE @dpone_updated_count bigint = 0;
    DECLARE @dpone_inserted_count bigint = 0;
    SET @dpone_statement_started_at = SYSUTCDATETIME();
    UPDATE target
    SET {{ update_sql }}
    FROM {{ target_relation }} AS target
    INNER JOIN #dpone_semantic_refresh_source AS source ON {{ join_sql }}
    WHERE target.{{ event_time }} >= {{ scope_start }} AND target.{{ event_time }} < {{ scope_end }};
    SET @dpone_updated_count = @@ROWCOUNT;
    {{ dpone_semantic_refresh_statement_budget_gate(attempt['resource_policy']) }}
    {{ dpone_semantic_refresh_runtime_budget_gate(attempt['resource_policy']) }}

    SET @dpone_statement_started_at = SYSUTCDATETIME();
    INSERT INTO {{ target_relation }} ({{ column_csv }})
    SELECT {{ column_csv }}
    FROM #dpone_semantic_refresh_source AS source
    WHERE NOT EXISTS (
        SELECT 1 FROM {{ target_relation }} AS target WITH (UPDLOCK, HOLDLOCK)
        WHERE {{ join_sql }}
    );
    SET @dpone_inserted_count = @@ROWCOUNT;
    {{ dpone_semantic_refresh_statement_budget_gate(attempt['resource_policy']) }}
    {{ dpone_semantic_refresh_runtime_budget_gate(attempt['resource_policy']) }}

    -- DPONE_TARGET_GATES: the resulting full target scope must retain exact keys.
    IF EXISTS (
        SELECT 1 FROM {{ target_relation }}
        WHERE {{ event_time }} >= {{ scope_start }} AND {{ event_time }} < {{ scope_end }}
          AND ({{ dpone_semantic_refresh_invalid_key_predicate('', key_specs) }})
    )
        THROW 51008, 'DPONE_SEMANTIC_REFRESH_TARGET_KEY_INVALID', 1;
    IF EXISTS (
        SELECT 1 FROM {{ target_relation }}
        WHERE {{ event_time }} >= {{ scope_start }} AND {{ event_time }} < {{ scope_end }}
        GROUP BY {{ key_csv }} HAVING COUNT_BIG(*) <> 1
    )
        THROW 51009, 'DPONE_SEMANTIC_REFRESH_TARGET_KEY_DUPLICATE', 1;

    -- DPONE_AFTER_IMAGE: receipt digests bind both full-scope images.
    SET @dpone_statement_started_at = SYSUTCDATETIME();
    IF OBJECT_ID({{ dpone_semantic_refresh_literal(after_image_relation | string) }}, N'U') IS NOT NULL
        THROW 51010, 'DPONE_SEMANTIC_REFRESH_AFTER_IMAGE_CONFLICT', 1;
    SELECT {{ column_csv }}
    INTO {{ after_image_relation }}
    FROM {{ target_relation }} WITH (UPDLOCK, HOLDLOCK)
    WHERE {{ event_time }} >= {{ scope_start }} AND {{ event_time }} < {{ scope_end }};
    SET @dpone_after_image_rows = @@ROWCOUNT;
    SELECT @dpone_after_image_bytes = COALESCE(SUM({{ bounded_row_bytes }}), 2)
    FROM {{ after_image_relation }};
    IF @dpone_after_image_rows > {{ attempt['resource_policy']['max_after_image_rows'] }}
       OR @dpone_after_image_bytes > {{ attempt['resource_policy']['max_after_image_bytes'] }}
       OR @dpone_before_image_bytes + @dpone_after_image_bytes > {{ attempt['resource_policy']['max_scope_image_total_bytes'] }}
       OR @dpone_after_image_bytes > 67108864
        THROW 51022, 'DPONE_SEMANTIC_REFRESH_RESOURCE_BUDGET_EXCEEDED', 1;
    SELECT @dpone_after_json = (
        SELECT {{ column_csv }} FROM {{ after_image_relation }}
        ORDER BY {{ key_order | join(', ') }} FOR JSON PATH, INCLUDE_NULL_VALUES
    );
    SET @dpone_after_image_bytes = DATALENGTH(COALESCE(@dpone_after_json, N'[]'));
    IF @dpone_after_image_rows > {{ attempt['resource_policy']['max_after_image_rows'] }}
       OR @dpone_after_image_bytes > {{ attempt['resource_policy']['max_after_image_bytes'] }}
       OR @dpone_before_image_bytes + @dpone_after_image_bytes > {{ attempt['resource_policy']['max_scope_image_total_bytes'] }}
       OR @dpone_after_image_bytes > 67108864
        THROW 51022, 'DPONE_SEMANTIC_REFRESH_RESOURCE_BUDGET_EXCEEDED', 1;
    {{ dpone_semantic_refresh_statement_budget_gate(attempt['resource_policy']) }}
    {{ dpone_semantic_refresh_runtime_budget_gate(attempt['resource_policy']) }}
    SET @dpone_before_sha = 'sha256:' + LOWER(CONVERT(varchar(64), HASHBYTES('SHA2_256', COALESCE(@dpone_before_json, N'[]')), 2));
    SET @dpone_after_sha = 'sha256:' + LOWER(CONVERT(varchar(64), HASHBYTES('SHA2_256', COALESCE(@dpone_after_json, N'[]')), 2));
    SELECT @dpone_build_receipt_json = (
        SELECT
            {{ dpone_semantic_refresh_literal(after_image_relation | string) }} AS [after_image_relation],
            @dpone_after_sha AS [after_image_sha256],
            {{ dpone_semantic_refresh_literal(attempt['attempt_binding_sha256']) }} AS [attempt_binding_sha256],
            {{ dpone_semantic_refresh_literal(before_image_relation | string) }} AS [before_image_relation],
            @dpone_before_sha AS [before_image_sha256],
            {{ attempt['fencing_epoch'] }} AS [fencing_epoch],
            @dpone_inserted_count AS [inserted_count],
            {{ dpone_semantic_refresh_literal(attempt['model_unique_id']) }} AS [model_unique_id],
            {{ dpone_semantic_refresh_literal(attempt['operation_id']) }} AS [operation_id],
            {{ dpone_semantic_refresh_literal(attempt['operation_plan_sha256']) }} AS [operation_plan_sha256],
            N'dpone.semantic-refresh-model-build-receipt.v1' AS [schema],
            @dpone_actual_strategy_authority_sha256 AS [strategy_authority_sha256],
            @dpone_updated_count AS [updated_count]
        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    );
    SET @dpone_build_receipt_sha = 'sha256:' + LOWER(CONVERT(varchar(64), HASHBYTES('SHA2_256', @dpone_build_receipt_json), 2));

    -- DPONE_BUILD_RECEIPT: immutable authority is written after images and DML.
    INSERT INTO {{ receipt_relation }} (
        operation_id, operation_plan_sha256, attempt_binding_sha256, fencing_epoch,
        before_image_relation, before_image_sha256, after_image_relation,
        after_image_sha256, updated_count, inserted_count, build_receipt_sha256
    ) VALUES (
        {{ dpone_semantic_refresh_literal(attempt['operation_id']) }},
        {{ dpone_semantic_refresh_literal(attempt['operation_plan_sha256']) }},
        {{ dpone_semantic_refresh_literal(attempt['attempt_binding_sha256']) }},
        {{ attempt['fencing_epoch'] }},
        {{ dpone_semantic_refresh_literal(before_image_relation | string) }}, @dpone_before_sha,
        {{ dpone_semantic_refresh_literal(after_image_relation | string) }}, @dpone_after_sha,
        @dpone_updated_count, @dpone_inserted_count, @dpone_build_receipt_sha
    );
END;
  {% endset %}
  {% do return(sql) %}
{% endmacro %}


{% macro dpone_semantic_refresh_statement_budget_gate(value) %}
    IF DATEDIFF_BIG(millisecond, @dpone_statement_started_at, SYSUTCDATETIME()) > {{ value['max_statement_seconds'] * 1000 }}
        THROW 51022, 'DPONE_SEMANTIC_REFRESH_RESOURCE_BUDGET_EXCEEDED', 1;
{% endmacro %}


{% macro dpone_semantic_refresh_runtime_budget_gate(value) %}
    SET @dpone_log_free_bytes = NULL;
    SET @dpone_transaction_log_bytes = NULL;
    SET @dpone_version_store_bytes = NULL;
    BEGIN TRY
        SELECT @dpone_log_free_bytes = CONVERT(bigint, total_log_size_in_bytes) - CONVERT(bigint, used_log_space_in_bytes)
        FROM sys.dm_db_log_space_usage;
        SELECT @dpone_transaction_log_bytes = CONVERT(bigint, database_transaction_log_bytes_used)
        FROM sys.dm_tran_current_transaction AS current_transaction
        INNER JOIN sys.dm_tran_database_transactions AS database_transaction
            ON database_transaction.transaction_id = current_transaction.transaction_id
        WHERE database_transaction.database_id = DB_ID();
        SELECT @dpone_version_store_bytes = CONVERT(bigint, reserved_page_count) * 8192
        FROM sys.dm_tran_version_store_space_usage
        WHERE database_id = DB_ID();
    END TRY
    BEGIN CATCH
        THROW 51023, 'DPONE_SEMANTIC_REFRESH_RESOURCE_BUDGET_UNVERIFIED', 1;
    END CATCH;
    IF @dpone_log_free_bytes IS NULL
       OR @dpone_transaction_log_bytes IS NULL
       OR @dpone_version_store_bytes IS NULL
        THROW 51023, 'DPONE_SEMANTIC_REFRESH_RESOURCE_BUDGET_UNVERIFIED', 1;
    IF @dpone_log_free_bytes < {{ value['min_mssql_log_free_bytes'] }}
       OR @dpone_transaction_log_bytes > {{ value['max_transaction_log_bytes'] }}
       OR @dpone_version_store_bytes > {{ value['max_mssql_version_store_bytes'] }}
       OR DATEDIFF_BIG(millisecond, @dpone_scope_lock_started_at, SYSUTCDATETIME()) > {{ value['max_mssql_scope_lock_seconds'] * 1000 }}
        THROW 51022, 'DPONE_SEMANTIC_REFRESH_RESOURCE_BUDGET_EXCEEDED', 1;
{% endmacro %}


{% macro dpone_semantic_refresh_literal(value) %}
  {% if value is not string or value | length == 0 %}
    {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_ATTEMPT_INVALID: literal must be a non-empty string') }}
  {% endif %}
  {% do return("N'" ~ (value | replace("'", "''")) ~ "'") %}
{% endmacro %}


{% macro dpone_semantic_refresh_require_digest(value, field_name) %}
  {% if value is not string or modules.re.fullmatch('sha256:[0-9a-f]{64}', value) is none %}
    {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_ATTEMPT_INVALID: ' ~ field_name ~ ' must be a lowercase sha256 digest') }}
  {% endif %}
  {% do return(none) %}
{% endmacro %}


{% macro dpone_semantic_refresh_relation(value, field_name) %}
  {% if value is not mapping or value.keys() | list | sort != ['database', 'identifier', 'schema'] %}
    {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_ATTEMPT_INVALID: ' ~ field_name ~ ' relation is not closed') }}
  {% endif %}
  {% for part in ['schema', 'identifier'] %}
    {% if value[part] is not string or modules.re.fullmatch('[A-Za-z_][A-Za-z0-9_]*', value[part]) is none %}
      {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_ATTEMPT_INVALID: ' ~ field_name ~ '.' ~ part ~ ' is invalid') }}
    {% endif %}
  {% endfor %}
  {% if value['database'] is not none and (value['database'] is not string or modules.re.fullmatch('[A-Za-z_][A-Za-z0-9_]*', value['database']) is none) %}
    {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_ATTEMPT_INVALID: ' ~ field_name ~ '.database is invalid') }}
  {% endif %}
  {% do return(api.Relation.create(database=value['database'], schema=value['schema'], identifier=value['identifier'])) %}
{% endmacro %}


{% macro dpone_semantic_refresh_require_resource_policy(value) %}
  {% set fields = [
      'max_after_image_bytes', 'max_after_image_rows', 'max_before_image_bytes',
      'max_before_image_rows', 'max_clickhouse_retained_backup_bytes',
      'max_clickhouse_shadow_bytes', 'max_clickhouse_staging_bytes',
      'max_clickhouse_total_transient_bytes', 'max_dbt_temp_bytes',
      'max_dbt_temp_rows', 'max_mssql_scope_lock_seconds',
      'max_mssql_version_store_bytes', 'max_scope_image_total_bytes',
      'max_source_scope_rows', 'max_statement_seconds', 'max_target_scope_rows',
      'max_transaction_log_bytes', 'min_mssql_log_free_bytes',
      'resource_policy_sha256'
  ] | sort %}
  {% if value is not mapping or value.keys() | list | sort != fields %}
    {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_ATTEMPT_INVALID: resource_policy is not closed') }}
  {% endif %}
  {% do dpone_semantic_refresh_require_digest(value['resource_policy_sha256'], 'resource_policy_sha256') %}
  {% for field in fields if field != 'resource_policy_sha256' %}
    {% if value[field] is boolean or value[field] is not integer or value[field] <= 0 %}
      {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_ATTEMPT_INVALID: resource_policy.' ~ field ~ ' must be positive') }}
    {% endif %}
  {% endfor %}
  {% do return(none) %}
{% endmacro %}


{% macro dpone_semantic_refresh_require_artifact_authority(value) %}
  {% set fields = [
      'artifact_prefix', 'bucket_or_container_authority_id',
      'capability_evidence_sha256', 'endpoint_authority_id',
      'encryption_policy_sha256', 'kms_key_authority_id', 'max_artifact_bytes',
      'provider', 'provider_profile', 'retention_days', 'retention_policy_id',
      'retention_issued_at', 'retention_policy_sha256', 'retention_until',
      'writer_scope'
  ] | sort %}
  {% if value is not mapping or value.keys() | list | sort != fields or value['provider'] != 's3' %}
    {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_ATTEMPT_INVALID: artifact_authority is not closed') }}
  {% endif %}
  {% for field in ['capability_evidence_sha256', 'encryption_policy_sha256', 'retention_policy_sha256'] %}
    {% do dpone_semantic_refresh_require_digest(value[field], 'artifact_authority.' ~ field) %}
  {% endfor %}
  {% for field in [
      'artifact_prefix', 'bucket_or_container_authority_id', 'endpoint_authority_id',
      'kms_key_authority_id', 'provider_profile', 'retention_policy_id',
      'retention_issued_at', 'retention_until', 'writer_scope'
  ] %}
    {% if value[field] is not string or value[field] | length == 0 %}
      {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_ATTEMPT_INVALID: artifact_authority.' ~ field ~ ' must be non-empty') }}
    {% endif %}
  {% endfor %}
  {% for field in ['max_artifact_bytes', 'retention_days'] %}
    {% if value[field] is boolean or value[field] is not integer or value[field] <= 0 %}
      {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_ATTEMPT_INVALID: artifact_authority.' ~ field ~ ' must be positive') }}
    {% endif %}
  {% endfor %}
  {% if value['artifact_prefix'].startswith('/') or '..' in value['artifact_prefix'].split('/') %}
    {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_ATTEMPT_INVALID: artifact_authority prefix is unconfined') }}
  {% endif %}
  {% do return(none) %}
{% endmacro %}


{% macro dpone_semantic_refresh_require_writable_columns(value, destination_columns) %}
  {% if value is not sequence or value is string or value | length != destination_columns | length %}
    {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_ATTEMPT_INVALID: writable column closure differs from dbt contract') }}
  {% endif %}
  {% for column in value %}
    {% if column is not mapping or column.keys() | list | sort != ['name', 'nullable', 'source_type', 'target_type', 'writable_role'] %}
      {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_ATTEMPT_INVALID: writable column is not closed') }}
    {% endif %}
    {% if column['name'] != destination_columns[loop.index0] %}
      {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_ATTEMPT_INVALID: writable column order differs from dbt contract') }}
    {% endif %}
    {% if column['nullable'] is not boolean
          or column['source_type'] is not string or column['source_type'] | length == 0
          or column['target_type'] is not string or column['target_type'] | length == 0
          or column['writable_role'] not in ['EFFECTIVE_KEY', 'EFFECTIVE_KEY_EVENT_TIME', 'MUTABLE_VALUE'] %}
      {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_ATTEMPT_INVALID: writable column policy is invalid') }}
    {% endif %}
  {% endfor %}
  {% do return(none) %}
{% endmacro %}


{% macro dpone_semantic_refresh_invalid_key_predicate(alias, key_specs) %}
  {% set predicates = [] %}
  {% for spec in key_specs %}
    {% set column = (alias ~ '.' if alias else '') ~ adapter.quote(spec['name']) %}
    {% do predicates.append(column ~ ' IS NULL') %}
    {% if spec['data_type'] == 'date' %}
      {% do predicates.append(column ~ " < CONVERT(date, '19700101', 112)") %}
      {% do predicates.append(column ~ " > CONVERT(date, '21490606', 112)") %}
    {% elif spec['data_type'] == 'datetime2(6)' %}
      {% do predicates.append(column ~ " < CONVERT(datetime2(6), '1900-01-01T00:00:00.000000', 126)") %}
      {% do predicates.append(column ~ " > CONVERT(datetime2(6), '2299-12-31T23:59:59.999999', 126)") %}
    {% endif %}
  {% endfor %}
  {% do return(predicates | join(' OR ')) %}
{% endmacro %}
