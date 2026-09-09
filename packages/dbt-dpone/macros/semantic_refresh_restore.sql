{% macro semantic_refresh_restore_predecessor_scope(target_relation, destination_columns, unique_keys, attempt) %}
  {% set quoted_columns = [] %}
  {% set bounded_row_byte_terms = [] %}
  {% for column in destination_columns %}
    {% do quoted_columns.append(adapter.quote(column)) %}
    {% do bounded_row_byte_terms.append(
        '(CONVERT(bigint, 512) + (CONVERT(bigint, COALESCE(DATALENGTH(' ~ adapter.quote(column) ~ '), 0)) * 6))'
    ) %}
  {% endfor %}
  {% set event_time = adapter.quote(attempt['event_time_column']) %}
  {% set scope_start = dpone_semantic_refresh_literal(attempt['scope_start_utc']) %}
  {% set scope_end = dpone_semantic_refresh_literal(attempt['scope_end_utc']) %}
  {% set columns = quoted_columns | join(', ') %}
  {% set bounded_row_bytes = bounded_row_byte_terms | join(' + ') %}
  {% set predecessor_before = dpone_semantic_refresh_relation(attempt['predecessor_before_image_relation'], 'predecessor_before_image_relation') %}
  {% set predecessor_after = dpone_semantic_refresh_relation(attempt['predecessor_after_image_relation'], 'predecessor_after_image_relation') %}
  {% set predecessor_receipt = dpone_semantic_refresh_relation(attempt['predecessor_receipt_relation'], 'predecessor_receipt_relation') %}
  {% do dpone_semantic_refresh_require_digest(attempt['predecessor_operation_id'], 'predecessor_operation_id') %}
  {% do dpone_semantic_refresh_require_digest(attempt['predecessor_operation_plan_sha256'], 'predecessor_operation_plan_sha256') %}
  {% do dpone_semantic_refresh_require_digest(attempt['predecessor_attempt_binding_sha256'], 'predecessor_attempt_binding_sha256') %}
  {% do dpone_semantic_refresh_require_digest(attempt['predecessor_before_image_sha256'], 'predecessor_before_image_sha256') %}
  {% do dpone_semantic_refresh_require_digest(attempt['predecessor_after_image_sha256'], 'predecessor_after_image_sha256') %}
  {% if attempt['predecessor_fencing_epoch'] is boolean or attempt['predecessor_fencing_epoch'] is not integer or attempt['predecessor_fencing_epoch'] <= 0 %}
    {{ exceptions.raise_compiler_error('DPONE_SEMANTIC_REFRESH_RESTORE_INVALID: predecessor fence must be positive') }}
  {% endif %}
  {% set key_order = [] %}
  {% for key in unique_keys %}
    {% set spec = attempt['effective_keys'][loop.index0] %}
    {% if spec['data_type'] == 'uniqueidentifier' %}
      {% do key_order.append('CONVERT(char(36), ' ~ adapter.quote(key) ~ ')') %}
    {% else %}
      {% do key_order.append(adapter.quote(key)) %}
    {% endif %}
  {% endfor %}
  {% set sql %}
    DECLARE @dpone_predecessor_receipt_count bigint;
    SELECT @dpone_predecessor_receipt_count = COUNT_BIG(*)
    FROM {{ predecessor_receipt }} WITH (UPDLOCK, HOLDLOCK)
    WHERE operation_id COLLATE Latin1_General_100_BIN2 = {{ dpone_semantic_refresh_literal(attempt['predecessor_operation_id']) }} COLLATE Latin1_General_100_BIN2
      AND operation_plan_sha256 COLLATE Latin1_General_100_BIN2 = {{ dpone_semantic_refresh_literal(attempt['predecessor_operation_plan_sha256']) }} COLLATE Latin1_General_100_BIN2
      AND attempt_binding_sha256 COLLATE Latin1_General_100_BIN2 = {{ dpone_semantic_refresh_literal(attempt['predecessor_attempt_binding_sha256']) }} COLLATE Latin1_General_100_BIN2
      AND fencing_epoch = {{ attempt['predecessor_fencing_epoch'] }}
      AND before_image_relation = {{ dpone_semantic_refresh_literal(predecessor_before | string) }}
      AND before_image_sha256 COLLATE Latin1_General_100_BIN2 = {{ dpone_semantic_refresh_literal(attempt['predecessor_before_image_sha256']) }} COLLATE Latin1_General_100_BIN2
      AND after_image_relation = {{ dpone_semantic_refresh_literal(predecessor_after | string) }}
      AND after_image_sha256 COLLATE Latin1_General_100_BIN2 = {{ dpone_semantic_refresh_literal(attempt['predecessor_after_image_sha256']) }} COLLATE Latin1_General_100_BIN2;
    IF @dpone_predecessor_receipt_count <> 1
        THROW 51019, 'DPONE_SEMANTIC_REFRESH_PREDECESSOR_RECEIPT_CONFLICT', 1;

    DECLARE @dpone_current_scope_json nvarchar(max);
    DECLARE @dpone_restore_current_rows bigint;
    DECLARE @dpone_restore_current_preflight_bytes bigint;
    SELECT @dpone_restore_current_rows = COUNT_BIG(*),
           @dpone_restore_current_preflight_bytes = COALESCE(SUM({{ bounded_row_bytes }}), 2)
    FROM {{ target_relation }}
    WHERE {{ event_time }} >= {{ scope_start }} AND {{ event_time }} < {{ scope_end }};
    IF @dpone_restore_current_rows > {{ attempt['resource_policy']['max_after_image_rows'] }}
       OR @dpone_restore_current_preflight_bytes > {{ attempt['resource_policy']['max_after_image_bytes'] }}
       OR @dpone_restore_current_preflight_bytes > 67108864
        THROW 51022, 'DPONE_SEMANTIC_REFRESH_RESOURCE_BUDGET_EXCEEDED', 1;
    SELECT @dpone_current_scope_json = (
        SELECT {{ columns }} FROM {{ target_relation }}
        WHERE {{ event_time }} >= {{ scope_start }} AND {{ event_time }} < {{ scope_end }}
        ORDER BY {{ key_order | join(', ') }} FOR JSON PATH, INCLUDE_NULL_VALUES
    );
    IF {{ dpone_semantic_refresh_literal(attempt['predecessor_after_image_sha256']) }} <>
       'sha256:' + LOWER(CONVERT(varchar(64), HASHBYTES('SHA2_256', COALESCE(@dpone_current_scope_json, N'[]')), 2))
        THROW 51020, 'DPONE_SEMANTIC_REFRESH_PREDECESSOR_AFTER_IMAGE_DRIFT', 1;

    -- Restore is authorized only after exact predecessor after-image multiset equality.
    IF EXISTS (
        SELECT {{ columns }}, COUNT_BIG(*) AS dpone_multiplicity FROM {{ target_relation }}
        WHERE {{ event_time }} >= {{ scope_start }} AND {{ event_time }} < {{ scope_end }}
        GROUP BY {{ columns }}
        EXCEPT
        SELECT {{ columns }}, COUNT_BIG(*) AS dpone_multiplicity FROM {{ predecessor_after }}
        GROUP BY {{ columns }}
    ) OR EXISTS (
        SELECT {{ columns }}, COUNT_BIG(*) AS dpone_multiplicity FROM {{ predecessor_after }}
        GROUP BY {{ columns }}
        EXCEPT
        SELECT {{ columns }}, COUNT_BIG(*) AS dpone_multiplicity FROM {{ target_relation }}
        WHERE {{ event_time }} >= {{ scope_start }} AND {{ event_time }} < {{ scope_end }}
        GROUP BY {{ columns }}
    )
        THROW 51020, 'DPONE_SEMANTIC_REFRESH_PREDECESSOR_AFTER_IMAGE_DRIFT', 1;

    DELETE FROM {{ target_relation }}
    WHERE {{ event_time }} >= {{ scope_start }} AND {{ event_time }} < {{ scope_end }};
    INSERT INTO {{ target_relation }} ({{ columns }})
    SELECT {{ columns }} FROM {{ predecessor_before }};

    DECLARE @dpone_restored_scope_json nvarchar(max);
    DECLARE @dpone_restored_scope_rows bigint;
    DECLARE @dpone_restored_scope_preflight_bytes bigint;
    SELECT @dpone_restored_scope_rows = COUNT_BIG(*),
           @dpone_restored_scope_preflight_bytes = COALESCE(SUM({{ bounded_row_bytes }}), 2)
    FROM {{ target_relation }}
    WHERE {{ event_time }} >= {{ scope_start }} AND {{ event_time }} < {{ scope_end }};
    IF @dpone_restored_scope_rows > {{ attempt['resource_policy']['max_before_image_rows'] }}
       OR @dpone_restored_scope_preflight_bytes > {{ attempt['resource_policy']['max_before_image_bytes'] }}
       OR @dpone_restored_scope_preflight_bytes > 67108864
        THROW 51022, 'DPONE_SEMANTIC_REFRESH_RESOURCE_BUDGET_EXCEEDED', 1;
    SELECT @dpone_restored_scope_json = (
        SELECT {{ columns }} FROM {{ target_relation }}
        WHERE {{ event_time }} >= {{ scope_start }} AND {{ event_time }} < {{ scope_end }}
        ORDER BY {{ key_order | join(', ') }} FOR JSON PATH, INCLUDE_NULL_VALUES
    );
    IF {{ dpone_semantic_refresh_literal(attempt['predecessor_before_image_sha256']) }} <>
       'sha256:' + LOWER(CONVERT(varchar(64), HASHBYTES('SHA2_256', COALESCE(@dpone_restored_scope_json, N'[]')), 2))
        THROW 51021, 'DPONE_SEMANTIC_REFRESH_PREDECESSOR_BEFORE_IMAGE_RESTORE_FAILED', 1;

    IF EXISTS (
        SELECT {{ columns }}, COUNT_BIG(*) AS dpone_multiplicity FROM {{ target_relation }}
        WHERE {{ event_time }} >= {{ scope_start }} AND {{ event_time }} < {{ scope_end }}
        GROUP BY {{ columns }}
        EXCEPT
        SELECT {{ columns }}, COUNT_BIG(*) AS dpone_multiplicity FROM {{ predecessor_before }}
        GROUP BY {{ columns }}
    ) OR EXISTS (
        SELECT {{ columns }}, COUNT_BIG(*) AS dpone_multiplicity FROM {{ predecessor_before }}
        GROUP BY {{ columns }}
        EXCEPT
        SELECT {{ columns }}, COUNT_BIG(*) AS dpone_multiplicity FROM {{ target_relation }}
        WHERE {{ event_time }} >= {{ scope_start }} AND {{ event_time }} < {{ scope_end }}
        GROUP BY {{ columns }}
    )
        THROW 51021, 'DPONE_SEMANTIC_REFRESH_PREDECESSOR_BEFORE_IMAGE_RESTORE_FAILED', 1;
  {% endset %}
  {% do return(sql) %}
{% endmacro %}
