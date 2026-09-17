{# Fixed model-database endpoints. No connection, transaction ownership, or retry. #}
{% macro dpone_mssql_attach(model_unique_id) %}
  {% if not execute %}{% do return(none) %}{% endif %}
  {% set request = dpone_managed_envelope() %}
  {% do dpone_managed_text(model_unique_id, 'model_unique_id') %}
  {% set sql %}
    EXEC [dpone_physical].[physical_attach_session_v1]
      @registration_id={{ dpone_managed_literal(request.runtime_registration_id) }},
      @generation={{ dpone_managed_literal(request.generation_id) }},
      @expected_invocation={{ dpone_managed_literal(request.invocation_id) }},
      @plan_set_locator={{ dpone_managed_literal(request.plan_set.locator) }},
      @plan_set_sha256={{ dpone_managed_literal(request.plan_set.sha256) }},
      @model_unique_id={{ dpone_managed_literal(model_unique_id) }};
  {% endset %}
  {% set row = dpone_managed_row(run_query(sql), 'attach') %}
  {% do dpone_managed_compare(row, {'registration_id': request.runtime_registration_id, 'generation_id': request.generation_id, 'executor_invocation_id': request.invocation_id, 'plan_set_sha256': request.plan_set.sha256, 'model_unique_id': model_unique_id}, ['registration_id', 'generation_id', 'executor_invocation_id', 'plan_set_sha256', 'model_unique_id']) %}
  {% do return(row) %}
{% endmacro %}

{% macro dpone_mssql_bind_transaction(attach) %}
  {% if not execute %}{% do return(none) %}{% endif %}
  {% do dpone_managed_validate_row(attach, 'attach') %}
  {% set sql %}
    EXEC [dpone_physical].[physical_bind_transaction_v1]
      @session_registration_id={{ dpone_managed_literal(attach.session_registration_id) }};
  {% endset %}
  {% set row = dpone_managed_row(run_query(sql), 'transaction') %}
  {% do dpone_managed_compare(row, attach, ['registration_id', 'registration_digest', 'generation_id', 'executor_invocation_id', 'model_unique_id', 'model_plan_sha256', 'session_registration_id', 'session_id']) %}
  {% do return(row) %}
{% endmacro %}

{% macro dpone_mssql_require_transaction(attach, transaction) %}
  {% if not execute %}{% do return(none) %}{% endif %}
  {% do dpone_managed_validate_row(attach, 'attach') %}
  {% do dpone_managed_validate_row(transaction, 'transaction') %}
  {% do dpone_managed_compare(transaction, attach, ['registration_id', 'registration_digest', 'generation_id', 'executor_invocation_id', 'model_unique_id', 'model_plan_sha256', 'session_registration_id', 'session_id']) %}
  {% set sql %}
    EXEC [dpone_physical].[physical_require_transaction_v1]
      @session_registration_id={{ dpone_managed_literal(attach.session_registration_id) }},
      @expected_transaction_id={{ transaction.transaction_id | int }};
  {% endset %}
  {% set row = dpone_managed_row(run_query(sql), 'transaction') %}
  {% do dpone_managed_compare(row, transaction, dpone_managed_columns('transaction')) %}
  {% do return(row) %}
{% endmacro %}
