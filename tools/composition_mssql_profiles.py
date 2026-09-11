"""Closed SQL component case inventories and their exact fixture configuration.

These four built-in profiles are test selection data, never backend authority or
an extensible plugin registry. The lifecycle runner owns isolation and evidence.
"""

from dataclasses import dataclass

TEST_FILE = "tests/integration/composition/test_composition_mssql_store_live.py"
TEST_CLASS = "tests.integration.composition.test_composition_mssql_store_live"
EXPECTED_TESTS = (
    "test_external_ddl_and_mixed_engine_lifecycle",
    "test_conflict_and_epoch_ceiling_leave_no_partial_mutation",
    "test_concurrent_same_domain_prepare_has_one_winner",
    "test_lost_commit_ack_uses_independent_sql_readback",
    "test_foreign_control_identity_and_corrupt_request_fail_closed",
    "test_unresolved_attempt_blocks_retirement[RUNNING]",
    "test_unresolved_attempt_blocks_retirement[COMMIT_UNKNOWN]",
)
GATE_CASES = {
    "tests.integration.composition.test_composition_mssql_transfer_fence_live": (
        "test_transfer_exact_binding_commits_actual_target_rows",
        "test_transfer_foreign_operation_and_receipt_cannot_mutate",
        "test_transfer_server_rejects_foreign_binding_digest_and_bytes",
        "test_transfer_server_rejects_stale_domain_epoch",
        "test_transfer_server_rejects_retiring_parent",
        "test_transfer_closed_gate_denies_existing_worker_transaction",
        "test_transfer_worker_has_only_control_procedure_permission",
        "test_transfer_public_permission_drift_prevents_ready_gate",
    ),
    "tests.integration.composition.test_composition_mssql_gate_live": (
        "test_installed_gate_policy_and_reader_permissions",
        "test_issued_principal_has_only_managed_writer_scope",
        "test_gate_transitions_are_monotonic_and_reconnect_is_denied",
        "test_stale_and_replayed_attempts_never_issue_credentials",
        "test_recreated_target_database_blocks_issuance",
        "test_owner_and_role_permission_drift_block_issuance",
        "test_concurrent_issuance_returns_credentials_once",
        "test_missing_or_recreated_sid_cannot_prove_closed",
    ),
    "tests.integration.composition.test_composition_mssql_gate_recovery_live": (
        "test_lost_admission_ack_never_authorizes_replay",
        "test_lost_journal_ack_never_creates_or_reissues_login",
        "test_lost_ready_ack_closes_the_real_issued_login",
        "test_connection_attempts_racing_close_cannot_reconnect",
        "test_inflight_command_and_open_transaction_block_quiescence",
        "test_foreign_transaction_blocks_but_observer_does_not",
        "test_missing_terminal_proof_blocks_overlapping_reuse",
        "test_real_commit_unknown_requires_explicit_reconciliation",
    ),
}
TRUST_TEST_CLASS = "tests.integration.composition.test_nonproduction_mssql_trust_live"
TRUST_TESTS = (
    "test_external_trust_ddl_and_original_byte_readback",
    "test_append_only_revision_rejects_update_delete_and_replay",
    "test_trust_revision_change_blocks_same_ledger_compare",
    "test_concurrent_appends_admit_one_next_revision",
    "test_invalid_original_hash_or_epoch_cannot_append",
    "test_read_rejects_changed_trigger_or_schema",
    "test_insufficient_transaction_lock_never_returns_trust",
    "test_lost_read_ack_returns_no_trusted_revision",
    "test_provisioner_can_append_without_schema_bypass",
)


REGISTRATION_CASES = {
    "tests.integration.composition.test_nonproduction_mssql_registration_live": (
        "test_external_registration_ddl_and_catalog",
        "test_complete_binary_originals_survive_independent_readback",
        "test_append_only_and_invalid_direct_dml",
        "test_execution_replay_rebind_and_documentary_candidates",
        "test_qualification_consumes_once_without_run_rebind",
        "test_complete_campaign_membership_obeys_current_ceilings",
        "test_paged_history_audits_later_originals",
        "test_historical_reads_survive_current_trust_changes",
        "test_actual_ledger_identity_revision_and_clock_preconditions",
    ),
    "tests.integration.composition.test_nonproduction_mssql_registration_recovery_live": (
        "test_concurrent_qualification_has_one_durable_consumption",
        "test_concurrent_execution_preserves_membership_ceiling",
        "test_partial_write_failure_rolls_back_complete_registration",
        "test_lost_commit_ack_reconciles_exact_durable_originals",
        "test_failed_commit_and_unavailable_readback_return_no_ack",
        "test_membership_corruption_and_overflow_fail_closed",
        "test_installed_catalog_drift_blocks_registration",
        "test_restricted_login_cannot_bypass_registration_storage",
        "test_session_options_reject_new_writes_without_mutation",
    ),
}


@dataclass(frozen=True, slots=True)
class ComponentProfile:
    """One complete case inventory, scoped fixture plugin and explicit opt-in."""

    cases: tuple[str, ...]
    plugin: str
    enable_flag: str | None
    sql_scope: str


def _cases(groups):
    return tuple(f"{module}::{name}" for module, names in groups.items() for name in names)


_PROFILES = {
    "store": ComponentProfile(
        _cases(
            {
                TEST_CLASS: EXPECTED_TESTS,
                "tests.integration.composition.test_composition_dispatch_schema_live": (
                    "test_dispatch_catalog_and_append_only_closure",
                    "test_dispatch_catalog_drift_is_rejected",
                    "test_dispatch_claim_terminal_and_closure_order",
                ),
            }
        ),
        "tests.integration.composition.mssql_store_live_support",
        None,
        "real_dbapi_control_ledger",
    ),
    "gate": ComponentProfile(
        _cases(GATE_CASES),
        "tests.integration.composition.mssql_gate_live_support",
        "DPONE_RUN_COMPOSITION_MSSQL_GATE_LIVE",
        "real_issued_principal_gate_and_recovery",
    ),
    "trust": ComponentProfile(
        _cases({TRUST_TEST_CLASS: TRUST_TESTS}),
        "tests.integration.composition.nonproduction_mssql_trust_live_support",
        "DPONE_RUN_COMPOSITION_MSSQL_TRUST_LIVE",
        "real_append_only_nonproduction_trust",
    ),
    "registration": ComponentProfile(
        _cases(REGISTRATION_CASES),
        "tests.integration.composition.nonproduction_mssql_registration_live_support",
        "DPONE_RUN_COMPOSITION_MSSQL_REGISTRATION_LIVE",
        "real_append_only_nonproduction_registration",
    ),
}
PROFILE_NAMES = tuple(_PROFILES)
PROFILE_FLAGS = tuple(value.enable_flag for value in _PROFILES.values() if value.enable_flag)


def component_profile(name: str) -> ComponentProfile:
    """Select an exact built-in profile; an unknown name always rejects."""
    return _PROFILES[name]
