"""Reviewed runtime, authority, and transactional route case authority."""

from __future__ import annotations

from itertools import product

from .reviewed_case import ReviewedCase, ReviewedSuite, case


def artifact_integrity_suite() -> ReviewedSuite:
    """Owned payload integrity, identity, scope, and replay boundaries."""

    suite_id = "artifact_integrity_faults"
    definitions = (
        ("valid_exact_payload", "consume_verified_payload", "exact_payload_consumed", True),
        ("missing_payload", "artifact_preflight", "typed_missing_reject", False),
        ("digest_mismatch", "artifact_preflight", "typed_digest_reject", False),
        ("size_mismatch", "artifact_preflight", "typed_size_reject", False),
        ("inode_replacement", "artifact_preflight", "typed_identity_reject", False),
        ("truncate_after_verification", "artifact_consumption", "transaction_rollback", False),
        ("append_after_verification", "artifact_consumption", "transaction_rollback", False),
        ("scope_escape", "artifact_preflight", "typed_scope_reject", False),
        ("symlink_alias", "artifact_preflight", "typed_alias_reject", False),
        ("retry_identical_bytes", "artifact_replay", "idempotent_exact_replay", True),
        ("retry_changed_bytes", "artifact_replay", "typed_replay_conflict", False),
        ("post_consume_cleanup", "artifact_terminal", "owned_scope_removed", True),
    )
    cases = [
        case(
            suite_id,
            case_id,
            {
                "fault": case_id,
                "hash": "sha256",
                "identity": ["device", "inode", "size", "mtime_ns"],
                "owned_scope_required": True,
            },
            action=action,
            outcome=outcome,
            mutation=mutation,
        )
        for case_id, action, outcome, mutation in definitions
    ]
    return ReviewedSuite.create(suite_id, "dpone.postgres_mssql.artifact_integrity_faults.v1", cases)


def backfill_orchestration_suite() -> ReviewedSuite:
    """Finite inner-mode, worker, lease, retry, and cancellation product."""

    suite_id = "backfill_orchestration"
    cases: list[ReviewedCase] = []
    for inner_mode, workers, lifecycle in product(
        ("partition_replace", "replace", "incremental_merge"),
        (1, 2),
        ("success", "worker_failure", "lease_expiry", "heartbeat_failure", "retry_resume"),
    ):
        mutates = lifecycle in {"success", "retry_resume"}
        parameters = {
            "inner_mode": inner_mode,
            "parallel_workers": workers,
            "lifecycle": lifecycle,
            "lease_ttl_minutes": 1,
        }
        cases.append(
            case(
                suite_id,
                f"{inner_mode}__workers_{workers}__{lifecycle}",
                parameters,
                action="standard_etl_backfill_orchestration",
                outcome=(
                    "exact_partition_receipts_and_checkpoint"
                    if mutates
                    else "rollback_or_typed_abort_without_checkpoint"
                ),
                mutation=mutates,
            )
        )
    return ReviewedSuite.create(suite_id, "dpone.postgres_mssql.backfill_orchestration.v1", cases)


def source_identity_suite() -> ReviewedSuite:
    """PostgreSQL physical-system, principal, topology, and scope identity."""

    suite_id = "source_identity_authority"
    definitions = (
        ("stable_same_cluster", True, "identity_stable"),
        ("database_oid_change", False, "typed_source_identity_mismatch"),
        ("system_identifier_change", False, "typed_source_identity_mismatch"),
        ("timeline_change", False, "typed_source_identity_mismatch"),
        ("principal_change", False, "typed_source_principal_mismatch"),
        ("schema_oid_recreate", False, "typed_source_scope_mismatch"),
        ("table_oid_recreate", False, "typed_source_scope_mismatch"),
        ("case_alias_same_oid", True, "canonical_identity_stable"),
        ("metadata_permission_denied", False, "typed_pre_source_authority_failure"),
    )
    cases = [
        case(
            suite_id,
            case_id,
            {
                "case": case_id,
                "identity_fields": [
                    "system_identifier",
                    "timeline_id",
                    "database_oid",
                    "schema_oid",
                    "relation_oid",
                    "principal_oid",
                ],
            },
            action="source_identity_preflight",
            outcome=outcome,
            mutation=mutation,
        )
        for case_id, mutation, outcome in definitions
    ]
    return ReviewedSuite.create(suite_id, "dpone.postgres_mssql.source_identity_authority.v1", cases)


def target_database_suite() -> ReviewedSuite:
    """Cross-database target/state coordinates and database authority."""

    suite_id = "target_database_authority"
    definitions = (
        ("target_and_state_distinct_databases", True, "cross_database_atomic_authority"),
        ("target_database_missing", False, "typed_pre_source_database_missing"),
        ("state_database_missing", False, "typed_pre_source_state_database_missing"),
        ("target_database_id_changed", False, "typed_target_database_identity_mismatch"),
        ("state_database_id_changed", False, "typed_state_database_identity_mismatch"),
        ("target_offline", False, "typed_pre_source_target_unavailable"),
        ("state_offline", False, "typed_pre_source_state_unavailable"),
        ("cross_database_permission_denied", False, "typed_pre_source_permission_failure"),
        ("three_part_name_case_alias", True, "canonical_database_identity_stable"),
    )
    cases = [
        case(
            suite_id,
            case_id,
            {"case": case_id, "target_database_pinned": True, "state_database_pinned": True},
            action="target_database_preflight",
            outcome=outcome,
            mutation=mutation,
        )
        for case_id, mutation, outcome in definitions
    ]
    return ReviewedSuite.create(suite_id, "dpone.postgres_mssql.target_database_authority.v1", cases)


def target_identity_suite() -> ReviewedSuite:
    """Binary target registry, aliases, conflicts, and repair transfer."""

    suite_id = "target_identity_authority"
    case_ids = (
        "target_identity_ci_alias_convergence",
        "target_identity_cs_case_distinct_mixed_collation",
        "target_identity_missing_decoy_pre_source",
        "target_identity_concurrent_distinct_owners",
        "target_config_coordinate_drift",
        "repair_empty_snapshot_all_delete",
        "repair_wraparound_and_freeze_full_baseline",
        "repair_target_authority_transfer",
    )
    cases = [
        case(
            suite_id,
            case_id,
            {
                "case": case_id,
                "registry_collation": "Latin1_General_100_BIN2",
                "immutable_binding_id": True,
                "one_active_owner": True,
            },
            action="standard_etl_target_identity_authority",
            outcome="exact_registry_state_receipt_and_repair_authority",
            mutation=case_id
            not in {
                "target_identity_ci_alias_convergence",
                "target_identity_missing_decoy_pre_source",
            },
        )
        for case_id in case_ids
    ]
    return ReviewedSuite.create(suite_id, "dpone.postgres_mssql.target_identity_authority.v1", cases)


def transaction_governance_suite() -> ReviewedSuite:
    """Generic fence/attempt/operation/receipt transaction lifecycle."""

    suite_id = "transaction_governance"
    success_phases = ("admit", "stage", "mutate", "catalog_verify", "checkpoint", "receipt")
    cases: list[ReviewedCase] = [
        case(
            suite_id,
            "successful_atomic_commit",
            {
                "objects": [
                    "dpone_target_fence",
                    "dpone_load_attempt",
                    "dpone_load_operation",
                    "dpone_load_receipt",
                ],
                "phases": list(success_phases),
            },
            action="standard_etl_transaction_governance",
            outcome="one_committed_receipt_and_checkpoint",
            mutation=True,
        )
    ]
    for fault_after in success_phases:
        cases.append(
            case(
                suite_id,
                f"fault_after_{fault_after}",
                {"fault_after": fault_after, "transaction_scope": "business_state_catalog_receipt"},
                action="transaction_fault_injection",
                outcome="complete_rollback_and_retryable_attempt",
                mutation=False,
            )
        )
    for case_id, outcome in (
        ("parallel_owner_applock", "one_owner_at_a_time"),
        ("stale_operation_epoch", "typed_epoch_reject"),
        ("expired_owner_lease", "typed_lease_reject"),
        ("duplicate_receipt", "idempotent_exact_receipt"),
        ("conflicting_receipt", "typed_attempt_identity_collision_before_receipt_probe"),
        ("fresh_session_ack_probe", "committed_receipt_observed"),
        ("commit_outcome_unknown", "fresh_probe_resolves_or_fails_closed"),
    ):
        cases.append(
            case(
                suite_id,
                case_id,
                {"case": case_id, "fresh_session_probe": True},
                action="transaction_authority_probe",
                outcome=outcome,
                mutation=case_id == "fresh_session_ack_probe",
            )
        )
    return ReviewedSuite.create(suite_id, "dpone.postgres_mssql.transaction_governance.v1", cases)


def target_behavior_suite() -> ReviewedSuite:
    """Strategy-aware blockers for temporal, graph, trigger, and FK behavior."""

    suite_id = "target_behavior"
    strategy_groups = ("append_key_preserving", "merge_key_preserving", "destructive_replace")
    behaviors = (
        "ordinary_table",
        "dml_trigger",
        "temporal",
        "ledger",
        "memory_optimized",
        "filetable",
        "graph_node",
        "graph_edge",
        "inbound_fk_no_action",
        "inbound_fk_cascade",
        "inbound_fk_set_null",
        "inbound_fk_set_default",
    )
    cases: list[ReviewedCase] = []
    for strategy_group, behavior in product(strategy_groups, behaviors):
        if behavior == "filetable":
            cases.append(
                case(
                    suite_id,
                    f"{strategy_group}__{behavior}",
                    {"strategy_group": strategy_group, "target_behavior": behavior},
                    action="pinned_vendor_capability_preflight",
                    outcome="pinned_sqlserver_linux_filetable_unavailable",
                    mutation=False,
                )
            )
            continue
        safe = behavior == "ordinary_table" or (
            behavior == "inbound_fk_no_action" and strategy_group != "destructive_replace"
        )
        cases.append(
            case(
                suite_id,
                f"{strategy_group}__{behavior}",
                {"strategy_group": strategy_group, "target_behavior": behavior},
                action="target_behavior_preflight" if not safe else "standard_etl_target_behavior",
                outcome="strategy_safe_behavior" if safe else "typed_reject_before_source_copy",
                mutation=safe,
            )
        )
    return ReviewedSuite.create(suite_id, "dpone.postgres_mssql.target_behavior.v1", cases)


def xmin_reconciliation_suite() -> ReviewedSuite:
    """Same-snapshot keys, soft-delete lifecycle, concurrency, and failures."""

    suite_id = "xmin_reconciliation"
    definitions: tuple[tuple[str, bool, str], ...] = (
        ("baseline", True, "insert_and_checkpoint"),
        ("identical_rerun", False, "unchanged_and_checkpoint_advance"),
        ("update_insert_delete", True, "update_insert_soft_delete_atomic"),
        ("repeated_delete", False, "tombstone_timestamp_stable"),
        ("reactivate", True, "reactivated_without_duplicate"),
        ("identical_after_reactivate", False, "unchanged_after_reactivation"),
        ("parallel_applock_and_stale_cas", True, "one_commit_one_complete_rollback"),
        ("concurrent_source_mutation_same_snapshot", True, "delta_and_keys_same_mvcc_snapshot"),
        ("state_permission_denial", False, "business_and_state_rollback"),
        ("per_dml_and_checkpoint_fault_injection", False, "all_fault_points_rollback"),
        ("empty_guard_and_checksum_negative_cases", False, "typed_admission_reject"),
        ("commit_outcome_unknown_fresh_probe", True, "receipt_resolves_commit"),
    )
    cases = [
        case(
            suite_id,
            case_id,
            {
                "case": case_id,
                "same_exported_snapshot": True,
                "soft_delete_marker": "__dpone__deleted_at",
                "atomic_checkpoint": True,
            },
            action="standard_etl_xmin_snapshot_reconciliation",
            outcome=outcome,
            mutation=mutation,
        )
        for case_id, mutation, outcome in definitions
    ]
    return ReviewedSuite.create(suite_id, "dpone.postgres_mssql.xmin_reconciliation.v1", cases)


__all__ = [
    "artifact_integrity_suite",
    "backfill_orchestration_suite",
    "source_identity_suite",
    "target_behavior_suite",
    "target_database_suite",
    "target_identity_suite",
    "transaction_governance_suite",
    "xmin_reconciliation_suite",
]
