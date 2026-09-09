"""Regression guardrails for the reviewed REST bulk-delivery design."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from dpone.manifest.bounded_yaml import load_bounded_yaml

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "docs" / "feature-design-rest-api-sink-v1.md"
DESIGN_CONTRACT = ROOT / "docs" / "rest-bulk-delivery-design-contract-v1.yaml"
THREAT_MODEL = ROOT / "docs" / "rest-bulk-delivery-threat-model.md"
ADR_PATHS = (
    ROOT / "docs" / "adr" / "0053-rest-delivery-identity-effect-journal.md",
    ROOT / "docs" / "adr" / "0054-resumable-delivery-runtime-airflow.md",
    ROOT / "docs" / "adr" / "0055-rest-operation-profile-authority.md",
)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _contract() -> dict[str, Any]:
    loaded = load_bounded_yaml(DESIGN_CONTRACT.read_bytes())
    assert isinstance(loaded, dict)
    return loaded


def test_rest_delivery_design_keeps_separate_identity_and_outcome_axes() -> None:
    spec = _read(SPEC)

    assert "- Status: RESEARCHED" in spec
    for contract in (
        "delivery_key = H(",
        "mutation_intent_digest = H(",
        "execution_policy_digest = H(",
        "operation_semantics_digest = H(",
        "rendered_request_semantics_digest = H(",
        "profile_contract_digest = H(",
        "REST_DELIVERY_INTENT_CONFLICT",
        "explicit_generation",
        "remote_effect:",
        "acknowledgement_state:",
        "verification_state:",
        "checkpoint_state:",
    ):
        assert contract in spec


def test_rest_delivery_design_keeps_durable_continuation_and_recovery() -> None:
    spec = _read(SPEC)

    for contract in (
        "DeliveryCompleted",
        "DeliveryPending",
        "DeliveryBlocked",
        "ResumableDeliveryProcessor",
        "PostgreSQL 15+",
        "mutation_lease",
        "observation_lease",
        "checkpoint_lease",
        "compact operation tombstone",
        "--operation-id op_v1_...",
        "Recovery never accepts an authoring manifest",
    ):
        assert contract in spec


def test_rest_delivery_design_keeps_source_and_airflow_safety_boundaries() -> None:
    spec = _read(SPEC)

    for contract in (
        "One parent source boundary is frozen",
        "highest contiguous completed frontier",
        "trigger code is read-only toward the business receiver",
        "duplicate trigger instances and repeated events are safe",
        "Airflow 3.2.x",
        "2.10, 2.11, 3.0, 3.1, 3.2, and 3.3",
        "dpone-airflow-pack",
        "apache-airflow-providers-dpone",
    ):
        assert contract in spec


def test_rest_delivery_v1_scope_is_bounded_and_partial_results_fail_closed() -> None:
    spec = _read(SPEC)

    assert "synchronous batch plus one-submit async bulk job" in spec
    assert "Automatic item retry" in spec
    assert "Automatic quarantine" in spec
    assert "V1.1" in spec
    assert "V1.2" in spec
    assert "partial_result: fail_and_retain" in spec
    assert "V1 does not promote a checkpoint for `PARTIALLY_APPLIED`" in spec


def test_rest_delivery_market_claims_are_sourced_and_measurable() -> None:
    spec = _read(SPEC)

    for comparator in (
        "dlt, current docs",
        "Duckle, current repository/docs",
        "Redpanda Connect `http_client`",
        "Apache NiFi InvokeHTTP 2.11.0",
        "Informatica REST V2",
        "Microsoft SSIS",
        "Airbyte, repository master",
        "Fivetran Activations",
        "Sling, current docs",
        "Pentaho Data Integration 11.0",
        "Apache Beam",
        "gusty",
        "Astronomer Cosmos",
    ):
        assert comparator in spec

    assert spec.count("checked 2026-08-30") >= 14
    for evidence in (
        "remote_effect_fault_matrix.json",
        "identity_conflict_matrix.json",
        "airflow_async_acceptance.json",
        "adaptive_frontier_benchmark.json",
        "recovery_ux.json",
    ):
        assert evidence in spec


def test_rest_delivery_adrs_are_proposed_linked_and_navigable() -> None:
    spec = _read(SPEC)
    index = _read(ROOT / "docs" / "adr-index.md")
    navigation = _read(ROOT / "mkdocs.yml")

    assert DESIGN_CONTRACT.name in spec
    assert THREAT_MODEL.name in spec
    assert THREAT_MODEL.name in navigation

    for path in ADR_PATHS:
        adr = _read(path)
        relative_path = path.relative_to(ROOT / "docs").as_posix()
        assert "## Status\n\nProposed" in adr
        assert path.name in spec
        assert relative_path in index
        assert relative_path in navigation


def test_older_generation_cannot_submit_after_newer_overlap() -> None:
    contract = _contract()
    generation = contract["generation_fence"]
    assert isinstance(generation, dict)

    assert generation["mutation_policies"]["partition_replace"]["scope_conflict_mode"] == "overlap"
    assert generation["mutation_policies"]["incremental_append"]["correction_revision_allowed"] is False
    assert generation["value"]["ordered_fields"] == [
        "scheduled_interval_end_utc",
        "explicit_correction_revision",
    ]
    assert (
        "lower_overlapping_generation_cannot_enter_submitting_after_higher_admission" in generation["admission_rules"]
    )
    assert generation["errors"]["stale"] == "REST_DELIVERY_STALE_GENERATION"


def test_new_generation_blocks_on_older_unknown_effect() -> None:
    generation = _contract()["generation_fence"]
    assert isinstance(generation, dict)

    assert "UNKNOWN" in generation["unresolved_effect_states"]
    assert "PARTIALLY_APPLIED" in generation["unresolved_effect_states"]
    assert generation["errors"]["overlap_in_flight"] == ("REST_DELIVERY_OVERLAPPING_EFFECT_IN_FLIGHT")


def test_parent_plan_is_admitted_before_first_child_submission() -> None:
    parent_plan = _contract()["parent_plan"]
    assert isinstance(parent_plan, dict)

    required = {
        "parent_operation",
        "immutable_parent_plan",
        "every_child_delivery_key",
        "every_child_payload_ref_and_digest",
        "coverage_proof",
        "generation",
        "frontier_order",
    }
    assert required <= set(parent_plan["admission_transaction_contains"])
    assert "admit_complete_parent_plan_before_first_child_submitting" in parent_plan["barriers"]


def test_existing_parent_never_replans_after_code_upgrade() -> None:
    contract = _contract()
    parent_plan = contract["parent_plan"]
    identities = contract["identities"]
    assert isinstance(parent_plan, dict)
    assert isinstance(identities, dict)

    assert "existing_parent_operation_never_replans" in parent_plan["barriers"]
    assert "planner_algorithm_version" in identities["parent_plan_digest"]["fields"]


def test_snapshot_is_released_after_immutable_parent_materialization() -> None:
    barriers = _contract()["parent_plan"]["barriers"]

    assert barriers.index("verify_parent_completeness_and_digest_before_snapshot_close") < barriers.index(
        "close_source_snapshot_after_parent_materialization"
    )
    assert barriers.index("close_source_snapshot_after_parent_materialization") < barriers.index(
        "seal_every_child_before_parent_admission"
    )
    assert "admit_complete_parent_plan_before_first_child_submitting" in barriers


def test_journal_recovery_epoch_blocks_mutation_after_restore() -> None:
    journal = _contract()["journal"]
    assert isinstance(journal, dict)

    assert journal["mutation_admission_rpo"] == "zero"
    assert journal["submit_attempt_rpo"] == "zero"
    marker = journal["immutable_pre_attempt_marker"]
    assert marker["kind"] == "MAY_ATTEMPT"
    assert marker["create_if_absent_key"] == [
        "operation_namespace",
        "operation_id",
        "attempt_ordinal",
    ]
    assert "restore_without_proven_zero_rpo" in journal["mutation_gate"]["blocked_after"]
    assert "journal_epoch_mismatch" in journal["mutation_gate"]["blocked_after"]


def test_policy_only_change_does_not_change_mutation_intent() -> None:
    identities = _contract()["identities"]
    assert isinstance(identities, dict)

    mutation_fields = set(identities["mutation_intent_digest"]["fields"])
    profile_fields = set(identities["profile_contract_digest"]["fields"])
    assert "execution_policy_digest" not in mutation_fields
    assert "operation_semantics_digest" in mutation_fields
    assert {"operation_semantics_digest", "execution_policy_digest"} <= profile_fields


def test_operation_namespace_and_remote_principal_bind_identity() -> None:
    contract = _contract()
    namespace = contract["namespaces"]["operation_namespace"]
    receiver = contract["receiver_binding"]
    identities = contract["identities"]

    assert namespace["fields"] == [
        "platform_instance_id",
        "tenant_id",
        "environment_id",
        "namespace_revision",
    ]
    assert contract["namespaces"]["unique_admission_key"] == [
        "operation_namespace",
        "delivery_key",
    ]
    assert {
        "remote_principal_id",
        "remote_account_id",
        "remote_resource_namespace",
    } <= set(receiver["stable_non_secret_identity_fields"])
    assert receiver["certification"]["principal_swap_changes_identity"] is True
    assert identities["receiver_mutation_key"]["fields"] == ["operation_namespace", "delivery_key"]
    assert set(identities["receiver_binding_semantics_digest"]["fields"]) == {
        "authority_origin",
        "remote_principal_id",
        "remote_account_id",
        "remote_resource_namespace",
        "authentication_semantics",
        "tls_policy_identity",
        "network_policy_identity",
    }


def test_remote_identity_and_query_authentication_fail_closed_by_default() -> None:
    contract = _contract()
    identity = contract["receiver_binding"]["certification"]["remote_identity"]
    query_key = contract["authentication"]["api_key_query"]

    assert set(identity["allowed_assurance_modes"]) == {"receiver_attested", "platform_attested"}
    assert {"dual_approval", "periodic_recertification", "immutable_evidence"} <= set(
        identity["platform_attested_requires"]
    )
    assert query_key["production_default"] == "FORBIDDEN"
    assert {"security_approval", "end_to_end_url_redaction"} <= set(query_key["exception_requires"])


def test_stable_error_catalog_carries_operator_metadata() -> None:
    contract = _contract()
    catalog = contract["error_catalog"]
    required_metadata = {"exit_class", "retryability", "operator_action", "alert_severity", "evidence_schema"}

    required_codes = {
        "REST_DELIVERY_INTENT_CONFLICT",
        "REST_DELIVERY_REMOTE_EFFECT_UNKNOWN",
        "REST_DELIVERY_PARTIALLY_APPLIED",
        "REST_DELIVERY_UNSAFE_RETRY",
        "REST_DELIVERY_SCOPE_EXCEEDS_LIMIT",
        "REST_DELIVERY_COUNT_MISMATCH",
        "REST_DELIVERY_RECEIPT_EXPIRED",
        "REST_DELIVERY_PROTOCOL_UNKNOWN",
        "REST_DELIVERY_STALE_GENERATION",
        "REST_DELIVERY_OVERLAPPING_EFFECT_IN_FLIGHT",
        "REST_DELIVERY_RETRY_AFTER_EXCEEDS_BUDGET",
        "REST_DELIVERY_APPEND_SCOPE_ALREADY_APPLIED",
        "REST_DELIVERY_JOURNAL_EPOCH_BLOCKED",
    }
    assert set(contract["stable_error_codes"]) == required_codes
    assert set(catalog) == required_codes
    for code in required_codes:
        assert required_metadata == set(catalog[code])


def test_effect_conflict_domain_tracks_physical_receiver_effect() -> None:
    generation = _contract()["generation_fence"]

    assert generation["effect_conflict_domain_fields"] == [
        "remote_principal_id",
        "remote_account_id",
        "remote_resource_namespace",
        "receiver_resource_id",
        "effect_conflict_group",
    ]
    assert generation["linearization"]["domain_lock"]["key_fields"] == ["effect_conflict_domain"]


def test_reconciliation_preserves_acknowledgement_and_resolves_effect_independently() -> None:
    machine = _contract()["state_machine"]
    legal = machine["legal_tuples"]
    transitions = machine["operator_only_transitions"]

    for acknowledgement, source in (
        ("NOT_ACCEPTED", "reconciling_unknown_not_accepted"),
        ("ACCEPTED", "reconciling_unknown_accepted_ack"),
        ("SUCCEEDED", "reconciling_unknown_success_ack"),
        ("FAILED", "reconciling_unknown_failed_ack"),
        ("CANCELLED", "reconciling_unknown_cancelled_ack"),
        ("EXPIRED", "reconciling_unknown_expired_ack"),
        ("PROTOCOL_UNKNOWN", "reconciling_unknown_protocol_ack"),
    ):
        targets = [transition["to"] for transition in transitions if transition["from"] == source]
        assert {legal[target][1] for target in targets} == {
            "NONE_PROVEN",
            "FULLY_APPLIED",
            "PARTIALLY_APPLIED",
            "UNKNOWN",
        }
        assert {legal[target][2] for target in targets} == {acknowledgement}


def test_researched_design_promotes_through_separate_immutable_receipts() -> None:
    strategy = _contract()["promotion_strategy"]

    assert strategy["researched_contract"] == {
        "immutable": True,
        "status": "RESEARCHED",
        "digest_algorithm": "sha256",
    }
    assert strategy["approval_receipt"]["schema"] == "dpone.rest-bulk-delivery-approval.v1"
    assert strategy["approval_receipt"]["mutates_researched_contract"] is False
    assert strategy["production_certification"]["separate_receipt_required"] is True


def test_rendered_request_semantics_bind_complete_multipart_envelope() -> None:
    fields = set(_contract()["identities"]["rendered_request_semantics_digest"]["fields"])

    assert {
        "ordered_scalar_multipart_parts",
        "file_part_name",
        "deterministic_filename",
        "content_disposition",
        "content_type",
        "deterministic_boundary",
        "sealed_payload_digest",
    } <= fields


def test_success_tombstone_survives_maximum_replay_horizon() -> None:
    retention = _contract()["effect_authority_retention"]
    assert isinstance(retention, dict)

    assert retention["append"] == "until_route_decommission_or_audited_namespace_reset"
    assert retention["partition_replace"] == ("permanent_highest_applied_generation_per_conflict_scope")
    assert retention["reset"] == "audited_operator_only"


def test_remote_job_capacity_is_reserved_before_submit_and_unknown_holds_it() -> None:
    invariants = set(_contract()["invariants"])

    assert "remote_job_semaphore_reserved_before_submit" in invariants
    assert "unknown_remote_job_retains_capacity_until_reconciliation" in invariants


def test_state_cross_product_contains_only_legal_tuples() -> None:
    state_machine = _contract()["state_machine"]
    assert isinstance(state_machine, dict)
    axes = state_machine["axes"]
    legal_tuples = state_machine["legal_tuples"]
    assert isinstance(axes, dict)
    assert isinstance(legal_tuples, dict)

    axis_names = ["execution", "remote_effect", "acknowledgement", "verification", "checkpoint"]
    assert list(axes) == axis_names
    assert state_machine["initial_tuple"] == "planned"

    tuples = list(legal_tuples.values())
    assert len(tuples) == len({tuple(values) for values in tuples})
    for values in tuples:
        assert len(values) == len(axis_names)
        for index, axis_name in enumerate(axis_names):
            assert values[index] in axes[axis_name]

    tuple_names = set(legal_tuples)
    for transition_group in ("automatic_transitions", "operator_only_transitions"):
        for transition in state_machine[transition_group]:
            assert transition["from"] in tuple_names
            assert transition["to"] in tuple_names
            assert transition["cas"]

    assert legal_tuples["waiting"][1] == "UNDETERMINED"
    assert legal_tuples["final_unknown_success_ack"][1:3] == ["UNKNOWN", "SUCCEEDED"]
    assert ["FINALIZED", "UNKNOWN", "RUNNING"] in state_machine["forbidden_combinations"]


def test_threat_model_covers_second_review_security_boundaries() -> None:
    threat_model = _read(THREAT_MODEL)

    for threat in (
        "PostgreSQL failover loses `SUBMITTING`",
        "PITR or lossy restore rewinds journal",
        "Token rotation silently changes receiver tenant/account",
        "Cross-tenant/environment delivery-key collision",
        "Object-store payload is replaced or read cross-tenant",
        "Triggerer is exhausted by blocking I/O",
        "Mutable deny-only revocation",
    ):
        assert threat in threat_model
