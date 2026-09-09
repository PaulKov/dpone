"""Identity, generation, journal, and orchestration rules for REST delivery."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from .rest_delivery_design_contract_types import (
    DesignContractIssue,
    DesignContractReport,
    as_mapping,
    as_sequence,
    build_contract_report,
    contract_issue,
)

_REQUIRED_INVARIANTS = {
    "async_bulk_job_has_one_in_flight_child_in_v1",
    "existing_parent_never_replans",
    "every_unknown_reconciliation_can_resolve_none_full_partial_or_remain_unknown",
    "physical_receiver_effects_share_conflict_domain_across_local_namespaces_and_mutation_kinds",
    "incremental_append_rejects_correction_generation_by_default",
    "journal_restore_blocks_submission_without_zero_rpo",
    "older_overlapping_generation_cannot_submit",
    "policy_only_change_does_not_change_mutation_intent",
    "request_not_started_can_retry_with_same_delivery_key",
    "credential_rotation_preserves_binding_when_semantics_unchanged",
    "receiver_binding_semantics_changes_mutation_intent",
    "receiver_mutation_key_includes_operation_namespace_without_payload_digest",
    "reconciliation_preserves_acknowledgement_while_resolving_effect",
    "researched_design_contract_is_immutable_and_approval_is_separate_receipt",
    "stable_error_catalog_is_complete_and_metadata_bearing",
    "successful_acknowledgement_preserved_on_partial_or_unknown_effect",
}


def validate_authority_semantics(contract: Mapping[str, Any]) -> Iterable[DesignContractIssue]:
    """Validate non-state correctness decisions frozen by the reviewed design."""

    yield from _identity_issues(contract)
    yield from _generation_issues(contract)
    yield from _attempt_and_capacity_issues(contract)
    yield from _continuation_issues(contract)
    yield from _source_barrier_issues(contract)
    yield from _error_catalog_issues(contract)
    yield from _promotion_issues(contract)
    missing_invariants = _REQUIRED_INVARIANTS - set(as_sequence(contract["invariants"]))
    for invariant in sorted(missing_invariants):
        yield contract_issue(
            "invariant.missing", f"$.invariants.{invariant}", "Required executable invariant is missing."
        )


def _generation_issues(contract: Mapping[str, Any]) -> Iterable[DesignContractIssue]:
    generation = as_mapping(contract["generation_fence"])
    append = as_mapping(as_mapping(generation["mutation_policies"])["incremental_append"])
    if append.get("correction_revision_allowed") is not False:
        yield contract_issue(
            "generation.append_correction",
            "$.generation_fence.mutation_policies.incremental_append",
            "Append correction must be disabled by default.",
        )
    if append.get("already_applied_error") != "REST_DELIVERY_APPEND_SCOPE_ALREADY_APPLIED":
        yield contract_issue(
            "generation.append_error",
            "$.generation_fence.mutation_policies.incremental_append",
            "Append duplicate scope needs the stable error.",
        )
    linearization = as_mapping(generation["linearization"])
    domain_lock = as_mapping(linearization["domain_lock"])
    scope_range = as_mapping(linearization["scope_range"])
    watermark = as_mapping(linearization["watermark"])
    if domain_lock.get("acquisition") != "insert_on_conflict_then_select_for_update":
        yield contract_issue(
            "generation.linearization_lock",
            "$.generation_fence.linearization.domain_lock",
            "PostgreSQL domain row SELECT FOR UPDATE is required.",
        )
    conflict_fields = [str(value) for value in as_sequence(generation["effect_conflict_domain_fields"])]
    expected_conflict_fields = [
        "remote_principal_id",
        "remote_account_id",
        "remote_resource_namespace",
        "receiver_resource_id",
        "effect_conflict_group",
    ]
    if conflict_fields != expected_conflict_fields or as_sequence(domain_lock["key_fields"]) != [
        "effect_conflict_domain"
    ]:
        yield contract_issue(
            "generation.physical_conflict_domain",
            "$.generation_fence.effect_conflict_domain_fields",
            "Effect conflicts must be keyed by physical receiver identity and profile-owned conflict group.",
        )
    if scope_range.get("canonical_form") != "half_open" or scope_range.get("index") != "gist":
        yield contract_issue(
            "generation.range_index",
            "$.generation_fence.linearization.scope_range",
            "Half-open GiST-backed ranges are required.",
        )
    if watermark.get("representation") != "normalized_non_overlapping_interval_map":
        yield contract_issue(
            "generation.watermark_scope",
            "$.generation_fence.linearization.watermark",
            "Watermark must be an interval map.",
        )
    required_rules = {
        "lower_overlapping_generation_cannot_enter_submitting_after_higher_admission",
        "higher_generation_cannot_submit_while_lower_overlap_is_effect_unresolved",
        "fully_applied_generation_permanently_stales_every_lower_overlap",
    }
    if not required_rules <= set(as_sequence(generation["admission_rules"])):
        yield contract_issue(
            "generation.admission_rules",
            "$.generation_fence.admission_rules",
            "Generation monotonicity rules are incomplete.",
        )
    ordered_steps = [
        "select_domain_lock_for_update",
        "query_overlapping_operations_and_interval_watermarks",
        "reject_stale_or_unresolved_overlap",
        "insert_generation_admission",
        "commit_before_submit",
    ]
    if not _is_ordered_subsequence(ordered_steps, as_sequence(linearization["admission_transaction_steps"])):
        yield contract_issue(
            "generation.transaction_order",
            "$.generation_fence.linearization.admission_transaction_steps",
            "Generation fence must linearize before admission and submit.",
        )


def _identity_issues(contract: Mapping[str, Any]) -> Iterable[DesignContractIssue]:
    identities = as_mapping(contract["identities"])
    mutation_fields = set(as_sequence(as_mapping(identities["mutation_intent_digest"])["fields"]))
    profile_fields = set(as_sequence(as_mapping(identities["profile_contract_digest"])["fields"]))
    if "execution_policy_digest" in mutation_fields:
        yield contract_issue(
            "identity.policy_changes_intent",
            "$.identities.mutation_intent_digest.fields",
            "Execution policy must not change mutation intent.",
        )
    if not {"operation_semantics_digest", "execution_policy_digest"} <= profile_fields:
        yield contract_issue(
            "identity.profile_contract",
            "$.identities.profile_contract_digest.fields",
            "Profile identity must bind semantics and execution policy.",
        )
    receiver_key_fields = [
        str(value) for value in as_sequence(as_mapping(identities["receiver_mutation_key"])["fields"])
    ]
    if receiver_key_fields != ["operation_namespace", "delivery_key"]:
        yield contract_issue(
            "identity.receiver_mutation_key",
            "$.identities.receiver_mutation_key.fields",
            "Receiver mutation identity must namespace delivery_key without binding payload content.",
        )
    binding_fields = set(
        str(value) for value in as_sequence(as_mapping(identities["receiver_binding_semantics_digest"])["fields"])
    )
    required_binding_fields = {
        "authority_origin",
        "remote_principal_id",
        "remote_account_id",
        "remote_resource_namespace",
        "authentication_semantics",
        "tls_policy_identity",
        "network_policy_identity",
    }
    if binding_fields != required_binding_fields or "receiver_binding_semantics_digest" not in mutation_fields:
        yield contract_issue(
            "identity.receiver_binding_semantics",
            "$.identities.receiver_binding_semantics_digest.fields",
            "Mutation intent must bind the complete stable non-secret receiver semantics.",
        )


def _attempt_and_capacity_issues(contract: Mapping[str, Any]) -> Iterable[DesignContractIssue]:
    journal = as_mapping(contract["journal"])
    marker = as_mapping(journal["immutable_pre_attempt_marker"])
    marker_key = [str(value) for value in as_sequence(marker["create_if_absent_key"])]
    marker_fields = {str(value) for value in as_sequence(marker["fields"])}
    if marker_key != ["operation_namespace", "operation_id", "attempt_ordinal"]:
        yield contract_issue(
            "attempt.marker_key",
            "$.journal.immutable_pre_attempt_marker.create_if_absent_key",
            "Marker key must identify one attempt.",
        )
    if not {"attempt_id", "expected_journal_revision", "journal_epoch", "mutation_intent_digest"} <= marker_fields:
        yield contract_issue(
            "attempt.marker_fields",
            "$.journal.immutable_pre_attempt_marker.fields",
            "Attempt marker lacks required correlation fields.",
        )
    semaphore = as_mapping(journal["remote_job_semaphore"])
    if semaphore.get("authority") != "same_postgresql_control_plane":
        yield contract_issue(
            "capacity.authority",
            "$.journal.remote_job_semaphore.authority",
            "V1 capacity authority must be PostgreSQL.",
        )
    transaction = {str(value) for value in as_sequence(semaphore["atomic_submit_transaction_contains"])}
    required = {
        "capacity_reservation",
        "attempt_scoped_may_attempt_binding",
        "attempt_row",
        "operation_cas_to_submitting",
    }
    if not required <= transaction:
        yield contract_issue(
            "capacity.atomicity",
            "$.journal.remote_job_semaphore.atomic_submit_transaction_contains",
            "Submit transaction does not atomically bind capacity and attempt.",
        )
    blocked_after = set(as_sequence(as_mapping(journal["mutation_gate"])["blocked_after"]))
    if not {"restore_without_proven_zero_rpo", "journal_epoch_mismatch"} <= blocked_after:
        yield contract_issue(
            "journal.restore_gate",
            "$.journal.mutation_gate.blocked_after",
            "Restore or journal-epoch uncertainty must block submit.",
        )


def _continuation_issues(contract: Mapping[str, Any]) -> Iterable[DesignContractIssue]:
    continuations = as_mapping(contract["continuations"])
    if as_sequence(continuations["automatic"]) != ["poll_receipt"]:
        yield contract_issue(
            "continuation.automatic", "$.continuations.automatic", "poll_receipt must be the only continuation."
        )
    if "RETRY_WAIT" not in as_sequence(continuations["internal_automatic_states"]):
        yield contract_issue(
            "continuation.retry_wait", "$.continuations.internal_automatic_states", "RETRY_WAIT must remain internal."
        )
    async_bulk = as_mapping(as_mapping(contract["async_execution"])["async_bulk_job"])
    if async_bulk.get("max_in_flight_children") != 1:
        yield contract_issue(
            "continuation.async_parallel",
            "$.async_execution.async_bulk_job.max_in_flight_children",
            "V1 async bulk must be serial.",
        )


def _source_barrier_issues(contract: Mapping[str, Any]) -> Iterable[DesignContractIssue]:
    barriers = [str(value) for value in as_sequence(as_mapping(contract["parent_plan"])["barriers"])]
    ordered_barriers = [
        "materialize_immutable_parent_before_planning",
        "verify_parent_completeness_and_digest_before_snapshot_close",
        "close_source_snapshot_after_parent_materialization",
        "seal_every_child_before_parent_admission",
        "admit_complete_parent_plan_before_first_child_submitting",
    ]
    if not _is_ordered_subsequence(ordered_barriers, barriers):
        yield contract_issue(
            "source.snapshot_lifetime",
            "$.parent_plan.barriers",
            "Source snapshot must close before planning/sealing children.",
        )
    if "existing_parent_operation_never_replans" not in barriers:
        yield contract_issue("parent.replan", "$.parent_plan.barriers", "An admitted parent must never replan.")


def _error_catalog_issues(contract: Mapping[str, Any]) -> Iterable[DesignContractIssue]:
    catalog = set(as_mapping(contract["error_catalog"]))
    stable_codes = {str(value) for value in as_sequence(contract["stable_error_codes"])}
    for code in sorted(stable_codes - catalog):
        yield contract_issue(
            "error_catalog.missing", f"$.error_catalog.{code}", "Required stable error metadata is missing."
        )
    for code in sorted(catalog - stable_codes):
        yield contract_issue(
            "error_catalog.undeclared", f"$.error_catalog.{code}", "Catalog entry is absent from stable_error_codes."
        )
    for code in sorted(_referenced_error_codes(contract) - catalog):
        yield contract_issue(
            "error_catalog.reference_missing",
            f"$.error_catalog.{code}",
            "A normative contract error reference has no metadata.",
        )


def _promotion_issues(contract: Mapping[str, Any]) -> Iterable[DesignContractIssue]:
    strategy = as_mapping(contract["promotion_strategy"])
    researched = as_mapping(strategy["researched_contract"])
    approval = as_mapping(strategy["approval_receipt"])
    references = set(as_sequence(approval["references"]))
    required_references = {
        "design_contract_digest",
        "adr_digests",
        "owner_approvals",
        "evidence_requirements",
    }
    if researched.get("immutable") is not True or approval.get("mutates_researched_contract") is not False:
        yield contract_issue(
            "promotion.researched_contract_mutable",
            "$.promotion_strategy",
            "The RESEARCHED design must remain immutable and approval must be a separate receipt.",
        )
    if references != required_references:
        yield contract_issue(
            "promotion.approval_references",
            "$.promotion_strategy.approval_receipt.references",
            "Approval receipt must bind design, ADRs, owners, and evidence requirements.",
        )


def _referenced_error_codes(value: object, *, inside_catalog: bool = False) -> set[str]:
    if isinstance(value, Mapping):
        mapping_codes: set[str] = set()
        for key, item in value.items():
            mapping_codes.update(_referenced_error_codes(item, inside_catalog=inside_catalog or key == "error_catalog"))
        return mapping_codes
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        sequence_codes: set[str] = set()
        for item in value:
            sequence_codes.update(_referenced_error_codes(item, inside_catalog=inside_catalog))
        return sequence_codes
    if not inside_catalog and isinstance(value, str) and value.startswith("REST_DELIVERY_"):
        return {value}
    return set()


def _is_ordered_subsequence(expected: Sequence[str], actual: Sequence[Any]) -> bool:
    positions = {str(value): index for index, value in enumerate(actual)}
    return all(value in positions for value in expected) and all(
        positions[left] < positions[right] for left, right in zip(expected, expected[1:])
    )


__all__ = [
    "DesignContractIssue",
    "DesignContractReport",
    "build_contract_report",
    "validate_authority_semantics",
]
