"""Semantic invariants that portable JSON Schema cannot express."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from dpone.contracts.airflow_runtime_pod_retention_apply import derive_runtime_pod_apply_state
from dpone.contracts.deployment_cache_retention_state import (
    DeploymentCacheRetentionStateError,
    canonical_digest,
    parse_retention_recovery,
    retention_operation_id,
)

SemanticViolation = tuple[str, str, str]

_RETENTION_PLAN_KIND = "dpone.deployment-cache-retention-plan.v1"
_RETENTION_APPLY_KINDS = frozenset(
    {
        "dpone.deployment-cache-retention-apply.v1",
        "dpone.deployment-cache-retention-apply.v2",
        "dpone.deployment-cache-retention-apply.v3",
    }
)
_RUNTIME_POD_APPLY_KIND = "dpone.airflow-runtime-pod-retention-apply.v1"
_RUNTIME_POD_PLAN_KIND = "dpone.airflow-runtime-pod-retention-plan.v1"
_RECOVERY_KINDS = frozenset(
    {
        "dpone.deployment-cache-retention-recovery.v1",
        "dpone.deployment-cache-retention-recovery.v2",
    }
)


def validate_gitops_semantics(payload: Mapping[str, object], *, kind: str) -> tuple[SemanticViolation, ...]:
    """Validate authoritative-item projections and canonical state semantics."""

    if kind == _RETENTION_PLAN_KIND:
        return _validate_retention_plan(payload)
    if kind in _RETENTION_APPLY_KINDS:
        return _validate_retention_apply(payload, kind=kind)
    if kind == _RUNTIME_POD_APPLY_KIND:
        return _validate_runtime_pod_apply_projection(payload)
    if kind == _RUNTIME_POD_PLAN_KIND:
        return _validate_runtime_pod_plan_projection(payload)
    if kind in _RECOVERY_KINDS:
        return _validate_recovery_state(payload)
    return ()


def _validate_retention_plan(payload: Mapping[str, object]) -> tuple[SemanticViolation, ...]:
    items = _mapping_items(payload.get("items"))
    delete_candidates = _item_values(items, action="delete", field="deployment_id")
    issues = list(_projection_issues(payload, (("delete_candidates", delete_candidates),)))
    issues.extend(
        _duplicate_issues(
            (
                ("items", _field_values(items, field="deployment_id")),
                ("delete_candidates", _string_sequence(payload.get("delete_candidates"))),
            )
        )
    )
    protected = set(_string_sequence(payload.get("protected_deployment_ids")))
    current = payload.get("current_deployment_id")
    forbidden = protected | ({current} if isinstance(current, str) else set())
    if forbidden.intersection(delete_candidates):
        issues.append(
            (
                "schema_state_semantics_invalid",
                "delete_candidates must not contain current or protected deployments",
                "delete_candidates",
            )
        )
    plan_sha256 = payload.get("plan_sha256")
    if isinstance(plan_sha256, str):
        body = {
            key: payload[key]
            for key in (
                "schema",
                "environment",
                "current_deployment_id",
                "protected_deployment_ids",
                "items",
                "delete_candidates",
                "recovery_revision",
            )
            if key in payload
        }
        if plan_sha256 != canonical_digest(body):
            issues.append(
                (
                    "schema_state_semantics_invalid",
                    "plan_sha256 must match the canonical retention plan body",
                    "plan_sha256",
                )
            )
    return tuple(issues)


def _validate_retention_apply(payload: Mapping[str, object], *, kind: str) -> tuple[SemanticViolation, ...]:
    items = _mapping_items(payload.get("items"))
    deleted = _item_values(items, action="deleted", field="deployment_id")
    skipped = _item_values(items, action="skipped", field="deployment_id")
    issues = list(
        _projection_issues(
            payload,
            (("deleted_deployment_ids", deleted), ("skipped_deployment_ids", skipped)),
        )
    )
    issues.extend(
        _duplicate_issues(
            (
                ("items", _field_values(items, field="deployment_id")),
                ("deleted_deployment_ids", _string_sequence(payload.get("deleted_deployment_ids"))),
                ("skipped_deployment_ids", _string_sequence(payload.get("skipped_deployment_ids"))),
            )
        )
    )
    current = payload.get("current_deployment_id")
    if isinstance(current, str) and current in deleted:
        issues.append(
            (
                "schema_state_semantics_invalid",
                "deleted_deployment_ids must not contain the current deployment",
                "deleted_deployment_ids",
            )
        )
    if set(deleted).intersection(skipped):
        issues.append(
            (
                "schema_state_semantics_invalid",
                "one deployment cannot be both deleted and skipped",
                "items",
            )
        )
    if kind == "dpone.deployment-cache-retention-apply.v3":
        environment = payload.get("environment")
        reviewed_plan = payload.get("reviewed_plan_sha256")
        review_id = payload.get("review_id")
        operation_id = payload.get("operation_id")
        if isinstance(operation_id, str) and all(
            isinstance(value, str) for value in (environment, reviewed_plan, review_id)
        ):
            try:
                expected_operation_id = retention_operation_id(
                    environment=str(environment),
                    reviewed_plan_sha256=str(reviewed_plan),
                    review_id=str(review_id),
                )
            except DeploymentCacheRetentionStateError:
                issues.append(
                    (
                        "schema_state_semantics_invalid",
                        "operation identity fields must satisfy the canonical retention bounds",
                        "operation_id",
                    )
                )
                expected_operation_id = None
        else:
            expected_operation_id = None
        if (
            isinstance(operation_id, str)
            and expected_operation_id is not None
            and operation_id != expected_operation_id
        ):
            issues.append(
                (
                    "schema_state_semantics_invalid",
                    "operation_id must bind environment, reviewed plan, and review occurrence",
                    "operation_id",
                )
            )
    return tuple(issues)


def _validate_runtime_pod_apply_projection(payload: Mapping[str, object]) -> tuple[SemanticViolation, ...]:
    items = _mapping_items(payload.get("items"))
    evidence_status = payload.get("evidence_status")
    assert isinstance(evidence_status, str)
    state = derive_runtime_pod_apply_state(items, evidence_status=evidence_status)
    issues = list(
        _projection_issues(
            payload,
            (
                ("delete_accepted_pod_names", list(state.delete_accepted_pod_names)),
                ("skipped_pod_names", list(state.skipped_pod_names)),
                ("failed_pod_names", list(state.failed_pod_names)),
            ),
        )
    )
    issues.extend(_runtime_pod_identity_issues(items))
    sequences = tuple(item.get("sequence") for item in items)
    if sequences != tuple(range(1, len(items) + 1)):
        issues.append(
            (
                "schema_state_semantics_invalid",
                "runtime Pod apply item sequences must be contiguous and ordered",
                "items",
            )
        )
    if payload.get("status") != state.status:
        issues.append(
            (
                "schema_state_semantics_invalid",
                "status must be derived exactly from authoritative items and evidence_status",
                "status",
            )
        )
    return tuple(issues)


def _validate_runtime_pod_plan_projection(payload: Mapping[str, object]) -> tuple[SemanticViolation, ...]:
    items = _mapping_items(payload.get("items"))
    candidates = tuple(
        sorted(
            (item for item in items if item.get("action") == "delete"),
            key=_runtime_pod_candidate_sort_key,
        )
    )
    delete_candidates = [str(item["pod_name"]) for item in candidates]
    issues = list(_projection_issues(payload, (("delete_candidates", delete_candidates),)))
    issues.extend(_runtime_pod_identity_issues(items))
    inventory = payload.get("inventory")
    if isinstance(inventory, Mapping):
        expected_inventory = {
            "terminal_pods": len(items),
            "succeeded_pods": sum(item.get("phase") == "Succeeded" for item in items),
            "failed_pods": sum(item.get("phase") == "Failed" for item in items),
            "quarantined": sum(item.get("action") == "quarantine" for item in items),
        }
        if inventory != expected_inventory:
            issues.append(
                (
                    "schema_state_semantics_invalid",
                    "runtime Pod inventory must be derived exactly from authoritative items",
                    "inventory",
                )
            )
        expected_status = (
            "needs_attention" if expected_inventory["quarantined"] else "needs_cleanup" if delete_candidates else "ok"
        )
        if payload.get("status") != expected_status:
            issues.append(
                (
                    "schema_state_semantics_invalid",
                    "runtime Pod plan status must be derived exactly from authoritative items",
                    "status",
                )
            )
    return tuple(issues)


def _runtime_pod_candidate_sort_key(item: Mapping[str, object]) -> tuple[int, str, str]:
    age_seconds = item.get("age_seconds")
    age = age_seconds if isinstance(age_seconds, int) and not isinstance(age_seconds, bool) else 0
    return (-age, str(item.get("pod_name")), str(item.get("pod_ref")))


def _runtime_pod_identity_issues(items: Sequence[Mapping[str, object]]) -> tuple[SemanticViolation, ...]:
    return _duplicate_issues(
        (
            ("items.pod_ref", _field_values(items, field="pod_ref")),
            ("items.precondition_ref", _field_values(items, field="precondition_ref")),
            ("items.pod_name", _field_values(items, field="pod_name")),
        ),
        identity="runtime Pod identity",
    )


def _validate_recovery_state(payload: Mapping[str, object]) -> tuple[SemanticViolation, ...]:
    try:
        parse_retention_recovery(payload)
    except DeploymentCacheRetentionStateError as exc:
        return (("schema_state_semantics_invalid", str(exc), "$"),)
    return ()


def _projection_issues(
    payload: Mapping[str, object],
    expected: Sequence[tuple[str, list[str]]],
) -> tuple[SemanticViolation, ...]:
    return tuple(
        (
            "schema_derived_projection_mismatch",
            f"{field} must be derived exactly from authoritative items",
            field,
        )
        for field, values in expected
        if payload.get(field) != values
    )


def _mapping_items(value: object) -> tuple[Mapping[str, object], ...]:
    assert isinstance(value, list)
    assert all(isinstance(item, Mapping) for item in value)
    return tuple(item for item in value if isinstance(item, Mapping))


def _string_sequence(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, str))


def _item_values(items: Sequence[Mapping[str, object]], *, action: str, field: str) -> list[str]:
    return [value for item in items if item.get("action") == action and isinstance((value := item.get(field)), str)]


def _field_values(items: Sequence[Mapping[str, object]], *, field: str) -> tuple[str, ...]:
    return tuple(value for item in items if isinstance((value := item.get(field)), str))


def _duplicate_issues(
    projections: Sequence[tuple[str, Sequence[str]]],
    *,
    identity: str = "deployment identity",
) -> tuple[SemanticViolation, ...]:
    issues: list[SemanticViolation] = []
    for path, values in projections:
        if len(values) != len(set(values)):
            issues.append(
                (
                    "schema_state_semantics_invalid",
                    f"{path} must contain each {identity} at most once",
                    path,
                )
            )
    return tuple(issues)


__all__ = ["SemanticViolation", "validate_gitops_semantics"]
