"""Pure identity payload and lineage rules for semantic-refresh operations."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol

from dpone.contracts.semantic_refresh_core import (
    SemanticRefreshContractError,
    require_digest,
    require_positive_int,
    require_text,
)


class _EffectiveKeyColumn(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def source_type(self) -> str: ...

    @property
    def target_type(self) -> str: ...

    def to_dict(self) -> dict[str, object]: ...


class _WorkflowMode(Protocol):
    @property
    def value(self) -> str: ...


OPERATION_PLAN_REQUIRED_FIELDS = frozenset(
    {
        "schema",
        "operation_id",
        "operation_plan_sha256",
        "model_unique_id",
        "release_id",
        "deployment_id",
        "environment",
        "workflow_id",
        "scope_family_id",
        "scope_revision",
        "target_predecessor_generation_id",
        "owner_generation",
        "platform_policy_digest",
        "resource_policy_digest",
        "writer_assurance_digest",
        "operation_kind",
        "model_definition_proof_sha256",
        "mutation_closure_sha256",
        "sqlserver_lifecycle_policy_sha256",
        "read_dependency_proof_sha256",
        "scope_start",
        "scope_end",
        "archetype",
        "event_time_column",
        "effective_key_columns",
        "effective_key_template_sha256",
        "effective_key_mapping_sha256",
        "missing_key_policy",
        "mutation_order",
    }
)
OPERATION_PLAN_OPTIONAL_FIELDS = frozenset(
    {
        "scope_predecessor_operation_id",
        "replaces_failed_operation_id",
        "replacement_reason",
        "replacement_ordinal",
    }
)


def validate_effective_key(
    columns: Sequence[_EffectiveKeyColumn],
    event_time_column: str,
) -> None:
    """Require a unique, non-empty key containing the event-time column."""

    if not isinstance(columns, tuple) or not columns or any(not _is_effective_key_column(item) for item in columns):
        raise SemanticRefreshContractError("effective_key_columns must be a non-empty tuple")
    names = tuple(column.name for column in columns)
    if len(names) != len(set(names)):
        raise SemanticRefreshContractError("effective key column names must be unique")
    if event_time_column not in names:
        raise SemanticRefreshContractError("event_time_column must be part of the effective key")
    event_time = columns[names.index(event_time_column)]
    if (event_time.source_type, event_time.target_type) not in {
        ("date", "Date"),
        ("datetime2(6)", "DateTime64(6,'UTC')"),
    }:
        raise SemanticRefreshContractError("event_time_column must use date->Date or datetime2(6)->DateTime64(6,'UTC')")


def parse_mutation_order(value: list[str]) -> tuple[str, str]:
    """Parse the only admitted SQL Server DML order."""

    expected = ("UPDATE", "INSERT")
    if tuple(value) != expected:
        raise SemanticRefreshContractError("semantic refresh mutation order must be UPDATE then INSERT")
    return expected


def validate_operation_lineage(
    *,
    operation_kind: _WorkflowMode,
    scope_revision: int,
    scope_predecessor_operation_id: str | None,
    replaces_failed_operation_id: str | None,
    replacement_reason: str | None,
    replacement_ordinal: int | None,
) -> None:
    """Enforce disjoint normal, replay, and failed-replacement lineage."""

    replacement_values = (
        replaces_failed_operation_id,
        replacement_reason,
        replacement_ordinal,
    )
    if operation_kind.value == "normal":
        if (
            scope_revision != 1
            or scope_predecessor_operation_id is not None
            or any(item is not None for item in replacement_values)
        ):
            raise SemanticRefreshContractError("normal operation lineage is invalid")
    elif operation_kind.value == "complete_scope_replay":
        if (
            scope_revision < 2
            or scope_predecessor_operation_id is None
            or any(item is not None for item in replacement_values)
        ):
            raise SemanticRefreshContractError("complete-scope replay lineage is invalid")
        require_digest(scope_predecessor_operation_id, "scope_predecessor_operation_id")
    else:
        if scope_predecessor_operation_id is not None or any(item is None for item in replacement_values):
            raise SemanticRefreshContractError("failed-precommit replacement lineage is invalid")
        require_digest(replaces_failed_operation_id, "replaces_failed_operation_id")
        require_text(replacement_reason, "replacement_reason")
        require_positive_int(replacement_ordinal, "replacement_ordinal")


def optional_digest(raw: Mapping[str, object], field_name: str) -> str | None:
    """Parse an optional canonical digest without treating explicit null as absent."""

    if field_name not in raw:
        return None
    return require_digest(raw[field_name], field_name)


def optional_text(raw: Mapping[str, object], field_name: str) -> str | None:
    """Parse optional canonical non-empty text."""

    if field_name not in raw:
        return None
    return require_text(raw[field_name], field_name)


def optional_positive_int(raw: Mapping[str, object], field_name: str) -> int | None:
    """Parse an optional positive integer."""

    if field_name not in raw:
        return None
    return require_positive_int(raw[field_name], field_name)


def operation_identity_payload(
    *,
    schema: str,
    archetype: str,
    missing_key_policy: str,
    mutation_order: tuple[str, str],
    model_unique_id: str,
    release_id: str,
    deployment_id: str,
    environment: str,
    workflow_id: str,
    scope_family_id: str,
    scope_revision: int,
    target_predecessor_generation_id: str,
    owner_generation: int,
    platform_policy_digest: str,
    resource_policy_digest: str,
    writer_assurance_digest: str,
    operation_kind: _WorkflowMode,
    model_definition_proof_sha256: str,
    mutation_closure_sha256: str,
    sqlserver_lifecycle_policy_sha256: str,
    read_dependency_proof_sha256: str,
    scope_start: str,
    scope_end: str,
    event_time_column: str,
    effective_key_columns: Sequence[_EffectiveKeyColumn],
    effective_key_template_sha256: str,
    effective_key_mapping_sha256: str,
    scope_predecessor_operation_id: str | None,
    replaces_failed_operation_id: str | None,
    replacement_reason: str | None,
    replacement_ordinal: int | None,
) -> dict[str, object]:
    """Return semantic operation identity with no attempt/runtime facts."""

    result: dict[str, object] = {
        "archetype": archetype,
        "deployment_id": deployment_id,
        "effective_key_columns": [column.to_dict() for column in effective_key_columns],
        "effective_key_mapping_sha256": effective_key_mapping_sha256,
        "effective_key_template_sha256": effective_key_template_sha256,
        "environment": environment,
        "event_time_column": event_time_column,
        "missing_key_policy": missing_key_policy,
        "model_definition_proof_sha256": model_definition_proof_sha256,
        "model_unique_id": model_unique_id,
        "mutation_closure_sha256": mutation_closure_sha256,
        "mutation_order": list(mutation_order),
        "operation_kind": operation_kind.value,
        "owner_generation": owner_generation,
        "platform_policy_digest": platform_policy_digest,
        "read_dependency_proof_sha256": read_dependency_proof_sha256,
        "release_id": release_id,
        "resource_policy_digest": resource_policy_digest,
        "schema": schema,
        "scope_end": scope_end,
        "scope_family_id": scope_family_id,
        "scope_revision": scope_revision,
        "scope_start": scope_start,
        "sqlserver_lifecycle_policy_sha256": sqlserver_lifecycle_policy_sha256,
        "target_predecessor_generation_id": target_predecessor_generation_id,
        "workflow_id": workflow_id,
        "writer_assurance_digest": writer_assurance_digest,
    }
    optional_fields = {
        "scope_predecessor_operation_id": scope_predecessor_operation_id,
        "replaces_failed_operation_id": replaces_failed_operation_id,
        "replacement_reason": replacement_reason,
        "replacement_ordinal": replacement_ordinal,
    }
    result.update({key: value for key, value in optional_fields.items() if value is not None})
    return result


def _is_effective_key_column(value: object) -> bool:
    return all(hasattr(value, field) for field in ("name", "source_type", "target_type", "to_dict"))


__all__ = [
    "OPERATION_PLAN_OPTIONAL_FIELDS",
    "OPERATION_PLAN_REQUIRED_FIELDS",
    "operation_identity_payload",
    "optional_digest",
    "optional_positive_int",
    "optional_text",
    "parse_mutation_order",
    "validate_effective_key",
    "validate_operation_lineage",
]
