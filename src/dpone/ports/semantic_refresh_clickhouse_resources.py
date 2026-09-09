"""Aggregate workflow-resource boundary for semantic-refresh publication."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from typing import Protocol

_KINDS = (
    "clickhouse_staging_bytes",
    "prepared_models",
    "retained_generation_bytes",
    "sealed_extract_bytes",
    "shadow_bytes",
)


@dataclass(frozen=True, order=True, slots=True)
class SemanticRefreshPublicationResourceAllocation:
    """One deterministic authority-derived operation allocation."""

    allocation_id: str
    reservation_id: str
    operation_id: str
    resource_kind: str
    amount: int


@dataclass(frozen=True, order=True, slots=True)
class ClickHouseScratchRelationAbsence:
    """Exact post-cleanup absence observation for one deterministic relation."""

    kind: str
    name: str
    expected_uuid: str | None
    observed_uuid: None = None

    def __post_init__(self) -> None:
        if self.kind not in {"shadow", "staging"}:
            raise ValueError("semantic-refresh scratch relation kind is unsupported")
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError("semantic-refresh scratch relation name must be non-empty")
        if self.expected_uuid is not None:
            _uuid(self.expected_uuid, "expected_uuid")
        if self.observed_uuid is not None:
            raise ValueError("semantic-refresh cleaned scratch relation must be absent")

    def to_mapping(self) -> dict[str, object]:
        """Return the canonical JSON projection used by cleanup evidence."""

        return {
            "expected_uuid": self.expected_uuid,
            "kind": self.kind,
            "name": self.name,
            "observed_uuid": None,
        }


@dataclass(frozen=True, slots=True)
class ClickHouseFailedScratchCleanupReceipt:
    """Canonical proof that both FAILED_PRE_COMMIT scratch relations are absent."""

    workflow_execution_id: str
    workflow_execution_binding_sha256: str
    operation_id: str
    operation_plan_sha256: str
    attempt_binding_sha256: str
    fencing_epoch: int
    target_uuid: str
    relations: tuple[ClickHouseScratchRelationAbsence, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.workflow_execution_id, str) or not self.workflow_execution_id.strip():
            raise ValueError("semantic-refresh workflow_execution_id must be non-empty")
        for field_name in (
            "workflow_execution_binding_sha256",
            "operation_id",
            "operation_plan_sha256",
            "attempt_binding_sha256",
        ):
            _digest(getattr(self, field_name), field_name)
        if isinstance(self.fencing_epoch, bool) or not isinstance(self.fencing_epoch, int) or self.fencing_epoch <= 0:
            raise ValueError("semantic-refresh fencing_epoch must be positive")
        _uuid(self.target_uuid, "target_uuid")
        if tuple(item.kind for item in self.relations) != ("shadow", "staging"):
            raise ValueError("semantic-refresh scratch absence must contain shadow/staging closure")

    @property
    def scratch_absence_evidence_sha256(self) -> str:
        """Return the canonical self-digest over the complete absence evidence."""

        return _fingerprint(self._identity_mapping())

    def to_mapping(self) -> dict[str, object]:
        """Return the closed evidence mapping including its recomputable digest."""

        return {
            **self._identity_mapping(),
            "scratch_absence_evidence_sha256": self.scratch_absence_evidence_sha256,
        }

    def _identity_mapping(self) -> dict[str, object]:
        return {
            "attempt_binding_sha256": self.attempt_binding_sha256,
            "fencing_epoch": self.fencing_epoch,
            "operation_id": self.operation_id,
            "operation_plan_sha256": self.operation_plan_sha256,
            "relations": [item.to_mapping() for item in self.relations],
            "schema": "dpone.semantic-refresh-clickhouse-failed-scratch-cleanup-receipt.v1",
            "target_uuid": self.target_uuid,
            "workflow_execution_binding_sha256": self.workflow_execution_binding_sha256,
            "workflow_execution_id": self.workflow_execution_id,
        }


def semantic_refresh_publication_resource_allocations(
    *,
    reservation_id: str,
    operation_id: str,
    sealed_extract_bytes: int,
    clickhouse_staging_bytes: int,
    shadow_bytes: int,
    retained_generation_bytes: int,
) -> tuple[SemanticRefreshPublicationResourceAllocation, ...]:
    """Derive the exact five-allocation closure from protected operation maxima."""

    if not isinstance(reservation_id, str) or not reservation_id.strip():
        raise ValueError("semantic-refresh reservation_id must be non-empty")
    _digest(operation_id, "operation_id")
    amounts = {
        "clickhouse_staging_bytes": clickhouse_staging_bytes,
        "prepared_models": 1,
        "retained_generation_bytes": retained_generation_bytes,
        "sealed_extract_bytes": sealed_extract_bytes,
        "shadow_bytes": shadow_bytes,
    }
    for kind, amount in amounts.items():
        if isinstance(amount, bool) or not isinstance(amount, int) or amount <= 0:
            raise ValueError(f"semantic-refresh {kind} allocation must be positive")
    return tuple(
        SemanticRefreshPublicationResourceAllocation(
            allocation_id=_allocation_id(reservation_id, operation_id, kind),
            reservation_id=reservation_id,
            operation_id=operation_id,
            resource_kind=kind,
            amount=amounts[kind],
        )
        for kind in _KINDS
    )


def _allocation_id(reservation_id: str, operation_id: str, resource_kind: str) -> str:
    return _fingerprint(
        {
            "operation_id": operation_id,
            "reservation_id": reservation_id,
            "resource_kind": resource_kind,
            "schema": "dpone.semantic-refresh-publication-resource-allocation.v1",
        }
    )


def _fingerprint(value: object) -> str:
    raw = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _digest(value: object, field: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 71
        or not value.startswith("sha256:")
        or any(character not in "0123456789abcdef" for character in value[7:])
    ):
        raise ValueError(f"semantic-refresh {field} must be a canonical digest")
    return value


def _uuid(value: object, field: str) -> str:
    try:
        canonical = str(uuid.UUID(str(value)))
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError(f"semantic-refresh {field} must be a canonical UUID") from exc
    if canonical != value:
        raise ValueError(f"semantic-refresh {field} must be a canonical UUID")
    return canonical


class SemanticRefreshPublicationResourcePort(Protocol):
    """Reserve/release one operation's authority-derived aggregate allocation set."""

    def reserve_operation(
        self,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> None:
        """Idempotently reserve the exact protected operation allocation closure."""

    def release_operation(
        self,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> None:
        """Idempotently release the exact allocation closure after safe cleanup."""


__all__ = [
    "ClickHouseFailedScratchCleanupReceipt",
    "ClickHouseScratchRelationAbsence",
    "SemanticRefreshPublicationResourceAllocation",
    "SemanticRefreshPublicationResourcePort",
    "semantic_refresh_publication_resource_allocations",
]
