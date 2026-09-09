"""Protected MSSQL current-head evidence for complete-scope replay."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from dpone.contracts.dbt_semantic_refresh_recovery_head import (
    semantic_refresh_recovery_target_head_sha256,
)


@dataclass(frozen=True, order=True, slots=True)
class MssqlAdmissionTargetHeadAuthority:
    """Protected current-head projection rechecked during atomic admission."""

    target_resource_id: str
    model_unique_id: str
    clickhouse_target_authority_id: str
    target_generation: int
    target_generation_id: str
    target_uuid: str
    owner_operation_id: str
    terminal_receipt_sha256: str | None
    head_authority_receipt_sha256: str

    def __post_init__(self) -> None:
        for field_name in (
            "target_resource_id",
            "model_unique_id",
            "clickhouse_target_authority_id",
        ):
            _text(getattr(self, field_name), field_name)
        if isinstance(self.target_generation, bool) or not isinstance(self.target_generation, int):
            raise ValueError("target_generation must be an integer")
        if self.target_generation <= 0:
            raise ValueError("target_generation must be positive")
        for field_name in (
            "target_generation_id",
            "owner_operation_id",
            "head_authority_receipt_sha256",
        ):
            _digest(getattr(self, field_name), field_name)
        _uuid(self.target_uuid)
        if self.terminal_receipt_sha256 is not None:
            _digest(self.terminal_receipt_sha256, "terminal_receipt_sha256")
            expected = semantic_refresh_recovery_target_head_sha256(
                model_unique_id=self.model_unique_id,
                clickhouse_target_authority_id=self.clickhouse_target_authority_id,
                target_generation=self.target_generation,
                target_generation_id=self.target_generation_id,
                target_uuid=self.target_uuid,
                owner_operation_id=self.owner_operation_id,
                terminal_receipt_sha256=self.terminal_receipt_sha256,
            )
            if self.head_authority_receipt_sha256 != expected:
                raise ValueError("target head authority receipt differs from its exact projection")


@dataclass(frozen=True, order=True, slots=True)
class MssqlRecoveryHeadLocator:
    """Canonical predecessor operation and physical ClickHouse target lookup."""

    model_unique_id: str
    clickhouse_target_authority_id: str
    database_name: str
    target_table: str
    predecessor_operation_id: str

    def __post_init__(self) -> None:
        for field_name in (
            "model_unique_id",
            "clickhouse_target_authority_id",
            "database_name",
            "target_table",
        ):
            _text(getattr(self, field_name), field_name)
        _digest(self.predecessor_operation_id, "predecessor_operation_id")


@dataclass(frozen=True, slots=True)
class MssqlRecoveryHeadReadRequest:
    """Exact canonical authority and model closure locked by one read."""

    workflow_execution_id: str
    workflow_execution_binding_sha256: str
    workflow_plan_sha256: str
    canonical_authority_sha256: str
    locators: tuple[MssqlRecoveryHeadLocator, ...]

    def __post_init__(self) -> None:
        _text(self.workflow_execution_id, "workflow_execution_id")
        for field_name in (
            "workflow_execution_binding_sha256",
            "workflow_plan_sha256",
            "canonical_authority_sha256",
        ):
            _digest(getattr(self, field_name), field_name)
        if (
            not self.locators
            or any(not isinstance(item, MssqlRecoveryHeadLocator) for item in self.locators)
            or self.locators != tuple(sorted(self.locators))
            or len({item.model_unique_id for item in self.locators}) != len(self.locators)
        ):
            raise ValueError("recovery head locators must be a canonical typed closure")


@dataclass(frozen=True, order=True, slots=True)
class MssqlDurableRecoveryTargetHead:
    """Locked complete journal and current target-head evidence."""

    model_unique_id: str
    clickhouse_target_authority_id: str
    target_generation: int
    target_generation_id: str
    target_uuid: str
    owner_operation_id: str
    terminal_receipt_sha256: str

    def __post_init__(self) -> None:
        _text(self.model_unique_id, "model_unique_id")
        _text(self.clickhouse_target_authority_id, "clickhouse_target_authority_id")
        if isinstance(self.target_generation, bool) or not isinstance(self.target_generation, int):
            raise ValueError("target_generation must be an integer")
        if self.target_generation <= 0:
            raise ValueError("target_generation must be positive")
        for field_name in (
            "target_generation_id",
            "owner_operation_id",
            "terminal_receipt_sha256",
        ):
            _digest(getattr(self, field_name), field_name)
        _uuid(self.target_uuid)


@dataclass(frozen=True, order=True, slots=True)
class MssqlDurableRecoveryPublication:
    """Locked predecessor journal projection matching a durable summary model."""

    model_unique_id: str
    operation_id: str
    operation_plan_sha256: str
    workflow_execution_binding_sha256: str
    attempt_binding_sha256: str
    artifact_manifest_sha256: str
    clickhouse_terminal_receipt_sha256: str
    terminal_receipt_sha256: str
    target_generation: int
    scope_revision: int

    def __post_init__(self) -> None:
        _text(self.model_unique_id, "model_unique_id")
        for field_name in (
            "operation_id",
            "operation_plan_sha256",
            "workflow_execution_binding_sha256",
            "attempt_binding_sha256",
            "artifact_manifest_sha256",
            "clickhouse_terminal_receipt_sha256",
            "terminal_receipt_sha256",
        ):
            _digest(getattr(self, field_name), field_name)
        for field_name in ("target_generation", "scope_revision"):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{field_name} must be positive")


@dataclass(frozen=True, slots=True)
class MssqlDurableRecoveryAuthority:
    """One transactionally consistent completed summary and target-head closure."""

    workflow_execution_id: str
    workflow_execution_binding_sha256: str
    workflow_plan_sha256: str
    plan_bundle_sha256: str
    canonical_authority_sha256: str
    terminal_summary_sha256: str
    terminal_summary_json: str
    predecessor_publications: tuple[MssqlDurableRecoveryPublication, ...]
    target_heads: tuple[MssqlDurableRecoveryTargetHead, ...]

    def __post_init__(self) -> None:
        _text(self.workflow_execution_id, "workflow_execution_id")
        _text(self.terminal_summary_json, "terminal_summary_json")
        for field_name in (
            "workflow_execution_binding_sha256",
            "workflow_plan_sha256",
            "plan_bundle_sha256",
            "canonical_authority_sha256",
            "terminal_summary_sha256",
        ):
            _digest(getattr(self, field_name), field_name)
        if (
            not self.predecessor_publications
            or any(not isinstance(item, MssqlDurableRecoveryPublication) for item in self.predecessor_publications)
            or self.predecessor_publications != tuple(sorted(self.predecessor_publications))
            or len({item.model_unique_id for item in self.predecessor_publications})
            != len(self.predecessor_publications)
        ):
            raise ValueError("recovery predecessor publications must be a canonical typed closure")
        if (
            not self.target_heads
            or any(not isinstance(item, MssqlDurableRecoveryTargetHead) for item in self.target_heads)
            or self.target_heads != tuple(sorted(self.target_heads))
            or len({item.model_unique_id for item in self.target_heads}) != len(self.target_heads)
        ):
            raise ValueError("recovery target heads must be a canonical typed closure")


class SemanticRefreshMssqlRecoveryHeadStatePort(Protocol):
    """Read a complete summary and current heads in one transaction."""

    def load_recovery_authority(
        self,
        request: MssqlRecoveryHeadReadRequest,
    ) -> MssqlDurableRecoveryAuthority:
        """Return exact completed summary/head authority or fail closed."""


def _text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be non-empty text")
    return value


def _digest(value: object, field_name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 71
        or not value.startswith("sha256:")
        or any(character not in "0123456789abcdef" for character in value[7:])
    ):
        raise ValueError(f"{field_name} must be a lowercase sha256 digest")
    return value


def _uuid(value: object) -> str:
    try:
        if not isinstance(value, str) or str(UUID(value)) != value:
            raise ValueError
    except (TypeError, ValueError, AttributeError) as exc:
        raise ValueError("target_uuid must be canonical") from exc
    return value


__all__ = [
    "MssqlAdmissionTargetHeadAuthority",
    "MssqlDurableRecoveryAuthority",
    "MssqlDurableRecoveryPublication",
    "MssqlDurableRecoveryTargetHead",
    "MssqlRecoveryHeadLocator",
    "MssqlRecoveryHeadReadRequest",
    "SemanticRefreshMssqlRecoveryHeadStatePort",
]
