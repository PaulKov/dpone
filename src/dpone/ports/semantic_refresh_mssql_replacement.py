"""Durable predecessor outcome boundary for MSSQL replacement planning."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from dpone.ports.semantic_refresh_clickhouse_authority import (
    clickhouse_operation_table_names,
)


@dataclass(frozen=True, order=True, slots=True)
class MssqlPersistedModelOutcome:
    """One terminal model outcome stored in a predecessor journal."""

    model_unique_id: str
    operation_id: str
    attempt_binding_sha256: str
    mssql_outcome: str
    mssql_evidence_sha256: str
    operation_plan_sha256: str | None = None
    fencing_epoch: int | None = None
    strategy_authority_json: str | None = None
    strategy_authority_sha256: str | None = None
    before_image_relation: str | None = None
    before_image_sha256: str | None = None
    after_image_relation: str | None = None
    after_image_sha256: str | None = None


@dataclass(frozen=True, slots=True)
class MssqlPredecessorFailureState:
    """Exact failed-workflow summary and journal outcome closure."""

    workflow_id: str
    workflow_plan_sha256: str
    workflow_execution_binding_sha256: str
    terminal_summary_sha256: str
    status: str
    models: tuple[MssqlPersistedModelOutcome, ...]


@dataclass(frozen=True, slots=True)
class MssqlFailedRecoveryReadRequest:
    """Canonical failed-run identity rechecked by one protected MSSQL read."""

    workflow_execution_id: str
    workflow_plan_sha256: str
    workflow_execution_binding_sha256: str
    canonical_authority_sha256: str

    def __post_init__(self) -> None:
        _text(self.workflow_execution_id, "workflow_execution_id")
        for field_name in (
            "workflow_plan_sha256",
            "workflow_execution_binding_sha256",
            "canonical_authority_sha256",
        ):
            _digest(getattr(self, field_name), field_name)


@dataclass(frozen=True, slots=True)
class MssqlDurableFailedRecoveryAuthority:
    """One transactionally consistent failed summary and model evidence closure."""

    workflow_execution_id: str
    workflow_plan_sha256: str
    workflow_execution_binding_sha256: str
    plan_bundle_sha256: str
    canonical_authority_sha256: str
    terminal_summary_sha256: str
    terminal_summary_json: str
    models: tuple[MssqlPersistedModelOutcome, ...]

    def __post_init__(self) -> None:
        _text(self.workflow_execution_id, "workflow_execution_id")
        _text(self.terminal_summary_json, "terminal_summary_json")
        for field_name in (
            "workflow_plan_sha256",
            "workflow_execution_binding_sha256",
            "plan_bundle_sha256",
            "canonical_authority_sha256",
            "terminal_summary_sha256",
        ):
            _digest(getattr(self, field_name), field_name)
        if (
            not self.models
            or any(not isinstance(item, MssqlPersistedModelOutcome) for item in self.models)
            or self.models != tuple(sorted(self.models))
            or len({item.model_unique_id for item in self.models}) != len(self.models)
        ):
            raise ValueError("failed recovery models must be a canonical typed closure")
        for item in self.models:
            _text(item.model_unique_id, "model_unique_id")
            for field_name in (
                "operation_id",
                "operation_plan_sha256",
                "attempt_binding_sha256",
                "mssql_evidence_sha256",
            ):
                _digest(getattr(item, field_name), field_name)
            if item.mssql_outcome not in {"NOT_INVOKED", "ROLLED_BACK", "COMMITTED_WITH_IMAGES"}:
                raise ValueError("failed recovery model outcome is not safely terminal")
            if isinstance(item.fencing_epoch, bool) or not isinstance(item.fencing_epoch, int):
                raise ValueError("failed recovery fencing_epoch must be an integer")
            if item.fencing_epoch <= 0:
                raise ValueError("failed recovery fencing_epoch must be positive")


class SemanticRefreshMssqlPredecessorStatePort(Protocol):
    """Read persisted predecessor summary and journal outcomes."""

    def load_predecessor(self, workflow_id: str) -> MssqlPredecessorFailureState:
        """Return one FAILED_PRE_COMMIT workflow under durable authority."""


class SemanticRefreshMssqlFailedRecoveryStatePort(Protocol):
    """Read an authenticated failed predecessor in one serializable snapshot."""

    def load_failed_recovery_authority(
        self,
        request: MssqlFailedRecoveryReadRequest,
    ) -> MssqlDurableFailedRecoveryAuthority:
        """Return exact failed summary/model authority or fail closed."""


@dataclass(frozen=True, slots=True)
class MssqlFailedScratchCleanupReadRequest:
    """Exact failed-run and unchanged target coordinates for scratch inspection."""

    workflow_execution_id: str
    workflow_execution_binding_sha256: str
    canonical_authority_sha256: str
    model_unique_id: str
    operation_id: str
    operation_plan_sha256: str
    attempt_binding_sha256: str
    fencing_epoch: int
    target_authority_id: str
    database_name: str
    target_table: str
    target_generation: int
    target_generation_id: str
    target_uuid: str
    target_owner_operation_id: str
    staging_table: str
    shadow_table: str

    @classmethod
    def build(
        cls,
        *,
        workflow_execution_id: str,
        workflow_execution_binding_sha256: str,
        canonical_authority_sha256: str,
        model_unique_id: str,
        operation_id: str,
        operation_plan_sha256: str,
        attempt_binding_sha256: str,
        fencing_epoch: int,
        target_authority_id: str,
        database_name: str,
        target_table: str,
        target_generation: int,
        target_generation_id: str,
        target_uuid: str,
        target_owner_operation_id: str,
    ) -> MssqlFailedScratchCleanupReadRequest:
        """Derive the sole scratch relation names from protected identity."""

        staging_table, shadow_table = clickhouse_operation_table_names(target_table, operation_id)
        return cls(
            workflow_execution_id=workflow_execution_id,
            workflow_execution_binding_sha256=workflow_execution_binding_sha256,
            canonical_authority_sha256=canonical_authority_sha256,
            model_unique_id=model_unique_id,
            operation_id=operation_id,
            operation_plan_sha256=operation_plan_sha256,
            attempt_binding_sha256=attempt_binding_sha256,
            fencing_epoch=fencing_epoch,
            target_authority_id=target_authority_id,
            database_name=database_name,
            target_table=target_table,
            target_generation=target_generation,
            target_generation_id=target_generation_id,
            target_uuid=target_uuid,
            target_owner_operation_id=target_owner_operation_id,
            staging_table=staging_table,
            shadow_table=shadow_table,
        )

    def __post_init__(self) -> None:
        for field_name in (
            "workflow_execution_id",
            "model_unique_id",
            "target_authority_id",
            "database_name",
            "target_table",
            "staging_table",
            "shadow_table",
        ):
            _text(getattr(self, field_name), field_name)
        for field_name in (
            "workflow_execution_binding_sha256",
            "canonical_authority_sha256",
            "operation_id",
            "operation_plan_sha256",
            "attempt_binding_sha256",
            "target_generation_id",
            "target_owner_operation_id",
        ):
            _digest(getattr(self, field_name), field_name)
        for field_name in ("fencing_epoch", "target_generation"):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{field_name} must be positive")
        _uuid(self.target_uuid)
        if (self.staging_table, self.shadow_table) != clickhouse_operation_table_names(
            self.target_table,
            self.operation_id,
        ):
            raise ValueError("scratch relation names differ from the deterministic operation identity")


@dataclass(frozen=True, slots=True)
class MssqlFailedScratchCleanupStateRecord:
    """Raw create-once PREPARE documents locked under failed-precommit state."""

    request: MssqlFailedScratchCleanupReadRequest
    mssql_outcome: str
    prepare_plan_sha256: str | None
    prepare_plan_json: str | None
    prepared_receipt_sha256: str | None
    prepared_receipt_json: str | None


@dataclass(frozen=True, order=True, slots=True)
class MssqlScratchCleanupRelation:
    """One non-business relation eligible for UUID-bound inspection and cleanup."""

    relation_role: str
    database_name: str
    table_name: str
    observed_uuid: str | None

    def __post_init__(self) -> None:
        if self.relation_role not in {"SHADOW", "STAGING"}:
            raise ValueError("scratch cleanup relation role is unsupported")
        _text(self.database_name, "database_name")
        _text(self.table_name, "table_name")
        if self.observed_uuid is not None:
            _uuid(self.observed_uuid)


@dataclass(frozen=True, slots=True)
class MssqlFailedScratchCleanupAuthority:
    """Bounded authority that can never name the protected business target."""

    workflow_execution_id: str
    workflow_execution_binding_sha256: str
    operation_id: str
    operation_plan_sha256: str
    attempt_binding_sha256: str
    fencing_epoch: int
    protected_target_authority_id: str
    protected_target_database: str
    protected_target_table: str
    protected_target_uuid: str
    relations: tuple[MssqlScratchCleanupRelation, ...]

    def __post_init__(self) -> None:
        _text(self.workflow_execution_id, "workflow_execution_id")
        _text(self.protected_target_authority_id, "protected_target_authority_id")
        _text(self.protected_target_database, "protected_target_database")
        _text(self.protected_target_table, "protected_target_table")
        for field_name in (
            "workflow_execution_binding_sha256",
            "operation_id",
            "operation_plan_sha256",
            "attempt_binding_sha256",
        ):
            _digest(getattr(self, field_name), field_name)
        if isinstance(self.fencing_epoch, bool) or not isinstance(self.fencing_epoch, int) or self.fencing_epoch <= 0:
            raise ValueError("fencing_epoch must be positive")
        _uuid(self.protected_target_uuid)
        if tuple(item.relation_role for item in self.relations) != ("SHADOW", "STAGING"):
            raise ValueError("scratch cleanup authority must contain exact shadow/staging closure")
        staging_table, shadow_table = clickhouse_operation_table_names(
            self.protected_target_table,
            self.operation_id,
        )
        if tuple((item.database_name, item.table_name) for item in self.relations) != (
            (self.protected_target_database, shadow_table),
            (self.protected_target_database, staging_table),
        ):
            raise ValueError("scratch cleanup relations differ from protected deterministic names")
        if not self.protected_target_authority_id.endswith(
            f"/{self.protected_target_database}/{self.protected_target_table}"
        ):
            raise ValueError("scratch cleanup protected target authority differs from its relation")


class SemanticRefreshMssqlFailedScratchCleanupStatePort(Protocol):
    """Read one failed-precommit cleanup boundary in a serializable snapshot."""

    def load_failed_scratch_cleanup(
        self,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> MssqlFailedScratchCleanupStateRecord:
        """Return exact protected state or fail closed."""


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
    "MssqlDurableFailedRecoveryAuthority",
    "MssqlFailedScratchCleanupAuthority",
    "MssqlFailedScratchCleanupReadRequest",
    "MssqlFailedScratchCleanupStateRecord",
    "MssqlFailedRecoveryReadRequest",
    "MssqlPersistedModelOutcome",
    "MssqlPredecessorFailureState",
    "MssqlScratchCleanupRelation",
    "SemanticRefreshMssqlFailedScratchCleanupStatePort",
    "SemanticRefreshMssqlFailedRecoveryStatePort",
    "SemanticRefreshMssqlPredecessorStatePort",
]
