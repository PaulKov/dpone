"""Protected canonical-authority persistence boundary for MSSQL admission."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from dpone.ports.semantic_refresh_mssql_authority_records import (
    MssqlCanonicalAuthorityRecord,
    MssqlProtectedArtifactAuthority,
    MssqlProtectedResourcePolicy,
    MssqlProtectedWritableColumn,
)

if TYPE_CHECKING:
    from dpone.ports.semantic_refresh_mssql_primitives import MssqlPrerequisiteAuthorityClaim


@dataclass(frozen=True, slots=True)
class MssqlProtectedOperationAuthority:
    """Authenticated operation and publication coordinates for one model."""

    workflow_execution_id: str
    workflow_execution_binding_sha256: str
    canonical_authority_sha256: str
    workflow_plan_sha256: str
    operation_id: str
    operation_plan_sha256: str
    attempt_binding_sha256: str
    fencing_epoch: int
    owner_id: str
    guard_resource_id: str
    guard_status: str
    journal_status: str
    strategy_authority_json: str
    strategy_authority_sha256: str
    model_unique_id: str
    target_resource_id: str
    target_authority_id: str
    mssql_connection_authority_id: str
    mssql_target_authority_id: str
    clickhouse_cluster_authority_id: str
    publication_database: str
    publication_target_table: str
    publication_scope_id: str
    scope_family_id: str
    scope_start: str
    scope_end: str
    scope_revision: int
    mutation_closure_sha256: str
    target_predecessor_generation_id: str
    scope_predecessor_operation_id: str | None
    predecessor_target_generation: int
    predecessor_target_uuid: str
    predecessor_target_operation_id: str
    predecessor_scope_revision: int | None
    predecessor_checkpoint_sha256: str | None
    predecessor_checkpoint_operation_id: str | None
    predecessor_checkpoint_version: int | None
    clickhouse_target_uuid: str
    model_definition_proof_sha256: str
    effective_key_template_sha256: str
    effective_key_mapping_sha256: str
    writable_columns: tuple[MssqlProtectedWritableColumn, ...]
    writable_schema_sha256: str
    resource_policy: MssqlProtectedResourcePolicy
    route_certification_receipt_sha256: str
    writer_exclusivity_assurance_receipt_sha256: str
    ddl_freeze_assurance_receipt_sha256: str
    utc_semantics_assurance_receipt_sha256: str | None
    artifact_authority: MssqlProtectedArtifactAuthority
    before_image_relation: str | None
    before_image_sha256: str | None
    after_image_relation: str | None
    after_image_sha256: str | None
    scope_map_authority_receipt_sha256: str
    prerequisite_authority: MssqlPrerequisiteAuthorityClaim | None = None
    journal_version: int = 1
    workflow_id: str | None = None
    baseline_receipt_sha256: str | None = None


@dataclass(frozen=True, slots=True)
class MssqlProtectedOperationStateRecord:
    """Canonical document plus exact locked admission/journal/guard state."""

    authority: MssqlCanonicalAuthorityRecord
    workflow_execution_id: str
    workflow_plan_sha256: str
    operation_id: str
    operation_plan_sha256: str
    attempt_binding_sha256: str
    fencing_epoch: int
    owner_id: str
    guard_resource_id: str
    guard_status: str
    journal_status: str
    strategy_authority_json: str
    strategy_authority_sha256: str
    target_predecessor_generation_id: str
    scope_predecessor_operation_id: str | None
    predecessor_target_generation: int
    predecessor_target_uuid: str
    predecessor_target_operation_id: str
    predecessor_scope_revision: int | None
    predecessor_checkpoint_sha256: str | None
    predecessor_checkpoint_operation_id: str | None
    predecessor_checkpoint_version: int | None
    before_image_relation: str | None
    before_image_sha256: str | None
    after_image_relation: str | None
    after_image_sha256: str | None
    journal_version: int = 1


class SemanticRefreshMssqlCanonicalAuthorityPort(Protocol):
    """Load canonical admission authority by immutable execution binding."""

    def load(self, workflow_execution_binding_sha256: str) -> MssqlCanonicalAuthorityRecord:
        """Return one durable ACTIVE authority record or fail closed."""


class SemanticRefreshMssqlProtectedAuthorityPort(Protocol):
    """Resolve one authenticated operation from canonical control authority."""

    def load_operation(
        self,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> MssqlProtectedOperationAuthority:
        """Return exact model, target and scope authority or fail closed."""


class SemanticRefreshMssqlProtectedOperationStatePort(Protocol):
    """Lock and return ACTIVE canonical authority with one admitted operation."""

    def load_operation_state(
        self,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> MssqlProtectedOperationStateRecord:
        """Return one transactionally consistent authority/state record."""


class SemanticRefreshMssqlProtectedPlanStatePort(Protocol):
    """Lock and return the complete admitted operation state in one snapshot."""

    def load_plan_states(
        self,
        *,
        workflow_execution_binding_sha256: str,
    ) -> tuple[MssqlProtectedOperationStateRecord, ...]:
        """Return the exact canonical plan operation closure under one transaction."""


__all__ = [
    "MssqlCanonicalAuthorityRecord",
    "MssqlProtectedOperationAuthority",
    "MssqlProtectedOperationStateRecord",
    "MssqlProtectedArtifactAuthority",
    "MssqlProtectedResourcePolicy",
    "MssqlProtectedWritableColumn",
    "SemanticRefreshMssqlCanonicalAuthorityPort",
    "SemanticRefreshMssqlProtectedAuthorityPort",
    "SemanticRefreshMssqlProtectedOperationStatePort",
    "SemanticRefreshMssqlProtectedPlanStatePort",
]
