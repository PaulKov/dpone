"""Protected deployment and model-target authority for semantic refresh."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from dpone.contracts.dbt_semantic_refresh_plan_policy import (
    SemanticRefreshArtifactAuthority,
    SemanticRefreshResourcePolicy,
    SemanticRefreshWritableColumn,
)
from dpone.contracts.dbt_semantic_refresh_recovery_head import (
    semantic_refresh_recovery_target_head_sha256,
)
from dpone.contracts.semantic_refresh_baseline_receipt import SemanticRefreshBaselineAdoptionReceipt
from dpone.contracts.semantic_refresh_core import (
    SemanticRefreshContractError,
    require_digest,
    require_positive_int,
    require_text,
    semantic_refresh_sha256,
)
from dpone.contracts.semantic_refresh_evidence_common import require_uuid

DEPLOYMENT_AUTHORITY_SUBJECT_SCHEMA = "dpone.dbt-semantic-refresh-deployment-authority-subject.v1"


@dataclass(frozen=True, slots=True)
class SemanticRefreshDeploymentModelAuthority:
    """Protected deployment target and baseline identity; no attempt or fence."""

    model_unique_id: str
    target_predecessor_generation_id: str
    owner_generation: int
    target_resource_id: str
    mssql_connection_authority_id: str
    mssql_target_authority_id: str
    mssql_control_database: str
    mssql_control_schema: str
    mssql_image_schema: str
    scope_image_namespace_policy_sha256: str
    clickhouse_cluster_authority_id: str
    clickhouse_target_authority_id: str
    publication_database: str
    publication_target_table: str
    baseline_receipt: SemanticRefreshBaselineAdoptionReceipt
    artifact_authority: SemanticRefreshArtifactAuthority

    def __post_init__(self) -> None:
        for field_name in (
            "model_unique_id",
            "target_resource_id",
            "mssql_connection_authority_id",
            "mssql_target_authority_id",
            "mssql_control_database",
            "mssql_control_schema",
            "mssql_image_schema",
            "clickhouse_cluster_authority_id",
            "clickhouse_target_authority_id",
            "publication_database",
            "publication_target_table",
        ):
            require_text(getattr(self, field_name), field_name)
        require_digest(self.target_predecessor_generation_id, "target_predecessor_generation_id")
        require_digest(self.scope_image_namespace_policy_sha256, "scope_image_namespace_policy_sha256")
        require_positive_int(self.owner_generation, "owner_generation")
        if not isinstance(self.baseline_receipt, SemanticRefreshBaselineAdoptionReceipt):
            raise SemanticRefreshContractError("baseline_receipt must be a canonical typed receipt")
        if not isinstance(self.artifact_authority, SemanticRefreshArtifactAuthority):
            raise SemanticRefreshContractError("artifact_authority must be typed protected authority")
        if self.clickhouse_target_authority_id != _clickhouse_target_authority(
            self.clickhouse_cluster_authority_id,
            self.publication_database,
            self.publication_target_table,
        ):
            raise SemanticRefreshContractError(
                "clickhouse_target_authority_id differs from protected publication target"
            )
        if self.mssql_target_authority_id != _mssql_target_authority(
            self.mssql_connection_authority_id, self.target_resource_id
        ):
            raise SemanticRefreshContractError("mssql_target_authority_id differs from protected SQL Server target")

    def to_dict(self) -> dict[str, object]:
        return {
            "artifact_authority": self.artifact_authority.to_dict(),
            "baseline_receipt": self.baseline_receipt.to_dict(),
            "clickhouse_cluster_authority_id": self.clickhouse_cluster_authority_id,
            "clickhouse_target_authority_id": self.clickhouse_target_authority_id,
            "model_unique_id": self.model_unique_id,
            "mssql_control_database": self.mssql_control_database,
            "mssql_control_schema": self.mssql_control_schema,
            "mssql_image_schema": self.mssql_image_schema,
            "mssql_connection_authority_id": self.mssql_connection_authority_id,
            "mssql_target_authority_id": self.mssql_target_authority_id,
            "owner_generation": self.owner_generation,
            "publication_database": self.publication_database,
            "publication_target_table": self.publication_target_table,
            "scope_image_namespace_policy_sha256": self.scope_image_namespace_policy_sha256,
            "target_predecessor_generation_id": self.target_predecessor_generation_id,
            "target_resource_id": self.target_resource_id,
        }


@dataclass(frozen=True, slots=True)
class SemanticRefreshReleaseDeploymentAuthority:
    """Protected subject verified before any final semantic plan is created."""

    release_id: str
    deployment_id: str
    binding_set_ref: str
    connection_registry_ref: str
    credential_runtime_ref: str
    authority_receipt_sha256: str

    def __post_init__(self) -> None:
        require_digest(self.release_id, "release_id")
        require_digest(self.deployment_id, "deployment_id")
        require_digest(self.authority_receipt_sha256, "authority_receipt_sha256")
        for field_name in ("binding_set_ref", "connection_registry_ref", "credential_runtime_ref"):
            require_text(getattr(self, field_name), field_name)

    def to_dict(self) -> dict[str, object]:
        return {
            "authority_receipt_sha256": self.authority_receipt_sha256,
            "binding_set_ref": self.binding_set_ref,
            "connection_registry_ref": self.connection_registry_ref,
            "credential_runtime_ref": self.credential_runtime_ref,
            "deployment_id": self.deployment_id,
            "release_id": self.release_id,
        }


@dataclass(frozen=True, slots=True)
class SemanticRefreshDeploymentAuthoritySubject:
    """Complete protected subject including exact model targets and baselines."""

    authority: SemanticRefreshReleaseDeploymentAuthority
    models: tuple[SemanticRefreshDeploymentModelAuthority, ...]
    subject_sha256: str
    schema: str = DEPLOYMENT_AUTHORITY_SUBJECT_SCHEMA

    @classmethod
    def build(
        cls,
        authority: SemanticRefreshReleaseDeploymentAuthority,
        models: tuple[SemanticRefreshDeploymentModelAuthority, ...],
    ) -> SemanticRefreshDeploymentAuthoritySubject:
        if not isinstance(authority, SemanticRefreshReleaseDeploymentAuthority):
            raise SemanticRefreshContractError("release/deployment authority must be typed")
        if not models or any(not isinstance(item, SemanticRefreshDeploymentModelAuthority) for item in models):
            raise SemanticRefreshContractError("deployment model authority must be a non-empty typed tuple")
        canonical = tuple(sorted(models, key=lambda item: item.model_unique_id))
        if len({item.model_unique_id for item in canonical}) != len(canonical):
            raise SemanticRefreshContractError("deployment model authority contains duplicate models")
        unsigned = {
            "authority": authority.to_dict(),
            "models": [item.to_dict() for item in canonical],
            "schema": DEPLOYMENT_AUTHORITY_SUBJECT_SCHEMA,
        }
        return cls(authority, canonical, semantic_refresh_sha256(unsigned))

    def to_dict(self) -> dict[str, object]:
        return {
            "authority": self.authority.to_dict(),
            "models": [item.to_dict() for item in self.models],
            "schema": self.schema,
            "subject_sha256": self.subject_sha256,
        }


class SemanticRefreshReleaseDeploymentVerifierPort(Protocol):
    """Verify the complete deployment/model/baseline subject in protected state."""

    def verify(self, subject: SemanticRefreshDeploymentAuthoritySubject) -> bool: ...


@dataclass(frozen=True, order=True, slots=True)
class SemanticRefreshPlanTarget:
    """Model coordinates consumed by MSSQL canonical-authority composition."""

    model_unique_id: str
    target_resource_id: str
    mssql_connection_authority_id: str
    mssql_target_authority_id: str
    mssql_control_database: str
    mssql_control_schema: str
    mssql_image_schema: str
    scope_image_namespace_policy_sha256: str
    clickhouse_cluster_authority_id: str
    clickhouse_target_authority_id: str
    clickhouse_target_uuid: str
    target_predecessor_generation: int
    target_predecessor_generation_id: str
    target_predecessor_operation_id: str
    target_head_terminal_receipt_sha256: str | None
    target_head_authority_receipt_sha256: str
    publication_database: str
    publication_target_table: str
    publication_scope_id: str
    baseline_receipt: SemanticRefreshBaselineAdoptionReceipt
    baseline_receipt_sha256: str
    baseline_status: str
    strategy_template_sha256: str
    model_definition_proof_sha256: str
    effective_key_template_sha256: str
    effective_key_mapping_sha256: str
    writable_columns: tuple[SemanticRefreshWritableColumn, ...]
    writable_schema_sha256: str
    resource_policy: SemanticRefreshResourcePolicy
    resource_policy_sha256: str
    route_certification_receipt_sha256: str
    writer_exclusivity_assurance_receipt_sha256: str
    ddl_freeze_assurance_receipt_sha256: str
    ddl_epoch: int
    artifact_authority: SemanticRefreshArtifactAuthority
    utc_semantics_assurance_receipt_sha256: str | None = None

    def __post_init__(self) -> None:
        for field_name in (
            "model_unique_id",
            "target_resource_id",
            "mssql_connection_authority_id",
            "mssql_target_authority_id",
            "mssql_control_database",
            "mssql_control_schema",
            "mssql_image_schema",
            "clickhouse_cluster_authority_id",
            "publication_database",
            "publication_target_table",
            "publication_scope_id",
        ):
            require_text(getattr(self, field_name), field_name)
        if self.clickhouse_target_authority_id != _clickhouse_target_authority(
            self.clickhouse_cluster_authority_id,
            self.publication_database,
            self.publication_target_table,
        ):
            raise SemanticRefreshContractError(
                "clickhouse_target_authority_id differs from protected publication target"
            )
        if self.mssql_target_authority_id != _mssql_target_authority(
            self.mssql_connection_authority_id, self.target_resource_id
        ):
            raise SemanticRefreshContractError("mssql_target_authority_id differs from protected SQL Server target")
        require_uuid(self.clickhouse_target_uuid, "clickhouse_target_uuid")
        require_positive_int(self.target_predecessor_generation, "target_predecessor_generation")
        require_positive_int(self.ddl_epoch, "ddl_epoch")
        if not isinstance(self.baseline_receipt, SemanticRefreshBaselineAdoptionReceipt):
            raise SemanticRefreshContractError("baseline_receipt must be a canonical typed receipt")
        for field_name in (
            "baseline_receipt_sha256",
            "target_predecessor_generation_id",
            "target_predecessor_operation_id",
            "target_head_authority_receipt_sha256",
            "strategy_template_sha256",
            "scope_image_namespace_policy_sha256",
            "model_definition_proof_sha256",
            "effective_key_template_sha256",
            "effective_key_mapping_sha256",
            "writable_schema_sha256",
            "resource_policy_sha256",
            "route_certification_receipt_sha256",
            "writer_exclusivity_assurance_receipt_sha256",
            "ddl_freeze_assurance_receipt_sha256",
        ):
            require_digest(getattr(self, field_name), field_name)
        if self.baseline_status != "COMPLETE":
            raise SemanticRefreshContractError("baseline_status must equal COMPLETE")
        if (
            self.baseline_receipt_sha256 != self.baseline_receipt.baseline_adoption_receipt_sha256
            or self.baseline_status != self.baseline_receipt.status
        ):
            raise SemanticRefreshContractError("baseline projection differs from its complete protected receipt")
        if self.target_head_terminal_receipt_sha256 is None:
            if (
                self.target_head_authority_receipt_sha256 != self.baseline_receipt.baseline_adoption_receipt_sha256
                or self.target_predecessor_generation != self.baseline_receipt.clickhouse_generation
                or self.clickhouse_target_uuid != self.baseline_receipt.clickhouse_target_uuid
            ):
                raise SemanticRefreshContractError("baseline target head differs from its adoption receipt")
        else:
            require_digest(
                self.target_head_terminal_receipt_sha256,
                "target_head_terminal_receipt_sha256",
            )
            if self.target_head_authority_receipt_sha256 != semantic_refresh_recovery_target_head_sha256(
                model_unique_id=self.model_unique_id,
                clickhouse_target_authority_id=self.clickhouse_target_authority_id,
                target_generation=self.target_predecessor_generation,
                target_generation_id=self.target_predecessor_generation_id,
                target_uuid=self.clickhouse_target_uuid,
                owner_operation_id=self.target_predecessor_operation_id,
                terminal_receipt_sha256=self.target_head_terminal_receipt_sha256,
            ):
                raise SemanticRefreshContractError(
                    "target head authority receipt differs from its protected coordinates"
                )
        if not isinstance(self.resource_policy, SemanticRefreshResourcePolicy):
            raise SemanticRefreshContractError("resource_policy must be a typed bounded policy")
        if not isinstance(self.artifact_authority, SemanticRefreshArtifactAuthority):
            raise SemanticRefreshContractError("artifact_authority must be typed protected authority")
        if self.resource_policy_sha256 != self.resource_policy.resource_policy_sha256:
            raise SemanticRefreshContractError("resource policy document differs from its protected digest")
        if not self.writable_columns or any(
            not isinstance(item, SemanticRefreshWritableColumn) for item in self.writable_columns
        ):
            raise SemanticRefreshContractError("writable_columns must be a non-empty typed tuple")
        if self.utc_semantics_assurance_receipt_sha256 is not None:
            require_digest(
                self.utc_semantics_assurance_receipt_sha256,
                "utc_semantics_assurance_receipt_sha256",
            )

    def to_dict(self) -> dict[str, object]:
        result: dict[str, object] = {
            "artifact_authority": self.artifact_authority.to_dict(),
            "baseline_receipt": self.baseline_receipt.to_dict(),
            "baseline_receipt_sha256": self.baseline_receipt_sha256,
            "baseline_status": self.baseline_status,
            "clickhouse_cluster_authority_id": self.clickhouse_cluster_authority_id,
            "clickhouse_target_uuid": self.clickhouse_target_uuid,
            "target_predecessor_generation": self.target_predecessor_generation,
            "target_predecessor_generation_id": self.target_predecessor_generation_id,
            "target_predecessor_operation_id": self.target_predecessor_operation_id,
            "target_head_terminal_receipt_sha256": self.target_head_terminal_receipt_sha256,
            "target_head_authority_receipt_sha256": self.target_head_authority_receipt_sha256,
            "clickhouse_target_authority_id": self.clickhouse_target_authority_id,
            "ddl_freeze_assurance_receipt_sha256": self.ddl_freeze_assurance_receipt_sha256,
            "ddl_epoch": self.ddl_epoch,
            "effective_key_template_sha256": self.effective_key_template_sha256,
            "effective_key_mapping_sha256": self.effective_key_mapping_sha256,
            "model_definition_proof_sha256": self.model_definition_proof_sha256,
            "model_unique_id": self.model_unique_id,
            "mssql_control_database": self.mssql_control_database,
            "mssql_control_schema": self.mssql_control_schema,
            "mssql_image_schema": self.mssql_image_schema,
            "mssql_connection_authority_id": self.mssql_connection_authority_id,
            "mssql_target_authority_id": self.mssql_target_authority_id,
            "publication_database": self.publication_database,
            "publication_scope_id": self.publication_scope_id,
            "publication_target_table": self.publication_target_table,
            "resource_policy": self.resource_policy.to_dict(),
            "resource_policy_sha256": self.resource_policy_sha256,
            "route_certification_receipt_sha256": self.route_certification_receipt_sha256,
            "scope_image_namespace_policy_sha256": self.scope_image_namespace_policy_sha256,
            "strategy_template_sha256": self.strategy_template_sha256,
            "target_resource_id": self.target_resource_id,
            "writable_columns": [item.to_dict() for item in self.writable_columns],
            "writable_schema_sha256": self.writable_schema_sha256,
            "writer_exclusivity_assurance_receipt_sha256": (self.writer_exclusivity_assurance_receipt_sha256),
        }
        if self.utc_semantics_assurance_receipt_sha256 is not None:
            result["utc_semantics_assurance_receipt_sha256"] = self.utc_semantics_assurance_receipt_sha256
        return result


def _clickhouse_target_authority(cluster_authority: str, database: str, table: str) -> str:
    return f"clickhouse://{cluster_authority}/{database}/{table}"


def _mssql_target_authority(connection_authority: str, target_resource_id: str) -> str:
    database, separator, relation = target_resource_id.partition(".")
    if not separator or not database or not relation:
        raise SemanticRefreshContractError("target_resource_id must contain database and relation")
    return f"mssql://{connection_authority}/{database}/{relation}"


__all__ = [
    "DEPLOYMENT_AUTHORITY_SUBJECT_SCHEMA",
    "SemanticRefreshDeploymentAuthoritySubject",
    "SemanticRefreshDeploymentModelAuthority",
    "SemanticRefreshPlanTarget",
    "SemanticRefreshReleaseDeploymentAuthority",
    "SemanticRefreshReleaseDeploymentVerifierPort",
]
