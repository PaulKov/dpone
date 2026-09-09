"""Typed canonical MSSQL authority models and protected dbt projection."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from uuid import UUID

from dpone.contracts.dbt_semantic_refresh_plan_contracts import SemanticRefreshPlanTarget
from dpone.contracts.dbt_semantic_refresh_recovery_head import SemanticRefreshRecoveryTargetHead
from dpone.contracts.semantic_refresh_baseline_receipt import (
    SemanticRefreshBaselineAdoptionReceipt,
    semantic_refresh_target_predecessor_generation_id,
)
from dpone.contracts.semantic_refresh_core import semantic_refresh_sha256
from dpone.ports.semantic_refresh_mssql_authority_records import (
    MssqlProtectedArtifactAuthority,
    MssqlProtectedResourcePolicy,
    MssqlProtectedWritableColumn,
)

if TYPE_CHECKING:
    from dpone.contracts.semantic_refresh_attempt_binding import (
        SemanticRefreshAttemptBinding,
    )
    from dpone.contracts.semantic_refresh_execution_binding import (
        SemanticRefreshWorkflowExecutionBinding,
    )
    from dpone.contracts.semantic_refresh_operation_plan import (
        SemanticRefreshOperationPlan,
    )
    from dpone.contracts.semantic_refresh_workflow_plan import (
        SemanticRefreshWorkflowPlan,
    )
    from dpone.contracts.semantic_refresh_workflow_replacement import (
        SemanticRefreshWorkflowReplacementPlan,
    )
    from dpone.ports.semantic_refresh_mssql_primitives import (
        MssqlGuardClaim,
        MssqlWorkflowResourceBudget,
    )

_SHA256_PREFIX = "sha256:"


@dataclass(frozen=True, order=True, slots=True)
class MssqlModelResourceAuthority:
    """Protected physical target, schema, budget, and artifact authority."""

    model_unique_id: str
    target_resource_id: str
    target_authority_id: str
    mssql_connection_authority_id: str
    mssql_target_authority_id: str
    clickhouse_cluster_authority_id: str
    publication_database: str
    publication_target_table: str
    publication_scope_id: str
    mssql_control_database: str
    mssql_control_schema: str
    mssql_image_schema: str
    scope_image_namespace_policy_sha256: str
    strategy_template_sha256: str
    baseline_receipt_sha256: str
    baseline_kind: str
    baseline_receipt_json: str
    baseline_status: str
    baseline_clickhouse_generation: int
    baseline_clickhouse_target_uuid: str
    baseline_target_predecessor_generation_id: str
    clickhouse_target_uuid: str
    target_predecessor_generation: int
    target_predecessor_generation_id: str
    target_predecessor_operation_id: str
    target_head_terminal_receipt_sha256: str | None
    target_head_authority_receipt_sha256: str
    model_definition_proof_sha256: str
    effective_key_template_sha256: str
    effective_key_mapping_sha256: str
    writable_columns: tuple[MssqlProtectedWritableColumn, ...]
    writable_schema_sha256: str
    resource_policy: MssqlProtectedResourcePolicy
    route_certification_receipt_sha256: str
    writer_exclusivity_assurance_receipt_sha256: str
    ddl_freeze_assurance_receipt_sha256: str
    ddl_epoch: int
    artifact_authority: MssqlProtectedArtifactAuthority
    utc_semantics_assurance_receipt_sha256: str | None = None

    def __post_init__(self) -> None:
        for field_name in (
            "model_unique_id",
            "target_resource_id",
            "target_authority_id",
            "mssql_connection_authority_id",
            "mssql_target_authority_id",
            "clickhouse_cluster_authority_id",
            "publication_database",
            "publication_target_table",
            "publication_scope_id",
            "mssql_control_database",
            "mssql_control_schema",
            "mssql_image_schema",
        ):
            _require_text(getattr(self, field_name), field_name)
        expected_clickhouse = (
            f"clickhouse://{self.clickhouse_cluster_authority_id}/"
            f"{self.publication_database}/{self.publication_target_table}"
        )
        if self.target_authority_id != expected_clickhouse:
            raise ValueError("target_authority_id differs from protected ClickHouse target")
        for field_name in (
            "scope_image_namespace_policy_sha256",
            "strategy_template_sha256",
            "baseline_receipt_sha256",
            "baseline_target_predecessor_generation_id",
            "target_predecessor_generation_id",
            "target_predecessor_operation_id",
            "target_head_authority_receipt_sha256",
            "model_definition_proof_sha256",
            "effective_key_template_sha256",
            "effective_key_mapping_sha256",
            "writable_schema_sha256",
            "route_certification_receipt_sha256",
            "writer_exclusivity_assurance_receipt_sha256",
            "ddl_freeze_assurance_receipt_sha256",
        ):
            _require_digest(getattr(self, field_name), field_name)
        if self.utc_semantics_assurance_receipt_sha256 is not None:
            _require_digest(
                self.utc_semantics_assurance_receipt_sha256,
                "utc_semantics_assurance_receipt_sha256",
            )
        _require_text(self.baseline_kind, "baseline_kind")
        _require_text(self.baseline_receipt_json, "baseline_receipt_json")
        if self.baseline_status != "COMPLETE":
            raise ValueError("baseline_status must equal COMPLETE")
        if (
            isinstance(self.baseline_clickhouse_generation, bool)
            or not isinstance(self.baseline_clickhouse_generation, int)
            or self.baseline_clickhouse_generation <= 0
        ):
            raise ValueError("baseline_clickhouse_generation must be a positive integer")
        if (
            isinstance(self.target_predecessor_generation, bool)
            or not isinstance(self.target_predecessor_generation, int)
            or self.target_predecessor_generation <= 0
        ):
            raise ValueError("target_predecessor_generation must be a positive integer")
        if isinstance(self.ddl_epoch, bool) or not isinstance(self.ddl_epoch, int) or self.ddl_epoch <= 0:
            raise ValueError("ddl_epoch must be a positive integer")
        for field_name in ("baseline_clickhouse_target_uuid", "clickhouse_target_uuid"):
            value = getattr(self, field_name)
            try:
                if str(UUID(value)) != value:
                    raise ValueError
            except (TypeError, ValueError, AttributeError) as exc:
                raise ValueError(f"{field_name} must be a canonical UUID") from exc
        if self.target_head_terminal_receipt_sha256 is not None:
            _require_digest(
                self.target_head_terminal_receipt_sha256,
                "target_head_terminal_receipt_sha256",
            )
        baseline = _baseline_receipt(self.baseline_receipt_json)
        if (
            baseline.baseline_adoption_receipt_sha256 != self.baseline_receipt_sha256
            or baseline.baseline_kind.value != self.baseline_kind
            or baseline.status != self.baseline_status
            or baseline.model_unique_id != self.model_unique_id
            or baseline.mssql_relation_id != self.target_resource_id
            or baseline.mssql_connection_authority_id != self.mssql_connection_authority_id
            or baseline.mssql_target_authority_id != self.mssql_target_authority_id
            or baseline.clickhouse_cluster_authority_id != self.clickhouse_cluster_authority_id
            or baseline.clickhouse_target_authority_id != self.target_authority_id
            or baseline.clickhouse_generation != self.baseline_clickhouse_generation
            or baseline.clickhouse_target_uuid != self.baseline_clickhouse_target_uuid
            or semantic_refresh_target_predecessor_generation_id(baseline)
            != self.baseline_target_predecessor_generation_id
        ):
            raise ValueError("baseline receipt projection differs from protected receipt")
        if self.target_head_terminal_receipt_sha256 is None:
            if (
                self.target_predecessor_generation != self.baseline_clickhouse_generation
                or self.target_predecessor_generation_id != self.baseline_target_predecessor_generation_id
                or self.clickhouse_target_uuid != self.baseline_clickhouse_target_uuid
                or self.target_predecessor_operation_id != self.baseline_receipt_sha256
                or self.target_head_authority_receipt_sha256 != self.baseline_receipt_sha256
            ):
                raise ValueError("baseline target head differs from immutable baseline authority")
        else:
            head = SemanticRefreshRecoveryTargetHead.build(
                model_unique_id=self.model_unique_id,
                clickhouse_target_authority_id=self.target_authority_id,
                target_generation=self.target_predecessor_generation,
                target_generation_id=self.target_predecessor_generation_id,
                target_uuid=self.clickhouse_target_uuid,
                owner_operation_id=self.target_predecessor_operation_id,
                terminal_receipt_sha256=self.target_head_terminal_receipt_sha256,
            )
            if head.head_authority_receipt_sha256 != self.target_head_authority_receipt_sha256:
                raise ValueError("current target head authority receipt differs")
        if not isinstance(self.resource_policy, MssqlProtectedResourcePolicy):
            raise TypeError("resource_policy must be protected typed limits")
        if not isinstance(self.artifact_authority, MssqlProtectedArtifactAuthority):
            raise TypeError("artifact_authority must be protected typed authority")
        if not self.writable_columns or any(
            not isinstance(item, MssqlProtectedWritableColumn) for item in self.writable_columns
        ):
            raise ValueError("writable_columns must be a non-empty protected tuple")
        if len({item.name for item in self.writable_columns}) != len(self.writable_columns):
            raise ValueError("writable_columns contain duplicate names")
        policy_values = {
            name: getattr(self.resource_policy, name)
            for name in self.resource_policy.__dataclass_fields__
            if name != "resource_policy_sha256"
        }
        expected_policy = semantic_refresh_sha256(
            {"schema": "dpone.dbt-semantic-refresh-resource-policy.v1", **policy_values}
        )
        if self.resource_policy.resource_policy_sha256 != expected_policy:
            raise ValueError("resource policy digest differs from protected limits")


@dataclass(frozen=True, slots=True)
class MssqlCanonicalAdmissionBundle:
    """Immutable plan closure loaded from a protected platform authority."""

    workflow_execution_id: str
    workflow_plan: SemanticRefreshWorkflowPlan
    execution_binding: SemanticRefreshWorkflowExecutionBinding
    operation_plans: tuple[SemanticRefreshOperationPlan, ...]
    attempt_bindings: tuple[SemanticRefreshAttemptBinding, ...]
    workflow_guard: MssqlGuardClaim
    resource_guards: tuple[MssqlGuardClaim, ...]
    model_resources: tuple[MssqlModelResourceAuthority, ...]
    controller_id: str
    owner_id: str
    reservation_id: str
    resource_budget: MssqlWorkflowResourceBudget
    replacement_plan: SemanticRefreshWorkflowReplacementPlan | None = None
    authority_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        for field_name in (
            "workflow_execution_id",
            "controller_id",
            "owner_id",
            "reservation_id",
        ):
            _require_text(getattr(self, field_name), field_name)
        from dpone.ports.semantic_refresh_mssql_authority_codec import authority_sha256
        from dpone.ports.semantic_refresh_mssql_authority_validation import (
            validate_canonical_bundle,
        )

        validate_canonical_bundle(self)
        object.__setattr__(self, "authority_sha256", authority_sha256(self))


def mssql_model_resource_authority_from_plan_target(
    target: SemanticRefreshPlanTarget,
) -> MssqlModelResourceAuthority:
    """Project a frozen dbt target without accepting caller-authored coordinates."""

    if not isinstance(target, SemanticRefreshPlanTarget):
        raise TypeError("target must be a canonical semantic-refresh plan target")
    policy_values = {
        name: getattr(target.resource_policy, name) for name in MssqlProtectedResourcePolicy.__dataclass_fields__
    }
    artifact = target.artifact_authority
    return MssqlModelResourceAuthority(
        model_unique_id=target.model_unique_id,
        target_resource_id=target.target_resource_id,
        target_authority_id=target.clickhouse_target_authority_id,
        mssql_connection_authority_id=target.mssql_connection_authority_id,
        mssql_target_authority_id=target.mssql_target_authority_id,
        clickhouse_cluster_authority_id=target.clickhouse_cluster_authority_id,
        publication_database=target.publication_database,
        publication_target_table=target.publication_target_table,
        publication_scope_id=target.publication_scope_id,
        mssql_control_database=target.mssql_control_database,
        mssql_control_schema=target.mssql_control_schema,
        mssql_image_schema=target.mssql_image_schema,
        scope_image_namespace_policy_sha256=target.scope_image_namespace_policy_sha256,
        strategy_template_sha256=target.strategy_template_sha256,
        baseline_receipt_sha256=target.baseline_receipt_sha256,
        baseline_kind=target.baseline_receipt.baseline_kind.value,
        baseline_receipt_json=json.dumps(
            target.baseline_receipt.to_dict(),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ),
        baseline_status=target.baseline_status,
        baseline_clickhouse_generation=target.baseline_receipt.clickhouse_generation,
        baseline_clickhouse_target_uuid=target.baseline_receipt.clickhouse_target_uuid,
        baseline_target_predecessor_generation_id=(
            semantic_refresh_target_predecessor_generation_id(target.baseline_receipt)
        ),
        clickhouse_target_uuid=target.clickhouse_target_uuid,
        target_predecessor_generation=target.target_predecessor_generation,
        target_predecessor_generation_id=target.target_predecessor_generation_id,
        target_predecessor_operation_id=target.target_predecessor_operation_id,
        target_head_terminal_receipt_sha256=(target.target_head_terminal_receipt_sha256),
        target_head_authority_receipt_sha256=(target.target_head_authority_receipt_sha256),
        model_definition_proof_sha256=target.model_definition_proof_sha256,
        effective_key_template_sha256=target.effective_key_template_sha256,
        effective_key_mapping_sha256=target.effective_key_mapping_sha256,
        writable_columns=tuple(
            MssqlProtectedWritableColumn(
                name=item.name,
                source_type=item.source_type,
                target_type=item.target_type,
                nullable=item.nullable,
                writable_role=item.writable_role.value,
            )
            for item in target.writable_columns
        ),
        writable_schema_sha256=target.writable_schema_sha256,
        resource_policy=MssqlProtectedResourcePolicy(**policy_values),
        route_certification_receipt_sha256=target.route_certification_receipt_sha256,
        writer_exclusivity_assurance_receipt_sha256=(target.writer_exclusivity_assurance_receipt_sha256),
        ddl_freeze_assurance_receipt_sha256=(target.ddl_freeze_assurance_receipt_sha256),
        ddl_epoch=target.ddl_epoch,
        artifact_authority=MssqlProtectedArtifactAuthority(
            provider=artifact.provider,
            provider_profile=artifact.provider_profile,
            endpoint_authority_id=artifact.endpoint_authority_id,
            bucket_or_container_authority_id=artifact.bucket_or_container_authority_id,
            kms_key_authority_id=artifact.kms_key_authority_id,
            capability_evidence_sha256=artifact.capability_evidence_sha256,
            writer_scope=artifact.writer_scope,
            artifact_prefix=artifact.artifact_prefix,
            encryption_policy_sha256=artifact.encryption_policy_sha256,
            retention_policy_id=artifact.retention_policy_id,
            retention_policy_sha256=artifact.retention_policy_sha256,
            retention_days=artifact.retention_days,
            retention_issued_at=artifact.retention_issued_at,
            retention_until=artifact.retention_until,
            max_artifact_bytes=artifact.max_artifact_bytes,
        ),
        utc_semantics_assurance_receipt_sha256=(target.utc_semantics_assurance_receipt_sha256),
    )


def _baseline_receipt(value: str) -> SemanticRefreshBaselineAdoptionReceipt:
    try:
        return SemanticRefreshBaselineAdoptionReceipt.from_mapping(json.loads(value))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("baseline receipt JSON is invalid") from exc


def _require_text(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")


def _require_digest(value: str, field_name: str) -> None:
    if (
        not isinstance(value, str)
        or not value.startswith(_SHA256_PREFIX)
        or len(value) != len(_SHA256_PREFIX) + 64
        or any(character not in "0123456789abcdef" for character in value[7:])
    ):
        raise ValueError(f"{field_name} must be a lowercase sha256 digest")


__all__ = [
    "MssqlCanonicalAdmissionBundle",
    "MssqlModelResourceAuthority",
    "mssql_model_resource_authority_from_plan_target",
]
