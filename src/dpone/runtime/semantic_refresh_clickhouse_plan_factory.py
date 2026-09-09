"""Protected factories for semantic-refresh ClickHouse publication plans."""

from __future__ import annotations

from typing import TYPE_CHECKING

from dpone.ports.semantic_refresh_clickhouse_authority import (
    ClickHousePublicationAuthority,
    SemanticRefreshClickHousePublicationAuthorityPort,
    clickhouse_operation_table_names,
)
from dpone.runtime.semantic_refresh_clickhouse_models import (
    ClickHouseHeadPublicationPlan,
    ClickHousePreparedReceipt,
    ClickHousePreparePlan,
    GroupedMultisetConformance,
    ShadowEquation,
    semantic_refresh_fingerprint,
)

if TYPE_CHECKING:
    from dpone.ports.semantic_refresh_artifact_seal import (
        SealedArtifactReceipt,
        SemanticRefreshSealAuthorizationPort,
    )


class ClickHousePlanFactoryError(RuntimeError):
    """Raised when protected authority cannot produce one closed plan."""


class ProtectedClickHousePreparePlanFactory:
    """Build PREPARE only from protected operation and sealed-artifact authority."""

    def __init__(
        self,
        *,
        authority: SemanticRefreshClickHousePublicationAuthorityPort,
        seal_authorization: SemanticRefreshSealAuthorizationPort,
    ) -> None:
        self._authority = authority
        self._seal_authorization = seal_authorization

    def build(
        self,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
        sealed_artifact: SealedArtifactReceipt,
    ) -> ClickHousePreparePlan:
        """Authenticate all identities and derive operation-bound temporary names."""

        try:
            authority = self._authority.load(workflow_execution_binding_sha256, operation_id)
            seal = self._seal_authorization.load(
                workflow_execution_binding_sha256=workflow_execution_binding_sha256,
                operation_id=operation_id,
            )
        except Exception as exc:
            raise ClickHousePlanFactoryError("protected publication authority is unavailable") from exc
        manifest = sealed_artifact.sealed_manifest
        if not manifest.matches_seal_authorization(seal):
            raise ClickHousePlanFactoryError("sealed manifest differs from seal authorization")
        _assert_sealed_receipt(sealed_artifact)
        expected = (
            operation_id,
            authority.operation_id,
            seal.operation_id,
            manifest.operation_id,
        )
        if len(set(expected)) != 1:
            raise ClickHousePlanFactoryError("operation identity differs across protected authorities")
        if not (
            workflow_execution_binding_sha256
            == authority.workflow_execution_binding_sha256
            == seal.workflow_execution_binding_sha256
        ):
            raise ClickHousePlanFactoryError("workflow execution binding differs")
        authority_projection = (
            authority.operation_plan_sha256,
            authority.workflow_plan_sha256,
            authority.workflow_execution_id,
            authority.attempt_binding_sha256,
            authority.fencing_epoch,
            authority.effective_key_mapping_sha256,
            authority.route_certification_receipt_sha256,
            authority.artifact_prefix,
            authority.artifact_provider,
            authority.encryption_policy_sha256,
            authority.retention_policy_sha256,
        )
        seal_projection = (
            seal.operation_plan_sha256,
            seal.workflow_plan_sha256,
            seal.workflow_execution_id,
            seal.attempt_binding_sha256,
            seal.fencing_epoch,
            seal.effective_key_mapping_sha256,
            seal.route_certification_receipt_sha256,
            manifest.artifact_prefix,
            manifest.provider,
            manifest.encryption_policy_sha256,
            manifest.retention_policy_sha256,
        )
        if authority_projection != seal_projection:
            raise ClickHousePlanFactoryError("sealed artifact differs from protected target authority")
        if manifest.total_bytes > authority.max_artifact_bytes:
            raise ClickHousePlanFactoryError("sealed artifact exceeds protected byte budget")
        staging_table, shadow_table = clickhouse_operation_table_names(authority.target_table, operation_id)
        return ClickHousePreparePlan(
            operation_id=operation_id,
            operation_plan_sha256=authority.operation_plan_sha256,
            workflow_plan_sha256=authority.workflow_plan_sha256,
            workflow_execution_id=authority.workflow_execution_id,
            workflow_execution_binding_sha256=workflow_execution_binding_sha256,
            attempt_binding_sha256=authority.attempt_binding_sha256,
            fence_epoch=authority.fencing_epoch,
            artifact_manifest_key=sealed_artifact.manifest.key,
            artifact_manifest_version=sealed_artifact.manifest.version,
            artifact_manifest_sha256=manifest.artifact_manifest_sha256,
            target_resource_id=authority.target_resource_id,
            target_authority_id=authority.target_authority_id,
            clickhouse_cluster_authority_id=authority.clickhouse_cluster_authority_id,
            database=authority.database,
            target_table=authority.target_table,
            scope_id=authority.scope_id,
            scope_start=authority.scope_start,
            scope_end=authority.scope_end,
            scope_revision=authority.scope_revision,
            event_time_column=authority.event_time_column,
            staging_table=staging_table,
            shadow_table=shadow_table,
            expected_target_uuid=authority.expected_target_uuid,
            expected_schema_sha256=authority.expected_schema_sha256,
            expected_physical_sha256=authority.expected_physical_sha256,
            business_columns=authority.business_columns,
            effective_key_columns=authority.effective_key_columns,
            max_staging_rows=authority.max_staging_rows,
            max_target_scope_rows=authority.max_target_scope_rows,
            max_staging_bytes=authority.max_staging_bytes,
            max_shadow_bytes=authority.max_shadow_bytes,
            max_retained_backup_bytes=authority.max_retained_backup_bytes,
            max_total_transient_bytes=authority.max_total_transient_bytes,
            shadow_equation=ShadowEquation(
                retained_target_rule="TARGET_ANTI_JOIN_STAGED_EFFECTIVE_KEY",
                append_rule="APPEND_ALL_STAGING_ROWS",
            ),
            conformance=GroupedMultisetConformance(mode="BIDIRECTIONAL_GROUPED_MULTISET"),
            artifact_chunk_count=manifest.chunk_count,
            artifact_total_rows=manifest.total_rows,
        )


class ClickHouseHeadPublicationPlanFactory:
    """Derive successor target, scope, and checkpoint identities without caller claims."""

    @staticmethod
    def build(
        authority: ClickHousePublicationAuthority,
        prepared: ClickHousePreparedReceipt,
    ) -> ClickHouseHeadPublicationPlan:
        """Return the sole successor plan authorized by PREPARED shadow UUID evidence."""

        if (
            prepared.status != "PREPARED"
            or prepared.operation_id != authority.operation_id
            or prepared.attempt_binding_sha256 != authority.attempt_binding_sha256
            or prepared.target_uuid != authority.predecessor_target_uuid
        ):
            raise ClickHousePlanFactoryError("PREPARED receipt differs from protected authority")
        expected_scope_revision = authority.predecessor_scope_revision or 0
        if authority.scope_revision != expected_scope_revision + 1:
            raise ClickHousePlanFactoryError("protected scope revision is not the exact successor")
        if prepared.publication_mode == "EMPTY_SCOPE":
            return _empty_scope_heads(authority, prepared)
        if prepared.publication_mode != "EXCHANGE" or prepared.shadow_uuid is None:
            raise ClickHousePlanFactoryError("PREPARED receipt publication mode is invalid")
        target_generation = authority.predecessor_target_generation + 1
        target_generation_id = semantic_refresh_fingerprint(
            {
                "schema": "dpone.semantic-refresh-target-generation.v1",
                "target_authority_id": authority.target_authority_id,
                "operation_id": authority.operation_id,
                "operation_plan_sha256": authority.operation_plan_sha256,
                "target_generation": target_generation,
                "target_uuid": prepared.shadow_uuid,
            }
        )
        checkpoint_sha256 = semantic_refresh_fingerprint(
            {
                "schema": "dpone.semantic-refresh-checkpoint.v1",
                "operation_id": authority.operation_id,
                "scope_id": authority.scope_id,
                "scope_revision": authority.scope_revision,
                "target_generation_id": target_generation_id,
                "target_uuid": prepared.shadow_uuid,
                "scope_end": authority.scope_end,
            }
        )
        return ClickHouseHeadPublicationPlan(
            scope_id=authority.scope_id,
            expected_target_generation=authority.predecessor_target_generation,
            target_generation=target_generation,
            target_generation_id=target_generation_id,
            expected_scope_revision=expected_scope_revision,
            scope_revision=authority.scope_revision,
            expected_checkpoint_sha256=authority.predecessor_checkpoint_sha256,
            checkpoint_sha256=checkpoint_sha256,
        )


def _empty_scope_heads(
    authority: ClickHousePublicationAuthority,
    prepared: ClickHousePreparedReceipt,
) -> ClickHouseHeadPublicationPlan:
    if prepared.staging_uuid is not None or prepared.shadow_uuid is not None:
        raise ClickHousePlanFactoryError("empty-scope PREPARED receipt contains temporary UUIDs")
    checkpoint_sha256 = semantic_refresh_fingerprint(
        {
            "schema": "dpone.semantic-refresh-checkpoint.v1",
            "operation_id": authority.operation_id,
            "scope_id": authority.scope_id,
            "scope_revision": authority.scope_revision,
            "target_generation_id": authority.target_predecessor_generation_id,
            "target_uuid": authority.predecessor_target_uuid,
            "scope_end": authority.scope_end,
        }
    )
    return ClickHouseHeadPublicationPlan(
        scope_id=authority.scope_id,
        expected_target_generation=authority.predecessor_target_generation,
        target_generation=authority.predecessor_target_generation,
        target_generation_id=authority.target_predecessor_generation_id,
        expected_scope_revision=authority.predecessor_scope_revision or 0,
        scope_revision=authority.scope_revision,
        expected_checkpoint_sha256=authority.predecessor_checkpoint_sha256,
        checkpoint_sha256=checkpoint_sha256,
        target_mutation_outcome="NOT_REQUIRED_EMPTY_SCOPE",
        value_conversion_outcome="NOT_APPLICABLE_NO_DATA",
    )


def _assert_sealed_receipt(receipt: SealedArtifactReceipt) -> None:
    manifest = receipt.sealed_manifest
    if (
        receipt.status != "SEALED"
        or receipt.operation_id != manifest.operation_id
        or receipt.manifest.key != f"{manifest.artifact_prefix}/manifest.json"
        or receipt.manifest.version.strip() == ""
        or receipt.manifest.sha256 != semantic_refresh_fingerprint(manifest.to_dict())
        or receipt.manifest_payload != manifest.to_dict()
        or len(receipt.chunks) != len(manifest.chunks)
    ):
        raise ClickHousePlanFactoryError("sealed artifact receipt is invalid")
    for object_ref, chunk in zip(receipt.chunks, manifest.chunks, strict=True):
        if (
            object_ref.key,
            object_ref.version,
            object_ref.sha256,
            object_ref.size_bytes,
        ) != (
            chunk.object_key,
            chunk.provider_version,
            chunk.chunk_sha256,
            chunk.byte_count,
        ):
            raise ClickHousePlanFactoryError("sealed artifact chunk receipt differs")


__all__ = [
    "ClickHouseHeadPublicationPlanFactory",
    "ClickHousePlanFactoryError",
    "ProtectedClickHousePreparePlanFactory",
]
