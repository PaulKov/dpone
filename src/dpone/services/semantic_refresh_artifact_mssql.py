"""Protected committed-after-image to sealed-artifact application service."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from typing import TYPE_CHECKING

from dpone.ports.semantic_refresh_artifact_codec import (
    SemanticRefreshArtifactCodec,
    SemanticRefreshParquetField,
)
from dpone.ports.semantic_refresh_artifact_seal import (
    ArtifactSealPlan,
    SealedArtifactReceipt,
    SemanticRefreshSealAuthorizationPort,
)
from dpone.ports.semantic_refresh_artifact_store import (
    SemanticRefreshArtifactAuthority,
    SemanticRefreshArtifactStoreResolver,
    artifact_store_binding,
    operation_artifact_prefix,
)
from dpone.ports.semantic_refresh_seal_policy import (
    semantic_refresh_artifact_authority_sha256,
)
from dpone.services.semantic_refresh_artifact import SealedArtifactService

if TYPE_CHECKING:
    from dpone.contracts.semantic_refresh_seal_authorization import (
        SemanticRefreshSealAuthorizationReceipt,
    )
    from dpone.ports.semantic_refresh_mssql import SemanticRefreshMssqlPrerequisiteAuthorityPort
    from dpone.ports.semantic_refresh_mssql_after_image import (
        MssqlCommittedAfterImageSnapshot,
        SemanticRefreshMssqlAfterImagePort,
    )
    from dpone.ports.semantic_refresh_mssql_authority import (
        MssqlProtectedOperationAuthority,
        SemanticRefreshMssqlProtectedAuthorityPort,
    )


class MssqlCommittedAfterImageSealError(RuntimeError):
    """Raised when exact protected source-to-seal publication is unproved."""


class MssqlCommittedAfterImageArtifactPublisher:
    """Encode a verified committed snapshot and seal it without caller inventory."""

    def __init__(
        self,
        *,
        protected_operation: SemanticRefreshMssqlProtectedAuthorityPort,
        prerequisites: SemanticRefreshMssqlPrerequisiteAuthorityPort,
        seal_authorization: SemanticRefreshSealAuthorizationPort,
        after_image: SemanticRefreshMssqlAfterImagePort,
        artifact_stores: SemanticRefreshArtifactStoreResolver,
        codec: SemanticRefreshArtifactCodec,
    ) -> None:
        self._protected_operation = protected_operation
        self._prerequisites = prerequisites
        self._seal_authorization = seal_authorization
        self._after_image = after_image
        self._artifact_stores = artifact_stores
        self._codec = codec

    def seal(
        self,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> SealedArtifactReceipt:
        """Read, encode, and seal one exact protected committed after-image."""

        operation = self._load_operation(workflow_execution_binding_sha256, operation_id)
        seal = self._load_seal(workflow_execution_binding_sha256, operation_id)
        snapshot = self._read_after_image(operation)
        fields = tuple(
            SemanticRefreshParquetField(column.name, column.source_type.lower(), column.nullable)
            for column in snapshot.columns
        )
        self._assert_authority(operation, seal, snapshot, fields)
        chunks = self._codec.encode(
            fields=fields,
            rows=snapshot.rows,
            maximum_rows_per_chunk=operation.resource_policy.max_after_image_rows,
        )
        total_bytes = sum(len(chunk.content) for chunk in chunks)
        if total_bytes > min(
            operation.resource_policy.max_after_image_bytes,
            operation.artifact_authority.max_artifact_bytes,
        ):
            raise MssqlCommittedAfterImageSealError("encoded artifact exceeds protected byte budget")
        artifact_prefix = operation_artifact_prefix(
            operation.artifact_authority.artifact_prefix,
            operation.operation_id,
        )
        plan = ArtifactSealPlan(
            seal_authorization=seal,
            artifact_prefix=artifact_prefix,
            provider=operation.artifact_authority.provider,
            encryption_policy_sha256=operation.artifact_authority.encryption_policy_sha256,
            retention_policy_sha256=operation.artifact_authority.retention_policy_sha256,
            encryption_scope=operation.artifact_authority.writer_scope,
            retention_until=operation.artifact_authority.retention_until,
            chunks=chunks,
        )
        self._require_current(operation)
        store_binding = artifact_store_binding(operation.artifact_authority)
        store = self._artifact_stores.resolve(replace(store_binding, artifact_prefix=artifact_prefix))
        return SealedArtifactService(
            store=store,
            authority=_ExactInventoryAuthority(plan.authority_inventory()),
        ).seal(plan)

    def _load_operation(
        self,
        binding_sha256: str,
        operation_id: str,
    ) -> MssqlProtectedOperationAuthority:
        try:
            operation = self._protected_operation.load_operation(
                workflow_execution_binding_sha256=binding_sha256,
                operation_id=operation_id,
            )
            self._require_current(operation)
            return operation
        except Exception as exc:
            raise MssqlCommittedAfterImageSealError("protected operation authority is unavailable") from exc

    def _load_seal(
        self,
        binding_sha256: str,
        operation_id: str,
    ) -> SemanticRefreshSealAuthorizationReceipt:
        try:
            return self._seal_authorization.load(
                workflow_execution_binding_sha256=binding_sha256,
                operation_id=operation_id,
            )
        except Exception as exc:
            raise MssqlCommittedAfterImageSealError("seal authorization is unavailable") from exc

    def _read_after_image(
        self,
        operation: MssqlProtectedOperationAuthority,
    ) -> MssqlCommittedAfterImageSnapshot:
        try:
            return self._after_image.read(operation)
        except Exception as exc:
            raise MssqlCommittedAfterImageSealError("committed after-image read failed") from exc

    def _require_current(self, operation: MssqlProtectedOperationAuthority) -> None:
        if operation.prerequisite_authority is None:
            raise MssqlCommittedAfterImageSealError("protected prerequisite authority is absent")
        self._prerequisites.require_current(operation.prerequisite_authority)

    def _assert_authority(
        self,
        operation: MssqlProtectedOperationAuthority,
        seal: SemanticRefreshSealAuthorizationReceipt,
        snapshot: MssqlCommittedAfterImageSnapshot,
        fields: tuple[SemanticRefreshParquetField, ...],
    ) -> None:
        if operation.after_image_relation is None or operation.after_image_sha256 is None:
            raise MssqlCommittedAfterImageSealError("committed after-image authority is absent")
        if (
            snapshot.relation_id != operation.after_image_relation
            or snapshot.image_sha256 != operation.after_image_sha256
            or snapshot.row_count != seal.after_image_row_count
            or snapshot.row_count > operation.resource_policy.max_after_image_rows
            or snapshot.columns != operation.writable_columns
        ):
            raise MssqlCommittedAfterImageSealError("committed after-image differs from protected authority")
        expected = (
            operation.operation_id,
            operation.operation_plan_sha256,
            operation.workflow_execution_id,
            operation.workflow_execution_binding_sha256,
            operation.attempt_binding_sha256,
            operation.fencing_epoch,
            operation.model_unique_id,
            operation.effective_key_template_sha256,
            operation.effective_key_mapping_sha256,
            operation.writable_schema_sha256,
            operation.route_certification_receipt_sha256,
            operation.writer_exclusivity_assurance_receipt_sha256,
            operation.utc_semantics_assurance_receipt_sha256,
            operation.ddl_freeze_assurance_receipt_sha256,
            operation.after_image_relation,
            operation.after_image_sha256,
            operation.journal_version,
            semantic_refresh_artifact_authority_sha256(operation.artifact_authority),
        )
        observed = (
            seal.operation_id,
            seal.operation_plan_sha256,
            seal.workflow_execution_id,
            seal.workflow_execution_binding_sha256,
            seal.attempt_binding_sha256,
            seal.fencing_epoch,
            seal.model_unique_id,
            seal.effective_key_template_sha256,
            seal.effective_key_mapping_sha256,
            seal.ordered_writable_schema_sha256,
            seal.route_certification_receipt_sha256,
            seal.writer_exclusivity_assurance_receipt_sha256,
            seal.utc_semantics_assurance_receipt_sha256,
            seal.ddl_freeze_assurance_receipt_sha256,
            seal.after_image_relation_id,
            seal.after_image_sha256,
            seal.journal_version,
            seal.artifact_authority_sha256,
        )
        if observed != expected:
            raise MssqlCommittedAfterImageSealError("seal authorization differs from protected operation")
        if self._codec.serializer_sha256 != seal.serializer_sha256:
            raise MssqlCommittedAfterImageSealError("protected serializer authority differs")
        if self._codec.schema_mapping_sha256(fields) != seal.parquet_schema_mapping_sha256:
            raise MssqlCommittedAfterImageSealError("protected Parquet mapping authority differs")
        if (
            operation.baseline_receipt_sha256 is not None
            and operation.baseline_receipt_sha256 != seal.baseline_adoption_receipt_sha256
        ):
            raise MssqlCommittedAfterImageSealError("protected baseline authority differs")


class _ExactInventoryAuthority(SemanticRefreshArtifactAuthority):
    def __init__(self, expected: Mapping[str, object]) -> None:
        self._expected = dict(expected)

    def assert_authorized(self, *, inventory: Mapping[str, object]) -> None:
        if dict(inventory) != self._expected:
            raise MssqlCommittedAfterImageSealError("artifact inventory differs from protected composition")


__all__ = [
    "MssqlCommittedAfterImageArtifactPublisher",
    "MssqlCommittedAfterImageSealError",
]
