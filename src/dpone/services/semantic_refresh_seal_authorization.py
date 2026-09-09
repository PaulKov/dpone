"""Evidence-driven issuer for immutable semantic-refresh seal authority."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from dpone.contracts.semantic_refresh_seal_authorization import (
    SemanticRefreshSealAuthorizationReceipt,
)
from dpone.ports.semantic_refresh_seal_policy import (
    SemanticRefreshSealCodecAuthorityPort,
    SemanticRefreshSealPolicyAuthorityPort,
    SemanticRefreshSealPolicySubject,
    semantic_refresh_artifact_authority_sha256,
)

if TYPE_CHECKING:
    from dpone.ports.semantic_refresh_artifact_seal import (
        SemanticRefreshSealAuthorizationStorePort,
    )
    from dpone.ports.semantic_refresh_mssql import (
        SemanticRefreshMssqlEvidencePort,
        SemanticRefreshMssqlPrerequisiteAuthorityPort,
    )
    from dpone.ports.semantic_refresh_mssql_authority import (
        MssqlProtectedOperationAuthority,
        SemanticRefreshMssqlProtectedAuthorityPort,
    )
    from dpone.ports.semantic_refresh_mssql_evidence import (
        MssqlBuildReceiptEvidence,
        MssqlImageEvidence,
    )
    from dpone.services.semantic_refresh_mssql_failure import (
        SemanticRefreshMssqlOutcomeService,
    )

_UTC = timezone.utc  # noqa: UP017 - package type-checks against Python 3.10


class SemanticRefreshSealAuthorizationIssueError(RuntimeError):
    """Raised when committed-image transfer authority cannot be proven."""


class SemanticRefreshSealAuthorizationIssuer:
    """Issue a receipt only from protected operation, engine evidence, and Vault policy."""

    def __init__(
        self,
        *,
        protected_operation: SemanticRefreshMssqlProtectedAuthorityPort,
        prerequisites: SemanticRefreshMssqlPrerequisiteAuthorityPort,
        evidence: SemanticRefreshMssqlEvidencePort,
        outcome: SemanticRefreshMssqlOutcomeService,
        codec: SemanticRefreshSealCodecAuthorityPort,
        policy: SemanticRefreshSealPolicyAuthorityPort,
        store: SemanticRefreshSealAuthorizationStorePort,
        now: Callable[[], datetime],
    ) -> None:
        self._protected_operation = protected_operation
        self._prerequisites = prerequisites
        self._evidence = evidence
        self._outcome = outcome
        self._codec = codec
        self._policy = policy
        self._store = store
        self._now = now

    def issue(
        self,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> SemanticRefreshSealAuthorizationReceipt:
        """Persist and return exact create-once authority for a committed after-image."""

        try:
            operation = self._protected_operation.load_operation(
                workflow_execution_binding_sha256=workflow_execution_binding_sha256,
                operation_id=operation_id,
            )
            self._require_current(operation)
            evidence = self._evidence.read_operation_evidence(
                operation_id=operation.operation_id,
                attempt_binding_sha256=operation.attempt_binding_sha256,
            )
            if self._outcome.reconcile(evidence).value != "COMMITTED_WITH_IMAGES":
                raise SemanticRefreshSealAuthorizationIssueError("MSSQL outcome is not committed with images")
            receipt_evidence, before, after = evidence.receipt, evidence.before_image, evidence.after_image
            if receipt_evidence is None or before is None or after is None:
                raise SemanticRefreshSealAuthorizationIssueError("committed image evidence is incomplete")
            self._assert_operation_evidence(operation, receipt_evidence, before, after)
            subject = self._policy_subject(operation)
            policy = self._policy.load(subject)
            if policy.subject != subject:
                raise SemanticRefreshSealAuthorizationIssueError("protected seal policy subject differs")
            receipt = SemanticRefreshSealAuthorizationReceipt.build(
                operation_id=operation.operation_id,
                operation_plan_sha256=operation.operation_plan_sha256,
                model_unique_id=operation.model_unique_id,
                workflow_id=_required(operation.workflow_id, "workflow_id"),
                workflow_plan_sha256=operation.workflow_plan_sha256,
                workflow_execution_id=operation.workflow_execution_id,
                workflow_execution_binding_sha256=operation.workflow_execution_binding_sha256,
                attempt_binding_sha256=operation.attempt_binding_sha256,
                fencing_epoch=operation.fencing_epoch,
                journal_version=operation.journal_version,
                event_time_source_type=subject.event_time_source_type,
                effective_key_template_sha256=operation.effective_key_template_sha256,
                effective_key_mapping_sha256=operation.effective_key_mapping_sha256,
                ordered_writable_schema_sha256=operation.writable_schema_sha256,
                serializer_sha256=subject.serializer_sha256,
                parquet_schema_mapping_sha256=subject.parquet_schema_mapping_sha256,
                clickhouse_input_mapping_sha256=policy.clickhouse_input_mapping_sha256,
                codec_mapping_certification_sha256=policy.codec_mapping_certification_sha256,
                before_image_relation_id=_required(operation.before_image_relation, "before_image_relation"),
                before_image_sha256=_required(operation.before_image_sha256, "before_image_sha256"),
                before_image_row_count=before.row_count,
                after_image_relation_id=_required(operation.after_image_relation, "after_image_relation"),
                after_image_sha256=_required(operation.after_image_sha256, "after_image_sha256"),
                after_image_row_count=after.row_count,
                model_build_receipt_sha256=_required(
                    receipt_evidence.build_receipt_sha256,
                    "model_build_receipt_sha256",
                ),
                baseline_adoption_receipt_sha256=_required(
                    operation.baseline_receipt_sha256,
                    "baseline_receipt_sha256",
                ),
                route_certification_receipt_sha256=operation.route_certification_receipt_sha256,
                writer_exclusivity_assurance_receipt_sha256=(operation.writer_exclusivity_assurance_receipt_sha256),
                utc_semantics_assurance_receipt_sha256=(operation.utc_semantics_assurance_receipt_sha256),
                ddl_freeze_assurance_receipt_sha256=(operation.ddl_freeze_assurance_receipt_sha256),
                artifact_authority_sha256=subject.artifact_authority_sha256,
                seal_policy_sha256=policy.seal_policy_sha256,
                created_at=_utc_text(self._now()),
                issuer_authority=policy.issuer_authority,
                issuer_attestation_sha256=policy.issuer_attestation_sha256,
                issuer_signature_sha256=policy.issuer_signature_sha256,
            )
            self._require_current(operation)
            self._store.persist_exact(receipt)
            persisted = self._store.load(
                workflow_execution_binding_sha256=workflow_execution_binding_sha256,
                operation_id=operation_id,
            )
            if persisted != receipt:
                raise SemanticRefreshSealAuthorizationIssueError("durable seal receipt differs")
            self._require_current(operation)
            return persisted
        except SemanticRefreshSealAuthorizationIssueError:
            raise
        except Exception as exc:
            raise SemanticRefreshSealAuthorizationIssueError("seal authorization issuance failed") from exc

    def _policy_subject(self, operation: MssqlProtectedOperationAuthority) -> SemanticRefreshSealPolicySubject:
        prerequisite = operation.prerequisite_authority
        if prerequisite is None:
            raise SemanticRefreshSealAuthorizationIssueError("protected prerequisite authority is absent")
        return SemanticRefreshSealPolicySubject(
            release_id=prerequisite.release_id,
            deployment_id=prerequisite.deployment_id,
            model_unique_id=operation.model_unique_id,
            event_time_source_type=_event_time_source_type(operation),
            effective_key_mapping_sha256=operation.effective_key_mapping_sha256,
            ordered_writable_schema_sha256=operation.writable_schema_sha256,
            route_certification_receipt_sha256=operation.route_certification_receipt_sha256,
            writer_exclusivity_assurance_receipt_sha256=(operation.writer_exclusivity_assurance_receipt_sha256),
            ddl_freeze_assurance_receipt_sha256=operation.ddl_freeze_assurance_receipt_sha256,
            artifact_authority_sha256=semantic_refresh_artifact_authority_sha256(operation.artifact_authority),
            serializer_sha256=self._codec.serializer_sha256,
            parquet_schema_mapping_sha256=self._codec.parquet_schema_mapping_sha256(operation.writable_columns),
            utc_semantics_assurance_receipt_sha256=(operation.utc_semantics_assurance_receipt_sha256),
        )

    def _require_current(self, operation: MssqlProtectedOperationAuthority) -> None:
        if operation.guard_status != "HELD" or operation.journal_status != "PREPARING":
            raise SemanticRefreshSealAuthorizationIssueError("operation is outside the sealed PREPARING boundary")
        if operation.prerequisite_authority is None:
            raise SemanticRefreshSealAuthorizationIssueError("protected prerequisite authority is absent")
        self._prerequisites.require_current(operation.prerequisite_authority)

    @staticmethod
    def _assert_operation_evidence(
        operation: MssqlProtectedOperationAuthority,
        receipt: MssqlBuildReceiptEvidence,
        before: MssqlImageEvidence,
        after: MssqlImageEvidence,
    ) -> None:
        expected = (
            operation.operation_id,
            operation.operation_plan_sha256,
            operation.attempt_binding_sha256,
            operation.fencing_epoch,
        )
        if any(
            (
                item.operation_id,
                item.operation_plan_sha256,
                item.attempt_binding_sha256,
                item.fencing_epoch,
            )
            != expected
            for item in (receipt, before, after)
        ):
            raise SemanticRefreshSealAuthorizationIssueError("MSSQL evidence identity differs")
        if (
            receipt.before_image_relation != operation.before_image_relation
            or receipt.before_image_sha256 != operation.before_image_sha256
            or receipt.after_image_relation != operation.after_image_relation
            or receipt.after_image_sha256 != operation.after_image_sha256
            or receipt.model_unique_id != operation.model_unique_id
            or receipt.strategy_authority_sha256 != operation.strategy_authority_sha256
            or before.image_role != "BEFORE"
            or after.image_role != "AFTER"
        ):
            raise SemanticRefreshSealAuthorizationIssueError("MSSQL image evidence differs from protected state")


def _event_time_source_type(operation: MssqlProtectedOperationAuthority) -> str:
    columns = tuple(item for item in operation.writable_columns if item.writable_role == "EFFECTIVE_KEY_EVENT_TIME")
    if len(columns) != 1:
        raise SemanticRefreshSealAuthorizationIssueError("event-time writable-column authority is invalid")
    source_type = columns[0].source_type.lower()
    if source_type not in {"date", "datetime2(6)"}:
        raise SemanticRefreshSealAuthorizationIssueError("event-time source type is unsupported")
    return source_type


def _required(value: str | None, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise SemanticRefreshSealAuthorizationIssueError(f"protected {field_name} is absent")
    return value


def _utc_text(value: datetime) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise SemanticRefreshSealAuthorizationIssueError("issuer clock must be timezone-aware")
    return value.astimezone(_UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


__all__ = [
    "SemanticRefreshSealAuthorizationIssueError",
    "SemanticRefreshSealAuthorizationIssuer",
]
