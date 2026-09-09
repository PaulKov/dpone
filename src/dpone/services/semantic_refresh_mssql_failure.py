"""Evidence-derived failed-workflow terminalization policy."""

from __future__ import annotations

import json
from dataclasses import dataclass

from dpone.contracts.semantic_refresh_failure_summary import (
    FailedModelOutcome,
    SemanticRefreshFailedWorkflowSummary,
)
from dpone.contracts.semantic_refresh_types import SqlServerModelOutcome
from dpone.ports.semantic_refresh_mssql_evidence import (
    MssqlBuildReceiptEvidence,
    MssqlImageEvidence,
    MssqlOperationEvidence,
    MssqlTransactionDisposition,
    SemanticRefreshMssqlEvidencePort,
    mssql_operation_evidence_sha256,
)
from dpone.ports.semantic_refresh_mssql_failure import (
    MssqlDerivedFailureOutcome,
    MssqlFailureDecision,
    SemanticRefreshMssqlFailureContextPort,
    SemanticRefreshMssqlFailureStatePort,
)


@dataclass(frozen=True, slots=True)
class SemanticRefreshMssqlOutcomeService:
    """Reduce authoritative engine/controller evidence without optimistic inference."""

    def reconcile(self, evidence: MssqlOperationEvidence) -> SqlServerModelOutcome:
        """Return the only safe outcome supported by the complete evidence snapshot."""

        if not evidence.database_available:
            return SqlServerModelOutcome.COMMIT_UNKNOWN
        artifacts_present = any(
            item is not None for item in (evidence.receipt, evidence.before_image, evidence.after_image)
        )
        if artifacts_present:
            if self._proves_committed(evidence):
                return SqlServerModelOutcome.COMMITTED_WITH_IMAGES
            return SqlServerModelOutcome.COMMIT_UNKNOWN
        if (
            evidence.controller_proves_not_invoked
            and evidence.transaction_disposition is MssqlTransactionDisposition.NOT_OPENED
        ):
            return SqlServerModelOutcome.NOT_INVOKED
        if (
            not evidence.controller_proves_not_invoked
            and evidence.transaction_disposition is MssqlTransactionDisposition.ROLLED_BACK
        ):
            return SqlServerModelOutcome.ROLLED_BACK
        return SqlServerModelOutcome.COMMIT_UNKNOWN

    @staticmethod
    def _proves_committed(evidence: MssqlOperationEvidence) -> bool:
        receipt = evidence.receipt
        before = evidence.before_image
        after = evidence.after_image
        if receipt is None or before is None or after is None or receipt.build_receipt_sha256 is None:
            return False
        if evidence.controller_proves_not_invoked or evidence.transaction_disposition in {
            MssqlTransactionDisposition.NOT_OPENED,
            MssqlTransactionDisposition.ROLLED_BACK,
        }:
            return False
        expected_identity = (
            evidence.operation_id,
            evidence.operation_plan_sha256,
            evidence.attempt_binding_sha256,
            evidence.fencing_epoch,
        )
        if _receipt_identity(receipt) != expected_identity:
            return False
        if _image_identity(before) != expected_identity or _image_identity(after) != expected_identity:
            return False
        return (
            before.image_role == "BEFORE"
            and after.image_role == "AFTER"
            and before.committed
            and after.committed
            and receipt.before_image_sha256 == before.image_sha256
            and receipt.after_image_sha256 == after.image_sha256
        )


def _receipt_identity(receipt: MssqlBuildReceiptEvidence) -> tuple[str, str, str, int]:
    return (
        receipt.operation_id,
        receipt.operation_plan_sha256,
        receipt.attempt_binding_sha256,
        receipt.fencing_epoch,
    )


def _image_identity(image: MssqlImageEvidence) -> tuple[str, str, str, int]:
    return (
        image.operation_id,
        image.operation_plan_sha256,
        image.attempt_binding_sha256,
        image.fencing_epoch,
    )


class SemanticRefreshMssqlFailureBlocked(RuntimeError):
    """Raised when durable evidence cannot prove a terminal MSSQL outcome."""


@dataclass(frozen=True, slots=True)
class SemanticRefreshMssqlFailureTerminalizationService:
    """Derive every model outcome from durable evidence before state mutation."""

    contexts: SemanticRefreshMssqlFailureContextPort
    evidence: SemanticRefreshMssqlEvidencePort
    state: SemanticRefreshMssqlFailureStatePort
    outcomes: SemanticRefreshMssqlOutcomeService = SemanticRefreshMssqlOutcomeService()

    def terminalize(self, workflow_id: str) -> SemanticRefreshFailedWorkflowSummary:
        """Reconcile, build and atomically persist one failed-precommit summary."""

        context = self.contexts.load_failure_context(workflow_id)
        derived: list[MssqlDerivedFailureOutcome] = []
        contract_models: list[FailedModelOutcome] = []
        for journal in context.journals:
            snapshot = self.evidence.read_operation_evidence(
                operation_id=journal.operation_id,
                attempt_binding_sha256=journal.attempt_binding_sha256,
            )
            if (
                snapshot.operation_plan_sha256 != journal.operation_plan_sha256
                or snapshot.fencing_epoch != journal.fencing_epoch
            ):
                raise SemanticRefreshMssqlFailureBlocked("durable evidence identity differs from journal")
            outcome = self.outcomes.reconcile(snapshot)
            if outcome is SqlServerModelOutcome.COMMIT_UNKNOWN:
                raise SemanticRefreshMssqlFailureBlocked("COMMIT_UNKNOWN blocks failed-workflow terminalization")
            evidence_sha256 = mssql_operation_evidence_sha256(snapshot)
            derived.append(
                MssqlDerivedFailureOutcome(
                    operation_id=journal.operation_id,
                    attempt_binding_sha256=journal.attempt_binding_sha256,
                    mssql_outcome=outcome.value,
                    mssql_evidence_sha256=evidence_sha256,
                )
            )
            contract_models.append(
                FailedModelOutcome(
                    operation_id=journal.operation_id,
                    attempt_binding_sha256=journal.attempt_binding_sha256,
                    mssql_outcome=outcome,
                    mssql_evidence_sha256=evidence_sha256,
                )
            )
        summary = SemanticRefreshFailedWorkflowSummary.build(
            workflow_id=context.workflow_id,
            workflow_plan_sha256=context.workflow_plan_sha256,
            workflow_execution_binding_sha256=context.workflow_execution_binding_sha256,
            expected_operation_ids=tuple(item.operation_id for item in context.journals),
            models=tuple(contract_models),
        )
        if context.status == "FAILED_PRE_COMMIT" and context.terminal_summary_sha256 != summary.terminal_summary_sha256:
            raise SemanticRefreshMssqlFailureBlocked("durable failure summary replay differs")
        summary_json = json.dumps(
            summary.to_dict(),
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        if context.status == "FAILED_PRE_COMMIT" and context.terminal_summary_json != summary_json:
            raise SemanticRefreshMssqlFailureBlocked("durable failure summary document replay differs")
        self.state.persist_failure(
            MssqlFailureDecision(
                workflow_id=context.workflow_id,
                workflow_plan_sha256=context.workflow_plan_sha256,
                workflow_execution_binding_sha256=context.workflow_execution_binding_sha256,
                terminal_summary_sha256=summary.terminal_summary_sha256,
                terminal_summary_json=summary_json,
                models=tuple(derived),
            )
        )
        return summary


__all__ = [
    "SemanticRefreshMssqlFailureBlocked",
    "SemanticRefreshMssqlFailureTerminalizationService",
    "SemanticRefreshMssqlOutcomeService",
]
