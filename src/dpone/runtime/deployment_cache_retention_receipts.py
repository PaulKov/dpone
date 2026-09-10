"""Runtime boundary for durable deployment-cache retention receipts."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dpone.runtime.deployment_cache_common import DeploymentCacheError
from dpone.runtime.deployment_cache_retention_contracts import (
    DeploymentCacheRetentionApplyError,
    DeploymentRetentionApplyItem,
    DeploymentRetentionApplyReport,
    RetentionTransactionOccurrence,
    is_canonical_retention_digest,
)
from dpone.runtime.deployment_cache_retention_state_codec import retention_operation_id

if TYPE_CHECKING:
    from dpone.ports.deployment_cache_retention_receipts import DeploymentCacheRetentionReceiptStorePort


class DeploymentCacheRetentionReceipts:
    """Map persistence outcomes to stable runtime reports and errors."""

    def __init__(self, store: DeploymentCacheRetentionReceiptStorePort) -> None:
        self._store = store

    def operation_id(
        self,
        *,
        environment: str,
        reviewed_plan_sha256: str | None,
        review_id: str | None = None,
    ) -> str | None:
        if not is_canonical_retention_digest(reviewed_plan_sha256) or review_id is None:
            return None
        try:
            return retention_operation_id(
                environment=environment,
                reviewed_plan_sha256=str(reviewed_plan_sha256),
                review_id=review_id,
            )
        except ValueError as exc:
            raise DeploymentCacheRetentionApplyError(
                "DPONE_DEPLOYMENT_CACHE_GC_REVIEW_ID_INVALID",
                "deployment cache GC review id must be a canonical UUIDv4",
                details={"state_may_have_changed": False},
            ) from exc

    def is_canonical_digest(self, value: object) -> bool:
        return is_canonical_retention_digest(value)

    def load(self, operation_id: str) -> dict[str, Any] | None:
        try:
            return self._store.load(operation_id)
        except DeploymentCacheError as exc:
            raise _apply_error(exc) from exc

    def incomplete_operation_ids(self) -> tuple[str, ...]:
        try:
            return self._store.incomplete_operation_ids()
        except DeploymentCacheError as exc:
            raise _apply_error(exc) from exc

    def begin(
        self,
        *,
        operation_id: str,
        environment: str,
        promoted_by: str,
        current_deployment_id: str | None,
        reviewed_plan_sha256: str,
        review_id: str,
        reviewed_protected_deployment_ids: tuple[str, ...],
        reviewed_recovery_revision: str | None,
        activation_history_revision: str,
        items: list[DeploymentRetentionApplyItem],
    ) -> dict[str, Any]:
        try:
            return self._store.begin(
                operation_id=operation_id,
                environment=environment,
                promoted_by=promoted_by,
                current_deployment_id=current_deployment_id,
                reviewed_plan_sha256=reviewed_plan_sha256,
                review_id=review_id,
                reviewed_protected_deployment_ids=reviewed_protected_deployment_ids,
                reviewed_recovery_revision=reviewed_recovery_revision,
                activation_history_revision=activation_history_revision,
                items=[item.to_dict() for item in items],
            )
        except DeploymentCacheError as exc:
            raise _apply_error(exc) from exc

    def mark_deleted(self, receipt: dict[str, Any], deployment_id: str) -> dict[str, Any]:
        try:
            return self._store.mark_deleted(receipt, deployment_id)
        except DeploymentCacheError as exc:
            raise _apply_error(exc) from exc

    @staticmethod
    def is_legacy(receipt: dict[str, Any]) -> bool:
        return "review_id" not in receipt

    def abort_for_legacy_receipt(
        self,
        receipt: dict[str, Any],
        *,
        committed_deployment_ids: tuple[str, ...],
    ) -> dict[str, Any]:
        try:
            return self._store.abort(
                receipt,
                committed_deployment_ids=committed_deployment_ids,
                restored_deployment_ids=(),
                reason="legacy_receipt_requires_fresh_review",
                error_code="DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_REQUIRED",
            )
        except DeploymentCacheError as exc:
            raise _apply_error(exc) from exc

    def abort_after_recovery(
        self,
        *,
        environment: str,
        restored_occurrences: tuple[RetentionTransactionOccurrence, ...],
    ) -> tuple[str, ...]:
        aborted: list[str] = []
        for operation_id in self.incomplete_operation_ids():
            receipt = self.load(operation_id)
            if receipt is None or receipt["environment"] != environment:
                continue
            receipt_ids = {str(item["deployment_id"]) for item in receipt["items"] if item["deployment_id"]}
            restored_deployment_ids = tuple(
                sorted(
                    {
                        item.deployment_id
                        for item in restored_occurrences
                        if item.operation_id == operation_id and item.deployment_id in receipt_ids
                    }
                )
            )
            if not restored_deployment_ids:
                continue
            try:
                self._store.abort(
                    receipt,
                    committed_deployment_ids=(),
                    restored_deployment_ids=restored_deployment_ids,
                    reason="recovery_restored_requires_fresh_review",
                    error_code="DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_REQUIRED",
                )
            except DeploymentCacheError as exc:
                raise _apply_error(exc) from exc
            aborted.append(operation_id)
        return tuple(aborted)

    def abort_for_authority_drift(
        self,
        receipt: dict[str, Any],
        *,
        committed_deployment_ids: tuple[str, ...],
    ) -> dict[str, Any]:
        try:
            return self._store.abort(
                receipt,
                committed_deployment_ids=committed_deployment_ids,
                restored_deployment_ids=(),
                reason="activation_authority_changed_requires_fresh_review",
                error_code="DPONE_DEPLOYMENT_CACHE_GC_PLAN_CHANGED",
            )
        except DeploymentCacheError as exc:
            raise _apply_error(exc) from exc

    def abort_for_protection_drift(
        self,
        receipt: dict[str, Any],
        *,
        committed_deployment_ids: tuple[str, ...],
    ) -> dict[str, Any]:
        try:
            return self._store.abort(
                receipt,
                committed_deployment_ids=committed_deployment_ids,
                restored_deployment_ids=(),
                reason="candidate_became_protected_requires_fresh_review",
                error_code="DPONE_DEPLOYMENT_CACHE_GC_PLAN_CHANGED",
            )
        except DeploymentCacheError as exc:
            raise _apply_error(exc) from exc

    def abort_for_occurrence_drift(
        self,
        receipt: dict[str, Any],
        *,
        committed_deployment_ids: tuple[str, ...],
    ) -> dict[str, Any]:
        try:
            return self._store.abort(
                receipt,
                committed_deployment_ids=committed_deployment_ids,
                restored_deployment_ids=(),
                reason="occurrence_identity_changed_requires_fresh_review",
                error_code="DPONE_DEPLOYMENT_CACHE_GC_PLAN_CHANGED",
            )
        except DeploymentCacheError as exc:
            raise _apply_error(exc) from exc

    def abort_for_unbound_legacy_wal(
        self,
        receipt: dict[str, Any],
    ) -> dict[str, Any]:
        try:
            return self._store.abort(
                receipt,
                committed_deployment_ids=(),
                restored_deployment_ids=(),
                reason="legacy_wal_requires_fresh_review",
                error_code="DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_REQUIRED",
            )
        except DeploymentCacheError as exc:
            raise _apply_error(exc) from exc

    def commit(self, receipt: dict[str, Any]) -> dict[str, Any]:
        try:
            return self._store.commit(receipt)
        except DeploymentCacheError as exc:
            raise _apply_error(exc) from exc

    def report(self, receipt: dict[str, Any]) -> DeploymentRetentionApplyReport:
        try:
            committed = self._store.committed(receipt)
        except DeploymentCacheError as exc:
            raise _apply_error(exc) from exc
        current_deployment_id = committed["current_deployment_id"]
        return DeploymentRetentionApplyReport(
            environment=str(committed["environment"]),
            promoted_by=str(committed["promoted_by"]),
            current_deployment_id=str(current_deployment_id) if current_deployment_id is not None else None,
            reviewed_plan_sha256=str(committed["reviewed_plan_sha256"]),
            activation_history_revision=str(committed["activation_history_revision"]),
            operation_id=str(committed["operation_id"]),
            review_id=(str(committed["review_id"]) if "review_id" in committed else None),
            receipt_revision=str(committed["receipt_revision"]),
            transaction_status=str(committed["status"]),
            items=tuple(DeploymentRetentionApplyItem(**item) for item in committed["items"]),
        )


def _apply_error(exc: DeploymentCacheError) -> DeploymentCacheRetentionApplyError:
    return DeploymentCacheRetentionApplyError(
        exc.code,
        str(exc),
        path=exc.path,
        details=exc.details,
    )


__all__ = ["DeploymentCacheRetentionReceipts"]
