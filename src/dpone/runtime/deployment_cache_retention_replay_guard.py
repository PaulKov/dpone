"""Fail-closed reconciliation guards for deployment-cache receipt replay."""

from __future__ import annotations

from typing import Any

from dpone.runtime.deployment_cache_retention_candidate import (
    require_pending_candidates_remain_deletable,
)
from dpone.runtime.deployment_cache_retention_contracts import DeploymentCacheRetentionApplyError
from dpone.runtime.deployment_cache_retention_planner import DeploymentCacheRetentionPlanner
from dpone.runtime.deployment_cache_retention_receipts import DeploymentCacheRetentionReceipts
from dpone.runtime.deployment_cache_retention_transaction import (
    DeploymentCacheRetentionTransactionCoordinator,
)


class DeploymentCacheRetentionReplayGuard:
    """Bind replay decisions to one receipt operation and close drift safely."""

    def __init__(
        self,
        *,
        transactions: DeploymentCacheRetentionTransactionCoordinator,
        receipts: DeploymentCacheRetentionReceipts,
        planner: DeploymentCacheRetentionPlanner,
    ) -> None:
        self._transactions = transactions
        self._receipts = receipts
        self._planner = planner

    def abort_unbound_legacy_wal(self, receipt: dict[str, Any], *, environment: str) -> None:
        receipt_ids = _receipt_deployment_ids(receipt)
        unbound = tuple(
            item
            for item in self._transactions.unbound_committed_deployment_ids(environment=environment)
            if item in receipt_ids
        )
        if not unbound:
            return
        aborted = self._receipts.abort_for_unbound_legacy_wal(receipt)
        raise DeploymentCacheRetentionApplyError(
            "DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_REQUIRED",
            "legacy retention WAL was not bound to one operation; review a fresh plan",
            details={
                "state_may_have_changed": True,
                "operation_id": receipt["operation_id"],
                "receipt_revision": aborted["receipt_revision"],
                "transaction_status": "aborted",
                "unbound_deployment_ids": list(unbound),
            },
        )

    def require_candidates_deletable(
        self,
        receipt: dict[str, Any],
        *,
        environment: str,
        protected_deployment_ids: tuple[str, ...],
    ) -> None:
        committed = self.committed_ids(receipt, environment=environment)
        try:
            require_pending_candidates_remain_deletable(
                receipt,
                fresh_plan=self._planner.plan(
                    environment=environment,
                    protected_deployment_ids=protected_deployment_ids,
                    exclude_operation_id=str(receipt["operation_id"]),
                ),
                committed_deployment_ids=committed,
            )
        except DeploymentCacheRetentionApplyError as exc:
            if exc.code != "DPONE_DEPLOYMENT_CACHE_GC_PLAN_CHANGED":
                raise
            aborted = self._receipts.abort_for_protection_drift(
                receipt,
                committed_deployment_ids=committed,
            )
            details = dict(exc.details)
            details.update(
                operation_id=receipt["operation_id"],
                receipt_revision=aborted["receipt_revision"],
                transaction_status="aborted",
                committed_deployment_ids=list(committed),
            )
            raise DeploymentCacheRetentionApplyError(
                exc.code,
                str(exc),
                path=exc.path,
                details=details,
            ) from exc

    def abort_authority_drift(
        self,
        receipt: dict[str, Any],
        *,
        environment: str,
        code: str,
        message: str,
    ) -> None:
        committed = self.committed_ids(receipt, environment=environment)
        aborted = self._receipts.abort_for_authority_drift(
            receipt,
            committed_deployment_ids=committed,
        )
        raise DeploymentCacheRetentionApplyError(
            code,
            message,
            details={
                "state_may_have_changed": True,
                "operation_id": receipt["operation_id"],
                "receipt_revision": aborted["receipt_revision"],
                "transaction_status": "aborted",
                "committed_deployment_ids": list(committed),
            },
        )

    def committed_ids(self, receipt: dict[str, Any], *, environment: str) -> tuple[str, ...]:
        receipt_ids = _receipt_deployment_ids(receipt)
        return tuple(
            item
            for item in self._transactions.committed_deployment_ids(
                environment=environment,
                operation_id=str(receipt["operation_id"]),
            )
            if item in receipt_ids
        )


def _receipt_deployment_ids(receipt: dict[str, Any]) -> set[str]:
    return {str(item["deployment_id"]) for item in receipt["items"] if item["deployment_id"] is not None}


__all__ = ["DeploymentCacheRetentionReplayGuard"]
