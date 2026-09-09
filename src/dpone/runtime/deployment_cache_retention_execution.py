"""Receipt execution and success-WAL acknowledgement for cache retention."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, NoReturn

from dpone.runtime.deployment_cache_retention_candidate import DeploymentCacheRetentionCandidateValidator
from dpone.runtime.deployment_cache_retention_contracts import (
    DeploymentCacheRetentionApplyError,
    DeploymentRetentionApplyReport,
    DeploymentRetentionPlanItem,
    RetentionTransactionOccurrence,
)
from dpone.runtime.deployment_cache_retention_deletion import DirectoryIdentity
from dpone.runtime.deployment_cache_retention_receipts import DeploymentCacheRetentionReceipts
from dpone.runtime.deployment_cache_retention_transaction import DeploymentCacheRetentionTransactionCoordinator

_MAX_EVIDENCE_ITEMS = 20


class DeploymentCacheRetentionOccurrenceGuard:
    """Require receipt, WAL and current cache root to name one occurrence."""

    def __init__(
        self,
        *,
        receipts: DeploymentCacheRetentionReceipts,
        candidate_validator: DeploymentCacheRetentionCandidateValidator,
    ) -> None:
        self._receipts = receipts
        self._candidate_validator = candidate_validator

    def validate(
        self,
        receipt: dict[str, Any],
        *,
        environment: str,
        occurrences: tuple[RetentionTransactionOccurrence, ...],
        terminal: bool,
    ) -> dict[str, RetentionTransactionOccurrence]:
        destructive = {
            str(item["deployment_id"]): item
            for item in receipt["items"]
            if item["deployment_id"] is not None and item["action"] in {"pending", "deleted"}
        }
        mismatches = self._receipt_path_mismatches(destructive, environment=environment)
        committed: dict[str, RetentionTransactionOccurrence] = {}
        required_absent = {deployment_id for deployment_id, item in destructive.items() if item["action"] == "deleted"}
        observed_deployment_ids = {occurrence.deployment_id for occurrence in occurrences}
        for occurrence in occurrences:
            deployment_id = occurrence.deployment_id
            item = destructive.get(deployment_id)
            canonical = self._canonical_path(deployment_id, environment=environment)
            if (
                item is None
                or deployment_id in committed
                or occurrence.original_path != canonical
                or str(item["path"]) != canonical
            ):
                mismatches.append(
                    {
                        "deployment_id": deployment_id,
                        "wal_path": occurrence.original_path,
                        "receipt_path": str(item["path"]) if item is not None else "",
                        "canonical_path": canonical,
                    }
                )
                continue
            committed[deployment_id] = occurrence
            required_absent.add(deployment_id)
        if receipt["status"] == "applying" and not terminal:
            for deployment_id, item in destructive.items():
                if item["action"] == "deleted" and deployment_id not in observed_deployment_ids:
                    canonical = self._canonical_path(deployment_id, environment=environment)
                    mismatches.append(
                        {
                            "deployment_id": deployment_id,
                            "wal_path": "",
                            "receipt_path": str(item["path"]),
                            "canonical_path": canonical,
                        }
                    )
        replacements = tuple(
            committed.get(deployment_id)
            or RetentionTransactionOccurrence(
                transaction_id="",
                deployment_id=deployment_id,
                operation_id=str(receipt["operation_id"]),
                original_path=self._canonical_path(deployment_id, environment=environment),
            )
            for deployment_id in sorted(required_absent)
            if os.path.lexists(self._canonical_path(deployment_id, environment=environment))
        )
        if not mismatches and not replacements:
            return committed
        aborted = self._receipts.abort_for_occurrence_drift(
            receipt,
            committed_deployment_ids=tuple(sorted(committed)),
        )
        self._raise_drift(
            receipt,
            mismatches=mismatches,
            replacements=replacements,
            aborted=aborted,
        )

    def _receipt_path_mismatches(
        self,
        destructive: dict[str, dict[str, Any]],
        *,
        environment: str,
    ) -> list[dict[str, str]]:
        mismatches: list[dict[str, str]] = []
        for deployment_id, item in destructive.items():
            canonical = self._canonical_path(deployment_id, environment=environment)
            if str(item["path"]) != canonical:
                mismatches.append(
                    {
                        "deployment_id": deployment_id,
                        "wal_path": "",
                        "receipt_path": str(item["path"]),
                        "canonical_path": canonical,
                    }
                )
        return mismatches

    def _canonical_path(self, deployment_id: str, *, environment: str) -> str:
        return self._candidate_validator.canonical_path(
            deployment_id,
            environment=environment,
        ).as_posix()

    @staticmethod
    def _raise_drift(
        receipt: dict[str, Any],
        *,
        mismatches: list[dict[str, str]],
        replacements: tuple[RetentionTransactionOccurrence, ...],
        aborted: dict[str, Any],
    ) -> NoReturn:
        first_path = mismatches[0]["canonical_path"] if mismatches else replacements[0].original_path
        raise DeploymentCacheRetentionApplyError(
            "DPONE_DEPLOYMENT_CACHE_GC_PLAN_CHANGED",
            "retention occurrence identity changed; review a fresh attempt",
            path=first_path,
            details={
                "state_may_have_changed": True,
                "operation_id": receipt["operation_id"],
                "receipt_revision": aborted["receipt_revision"],
                "transaction_status": aborted["status"],
                "occurrence_path_mismatch": bool(mismatches),
                "occurrence_path_mismatch_count": len(mismatches),
                "occurrence_path_mismatches": mismatches[:_MAX_EVIDENCE_ITEMS],
                "replacement_count": len(replacements),
                "replacement_deployment_ids": [item.deployment_id for item in replacements[:_MAX_EVIDENCE_ITEMS]],
                "transaction_ids": [
                    item.transaction_id for item in replacements[:_MAX_EVIDENCE_ITEMS] if item.transaction_id
                ],
                "evidence_truncated": (
                    len(mismatches) > _MAX_EVIDENCE_ITEMS or len(replacements) > _MAX_EVIDENCE_ITEMS
                ),
            },
        )


class DeploymentCacheRetentionReceiptExecutor:
    """Replay pending outcomes and close their receipt/WAL ordering boundary."""

    def __init__(
        self,
        *,
        transactions: DeploymentCacheRetentionTransactionCoordinator,
        receipts: DeploymentCacheRetentionReceipts,
        candidate_validator: DeploymentCacheRetentionCandidateValidator,
    ) -> None:
        self._transactions = transactions
        self._receipts = receipts
        self._candidate_validator = candidate_validator
        self._occurrence_guard = DeploymentCacheRetentionOccurrenceGuard(
            receipts=receipts,
            candidate_validator=candidate_validator,
        )

    def execute(
        self,
        receipt: dict[str, Any],
        *,
        environment: str,
        prevalidated: dict[str, tuple[DeploymentRetentionPlanItem, Path, DirectoryIdentity]] | None = None,
    ) -> DeploymentRetentionApplyReport:
        operation_id = str(receipt["operation_id"])
        committed_occurrences = self._transactions.committed_occurrences(
            environment=environment,
            operation_id=operation_id,
        )
        committed = self._occurrence_guard.validate(
            receipt,
            environment=environment,
            occurrences=committed_occurrences,
            terminal=False,
        )
        queue: list[tuple[DeploymentRetentionPlanItem, Path, DirectoryIdentity]] = []
        for raw_item in receipt["items"]:
            item_payload = dict(raw_item)
            if item_payload["action"] != "pending":
                continue
            deployment_id = str(item_payload["deployment_id"])
            if deployment_id in committed:
                receipt = self._receipts.mark_deleted(receipt, deployment_id)
                continue
            plan_item = DeploymentRetentionPlanItem(**item_payload)
            candidate = (prevalidated or {}).get(deployment_id)
            if candidate is None:
                path, identity = self._candidate_validator.validate(plan_item, environment=environment)
                candidate = (plan_item, path, identity)
            queue.append(candidate)
        for item, path, identity in queue:
            receipt = self._delete(receipt, item=item, path=path, identity=identity, environment=environment)
        committed_receipt = self._receipts.commit(receipt)
        self.acknowledge_committed_wal(committed_receipt)
        return self._receipts.report(committed_receipt)

    def abort_legacy_receipt(
        self,
        receipt: dict[str, Any],
        *,
        environment: str,
    ) -> dict[str, Any]:
        """Reconcile only proven WAL outcomes, then close legacy authority."""

        occurrences = self._transactions.committed_occurrences(
            environment=environment,
            operation_id=str(receipt["operation_id"]),
        )
        committed = self._occurrence_guard.validate(
            receipt,
            environment=environment,
            occurrences=occurrences,
            terminal=False,
        )
        for deployment_id in sorted(committed):
            receipt = self._receipts.mark_deleted(receipt, deployment_id)
        return self._receipts.abort_for_legacy_receipt(
            receipt,
            committed_deployment_ids=tuple(sorted(committed)),
        )

    def validate_replay_receipt(self, receipt: dict[str, Any], *, environment: str) -> None:
        """Validate reviewed paths and terminal WAL before any replay decision."""

        self._occurrence_guard.validate(
            receipt,
            environment=environment,
            occurrences=self._transactions.committed_occurrences(
                environment=environment,
                operation_id=str(receipt["operation_id"]),
            ),
            terminal=False,
        )

    def validate_committed_receipt(self, receipt: dict[str, Any], *, environment: str) -> None:
        """Prevent a closed historical receipt from proving another cache occurrence."""

        self._occurrence_guard.validate(
            receipt,
            environment=environment,
            occurrences=self._transactions.committed_occurrences(
                environment=environment,
                operation_id=str(receipt["operation_id"]),
            ),
            terminal=True,
        )

    def acknowledge_committed_wal(self, receipt: dict[str, Any]) -> None:
        deleted_ids = tuple(
            str(item["deployment_id"])
            for item in receipt["items"]
            if item["action"] == "deleted" and item["deployment_id"] is not None
        )
        self._transactions.acknowledge_committed_receipt(
            operation_id=str(receipt["operation_id"]),
            deployment_ids=deleted_ids,
        )

    def _delete(
        self,
        receipt: dict[str, Any],
        *,
        item: DeploymentRetentionPlanItem,
        path: Path,
        identity: DirectoryIdentity,
        environment: str,
    ) -> dict[str, Any]:
        assert item.deployment_id is not None
        try:
            self._transactions.delete(
                path,
                expected_identity=identity,
                deployment_id=item.deployment_id,
                environment=environment,
                operation_id=str(receipt["operation_id"]),
            )
        except DeploymentCacheRetentionApplyError as exc:
            deleted_ids = [str(value["deployment_id"]) for value in receipt["items"] if value["action"] == "deleted"]
            details = dict(exc.details)
            details.update(
                deleted_deployment_ids=deleted_ids,
                deleted_count=len(deleted_ids),
                failed_deployment_id=item.deployment_id,
                failed_paths=[item.path],
                retention_incomplete=True,
                activation_history_revision=receipt["activation_history_revision"],
                operation_id=receipt["operation_id"],
            )
            raise DeploymentCacheRetentionApplyError(
                exc.code,
                str(exc),
                path=exc.path or item.path,
                details=details,
            ) from exc
        return self._receipts.mark_deleted(receipt, item.deployment_id)


__all__ = ["DeploymentCacheRetentionOccurrenceGuard", "DeploymentCacheRetentionReceiptExecutor"]
