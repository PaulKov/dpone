"""Destructive deployment-cache retention policy with injected dependencies."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from dpone.ports.airflow_desired_state import DesiredStateCheckpointReader
from dpone.runtime.deployment_cache_activation_history import (
    DeploymentCacheActivationHistory,
    DeploymentCacheRetentionHistoryBoundary,
)
from dpone.runtime.deployment_cache_common import (
    DeploymentCacheError,
    promotion_lock,
    reconcile_lock,
)
from dpone.runtime.deployment_cache_projection_validator import DeploymentCacheProjectionValidator
from dpone.runtime.deployment_cache_retention_authority import DeploymentCacheRetentionAuthority
from dpone.runtime.deployment_cache_retention_candidate import (
    DeploymentCacheRetentionCandidateValidator,
    candidate_validation_failure,
)
from dpone.runtime.deployment_cache_retention_contracts import (
    AirflowLoaderAcknowledgement,
    AirflowLoaderAcknowledgementReader,
    DeploymentCacheRetentionApplyError,
    DeploymentRetentionApplyItem,
    DeploymentRetentionApplyReport,
    DeploymentRetentionPlanItem,
)
from dpone.runtime.deployment_cache_retention_deletion import DirectoryIdentity
from dpone.runtime.deployment_cache_retention_execution import DeploymentCacheRetentionReceiptExecutor
from dpone.runtime.deployment_cache_retention_planner import DeploymentCacheRetentionPlanner
from dpone.runtime.deployment_cache_retention_receipts import DeploymentCacheRetentionReceipts
from dpone.runtime.deployment_cache_retention_replay_guard import DeploymentCacheRetentionReplayGuard
from dpone.runtime.deployment_cache_retention_transaction import (
    DeploymentCacheRetentionTransactionCoordinator,
)


class InjectedDeploymentCacheRetentionApplier:
    """Apply deployment cache retention by deleting only fresh delete candidates."""

    def __init__(
        self,
        cache_root: str | Path,
        *,
        allowed_promoters: tuple[str, ...] = (),
        validator: DeploymentCacheProjectionValidator,
        transactions: DeploymentCacheRetentionTransactionCoordinator,
        activation_history: DeploymentCacheActivationHistory,
        planner: DeploymentCacheRetentionPlanner,
        receipts: DeploymentCacheRetentionReceipts,
        candidate_validator: DeploymentCacheRetentionCandidateValidator,
    ) -> None:
        self._cache_root = Path(cache_root).resolve(strict=False)
        self._allowed_promoters = frozenset(item for item in allowed_promoters if item)
        self._validator = validator
        self._transactions = transactions
        self._history = DeploymentCacheRetentionHistoryBoundary(self._cache_root, activation_history)
        self._planner = planner
        self._receipts = receipts
        self._candidate_validator = candidate_validator
        self._receipt_executor = DeploymentCacheRetentionReceiptExecutor(
            transactions=transactions,
            receipts=receipts,
            candidate_validator=candidate_validator,
        )
        self._replay_guard = DeploymentCacheRetentionReplayGuard(
            transactions=transactions,
            receipts=receipts,
            planner=planner,
        )

    def apply(
        self,
        *,
        environment: str,
        confirm_delete: bool,
        promoted_by: str,
        expected_plan_sha256: str | None = None,
        review_id: str | None = None,
        loader_ack_reader: AirflowLoaderAcknowledgementReader | None = None,
        checkpoint_reader: DesiredStateCheckpointReader | None = None,
        protected_deployment_ids: tuple[str, ...] = (),
    ) -> DeploymentRetentionApplyReport:
        if not confirm_delete:
            raise DeploymentCacheRetentionApplyError(
                "DPONE_DEPLOYMENT_CACHE_GC_CONFIRMATION_REQUIRED",
                "deployment cache GC requires explicit delete confirmation",
            )
        if not promoted_by:
            raise DeploymentCacheRetentionApplyError(
                "DPONE_CURRENT_POINTER_PROMOTER_MISSING",
                "deployment cache GC requires a CI/service identity",
            )
        if not self._allowed_promoters or promoted_by not in self._allowed_promoters:
            raise DeploymentCacheRetentionApplyError(
                "DPONE_CURRENT_POINTER_PROMOTER_UNAUTHORIZED",
                "deployment cache GC requires an allowed CI/service identity",
            )
        with reconcile_lock(self._cache_root), promotion_lock(self._cache_root):
            restored_occurrences = self._transactions.recover(environment=environment)
            if restored_occurrences:
                aborted_operation_ids = self._receipts.abort_after_recovery(
                    environment=environment,
                    restored_occurrences=restored_occurrences,
                )
                self._transactions.acknowledge_recovery(
                    environment=environment,
                    restored_occurrences=restored_occurrences,
                )
                raise DeploymentCacheRetentionApplyError(
                    "DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_REQUIRED",
                    "interrupted deployment retention was restored; review a fresh plan before retrying",
                    details={
                        "state_may_have_changed": True,
                        "restored_deployment_ids": sorted({item.deployment_id for item in restored_occurrences}),
                        "restored_transaction_ids": [item.transaction_id for item in restored_occurrences],
                        "aborted_operation_ids": list(aborted_operation_ids),
                    },
                )
            operation_id = self._receipts.operation_id(
                environment=environment,
                reviewed_plan_sha256=expected_plan_sha256,
                review_id=review_id,
            )
            incomplete_operation_ids = self._receipts.incomplete_operation_ids()
            legacy_operations = self._abort_legacy_receipts(
                incomplete_operation_ids,
                environment=environment,
            )
            if legacy_operations:
                raise DeploymentCacheRetentionApplyError(
                    "DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_REQUIRED",
                    "legacy retention receipts were closed; review a fresh destructive attempt",
                    details={
                        "state_may_have_changed": True,
                        "legacy_receipt_requires_fresh_review": True,
                        "aborted_operation_ids": list(legacy_operations),
                    },
                )
            foreign_operations = tuple(item for item in incomplete_operation_ids if item != operation_id)
            if foreign_operations:
                raise DeploymentCacheRetentionApplyError(
                    "DPONE_DEPLOYMENT_CACHE_GC_INCOMPLETE_OPERATION",
                    "an earlier deployment cache retention operation must be replayed before a new plan can run",
                    details={
                        "state_may_have_changed": True,
                        "incomplete_operation_ids": list(foreign_operations),
                    },
                )
            if operation_id is not None:
                existing_receipt = self._receipts.load(operation_id)
                if existing_receipt is not None:
                    return self._replay_receipt(
                        existing_receipt,
                        environment=environment,
                        loader_ack_reader=loader_ack_reader,
                        checkpoint_reader=checkpoint_reader,
                        protected_deployment_ids=protected_deployment_ids,
                    )
            try:
                plan = self._planner.plan(
                    environment=environment,
                    protected_deployment_ids=protected_deployment_ids,
                )
            except DeploymentCacheError as exc:
                raise DeploymentCacheRetentionApplyError(
                    "DPONE_DEPLOYMENT_CACHE_RECOVERY_REQUIRED",
                    "deployment cache control state is inconsistent; recover it before GC",
                    path=exc.path,
                ) from exc
            reviewed_plan_supplied = expected_plan_sha256 is not None
            if (reviewed_plan_supplied or plan.delete_candidates) and not self._receipts.is_canonical_digest(
                expected_plan_sha256
            ):
                raise DeploymentCacheRetentionApplyError(
                    "DPONE_DEPLOYMENT_CACHE_GC_PLAN_REQUIRED",
                    "deployment cache GC requires the exact reviewed plan sha256",
                )
            if reviewed_plan_supplied and plan.plan_sha256 != expected_plan_sha256:
                raise DeploymentCacheRetentionApplyError(
                    "DPONE_DEPLOYMENT_CACHE_GC_PLAN_CHANGED",
                    "deployment cache retention plan changed after review",
                    details={"expected_plan_sha256": expected_plan_sha256, "actual_plan_sha256": plan.plan_sha256},
                )
            loader_ack = (
                self._validate_authority(
                    environment=environment,
                    loader_ack_reader=loader_ack_reader,
                    checkpoint_reader=checkpoint_reader,
                )
                if plan.delete_candidates
                else None
            )
            items = [
                DeploymentRetentionApplyItem(
                    deployment_id=item.deployment_id,
                    action="skipped",
                    reason=item.reason,
                    path=item.path,
                    error_code=item.error_code,
                )
                for item in plan.items
                if item.action != "delete"
            ]
            delete_candidates: dict[str, tuple[DeploymentRetentionPlanItem, Path, DirectoryIdentity]] = {}
            for item in plan.items:
                if item.action != "delete":
                    continue
                deployment_id = item.deployment_id
                if deployment_id is None:
                    raise DeploymentCacheRetentionApplyError(
                        "DPONE_DEPLOYMENT_CACHE_GC_VALIDATION_FAILED",
                        "deployment cache GC candidate has no canonical deployment identity",
                        path=item.path,
                        details={
                            "failed_step": "validate_candidates",
                            "state_may_have_changed": False,
                            "deleted_deployment_ids": [],
                            "deleted_count": 0,
                            "failed_deployment_id": None,
                            "failed_paths": [item.path],
                            "over_budget": None,
                            "capacity_state": "unavailable_no_budget_policy",
                            "retention_incomplete": True,
                        },
                    )
                try:
                    path, identity = self._candidate_validator.validate(item, environment=environment)
                except DeploymentCacheRetentionApplyError as exc:
                    if exc.code in {
                        "DPONE_DEPLOYMENT_CACHE_GC_CURRENT_CHANGED",
                        "DPONE_DEPLOYMENT_CACHE_RECOVERY_REQUIRED",
                    }:
                        raise
                    raise candidate_validation_failure(item, cause_code=exc.code, path=exc.path) from exc
                except OSError as exc:
                    raise candidate_validation_failure(
                        item,
                        cause_code="DPONE_FILESYSTEM_ERROR",
                        path=item.path,
                    ) from exc
                delete_candidates[deployment_id] = (item, path, identity)
            if not delete_candidates:
                return DeploymentRetentionApplyReport(
                    environment=environment,
                    promoted_by=promoted_by,
                    current_deployment_id=plan.current_deployment_id,
                    reviewed_plan_sha256=plan.plan_sha256,
                    items=tuple(items),
                )
            if review_id is None:
                raise DeploymentCacheRetentionApplyError(
                    "DPONE_DEPLOYMENT_CACHE_GC_REVIEW_ID_REQUIRED",
                    "deployment cache GC requires a unique review id for this approved attempt",
                    details={"state_may_have_changed": False},
                )
            assert loader_ack is not None and operation_id is not None and review_id is not None
            activation_history_revision = self._history.prospective_revision(loader_ack)
            items.extend(
                DeploymentRetentionApplyItem(
                    deployment_id=item.deployment_id,
                    action="pending",
                    reason=item.reason,
                    path=item.path,
                )
                for item, _, _ in delete_candidates.values()
            )
            receipt = self._receipts.begin(
                operation_id=operation_id,
                environment=environment,
                promoted_by=promoted_by,
                current_deployment_id=plan.current_deployment_id,
                reviewed_plan_sha256=plan.plan_sha256,
                review_id=review_id,
                reviewed_protected_deployment_ids=plan.protected_deployment_ids,
                reviewed_recovery_revision=plan.recovery_revision,
                activation_history_revision=activation_history_revision,
                items=items,
            )
            self._history.persist(
                loader_ack,
                expected_revision=activation_history_revision,
                operation_id=operation_id,
            )
            return self._receipt_executor.execute(
                receipt,
                environment=environment,
                prevalidated=delete_candidates,
            )

    def _replay_receipt(
        self,
        receipt: dict[str, Any],
        *,
        environment: str,
        loader_ack_reader: AirflowLoaderAcknowledgementReader | None,
        checkpoint_reader: DesiredStateCheckpointReader | None,
        protected_deployment_ids: tuple[str, ...],
    ) -> DeploymentRetentionApplyReport:
        if receipt["status"] == "committed":
            if self._receipts.is_legacy(receipt):
                raise DeploymentCacheRetentionApplyError(
                    "DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_REQUIRED",
                    "legacy committed receipt is historical evidence only; review a fresh destructive attempt",
                    details={
                        "state_may_have_changed": False,
                        "legacy_receipt_requires_fresh_review": True,
                        "operation_id": receipt["operation_id"],
                    },
                )
            self._receipt_executor.validate_committed_receipt(receipt, environment=environment)
            self._receipt_executor.acknowledge_committed_wal(receipt)
            return self._receipts.report(receipt)
        if receipt["status"] == "aborted":
            raise DeploymentCacheRetentionApplyError(
                "DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_REQUIRED",
                "the interrupted retention operation was restored; review and apply a fresh plan",
                details={"state_may_have_changed": True, "operation_id": receipt["operation_id"]},
            )
        self._receipt_executor.validate_replay_receipt(receipt, environment=environment)
        self._replay_guard.abort_unbound_legacy_wal(receipt, environment=environment)
        loader_ack = self._validate_authority(
            environment=environment,
            loader_ack_reader=loader_ack_reader,
            checkpoint_reader=checkpoint_reader,
        )
        if loader_ack.deployment_id != receipt["current_deployment_id"]:
            self._replay_guard.abort_authority_drift(
                receipt,
                environment=environment,
                code="DPONE_DEPLOYMENT_CACHE_GC_PLAN_CHANGED",
                message="current deployment changed while a retention operation was incomplete",
            )
        if (
            self._history.ensure(
                loader_ack,
                expected_revision=str(receipt["activation_history_revision"]),
                operation_id=str(receipt["operation_id"]),
            )
            != receipt["activation_history_revision"]
        ):
            self._replay_guard.abort_authority_drift(
                receipt,
                environment=environment,
                code="DPONE_DEPLOYMENT_CACHE_GC_PLAN_CHANGED",
                message="activation history changed while a retention operation was incomplete",
            )
        self._replay_guard.require_candidates_deletable(
            receipt,
            environment=environment,
            protected_deployment_ids=protected_deployment_ids,
        )
        return self._receipt_executor.execute(receipt, environment=environment)

    def _abort_legacy_receipts(
        self,
        operation_ids: tuple[str, ...],
        *,
        environment: str,
    ) -> tuple[str, ...]:
        aborted: list[str] = []
        for operation_id in operation_ids:
            receipt = self._receipts.load(operation_id)
            if receipt is None or receipt["environment"] != environment or not self._receipts.is_legacy(receipt):
                continue
            self._receipt_executor.abort_legacy_receipt(receipt, environment=environment)
            aborted.append(operation_id)
        return tuple(aborted)

    def _validate_authority(
        self,
        *,
        environment: str,
        loader_ack_reader: AirflowLoaderAcknowledgementReader | None,
        checkpoint_reader: DesiredStateCheckpointReader | None,
    ) -> AirflowLoaderAcknowledgement:
        loader_ack = loader_ack_reader() if loader_ack_reader is not None else None
        if loader_ack is None:
            raise DeploymentCacheRetentionApplyError(
                "DPONE_DEPLOYMENT_CACHE_GC_ACK_REQUIRED",
                "deployment cache GC requires a strict current loader acknowledgement",
            )
        if checkpoint_reader is None:
            raise DeploymentCacheRetentionApplyError(
                "DPONE_DEPLOYMENT_CACHE_GC_DESIRED_CHECKPOINT_REQUIRED",
                "destructive retention requires an injected desired-state checkpoint reader",
            )
        DeploymentCacheRetentionAuthority(
            self._cache_root,
            projection_validator=self._validator,
            checkpoint_reader=checkpoint_reader,
        ).validate(loader_ack, environment=environment)
        return loader_ack


__all__ = ["InjectedDeploymentCacheRetentionApplier"]
