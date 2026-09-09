"""Value contracts for deployment cache retention operations."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol


class AirflowLoaderAcknowledgement(Protocol):
    """Structural loader proof consumed by destructive cache retention."""

    release_id: str
    deployment_id: str
    airflow_index_sha256: str
    activation_id: str
    loaded_dag_ids: tuple[str, ...]
    skipped_dag_ids: tuple[str, ...]
    error_codes: tuple[str, ...]
    fatal: bool
    acknowledged_at: str


class AirflowLoaderAcknowledgementReader(Protocol):
    """Read the loader proof inside the retention finalization boundary."""

    def __call__(self) -> AirflowLoaderAcknowledgement | None: ...


@dataclass(frozen=True, slots=True)
class RetentionTransactionOccurrence:
    """One exact WAL outcome used for replay and recovery acknowledgement."""

    transaction_id: str
    deployment_id: str
    operation_id: str | None
    original_path: str


def is_canonical_retention_digest(value: object) -> bool:
    """Return whether a value is one lowercase canonical sha256 identity."""

    return bool(
        isinstance(value, str)
        and len(value) == 71
        and value.startswith("sha256:")
        and all(character in "0123456789abcdef" for character in value[7:])
    )


@dataclass(frozen=True, slots=True)
class DeploymentRetentionPlanItem:
    deployment_id: str | None
    action: str
    reason: str
    path: str
    error_code: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "deployment_id": self.deployment_id,
            "action": self.action,
            "reason": self.reason,
            "path": self.path,
        }
        if self.error_code:
            payload["error_code"] = self.error_code
        return payload


@dataclass(frozen=True, slots=True)
class DeploymentRetentionPlan:
    environment: str
    current_deployment_id: str | None
    protected_deployment_ids: tuple[str, ...]
    items: tuple[DeploymentRetentionPlanItem, ...]
    recovery_revision: str | None = None

    @property
    def delete_candidates(self) -> tuple[str, ...]:
        return tuple(
            item.deployment_id for item in self.items if item.action == "delete" and item.deployment_id is not None
        )

    @property
    def plan_sha256(self) -> str:
        body = json.dumps(self._payload(), ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
        return "sha256:" + hashlib.sha256(body).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return {**self._payload(), "plan_sha256": self.plan_sha256}

    def _payload(self) -> dict[str, Any]:
        payload = {
            "schema": "dpone.deployment-cache-retention-plan.v1",
            "environment": self.environment,
            "current_deployment_id": self.current_deployment_id,
            "protected_deployment_ids": list(self.protected_deployment_ids),
            "items": [item.to_dict() for item in self.items],
            "delete_candidates": list(self.delete_candidates),
        }
        if self.recovery_revision is not None:
            payload["recovery_revision"] = self.recovery_revision
        return payload


class DeploymentCacheRetentionApplyError(RuntimeError):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        path: str | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.path = path
        self.details = dict(details or {})


@dataclass(frozen=True, slots=True)
class DeploymentRetentionApplyItem:
    deployment_id: str | None
    action: str
    reason: str
    path: str
    error_code: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "deployment_id": self.deployment_id,
            "action": self.action,
            "reason": self.reason,
            "path": self.path,
        }
        if self.error_code:
            payload["error_code"] = self.error_code
        return payload


@dataclass(frozen=True, slots=True)
class DeploymentRetentionApplyReport:
    environment: str
    promoted_by: str
    current_deployment_id: str | None
    items: tuple[DeploymentRetentionApplyItem, ...]
    reviewed_plan_sha256: str | None = None
    activation_history_revision: str | None = None
    operation_id: str | None = None
    review_id: str | None = None
    receipt_revision: str | None = None
    transaction_status: str | None = None

    @property
    def deleted_deployment_ids(self) -> tuple[str, ...]:
        return tuple(
            item.deployment_id for item in self.items if item.action == "deleted" and item.deployment_id is not None
        )

    @property
    def skipped_deployment_ids(self) -> tuple[str, ...]:
        return tuple(
            item.deployment_id for item in self.items if item.action == "skipped" and item.deployment_id is not None
        )

    def to_dict(self) -> dict[str, Any]:
        """Project the published v1 compatibility contract."""

        return {
            "schema": "dpone.deployment-cache-retention-apply.v1",
            "environment": self.environment,
            "promoted_by": self.promoted_by,
            "current_deployment_id": self.current_deployment_id,
            "items": [item.to_dict() for item in self.items],
            "deleted_deployment_ids": list(self.deleted_deployment_ids),
            "skipped_deployment_ids": list(self.skipped_deployment_ids),
        }

    def to_v2_dict(self) -> dict[str, Any]:
        """Project reviewed-plan and activation-history evidence."""

        payload: dict[str, Any] = {
            "schema": "dpone.deployment-cache-retention-apply.v2",
            "environment": self.environment,
            "promoted_by": self.promoted_by,
            "current_deployment_id": self.current_deployment_id,
            "items": [item.to_dict() for item in self.items],
            "deleted_deployment_ids": list(self.deleted_deployment_ids),
            "skipped_deployment_ids": list(self.skipped_deployment_ids),
        }
        if self.reviewed_plan_sha256 is not None:
            payload["reviewed_plan_sha256"] = self.reviewed_plan_sha256
        if self.activation_history_revision is not None:
            payload["activation_history_revision"] = self.activation_history_revision
        return payload

    def to_v3_dict(self) -> dict[str, Any]:
        """Project receipt-backed transaction evidence for strict controllers."""

        payload = self.to_v2_dict()
        payload["schema"] = "dpone.deployment-cache-retention-apply.v3"
        if not self.deleted_deployment_ids:
            return payload
        missing = tuple(
            name
            for name, value in (
                ("reviewed_plan_sha256", self.reviewed_plan_sha256),
                ("activation_history_revision", self.activation_history_revision),
                ("operation_id", self.operation_id),
                ("review_id", self.review_id),
                ("receipt_revision", self.receipt_revision),
            )
            if value is None
        )
        if missing or self.transaction_status != "committed":
            raise DeploymentCacheRetentionApplyError(
                "DPONE_DEPLOYMENT_CACHE_GC_V3_EVIDENCE_UNAVAILABLE",
                "v3 retention evidence requires one reviewed committed v2 receipt",
                details={
                    "state_may_have_changed": False,
                    "missing_fields": list(missing),
                    "transaction_status": self.transaction_status,
                },
            )
        payload["operation_id"] = self.operation_id
        payload["review_id"] = self.review_id
        payload["receipt_revision"] = self.receipt_revision
        payload["transaction_status"] = self.transaction_status
        return payload


__all__ = [
    "AirflowLoaderAcknowledgement",
    "AirflowLoaderAcknowledgementReader",
    "DeploymentCacheRetentionApplyError",
    "DeploymentRetentionApplyItem",
    "DeploymentRetentionApplyReport",
    "DeploymentRetentionPlan",
    "DeploymentRetentionPlanItem",
    "RetentionTransactionOccurrence",
    "is_canonical_retention_digest",
]
