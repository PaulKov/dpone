"""Persistence capability for replayable deployment-cache retention receipts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Protocol


class DeploymentCacheRetentionReceiptStorePort(Protocol):
    def load(self, operation_id: str) -> dict[str, Any] | None: ...

    def incomplete_operation_ids(self) -> tuple[str, ...]: ...

    def begin(
        self,
        *,
        operation_id: str,
        environment: str,
        promoted_by: str,
        current_deployment_id: str | None,
        reviewed_plan_sha256: str,
        review_id: str,
        reviewed_protected_deployment_ids: Sequence[str],
        reviewed_recovery_revision: str | None,
        activation_history_revision: str,
        items: Sequence[Mapping[str, object]],
    ) -> dict[str, Any]: ...

    def mark_deleted(self, receipt: dict[str, Any], deployment_id: str) -> dict[str, Any]: ...

    def abort(
        self,
        receipt: dict[str, Any],
        *,
        committed_deployment_ids: tuple[str, ...],
        restored_deployment_ids: tuple[str, ...],
        reason: str,
        error_code: str,
    ) -> dict[str, Any]: ...

    def commit(self, receipt: dict[str, Any]) -> dict[str, Any]: ...

    def committed(self, receipt: dict[str, Any]) -> dict[str, Any]: ...


__all__ = [
    "DeploymentCacheRetentionReceiptStorePort",
]
