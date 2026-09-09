"""Plan-first retention policy for local Airflow deployment caches."""

from __future__ import annotations

from pathlib import Path

from dpone.runtime.deployment_cache_common import (
    DeploymentCacheError,
    deployment_id_from_dir,
    read_regular_json_object,
)
from dpone.runtime.deployment_cache_current_state import DeploymentCacheCurrentState
from dpone.runtime.deployment_cache_projection_validator import DeploymentCacheProjectionValidator
from dpone.runtime.deployment_cache_retention_contracts import (
    DeploymentRetentionPlan,
    DeploymentRetentionPlanItem,
    is_canonical_retention_digest,
)
from dpone.runtime.deployment_cache_retention_state_codec import (
    DeploymentCacheRetentionStateError,
    build_retention_recovery,
    parse_retention_recovery,
)


class DeploymentCacheRetentionPlanner:
    """Build a plan-first GC view for local deployment cache entries."""

    def __init__(self, cache_root: str | Path) -> None:
        self._cache_root = Path(cache_root).resolve(strict=False)
        self._validator = DeploymentCacheProjectionValidator(self._cache_root)

    def plan(
        self,
        *,
        environment: str,
        protected_deployment_ids: tuple[str, ...] = (),
        exclude_operation_id: str | None = None,
    ) -> DeploymentRetentionPlan:
        current_deployment_id, previous_deployment_id = self._current_deployment_ids(environment=environment)
        recovery_revision = self._recovery_revision(exclude_operation_id=exclude_operation_id)
        protected_ids = set(protected_deployment_ids)
        if previous_deployment_id is not None:
            protected_ids.add(previous_deployment_id)
        deployment_root = self._cache_root / "deployments" / environment
        if not deployment_root.exists():
            return DeploymentRetentionPlan(
                environment=environment,
                current_deployment_id=current_deployment_id,
                protected_deployment_ids=tuple(sorted(protected_ids)),
                items=(),
                recovery_revision=recovery_revision,
            )
        items = tuple(
            sorted(
                (
                    self._plan_item(
                        path,
                        environment=environment,
                        current_deployment_id=current_deployment_id,
                        previous_deployment_id=previous_deployment_id,
                        protected_deployment_ids=protected_ids,
                    )
                    for path in deployment_root.iterdir()
                ),
                key=lambda item: (item.deployment_id or "", item.path),
            )
        )
        return DeploymentRetentionPlan(
            environment=environment,
            current_deployment_id=current_deployment_id,
            protected_deployment_ids=tuple(sorted(protected_ids)),
            items=items,
            recovery_revision=recovery_revision,
        )

    def _recovery_revision(self, *, exclude_operation_id: str | None) -> str | None:
        path = self._cache_root / ".retention-recovery.json"
        if not path.exists():
            return None
        payload = read_regular_json_object(
            path,
            missing_code="DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_INVALID",
            invalid_code="DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_INVALID",
            label="deployment cache retention recovery marker",
            root=self._cache_root,
        )
        try:
            recovery = parse_retention_recovery(payload)
        except DeploymentCacheRetentionStateError as exc:
            raise DeploymentCacheError(
                "DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_INVALID",
                "deployment cache retention recovery marker is invalid",
                path=path.as_posix(),
            ) from exc
        transactions = {
            str(key): dict(value)
            for key, value in recovery["transactions"].items()
            if exclude_operation_id is None or value.get("operation_id") != exclude_operation_id
        }
        if not transactions:
            return None
        pending = sorted(
            {
                str(value["deployment_id"])
                for value in transactions.values()
                if value["phase"] not in {"committed", "restored"}
            }
        )
        restored = sorted(
            {str(value["deployment_id"]) for value in transactions.values() if value["phase"] == "restored"}
        )
        quarantined = sorted(
            str(value["detached_path"]) for value in transactions.values() if value["phase"] == "blocked"
        )
        if pending:
            raise DeploymentCacheError(
                "DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_REQUIRED",
                "deployment cache retention recovery is incomplete",
                path=path.as_posix(),
                details={
                    "state_may_have_changed": True,
                    "restored_deployment_ids": restored,
                    "pending_deployment_ids": pending,
                    "quarantined_paths": quarantined,
                },
            )
        scoped = build_retention_recovery(
            status="recovered",
            restored_deployment_ids=restored,
            pending_deployment_ids=(),
            transactions=transactions,
            quarantined_paths=(),
        )
        return str(scoped["revision"])

    def _current_deployment_ids(self, *, environment: str) -> tuple[str | None, str | None]:
        current = DeploymentCacheCurrentState(self._cache_root).validated_active_deployment_id(
            expected_environment=environment,
            projection_validator=self._validator,
        )
        if current is None:
            return None, None
        pointer = read_regular_json_object(
            self._cache_root / "current-pointer.json",
            missing_code="DPONE_CURRENT_POINTER_NOT_FOUND",
            invalid_code="DPONE_CURRENT_POINTER_INVALID",
            label="current pointer",
            root=self._cache_root,
        )
        previous = pointer.get("previous_deployment_id")
        return current, str(previous) if is_canonical_retention_digest(previous) else None

    def _plan_item(
        self,
        path: Path,
        *,
        environment: str,
        current_deployment_id: str | None,
        previous_deployment_id: str | None,
        protected_deployment_ids: set[str],
    ) -> DeploymentRetentionPlanItem:
        raw_deployment_id = deployment_id_from_dir(path)
        deployment_id = raw_deployment_id if is_canonical_retention_digest(raw_deployment_id) else None
        if path.is_symlink() or not path.is_dir():
            return DeploymentRetentionPlanItem(
                deployment_id=deployment_id,
                action="quarantine",
                reason="invalid",
                path=path.as_posix(),
                error_code="DPONE_DEPLOYMENT_PATH_INVALID",
            )
        try:
            projection = self._validator.validate_details(path, environment=environment)
            deployment_id = projection.deployment_id
        except DeploymentCacheError as exc:
            return DeploymentRetentionPlanItem(
                deployment_id=deployment_id,
                action="quarantine",
                reason="incomplete" if exc.code == "DPONE_DEPLOYMENT_INCOMPLETE" else "invalid",
                path=path.as_posix(),
                error_code=exc.code,
            )
        if deployment_id == current_deployment_id:
            return _protected_item(deployment_id, "current", path)
        if deployment_id == previous_deployment_id:
            return _protected_item(deployment_id, "retention_evidence", path)
        if deployment_id in protected_deployment_ids:
            return _protected_item(deployment_id, "retention_evidence", path)
        return DeploymentRetentionPlanItem(
            deployment_id=deployment_id,
            action="delete",
            reason="unreferenced",
            path=path.as_posix(),
        )


def _protected_item(deployment_id: str, reason: str, path: Path) -> DeploymentRetentionPlanItem:
    return DeploymentRetentionPlanItem(
        deployment_id=deployment_id,
        action="protect",
        reason=reason,
        path=path.as_posix(),
    )


__all__ = ["DeploymentCacheRetentionPlanner"]
