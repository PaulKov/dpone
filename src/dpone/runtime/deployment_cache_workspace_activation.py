"""Durable workspace occurrence saga around the filesystem pointer commit."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from dpone.gitops.release_set_validation import release_activation_failure
from dpone.runtime.deployment_cache_common import DeploymentCacheError

if TYPE_CHECKING:
    from dpone.ports.dbt_workspace_activation import DbtWorkspaceActivationCoordinatorPort


class DeploymentCacheWorkspaceActivation:
    """Validate coordinator readback before and after local pointer mutation."""

    def __init__(self, coordinator: DbtWorkspaceActivationCoordinatorPort | None) -> None:
        self._coordinator = coordinator

    def prepare_occurrence(
        self,
        *,
        projection_root: Path,
        dbt_wire: str | None,
        activation_id: str,
        environment: str,
        release_id: str,
        deployment_id: str,
        previous_deployment_id: str | None,
    ) -> Any | None:
        failure = release_activation_failure(dbt_wire)
        if failure is None:
            return None
        coordinator = self._coordinator
        if coordinator is None:
            raise DeploymentCacheError(failure.code, failure.message)
        try:
            prepared = coordinator.prepare(
                projection_root=projection_root,
                activation_id=activation_id,
                environment=environment,
                release_id=release_id,
                deployment_id=deployment_id,
                previous_deployment_id=previous_deployment_id,
            )
            _require_coordinates(
                prepared.request,
                activation_id=activation_id,
                environment=environment,
                release_id=release_id,
                deployment_id=deployment_id,
                previous_deployment_id=previous_deployment_id,
            )
            prepared.__post_init__()
            return prepared
        except DeploymentCacheError:
            raise
        except Exception:
            raise DeploymentCacheError(failure.code, failure.message) from None

    def activate_occurrence(self, prepared: Any | None, *, projection_root: Path) -> None:
        if prepared is None:
            return
        try:
            coordinator = self._coordinator
            if coordinator is None:
                raise ValueError("workspace activation coordinator disappeared")
            active = coordinator.activate(prepared, projection_root=projection_root)
            _require_coordinates(
                active.request,
                activation_id=prepared.request.activation_id,
                environment=prepared.request.environment,
                release_id=prepared.request.release_id,
                deployment_id=prepared.request.deployment_id,
                previous_deployment_id=prepared.request.previous_deployment_id,
            )
            active.__post_init__()
        except Exception:
            raise DeploymentCacheError(
                "DPONE_DBT_WORKSPACE_ACTIVATION_COMMIT_UNKNOWN",
                "workspace pointer changed but durable ACTIVE acknowledgement was not observed",
                details={"state_may_have_changed": True, "recovery_required": True},
            ) from None

    def require_existing(
        self,
        *,
        projection_root: Path,
        dbt_wire: str | None,
        activation_id: str | None,
        environment: str,
        release_id: str,
        deployment_id: str,
        previous_deployment_id: str | None,
    ) -> None:
        failure = release_activation_failure(dbt_wire)
        if failure is None:
            return
        coordinator = self._coordinator
        if coordinator is None or activation_id is None:
            raise DeploymentCacheError(failure.code, failure.message)
        try:
            active = coordinator.require_active(
                projection_root=projection_root,
                activation_id=activation_id,
                environment=environment,
                release_id=release_id,
                deployment_id=deployment_id,
                previous_deployment_id=previous_deployment_id,
            )
            _require_coordinates(
                active.request,
                activation_id=activation_id,
                environment=environment,
                release_id=release_id,
                deployment_id=deployment_id,
                previous_deployment_id=previous_deployment_id,
            )
            active.__post_init__()
        except DeploymentCacheError:
            raise
        except Exception:
            raise DeploymentCacheError(failure.code, failure.message) from None


def _require_coordinates(
    request: Any,
    *,
    activation_id: str,
    environment: str,
    release_id: str,
    deployment_id: str,
    previous_deployment_id: str | None,
) -> None:
    if (
        getattr(request, "activation_id", None) != activation_id
        or getattr(request, "environment", None) != environment
        or getattr(request, "release_id", None) != release_id
        or getattr(request, "deployment_id", None) != deployment_id
        or getattr(request, "previous_deployment_id", None) != previous_deployment_id
    ):
        raise ValueError("workspace activation occurrence differs from cache promotion")


__all__ = ["DeploymentCacheWorkspaceActivation"]
