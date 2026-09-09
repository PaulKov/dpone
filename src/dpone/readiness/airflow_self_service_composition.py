"""Composition root for the local self-service application."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from dpone.adapters.project_authoring_lock import project_authoring_lock
from dpone.readiness.airflow_self_service_application import AirflowSelfServiceService as _AirflowSelfServiceService

if TYPE_CHECKING:
    from dpone.manifest.authoring import AuthoringCompiler
    from dpone.ports.project_authoring_lock import AuthoringLockFactory
    from dpone.readiness.airflow_live_preflight import AirflowLivePreflightRunner


class AirflowSelfServiceService(_AirflowSelfServiceService):
    """Compatibility facade that wires the safe local lock by default."""

    def __init__(
        self,
        *,
        root: str | Path = ".",
        live_preflight_runner: AirflowLivePreflightRunner | None = None,
        authoring_compiler: AuthoringCompiler | None = None,
        authoring_lock: AuthoringLockFactory = project_authoring_lock,
    ) -> None:
        super().__init__(
            root=root,
            live_preflight_runner=live_preflight_runner,
            authoring_compiler=authoring_compiler,
            authoring_lock=authoring_lock,
        )


def build_airflow_self_service_service(
    *,
    root: str | Path = ".",
    live_preflight_runner: AirflowLivePreflightRunner | None = None,
    authoring_compiler: AuthoringCompiler | None = None,
) -> AirflowSelfServiceService:
    """Wire the POSIX project lock at the outer application boundary."""

    return AirflowSelfServiceService(
        root=root,
        live_preflight_runner=live_preflight_runner,
        authoring_compiler=authoring_compiler,
    )


__all__ = ["AirflowSelfServiceService", "build_airflow_self_service_service"]
