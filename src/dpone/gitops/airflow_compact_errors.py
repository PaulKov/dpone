"""Stable compact-pack build errors and connection projection boundary."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

from dpone.gitops.airflow_connection_projection_closure import (
    AirflowConnectionProjectionClosureError,
    close_connection_projection,
    required_runtime_connection_refs,
)


class CompactProcessPlanError(ValueError):
    """A stable build-time selector-plan failure."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)


class CompactConnectionProjectionError(AirflowConnectionProjectionClosureError):
    """A compiled process dependency closure cannot be materialized."""


class AirflowCompactPackBuildError(ValueError):
    """A known workload configuration problem prevents compact-pack planning."""

    def __init__(self, message: str, *, code: str | None = None) -> None:
        self.code = code
        super().__init__(message)


class ConnectionProjector(Protocol):
    """Project one workload's exact runtime connection dependencies."""

    def project_connections(self, projection: Mapping[str, Any]) -> dict[str, Any]: ...


def project_compact_connections(
    projector: ConnectionProjector,
    projection: Mapping[str, Any],
) -> dict[str, Any]:
    """Map a typed projection failure into the compact-pack build contract."""

    try:
        return projector.project_connections(projection)
    except CompactConnectionProjectionError as exc:
        raise AirflowCompactPackBuildError(str(exc), code=exc.code) from exc


__all__ = [
    "AirflowCompactPackBuildError",
    "CompactConnectionProjectionError",
    "CompactProcessPlanError",
    "ConnectionProjector",
    "AirflowConnectionProjectionClosureError",
    "close_connection_projection",
    "project_compact_connections",
    "required_runtime_connection_refs",
]
