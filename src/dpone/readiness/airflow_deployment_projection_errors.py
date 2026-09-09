"""Stable errors for Airflow deployment projection services."""

from __future__ import annotations

from typing import Literal

ProjectionWriteOperation = Literal[
    "cleanup",
    "create_parent",
    "create_staging",
    "fsync",
    "inspect",
    "rename",
    "write",
]
ProjectionPublicationState = Literal["not_published", "published", "unknown"]


class AirflowDeploymentProjectionError(RuntimeError):
    """Stable public failure with path-free temporary cleanup metadata."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        path: str | None = None,
        operation: ProjectionWriteOperation | None = None,
        publication_state: ProjectionPublicationState = "not_published",
        cleanup_required: bool = False,
        cleanup_code: str | None = None,
        cleanup_artifact: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.path = path
        self.operation = operation
        self.publication_state = publication_state
        self.cleanup_required = cleanup_required
        self.cleanup_code = cleanup_code
        self.cleanup_artifact = cleanup_artifact

    def record_temporary_cleanup_failure(self) -> None:
        """Record residue without exposing its random or absolute pathname."""

        self.cleanup_required = True
        self.cleanup_code = "DPONE_DEPLOYMENT_TEMP_CLEANUP_FAILED"
        self.cleanup_artifact = "temporary_projection"


__all__ = [
    "AirflowDeploymentProjectionError",
    "ProjectionPublicationState",
    "ProjectionWriteOperation",
]
