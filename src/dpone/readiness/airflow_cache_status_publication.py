"""Composition facade for cache-status publication commands."""

from __future__ import annotations

from pathlib import Path

from dpone.adapters.airflow_cache_status_files import PosixAirflowCacheStatusFileStorage
from dpone.gitops.airflow_cache_status_schema_validator import (
    GitOpsAirflowCacheStatusSchemaValidator,
)
from dpone.services.airflow_cache_status_publication import (
    AirflowCacheStatusPublicationRequest,
    AirflowCacheStatusPublisher,
)


def publish_airflow_cache_status(
    *,
    status_root: Path,
    source_name: str,
    target_name: str,
    failure_marker_name: str,
    expected_schema: str,
) -> tuple[dict[str, object], int]:
    """Publish one status document through the canonical composition root."""

    report = AirflowCacheStatusPublisher(
        validator=GitOpsAirflowCacheStatusSchemaValidator(),
        storage=PosixAirflowCacheStatusFileStorage(),
    ).publish(
        AirflowCacheStatusPublicationRequest(
            status_root=status_root,
            source_name=source_name,
            target_name=target_name,
            failure_marker_name=failure_marker_name,
            expected_schema=expected_schema,
        )
    )
    return report.to_dict(), report.exit_code


__all__ = ["publish_airflow_cache_status"]
