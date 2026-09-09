"""Public parse-safe reader for dpone Airflow deployment indexes."""

from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter
from typing import Any

from dpone_airflow_pack.cache_artifact_resolver import CacheResolution, CacheResolver
from dpone_airflow_pack.deployment_index_contract import (  # re-export compatibility
    DEFAULT_MAX_ARTIFACT_BYTES,
    DEFAULT_MAX_INDEX_BYTES,
    INDEX_SCHEMA,
    INDEX_SCHEMA_V1,
    INDEX_SCHEMA_V2,
    AirflowDeploymentIndex,
    AirflowDeploymentIndexError,
    AirflowIndexArtifact,
    infer_cache_root,
    load_airflow_deployment_index,
    resolve_cache_artifact,
)
from dpone_airflow_pack.semantic_refresh_index_artifacts import (
    SemanticRefreshDagProjectionArtifact,
    load_semantic_refresh_dag_projection_artifact,
)


@dataclass(frozen=True)
class LoadReport:
    loaded: tuple[str, ...] = ()
    skipped: tuple[dict[str, str], ...] = ()
    errors: tuple[dict[str, str], ...] = ()
    duration_ms: int = 0
    release_id: str | None = None
    deployment_id: str | None = None
    airflow_index_sha256: str | None = None
    cache_root: str | None = None
    activation_id: str | None = None
    fatal: bool = False

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "loaded": list(self.loaded),
            "skipped": list(self.skipped),
            "errors": list(self.errors),
            "duration_ms": self.duration_ms,
            "release_id": self.release_id,
            "deployment_id": self.deployment_id,
            "airflow_index_sha256": self.airflow_index_sha256,
            "cache_root": self.cache_root,
            "activation_id": self.activation_id,
            "fatal": self.fatal,
        }


def load_report_started() -> float:
    return perf_counter()


def load_report(
    *,
    started_at: float,
    release_id: str | None,
    deployment_id: str | None,
    airflow_index_sha256: str | None = None,
    cache_root: str | None = None,
    activation_id: str | None = None,
    loaded: list[str] | None = None,
    skipped: list[dict[str, str]] | None = None,
    errors: list[dict[str, str]] | None = None,
    fatal: bool = False,
) -> LoadReport:
    return LoadReport(
        loaded=tuple(loaded or ()),
        skipped=tuple(skipped or ()),
        errors=tuple(errors or ()),
        duration_ms=max(0, int((perf_counter() - started_at) * 1000)),
        release_id=release_id,
        deployment_id=deployment_id,
        airflow_index_sha256=airflow_index_sha256,
        cache_root=cache_root,
        activation_id=activation_id,
        fatal=fatal,
    )


__all__ = [
    "AirflowDeploymentIndex",
    "AirflowDeploymentIndexError",
    "AirflowIndexArtifact",
    "CacheResolution",
    "CacheResolver",
    "INDEX_SCHEMA",
    "INDEX_SCHEMA_V1",
    "INDEX_SCHEMA_V2",
    "LoadReport",
    "SemanticRefreshDagProjectionArtifact",
    "DEFAULT_MAX_ARTIFACT_BYTES",
    "DEFAULT_MAX_INDEX_BYTES",
    "infer_cache_root",
    "load_airflow_deployment_index",
    "load_semantic_refresh_dag_projection_artifact",
    "load_report",
    "load_report_started",
    "resolve_cache_artifact",
]
