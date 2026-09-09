"""Safe-sample v1 deployment context resolution (compatibility lane only)."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_PARSE_SIDE_EFFECTS = {
    "network": False,
    "metadata_db": False,
    "airflow_variables": False,
    "airflow_connections": False,
    "vault": False,
    "cache_refresh": False,
}

ProjectionPayloadLoader = Callable[[Path], tuple[dict[str, Any], dict[str, Any]] | None]


@dataclass(frozen=True, slots=True)
class AirflowDeploymentContext:
    release_id: str
    deployment_id: str
    deployment_type: str
    runnable: bool
    runtime_artifact_delivery: dict[str, Any]
    workload_packs: tuple[dict[str, Any], ...]
    index_path: str
    deployment_path: str | None
    binding_set_ref: str | None = None
    connection_registry_ref: str | None = None
    credential_runtime_ref: str | None = None
    runtime_image_digest: str | None = None
    airflow_bundle_ref: str | None = None
    environment: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "dpone.safe-sample-airflow-deployment-context.v1",
            "release_id": self.release_id,
            "deployment_id": self.deployment_id,
            "environment": self.environment,
            "deployment_type": self.deployment_type,
            "runnable": self.runnable,
            "runtime_artifact_delivery": self.runtime_artifact_delivery,
            "workload_packs": [dict(pack) for pack in self.workload_packs],
            "binding_set_ref": self.binding_set_ref,
            "connection_registry_ref": self.connection_registry_ref,
            "credential_runtime_ref": self.credential_runtime_ref,
            "runtime_image_digest": self.runtime_image_digest,
            "airflow_bundle_ref": self.airflow_bundle_ref,
            "index_path": self.index_path,
            "deployment_path": self.deployment_path,
            "parse_side_effects": dict(_PARSE_SIDE_EFFECTS),
        }


@dataclass(frozen=True, slots=True)
class AirflowDeploymentContextResolution:
    """Outcome of mapping a verified projection into the safe-sample v1 lane."""

    context: AirflowDeploymentContext | None = None
    error: dict[str, Any] | None = None


def load_current_airflow_deployment_context(
    cache_root: str | Path = ".dpone-cache",
    *,
    projection_loader: ProjectionPayloadLoader | None = None,
) -> AirflowDeploymentContext | None:
    """Build context only from an explicitly injected, verified projection loader.

    Filesystem discovery is intentionally absent from this application service.
    Production callers use the readiness adapter that verifies pointer, audit,
    release, projection, and artifact integrity before injecting payloads.
    """

    return resolve_current_airflow_deployment_context(
        cache_root,
        projection_loader=projection_loader,
    ).context


def resolve_current_airflow_deployment_context(
    cache_root: str | Path = ".dpone-cache",
    *,
    projection_loader: ProjectionPayloadLoader | None = None,
) -> AirflowDeploymentContextResolution:
    """Resolve context or an explicit compatibility error for one injected projection."""

    if projection_loader is None:
        return AirflowDeploymentContextResolution()
    root = Path(cache_root).resolve(strict=False)
    payloads = projection_loader(root)
    if payloads is None:
        return AirflowDeploymentContextResolution()
    deployment, index = payloads
    return resolve_airflow_deployment_context_from_projection(
        deployment=deployment,
        index=index,
        index_path=root / "current" / "airflow-index.json",
        deployment_path=root / "current" / "deployment.json",
    )


def airflow_deployment_context_from_projection(
    *,
    deployment: dict[str, Any],
    index: dict[str, Any],
    index_path: Path,
    deployment_path: Path,
) -> AirflowDeploymentContext | None:
    """Normalize one already verified deployment/index projection."""

    return resolve_airflow_deployment_context_from_projection(
        deployment=deployment,
        index=index,
        index_path=index_path,
        deployment_path=deployment_path,
    ).context


def resolve_airflow_deployment_context_from_projection(
    *,
    deployment: dict[str, Any],
    index: dict[str, Any],
    index_path: Path,
    deployment_path: Path,
) -> AirflowDeploymentContextResolution:
    """Map a verified projection into safe-sample context without silent v2 nulling."""

    schema = index.get("schema")
    if schema in {
        "dpone.airflow-deployment-index.v2",
        "dpone.airflow-deployment-index.v3",
    }:
        return AirflowDeploymentContextResolution(
            error=_compatibility_error(
                "DPONE_SAFE_SAMPLE_EXECUTABLE_INDEX_UNSUPPORTED",
                "Safe-sample local handoff accepts only airflow-deployment-index.v1 "
                "compatibility init_fetch; executable indexed KPOs require the v2/v3 "
                "Airflow provider path.",
            )
        )
    if schema != "dpone.airflow-deployment-index.v1":
        return AirflowDeploymentContextResolution()
    return AirflowDeploymentContextResolution(
        context=AirflowDeploymentContext(
            release_id=str(index.get("release_id") or deployment.get("release_ref") or ""),
            deployment_id=str(index.get("deployment_id") or deployment.get("deployment_id") or ""),
            environment=_optional_string(index.get("environment") or deployment.get("environment")),
            deployment_type=str(deployment.get("deployment_type") or "unknown"),
            runnable=bool(deployment.get("runnable")),
            runtime_artifact_delivery=_mapping(index.get("runtime_artifact_delivery")),
            workload_packs=_mappings(index.get("workload_packs")),
            index_path=_public_cache_path(index_path, fallback_name="airflow-index.json"),
            deployment_path=_public_cache_path(deployment_path, fallback_name="deployment.json"),
            binding_set_ref=_optional_string(index.get("binding_set_ref") or deployment.get("binding_set_ref")),
            connection_registry_ref=_optional_string(
                index.get("connection_registry_ref") or deployment.get("connection_registry_ref")
            ),
            credential_runtime_ref=_optional_string(
                index.get("credential_runtime_ref") or deployment.get("credential_runtime_ref")
            ),
            runtime_image_digest=_optional_string(
                index.get("runtime_image_digest") or deployment.get("runtime_image_digest")
            ),
            airflow_bundle_ref=_optional_string(
                index.get("airflow_bundle_ref") or deployment.get("airflow_bundle_ref")
            ),
        )
    )


def _compatibility_error(code: str, message: str) -> dict[str, Any]:
    return {
        "schema": "dpone.error.v1",
        "code": code,
        "stage": "safe_sample_execution_plan",
        "severity": "error",
        "message": message,
        "fixes": [],
    }


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _mappings(value: object) -> tuple[dict[str, Any], ...]:
    if not isinstance(value, list | tuple):
        return ()
    return tuple(dict(item) for item in value if isinstance(item, Mapping))


def _optional_string(value: object) -> str | None:
    text = str(value or "")
    return text or None


def _public_cache_path(path: Path, *, fallback_name: str) -> str:
    try:
        return path.resolve(strict=False).relative_to(Path.cwd().resolve(strict=False)).as_posix()
    except (OSError, RuntimeError, ValueError):
        return f"$CACHE_ROOT/{path.name or fallback_name}"


__all__ = [
    "AirflowDeploymentContext",
    "AirflowDeploymentContextResolution",
    "ProjectionPayloadLoader",
    "airflow_deployment_context_from_projection",
    "load_current_airflow_deployment_context",
    "resolve_airflow_deployment_context_from_projection",
    "resolve_current_airflow_deployment_context",
]
