"""Local Phase 1B deployment projection for beginner safe-sample runs.

The golden path intentionally keeps the user away from release/deployment
internals. This module bridges that UX with the frozen architecture by
materializing a deterministic, local-only runnable deployment projection from
already-authored sources when ``dpone run --sample --target temporary`` is used
in the development environment.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.manifest.authoring import AuthoringCompilation
    from dpone.readiness.airflow_authoring_check_service import CheckedPipelineSource


import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.readiness.airflow_authoring_check_service import AirflowAuthoringCheckService
from dpone.readiness.airflow_deployment_projection import (
    AirflowDeploymentProjectionError,
    AirflowDeploymentProjectionService,
)
from dpone.readiness.airflow_local_workload_pack import (
    LocalWorkloadPackBuildError,
    build_verified_local_airflow_workload_pack,
)
from dpone.readiness.airflow_self_service_cache_sync import local_cache_sync_result
from dpone.readiness.airflow_self_service_templates import dag_spec_payload
from dpone.readiness.airflow_verified_current_deployment import (
    resolve_verified_airflow_deployment_context,
)
from dpone.runtime.immutable_local_release import materialize_immutable_local_release

_DEVELOPMENT_ENVIRONMENTS = {"dev", "development", "local"}
_DEFAULT_DEPLOYMENT_ENVIRONMENT = "dev"
_LOCAL_ARTIFACT_REGISTRY_REF = "local-safe-sample-artifacts"
_LOCAL_RUNTIME_IMAGE_DIGEST = "sha256:" + hashlib.sha256(b"dpone-local-safe-sample-runtime").hexdigest()
_LOCAL_CACHE_RELATIVE_PATH = Path(".dpone-cache") / "safe-sample-deployments"


@dataclass(frozen=True, slots=True)
class LocalSafeSampleDeploymentResult:
    """Outcome of preparing the local runnable deployment projection."""

    status: str
    environment: str | None
    release_id: str | None = None
    deployment_id: str | None = None
    deployment_dir: str | None = None
    error: dict[str, Any] | None = None

    @property
    def usable(self) -> bool:
        return self.status in {"already_runnable", "promoted"}

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"status": self.status, "environment": self.environment}
        if self.release_id:
            payload["release_id"] = self.release_id
        if self.deployment_id:
            payload["deployment_id"] = self.deployment_id
        if self.deployment_dir:
            payload["deployment_dir"] = self.deployment_dir
        if self.error:
            payload["error"] = dict(self.error)
        return payload


def ensure_local_safe_sample_deployment(
    *,
    root: str | Path,
    pipeline_source_path: str | Path,
    policy_environment: str,
    checked_source: CheckedPipelineSource | None = None,
) -> LocalSafeSampleDeploymentResult:
    """Ensure a local development deployment can be used for safe-sample runtime.

    Non-development environments are intentionally skipped so production sample
    runs cannot silently promote local projections. The function does not read
    secrets, contact Airflow, touch Vault, or access source/sink systems.
    """

    deployment_environment = _deployment_environment(policy_environment)
    if deployment_environment is None:
        return LocalSafeSampleDeploymentResult(status="skipped", environment=None)

    root_path = Path(root).resolve(strict=False)
    cache_root = local_safe_sample_cache_root(root_path)
    checked = checked_source or AirflowAuthoringCheckService(root=root_path).inspect(pipeline_source_path)
    if not checked.result.passed:
        error = (
            dict(checked.result.errors[0])
            if checked.result.errors
            else _error(
                "DPONE_LOCAL_SAFE_SAMPLE_DEPLOYMENT_FAILED",
                "Pipeline source did not pass the static authoring check.",
            )
        )
        return LocalSafeSampleDeploymentResult(
            status="failed",
            environment=deployment_environment,
            error=error,
        )
    source_path = checked.source_path
    assert checked.payload is not None
    assert checked.compilation is not None
    assert checked.source_label is not None
    assert checked.source_sha256 is not None
    payload = checked.payload
    if checked.compilation.pipeline_id is None:
        return LocalSafeSampleDeploymentResult(
            status="failed",
            environment=deployment_environment,
            error=_error(
                "DPONE_PIPELINE_ID_INVALID",
                "Safe sample execution requires canonical pipeline metadata.id.",
            ),
        )
    pipeline_id = str(checked.compilation.pipeline_id)
    try:
        release_id = _materialize_release(
            root=root_path,
            cache_root=cache_root,
            source_path=source_path,
            payload=payload,
            compilation=checked.compilation,
            source_label=checked.source_label,
            source_sha256=checked.source_sha256,
            pipeline_id=pipeline_id,
        )
        current = _current_runnable_deployment(
            cache_root,
            deployment_environment,
            pipeline_id,
            expected_release_id=release_id,
        )
        if current is not None:
            return LocalSafeSampleDeploymentResult(
                status="already_runnable",
                environment=deployment_environment,
                release_id=current.get("release_id"),
                deployment_id=current.get("deployment_id"),
            )
        projection = AirflowDeploymentProjectionService(
            root=root_path,
            cache_root=cache_root,
        ).materialize_local_safe_sample_v1(
            release_id=release_id,
            environment=deployment_environment,
            runtime_image_digest=_LOCAL_RUNTIME_IMAGE_DIGEST,
            artifact_registry_ref=_LOCAL_ARTIFACT_REGISTRY_REF,
            airflow_bundle_ref="local:dpone-safe-sample",
        )
        promotion = local_cache_sync_result(
            cache_root=cache_root,
            deployment_dir=projection.deployment_dir,
            environment=deployment_environment,
            promoted_by="local://dpone-safe-sample",
        )
    except AirflowDeploymentProjectionError as exc:
        return LocalSafeSampleDeploymentResult(
            status="failed",
            environment=deployment_environment,
            error=_error(
                exc.code,
                str(exc),
                path=_public_path(root_path, exc.path),
            ),
        )
    except LocalWorkloadPackBuildError as exc:
        return LocalSafeSampleDeploymentResult(
            status="failed",
            environment=deployment_environment,
            error=_error(exc.code, str(exc)),
        )
    except Exception as exc:  # noqa: BLE001 - beginner CLI needs stable diagnostics.
        return LocalSafeSampleDeploymentResult(
            status="failed",
            environment=deployment_environment,
            error=_error(
                "DPONE_LOCAL_SAFE_SAMPLE_DEPLOYMENT_FAILED",
                _bounded_message(exc),
                path=checked.source_label,
            ),
        )

    if not promotion.passed:
        error = (
            promotion.errors[0]
            if promotion.errors
            else _error(
                "DPONE_LOCAL_SAFE_SAMPLE_DEPLOYMENT_PROMOTION_FAILED",
                "local safe-sample deployment could not be promoted",
                path=_repo_relative(root_path, projection.deployment_dir),
            )
        )
        return LocalSafeSampleDeploymentResult(
            status="failed",
            environment=deployment_environment,
            release_id=release_id,
            deployment_id=projection.deployment["deployment_id"],
            deployment_dir=_repo_relative(root_path, projection.deployment_dir),
            error=dict(error),
        )

    return LocalSafeSampleDeploymentResult(
        status="promoted",
        environment=deployment_environment,
        release_id=release_id,
        deployment_id=projection.deployment["deployment_id"],
        deployment_dir=_repo_relative(root_path, projection.deployment_dir),
    )


def _deployment_environment(policy_environment: str) -> str | None:
    normalized = str(policy_environment or "").strip().lower()
    if normalized not in _DEVELOPMENT_ENVIRONMENTS:
        return None
    return _DEFAULT_DEPLOYMENT_ENVIRONMENT


def local_safe_sample_cache_root(root: str | Path) -> Path:
    """Return the cache root isolated from the scheduler-visible Airflow pointer."""

    return Path(root).resolve(strict=False) / _LOCAL_CACHE_RELATIVE_PATH


def _current_runnable_deployment(
    cache_root: Path,
    environment: str,
    pipeline_id: str,
    *,
    expected_release_id: str,
) -> dict[str, str] | None:
    resolution = resolve_verified_airflow_deployment_context(
        cache_root,
        expected_environment=environment,
    )
    if resolution.error is not None:
        return None
    context = resolution.context
    if context is None or not context.runnable or context.release_id != expected_release_id:
        return None
    if not any(pack.get("id") == pipeline_id for pack in context.workload_packs):
        return None
    return {"release_id": context.release_id, "deployment_id": context.deployment_id}


def _materialize_release(
    *,
    root: Path,
    cache_root: Path,
    source_path: Path,
    payload: dict[str, Any],
    compilation: AuthoringCompilation,
    source_label: str,
    source_sha256: str,
    pipeline_id: str,
) -> str:
    pack = build_verified_local_airflow_workload_pack(
        root=root,
        source_path=source_path,
        pipeline_payload=payload,
        pipeline_id=pipeline_id,
        runtime_image="local/dpone-runtime:safe-sample",
        runtime_image_digest=_LOCAL_RUNTIME_IMAGE_DIGEST,
        expected_authoring_dependencies=compilation.dependencies,
        expected_primary_path=source_label,
        expected_primary_sha256=source_sha256,
    )
    dag_spec = dag_spec_payload(pipeline_id, payload, pack=pack)
    dag_bytes = _json_bytes(dag_spec)
    pack_bytes = _json_bytes(pack)
    dag_sha = _sha256_bytes(dag_bytes)
    pack_sha = _sha256_bytes(pack_bytes)
    release_payload = {
        "schema": "dpone.release-set.v1",
        "release_id": "",
        "artifacts": {
            "dag_specs": [{"id": pipeline_id, "path": f"dags/{pipeline_id}.dag-spec.json", "sha256": dag_sha}],
            "workload_packs": [
                {"id": pipeline_id, "path": f"packs/{pipeline_id}.airflow-pack.json", "sha256": pack_sha}
            ],
            "canonical_schemas": [],
        },
        "provenance": {"source": _repo_relative(root, source_path), "built_by": "dpone run --sample"},
    }
    from dpone.readiness.airflow_deployment_projection import compute_release_id

    release_id = compute_release_id(release_payload)
    release_payload["release_id"] = release_id
    release_dir = cache_root / "releases" / _digest_dir(release_id)
    materialize_immutable_local_release(
        release_dir,
        {
            f"dags/{pipeline_id}.dag-spec.json": dag_bytes,
            f"packs/{pipeline_id}.airflow-pack.json": pack_bytes,
            "release-set.json": _json_bytes(release_payload),
        },
    )
    return release_id


def _repo_relative(root: Path, path: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return path.name or "artifact"


def _public_path(root: Path, raw_path: str | None) -> str | None:
    if not raw_path:
        return None
    path = Path(raw_path)
    return _repo_relative(root, path) if path.is_absolute() else path.as_posix()


def _json_bytes(payload: dict[str, Any]) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _sha256_bytes(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _digest_dir(digest: str) -> str:
    return digest.replace(":", "-")


def _bounded_message(exc: Exception) -> str:
    return (" ".join(str(exc).split()) or exc.__class__.__name__)[:500]


def _error(code: str, message: str, *, path: str | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema": "dpone.error.v1",
        "code": code,
        "stage": "local_safe_sample_deployment",
        "severity": "error",
        "message": message,
        "fixes": [],
    }
    if path:
        payload["path"] = path
    return payload


__all__ = [
    "ensure_local_safe_sample_deployment",
    "local_safe_sample_cache_root",
    "LocalSafeSampleDeploymentResult",
]
