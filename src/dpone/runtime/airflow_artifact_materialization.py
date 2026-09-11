"""Pinned, bounded local materialization use case for Airflow artifacts."""

from __future__ import annotations

import shutil
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any

from dpone.contracts.airflow_deployment import release_id as compute_release_id
from dpone.contracts.airflow_deployment_projection import deployment_projection_violation
from dpone.manifest.release_composition_files import composition_auxiliary_artifacts
from dpone.runtime.airflow_artifact_delivery_models import (
    AirflowArtifactDeliveryError,
    MaterializeReport,
    MaterializeRequest,
)
from dpone.runtime.airflow_artifact_delivery_support import (
    ArtifactRegistryError,
    ArtifactRegistryObjectNotFound,
    ArtifactRegistryReader,
    PinnedCacheRoot,
    download,
    from_cache_error,
    guard_cache_root,
    read_json,
    registry_unavailable,
    require_registry_ref,
    validate_materialization_target,
    verify_download,
)
from dpone.runtime.airflow_artifact_inventory import (
    declared_release_artifacts,
    deployment_file_names,
    semantic_refresh_deployment_files,
    validate_deployment_auxiliary_files,
)
from dpone.runtime.airflow_artifact_materialization_attestation import (
    ArtifactAttestationVerifier,
    MaterializationAttestationGate,
)
from dpone.runtime.deployment_cache_common import DeploymentCacheError
from dpone.runtime.deployment_cache_models import ValidatedDeploymentProjection
from dpone.runtime.deployment_cache_projection_validator import DeploymentCacheProjectionValidator
from dpone.runtime.immutable_local_tree import ImmutableLocalTreeError, materialize_immutable_local_tree_at

if TYPE_CHECKING:
    from dpone.ports.airflow_deployment_attestation import (
        AirflowDeploymentAttestationVerifier,
    )


class AirflowArtifactMaterializer:
    """Fetch one exact remote projection, verify it, and install without activation."""

    def __init__(
        self,
        *,
        registry: ArtifactRegistryReader,
        attestation_verifier: ArtifactAttestationVerifier | None = None,
        deployment_attestation_verifier: AirflowDeploymentAttestationVerifier | None = None,
    ) -> None:
        self._registry = registry
        self._attestation_gate = MaterializationAttestationGate(
            registry=registry,
            release_verifier=attestation_verifier,
            deployment_verifier=deployment_attestation_verifier,
        )

    def materialize(self, request: MaterializeRequest) -> MaterializeReport:
        self._attestation_gate.preflight()
        validate_materialization_target(request)
        request.cache_root.mkdir(parents=True, exist_ok=True)
        validate_materialization_target(request)
        staging = Path(tempfile.mkdtemp(prefix=".dpone-artifact-materialize-")).resolve(strict=True)
        staging.chmod(0o700)
        state = _DownloadState(request=request, registry=self._registry, staging_root=staging)
        release_state = "not_installed"
        deployment_state = "not_installed"
        try:
            with guard_cache_root(request.cache_root) as cache_root_guard:
                self._require_completion_markers(request)
                release, deployment, index = self._fetch_manifests(state)
                _validate_remote_headers(request, release=release, deployment=deployment, index=index)
                require_registry_ref(deployment, index, request.artifact_registry_ref)
                self._fetch_remaining(
                    state,
                    release=release,
                    deployment=deployment,
                    index=index,
                )
                validate_deployment_auxiliary_files(
                    deployment,
                    staging / "deployments" / request.environment / request.deployment_dir_name,
                )
                staged_projection = _validate_projection(request, cache_root=staging)
                self._attestation_gate.verify(staged_projection, cache_root=staging)
                cache_root_guard()
                release_state, deployment_state = _install_staged_projection(
                    request,
                    staging,
                    release,
                    deployment,
                    index,
                    cache_root_guard=cache_root_guard,
                )
                cache_root_guard()
                _validate_projection(request, cache_root=request.cache_root)
                cache_root_guard()
            status = "materialized" if "created" in {release_state, deployment_state} else "no_op"
            return MaterializeReport(
                status=status,
                release_id=request.release_id,
                deployment_id=request.deployment_id,
                environment=request.environment,
                artifact_registry_ref=request.artifact_registry_ref,
                downloaded_objects=state.downloaded_objects,
                downloaded_bytes=state.downloaded_bytes,
                local_release_state=release_state,
                local_deployment_state=deployment_state,
                projection_verified=True,
            )
        except AirflowArtifactDeliveryError as exc:
            exc.details.setdefault("downloaded_objects", state.downloaded_objects)
            exc.details.setdefault("downloaded_bytes", state.downloaded_bytes)
            exc.details.setdefault("local_release_state", release_state)
            exc.details.setdefault("local_deployment_state", deployment_state)
            raise
        except Exception as exc:
            raise AirflowArtifactDeliveryError(
                "DPONE_INTERNAL_AIRFLOW_ARTIFACT_DELIVERY_FAILED",
                "artifact materialization failed unexpectedly",
                details={
                    "downloaded_objects": state.downloaded_objects,
                    "downloaded_bytes": state.downloaded_bytes,
                    "local_release_state": release_state,
                    "local_deployment_state": deployment_state,
                },
            ) from exc
        finally:
            shutil.rmtree(staging, ignore_errors=True)

    def _require_completion_markers(self, request: MaterializeRequest) -> None:
        keys = (
            PurePosixPath("releases", request.release_dir_name, "_SUCCESS"),
            PurePosixPath("deployments", request.environment, request.deployment_dir_name, "_SUCCESS"),
        )
        for key in keys:
            try:
                metadata = self._registry.stat(key)
            except ArtifactRegistryObjectNotFound as exc:
                raise AirflowArtifactDeliveryError(
                    "DPONE_ARTIFACT_REGISTRY_INCOMPLETE",
                    "pinned artifact prefix has no completion marker",
                ) from exc
            except ArtifactRegistryError as exc:
                raise registry_unavailable() from exc
            _validate_metadata_size(metadata.size_bytes, max_object_bytes=request.max_object_bytes)

    def _fetch_manifests(
        self,
        state: _DownloadState,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        request = state.request
        release_path = state.fetch(PurePosixPath("releases", request.release_dir_name, "release-set.json"))
        deployment_path = state.fetch(
            PurePosixPath("deployments", request.environment, request.deployment_dir_name, "deployment.json")
        )
        index_path = state.fetch(
            PurePosixPath("deployments", request.environment, request.deployment_dir_name, "airflow-index.json")
        )
        return read_json(release_path), read_json(deployment_path), read_json(index_path)

    def _fetch_remaining(
        self,
        state: _DownloadState,
        *,
        release: Mapping[str, Any],
        deployment: Mapping[str, Any],
        index: Mapping[str, Any],
    ) -> None:
        request = state.request
        root = state.staging_root / "releases" / request.release_dir_name
        for relative, sha256 in (*declared_release_artifacts(release), *composition_auxiliary_artifacts(root, release)):
            state.fetch(
                PurePosixPath("releases", request.release_dir_name, relative.as_posix()),
                expected_sha256=sha256,
            )
        semantic_files = dict(semantic_refresh_deployment_files(deployment, index))
        for name in deployment_file_names(deployment, index):
            state.fetch(
                PurePosixPath(
                    "deployments",
                    request.environment,
                    request.deployment_dir_name,
                    name,
                ),
                expected_sha256=semantic_files.get(name),
            )


@dataclass
class _DownloadState:
    request: MaterializeRequest
    registry: ArtifactRegistryReader
    staging_root: Path
    downloaded_objects: int = 0
    downloaded_bytes: int = 0

    def fetch(self, key: PurePosixPath, *, expected_sha256: str | None = None) -> Path:
        destination = self.staging_root / key.as_posix()
        if destination.exists():
            if expected_sha256 is not None:
                verify_download(
                    destination,
                    expected_size=destination.stat().st_size,
                    expected_sha256=expected_sha256,
                    max_object_bytes=self.request.max_object_bytes,
                )
            return destination
        try:
            metadata = self.registry.stat(key)
        except ArtifactRegistryObjectNotFound as exc:
            raise AirflowArtifactDeliveryError(
                "DPONE_ARTIFACT_REGISTRY_INCOMPLETE",
                "pinned artifact object is missing",
            ) from exc
        except ArtifactRegistryError as exc:
            raise registry_unavailable() from exc
        _validate_metadata_size(metadata.size_bytes, max_object_bytes=self.request.max_object_bytes)
        if self.downloaded_bytes + metadata.size_bytes > self.request.max_total_bytes:
            raise AirflowArtifactDeliveryError(
                "DPONE_ARTIFACT_REGISTRY_TOTAL_LIMIT_EXCEEDED",
                "remote artifact projection exceeds the configured total limit",
            )
        destination.parent.mkdir(parents=True, exist_ok=True)
        download(self.registry, key, destination, max_bytes=metadata.size_bytes)
        verify_download(
            destination,
            expected_size=metadata.size_bytes,
            expected_sha256=expected_sha256,
            max_object_bytes=self.request.max_object_bytes,
        )
        self.downloaded_objects += 1
        self.downloaded_bytes += metadata.size_bytes
        return destination


def _validate_projection(
    request: MaterializeRequest,
    *,
    cache_root: Path,
) -> ValidatedDeploymentProjection:
    deployment_dir = cache_root / "deployments" / request.environment / request.deployment_dir_name
    try:
        projection = DeploymentCacheProjectionValidator(
            cache_root,
            max_artifact_bytes=request.max_object_bytes,
        ).validate_details(deployment_dir, environment=request.environment)
    except DeploymentCacheError as exc:
        raise from_cache_error(exc) from exc
    if projection.release_id != request.release_id or projection.deployment_id != request.deployment_id:
        raise AirflowArtifactDeliveryError(
            "DPONE_DEPLOYMENT_ID_MISMATCH",
            "validated deployment does not match the requested release/deployment pins",
        )
    return projection


def _validate_metadata_size(size_bytes: int, *, max_object_bytes: int) -> None:
    if isinstance(size_bytes, bool) or not isinstance(size_bytes, int) or size_bytes < 0:
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_REGISTRY_METADATA_INVALID",
            "remote artifact metadata contains an invalid size",
        )
    if size_bytes > max_object_bytes:
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_REGISTRY_OBJECT_TOO_LARGE",
            "remote artifact exceeds the configured per-object limit",
        )


def _validate_remote_headers(
    request: MaterializeRequest,
    *,
    release: Mapping[str, Any],
    deployment: Mapping[str, Any],
    index: Mapping[str, Any],
) -> None:
    release_schema = release.get("schema")
    if (
        not isinstance(release_schema, str)
        or release_schema
        not in {
            "dpone.release-set.v1",
            "dpone.release-set.v2",
            "dpone.release-set.v3",
        }
        or release.get("release_id") != request.release_id
    ):
        raise AirflowArtifactDeliveryError("DPONE_RELEASE_ID_MISMATCH", "remote release identity is invalid")
    if compute_release_id(release) != request.release_id:
        raise AirflowArtifactDeliveryError(
            "DPONE_RELEASE_FINGERPRINT_MISMATCH",
            "remote release content does not match its identity",
        )
    schema_pair = (deployment.get("schema"), index.get("schema"))
    if schema_pair not in {
        ("dpone.deployment-set.v1", "dpone.airflow-deployment-index.v1"),
        ("dpone.deployment-set.v2", "dpone.airflow-deployment-index.v2"),
        ("dpone.deployment-set.v3", "dpone.airflow-deployment-index.v3"),
    }:
        raise AirflowArtifactDeliveryError("DPONE_DEPLOYMENT_SCHEMA_INVALID", "remote deployment schema is invalid")
    if deployment.get("environment") != request.environment:
        raise AirflowArtifactDeliveryError(
            "DPONE_DEPLOYMENT_ENVIRONMENT_MISMATCH",
            "remote deployment environment does not match the request",
        )
    violation = deployment_projection_violation(
        deployment,
        index,
        release_schema=release_schema,
    )
    if violation is not None:
        raise AirflowArtifactDeliveryError(violation.code, violation.message)
    if deployment.get("deployment_id") != request.deployment_id or deployment.get("release_ref") != request.release_id:
        raise AirflowArtifactDeliveryError(
            "DPONE_DEPLOYMENT_ID_MISMATCH",
            "remote deployment does not match the requested pins",
        )


def _install_staged_projection(
    request: MaterializeRequest,
    staging: Path,
    release: Mapping[str, Any],
    deployment: Mapping[str, Any],
    index: Mapping[str, Any],
    cache_root_guard: PinnedCacheRoot,
) -> tuple[str, str]:
    staged_release = staging / "releases" / request.release_dir_name
    release_names = ["release-set.json", *(relative.as_posix() for relative, _ in declared_release_artifacts(release))]
    if release.get("schema") == "dpone.release-set.v3":
        release_names.extend(path.as_posix() for path, _ in composition_auxiliary_artifacts(staged_release, release))
    staged_deployment = staging / "deployments" / request.environment / request.deployment_dir_name
    deployment_names = deployment_file_names(deployment, index)
    release_state = "not_installed"
    deployment_state = "not_installed"
    try:
        cache_root_guard()
        release_state = materialize_immutable_local_tree_at(
            cache_root_guard.descriptor,
            PurePosixPath("releases", request.release_dir_name),
            {name: staged_release / name for name in release_names},
            error_root=request.cache_root,
        )
        cache_root_guard()
        deployment_state = materialize_immutable_local_tree_at(
            cache_root_guard.descriptor,
            PurePosixPath("deployments", request.environment, request.deployment_dir_name),
            {name: staged_deployment / name for name in deployment_names},
            error_root=request.cache_root,
        )
        cache_root_guard()
    except ImmutableLocalTreeError as exc:
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_REGISTRY_IMMUTABILITY_CONFLICT",
            "local immutable cache tree differs from downloaded content",
            details={
                "local_release_state": release_state,
                "local_deployment_state": deployment_state,
            },
        ) from exc
    return release_state, deployment_state


__all__ = ["AirflowArtifactMaterializer", "ArtifactAttestationVerifier", "validate_materialization_target"]
