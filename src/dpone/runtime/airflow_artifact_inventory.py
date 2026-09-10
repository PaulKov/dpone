"""Deterministic inventories for immutable Airflow artifact delivery."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any

from dpone.contracts.airflow_deployment import is_sha256_digest
from dpone.contracts.airflow_release_artifacts import ReleaseArtifactLocatorError, release_artifact_path
from dpone.contracts.release_composition_subject import COMPOSITION_SUBJECT, composition_subject_sha256
from dpone.runtime.airflow_artifact_attestation_inventory import attestation_publication_spec
from dpone.runtime.airflow_artifact_delivery_models import (
    AirflowArtifactDeliveryError,
    ArtifactFile,
    ArtifactInventory,
    PublicationCommitment,
    PublishedArtifactDescriptor,
)
from dpone.runtime.airflow_artifact_inventory_files import (
    attestation_artifact as _attestation_artifact,
)
from dpone.runtime.airflow_artifact_inventory_files import (
    local_artifact_file as _local_artifact_file,
)
from dpone.runtime.airflow_artifact_inventory_files import (
    marker_last as _marker_last,
)
from dpone.runtime.airflow_artifact_inventory_files import (
    runtime_connection_artifact as _runtime_connection_artifact,
)
from dpone.runtime.airflow_runtime_connection_inventory import (
    runtime_connection_publication_files,
    validate_deployment_auxiliary_files,
)
from dpone.runtime.deployment_cache_common import read_regular_json_object

if TYPE_CHECKING:
    from dpone.runtime.airflow_artifact_delivery_models import PublishRequest
    from dpone.runtime.deployment_cache_models import ValidatedDeploymentProjection

_BASE_RELEASE_SECTIONS = ("dag_specs", "workload_packs", "canonical_schemas")
_BASE_DEPLOYMENT_FILES = ("deployment.json", "airflow-index.json", "_SUCCESS")


def release_includes_runtime_payloads(
    release_schema: str,
    artifacts: Mapping[str, Any],
) -> bool:
    """Whether release ``runtime_payloads`` must be published and cache-verified.

    Compact ``dpone.release-set.v1`` may optionally carry ``runtime_payloads`` when
    packs declare ``runtime_payload_ids``. Those bytes must be uploaded;
    otherwise init-fetch fails with ``DPONE_CACHE_ARTIFACT_NOT_FOUND``.
    """

    return release_schema == "dpone.release-set.v2" or "runtime_payloads" in artifacts


def release_artifact_sections(
    release_schema: str,
    artifacts: Mapping[str, Any],
) -> tuple[str, ...]:
    """Return release artifact sections that must be published."""

    sections = list(_BASE_RELEASE_SECTIONS)
    if release_includes_runtime_payloads(release_schema, artifacts):
        sections.append("runtime_payloads")
    if release_schema == "dpone.release-set.v3":
        sections.append("composition_sources")
    return tuple(sections)


def build_publish_inventory(
    request: PublishRequest,
    projection: ValidatedDeploymentProjection,
) -> ArtifactInventory:
    """Build and locally verify the only files publication is allowed to touch."""

    release_dir = request.cache_root / "releases" / request.release_dir_name
    deployment_dir = request.cache_root / "deployments" / request.environment / request.deployment_dir_name
    release_path = release_dir / "release-set.json"
    release = read_regular_json_object(
        release_path,
        missing_code="DPONE_RELEASE_NOT_FOUND",
        invalid_code="DPONE_RELEASE_INVALID",
        label="release-set",
        root=request.cache_root,
    )
    release_set_file = _local_artifact_file(
        key=PurePosixPath("releases", request.release_dir_name, "release-set.json"),
        path=release_path,
        root=request.cache_root,
    )
    release_files = [release_set_file]
    for relative, declared_sha in declared_release_artifacts(release):
        item = _local_artifact_file(
            key=PurePosixPath("releases", request.release_dir_name, relative.as_posix()),
            path=release_dir / relative.as_posix(),
            root=request.cache_root,
        )
        if item.sha256.lower() != declared_sha.lower():
            raise AirflowArtifactDeliveryError(
                "DPONE_ARTIFACT_REGISTRY_CHECKSUM_MISMATCH",
                "local release artifact checksum differs from release-set",
                details={"logical_path": relative.as_posix()},
            )
        release_files.append(item)

    if release.get("schema") == "dpone.release-set.v3":
        subject = _local_artifact_file(
            key=PurePosixPath("releases", request.release_dir_name, COMPOSITION_SUBJECT),
            path=release_dir / COMPOSITION_SUBJECT,
            root=request.cache_root,
        )
        from dpone.manifest.confined_files import read_confined_file

        descriptor_payload = read_confined_file(release_dir, "release-set.json", max_bytes=8 * 1024 * 1024)
        if subject.sha256 != composition_subject_sha256(release, descriptor_payload):
            raise AirflowArtifactDeliveryError(
                "DPONE_COMPOSITION_INVALID", "composition subject differs from bound release bytes"
            )
        release_files.append(subject)

    deployment_names = deployment_file_names(
        projection.deployment,
        projection.airflow_index,
    )
    deployment_files = [
        _local_artifact_file(
            key=PurePosixPath("deployments", request.environment, request.deployment_dir_name, name),
            path=deployment_dir / name,
            root=request.cache_root,
            completion_marker=name == "_SUCCESS",
        )
        for name in deployment_names
    ]
    validate_deployment_auxiliary_files(projection.deployment, deployment_dir)
    if projection.deployment.get("schema") in {"dpone.deployment-set.v2", "dpone.deployment-set.v3"}:
        deployment_files.extend(
            _runtime_connection_artifact(item)
            for item in runtime_connection_publication_files(
                projection.deployment,
                deployment_dir=deployment_dir,
                root=request.cache_root,
            )
        )
    attestation_spec = attestation_publication_spec(
        request,
        release_schema=str(release.get("schema", "dpone.release-set.v1")),
        release_set_sha256=release_set_file.sha256,
    )
    if attestation_spec is not None:
        release_files.append(_attestation_artifact(attestation_spec))
    release_files.append(
        _local_artifact_file(
            key=PurePosixPath("releases", request.release_dir_name, "_SUCCESS"),
            path=deployment_dir / "_SUCCESS",
            root=request.cache_root,
            completion_marker=True,
        )
    )
    inventory = ArtifactInventory(
        release=_marker_last(release_files),
        deployment=_marker_last(deployment_files),
    )
    require_inventory_budget(
        inventory.objects,
        max_object_bytes=request.max_object_bytes,
        max_total_bytes=request.max_total_bytes,
    )
    return inventory


def build_publication_commitment(
    inventory: ArtifactInventory,
    *,
    request: PublishRequest,
    registry_scope_id: str,
) -> PublicationCommitment:
    """Select the exact control-plane roots from a verified inventory."""

    deployment_root = PurePosixPath(
        "deployments",
        request.environment,
        request.deployment_dir_name,
    )
    return PublicationCommitment(
        release=_published_descriptor(
            inventory,
            PurePosixPath("releases", request.release_dir_name, "release-set.json"),
        ),
        deployment=_published_descriptor(inventory, deployment_root / "deployment.json"),
        airflow_index=_published_descriptor(inventory, deployment_root / "airflow-index.json"),
        registry_scope_id=registry_scope_id,
    )


def _published_descriptor(
    inventory: ArtifactInventory,
    expected_key: PurePosixPath,
) -> PublishedArtifactDescriptor:
    matches = tuple(item for item in inventory.objects if item.key == expected_key)
    if len(matches) != 1:
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_REGISTRY_INCOMPLETE",
            "verified publication inventory is missing an exact root artifact",
            details={"logical_key": expected_key.as_posix()},
        )
    item = matches[0]
    return PublishedArtifactDescriptor(
        object_key=item.key.as_posix(),
        sha256=item.sha256,
        bytes=item.size_bytes,
    )


def declared_release_artifacts(release: Mapping[str, Any]) -> tuple[tuple[PurePosixPath, str], ...]:
    artifacts = release.get("artifacts")
    if not isinstance(artifacts, Mapping):
        raise AirflowArtifactDeliveryError(
            "DPONE_RELEASE_ARTIFACTS_INVALID",
            "release-set artifacts must be an object",
        )
    declared: dict[str, str] = {}
    executable_artifacts = 0
    release_schema = release.get("schema", "dpone.release-set.v1")
    if release_schema not in {
        "dpone.release-set.v1",
        "dpone.release-set.v2",
        "dpone.release-set.v3",
    }:
        raise AirflowArtifactDeliveryError(
            "DPONE_RELEASE_SCHEMA_INVALID",
            "release-set schema is unsupported",
        )
    sections = release_artifact_sections(release_schema, artifacts)
    for section in sections:
        items = artifacts.get(section)
        if not isinstance(items, list):
            raise AirflowArtifactDeliveryError(
                "DPONE_RELEASE_ARTIFACTS_INVALID",
                f"release-set {section} must be an array",
            )
        for item in items:
            if not isinstance(item, Mapping):
                raise AirflowArtifactDeliveryError(
                    "DPONE_RELEASE_ARTIFACTS_INVALID",
                    f"release-set {section} entries must be objects",
                )
            try:
                relative = release_artifact_path(item)
            except ReleaseArtifactLocatorError as exc:
                raise AirflowArtifactDeliveryError("DPONE_RELEASE_ARTIFACTS_INVALID", str(exc)) from exc
            sha256 = item.get("sha256")
            if not is_sha256_digest(sha256):
                raise AirflowArtifactDeliveryError(
                    "DPONE_RELEASE_ARTIFACTS_INVALID",
                    "release artifact sha256 is invalid",
                )
            key = relative.as_posix()
            if key in declared:
                raise AirflowArtifactDeliveryError(
                    "DPONE_RELEASE_ARTIFACTS_INVALID",
                    "release-set contains duplicate artifact paths",
                )
            declared[key] = str(sha256)
            if section in {"dag_specs", "workload_packs"}:
                executable_artifacts += 1
    if executable_artifacts == 0:
        raise AirflowArtifactDeliveryError(
            "DPONE_RELEASE_ARTIFACTS_INVALID",
            "release-set must declare at least one DAG spec or workload pack",
        )
    return tuple((PurePosixPath(path), declared[path]) for path in sorted(declared))


def deployment_file_names(
    deployment: Mapping[str, Any],
    index: Mapping[str, Any] | None = None,
) -> tuple[str, ...]:
    names = list(_BASE_DEPLOYMENT_FILES)
    if deployment.get("binding_set_ref") is not None:
        names.append("binding-set.json")
    if deployment.get("connection_registry_ref") is not None:
        names.append("connection-registry.ref")
    if deployment.get("credential_runtime_ref") is not None:
        names.append("credential-runtime.ref")
    names.extend(name for name, _ in semantic_refresh_deployment_files(deployment, index))
    return tuple(sorted(names, key=lambda name: (name == "_SUCCESS", name)))


def semantic_refresh_deployment_files(
    deployment: Mapping[str, Any],
    index: Mapping[str, Any] | None,
) -> tuple[tuple[str, str], ...]:
    """Return exact deployment-local semantic sidecar names and byte digests."""

    if index is None:
        return ()
    raw = index.get("semantic_refresh_dag_projections", [])
    if not isinstance(raw, list) or len(raw) > 64:
        raise AirflowArtifactDeliveryError(
            "DPONE_AIRFLOW_INDEX_FIELD_INVALID",
            "semantic-refresh DAG projection inventory is invalid",
        )
    deployment_id = deployment.get("deployment_id")
    environment = deployment.get("environment")
    if not isinstance(deployment_id, str) or not isinstance(environment, str):
        raise AirflowArtifactDeliveryError(
            "DPONE_DEPLOYMENT_ID_MISMATCH",
            "deployment identity is absent for semantic-refresh sidecars",
        )
    expected_parent = f"cache://deployments/{environment}/{deployment_id.replace(':', '-', 1)}/"
    files: dict[str, str] = {}
    descriptor_fields = {
        "artifact_bytes",
        "artifact_ref",
        "artifact_sha256",
        "authority",
        "dag_id",
        "dag_projection_sha256",
        "projection_id",
        "workflow_name",
    }
    for item in raw:
        if not isinstance(item, Mapping) or set(item) != descriptor_fields:
            raise AirflowArtifactDeliveryError(
                "DPONE_AIRFLOW_INDEX_FIELD_INVALID",
                "semantic-refresh DAG projection descriptor is not closed",
            )
        projection_sha = item.get("dag_projection_sha256")
        artifact_sha = item.get("artifact_sha256")
        artifact_ref = item.get("artifact_ref")
        declared_bytes = item.get("artifact_bytes")
        if (
            not is_sha256_digest(projection_sha)
            or not is_sha256_digest(artifact_sha)
            or not isinstance(artifact_ref, str)
            or isinstance(declared_bytes, bool)
            or not isinstance(declared_bytes, int)
            or declared_bytes <= 0
        ):
            raise AirflowArtifactDeliveryError(
                "DPONE_AIRFLOW_INDEX_FIELD_INVALID",
                "semantic-refresh DAG projection artifact identity is invalid",
            )
        filename = f"semantic-refresh-{str(projection_sha).removeprefix('sha256:')}.dag-projection.json"
        if artifact_ref != expected_parent + filename or filename in files:
            raise AirflowArtifactDeliveryError(
                "DPONE_AIRFLOW_INDEX_FIELD_INVALID",
                "semantic-refresh DAG projection is outside its exact deployment",
            )
        files[filename] = str(artifact_sha)
    return tuple(sorted(files.items()))


def require_inventory_budget(
    files: tuple[ArtifactFile, ...],
    *,
    max_object_bytes: int,
    max_total_bytes: int,
) -> None:
    total = 0
    for item in files:
        if item.size_bytes > max_object_bytes:
            raise AirflowArtifactDeliveryError(
                "DPONE_ARTIFACT_REGISTRY_OBJECT_TOO_LARGE",
                "artifact exceeds the configured per-object limit",
                details={"size_bytes": item.size_bytes, "max_object_bytes": max_object_bytes},
            )
        total += item.size_bytes
        if total > max_total_bytes:
            raise AirflowArtifactDeliveryError(
                "DPONE_ARTIFACT_REGISTRY_TOTAL_LIMIT_EXCEEDED",
                "artifact inventory exceeds the configured total limit",
                details={"total_bytes": total, "max_total_bytes": max_total_bytes},
            )


__all__ = [
    "build_publication_commitment",
    "build_publish_inventory",
    "declared_release_artifacts",
    "deployment_file_names",
    "semantic_refresh_deployment_files",
    "require_inventory_budget",
]
