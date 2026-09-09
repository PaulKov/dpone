"""Local-file projections for immutable Airflow artifact inventories."""

from __future__ import annotations

from pathlib import Path, PurePosixPath

from dpone.runtime.airflow_artifact_attestation_inventory import (
    MAX_ATTESTATION_BUNDLE_BYTES,
    AttestationPublicationSpec,
)
from dpone.runtime.airflow_artifact_delivery_models import AirflowArtifactDeliveryError, ArtifactFile
from dpone.runtime.airflow_runtime_connection_inventory import RuntimeConnectionPublicationSpec
from dpone.runtime.deployment_cache_common import DeploymentCacheError, regular_file_identity


def marker_last(files: list[ArtifactFile]) -> tuple[ArtifactFile, ...]:
    """Order the completion marker after every immutable artifact."""

    return tuple(sorted(files, key=lambda item: (item.completion_marker, item.key.as_posix())))


def local_artifact_file(
    *,
    key: PurePosixPath,
    path: Path,
    root: Path,
    completion_marker: bool = False,
) -> ArtifactFile:
    """Build one descriptor only after regular-file identity verification."""

    identity = regular_file_identity(
        path,
        root=root,
        missing_code="DPONE_ARTIFACT_REGISTRY_INCOMPLETE",
        invalid_code="DPONE_ARTIFACT_REGISTRY_INCOMPLETE",
        label="artifact delivery file",
    )
    return ArtifactFile(
        key=key,
        local_path=path,
        size_bytes=identity.size_bytes,
        sha256=identity.sha256,
        completion_marker=completion_marker,
        source_root=root,
    )


def runtime_connection_artifact(spec: RuntimeConnectionPublicationSpec) -> ArtifactFile:
    """Project one verified runtime-connection descriptor."""

    return local_artifact_file(key=spec.key, path=spec.path, root=spec.source_root)


def attestation_artifact(spec: AttestationPublicationSpec) -> ArtifactFile:
    """Project one bounded, non-empty attestation bundle."""

    try:
        artifact = local_artifact_file(key=spec.key, path=spec.path, root=spec.source_root)
    except DeploymentCacheError as exc:
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_ATTESTATION_BUNDLE_INVALID",
            "runtime artifact attestation bundle is missing or unsafe",
        ) from exc
    if artifact.size_bytes <= 0:
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_ATTESTATION_BUNDLE_INVALID",
            "runtime artifact attestation bundle is empty",
        )
    if artifact.size_bytes > MAX_ATTESTATION_BUNDLE_BYTES:
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_ATTESTATION_BUNDLE_TOO_LARGE",
            "runtime artifact attestation bundle exceeds its byte limit",
        )
    return artifact
