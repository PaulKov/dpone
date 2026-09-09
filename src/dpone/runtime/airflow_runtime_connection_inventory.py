"""Immutable runtime-connection files included in Airflow publication."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from dpone.contracts.airflow_deployment import canonical_fingerprint, is_canonical_sha256_digest
from dpone.runtime.airflow_artifact_delivery_models import (
    AirflowArtifactDeliveryError,
)
from dpone.runtime.deployment_cache_common import (
    read_regular_json_object,
    regular_file_identity,
)
from dpone.runtime.init_fetch_contract import InitFetchError, cache_relative_path

RUNTIME_CONNECTION_FILES = {
    "binding_set": (
        "binding_set_ref",
        "binding-set.json",
        "binding-set.json",
        "dpone.binding-set.v1",
    ),
    "connection_registry": (
        "connection_registry_ref",
        "connection-registry.ref",
        "connection-registry.json",
        "dpone.connection-registry.v1",
    ),
    "credential_runtime": (
        "credential_runtime_ref",
        "credential-runtime.ref",
        "credential-runtime.json",
        "dpone.credential-runtime.v1",
    ),
}


@dataclass(frozen=True, slots=True)
class RuntimeConnectionPublicationSpec:
    key: PurePosixPath
    path: Path
    source_root: Path


def validate_deployment_auxiliary_files(
    deployment: Mapping[str, Any],
    deployment_dir: Path,
) -> None:
    """Validate environment-specific files against deployment authority."""

    if deployment.get("schema") in {"dpone.deployment-set.v2", "dpone.deployment-set.v3"}:
        _validate_strict_runtime_connection_files(deployment, deployment_dir)
        return
    binding_ref = deployment.get("binding_set_ref")
    if binding_ref is not None:
        binding = read_regular_json_object(
            deployment_dir / "binding-set.json",
            missing_code="DPONE_ARTIFACT_REGISTRY_INCOMPLETE",
            invalid_code="DPONE_BINDING_SET_INVALID",
            label="binding-set",
            root=deployment_dir.parent.parent.parent,
        )
        if canonical_fingerprint(binding) != binding_ref:
            raise AirflowArtifactDeliveryError(
                "DPONE_DEPLOYMENT_FINGERPRINT_MISMATCH",
                "binding-set content does not match deployment reference",
            )
    _require_ref_file(deployment, deployment_dir, "connection_registry_ref", "connection-registry.ref")
    _require_ref_file(deployment, deployment_dir, "credential_runtime_ref", "credential-runtime.ref")


def runtime_connection_publication_files(
    deployment: Mapping[str, Any],
    *,
    deployment_dir: Path,
    root: Path,
) -> list[RuntimeConnectionPublicationSpec]:
    """Return strict runtime-connection snapshots under their immutable context."""

    return [
        RuntimeConnectionPublicationSpec(
            key=cache_relative_path(
                str(
                    _runtime_connection_descriptor(
                        deployment,
                        artifact_field=artifact_field,
                        remote_name=remote_name,
                    )["artifact_ref"]
                )
            ),
            path=deployment_dir / local_name,
            source_root=root,
        )
        for artifact_field, (_, local_name, remote_name, _) in RUNTIME_CONNECTION_FILES.items()
    ]


def _validate_strict_runtime_connection_files(
    deployment: Mapping[str, Any],
    deployment_dir: Path,
) -> None:
    expected_environment = deployment.get("environment")
    parents: set[PurePosixPath] = set()
    for artifact_field, (
        fingerprint_field,
        local_name,
        remote_name,
        schema,
    ) in RUNTIME_CONNECTION_FILES.items():
        descriptor = _runtime_connection_descriptor(
            deployment,
            artifact_field=artifact_field,
            remote_name=remote_name,
        )
        key = cache_relative_path(str(descriptor["artifact_ref"]))
        parents.add(key.parent)
        payload = read_regular_json_object(
            deployment_dir / local_name,
            missing_code="DPONE_ARTIFACT_REGISTRY_INCOMPLETE",
            invalid_code="DPONE_RUNTIME_CONNECTION_ARTIFACT_INVALID",
            label=artifact_field.replace("_", "-"),
            root=deployment_dir.parent.parent.parent,
        )
        if (
            payload.get("schema") != schema
            or payload.get("environment") != expected_environment
            or canonical_fingerprint(payload) != deployment.get(fingerprint_field)
        ):
            raise AirflowArtifactDeliveryError(
                "DPONE_DEPLOYMENT_FINGERPRINT_MISMATCH",
                "runtime connection snapshot does not match deployment authority",
            )
        identity = regular_file_identity(
            path=deployment_dir / local_name,
            root=deployment_dir.parent.parent.parent,
            missing_code="DPONE_ARTIFACT_REGISTRY_INCOMPLETE",
            invalid_code="DPONE_ARTIFACT_REGISTRY_INCOMPLETE",
            label="runtime connection snapshot",
        )
        if descriptor["sha256"] != identity.sha256 or descriptor["bytes"] != identity.size_bytes:
            raise AirflowArtifactDeliveryError(
                "DPONE_DEPLOYMENT_FINGERPRINT_MISMATCH",
                "runtime connection snapshot does not match its exact descriptor",
            )
    if len(parents) != 1:
        raise AirflowArtifactDeliveryError(
            "DPONE_DEPLOYMENT_FINGERPRINT_MISMATCH",
            "runtime connection snapshots do not share one immutable context",
        )


def _runtime_connection_descriptor(
    deployment: Mapping[str, Any],
    *,
    artifact_field: str,
    remote_name: str,
) -> Mapping[str, Any]:
    descriptor = deployment.get(artifact_field)
    if (
        not isinstance(descriptor, Mapping)
        or set(descriptor) != {"artifact_ref", "sha256", "bytes"}
        or not is_canonical_sha256_digest(descriptor.get("sha256"))
        or isinstance(descriptor.get("bytes"), bool)
        or not isinstance(descriptor.get("bytes"), int)
        or int(descriptor["bytes"]) <= 0
    ):
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_REGISTRY_INCOMPLETE",
            "runtime connection artifact descriptor is invalid",
        )
    try:
        key = cache_relative_path(str(descriptor.get("artifact_ref") or ""))
    except InitFetchError as exc:
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_REGISTRY_INCOMPLETE",
            "runtime connection artifact reference is invalid",
        ) from exc
    if (
        len(key.parts) != 3
        or key.parts[0] != "runtime-connection-contexts"
        or not is_canonical_sha256_digest(key.parts[1].replace("-", ":", 1))
        or key.name != remote_name
    ):
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_REGISTRY_INCOMPLETE",
            "runtime connection artifact is outside its immutable context",
        )
    return descriptor


def _require_ref_file(
    deployment: Mapping[str, Any],
    deployment_dir: Path,
    field: str,
    filename: str,
) -> None:
    expected = deployment.get(field)
    if expected is None:
        return
    try:
        actual = (deployment_dir / filename).read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_REGISTRY_INCOMPLETE",
            f"required {filename} is unavailable",
        ) from exc
    if actual != expected:
        raise AirflowArtifactDeliveryError(
            "DPONE_DEPLOYMENT_FINGERPRINT_MISMATCH",
            f"{filename} does not match deployment reference",
        )


__all__ = [
    "RuntimeConnectionPublicationSpec",
    "runtime_connection_publication_files",
    "validate_deployment_auxiliary_files",
]
