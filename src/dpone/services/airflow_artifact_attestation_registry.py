"""Immutable registry package for one signed Airflow deployment statement."""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from dpone.contracts.airflow_artifact_attestation import (
    AirflowArtifactAttestation,
    AirflowArtifactAttestationPackage,
    AirflowArtifactAttestationVerification,
    sha256_bytes,
)
from dpone.contracts.airflow_artifact_attestation_errors import (
    AirflowArtifactAttestationRegistryError,
)
from dpone.contracts.airflow_deployment import is_canonical_sha256_digest
from dpone.ports.artifact_registry import (
    ArtifactRegistry,
    ArtifactRegistryError,
    ArtifactRegistryObjectNotFound,
    ArtifactRegistryReader,
    ArtifactRegistryReadLimitExceeded,
)

_MARKER_SCHEMA = "dpone.airflow-artifact-attestation-marker.v1"
_PUBLISH_SCHEMA = "dpone.airflow-artifact-attestation-publish.v1"
_STATEMENT_NAME = "artifact-attestation.json"
_BUNDLE_NAME = "artifact-attestation.sigstore.json"
_MARKER_NAME = "_SUCCESS"
_MAX_STATEMENT_BYTES = 256 * 1024
_MAX_BUNDLE_BYTES = 2 * 1024 * 1024
_MAX_MARKER_BYTES = 64 * 1024


@dataclass(frozen=True, slots=True)
class AirflowArtifactAttestationPublishReceipt:
    attestation_id: str
    environment: str
    deployment_id: str
    created_objects: int
    existing_equal_objects: int
    verified_objects: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": _PUBLISH_SCHEMA,
            "status": "published" if self.created_objects else "already_published",
            "attestation_id": self.attestation_id,
            "environment": self.environment,
            "deployment_id": self.deployment_id,
            "created_objects": self.created_objects,
            "existing_equal_objects": self.existing_equal_objects,
            "verified_objects": self.verified_objects,
            "errors": [],
        }


class AirflowArtifactAttestationRegistryReader:
    """Fetch one complete package from a deterministic deployment prefix."""

    def __init__(self, registry: ArtifactRegistryReader) -> None:
        self._registry = registry

    def fetch(self, *, environment: str, deployment_id: str) -> AirflowArtifactAttestationPackage:
        prefix = _prefix(environment, deployment_id)
        try:
            marker_bytes = self._download(prefix / _MARKER_NAME, _MAX_MARKER_BYTES)
        except ArtifactRegistryObjectNotFound as exc:
            raise AirflowArtifactAttestationRegistryError(
                "DPONE_ARTIFACT_ATTESTATION_NOT_FOUND",
                "artifact attestation completion marker is missing",
            ) from exc
        marker = _parse_marker(marker_bytes, deployment_id=deployment_id)
        try:
            statement = self._download(
                prefix / _STATEMENT_NAME,
                int(marker["attestation_bytes"]),
            )
            bundle = self._download(
                prefix / _BUNDLE_NAME,
                int(marker["sigstore_bundle_bytes"]),
            )
        except ArtifactRegistryObjectNotFound as exc:
            raise AirflowArtifactAttestationRegistryError(
                "DPONE_ARTIFACT_ATTESTATION_INVALID",
                "artifact attestation package is partial",
            ) from exc
        except ArtifactRegistryReadLimitExceeded as exc:
            raise AirflowArtifactAttestationRegistryError(
                "DPONE_ARTIFACT_ATTESTATION_INVALID",
                "artifact attestation object exceeds its committed size",
            ) from exc
        if (
            sha256_bytes(statement) != marker["attestation_sha256"]
            or sha256_bytes(bundle) != marker["sigstore_bundle_sha256"]
        ):
            raise AirflowArtifactAttestationRegistryError(
                "DPONE_ARTIFACT_ATTESTATION_INVALID",
                "artifact attestation package digest does not match its marker",
            )
        package = AirflowArtifactAttestationPackage(statement, bundle)
        if AirflowArtifactAttestation.from_bytes(statement).attestation_id != marker["attestation_id"]:
            raise AirflowArtifactAttestationRegistryError(
                "DPONE_ARTIFACT_ATTESTATION_INVALID",
                "artifact attestation identity does not match its marker",
            )
        return package

    def _download(self, key: PurePosixPath, max_bytes: int) -> bytes:
        metadata = self._registry.stat(key)
        if metadata.size_bytes < 0 or metadata.size_bytes > max_bytes:
            raise AirflowArtifactAttestationRegistryError(
                "DPONE_ARTIFACT_ATTESTATION_INVALID",
                "artifact attestation object size is invalid",
            )
        with tempfile.TemporaryDirectory(prefix=".dpone-attestation-fetch-") as directory:
            destination = Path(directory) / "object"
            self._registry.download_file(key, destination, max_bytes=max_bytes)
            value = destination.read_bytes()
        if len(value) != metadata.size_bytes:
            raise AirflowArtifactAttestationRegistryError(
                "DPONE_ARTIFACT_ATTESTATION_INVALID",
                "artifact attestation object size changed during download",
            )
        return value


class AirflowArtifactAttestationRegistryPublisher:
    """Create and read back a verified package, committing its marker last."""

    def __init__(self, registry: ArtifactRegistry) -> None:
        self._registry = registry

    def publish(
        self,
        *,
        environment: str,
        deployment_id: str,
        package: AirflowArtifactAttestationPackage,
        verification: AirflowArtifactAttestationVerification,
    ) -> AirflowArtifactAttestationPublishReceipt:
        statement = AirflowArtifactAttestation.from_bytes(package.attestation)
        if not verification.is_verified or verification.attestation_id != statement.attestation_id:
            raise AirflowArtifactAttestationRegistryError(
                "DPONE_ARTIFACT_ATTESTATION_INVALID",
                "only a locally verified artifact attestation can be published",
            )
        prefix = _prefix(environment, deployment_id)
        marker = _marker_bytes(deployment_id=deployment_id, package=package, statement=statement)
        created = 0
        existing = 0
        verified = 0
        for name, raw in (
            (_STATEMENT_NAME, package.attestation),
            (_BUNDLE_NAME, package.sigstore_bundle),
            (_MARKER_NAME, marker),
        ):
            was_created = self._create_or_compare(prefix / name, raw)
            created += int(was_created)
            existing += int(not was_created)
            verified += 1
        return AirflowArtifactAttestationPublishReceipt(
            attestation_id=statement.attestation_id,
            environment=environment,
            deployment_id=deployment_id,
            created_objects=created,
            existing_equal_objects=existing,
            verified_objects=verified,
        )

    def _create_or_compare(self, key: PurePosixPath, raw: bytes) -> bool:
        with tempfile.TemporaryDirectory(prefix=".dpone-attestation-publish-") as directory:
            source = Path(directory) / "object"
            descriptor = os.open(source, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(raw)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                result = self._registry.create_file(key, source)
                readback = AirflowArtifactAttestationRegistryReader(self._registry)._download(key, len(raw))
            except ArtifactRegistryError as exc:
                raise AirflowArtifactAttestationRegistryError(
                    "DPONE_ARTIFACT_ATTESTATION_REGISTRY_UNAVAILABLE",
                    "artifact attestation registry operation failed",
                ) from exc
        if readback != raw:
            raise AirflowArtifactAttestationRegistryError(
                "DPONE_ARTIFACT_ATTESTATION_IMMUTABILITY_CONFLICT",
                "existing artifact attestation object differs from intended bytes",
            )
        return result.created


def _prefix(environment: str, deployment_id: str) -> PurePosixPath:
    if (
        not environment
        or len(environment) > 63
        or not all(char.islower() or char.isdigit() or char in "_-" for char in environment)
        or not is_canonical_sha256_digest(deployment_id)
    ):
        raise ValueError("artifact attestation deployment identity is invalid")
    return PurePosixPath(
        "attestations",
        "deployments",
        environment,
        deployment_id.replace(":", "-", 1),
    )


def _marker_bytes(
    *,
    deployment_id: str,
    package: AirflowArtifactAttestationPackage,
    statement: AirflowArtifactAttestation,
) -> bytes:
    payload = {
        "schema": _MARKER_SCHEMA,
        "deployment_id": deployment_id,
        "attestation_id": statement.attestation_id,
        "attestation_sha256": sha256_bytes(package.attestation),
        "attestation_bytes": len(package.attestation),
        "sigstore_bundle_sha256": sha256_bytes(package.sigstore_bundle),
        "sigstore_bundle_bytes": len(package.sigstore_bundle),
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()


def _parse_marker(raw: bytes, *, deployment_id: str) -> dict[str, Any]:
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AirflowArtifactAttestationRegistryError(
            "DPONE_ARTIFACT_ATTESTATION_INVALID",
            "artifact attestation marker is invalid",
        ) from exc
    required = {
        "schema",
        "deployment_id",
        "attestation_id",
        "attestation_sha256",
        "attestation_bytes",
        "sigstore_bundle_sha256",
        "sigstore_bundle_bytes",
    }
    if (
        not isinstance(payload, dict)
        or set(payload) != required
        or payload.get("schema") != _MARKER_SCHEMA
        or payload.get("deployment_id") != deployment_id
        or raw != json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
    ):
        raise AirflowArtifactAttestationRegistryError(
            "DPONE_ARTIFACT_ATTESTATION_INVALID",
            "artifact attestation marker contract is invalid",
        )
    for digest_field in ("attestation_id", "attestation_sha256", "sigstore_bundle_sha256"):
        if not is_canonical_sha256_digest(payload.get(digest_field)):
            raise AirflowArtifactAttestationRegistryError(
                "DPONE_ARTIFACT_ATTESTATION_INVALID",
                "artifact attestation marker digest is invalid",
            )
    for size_field, maximum in (
        ("attestation_bytes", _MAX_STATEMENT_BYTES),
        ("sigstore_bundle_bytes", _MAX_BUNDLE_BYTES),
    ):
        size = payload.get(size_field)
        if isinstance(size, bool) or not isinstance(size, int) or not 1 <= size <= maximum:
            raise AirflowArtifactAttestationRegistryError(
                "DPONE_ARTIFACT_ATTESTATION_INVALID",
                "artifact attestation marker size is invalid",
            )
    return payload


__all__ = [
    "AirflowArtifactAttestationPackage",
    "AirflowArtifactAttestationPublishReceipt",
    "AirflowArtifactAttestationRegistryError",
    "AirflowArtifactAttestationRegistryPublisher",
    "AirflowArtifactAttestationRegistryReader",
]
