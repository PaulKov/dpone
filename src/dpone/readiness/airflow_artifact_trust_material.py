"""Load mounted Airflow artifact trust material at a composition boundary."""

from __future__ import annotations

import json
import os
from pathlib import Path

from dpone.contracts.airflow_artifact_attestation import sha256_bytes
from dpone.contracts.airflow_deployment_trust_policy import AirflowDeploymentTrustPolicy
from dpone.contracts.runtime_artifact_attestation import (
    RUNTIME_ARTIFACT_TRUST_POLICY_V1,
    RUNTIME_ARTIFACT_TRUST_POLICY_V2,
)
from dpone.runtime.deployment_cache_common import (
    DeploymentCacheError,
    open_regular_file,
    resolve_mount_projected_regular_file,
)
from dpone.services.airflow_artifact_attestation_consumer import (
    AirflowArtifactTrustMaterial,
)

_MAX_PUBLIC_KEY_BYTES = 64 * 1024


class AirflowArtifactTrustMaterialError(ValueError):
    """Raised without leaking filesystem or key contents."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def load_airflow_artifact_trust_material(
    *,
    policy_bytes: bytes,
    key_root: Path,
) -> AirflowArtifactTrustMaterial:
    """Parse one policy snapshot and load only its allowlisted public keys."""

    try:
        policy = AirflowDeploymentTrustPolicy.from_bytes(policy_bytes)
        public_keys = {
            item.key_id: _read_projected_key(key_root / item.file, root=key_root) for item in policy.trusted_public_keys
        }
    except (DeploymentCacheError, OSError, ValueError) as exc:
        raise AirflowArtifactTrustMaterialError(
            "DPONE_ARTIFACT_ATTESTATION_POLICY_INVALID",
            "artifact trust policy or public-key material is invalid",
        ) from exc
    for item in policy.trusted_public_keys:
        if sha256_bytes(public_keys[item.key_id]) != item.sha256:
            raise AirflowArtifactTrustMaterialError(
                "DPONE_ARTIFACT_ATTESTATION_KEY_MISMATCH",
                "artifact trust public-key digest does not match policy",
            )
    return AirflowArtifactTrustMaterial(
        policy=policy,
        policy_sha256=sha256_bytes(policy_bytes),
        public_keys=public_keys,
    )


def load_airflow_artifact_trust_material_from_files(
    *,
    policy_path: Path,
    key_root: Path,
) -> AirflowArtifactTrustMaterial:
    """Snapshot one projected policy and its allowlisted public keys."""

    try:
        policy_bytes = _read_projected_file(
            policy_path,
            root=policy_path.parent,
            maximum=64 * 1024,
            label="artifact trust policy",
        )
    except (DeploymentCacheError, OSError, ValueError) as exc:
        raise AirflowArtifactTrustMaterialError(
            "DPONE_ARTIFACT_ATTESTATION_POLICY_INVALID",
            "artifact trust policy is invalid",
        ) from exc
    return load_airflow_artifact_trust_material(
        policy_bytes=policy_bytes,
        key_root=key_root,
    )


def load_optional_airflow_deployment_trust_material_from_files(
    *,
    policy_path: Path,
    key_root: Path,
) -> AirflowArtifactTrustMaterial | None:
    """Load Cosign deployment material while preserving GitHub/SLSA policies."""

    try:
        policy_bytes = _read_projected_file(
            policy_path,
            root=policy_path.parent,
            maximum=64 * 1024,
            label="artifact trust policy",
        )
        payload = json.loads(
            policy_bytes.decode("utf-8"),
            object_pairs_hook=_unique_policy_object,
        )
    except (
        DeploymentCacheError,
        OSError,
        UnicodeDecodeError,
        json.JSONDecodeError,
        ValueError,
    ) as exc:
        raise AirflowArtifactTrustMaterialError(
            "DPONE_ARTIFACT_ATTESTATION_POLICY_INVALID",
            "artifact trust policy is invalid",
        ) from exc
    if not isinstance(payload, dict):
        raise AirflowArtifactTrustMaterialError(
            "DPONE_ARTIFACT_ATTESTATION_POLICY_INVALID",
            "artifact trust policy must be an object",
        )
    if payload.get("schema") in {
        RUNTIME_ARTIFACT_TRUST_POLICY_V1,
        RUNTIME_ARTIFACT_TRUST_POLICY_V2,
    }:
        return None
    return load_airflow_artifact_trust_material(
        policy_bytes=policy_bytes,
        key_root=key_root,
    )


def _unique_policy_object(items: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in items:
        if key in value:
            raise ValueError("duplicate artifact trust policy key")
        value[key] = item
    return value


def _read_projected_key(path: Path, *, root: Path) -> bytes:
    return _read_projected_file(
        path,
        root=root,
        maximum=_MAX_PUBLIC_KEY_BYTES,
        label="artifact trust public key",
    )


def _read_projected_file(
    path: Path,
    *,
    root: Path,
    maximum: int,
    label: str,
) -> bytes:
    resolved = resolve_mount_projected_regular_file(path)
    descriptor = open_regular_file(
        resolved,
        missing_code="DPONE_ARTIFACT_TRUST_KEY_INVALID",
        invalid_code="DPONE_ARTIFACT_TRUST_KEY_INVALID",
        label=label,
        root=root.resolve(strict=False),
    )
    try:
        chunks: list[bytes] = []
        total = 0
        while total <= maximum:
            chunk = os.read(descriptor, min(16 * 1024, maximum + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
    finally:
        os.close(descriptor)
    value = b"".join(chunks)
    if not value or len(value) > maximum:
        raise AirflowArtifactTrustMaterialError(
            "DPONE_ARTIFACT_ATTESTATION_POLICY_INVALID",
            f"{label} size is invalid",
        )
    return value


__all__ = [
    "AirflowArtifactTrustMaterialError",
    "load_airflow_artifact_trust_material",
    "load_airflow_artifact_trust_material_from_files",
    "load_optional_airflow_deployment_trust_material_from_files",
]
