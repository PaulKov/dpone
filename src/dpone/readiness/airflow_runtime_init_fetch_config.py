"""Trusted runtime configuration snapshots for Airflow init-fetch."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.contracts.airflow_deployment_trust_policy import (
    POLICY_SCHEMA as AIRFLOW_DEPLOYMENT_TRUST_POLICY_SCHEMA,
)
from dpone.contracts.airflow_deployment_trust_policy import (
    AirflowDeploymentTrustPolicy,
)
from dpone.contracts.runtime_artifact_attestation import (
    RUNTIME_ARTIFACT_TRUST_POLICY_V1,
    RUNTIME_ARTIFACT_TRUST_POLICY_V2,
    RuntimeArtifactTrustPolicy,
    RuntimeArtifactTrustPolicyError,
    parse_runtime_artifact_trust_policy,
)
from dpone.ports.artifact_registry import ArtifactRegistryReader
from dpone.ports.runtime_artifact_attestation import (
    RuntimeArtifactAttestationVerifier,
)
from dpone.runtime.deployment_cache_common import (
    DeploymentCacheError,
    open_regular_file,
    resolve_mount_projected_regular_file,
)
from dpone.runtime.init_fetch_contract import InitFetchError
from dpone.runtime.runtime_init_fetch_plan import RuntimeInitFetchPlan

DEFAULT_REGISTRY_CONFIG_PATH = Path("/etc/dpone/artifact-registry/registry.json")
DEFAULT_TRUST_POLICY_PATH = Path("/etc/dpone/artifact-trust/policy.json")
_MAX_CONFIG_BYTES = 64 * 1024
_MAX_TRUST_POLICY_BYTES = 1024 * 1024


@dataclass(frozen=True, slots=True)
class RuntimeRegistryConfiguration:
    """One trusted logical artifact registry configuration."""

    logical_ref: str
    registry_uri: str
    access_mode: str


@dataclass(frozen=True, slots=True)
class RuntimeAttestationAuthority:
    """One plan-pinned trust authority selected from a verified snapshot."""

    requires_attestation: bool
    github_policy: RuntimeArtifactTrustPolicy | None = None
    deployment_policy_bytes: bytes | None = None

    @property
    def supports_stock_verification(self) -> bool:
        return self.deployment_policy_bytes is not None or (
            self.github_policy is not None and self.github_policy.verifier is not None
        )


def verified_snapshot(
    path: Path,
    *,
    expected_sha256: str,
    label: str,
    invalid_code: str,
    mismatch_code: str,
    max_bytes: int = _MAX_CONFIG_BYTES,
) -> bytes:
    """Read one bounded regular file and verify its pinned digest."""

    try:
        mount_root = path.parent.resolve(strict=False)
        resolved = resolve_mount_projected_regular_file(path)
    except FileNotFoundError as exc:
        raise InitFetchError(invalid_code, f"required {label} is missing") from exc
    except OSError as exc:
        raise InitFetchError(
            invalid_code,
            f"{label} must be a readable regular file under its mount directory",
        ) from exc
    try:
        descriptor = open_regular_file(
            resolved,
            missing_code=invalid_code,
            invalid_code=invalid_code,
            label=label,
            root=mount_root,
        )
    except DeploymentCacheError as exc:
        raise InitFetchError(exc.code, str(exc)) from exc
    chunks: list[bytes] = []
    total = 0
    try:
        while total <= max_bytes:
            chunk = os.read(
                descriptor,
                min(64 * 1024, max_bytes + 1 - total),
            )
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
    finally:
        os.close(descriptor)
    if total > max_bytes:
        raise InitFetchError(
            invalid_code,
            f"{label} exceeds the byte limit",
        )
    payload = b"".join(chunks)
    if "sha256:" + hashlib.sha256(payload).hexdigest() != expected_sha256:
        raise InitFetchError(
            mismatch_code,
            f"{label} checksum does not match the pinned reference",
        )
    return payload


def registry_configuration(
    payload: bytes,
    *,
    logical_ref: str,
) -> RuntimeRegistryConfiguration:
    """Resolve one exact logical registry from its trusted snapshot."""

    root = _json_object(
        payload,
        "artifact registry configuration",
        invalid_code="DPONE_ARTIFACT_REGISTRY_CONFIG_INVALID",
    )
    if set(root) != {"schema", "registries"} or root.get("schema") != "dpone.artifact-registry-runtime-config.v1":
        raise InitFetchError(
            "DPONE_ARTIFACT_REGISTRY_CONFIG_INVALID",
            "artifact registry configuration schema is invalid",
        )
    registries = root.get("registries")
    if not isinstance(registries, Mapping):
        raise _config_error("artifact registry configuration registries must be an object")
    item = registries.get(logical_ref)
    if not isinstance(item, Mapping) or set(item) != {"registry_uri", "access"}:
        raise _config_error("logical artifact registry configuration is missing or invalid")
    access = item.get("access")
    if not isinstance(access, Mapping) or set(access) != {"mode"}:
        raise _config_error("artifact registry access configuration is invalid")
    registry_uri = item.get("registry_uri")
    access_mode = access.get("mode")
    if not isinstance(registry_uri, str) or not registry_uri or not isinstance(access_mode, str):
        raise _config_error("artifact registry configuration contains an invalid value")
    return RuntimeRegistryConfiguration(
        logical_ref=logical_ref,
        registry_uri=registry_uri,
        access_mode=access_mode,
    )


def trusted_attestation_policy(
    plan: RuntimeInitFetchPlan,
    *,
    path: Path,
) -> RuntimeArtifactTrustPolicy:
    """Load the GitHub/SLSA policy contract retained for API compatibility."""

    authority = trusted_attestation_authority(plan, path=path)
    if authority.github_policy is None:
        raise InitFetchError(
            "DPONE_ARTIFACT_TRUST_POLICY_INVALID",
            "artifact trust policy is not a GitHub/SLSA runtime policy",
        )
    return authority.github_policy


def trusted_attestation_authority(
    plan: RuntimeInitFetchPlan,
    *,
    path: Path,
) -> RuntimeAttestationAuthority:
    """Load and classify one exact policy without overlapping authorities."""

    if plan.trust_policy_ref is None:
        policy = RuntimeArtifactTrustPolicy(
            schema=RUNTIME_ARTIFACT_TRUST_POLICY_V1,
            trust_tier=plan.trust_tier,
            attestations=("required_for_prod" if plan.trust_tier == "production" else "optional"),
            verifier=None,
        )
        return RuntimeAttestationAuthority(
            requires_attestation=policy.requires_attestation,
            github_policy=policy,
        )
    payload = verified_snapshot(
        path,
        expected_sha256=plan.trust_policy_ref["sha256"],
        label="artifact trust policy",
        invalid_code="DPONE_ARTIFACT_TRUST_POLICY_INVALID",
        mismatch_code="DPONE_ARTIFACT_TRUST_POLICY_MISMATCH",
        max_bytes=_MAX_TRUST_POLICY_BYTES,
    )
    policy = _json_object(
        payload,
        "artifact trust policy",
        invalid_code="DPONE_ARTIFACT_TRUST_POLICY_INVALID",
    )
    schema = policy.get("schema")
    if schema in {
        RUNTIME_ARTIFACT_TRUST_POLICY_V1,
        RUNTIME_ARTIFACT_TRUST_POLICY_V2,
    }:
        try:
            parsed = parse_runtime_artifact_trust_policy(policy)
        except RuntimeArtifactTrustPolicyError as exc:
            raise InitFetchError(exc.code, str(exc)) from exc
        if parsed.trust_tier != plan.trust_tier:
            raise InitFetchError(
                "DPONE_ARTIFACT_TRUST_POLICY_MISMATCH",
                "artifact trust policy does not match the pinned trust tier",
            )
        return RuntimeAttestationAuthority(
            requires_attestation=parsed.requires_attestation,
            github_policy=parsed,
        )
    if schema != AIRFLOW_DEPLOYMENT_TRUST_POLICY_SCHEMA:
        raise InitFetchError(
            "DPONE_ARTIFACT_TRUST_POLICY_INVALID",
            "artifact trust policy schema is unsupported",
        )
    if plan.trust_tier != "production":
        raise InitFetchError(
            "DPONE_ARTIFACT_TRUST_POLICY_MISMATCH",
            "Airflow deployment trust policy requires the production trust tier",
        )
    try:
        AirflowDeploymentTrustPolicy.from_bytes(payload)
    except ValueError as exc:
        raise InitFetchError(
            "DPONE_ARTIFACT_TRUST_POLICY_INVALID",
            "Airflow deployment trust policy is invalid",
        ) from exc
    return RuntimeAttestationAuthority(
        requires_attestation=True,
        deployment_policy_bytes=payload,
    )


def stock_attestation_verifier(
    registry: ArtifactRegistryReader,
    policy: RuntimeArtifactTrustPolicy,
) -> RuntimeArtifactAttestationVerifier | None:
    """Build the stock verifier only when trusted policy names one."""

    verifier_policy = policy.verifier
    if verifier_policy is None:
        return None
    from dpone.adapters.runtime_github_artifact_attestation import (
        RegistryGitHubArtifactAttestationVerifier,
    )

    return RegistryGitHubArtifactAttestationVerifier(
        registry=registry,
        policy=verifier_policy,
    )


def _json_object(
    payload: bytes,
    label: str,
    *,
    invalid_code: str,
) -> Mapping[str, Any]:
    try:
        value = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise InitFetchError(invalid_code, f"{label} is not valid JSON") from exc
    if not isinstance(value, Mapping):
        raise InitFetchError(invalid_code, f"{label} must be an object")
    return value


def _unique_object(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _config_error(message: str) -> InitFetchError:
    return InitFetchError(
        "DPONE_ARTIFACT_REGISTRY_CONFIG_INVALID",
        message,
    )


__all__ = [
    "DEFAULT_REGISTRY_CONFIG_PATH",
    "DEFAULT_TRUST_POLICY_PATH",
    "RuntimeAttestationAuthority",
    "RuntimeRegistryConfiguration",
    "registry_configuration",
    "stock_attestation_verifier",
    "trusted_attestation_authority",
    "trusted_attestation_policy",
    "verified_snapshot",
]
