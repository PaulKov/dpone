"""Validated immutable contracts for runtime init-fetch delivery."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import PurePosixPath
from types import MappingProxyType
from typing import Any

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest
from dpone.contracts.runtime_artifact_delivery import is_pinned_artifact_registry_ref

_CACHE_SCHEME = "cache://"
_ALLOWED_ATTESTATIONS = frozenset({"optional", "required_for_prod"})
_IDENTITY_KEYS = frozenset({"method", "service_account"})
_VERIFY_KEYS = frozenset({"checksums", "attestations"})
_MUTABLE_PATH_PARTS = frozenset({"current", "latest"})
_RUNTIME_ARTIFACT_ROOT = "$RUNTIME_ARTIFACT_ROOT"


class InitFetchError(RuntimeError):
    """Stable runtime init-fetch failure with an optional logical artifact ref."""

    def __init__(self, code: str, message: str, *, artifact_ref: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.artifact_ref = artifact_ref


@dataclass(frozen=True)
class RuntimeArtifactRef:
    """One exact release artifact required by a runtime init-fetch plan."""

    id: str
    artifact_ref: str
    sha256: str
    bytes: int

    def __post_init__(self) -> None:
        _validate_artifact(self)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "artifact_ref": self.artifact_ref,
            "sha256": self.sha256,
            "bytes": self.bytes,
        }


@dataclass(frozen=True)
class InitFetchPlan:
    """A fully validated, defensively immutable init-fetch execution plan."""

    release_id: str
    deployment_id: str
    artifact_registry_ref: str
    artifacts: tuple[RuntimeArtifactRef, ...]
    identity: Mapping[str, str]
    verify: Mapping[str, str]

    def __post_init__(self) -> None:
        artifacts = tuple(self.artifacts)
        identity = _normalized_identity(self.identity)
        verify = _normalized_verify(self.verify)
        _validate_plan_values(
            release_id=self.release_id,
            deployment_id=self.deployment_id,
            artifact_registry_ref=self.artifact_registry_ref,
            artifacts=artifacts,
        )
        object.__setattr__(self, "artifacts", artifacts)
        object.__setattr__(self, "identity", MappingProxyType(identity))
        object.__setattr__(self, "verify", MappingProxyType(verify))

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": "init_fetch",
            "release_id": self.release_id,
            "deployment_id": self.deployment_id,
            "artifact_registry_ref": self.artifact_registry_ref,
            "identity": dict(self.identity),
            "verify": dict(self.verify),
            "artifacts": [artifact.to_dict() for artifact in self.artifacts],
        }


@dataclass(frozen=True)
class InitFetchedArtifact:
    """Verified artifact metadata emitted without a machine-local path."""

    id: str
    artifact_ref: str
    sha256: str
    path: str
    bytes: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "artifact_ref": self.artifact_ref,
            "sha256": self.sha256,
            "path": self.path,
            "bytes": self.bytes,
        }


@dataclass(frozen=True)
class InitFetchResult:
    """Successful init-fetch publication evidence."""

    passed: bool
    release_id: str
    deployment_id: str
    artifact_registry_ref: str
    artifacts: tuple[InitFetchedArtifact, ...]
    manifest_path: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "dpone.init-fetch-result.v1",
            "passed": self.passed,
            "release_id": self.release_id,
            "deployment_id": self.deployment_id,
            "artifact_registry_ref": self.artifact_registry_ref,
            "artifacts": [artifact.to_dict() for artifact in self.artifacts],
            "manifest_path": self.manifest_path,
        }


def build_init_fetch_plan(
    *,
    release_id: str,
    deployment_id: str,
    artifact_registry_ref: str,
    workload_packs: Sequence[Mapping[str, Any]],
    service_account: str = "dpone-runtime",
    checksums: str = "required",
    attestations: str = "required_for_prod",
) -> InitFetchPlan:
    """Build the canonical immutable plan from deployment workload entries."""

    return InitFetchPlan(
        release_id=release_id,
        deployment_id=deployment_id,
        artifact_registry_ref=artifact_registry_ref,
        identity={
            "method": "kubernetes_workload_identity",
            "service_account": service_account,
        },
        verify={"checksums": checksums, "attestations": attestations},
        artifacts=tuple(_artifact(item) for item in workload_packs),
    )


def validate_init_fetch_plan(plan: InitFetchPlan) -> InitFetchPlan:
    """Return a fresh canonical copy, detecting post-construction tampering."""

    if not isinstance(plan, InitFetchPlan):
        raise TypeError("plan must be an InitFetchPlan")
    return InitFetchPlan(
        release_id=plan.release_id,
        deployment_id=plan.deployment_id,
        artifact_registry_ref=plan.artifact_registry_ref,
        artifacts=plan.artifacts,
        identity=plan.identity,
        verify=plan.verify,
    )


def cache_relative_path(artifact_ref: str) -> PurePosixPath:
    """Validate one immutable cache locator and return its registry key."""

    if not isinstance(artifact_ref, str) or not artifact_ref.startswith(_CACHE_SCHEME):
        raise InitFetchError(
            "DPONE_CACHE_REFERENCE_INVALID",
            "artifact_ref must use cache://",
            artifact_ref=artifact_ref if isinstance(artifact_ref, str) else None,
        )
    raw_path = artifact_ref[len(_CACHE_SCHEME) :]
    relative = PurePosixPath(raw_path)
    if (
        not raw_path
        or "\\" in raw_path
        or "?" in raw_path
        or "#" in raw_path
        or "%" in raw_path
        or relative.is_absolute()
        or relative.as_posix() != raw_path
        or any(part in {"", ".", ".."} for part in relative.parts)
    ):
        raise InitFetchError(
            "DPONE_CACHE_PATH_ESCAPE",
            "cache artifact path is unsafe",
            artifact_ref=artifact_ref,
        )
    if any(part.casefold() in _MUTABLE_PATH_PARTS for part in relative.parts):
        raise InitFetchError(
            "DPONE_CACHE_UNPINNED_REFERENCE",
            "runtime init_fetch must not resolve current/latest aliases",
            artifact_ref=artifact_ref,
        )
    return relative


def runtime_artifact_locator(relative: PurePosixPath) -> str:
    """Return a topology-free runtime artifact location for evidence."""

    return f"{_RUNTIME_ARTIFACT_ROOT}/{relative.as_posix()}"


def json_bytes(payload: Mapping[str, Any]) -> bytes:
    """Serialize deterministic UTF-8 JSON evidence."""

    return (json.dumps(dict(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode()


def _artifact(item: Mapping[str, Any]) -> RuntimeArtifactRef:
    if not isinstance(item, Mapping):
        raise ValueError("workload pack must be a mapping")
    return RuntimeArtifactRef(
        id=str(item.get("id") or ""),
        artifact_ref=str(item.get("artifact_ref") or ""),
        sha256=str(item.get("sha256") or ""),
        bytes=_require_positive_declared_bytes(item.get("bytes")),
    )


def _validate_plan_values(
    *,
    release_id: str,
    deployment_id: str,
    artifact_registry_ref: str,
    artifacts: tuple[RuntimeArtifactRef, ...],
) -> None:
    _require_digest("release_id", release_id)
    _require_digest("deployment_id", deployment_id)
    if not is_pinned_artifact_registry_ref(artifact_registry_ref):
        raise ValueError("artifact_registry_ref must not contain current/latest aliases")
    release_prefix = ("releases", release_id.replace(":", "-"))
    ids: set[str] = set()
    paths: set[PurePosixPath] = set()
    for artifact in artifacts:
        if not isinstance(artifact, RuntimeArtifactRef):
            raise ValueError("artifacts must contain RuntimeArtifactRef values")
        relative = _validate_artifact(artifact)
        if len(relative.parts) <= len(release_prefix) or relative.parts[:2] != release_prefix:
            raise ValueError("artifact_ref must belong to the plan release_id")
        if artifact.id in ids:
            raise ValueError("init-fetch plan contains duplicate artifact ids")
        if relative in paths:
            raise ValueError("init-fetch plan contains duplicate artifact paths")
        ids.add(artifact.id)
        paths.add(relative)


def _validate_artifact(artifact: RuntimeArtifactRef) -> PurePosixPath:
    if not isinstance(artifact.id, str) or not artifact.id.strip():
        raise ValueError("artifact id is required")
    try:
        relative = cache_relative_path(artifact.artifact_ref)
    except InitFetchError as exc:
        raise ValueError(str(exc)) from exc
    _require_digest("sha256", artifact.sha256)
    _require_positive_declared_bytes(artifact.bytes)
    return relative


def _normalized_identity(value: Mapping[str, str]) -> dict[str, str]:
    normalized = _normalized_string_mapping("identity", value, required_keys=_IDENTITY_KEYS)
    if normalized["method"] != "kubernetes_workload_identity":
        raise ValueError("identity.method must be kubernetes_workload_identity")
    if not normalized["service_account"].strip():
        raise ValueError("identity.service_account is required")
    return normalized


def _normalized_verify(value: Mapping[str, str]) -> dict[str, str]:
    normalized = _normalized_string_mapping("verify", value, required_keys=_VERIFY_KEYS)
    if normalized["checksums"] != "required":
        raise ValueError("checksums must be required")
    if normalized["attestations"] not in _ALLOWED_ATTESTATIONS:
        raise ValueError("attestations must be optional or required_for_prod")
    return normalized


def _normalized_string_mapping(
    name: str,
    value: Mapping[str, str],
    *,
    required_keys: frozenset[str],
) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be a mapping")
    copied = dict(value)
    if set(copied) != required_keys or any(not isinstance(item, str) for item in copied.values()):
        raise ValueError(f"{name} must contain exactly {sorted(required_keys)} string fields")
    return copied


def _require_positive_declared_bytes(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError("artifact bytes must be a positive integer")
    return value


def _require_digest(name: str, value: str) -> None:
    if not is_canonical_sha256_digest(value):
        raise ValueError(f"{name} must be a canonical sha256 digest")


__all__ = [
    "build_init_fetch_plan",
    "cache_relative_path",
    "InitFetchedArtifact",
    "InitFetchError",
    "InitFetchPlan",
    "InitFetchResult",
    "json_bytes",
    "runtime_artifact_locator",
    "RuntimeArtifactRef",
    "validate_init_fetch_plan",
]
