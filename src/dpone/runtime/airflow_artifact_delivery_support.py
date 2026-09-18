"""Shared fail-closed helpers for Airflow artifact delivery use cases."""

from __future__ import annotations

import hashlib
import inspect
import json
import os
import stat
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, Protocol, cast

from dpone.ports.artifact_registry import (
    ArtifactRegistry,
    ArtifactRegistryAuthority,
    ArtifactRegistryError,
    ArtifactRegistryKeyError,
    ArtifactRegistryObjectNotFound,
    ArtifactRegistryReader,
    ArtifactRegistryReadLimitExceeded,
)
from dpone.runtime.airflow_artifact_delivery_models import (
    AirflowArtifactDeliveryError,
    MaterializeRequest,
    canonical_digest_or_none,
)
from dpone.runtime.deployment_cache_common import DeploymentCacheError
from dpone.runtime.development_target_admission import (
    DevelopmentTargetAdmission,
    DevelopmentTargetAdmissionVerifier,
    DevelopmentTargetOperation,
)

_DEVELOPMENT_RELEASE_SCHEMA = "dpone.dbt-release-set.development.v1"
_DEVELOPMENT_COMPOSITION_PROFILE = "development_workspace_delivery_v1"


class DevelopmentDeliveryAuthority(Protocol):
    """Legacy delivery capability retained only for fail-closed API migration."""

    def release_projection(self) -> dict[str, Any]: ...

    def require_release_budget(self, *, workload_ids: tuple[str, ...], source_bytes: int) -> None: ...


def require_development_delivery_authority(
    release: Mapping[str, object],
    *,
    deployment: Mapping[str, object],
    admission: DevelopmentTargetAdmission | None,
    admission_verifier: DevelopmentTargetAdmissionVerifier | None,
    operation: DevelopmentTargetOperation,
    checked_at: datetime | None = None,
    clock: Callable[[], datetime] | None = None,
    source_bytes: int | None = None,
) -> None:
    """Require artifact scope plus exact current target-operation admission."""

    try:
        projection = development_authority_projection(release)
        if projection is None:
            return
        release_id = release.get("release_id")
        deployment_id = deployment.get("deployment_id")
        environment = deployment.get("environment")
        trust_tier = deployment.get("trust_tier")
        if (
            admission is None
            or admission_verifier is None
            or not isinstance(release_id, str)
            or not isinstance(deployment_id, str)
            or not isinstance(environment, str)
            or not isinstance(trust_tier, str)
        ):
            raise ValueError("current external authority is required")
        admission_verifier.require_current(
            admission,
            now=_current_authority_time(checked_at=checked_at, clock=clock),
        )
        admission.require(
            authority_projection=projection,
            operation=operation,
            release_id=release_id,
            deployment_id=deployment_id,
            target_environment=environment,
            target_trust_tier=trust_tier,
            now=_current_authority_time(checked_at=checked_at, clock=clock),
        )
        if source_bytes is not None:
            admission.authority.require_release_budget(
                workload_ids=_development_workload_ids(release),
                source_bytes=_development_budget_bytes(release, delivered_bytes=source_bytes),
            )
    except Exception:  # noqa: BLE001 - external authority adapters must fail closed.
        pass
    else:
        return
    raise AirflowArtifactDeliveryError(
        "DPONE_DEVELOPMENT_AUTHORITY_REQUIRED",
        "development artifact delivery requires matching externally verified authority",
    ) from None


def _current_authority_time(
    *,
    checked_at: datetime | None,
    clock: Callable[[], datetime] | None,
) -> datetime:
    current = clock() if clock is not None else checked_at
    if not isinstance(current, datetime):
        raise ValueError("current external authority time is required")
    return current


def development_authority_projection(release: Mapping[str, object]) -> object | None:
    if release.get("schema") == _DEVELOPMENT_RELEASE_SCHEMA:
        return release.get("development_authority")
    promotion = release.get("promotion")
    if not isinstance(promotion, Mapping) or promotion.get("profile") != _DEVELOPMENT_COMPOSITION_PROFILE:
        return None
    constituents = release.get("constituents")
    if not isinstance(constituents, list):
        raise ValueError("development constituents are invalid")
    native = tuple(item for item in constituents if isinstance(item, Mapping) and item.get("id") == "native")
    if len(native) != 1 or not isinstance(native[0].get("release"), Mapping):
        raise ValueError("development native constituent is invalid")
    return native[0]["release"].get("development_authority")


def _development_workload_ids(release: Mapping[str, object]) -> tuple[str, ...]:
    artifacts = release.get("artifacts")
    packs = artifacts.get("workload_packs") if isinstance(artifacts, Mapping) else None
    if not isinstance(packs, list):
        raise ValueError("development workload inventory is invalid")
    identifiers = tuple(
        sorted(str(item.get("id")) for item in packs if isinstance(item, Mapping) and isinstance(item.get("id"), str))
    )
    if len(identifiers) != len(packs):
        raise ValueError("development workload inventory is invalid")
    return identifiers


def _development_budget_bytes(release: Mapping[str, object], *, delivered_bytes: int) -> int:
    if release.get("schema") != _DEVELOPMENT_RELEASE_SCHEMA:
        return delivered_bytes
    artifacts = release.get("artifacts")
    if not isinstance(artifacts, Mapping):
        raise ValueError("development artifact inventory is invalid")
    total = 0
    for section in ("dag_specs", "workload_packs", "runtime_payloads"):
        rows = artifacts.get(section)
        if not isinstance(rows, list):
            raise ValueError("development artifact inventory is invalid")
        for row in rows:
            size = row.get("bytes") if isinstance(row, Mapping) else None
            if type(size) is not int or size < 0:
                raise ValueError("development artifact inventory is invalid")
            total += size
    return total


def require_registry_ref(
    deployment: Mapping[str, Any],
    index: Mapping[str, Any],
    expected: str,
) -> None:
    refs: set[object] = set()
    for value in (deployment.get("runtime_artifact_delivery"), index.get("runtime_artifact_delivery")):
        if isinstance(value, Mapping):
            refs.add(value.get("artifact_registry_ref"))
            source = value.get("source")
            if isinstance(source, Mapping):
                refs.add(source.get("artifact_registry_ref"))
    if refs != {expected}:
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_REGISTRY_REF_MISMATCH",
            "requested artifact registry differs from deployment configuration",
        )


def download(
    registry: ArtifactRegistryReader,
    key: PurePosixPath,
    destination: Path,
    *,
    max_bytes: int,
) -> None:
    try:
        registry.download_file(key, destination, max_bytes=max_bytes)
    except ArtifactRegistryObjectNotFound as exc:
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_REGISTRY_INCOMPLETE",
            "pinned artifact object disappeared during download",
        ) from exc
    except ArtifactRegistryReadLimitExceeded as exc:
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_REGISTRY_CHECKSUM_MISMATCH",
            "pinned artifact body exceeds its declared metadata size",
        ) from exc
    except ArtifactRegistryError as exc:
        raise registry_unavailable() from exc


def verify_download(
    path: Path,
    *,
    expected_size: int,
    expected_sha256: str | None,
    max_object_bytes: int,
) -> None:
    size = path.stat().st_size
    if size > max_object_bytes:
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_REGISTRY_OBJECT_TOO_LARGE",
            "downloaded artifact exceeds the configured per-object limit",
        )
    if size != expected_size:
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_REGISTRY_CHECKSUM_MISMATCH",
            "downloaded artifact size differs from remote metadata",
        )
    if expected_sha256 is not None and file_sha256(path).lower() != expected_sha256.lower():
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_REGISTRY_CHECKSUM_MISMATCH",
            "downloaded artifact checksum differs from its declared digest",
        )


def read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_REGISTRY_INCOMPLETE",
            "downloaded artifact manifest is invalid",
        ) from exc
    if not isinstance(payload, dict):
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_REGISTRY_INCOMPLETE",
            "downloaded artifact manifest must be an object",
        )
    return payload


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def from_cache_error(exc: DeploymentCacheError) -> AirflowArtifactDeliveryError:
    return AirflowArtifactDeliveryError(exc.code, str(exc), details=dict(exc.details))


def registry_unavailable() -> AirflowArtifactDeliveryError:
    return AirflowArtifactDeliveryError(
        "DPONE_ARTIFACT_REGISTRY_UNAVAILABLE",
        "artifact registry operation is unavailable",
    )


def require_registry_scope(
    registry: ArtifactRegistry,
    expected_scope_id: str | None,
) -> str:
    """Require explicit exact-authority capability before publication I/O."""

    if inspect.getattr_static(registry, "authority_scope_id", None) is None:
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_REGISTRY_SCOPE_INVALID",
            "artifact registry does not expose exact authority identity",
        )
    authority = cast(ArtifactRegistryAuthority, registry)
    try:
        scope_id = authority.authority_scope_id
    except ArtifactRegistryKeyError as exc:
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_REGISTRY_SCOPE_INVALID",
            "artifact registry returned an invalid non-secret scope identity",
        ) from exc
    if not isinstance(scope_id, str) or canonical_digest_or_none(scope_id) is None:
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_REGISTRY_SCOPE_INVALID",
            "artifact registry returned an invalid non-secret scope identity",
        )
    if expected_scope_id is not None and scope_id != expected_scope_id:
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_REGISTRY_SCOPE_MISMATCH",
            "configured artifact registry differs from the expected trusted scope",
        )
    return scope_id


def validate_materialization_target(request: MaterializeRequest) -> None:
    """Reject an existing unsafe cache root before registry or filesystem writes."""

    try:
        mode = request.cache_root.lstat().st_mode
    except FileNotFoundError:
        return
    except OSError as exc:
        raise AirflowArtifactDeliveryError(
            "DPONE_CACHE_PATH_ESCAPE",
            "materialization cache root metadata could not be inspected safely",
        ) from exc
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        raise AirflowArtifactDeliveryError(
            "DPONE_CACHE_PATH_ESCAPE",
            "materialization cache root must be a regular directory and must not be a symlink",
        )


@dataclass(frozen=True, slots=True)
class PinnedCacheRoot:
    """One open cache-root identity shared by the complete local install."""

    path: Path
    descriptor: int

    def __call__(self) -> None:
        try:
            current = os.stat(self.path, follow_symlinks=False)
        except OSError as exc:
            raise AirflowArtifactDeliveryError(
                "DPONE_CACHE_PATH_ESCAPE",
                "materialization cache root changed during execution",
            ) from exc
        opened = os.fstat(self.descriptor)
        if not stat.S_ISDIR(current.st_mode) or (current.st_dev, current.st_ino) != (opened.st_dev, opened.st_ino):
            raise AirflowArtifactDeliveryError(
                "DPONE_CACHE_PATH_ESCAPE",
                "materialization cache root changed during execution",
            )


@contextmanager
def guard_cache_root(cache_root: Path) -> Iterator[PinnedCacheRoot]:
    """Pin one cache-root inode for checks and descriptor-relative installs."""

    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(cache_root, flags)
    guard = PinnedCacheRoot(cache_root, descriptor)

    try:
        guard()
        yield guard
    finally:
        os.close(descriptor)


__all__ = [
    "ArtifactRegistry",
    "ArtifactRegistryError",
    "ArtifactRegistryObjectNotFound",
    "ArtifactRegistryReader",
    "DevelopmentTargetAdmission",
    "DevelopmentTargetAdmissionVerifier",
    "development_authority_projection",
    "download",
    "file_sha256",
    "from_cache_error",
    "guard_cache_root",
    "PinnedCacheRoot",
    "read_json",
    "registry_unavailable",
    "require_registry_ref",
    "require_development_delivery_authority",
    "validate_materialization_target",
    "verify_download",
]
