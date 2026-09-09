"""Bounded staged execution for validated runtime init-fetch plans."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.ports.artifact_registry import ArtifactMetadata, ArtifactRegistryReader
    from dpone.runtime.init_fetch_contract import InitFetchPlan, RuntimeArtifactRef


import hashlib
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Protocol

from dpone.ports.artifact_registry import (
    ArtifactRegistryError,
    ArtifactRegistryObjectNotFound,
    ArtifactRegistryReadLimitExceeded,
)
from dpone.runtime.deployment_cache_common import DeploymentCacheError, open_regular_file
from dpone.runtime.immutable_local_tree import (
    ImmutableLocalTreeError,
    materialize_immutable_local_tree,
)
from dpone.runtime.init_fetch_contract import (
    InitFetchedArtifact,
    InitFetchError,
    InitFetchResult,
    cache_relative_path,
    json_bytes,
    runtime_artifact_locator,
    validate_init_fetch_plan,
)

DEFAULT_MAX_ARTIFACT_COUNT = 10_000
DEFAULT_MAX_TOTAL_BYTES = 512 * 1024 * 1024
_READ_CHUNK_BYTES = 64 * 1024
_MANIFEST_NAME = "init-fetch-manifest.json"


@dataclass(frozen=True, slots=True)
class StagedInitFetchArtifact:
    """Exact verified staged file presented to an attestation verifier."""

    artifact: RuntimeArtifactRef
    path: Path


class InitFetchAttestationVerifier(Protocol):
    """Trust boundary for exact pinned plan and staged artifact attestations."""

    def verify(
        self,
        *,
        plan: InitFetchPlan,
        staged_artifacts: tuple[StagedInitFetchArtifact, ...],
    ) -> None:
        """Return only when every staged artifact is trusted for this exact plan."""


class InitFetchExecutor:
    """Fetch, verify, attest, and atomically publish one pinned runtime plan."""

    def __init__(
        self,
        *,
        registry: ArtifactRegistryReader,
        destination_root: str | Path,
        max_artifact_count: int = DEFAULT_MAX_ARTIFACT_COUNT,
        max_total_bytes: int = DEFAULT_MAX_TOTAL_BYTES,
        attestation_verifier: InitFetchAttestationVerifier | None = None,
    ) -> None:
        self._registry = registry
        self._destination_root = Path(destination_root).absolute()
        self._max_artifact_count = _positive_limit(
            "max_artifact_count",
            max_artifact_count,
        )
        self._max_total_bytes = _positive_limit("max_total_bytes", max_total_bytes)
        self._attestation_verifier = attestation_verifier

    def execute(self, plan: InitFetchPlan) -> InitFetchResult:
        canonical_plan = validate_init_fetch_plan(plan)
        self._preflight(canonical_plan)
        with tempfile.TemporaryDirectory(prefix=".dpone-init-fetch-") as staging_name:
            staging_root = Path(staging_name)
            staging_root.chmod(0o700)
            staged = tuple(self._stage_artifact(staging_root, artifact) for artifact in canonical_plan.artifacts)
            self._verify_attestation(canonical_plan, staged)
            for item in staged:
                _verify_staged_artifact(
                    item,
                    staging_root=staging_root,
                )
            result = _result(canonical_plan, staged)
            files = {cache_relative_path(item.artifact.artifact_ref).as_posix(): item.path for item in staged}
            manifest_path = staging_root / _MANIFEST_NAME
            _write_new_file(manifest_path, json_bytes(result.to_dict()))
            files[_MANIFEST_NAME] = manifest_path
            self._publish(files)
            return result

    def _preflight(self, plan: InitFetchPlan) -> None:
        if len(plan.artifacts) > self._max_artifact_count:
            raise InitFetchError(
                "DPONE_INIT_FETCH_ARTIFACT_LIMIT_EXCEEDED",
                "init-fetch artifact count exceeds the configured limit",
            )
        total_bytes = sum(artifact.bytes for artifact in plan.artifacts)
        if total_bytes > self._max_total_bytes:
            raise InitFetchError(
                "DPONE_ARTIFACT_REGISTRY_TOTAL_LIMIT_EXCEEDED",
                "init-fetch declared artifact bytes exceed the configured total limit",
            )
        if plan.verify["attestations"] == "required_for_prod" and self._attestation_verifier is None:
            raise InitFetchError(
                "DPONE_ARTIFACT_ATTESTATION_REQUIRED",
                "init-fetch requires a configured artifact attestation verifier",
            )

    def _stage_artifact(
        self,
        staging_root: Path,
        artifact: RuntimeArtifactRef,
    ) -> StagedInitFetchArtifact:
        key = cache_relative_path(artifact.artifact_ref)
        metadata = self._stat(key, artifact_ref=artifact.artifact_ref)
        _require_expected_metadata(metadata, key=key, artifact=artifact)
        destination = staging_root.joinpath(*key.parts)
        destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            self._registry.download_file(key, destination, max_bytes=artifact.bytes)
        except ArtifactRegistryObjectNotFound as exc:
            raise InitFetchError(
                "DPONE_CACHE_ARTIFACT_NOT_FOUND",
                "cache artifact was not found",
                artifact_ref=artifact.artifact_ref,
            ) from exc
        except ArtifactRegistryReadLimitExceeded as exc:
            raise InitFetchError(
                "DPONE_CACHE_ARTIFACT_TOO_LARGE",
                "cache artifact exceeds the declared size",
                artifact_ref=artifact.artifact_ref,
            ) from exc
        except ArtifactRegistryError as exc:
            raise InitFetchError(
                "DPONE_ARTIFACT_REGISTRY_UNAVAILABLE",
                "artifact registry download is unavailable",
                artifact_ref=artifact.artifact_ref,
            ) from exc
        staged = StagedInitFetchArtifact(artifact=artifact, path=destination)
        _verify_staged_artifact(staged, staging_root=staging_root)
        return staged

    def _stat(
        self,
        key: PurePosixPath,
        *,
        artifact_ref: str,
    ) -> ArtifactMetadata:
        try:
            return self._registry.stat(key)
        except ArtifactRegistryObjectNotFound as exc:
            raise InitFetchError(
                "DPONE_CACHE_ARTIFACT_NOT_FOUND",
                "cache artifact was not found",
                artifact_ref=artifact_ref,
            ) from exc
        except ArtifactRegistryError as exc:
            raise InitFetchError(
                "DPONE_ARTIFACT_REGISTRY_UNAVAILABLE",
                "artifact registry metadata is unavailable",
                artifact_ref=artifact_ref,
            ) from exc

    def _verify_attestation(
        self,
        plan: InitFetchPlan,
        staged: tuple[StagedInitFetchArtifact, ...],
    ) -> None:
        if plan.verify["attestations"] != "required_for_prod":
            return
        if self._attestation_verifier is None:
            raise InitFetchError(
                "DPONE_ARTIFACT_ATTESTATION_REQUIRED",
                "init-fetch requires a configured artifact attestation verifier",
            )
        try:
            self._attestation_verifier.verify(
                plan=plan,
                staged_artifacts=staged,
            )
        except Exception as exc:  # noqa: BLE001 - verifier details must not cross the boundary.
            raise InitFetchError(
                "DPONE_ARTIFACT_ATTESTATION_REQUIRED",
                "init-fetch artifact attestation could not be verified",
            ) from exc

    def _publish(self, files: dict[str, Path]) -> None:
        try:
            materialize_immutable_local_tree(
                self._destination_root,
                files,
                allowed_parent=self._destination_root.parent,
                root=self._destination_root.parent,
            )
        except ImmutableLocalTreeError as exc:
            raise InitFetchError(
                "DPONE_INIT_FETCH_DESTINATION_CONFLICT",
                "runtime artifact destination already contains different or unsafe content",
            ) from exc


def _result(
    plan: InitFetchPlan,
    staged: tuple[StagedInitFetchArtifact, ...],
) -> InitFetchResult:
    return InitFetchResult(
        passed=True,
        release_id=plan.release_id,
        deployment_id=plan.deployment_id,
        artifact_registry_ref=plan.artifact_registry_ref,
        artifacts=tuple(
            InitFetchedArtifact(
                id=item.artifact.id,
                artifact_ref=item.artifact.artifact_ref,
                sha256=item.artifact.sha256,
                path=runtime_artifact_locator(cache_relative_path(item.artifact.artifact_ref)),
                bytes=item.artifact.bytes,
            )
            for item in staged
        ),
        manifest_path=runtime_artifact_locator(PurePosixPath(_MANIFEST_NAME)),
    )


def _require_expected_metadata(
    metadata: ArtifactMetadata,
    *,
    key: PurePosixPath,
    artifact: RuntimeArtifactRef,
) -> None:
    if (
        metadata.key != key
        or isinstance(metadata.size_bytes, bool)
        or not isinstance(metadata.size_bytes, int)
        or metadata.size_bytes < 0
        or (metadata.advertised_sha256 is not None and not isinstance(metadata.advertised_sha256, str))
    ):
        raise InitFetchError(
            "DPONE_ARTIFACT_REGISTRY_METADATA_INVALID",
            "artifact registry returned invalid metadata",
            artifact_ref=artifact.artifact_ref,
        )
    if metadata.size_bytes != artifact.bytes:
        raise InitFetchError(
            "DPONE_CACHE_ARTIFACT_SIZE_MISMATCH",
            "cache artifact size does not match init-fetch plan",
            artifact_ref=artifact.artifact_ref,
        )
    if metadata.advertised_sha256 is not None and metadata.advertised_sha256.casefold() != artifact.sha256:
        raise InitFetchError(
            "DPONE_CACHE_CHECKSUM_MISMATCH",
            "cache artifact checksum does not match init-fetch plan",
            artifact_ref=artifact.artifact_ref,
        )


def _verify_staged_artifact(
    staged: StagedInitFetchArtifact,
    *,
    staging_root: Path,
) -> None:
    artifact = staged.artifact
    try:
        descriptor = open_regular_file(
            staged.path,
            missing_code="DPONE_CACHE_ARTIFACT_NOT_FOUND",
            invalid_code="DPONE_CACHE_PATH_ESCAPE",
            label="staged cache artifact",
            root=staging_root,
        )
    except DeploymentCacheError as exc:
        raise InitFetchError(exc.code, str(exc), artifact_ref=artifact.artifact_ref) from exc
    try:
        if os.fstat(descriptor).st_size != artifact.bytes:
            raise InitFetchError(
                "DPONE_CACHE_ARTIFACT_SIZE_MISMATCH",
                "cache artifact size does not match init-fetch plan",
                artifact_ref=artifact.artifact_ref,
            )
        try:
            actual_sha256 = _sha256_descriptor(descriptor)
        except OSError as exc:
            raise InitFetchError(
                "DPONE_CACHE_ARTIFACT_READ_FAILED",
                "staged cache artifact could not be read",
                artifact_ref=artifact.artifact_ref,
            ) from exc
        if actual_sha256 != artifact.sha256:
            raise InitFetchError(
                "DPONE_CACHE_CHECKSUM_MISMATCH",
                "cache artifact checksum does not match init-fetch plan",
                artifact_ref=artifact.artifact_ref,
            )
    finally:
        os.close(descriptor)


def _sha256_descriptor(descriptor: int) -> str:
    digest = hashlib.sha256()
    while chunk := os.read(descriptor, _READ_CHUNK_BYTES):
        digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _write_new_file(path: Path, payload: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


def _positive_limit(name: str, value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


__all__ = [
    "DEFAULT_MAX_ARTIFACT_COUNT",
    "DEFAULT_MAX_TOTAL_BYTES",
    "InitFetchAttestationVerifier",
    "InitFetchExecutor",
    "StagedInitFetchArtifact",
]
