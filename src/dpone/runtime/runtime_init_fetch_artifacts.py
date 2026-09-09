"""Stage and publish checksum-verified runtime artifacts."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING

from dpone.ports.artifact_registry import (
    ArtifactRegistryError,
    ArtifactRegistryObjectNotFound,
    ArtifactRegistryReadLimitExceeded,
)
from dpone.runtime.immutable_local_tree import (
    ImmutableLocalTreeError,
    materialize_immutable_local_tree,
)
from dpone.runtime.init_fetch_contract import InitFetchError, cache_relative_path
from dpone.runtime.runtime_init_fetch_attestation import StagedRuntimeArtifact
from dpone.runtime.runtime_init_fetch_plan import (
    RuntimeArtifactDescriptor,
    RuntimePayloadDescriptor,
    RuntimeWorkloadPackRef,
)
from dpone.runtime.runtime_init_fetch_ready_models import ReadyArtifact
from dpone.runtime.runtime_init_fetch_storage import (
    read_verified_file,
    verify_registry_metadata,
)

if TYPE_CHECKING:
    from dpone.ports.artifact_registry import ArtifactMetadata, ArtifactRegistryReader

RuntimeArtifact = RuntimeArtifactDescriptor | RuntimeWorkloadPackRef | RuntimePayloadDescriptor


class RuntimeArtifactStager:
    """Download one exact descriptor set into a private staging tree."""

    def __init__(self, registry: ArtifactRegistryReader) -> None:
        self._registry = registry

    def stage(
        self,
        root: Path,
        descriptors: Iterable[RuntimeArtifact],
    ) -> tuple[StagedRuntimeArtifact, ...]:
        return tuple(self._stage_one(root, descriptor) for descriptor in descriptors)

    def _stage_one(
        self,
        root: Path,
        descriptor: RuntimeArtifact,
    ) -> StagedRuntimeArtifact:
        key = cache_relative_path(descriptor.artifact_ref)
        metadata = self._stat(key, artifact_ref=descriptor.artifact_ref)
        verify_registry_metadata(metadata, key=key, descriptor=descriptor)
        destination = root.joinpath(*key.parts)
        destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            self._registry.download_file(key, destination, max_bytes=descriptor.bytes)
        except ArtifactRegistryObjectNotFound as exc:
            raise InitFetchError(
                "DPONE_CACHE_ARTIFACT_NOT_FOUND",
                "pinned runtime artifact was not found",
                artifact_ref=descriptor.artifact_ref,
            ) from exc
        except ArtifactRegistryReadLimitExceeded as exc:
            raise InitFetchError(
                "DPONE_CACHE_ARTIFACT_TOO_LARGE",
                "pinned runtime artifact exceeds its declared byte limit",
                artifact_ref=descriptor.artifact_ref,
            ) from exc
        except ArtifactRegistryError as exc:
            raise InitFetchError(
                "DPONE_ARTIFACT_REGISTRY_UNAVAILABLE",
                "runtime artifact registry download is unavailable",
                artifact_ref=descriptor.artifact_ref,
            ) from exc
        read_verified_file(
            destination,
            expected_sha256=descriptor.sha256,
            expected_bytes=descriptor.bytes,
            root=root,
        )
        return StagedRuntimeArtifact(descriptor=descriptor, path=destination)

    def _stat(self, key: PurePosixPath, *, artifact_ref: str) -> ArtifactMetadata:
        try:
            return self._registry.stat(key)
        except ArtifactRegistryObjectNotFound as exc:
            raise InitFetchError(
                "DPONE_CACHE_ARTIFACT_NOT_FOUND",
                "pinned runtime artifact was not found",
                artifact_ref=artifact_ref,
            ) from exc
        except ArtifactRegistryError as exc:
            raise InitFetchError(
                "DPONE_ARTIFACT_REGISTRY_UNAVAILABLE",
                "runtime artifact registry metadata is unavailable",
                artifact_ref=artifact_ref,
            ) from exc


def publish_runtime_artifacts(
    staged: tuple[StagedRuntimeArtifact, ...],
    *,
    artifact_root: Path,
) -> Mapping[str, ReadyArtifact]:
    """Atomically expose verified staging bytes through immutable local paths."""

    payload_root = artifact_root / "payload"
    sources = {cache_relative_path(item.descriptor.artifact_ref).as_posix(): item.path for item in staged}
    try:
        materialize_immutable_local_tree(
            payload_root,
            sources,
            allowed_parent=artifact_root,
            root=artifact_root,
        )
    except (ImmutableLocalTreeError, OSError, ValueError) as exc:
        raise InitFetchError(
            "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED",
            "verified runtime artifact publication failed",
        ) from exc
    published: dict[str, ReadyArtifact] = {}
    for item in staged:
        relative = cache_relative_path(item.descriptor.artifact_ref)
        destination = payload_root.joinpath(*relative.parts)
        read_verified_file(
            destination,
            expected_sha256=item.descriptor.sha256,
            expected_bytes=item.descriptor.bytes,
            root=payload_root,
        )
        published[item.descriptor.artifact_ref] = ReadyArtifact(
            artifact_ref=item.descriptor.artifact_ref,
            locator=f"payload/{relative.as_posix()}",
            sha256=item.descriptor.sha256,
            bytes=item.descriptor.bytes,
        )
    return published


__all__ = ["RuntimeArtifactStager", "publish_runtime_artifacts"]
