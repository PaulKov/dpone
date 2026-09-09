"""Protocols for object storage clients."""

from __future__ import annotations

from pathlib import Path
from typing import Protocol, runtime_checkable

from dpone.storage.models import ObjectStorageObject, ObjectStorageUri


class ObjectStorageClient(Protocol):
    def put_file(
        self,
        local_path: str | Path,
        destination: ObjectStorageUri,
        *,
        content_type: str | None = None,
    ) -> ObjectStorageObject:
        """Upload a local file and return durable object metadata."""

    def get_file(self, source: ObjectStorageUri, local_path: str | Path) -> None:
        """Download an object to a local file."""

    def delete_prefix(self, prefix: ObjectStorageUri) -> int:
        """Delete every object under a prefix and return deleted object count."""

    def exists(self, uri: ObjectStorageUri) -> bool:
        """Return whether the object exists."""

    def list_prefix(self, prefix: ObjectStorageUri) -> tuple[str, ...]:
        """Return object URIs under a prefix."""

    def list_objects(self, prefix: ObjectStorageUri) -> tuple[ObjectStorageObject, ...]:
        """Return object metadata under a prefix when the backend supports inventory."""


class ImmutableObjectStorageClient(ObjectStorageClient, Protocol):
    """Capability required by immutable create-or-compare artifact registries."""

    def put_file_if_absent(
        self,
        local_path: str | Path,
        destination: ObjectStorageUri,
        *,
        content_type: str | None = None,
    ) -> ObjectStorageObject:
        """Atomically create an object or raise ObjectStorageWriteConflict."""

    def stat(self, uri: ObjectStorageUri) -> ObjectStorageObject:
        """Return exact object metadata or raise when it does not exist."""

    def get_file_bounded(
        self,
        source: ObjectStorageUri,
        local_path: str | Path,
        *,
        max_bytes: int,
    ) -> None:
        """Stream one object and abort before writing more than max_bytes."""


@runtime_checkable
class ObjectStorageEndpointAuthority(Protocol):
    """Capability exposing the endpoint selected by a constructed SDK client."""

    @property
    def endpoint_authority(self) -> str | None:
        """Return a credential-free endpoint URL or None when unavailable."""


__all__ = [
    "ImmutableObjectStorageClient",
    "ObjectStorageClient",
    "ObjectStorageEndpointAuthority",
]
