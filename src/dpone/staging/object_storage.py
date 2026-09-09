"""Object storage staging manifests and service."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.runtime.object_storage_retention import canonical_cleanup_policy
from dpone.storage.models import ObjectStorageUri
from dpone.storage.protocols import ObjectStorageClient


@dataclass(frozen=True, slots=True)
class ObjectStorageStagingPlan:
    base_uri: str
    run_id: str
    dataset: str
    table: str
    file_format: str
    compression: str = "none"
    cleanup_policy: str = "retain"

    @property
    def base(self) -> ObjectStorageUri:
        return ObjectStorageUri.parse(self.base_uri).prefix()

    def object_uri(self, file_name: str) -> ObjectStorageUri:
        return self.base.child(self.run_id, file_name)


@dataclass(frozen=True, slots=True)
class ObjectStorageStagingObject:
    uri: str
    file_name: str
    size_bytes: int
    sha256: str
    content_type: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "uri": self.uri,
            "file_name": self.file_name,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "content_type": self.content_type,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> ObjectStorageStagingObject:
        return cls(
            uri=str(payload["uri"]),
            file_name=str(payload["file_name"]),
            size_bytes=int(payload["size_bytes"]),
            sha256=str(payload["sha256"]),
            content_type=str(payload["content_type"]) if payload.get("content_type") else None,
        )


@dataclass(frozen=True, slots=True)
class ObjectStorageStagingManifest:
    provider: str
    base_uri: str
    run_id: str
    dataset: str
    table: str
    file_format: str
    compression: str
    cleanup_policy: str
    objects: tuple[ObjectStorageStagingObject, ...]

    @property
    def object_count(self) -> int:
        return len(self.objects)

    @property
    def total_size_bytes(self) -> int:
        return sum(item.size_bytes for item in self.objects)

    def to_dict(self) -> dict[str, object]:
        return {
            "provider": self.provider,
            "base_uri": self.base_uri,
            "run_id": self.run_id,
            "dataset": self.dataset,
            "table": self.table,
            "file_format": self.file_format,
            "compression": self.compression,
            "cleanup_policy": self.cleanup_policy,
            "object_count": self.object_count,
            "total_size_bytes": self.total_size_bytes,
            "objects": [item.to_dict() for item in self.objects],
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> ObjectStorageStagingManifest:
        return cls(
            provider=str(payload["provider"]),
            base_uri=str(payload["base_uri"]),
            run_id=str(payload["run_id"]),
            dataset=str(payload["dataset"]),
            table=str(payload["table"]),
            file_format=str(payload["file_format"]),
            compression=str(payload["compression"]),
            cleanup_policy=str(payload["cleanup_policy"]),
            objects=tuple(ObjectStorageStagingObject.from_dict(item) for item in payload.get("objects", [])),
        )


class ObjectStorageStagingService:
    """Upload local staging files to object storage and produce evidence."""

    def __init__(self, *, client: ObjectStorageClient) -> None:
        self._client = client

    def stage_files(
        self,
        plan: ObjectStorageStagingPlan,
        files: list[str | Path] | tuple[str | Path, ...],
        *,
        content_type: str | None = None,
    ) -> ObjectStorageStagingManifest:
        objects: list[ObjectStorageStagingObject] = []
        for local_path in files:
            path = Path(local_path)
            destination = plan.object_uri(path.name)
            uploaded = self._client.put_file(path, destination, content_type=content_type)
            objects.append(
                ObjectStorageStagingObject(
                    uri=uploaded.uri,
                    file_name=path.name,
                    size_bytes=uploaded.size_bytes,
                    sha256=uploaded.sha256,
                    content_type=uploaded.content_type,
                )
            )
        return ObjectStorageStagingManifest(
            provider=plan.base.provider.value,
            base_uri=str(plan.base).rstrip("/"),
            run_id=plan.run_id,
            dataset=plan.dataset,
            table=plan.table,
            file_format=plan.file_format,
            compression=plan.compression,
            cleanup_policy=plan.cleanup_policy,
            objects=tuple(objects),
        )

    def cleanup(self, manifest: ObjectStorageStagingManifest) -> int:
        if canonical_cleanup_policy(manifest.cleanup_policy) not in {"eager", "on_success"}:
            return 0
        prefix = ObjectStorageUri.parse(manifest.base_uri).prefix().child(manifest.run_id).prefix()
        return self._client.delete_prefix(prefix)
