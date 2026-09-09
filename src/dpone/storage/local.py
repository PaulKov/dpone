"""Local filesystem implementation of the object storage protocol."""

from __future__ import annotations

import hashlib
import shutil
from datetime import datetime
from pathlib import Path

from dpone._compat import UTC
from dpone.storage.checksum import BoundedReadOverflow, write_bounded_chunks
from dpone.storage.local_fs import AnchoredLocalFilesystem, LocalFileFacts
from dpone.storage.models import (
    ObjectStorageObject,
    ObjectStorageReadLimitExceeded,
    ObjectStorageUri,
    ObjectStorageWriteConflict,
)


class LocalObjectStorageClient:
    """Provider-aware object storage emulator anchored below one local root."""

    def __init__(self, root_dir: str | Path) -> None:
        self.root_dir = Path(root_dir)
        self._filesystem = AnchoredLocalFilesystem(self.root_dir)

    @property
    def endpoint_authority(self) -> str:
        """Return a non-secret identity for the actual emulator filesystem root."""

        root = str(self.root_dir.expanduser().resolve()).encode("utf-8")
        return "local://path-" + hashlib.sha256(root).hexdigest()

    def put_file(
        self,
        local_path: str | Path,
        destination: ObjectStorageUri,
        *,
        content_type: str | None = None,
    ) -> ObjectStorageObject:
        facts = self._filesystem.write_file(local_path, _object_parts(destination), create_only=False)
        return _object_metadata(destination, facts, content_type=content_type)

    def get_file(self, source: ObjectStorageUri, local_path: str | Path) -> None:
        target = Path(local_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        with self._filesystem.open_reader(_object_parts(source)) as source_handle, target.open("wb") as target_handle:
            shutil.copyfileobj(source_handle, target_handle)

    def put_file_if_absent(
        self,
        local_path: str | Path,
        destination: ObjectStorageUri,
        *,
        content_type: str | None = None,
    ) -> ObjectStorageObject:
        try:
            facts = self._filesystem.write_file(local_path, _object_parts(destination), create_only=True)
        except FileExistsError as exc:
            raise ObjectStorageWriteConflict(destination) from exc
        return _object_metadata(destination, facts, content_type=content_type)

    def get_file_bounded(
        self,
        source: ObjectStorageUri,
        local_path: str | Path,
        *,
        max_bytes: int,
    ) -> None:
        with self._filesystem.open_reader(_object_parts(source)) as handle:
            try:
                write_bounded_chunks(
                    iter(lambda: handle.read(1024 * 1024), b""),
                    Path(local_path),
                    max_bytes=max_bytes,
                )
            except BoundedReadOverflow as exc:
                raise ObjectStorageReadLimitExceeded(max_bytes) from exc

    def stat(self, uri: ObjectStorageUri) -> ObjectStorageObject:
        try:
            facts = self._filesystem.facts(_object_parts(uri))
        except FileNotFoundError as exc:
            raise FileNotFoundError(str(uri)) from exc
        return _object_metadata(uri, facts)

    def delete_prefix(self, prefix: ObjectStorageUri) -> int:
        return self._filesystem.delete_tree(_object_parts(prefix))

    def exists(self, uri: ObjectStorageUri) -> bool:
        return self._filesystem.exists(_object_parts(uri))

    def list_prefix(self, prefix: ObjectStorageUri) -> tuple[str, ...]:
        return tuple(item.uri for item in self.list_objects(prefix))

    def list_objects(self, prefix: ObjectStorageUri) -> tuple[ObjectStorageObject, ...]:
        namespace_parts = _namespace_parts(prefix)
        objects: list[ObjectStorageObject] = []
        for item in self._filesystem.list_files(_object_parts(prefix)):
            key = "/".join(item.parts[len(namespace_parts) :])
            uri = ObjectStorageUri(prefix.provider, prefix.bucket, key, account=prefix.account)
            objects.append(
                _object_metadata(
                    uri,
                    item.facts,
                    metadata={"last_modified": datetime.fromtimestamp(item.facts.modified_at, tz=UTC).isoformat()},
                )
            )
        return tuple(objects)


def _object_parts(uri: ObjectStorageUri) -> tuple[str, ...]:
    normalized_key = uri.key.rstrip("/")
    key_parts = tuple(normalized_key.split("/")) if normalized_key else ()
    if "\\" in uri.key or any(part in {"", ".", ".."} for part in key_parts):
        raise ValueError("Object storage key must be a normalized relative POSIX path")
    return (*_namespace_parts(uri), *key_parts)


def _namespace_parts(uri: ObjectStorageUri) -> tuple[str, ...]:
    return tuple(part for part in (uri.provider.value, uri.account, uri.bucket) if part is not None)


def _object_metadata(
    uri: ObjectStorageUri,
    facts: LocalFileFacts,
    *,
    content_type: str | None = None,
    metadata: dict[str, object] | None = None,
) -> ObjectStorageObject:
    return ObjectStorageObject(
        uri=str(uri),
        size_bytes=facts.size_bytes,
        sha256=facts.sha256,
        content_type=content_type,
        metadata=dict(metadata or {}),
    )


__all__ = ["LocalObjectStorageClient"]
