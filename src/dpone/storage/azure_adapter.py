"""Azure Blob Storage adapter with a lazy optional SDK import."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from dpone.storage.checksum import (
    BoundedReadOverflow,
    file_sha256,
    is_conditional_create_conflict,
    write_bounded_chunks,
)
from dpone.storage.models import (
    ObjectStorageObject,
    ObjectStorageReadLimitExceeded,
    ObjectStorageUri,
    ObjectStorageWriteConflict,
)


class AzureBlobObjectStorageClient:
    """Azure Blob Storage adapter backed by azure-storage-blob."""

    def __init__(
        self,
        *,
        service_client: Any | None = None,
        connection_string: str | None = None,
        account_url: str | None = None,
        credential: Any | None = None,
    ) -> None:
        if service_client is None:
            from azure.storage.blob import BlobServiceClient

            if connection_string:
                service_client = BlobServiceClient.from_connection_string(connection_string)
            else:
                if not account_url:
                    raise ValueError("Azure Blob client requires connection_string or account_url")
                service_client = BlobServiceClient(account_url=account_url, credential=credential)
        self._service_client = service_client

    @property
    def endpoint_authority(self) -> str | None:
        """Return the account endpoint selected by the constructed Azure client."""

        endpoint = getattr(self._service_client, "url", None)
        return _credential_free_endpoint(str(endpoint)) if endpoint else None

    def put_file(
        self,
        local_path: str | Path,
        destination: ObjectStorageUri,
        *,
        content_type: str | None = None,
    ) -> ObjectStorageObject:
        del content_type
        path = Path(local_path)
        blob = self._service_client.get_blob_client(container=destination.bucket, blob=destination.key)
        with path.open("rb") as handle:
            blob.upload_blob(handle, overwrite=True)
        return _uploaded_object(path, destination)

    def get_file(self, source: ObjectStorageUri, local_path: str | Path) -> None:
        target = Path(local_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        blob = self._service_client.get_blob_client(container=source.bucket, blob=source.key)
        with target.open("wb") as handle:
            handle.write(blob.download_blob().readall())

    def get_file_bounded(
        self,
        source: ObjectStorageUri,
        local_path: str | Path,
        *,
        max_bytes: int,
    ) -> None:
        blob = self._service_client.get_blob_client(container=source.bucket, blob=source.key)
        try:
            write_bounded_chunks(blob.download_blob().chunks(), Path(local_path), max_bytes=max_bytes)
        except BoundedReadOverflow as exc:
            raise ObjectStorageReadLimitExceeded(max_bytes) from exc

    def put_file_if_absent(
        self,
        local_path: str | Path,
        destination: ObjectStorageUri,
        *,
        content_type: str | None = None,
    ) -> ObjectStorageObject:
        del content_type
        path = Path(local_path)
        blob = self._service_client.get_blob_client(container=destination.bucket, blob=destination.key)
        with _write_conflict_boundary(destination):
            with path.open("rb") as handle:
                blob.upload_blob(
                    handle,
                    overwrite=False,
                    metadata={"dpone-sha256": file_sha256(path)},
                )
        return _uploaded_object(path, destination)

    def stat(self, uri: ObjectStorageUri) -> ObjectStorageObject:
        blob = self._service_client.get_blob_client(container=uri.bucket, blob=uri.key)
        properties = blob.get_blob_properties()
        metadata = dict(getattr(properties, "metadata", None) or {})
        return ObjectStorageObject(
            uri=str(uri),
            size_bytes=int(getattr(properties, "size", 0) or 0),
            sha256=str(metadata.get("dpone-sha256") or ""),
            content_type=getattr(getattr(properties, "content_settings", None), "content_type", None),
            metadata=metadata,
        )

    def delete_prefix(self, prefix: ObjectStorageUri) -> int:
        container = self._service_client.get_container_client(prefix.bucket)
        deleted = 0
        for blob in container.list_blobs(name_starts_with=prefix.key):
            container.delete_blob(blob.name)
            deleted += 1
        return deleted

    def exists(self, uri: ObjectStorageUri) -> bool:
        return bool(self._service_client.get_blob_client(container=uri.bucket, blob=uri.key).exists())

    def list_prefix(self, prefix: ObjectStorageUri) -> tuple[str, ...]:
        return tuple(item.uri for item in self.list_objects(prefix))

    def list_objects(self, prefix: ObjectStorageUri) -> tuple[ObjectStorageObject, ...]:
        container = self._service_client.get_container_client(prefix.bucket)
        objects: list[ObjectStorageObject] = []
        for blob in container.list_blobs(name_starts_with=prefix.key):
            objects.append(
                _listed_object(
                    ObjectStorageUri(prefix.provider, prefix.bucket, blob.name, account=prefix.account),
                    getattr(blob, "size", 0),
                    getattr(blob, "etag", ""),
                    modified=getattr(blob, "last_modified", None),
                )
            )
        return tuple(objects)


def _uploaded_object(path: Path, destination: ObjectStorageUri) -> ObjectStorageObject:
    return ObjectStorageObject(str(destination), path.stat().st_size, file_sha256(path))


def _credential_free_endpoint(value: str) -> str | None:
    """Strip SAS material while retaining an emulator's routing path."""

    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        return None
    if not parsed.scheme or not parsed.hostname or parsed.username is not None or parsed.password is not None:
        return None
    host = parsed.hostname
    if ":" in host:
        host = f"[{host}]"
    if port is not None:
        host = f"{host}:{port}"
    return urlunsplit((parsed.scheme, host, parsed.path, "", ""))


def _listed_object(
    uri: ObjectStorageUri,
    size: Any,
    checksum: Any,
    *,
    modified: Any,
) -> ObjectStorageObject:
    metadata = {"last_modified": modified.isoformat()} if modified is not None else {}
    return ObjectStorageObject(str(uri), int(size or 0), str(checksum or "").strip('"'), metadata=metadata)


@contextmanager
def _write_conflict_boundary(destination: ObjectStorageUri) -> Iterator[None]:
    try:
        yield
    except Exception as exc:  # noqa: BLE001 - optional SDK errors are inspected without importing SDKs.
        if is_conditional_create_conflict(exc):
            raise ObjectStorageWriteConflict(destination) from exc
        raise


__all__ = ["AzureBlobObjectStorageClient"]
