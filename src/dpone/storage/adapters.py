"""Cloud object storage adapters with lazy optional imports."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from importlib import import_module
from pathlib import Path
from typing import Any

from dpone.storage.checksum import (
    BoundedReadOverflow,
    file_sha256,
    is_conditional_create_conflict,
    reader_chunks,
    write_bounded_chunks,
    write_bounded_download,
)
from dpone.storage.models import (
    ObjectStorageObject,
    ObjectStorageReadLimitExceeded,
    ObjectStorageUri,
    ObjectStorageWriteConflict,
)


class _ListPrefixMixin:
    def list_prefix(self, prefix: ObjectStorageUri) -> tuple[str, ...]:
        return tuple(item.uri for item in self.list_objects(prefix))

    def list_objects(self, prefix: ObjectStorageUri) -> tuple[ObjectStorageObject, ...]:
        raise NotImplementedError


class S3ObjectStorageClient(_ListPrefixMixin):
    """AWS S3 adapter backed by boto3 when no client is injected."""

    def __init__(
        self,
        *,
        client: Any | None = None,
        endpoint_url: str | None = None,
        region_name: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
    ) -> None:
        if client is None:
            import boto3

            kwargs = {
                key: value
                for key, value in {
                    "endpoint_url": endpoint_url,
                    "region_name": region_name,
                    "aws_access_key_id": aws_access_key_id,
                    "aws_secret_access_key": aws_secret_access_key,
                    "aws_session_token": aws_session_token,
                }.items()
                if value
            }
            client = boto3.client("s3", **kwargs)
        self._client = client

    @property
    def endpoint_url(self) -> str | None:
        """Return the endpoint authority selected by the constructed SDK client."""

        meta = getattr(self._client, "meta", None)
        endpoint = getattr(meta, "endpoint_url", None)
        return str(endpoint) if endpoint else None

    @property
    def endpoint_authority(self) -> str | None:
        """Expose the actual SDK endpoint to authority-aware registry adapters."""

        return self.endpoint_url

    @property
    def conditional_object_client(self) -> Any:
        """Expose the constructed SDK client to narrow conditional-object adapters."""

        return self._client

    def put_file(
        self,
        local_path: str | Path,
        destination: ObjectStorageUri,
        *,
        content_type: str | None = None,
    ) -> ObjectStorageObject:
        path = Path(local_path)
        extra_args = {"ContentType": content_type} if content_type else None
        if extra_args:
            self._client.upload_file(str(path), destination.bucket, destination.key, ExtraArgs=extra_args)
        else:
            self._client.upload_file(str(path), destination.bucket, destination.key)
        return _uploaded_object(path, destination, content_type)

    def get_file(self, source: ObjectStorageUri, local_path: str | Path) -> None:
        target = Path(local_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        self._client.download_file(source.bucket, source.key, str(target))

    def get_file_bounded(
        self,
        source: ObjectStorageUri,
        local_path: str | Path,
        *,
        max_bytes: int,
    ) -> None:
        body = self._client.get_object(Bucket=source.bucket, Key=source.key)["Body"]
        try:
            try:
                write_bounded_chunks(reader_chunks(body, max_bytes=max_bytes), Path(local_path), max_bytes=max_bytes)
            except BoundedReadOverflow as exc:
                raise ObjectStorageReadLimitExceeded(max_bytes) from exc
        finally:
            close = getattr(body, "close", None)
            if callable(close):
                close()

    def put_file_if_absent(
        self,
        local_path: str | Path,
        destination: ObjectStorageUri,
        *,
        content_type: str | None = None,
    ) -> ObjectStorageObject:
        path = Path(local_path)
        kwargs: dict[str, Any] = {
            "Bucket": destination.bucket,
            "Key": destination.key,
            "IfNoneMatch": "*",
            "Metadata": {"dpone-sha256": file_sha256(path)},
        }
        if content_type:
            kwargs["ContentType"] = content_type
        with _write_conflict_boundary(destination):
            with path.open("rb") as handle:
                self._client.put_object(Body=handle, **kwargs)
        return _uploaded_object(path, destination, content_type)

    def stat(self, uri: ObjectStorageUri) -> ObjectStorageObject:
        response = self._client.head_object(Bucket=uri.bucket, Key=uri.key)
        metadata = response.get("Metadata") or {}
        return ObjectStorageObject(
            uri=str(uri),
            size_bytes=int(response.get("ContentLength") or 0),
            sha256=str(metadata.get("dpone-sha256") or ""),
            content_type=response.get("ContentType"),
            metadata=dict(metadata),
        )

    def delete_prefix(self, prefix: ObjectStorageUri) -> int:
        paginator = self._client.get_paginator("list_objects_v2")
        deleted = 0
        for page in paginator.paginate(Bucket=prefix.bucket, Prefix=prefix.key):
            objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
            if not objects:
                continue
            self._client.delete_objects(Bucket=prefix.bucket, Delete={"Objects": objects})
            deleted += len(objects)
        return deleted

    def exists(self, uri: ObjectStorageUri) -> bool:
        try:
            self._client.head_object(Bucket=uri.bucket, Key=uri.key)
            return True
        except Exception:
            return False

    def list_objects(self, prefix: ObjectStorageUri) -> tuple[ObjectStorageObject, ...]:
        paginator = self._client.get_paginator("list_objects_v2")
        inventory: list[ObjectStorageObject] = []
        for page in paginator.paginate(Bucket=prefix.bucket, Prefix=prefix.key):
            for item in page.get("Contents", []):
                inventory.append(
                    _listed_object(
                        ObjectStorageUri(prefix.provider, prefix.bucket, item["Key"], account=prefix.account),
                        item.get("Size"),
                        item.get("ETag"),
                        modified=item.get("LastModified"),
                    )
                )
        return tuple(inventory)


class GCSObjectStorageClient(_ListPrefixMixin):
    """Google Cloud Storage adapter backed by google-cloud-storage."""

    def __init__(self, *, client: Any | None = None) -> None:
        if client is None:
            from google.cloud.storage import Client

            client = Client()
        self._client = client

    @property
    def endpoint_authority(self) -> str | None:
        """Return the API endpoint selected by the constructed GCS client."""

        connection = getattr(self._client, "_connection", None)
        endpoint = getattr(connection, "API_BASE_URL", None) or getattr(
            connection,
            "api_base_url",
            None,
        )
        return str(endpoint) if endpoint else None

    def put_file(
        self,
        local_path: str | Path,
        destination: ObjectStorageUri,
        *,
        content_type: str | None = None,
    ) -> ObjectStorageObject:
        path = Path(local_path)
        blob = self._client.bucket(destination.bucket).blob(destination.key)
        blob.upload_from_filename(str(path), content_type=content_type) if content_type else blob.upload_from_filename(
            str(path)
        )
        return _uploaded_object(path, destination, content_type)

    def get_file(self, source: ObjectStorageUri, local_path: str | Path) -> None:
        target = Path(local_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        self._client.bucket(source.bucket).blob(source.key).download_to_filename(str(target))

    def get_file_bounded(
        self,
        source: ObjectStorageUri,
        local_path: str | Path,
        *,
        max_bytes: int,
    ) -> None:
        blob = self._client.bucket(source.bucket).blob(source.key)
        try:
            write_bounded_download(
                lambda target: blob.download_to_file(target),
                Path(local_path),
                max_bytes=max_bytes,
            )
        except BoundedReadOverflow as exc:
            raise ObjectStorageReadLimitExceeded(max_bytes) from exc

    def put_file_if_absent(
        self,
        local_path: str | Path,
        destination: ObjectStorageUri,
        *,
        content_type: str | None = None,
    ) -> ObjectStorageObject:
        path = Path(local_path)
        blob = self._client.bucket(destination.bucket).blob(destination.key)
        blob.metadata = {"dpone-sha256": file_sha256(path)}
        kwargs: dict[str, Any] = {"if_generation_match": 0}
        if content_type:
            kwargs["content_type"] = content_type
        with _write_conflict_boundary(destination):
            blob.upload_from_filename(str(path), **kwargs)
        return _uploaded_object(path, destination, content_type)

    def stat(self, uri: ObjectStorageUri) -> ObjectStorageObject:
        blob = self._client.bucket(uri.bucket).blob(uri.key)
        blob.reload()
        metadata = dict(getattr(blob, "metadata", None) or {})
        return ObjectStorageObject(
            uri=str(uri),
            size_bytes=int(getattr(blob, "size", 0) or 0),
            sha256=str(metadata.get("dpone-sha256") or ""),
            content_type=getattr(blob, "content_type", None),
            metadata=metadata,
        )

    def delete_prefix(self, prefix: ObjectStorageUri) -> int:
        bucket = self._client.bucket(prefix.bucket)
        deleted = 0
        for blob in self._client.list_blobs(bucket, prefix=prefix.key):
            blob.delete()
            deleted += 1
        return deleted

    def exists(self, uri: ObjectStorageUri) -> bool:
        return bool(self._client.bucket(uri.bucket).blob(uri.key).exists())

    def list_objects(self, prefix: ObjectStorageUri) -> tuple[ObjectStorageObject, ...]:
        bucket = self._client.bucket(prefix.bucket)
        objects: list[ObjectStorageObject] = []
        for blob in self._client.list_blobs(bucket, prefix=prefix.key):
            objects.append(
                _listed_object(
                    ObjectStorageUri(prefix.provider, prefix.bucket, blob.name, account=prefix.account),
                    getattr(blob, "size", 0),
                    getattr(blob, "md5_hash", ""),
                    modified=getattr(blob, "updated", None),
                )
            )
        return tuple(objects)


def _uploaded_object(path: Path, destination: ObjectStorageUri, content_type: str | None) -> ObjectStorageObject:
    return ObjectStorageObject(
        uri=str(destination),
        size_bytes=path.stat().st_size,
        sha256=file_sha256(path),
        content_type=content_type,
    )


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


def __getattr__(name: str) -> Any:
    if name != "AzureBlobObjectStorageClient":
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module("dpone.storage.azure_adapter"), name)
    globals()[name] = value
    return value


__all__ = ["AzureBlobObjectStorageClient", "GCSObjectStorageClient", "S3ObjectStorageClient"]  # noqa: F822
