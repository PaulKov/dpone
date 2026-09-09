"""Small read-only artifact store adapters for scheduler cache sync."""

from __future__ import annotations

import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from urllib.parse import urlparse

from dpone_airflow_pack.artifact_uri_redaction import redact_artifact_uri


class ArtifactStoreError(RuntimeError):
    """Raised when a remote artifact cannot be read."""


class ArtifactReadPort(Protocol):
    """Bounded read port consumed by cache preparation services."""

    def read_bytes(self, uri: str, *, max_bytes: int | None = None) -> bytes: ...


class _ReadableBody(Protocol):
    def read(self, size: int = -1) -> bytes: ...


@dataclass(frozen=True)
class ArtifactReader:
    """Read bytes from local files or S3-compatible object storage."""

    reader_connection_id: str | None = None

    def read_bytes(self, uri: str, *, max_bytes: int | None = None) -> bytes:
        """Read one artifact, optionally stopping after one over-limit sentinel byte.

        ``None`` deliberately preserves the historical unbounded Python API.
        CLI callers use positive limits and cannot select this mode.
        """

        _validate_max_bytes(max_bytes)
        parsed = urlparse(uri)
        if parsed.scheme in ("", "file"):
            path = Path(parsed.path if parsed.scheme == "file" else uri)
            return _read_local_bytes(path, uri=uri, max_bytes=max_bytes)
        if parsed.scheme == "s3":
            return self._read_s3_bytes(
                parsed.netloc,
                parsed.path.lstrip("/"),
                uri=uri,
                max_bytes=max_bytes,
            )
        raise ArtifactStoreError(f"Unsupported artifact URI scheme: {parsed.scheme}")

    def read_text(self, uri: str, *, max_bytes: int | None = None) -> str:
        """Read UTF-8 text with the same optional byte boundary as :meth:`read_bytes`."""

        return self.read_bytes(uri, max_bytes=max_bytes).decode("utf-8")

    def _read_s3_bytes(
        self,
        bucket: str,
        key: str,
        *,
        uri: str,
        max_bytes: int | None,
    ) -> bytes:
        try:
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        except Exception as exc:  # noqa: BLE001 - optional Airflow provider dependency.
            raise ArtifactStoreError("Airflow S3 provider is unavailable for s3:// artifacts") from exc
        hook = S3Hook(aws_conn_id=self.reader_connection_id)
        obj = hook.get_key(key=key, bucket_name=bucket)
        if obj is None:
            raise ArtifactStoreError(f"S3 artifact is missing: {redact_artifact_uri(uri)}")
        response = obj.get()
        declared_bytes = response.get("ContentLength")
        if max_bytes is not None and isinstance(declared_bytes, int) and declared_bytes > max_bytes:
            raise _oversize_error(uri, max_bytes=max_bytes)
        return _read_body(response["Body"], uri=uri, max_bytes=max_bytes)


def _read_local_bytes(path: Path, *, uri: str, max_bytes: int | None) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise ArtifactStoreError(f"Local artifact could not be opened safely: {redact_artifact_uri(uri)}") from exc
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ArtifactStoreError(f"Local artifact must be a regular file: {redact_artifact_uri(uri)}")
        if max_bytes is not None and metadata.st_size > max_bytes:
            raise _oversize_error(uri, max_bytes=max_bytes)
        limit = metadata.st_size if max_bytes is None else max_bytes
        payload = _read_descriptor(descriptor, limit + 1)
        if max_bytes is not None and len(payload) > max_bytes:
            raise _oversize_error(uri, max_bytes=max_bytes)
        return payload
    finally:
        os.close(descriptor)


def _read_descriptor(descriptor: int, limit: int) -> bytes:
    chunks: list[bytes] = []
    remaining = max(0, limit)
    while remaining:
        chunk = os.read(descriptor, min(64 * 1024, remaining))
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _read_body(body: _ReadableBody, *, uri: str, max_bytes: int | None) -> bytes:
    payload = bytes(body.read() if max_bytes is None else body.read(max_bytes + 1))
    if max_bytes is not None and len(payload) > max_bytes:
        raise _oversize_error(uri, max_bytes=max_bytes)
    return payload


def _validate_max_bytes(max_bytes: int | None) -> None:
    if max_bytes is not None and max_bytes <= 0:
        raise ArtifactStoreError("max_bytes must be positive or None")


def _oversize_error(uri: str, *, max_bytes: int) -> ArtifactStoreError:
    return ArtifactStoreError(f"artifact exceeds max_bytes={max_bytes}: {redact_artifact_uri(uri)}")


__all__ = ["ArtifactReadPort", "ArtifactReader", "ArtifactStoreError"]
