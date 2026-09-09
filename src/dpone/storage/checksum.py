"""Integrity-checked local file I/O for object staging artifacts."""

from __future__ import annotations

import hashlib
import os
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import BinaryIO


class BoundedReadOverflow(RuntimeError):
    """Internal signal raised when a remote body exceeds its trusted limit."""


def file_sha256(path: str | Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as fh:
        while chunk := fh.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def is_conditional_create_conflict(exc: BaseException) -> bool:
    """Recognize immutable-create conflicts without importing optional SDKs."""

    status = getattr(exc, "status_code", None) or getattr(exc, "code", None)
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        metadata = response.get("ResponseMetadata")
        if isinstance(metadata, dict):
            status = status or metadata.get("HTTPStatusCode")
    return status in {409, 412, "409", "412", "PreconditionFailed", "ResourceExists"}


class BoundedWriter:
    """Binary writer that rejects payloads exceeding a configured byte limit."""

    def __init__(self, handle: BinaryIO, *, max_bytes: int) -> None:
        self._handle = handle
        self._max_bytes = max_bytes
        self._written = 0

    def write(self, payload: bytes) -> int:
        next_size = self._written + len(payload)
        if next_size > self._max_bytes:
            raise BoundedReadOverflow
        written = self._handle.write(payload)
        self._written += written
        return written

    def flush(self) -> None:
        self._handle.flush()

    def tell(self) -> int:
        return self._written


def reader_chunks(reader: object, *, max_bytes: int) -> Iterable[bytes]:
    """Read at most ``max_bytes + 1`` bytes so the writer can detect overflow."""

    read = getattr(reader, "read")
    remaining = max_bytes + 1
    while remaining > 0:
        chunk = read(min(1024 * 1024, remaining))
        if not chunk:
            return
        payload = bytes(chunk)
        remaining -= len(payload)
        yield payload


def write_bounded_chunks(chunks: Iterable[bytes], target: Path, *, max_bytes: int) -> None:
    """Write an iterable into a new local file without retaining an oversize partial."""

    def download(handle: BoundedWriter) -> None:
        for chunk in chunks:
            handle.write(chunk)

    write_bounded_download(download, target, max_bytes=max_bytes)


def write_bounded_download(
    download: Callable[[BoundedWriter], None],
    target: Path,
    *,
    max_bytes: int,
) -> None:
    """Run a streaming download against an exclusive, bounded local destination."""

    target.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(target, flags, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            bounded = BoundedWriter(handle, max_bytes=max_bytes)
            download(bounded)
            bounded.flush()
            os.fsync(handle.fileno())
    except BaseException:
        if descriptor >= 0:
            os.close(descriptor)
        target.unlink(missing_ok=True)
        raise


__all__ = [
    "BoundedReadOverflow",
    "BoundedWriter",
    "file_sha256",
    "is_conditional_create_conflict",
    "reader_chunks",
    "write_bounded_chunks",
    "write_bounded_download",
]
