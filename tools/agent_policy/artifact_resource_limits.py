"""Fail-closed resource limits for immutable Agent PR receipt artifacts."""

from __future__ import annotations

import io
import stat
import zipfile
import zlib
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import IO

MAX_GITHUB_JSON_BYTES = 4 * 1024 * 1024
MAX_GITHUB_API_PAGES = 100
MAX_GITHUB_API_ITEMS = 10_000
MAX_COMPRESSED_ARTIFACT_BYTES = 16 * 1024 * 1024
MAX_ZIP_MEMBERS = 256
MAX_ZIP_MEMBER_BYTES = 4 * 1024 * 1024
MAX_ZIP_TOTAL_BYTES = 32 * 1024 * 1024
MAX_ZIP_COMPRESSION_RATIO = 100
READ_CHUNK_BYTES = 64 * 1024


@dataclass(frozen=True)
class ArchiveMember:
    """One preflighted and bounded receipt ZIP member."""

    filename: str
    content: bytes
    is_dir: bool


def validate_provider_size(size: int, *, resource: str) -> None:
    """Reject invalid artifact metadata before a provider download starts."""

    if isinstance(size, bool) or not isinstance(size, int) or size <= 0:
        raise ValueError(f"{resource} provider size must be a positive integer")
    if size > MAX_COMPRESSED_ARTIFACT_BYTES:
        raise ValueError(
            f"{resource} provider size {size} exceeds compressed artifact limit {MAX_COMPRESSED_ARTIFACT_BYTES}"
        )


def validate_downloaded_size(content: bytes, *, resource: str) -> None:
    """Reject downloaded bytes that bypassed a bounded provider adapter."""

    if len(content) > MAX_COMPRESSED_ARTIFACT_BYTES:
        raise ValueError(f"{resource} download exceeds compressed artifact limit {MAX_COMPRESSED_ARTIFACT_BYTES}")


def read_bounded(stream: IO[bytes], *, max_bytes: int, resource: str) -> bytes:
    """Read at most ``max_bytes + 1`` bytes and reject an oversized stream."""

    if max_bytes < 0:
        raise ValueError("max_bytes must be non-negative")
    chunks: list[bytes] = []
    observed = 0
    while observed <= max_bytes:
        remaining = max_bytes + 1 - observed
        chunk = stream.read(min(READ_CHUNK_BYTES, remaining))
        if not chunk:
            break
        chunks.append(chunk)
        observed += len(chunk)
    if observed > max_bytes:
        raise ValueError(f"{resource} exceeds byte limit {max_bytes}")
    return b"".join(chunks)


def read_bounded_zip(content: bytes, *, resource: str) -> tuple[ArchiveMember, ...]:
    """Preflight and read one bounded, non-ambiguous receipt ZIP archive."""

    if len(content) > MAX_COMPRESSED_ARTIFACT_BYTES:
        raise ValueError(f"{resource} compressed size {len(content)} exceeds limit {MAX_COMPRESSED_ARTIFACT_BYTES}")
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            infos = archive.infolist()
            _preflight_members(infos, resource=resource)
            return tuple(_read_member(archive, info, resource=resource) for info in infos)
    except (OSError, UnicodeError, zipfile.BadZipFile, zipfile.LargeZipFile, zlib.error) as exc:
        raise ValueError(f"{resource} is not a valid ZIP archive") from exc


def _preflight_members(infos: Iterable[zipfile.ZipInfo], *, resource: str) -> None:
    materialized = tuple(infos)
    if len(materialized) > MAX_ZIP_MEMBERS:
        raise ValueError(f"{resource} member count {len(materialized)} exceeds limit {MAX_ZIP_MEMBERS}")
    seen: set[str] = set()
    aggregate = 0
    for info in materialized:
        _validate_member_identity(info, seen=seen, resource=resource)
        if info.file_size < 0 or info.file_size > MAX_ZIP_MEMBER_BYTES:
            raise ValueError(
                f"{resource} member {info.filename!r} size {info.file_size} "
                f"exceeds member size limit {MAX_ZIP_MEMBER_BYTES}"
            )
        aggregate += info.file_size
        if aggregate > MAX_ZIP_TOTAL_BYTES:
            raise ValueError(f"{resource} aggregate uncompressed size {aggregate} exceeds limit {MAX_ZIP_TOTAL_BYTES}")
        if info.file_size and (
            info.compress_size <= 0 or info.file_size > info.compress_size * MAX_ZIP_COMPRESSION_RATIO
        ):
            raise ValueError(
                f"{resource} member {info.filename!r} exceeds declared compression ratio {MAX_ZIP_COMPRESSION_RATIO}:1"
            )


def _validate_member_identity(
    info: zipfile.ZipInfo,
    *,
    seen: set[str],
    resource: str,
) -> None:
    filename = info.filename
    path = PurePosixPath(filename)
    path_segments = filename.split("/")
    if filename in seen:
        raise ValueError(f"{resource} contains duplicate member {filename!r}")
    seen.add(filename)
    if (
        not filename
        or "\\" in filename
        or any(ord(character) < 32 for character in filename)
        or path.is_absolute()
        or ".." in path.parts
        or (path.parts and path.parts[0].endswith(":"))
        or any(segment in {"", "."} for segment in path_segments[:-1])
        or path_segments[-1] == "."
    ):
        raise ValueError(f"{resource} contains unsafe member path {filename!r}")
    if info.flag_bits & 1:
        raise ValueError(f"{resource} contains encrypted member {filename!r}")
    if stat.S_ISLNK(info.external_attr >> 16):
        raise ValueError(f"{resource} contains symlink member {filename!r}")
    file_type = stat.S_IFMT(info.external_attr >> 16)
    if file_type not in {0, stat.S_IFREG, stat.S_IFDIR}:
        raise ValueError(f"{resource} contains unsafe member type {filename!r}")


def _read_member(
    archive: zipfile.ZipFile,
    info: zipfile.ZipInfo,
    *,
    resource: str,
) -> ArchiveMember:
    if info.is_dir():
        return ArchiveMember(filename=info.filename, content=b"", is_dir=True)
    with archive.open(info) as stream:
        content = read_bounded(
            stream,
            max_bytes=min(info.file_size, MAX_ZIP_MEMBER_BYTES),
            resource=f"{resource} member {info.filename!r}",
        )
    if len(content) != info.file_size:
        raise ValueError(
            f"{resource} member {info.filename!r} declared size {info.file_size} "
            f"does not match extracted size {len(content)}"
        )
    return ArchiveMember(filename=info.filename, content=content, is_dir=False)
