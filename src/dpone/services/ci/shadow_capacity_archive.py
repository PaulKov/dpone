"""Authenticate Actions ZIP transport before reading capacity JSON payloads."""

from __future__ import annotations

import hashlib
import io
import stat
import zipfile
from dataclasses import dataclass


class CapacityArchiveError(ValueError):
    """Artifact transport or member layout is unsafe, ambiguous, or malformed."""


@dataclass(frozen=True)
class ArchiveIdentity:
    """Provider-authenticated ZIP archive identity."""

    size_in_bytes: int
    sha256: str


@dataclass(frozen=True)
class JsonPayload:
    """One separately bounded direct JSON payload extracted from an archive."""

    payload: bytes
    sha256: str


def verify_provider_archive(
    archive: bytes,
    *,
    provider_size: int,
    provider_digest: str,
    max_archive_bytes: int,
) -> ArchiveIdentity:
    """Bind provider metadata to the exact downloaded ZIP bytes, never JSON bytes."""

    if provider_size < 1 or max_archive_bytes < 1 or provider_size > max_archive_bytes:
        raise CapacityArchiveError("provider archive size is outside the approved bound")
    if len(archive) != provider_size or len(archive) > max_archive_bytes:
        raise CapacityArchiveError("downloaded archive length does not match provider metadata")
    digest = _sha256(archive)
    if provider_digest != digest:
        raise CapacityArchiveError("downloaded archive digest does not match provider metadata")
    return ArchiveIdentity(size_in_bytes=len(archive), sha256=digest)


def extract_single_json_payload(archive: bytes, *, expected_name: str, max_payload_bytes: int) -> JsonPayload:
    """Extract exactly one confined regular JSON member without writing to disk."""

    if not expected_name or _unsafe_path(expected_name) or max_payload_bytes < 1:
        raise CapacityArchiveError("expected payload configuration is unsafe")
    try:
        with zipfile.ZipFile(io.BytesIO(archive)) as source:
            entries = source.infolist()
            if len(entries) != 1:
                raise CapacityArchiveError("artifact archive must contain exactly one payload member")
            entry = entries[0]
            if entry.filename != expected_name or _unsafe_path(entry.filename) or entry.is_dir():
                raise CapacityArchiveError("artifact payload member identity is invalid")
            if _is_symlink(entry) or entry.flag_bits & 0x1:
                raise CapacityArchiveError("artifact payload member is not a regular unencrypted file")
            if entry.compress_type not in {zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED}:
                raise CapacityArchiveError("artifact payload compression is unsupported")
            if entry.file_size < 1 or entry.file_size > max_payload_bytes:
                raise CapacityArchiveError("artifact payload size is outside the approved bound")
            with source.open(entry, "r") as member:
                payload = member.read(max_payload_bytes + 1)
    except (OSError, RuntimeError, zipfile.BadZipFile, zipfile.LargeZipFile) as exc:
        raise CapacityArchiveError("artifact archive cannot be safely read") from exc
    if len(payload) != entry.file_size or len(payload) > max_payload_bytes:
        raise CapacityArchiveError("artifact payload byte count is inconsistent")
    return JsonPayload(payload=payload, sha256=_sha256(payload))


def _is_symlink(entry: zipfile.ZipInfo) -> bool:
    return stat.S_IFMT(entry.external_attr >> 16) == stat.S_IFLNK


def _unsafe_path(path: str) -> bool:
    return path.startswith("/") or "\x00" in path or ".." in path.split("/")


def _sha256(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


__all__ = [
    "ArchiveIdentity",
    "CapacityArchiveError",
    "JsonPayload",
    "extract_single_json_payload",
    "verify_provider_archive",
]
