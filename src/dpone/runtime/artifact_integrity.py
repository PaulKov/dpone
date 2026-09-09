"""Immutable receipts for runtime-owned file transfer artifacts."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from dataclasses import dataclass, replace
from typing import Any, NoReturn


class ArtifactIntegrityError(RuntimeError):
    """Stable typed failure raised before target mutation."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class FileWireContract:
    """Frozen interpretation of file bytes at a staging boundary."""

    columns: tuple[str, ...]
    format: str
    compressed: bool
    has_header: bool
    codec: tuple[tuple[str, str], ...] | None
    version: int = 1

    @classmethod
    def resolve(
        cls,
        *,
        columns: tuple[str, ...],
        format: str,
        compressed: bool,
        has_header: bool,
        bulk_text_codec: Any | None,
    ) -> FileWireContract:
        codec = None
        if bulk_text_codec is not None:
            codec_id = getattr(bulk_text_codec, "codec_id", None)
            codec_version = getattr(bulk_text_codec, "codec_version", None)
            if (
                not isinstance(codec_id, str)
                or not codec_id
                or isinstance(codec_version, bool)
                or not isinstance(codec_version, int)
            ):
                raise ArtifactIntegrityError("artifact_integrity.codec_contract_invalid")
            codec = tuple(
                sorted(
                    (
                        ("codec_id", codec_id),
                        ("codec_version", str(codec_version)),
                        ("empty_string_marker", str(getattr(bulk_text_codec, "empty_string_marker", ""))),
                        ("field_terminator", str(getattr(bulk_text_codec, "field_terminator", ""))),
                        ("marker_prefix", str(getattr(bulk_text_codec, "marker_prefix", ""))),
                        ("row_terminator", str(getattr(bulk_text_codec, "row_terminator", ""))),
                    )
                )
            )
        return cls(
            columns=tuple(str(column) for column in columns),
            format=str(format).strip().lower(),
            compressed=bool(compressed),
            has_header=bool(has_header),
            codec=codec,
        )

    @property
    def sha256(self) -> str:
        payload = {
            "codec": self.codec,
            "columns": self.columns,
            "compressed": self.compressed,
            "format": self.format,
            "has_header": self.has_header,
            "version": self.version,
        }
        encoded = json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
        return hashlib.sha256(encoded.encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class CompletedFileWrite:
    """Digest and cardinality evidence produced while writing one file."""

    sha256: str
    size_bytes: int
    rows_exported: int | None


@dataclass(frozen=True, slots=True)
class FileIdentity:
    """Immutable physical identity of one regular filesystem object."""

    device: int
    inode: int
    size: int
    mtime_ns: int

    @classmethod
    def from_stat(cls, value: os.stat_result) -> FileIdentity:
        """Project only the fields required at the artifact trust boundary."""

        return cls(
            device=int(value.st_dev),
            inode=int(value.st_ino),
            size=int(value.st_size),
            mtime_ns=int(value.st_mtime_ns),
        )

    def same_object(self, other: FileIdentity) -> bool:
        """Return whether two observations name the same physical object."""

        return (self.device, self.inode) == (other.device, other.inode)


@dataclass(frozen=True, slots=True)
class FileOwnedScope:
    """Canonical directory authority captured with a source-owned file."""

    path: str
    device: int
    inode: int

    @classmethod
    def capture(cls, file_path: str) -> FileOwnedScope:
        """Freeze the canonical parent directory and its physical identity."""

        parent = _canonical_parent(file_path)
        try:
            details = os.lstat(parent)
        except OSError as exc:
            raise ArtifactIntegrityError("artifact_integrity.file_unavailable") from exc
        if not stat.S_ISDIR(details.st_mode):
            raise ArtifactIntegrityError("artifact_integrity.owned_scope_unavailable")
        return cls(path=parent, device=int(details.st_dev), inode=int(details.st_ino))

    def verify(self, file_path: str) -> None:
        """Reject a path rebound outside or away from the captured directory."""

        parent = _canonical_parent(file_path)
        if parent != self.path:
            raise ArtifactIntegrityError("artifact_integrity.owned_scope_mismatch")
        try:
            details = os.lstat(parent)
        except OSError as exc:
            raise ArtifactIntegrityError("artifact_integrity.file_unavailable") from exc
        exact = (
            stat.S_ISDIR(details.st_mode) and int(details.st_dev) == self.device and int(details.st_ino) == self.inode
        )
        if not exact:
            raise ArtifactIntegrityError("artifact_integrity.owned_scope_mismatch")


@dataclass(frozen=True, slots=True)
class FileArtifactReceipt:
    """Frozen bytes, wire interpretation, and exported-row authority."""

    sha256: str
    size_bytes: int
    wire_contract_sha256: str
    identity: FileIdentity
    owned_scope: FileOwnedScope
    rows_exported: int | None = None

    @classmethod
    def capture(
        cls,
        path: str,
        *,
        wire_contract: FileWireContract,
        rows_exported: int | None = None,
    ) -> FileArtifactReceipt:
        owned_scope = FileOwnedScope.capture(path)
        identity, digest = _file_identity(path)
        owned_scope.verify(path)
        return cls(
            sha256=digest,
            size_bytes=identity.size,
            wire_contract_sha256=wire_contract.sha256,
            identity=identity,
            owned_scope=owned_scope,
            rows_exported=_rows(rows_exported),
        )

    @classmethod
    def capture_descriptor(
        cls,
        descriptor: int,
        *,
        owned_scope: FileOwnedScope,
        wire_contract: FileWireContract,
        rows_exported: int | None = None,
    ) -> FileArtifactReceipt:
        """Freeze bytes from an already pinned, possibly unnamed POSIX file."""

        from dpone.runtime.pinned_file_integrity import descriptor_file_identity

        identity, digest = descriptor_file_identity(descriptor)
        return cls(
            sha256=digest,
            size_bytes=identity.size,
            wire_contract_sha256=wire_contract.sha256,
            identity=identity,
            owned_scope=owned_scope,
            rows_exported=_rows(rows_exported),
        )

    @classmethod
    def capture_completed_write(
        cls,
        path: str,
        *,
        wire_contract: FileWireContract,
        sha256: str,
        size_bytes: int,
        rows_exported: int | None = None,
    ) -> FileArtifactReceipt:
        """Freeze digest evidence computed by the writer's sequential pass.

        This avoids rereading a completed large file merely to reproduce work
        the producer already performed.  Physical identity and owned scope are
        still captured here, and :meth:`verify` independently rehashes the
        exact file at the consumer boundary before BCP.
        """

        digest = str(sha256).strip().lower()
        if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
            raise ArtifactIntegrityError("artifact_integrity.sha256_invalid")
        if isinstance(size_bytes, bool) or not isinstance(size_bytes, int) or size_bytes < 0:
            raise ArtifactIntegrityError("artifact_integrity.byte_count_invalid")
        owned_scope = FileOwnedScope.capture(path)
        try:
            details = os.lstat(path)
        except OSError as exc:
            raise ArtifactIntegrityError("artifact_integrity.file_unavailable") from exc
        if not stat.S_ISREG(details.st_mode):
            raise ArtifactIntegrityError("artifact_integrity.regular_file_required")
        identity = FileIdentity.from_stat(details)
        if identity.size != size_bytes:
            raise ArtifactIntegrityError("artifact_integrity.byte_count_mismatch")
        owned_scope.verify(path)
        return cls(
            sha256=digest,
            size_bytes=size_bytes,
            wire_contract_sha256=wire_contract.sha256,
            identity=identity,
            owned_scope=owned_scope,
            rows_exported=_rows(rows_exported),
        )

    def with_rows_exported(
        self,
        path: str,
        rows_exported: int,
        *,
        wire_contract: FileWireContract,
    ) -> FileArtifactReceipt:
        """Attach row authority once, after proving the bytes did not change."""

        self.verify(path, wire_contract=wire_contract)
        rows = _rows(rows_exported)
        if self.rows_exported is not None and self.rows_exported != rows:
            raise ArtifactIntegrityError("artifact_integrity.rows_exported_immutable")
        return replace(self, rows_exported=rows)

    def verify(self, path: str, *, wire_contract: FileWireContract) -> None:
        """Reject byte tampering or a changed interpretation of the bytes."""

        if wire_contract.sha256 != self.wire_contract_sha256:
            raise ArtifactIntegrityError("artifact_integrity.wire_contract_mismatch")
        self.owned_scope.verify(path)
        identity, digest = _file_identity(path)
        if not identity.same_object(self.identity):
            raise ArtifactIntegrityError("artifact_integrity.file_identity_mismatch")
        if identity.size != self.size_bytes:
            raise ArtifactIntegrityError("artifact_integrity.byte_count_mismatch")
        if digest != self.sha256:
            raise ArtifactIntegrityError("artifact_integrity.sha256_mismatch")
        if identity != self.identity:
            raise ArtifactIntegrityError("artifact_integrity.file_identity_mismatch")

    def verify_descriptor(self, descriptor: int, *, wire_contract: FileWireContract) -> None:
        """Rehash the same held descriptor without trusting a mutable basename."""

        if wire_contract.sha256 != self.wire_contract_sha256:
            raise ArtifactIntegrityError("artifact_integrity.wire_contract_mismatch")
        from dpone.runtime.pinned_file_integrity import descriptor_file_identity

        identity, digest = descriptor_file_identity(descriptor)
        if not identity.same_object(self.identity):
            raise ArtifactIntegrityError("artifact_integrity.file_identity_mismatch")
        if identity.size != self.size_bytes:
            raise ArtifactIntegrityError("artifact_integrity.byte_count_mismatch")
        if digest != self.sha256:
            raise ArtifactIntegrityError("artifact_integrity.sha256_mismatch")
        if identity != self.identity:
            raise ArtifactIntegrityError("artifact_integrity.file_identity_mismatch")

    def require_rows_exported(self) -> int:
        if self.rows_exported is None:
            raise ArtifactIntegrityError("artifact_integrity.rows_exported_missing")
        return self.rows_exported


def _file_identity(path: str) -> tuple[FileIdentity, str]:
    """Hash one no-follow descriptor and prove its path remained attached."""

    try:
        path_before = os.lstat(path)
    except OSError as exc:
        raise ArtifactIntegrityError("artifact_integrity.file_unavailable") from exc
    if not stat.S_ISREG(path_before.st_mode):
        raise ArtifactIntegrityError("artifact_integrity.regular_file_required")
    path_identity = FileIdentity.from_stat(path_before)

    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_BINARY", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        _raise_open_failure(path, expected=path_identity, error=exc)

    digest = hashlib.sha256()
    try:
        descriptor_before = os.fstat(descriptor)
        if not stat.S_ISREG(descriptor_before.st_mode):
            raise ArtifactIntegrityError("artifact_integrity.file_replaced_during_hash")
        before = FileIdentity.from_stat(descriptor_before)
        if not before.same_object(path_identity):
            raise ArtifactIntegrityError("artifact_integrity.file_replaced_during_hash")
        with os.fdopen(descriptor, "rb", closefd=True) as handle:
            descriptor = -1
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
            descriptor_after = os.fstat(handle.fileno())
    except OSError as exc:
        raise ArtifactIntegrityError("artifact_integrity.file_unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)

    if not stat.S_ISREG(descriptor_after.st_mode):
        raise ArtifactIntegrityError("artifact_integrity.file_replaced_during_hash")
    after = FileIdentity.from_stat(descriptor_after)
    if not after.same_object(before):
        raise ArtifactIntegrityError("artifact_integrity.file_replaced_during_hash")
    if after != before:
        raise ArtifactIntegrityError("artifact_integrity.file_changed_during_hash")

    try:
        path_after = os.lstat(path)
    except OSError as exc:
        raise ArtifactIntegrityError("artifact_integrity.file_replaced_during_hash") from exc
    if not stat.S_ISREG(path_after.st_mode):
        raise ArtifactIntegrityError("artifact_integrity.file_replaced_during_hash")
    final = FileIdentity.from_stat(path_after)
    if not final.same_object(after):
        raise ArtifactIntegrityError("artifact_integrity.file_replaced_during_hash")
    if final != after:
        raise ArtifactIntegrityError("artifact_integrity.file_changed_during_hash")
    return final, digest.hexdigest()


def _raise_open_failure(path: str, *, expected: FileIdentity, error: OSError) -> NoReturn:
    """Classify a no-follow open failure without losing replacement evidence."""

    try:
        current_stat = os.lstat(path)
    except OSError as exc:
        raise ArtifactIntegrityError("artifact_integrity.file_unavailable") from exc
    if not stat.S_ISREG(current_stat.st_mode):
        raise ArtifactIntegrityError("artifact_integrity.file_replaced_during_hash") from error
    current = FileIdentity.from_stat(current_stat)
    if not current.same_object(expected):
        raise ArtifactIntegrityError("artifact_integrity.file_replaced_during_hash") from error
    raise ArtifactIntegrityError("artifact_integrity.file_unavailable") from error


def _canonical_parent(path: str) -> str:
    """Resolve parent aliases without following the artifact path itself."""

    absolute = os.path.abspath(os.fspath(path))
    return os.path.normcase(os.path.realpath(os.path.dirname(absolute)))


def _rows(value: int | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ArtifactIntegrityError("artifact_integrity.rows_exported_invalid")
    return value


__all__ = [
    "ArtifactIntegrityError",
    "FileArtifactReceipt",
    "FileIdentity",
    "FileOwnedScope",
    "FileWireContract",
]
