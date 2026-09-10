"""Reusable deterministic integrity inventory for bounded local artifact trees."""

from __future__ import annotations

import hashlib
import os
import stat
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from dpone.contracts.tree_integrity_subject import encode_tree_integrity_subject
from dpone.manifest.confined_files import (
    ConfinedFileError,
    read_confined_file,
    read_confined_file_snapshot,
)


class BoundedTreeIntegrityError(ValueError):
    """An artifact tree cannot be inventoried or verified safely."""


@dataclass(frozen=True, slots=True)
class BoundedTreeIntegrityPolicy:
    """Limits and stable wire names for one integrity subject."""

    subject_filename: str
    schema_header: str
    required_paths: tuple[str, ...]
    max_files: int
    max_file_bytes: int
    max_inventory_bytes: int
    max_total_bytes: int


@dataclass(frozen=True, slots=True)
class BoundedTreeIntegrityReport:
    """Result of writing or verifying one deterministic tree inventory."""

    subject_path: str
    subject_sha256: str
    file_count: int
    total_bytes: int
    no_op: bool


@dataclass(frozen=True, slots=True)
class _TreeFile:
    path: str
    sha256: str
    bytes: int


class BoundedTreeIntegrityService:
    """Create and verify one create-only checksum subject."""

    def __init__(self, policy: BoundedTreeIntegrityPolicy) -> None:
        self._policy = policy

    def write(self, root: Path) -> BoundedTreeIntegrityReport:
        artifact_root = self._validated_root(root)
        files = self._scan(artifact_root)
        encoded = self._encode(files)
        target = artifact_root / self._policy.subject_filename
        no_op = self._install_create_only(artifact_root, target, encoded)
        return self._report(encoded, files, no_op=no_op)

    def verify(self, root: Path) -> BoundedTreeIntegrityReport:
        artifact_root = self._validated_root(root)
        try:
            encoded = read_confined_file(
                artifact_root,
                self._policy.subject_filename,
                max_bytes=self._policy.max_inventory_bytes,
            )
        except (ConfinedFileError, OSError) as exc:
            raise BoundedTreeIntegrityError("checksum subject is missing or unsafe") from exc
        expected = self._parse(encoded)
        actual = self._scan(artifact_root)
        if self._identities(expected) != self._identities(actual):
            raise BoundedTreeIntegrityError("artifact bytes differ from checksum subject")
        return self._report(encoded, actual, no_op=True)

    def _validated_root(self, value: Path) -> Path:
        root = Path(value).absolute()
        try:
            metadata = root.lstat()
        except OSError as exc:
            raise BoundedTreeIntegrityError("artifact root does not exist") from exc
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise BoundedTreeIntegrityError("artifact root must be a real directory")
        return root

    def _scan(self, root: Path) -> tuple[_TreeFile, ...]:
        files: list[_TreeFile] = []
        total_bytes = 0
        try:
            candidates = sorted(
                root.rglob("*"),
                key=lambda item: item.relative_to(root).as_posix(),
            )
        except OSError as exc:
            raise BoundedTreeIntegrityError("artifact tree cannot be scanned safely") from exc
        for candidate in candidates:
            relative = candidate.relative_to(root).as_posix()
            _validate_relative_path(relative)
            try:
                metadata = candidate.lstat()
            except OSError as exc:
                raise BoundedTreeIntegrityError("artifact tree changed during scan") from exc
            if stat.S_ISLNK(metadata.st_mode):
                raise BoundedTreeIntegrityError("artifact tree contains a symbolic link")
            if stat.S_ISDIR(metadata.st_mode):
                continue
            if not stat.S_ISREG(metadata.st_mode):
                raise BoundedTreeIntegrityError("artifact tree contains a forbidden entry")
            if relative == self._policy.subject_filename:
                continue
            if metadata.st_size > self._policy.max_file_bytes:
                raise BoundedTreeIntegrityError("artifact file exceeds the byte limit")
            try:
                snapshot = read_confined_file_snapshot(
                    root,
                    relative,
                    max_bytes=self._policy.max_file_bytes,
                )
            except (ConfinedFileError, OSError) as exc:
                raise BoundedTreeIntegrityError("artifact file cannot be read safely") from exc
            total_bytes += snapshot.identity.size
            if total_bytes > self._policy.max_total_bytes:
                raise BoundedTreeIntegrityError("artifact tree exceeds the total byte limit")
            files.append(
                _TreeFile(
                    path=relative,
                    sha256=snapshot.sha256.removeprefix("sha256:"),
                    bytes=snapshot.identity.size,
                )
            )
            if len(files) > self._policy.max_files:
                raise BoundedTreeIntegrityError("artifact tree exceeds the file-count limit")
        present = {item.path for item in files}
        if not files or any(required not in present for required in self._policy.required_paths):
            raise BoundedTreeIntegrityError("artifact tree is missing a required file")
        return tuple(files)

    def _encode(self, files: tuple[_TreeFile, ...]) -> bytes:
        encoded = encode_tree_integrity_subject(
            self._policy.schema_header, ((item.path, item.sha256) for item in files)
        )
        if len(encoded) > self._policy.max_inventory_bytes:
            raise BoundedTreeIntegrityError("checksum subject exceeds its byte limit")
        return encoded

    def _parse(self, encoded: bytes) -> tuple[_TreeFile, ...]:
        try:
            text = encoded.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise BoundedTreeIntegrityError("checksum subject is not valid UTF-8") from exc
        lines = text.splitlines()
        if not lines or lines[0] != self._policy.schema_header or not text.endswith("\n"):
            raise BoundedTreeIntegrityError("checksum subject format is unsupported")
        files: list[_TreeFile] = []
        seen: set[str] = set()
        for line in lines[1:]:
            digest, separator, path = line.partition("  ")
            if (
                separator != "  "
                or len(digest) != 64
                or any(character not in "0123456789abcdef" for character in digest)
            ):
                raise BoundedTreeIntegrityError("checksum subject has an invalid record")
            _validate_relative_path(path)
            if path == self._policy.subject_filename or path in seen:
                raise BoundedTreeIntegrityError("checksum subject has a recursive or duplicate path")
            seen.add(path)
            files.append(_TreeFile(path=path, sha256=digest, bytes=0))
            if len(files) > self._policy.max_files:
                raise BoundedTreeIntegrityError("checksum subject exceeds the file-count limit")
        if [item.path for item in files] != sorted(seen):
            raise BoundedTreeIntegrityError("checksum subject paths are not canonical")
        return tuple(files)

    def _install_create_only(self, root: Path, target: Path, encoded: bytes) -> bool:
        if target.exists():
            return self._verify_existing_subject(root, encoded)
        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb",
                dir=root,
                prefix=f".{target.name}.",
                suffix=".tmp",
                delete=False,
            ) as handle:
                temporary_path = Path(handle.name)
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(temporary_path, target)
            except FileExistsError:
                return self._verify_existing_subject(root, encoded)
            _fsync_directory(root)
            return False
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)

    def _verify_existing_subject(self, root: Path, encoded: bytes) -> bool:
        try:
            current = read_confined_file(
                root,
                self._policy.subject_filename,
                max_bytes=self._policy.max_inventory_bytes,
            )
        except (ConfinedFileError, OSError) as exc:
            raise BoundedTreeIntegrityError("existing checksum subject is unsafe") from exc
        if current != encoded:
            raise BoundedTreeIntegrityError("existing checksum subject conflicts with artifact bytes")
        return True

    @staticmethod
    def _identities(files: tuple[_TreeFile, ...]) -> tuple[tuple[str, str], ...]:
        return tuple((item.path, item.sha256) for item in files)

    def _report(
        self,
        encoded: bytes,
        files: tuple[_TreeFile, ...],
        *,
        no_op: bool,
    ) -> BoundedTreeIntegrityReport:
        return BoundedTreeIntegrityReport(
            subject_path=self._policy.subject_filename,
            subject_sha256="sha256:" + hashlib.sha256(encoded).hexdigest(),
            file_count=len(files),
            total_bytes=sum(item.bytes for item in files),
            no_op=no_op,
        )


def _validate_relative_path(value: str) -> None:
    path = PurePosixPath(value)
    if (
        not value
        or "\\" in value
        or path.is_absolute()
        or path.as_posix() != value
        or any(part in {"", ".", ".."} for part in path.parts)
        or any(ord(character) < 32 for character in value)
    ):
        raise BoundedTreeIntegrityError("checksum subject contains an unsafe path")


def _fsync_directory(directory: Path) -> None:
    try:
        descriptor = os.open(directory, os.O_RDONLY)
    except OSError as exc:
        raise BoundedTreeIntegrityError("artifact directory could not be opened for durability") from exc
    try:
        os.fsync(descriptor)
    except OSError as exc:
        raise BoundedTreeIntegrityError("artifact checksum subject durability could not be proven") from exc
    finally:
        os.close(descriptor)


__all__ = [
    "BoundedTreeIntegrityError",
    "BoundedTreeIntegrityPolicy",
    "BoundedTreeIntegrityReport",
    "BoundedTreeIntegrityService",
]
