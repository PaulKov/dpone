"""Confined compare-and-swap storage for workload-init catalogs."""

from __future__ import annotations

import hashlib
import os
import secrets
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from dpone.manifest.confined_files import ConfinedFileError, read_confined_file
from dpone.manifest.confined_mutations import ConfinedMutationError, replace_file_if_digest
from dpone.readiness.airflow_pipeline_source import (
    ConcurrentAuthoringCreate,
    ConfinedAuthoringFileSystem,
    ConfinedAuthoringPathError,
    ConfinedFileCreation,
)

CATALOG_MAX_BYTES = 4 * 1024 * 1024
_DIRECTORY_FLAGS = (
    os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
)


class CatalogPatchConflict(OSError):
    """The catalog path or expected state is unsafe to update."""

    def __init__(self, message: str, *, recovery_artifacts: tuple[str, ...] = ()) -> None:
        super().__init__(message)
        self.recovery_artifacts = recovery_artifacts


@dataclass(frozen=True)
class CatalogPatchReceipt:
    """Exact before/after state needed for a no-clobber rollback."""

    path: Path
    before: bytes | None
    after: bytes
    creation: ConfinedFileCreation | None = None


def read_catalog_text(*, repo_root: Path, path: Path) -> str | None:
    """Read one bounded catalog through the repository confinement boundary."""

    return _CatalogFileSystem(repo_root).read(path)


def verify_catalog_content(*, repo_root: Path, path: Path, expected: bytes) -> None:
    """Verify that a planned no-op still observes the exact catalog bytes."""

    _CatalogFileSystem(repo_root).verify(path, expected=expected)


def apply_catalog_content(
    *,
    repo_root: Path,
    path: Path,
    expected: bytes | None,
    desired: bytes,
) -> CatalogPatchReceipt | None:
    """Apply expected catalog bytes and return the exact rollback receipt."""

    outcome = _CatalogFileSystem(repo_root).compare_and_swap(path, expected=expected, desired=desired)
    if outcome is False:
        return None
    creation = outcome if isinstance(outcome, ConfinedFileCreation) else None
    return CatalogPatchReceipt(path=path, before=expected, after=desired, creation=creation)


def rollback_catalog_patch(receipt: CatalogPatchReceipt, *, repo_root: Path) -> None:
    """Restore one catalog receipt without overwriting a concurrent winner."""

    if receipt.before is None:
        if receipt.creation is not None:
            outcome = ConfinedAuthoringFileSystem(repo_root).rollback(receipt.creation)
            if outcome.recovery_path is not None:
                raise CatalogPatchConflict(
                    "Catalog rollback preserved a recovery artifact.",
                    recovery_artifacts=(outcome.recovery_path.as_posix(),),
                )
        return
    _CatalogFileSystem(repo_root).compare_and_swap(
        receipt.path,
        expected=receipt.after,
        desired=receipt.before,
    )


class _CatalogFileSystem:
    """Descriptor-confined, no-clobber catalog reads and compare-and-swap writes."""

    def __init__(self, root: Path) -> None:
        self._root = root.resolve(strict=True)
        self._creator = ConfinedAuthoringFileSystem(self._root)

    def read(self, path: Path) -> str | None:
        payload = self._read_bytes(path)
        return payload.decode("utf-8") if payload is not None else None

    def verify(self, path: Path, *, expected: bytes) -> None:
        if self._read_bytes(path) != expected:
            raise CatalogPatchConflict("Catalog changed after planning.")

    def _read_bytes(self, path: Path) -> bytes | None:
        relative = _safe_relative(path)
        try:
            payload = read_confined_file(self._root, relative, max_bytes=CATALOG_MAX_BYTES)
        except ConfinedFileError as exc:
            if exc.code == "file_not_found":
                return None
            raise CatalogPatchConflict("Catalog path could not be accessed safely.") from exc
        return payload

    def compare_and_swap(
        self,
        path: Path,
        *,
        expected: bytes | None,
        desired: bytes,
    ) -> ConfinedFileCreation | bool:
        _safe_relative(path)
        if expected is None:
            try:
                return self._creator.create(path, desired) or False
            except (ConcurrentAuthoringCreate, ConfinedAuthoringPathError) as exc:
                raise CatalogPatchConflict("Catalog changed after planning.") from exc
        self._replace_expected(path, expected=expected, desired=desired)
        return True

    def _replace_expected(self, path: Path, *, expected: bytes, desired: bytes) -> None:
        with _open_existing_parent(self._root, path) as parent_fd:
            leaf = path.name
            try:
                metadata = os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False)
            except OSError as exc:
                raise CatalogPatchConflict("Catalog changed after planning.") from exc
            if not stat.S_ISREG(metadata.st_mode):
                raise CatalogPatchConflict("Catalog target must be a regular file.")
            temporary = _write_temporary(parent_fd, leaf, desired, mode=stat.S_IMODE(metadata.st_mode))
            try:
                replace_file_if_digest(
                    parent_fd,
                    leaf,
                    temporary,
                    expected_sha256=_sha256(expected),
                    max_bytes=CATALOG_MAX_BYTES,
                )
            except ConfinedMutationError as exc:
                raise CatalogPatchConflict(
                    "Catalog changed after planning.",
                    recovery_artifacts=_catalog_recovery_artifacts(plan_path=path, recovery_name=exc.recovery_name),
                ) from exc
            except OSError as exc:
                raise CatalogPatchConflict("Catalog changed after planning.") from exc
            finally:
                try:
                    os.unlink(temporary, dir_fd=parent_fd)
                except FileNotFoundError:
                    pass


@contextmanager
def _open_existing_parent(root: Path, path: Path) -> Iterator[int]:
    parts = PurePosixPath(_safe_relative(path)).parts
    try:
        descriptor = os.open(root, _DIRECTORY_FLAGS)
    except OSError as exc:
        raise CatalogPatchConflict("Catalog path could not be accessed safely.") from exc
    try:
        try:
            for part in parts[:-1]:
                child = os.open(part, _DIRECTORY_FLAGS, dir_fd=descriptor)
                os.close(descriptor)
                descriptor = child
        except OSError as exc:
            raise CatalogPatchConflict("Catalog path could not be accessed safely.") from exc
        yield descriptor
    finally:
        os.close(descriptor)


def _write_temporary(parent_fd: int, leaf: str, content: bytes, *, mode: int) -> str:
    temporary = f".{leaf}.dpone-cas-{secrets.token_hex(12)}"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(temporary, flags, 0o666, dir_fd=parent_fd)
    try:
        os.fchmod(descriptor, mode)
        _write_all(descriptor, content)
        os.fsync(descriptor)
    except Exception:
        try:
            os.unlink(temporary, dir_fd=parent_fd)
        except FileNotFoundError:
            pass
        raise
    finally:
        os.close(descriptor)
    return temporary


def _safe_relative(path: Path) -> str:
    raw = path.as_posix()
    parsed = PurePosixPath(raw)
    if (
        not raw
        or not parsed.parts
        or "\\" in raw
        or "\x00" in raw
        or parsed.is_absolute()
        or "." in parsed.parts
        or ".." in parsed.parts
    ):
        raise CatalogPatchConflict("Catalog path must be a normalized project-relative path.")
    return parsed.as_posix()


def _catalog_recovery_artifacts(*, plan_path: Path, recovery_name: str | None) -> tuple[str, ...]:
    if recovery_name is None:
        return ()
    parent = PurePosixPath(_safe_relative(plan_path)).parent
    return ((parent / recovery_name).as_posix(),)


def _write_all(descriptor: int, content: bytes) -> None:
    remaining = memoryview(content)
    while remaining:
        written = os.write(descriptor, remaining)
        if written <= 0:
            raise OSError("Catalog write made no progress.")
        remaining = remaining[written:]


def _sha256(content: bytes) -> str:
    return "sha256:" + hashlib.sha256(content).hexdigest()


__all__ = [
    "CATALOG_MAX_BYTES",
    "CatalogPatchConflict",
    "CatalogPatchReceipt",
    "apply_catalog_content",
    "read_catalog_text",
    "rollback_catalog_patch",
    "verify_catalog_content",
]
