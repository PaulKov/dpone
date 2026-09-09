"""Race-safe, bounded namespace observations for project discovery."""

from __future__ import annotations

import os
import stat
import sys
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Literal

from dpone.manifest.confined_files import project_relative_path
from dpone.manifest.project_identity import project_identity_fingerprint

NamespaceKind = Literal["missing", "directory", "symlink", "other"]


@dataclass(frozen=True, slots=True)
class NamespaceObservation:
    """One no-follow directory membership token and the entries it describes."""

    path: Path
    label: str
    kind: NamespaceKind
    children: tuple[Path, ...]
    fingerprint: str
    limit: int
    scan_limit: int
    scanned_entries: int
    scan_exceeded: bool


def observe_namespace(
    root: Path,
    path: Path,
    *,
    limit: int,
    scan_limit: int | None = None,
) -> NamespaceObservation | None:
    """Observe a path and one visible child level without following symlinks."""

    label = project_relative_path(root, path)
    effective_scan_limit = limit if scan_limit is None else scan_limit
    try:
        metadata = os.lstat(path)
    except FileNotFoundError:
        return _observation(path, label, "missing", (), (), limit, effective_scan_limit, 0, False)
    except OSError:
        return None
    if stat.S_ISLNK(metadata.st_mode):
        return _observation(path, label, "symlink", (), (), limit, effective_scan_limit, 0, False)
    if not stat.S_ISDIR(metadata.st_mode):
        return _observation(path, label, "other", (), (), limit, effective_scan_limit, 0, False)
    scanned = _scan_directory(
        root,
        path,
        label=label,
        expected=metadata,
        visible_limit=limit,
        scan_limit=effective_scan_limit,
    )
    if scanned is None:
        return None
    children, identities, scanned_entries, scan_exceeded = scanned
    ordered = tuple(sorted(children, key=lambda child: child.name))
    ordered_identities = tuple(sorted(identities, key=lambda item: str(item["name"])))
    return _observation(
        path,
        label,
        "directory",
        ordered,
        ordered_identities,
        limit,
        effective_scan_limit,
        scanned_entries,
        scan_exceeded,
    )


def namespace_unchanged(root: Path, expected: NamespaceObservation) -> bool:
    """Re-observe one namespace using the original bound and compare identity."""

    current = observe_namespace(
        root,
        expected.path,
        limit=expected.limit,
        scan_limit=expected.scan_limit,
    )
    return current is not None and current.fingerprint == expected.fingerprint


def path_kind(path: Path) -> NamespaceKind:
    """Return one path kind without following a final symbolic link."""

    try:
        metadata = os.lstat(path)
    except FileNotFoundError:
        return "missing"
    except OSError:
        return "other"
    if stat.S_ISLNK(metadata.st_mode):
        return "symlink"
    if stat.S_ISDIR(metadata.st_mode):
        return "directory"
    return "other"


def path_contains_symlink(root: Path, relative_path: str) -> bool:
    """Reject a configurable root when any traversed component is a symlink."""

    current = root
    for part in Path(relative_path).parts:
        current = current / part
        try:
            if stat.S_ISLNK(os.lstat(current).st_mode):
                return True
        except FileNotFoundError:
            return False
        except OSError:
            return True
    return False


def _scan_directory(
    root: Path,
    path: Path,
    *,
    label: str,
    expected: os.stat_result,
    visible_limit: int,
    scan_limit: int,
) -> tuple[list[Path], list[dict[str, int | str]], int, bool] | None:
    if sys.platform == "win32":
        return _scan_directory_windows(
            path,
            expected=expected,
            visible_limit=visible_limit,
            scan_limit=scan_limit,
        )
    descriptor = _open_confined_directory(root, label)
    if descriptor is None:
        return None
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISDIR(opened.st_mode) or opened.st_dev != expected.st_dev or opened.st_ino != expected.st_ino:
            return None
        children: list[Path] = []
        identities: list[dict[str, int | str]] = []
        scanned_entries = 0
        scan_exceeded = False
        with os.scandir(descriptor) as entries:
            for entry in entries:
                scanned_entries += 1
                if scanned_entries > scan_limit:
                    scan_exceeded = True
                    break
                child_metadata = entry.stat(follow_symlinks=False)
                identities.append(_entry_identity(entry.name, child_metadata))
                if entry.name.startswith("."):
                    continue
                children.append(path / entry.name)
                if len(children) > visible_limit:
                    break
        return children, identities, scanned_entries, scan_exceeded
    except OSError:
        return None
    finally:
        os.close(descriptor)


def _open_confined_directory(root: Path, label: str) -> int | None:
    flags = os.O_RDONLY | os.O_DIRECTORY | _flag("O_CLOEXEC") | _flag("O_NOFOLLOW")
    root_descriptor: int | None = None
    current_descriptor: int | None = None
    try:
        root_descriptor = os.open(root.resolve(strict=True), flags)
        current_descriptor = root_descriptor
        for part in PurePosixPath(label).parts:
            next_descriptor = os.open(part, flags, dir_fd=current_descriptor)
            if current_descriptor != root_descriptor:
                os.close(current_descriptor)
            current_descriptor = next_descriptor
        if current_descriptor == root_descriptor:
            return os.dup(root_descriptor)
        result = current_descriptor
        current_descriptor = None
        return result
    except OSError:
        return None
    finally:
        if current_descriptor is not None and current_descriptor != root_descriptor:
            os.close(current_descriptor)
        if root_descriptor is not None:
            os.close(root_descriptor)


def _scan_directory_windows(
    path: Path,
    *,
    expected: os.stat_result,
    visible_limit: int,
    scan_limit: int,
) -> tuple[list[Path], list[dict[str, int | str]], int, bool] | None:
    """Keep the Windows compatibility path fail-closed around reparse points."""

    children: list[Path] = []
    identities: list[dict[str, int | str]] = []
    scanned_entries = 0
    scan_exceeded = False
    try:
        for child in path.iterdir():
            scanned_entries += 1
            if scanned_entries > scan_limit:
                scan_exceeded = True
                break
            child_metadata = os.lstat(child)
            identities.append(_entry_identity(child.name, child_metadata))
            if child.name.startswith("."):
                continue
            children.append(child)
            if len(children) > visible_limit:
                break
        after = os.lstat(path)
    except OSError:
        return None
    if (
        stat.S_ISLNK(after.st_mode)
        or not stat.S_ISDIR(after.st_mode)
        or after.st_dev != expected.st_dev
        or after.st_ino != expected.st_ino
    ):
        return None
    return children, identities, scanned_entries, scan_exceeded


def _entry_identity(name: str, metadata: os.stat_result) -> dict[str, int | str]:
    return {
        "name": name,
        "kind": _entry_kind(metadata.st_mode),
        "device": metadata.st_dev,
        "inode": metadata.st_ino,
    }


def _flag(name: str) -> int:
    return int(getattr(os, name, 0))


def _observation(
    path: Path,
    label: str,
    kind: NamespaceKind,
    children: tuple[Path, ...],
    identities: tuple[dict[str, int | str], ...],
    limit: int,
    scan_limit: int,
    scanned_entries: int,
    scan_exceeded: bool,
) -> NamespaceObservation:
    fingerprint = project_identity_fingerprint(
        {
            "schema": "dpone.discovery-namespace-observation.v1",
            "path": label,
            "kind": kind,
            "entries": identities,
        }
    )
    return NamespaceObservation(
        path,
        label,
        kind,
        children,
        fingerprint,
        limit,
        scan_limit,
        scanned_entries,
        scan_exceeded,
    )


def _entry_kind(mode: int) -> str:
    if stat.S_ISDIR(mode):
        return "directory"
    if stat.S_ISREG(mode):
        return "file"
    if stat.S_ISLNK(mode):
        return "symlink"
    return "other"


__all__ = [
    "NamespaceObservation",
    "namespace_unchanged",
    "observe_namespace",
    "path_contains_symlink",
    "path_kind",
]
