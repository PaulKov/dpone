"""Singleton compatibility and exact workspace content for mirror transactions."""

from __future__ import annotations

import os
import stat
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING

from dpone.contracts.dbt_contract_validation import DbtPublishingError, sha256_bytes
from dpone.contracts.dbt_workspace_promotion import DbtWorkspaceProjectVerification
from dpone.services.dbt_prod_promotion_contract import DbtProdMirrorError

if TYPE_CHECKING:
    from dpone.contracts.dbt_source_inventory import DbtSourceInventory
    from dpone.ports.dbt_project_bundle import DbtProjectBundleOperations


class DbtSingletonMirrorContent:
    """Preserve existing singleton rebuild/no-op and strict staged verification."""

    def __init__(self, archive: bytes, *, bundle_operations: DbtProjectBundleOperations) -> None:
        self._archive = archive
        self._bundles = bundle_operations

    def stage(self, destination: Path) -> None:
        self._bundles.extract(self._archive, destination)
        self.verify(destination)

    def verify(self, destination: Path) -> None:
        self._bundles.verify(self._archive, destination)

    def matches(self, destination: Path) -> bool:
        if not _existing_directory(destination):
            return False
        try:
            return self._bundles.build(destination).archive == self._archive
        except Exception:
            return False


class DbtWorkspaceMirrorContent:
    """Freeze verified project bytes and reject any extra aggregate tree entries.

    The caller must first verify full release membership and authority. This
    adapter rechecks archive hashes when freezing bytes; it does not authenticate
    a release or DEV campaign. One workspace remains one transaction destination.
    """

    def __init__(
        self,
        *,
        inventory: DbtSourceInventory,
        project_bundles: Mapping[str, bytes],
        bundle_operations: DbtProjectBundleOperations,
    ) -> None:
        try:
            self._archives = inventory.bind_project_bundles(project_bundles)
            self._directories = inventory.project_directories
        except ValueError as exc:
            raise DbtProdMirrorError(str(exc)) from exc
        self._bundles = bundle_operations

    def stage(self, destination: Path) -> None:
        destination.mkdir(mode=0o700)
        for path, archive in self._archives.items():
            target = destination / path
            self._bundles.extract(archive, target)
            self._bundles.verify(archive, target)
        self.verify(destination)

    def verify(self, destination: Path) -> None:
        projects, tree_passed = self.observe(destination)
        if not tree_passed or not all(project.passed for project in projects):
            raise DbtProdMirrorError("workspace mirror differs from its complete source inventory")

    def observe(self, destination: Path) -> tuple[tuple[DbtWorkspaceProjectVerification, ...], bool]:
        """Observe all projects independently; never omit failed project B."""

        tree_passed = True
        try:
            _verify_structure(destination, self._directories, frozenset(self._archives))
        except (OSError, ValueError):
            tree_passed = False
        observations = []
        for path, archive in self._archives.items():
            digest = None
            passed = False
            try:
                _require_project_directories(destination, PurePosixPath(path))
                artifact = self._bundles.build(destination / path)
                digest = artifact.bundle.archive_sha256
                files = frozenset(item.path for item in artifact.bundle.files)
                directories = frozenset(parent.as_posix() for item in files for parent in PurePosixPath(item).parents)
                _verify_structure(destination / path, directories, frozenset(), files)
                passed = artifact.archive == archive
            except (OSError, ValueError, DbtPublishingError, RecursionError):
                pass
            observations.append(DbtWorkspaceProjectVerification(path, sha256_bytes(archive), digest, passed))
        return tuple(observations), tree_passed

    def matches(self, destination: Path) -> bool:
        if not _existing_directory(destination):
            return False
        try:
            self.verify(destination)
        except (OSError, ValueError):
            return False
        return True


def _existing_directory(destination: Path) -> bool:
    try:
        metadata = destination.lstat()
    except FileNotFoundError:
        return False
    if not stat.S_ISDIR(metadata.st_mode):
        raise DbtProdMirrorError("existing mirror path is unsafe")
    return True


def _require_project_directories(root: Path, relative: PurePosixPath) -> None:
    """Check project parents before passing a path to the no-follow bundle builder."""

    current = root
    for component in (None, *relative.parts):
        if component is not None:
            current /= component
        if not _existing_directory(current):
            raise DbtProdMirrorError("workspace project directory is missing")


def _verify_structure(
    root: Path, directories: frozenset[str], projects: frozenset[str], files: frozenset[str] = frozenset()
) -> None:
    """Visit only bounded structural parents; project verifiers own their trees.

    Open directory components relative to no-follow descriptors, not resolved
    paths. An unlisted entry fails immediately, before traversing its contents.
    """

    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    observed = {"."}

    def visit(descriptor: int, relative: PurePosixPath) -> None:
        if relative.as_posix() in projects:
            return
        with os.scandir(descriptor) as entries:
            for entry in entries:
                child = relative / entry.name
                key = child.as_posix()
                observed.add(key)
                if key in files:
                    metadata = entry.stat(follow_symlinks=False)
                    if not stat.S_ISREG(metadata.st_mode) or metadata.st_mode & 0o111:
                        raise DbtProdMirrorError("workspace audit file is unsafe or executable")
                    continue
                if key not in directories or not entry.is_dir(follow_symlinks=False):
                    raise DbtProdMirrorError("workspace mirror contains an unexpected or unsafe entry")
                child_descriptor = os.open(entry.name, flags, dir_fd=descriptor)
                try:
                    visit(child_descriptor, child)
                finally:
                    os.close(child_descriptor)

    descriptor = os.open(root, flags)
    try:
        visit(descriptor, PurePosixPath("."))
    finally:
        os.close(descriptor)
    if observed != directories | files:
        raise DbtProdMirrorError("workspace mirror tree is incomplete")


__all__ = ["DbtSingletonMirrorContent", "DbtWorkspaceMirrorContent"]
