"""Rollback-safe installation of the three bot-owned prod mirror outputs."""

from __future__ import annotations

import os
import stat
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from dpone.contracts.dbt_workspace_promotion import validate_workspace_mirror_paths
from dpone.ports.dbt_prod_mirror import DbtProdMirrorContent, DbtProdMirrorOwnership
from dpone.ports.dbt_project_bundle import DbtProjectBundleOperations
from dpone.services.dbt_prod_mirror_content import DbtSingletonMirrorContent
from dpone.services.dbt_prod_mirror_journal import (
    DbtProdMirrorJournal,
    MirrorReplacement,
    fsync_directory,
    serialized_prod_mirror,
)
from dpone.services.dbt_prod_mirror_paths import confined_mirror_destination
from dpone.services.dbt_prod_promotion_contract import DbtProdMirrorError


@dataclass(frozen=True, slots=True)
class DbtProdMirrorInstallReport:
    mirror_no_op: bool
    snapshot_no_op: bool
    descriptor_no_op: bool

    @property
    def no_op(self) -> bool:
        return self.mirror_no_op and self.snapshot_no_op and self.descriptor_no_op


class DbtProdMirrorTransaction:
    """Serialize, install and roll back one complete promotion source update."""

    def __init__(self, *, bundle_operations: DbtProjectBundleOperations) -> None:
        self._bundle_operations = bundle_operations

    def install(
        self,
        *,
        repository_root: Path,
        mirror_path: PurePosixPath,
        project_bundle: bytes,
        source_snapshot_path: PurePosixPath,
        source_snapshot_bytes: bytes,
        descriptor_path: PurePosixPath,
        descriptor_bytes: bytes,
    ) -> DbtProdMirrorInstallReport:
        return self.install_content(
            repository_root=repository_root,
            mirror_path=mirror_path,
            content=DbtSingletonMirrorContent(project_bundle, bundle_operations=self._bundle_operations),
            source_snapshot_path=source_snapshot_path,
            source_snapshot_bytes=source_snapshot_bytes,
            descriptor_path=descriptor_path,
            descriptor_bytes=descriptor_bytes,
        )

    def install_content(
        self,
        *,
        repository_root: Path,
        mirror_path: PurePosixPath,
        content: DbtProdMirrorContent,
        source_snapshot_path: PurePosixPath,
        source_snapshot_bytes: bytes,
        descriptor_path: PurePosixPath,
        descriptor_bytes: bytes,
        ownership: DbtProdMirrorOwnership | None = None,
    ) -> DbtProdMirrorInstallReport:
        """Install one already-authorized complete subtree under the shared lock."""

        root = repository_root.absolute()
        with serialized_prod_mirror(root):
            return self._install_locked(
                root=root,
                mirror_path=mirror_path,
                content=content,
                source_snapshot_path=source_snapshot_path,
                source_snapshot_bytes=source_snapshot_bytes,
                descriptor_path=descriptor_path,
                descriptor_bytes=descriptor_bytes,
                ownership=ownership,
            )

    def _install_locked(
        self,
        *,
        root: Path,
        mirror_path: PurePosixPath,
        content: DbtProdMirrorContent,
        source_snapshot_path: PurePosixPath,
        source_snapshot_bytes: bytes,
        descriptor_path: PurePosixPath,
        descriptor_bytes: bytes,
        ownership: DbtProdMirrorOwnership | None,
    ) -> DbtProdMirrorInstallReport:
        validate_workspace_mirror_paths(str(mirror_path), str(source_snapshot_path), str(descriptor_path))
        mirror = confined_mirror_destination(root, mirror_path)
        snapshot = confined_mirror_destination(root, source_snapshot_path)
        descriptor = confined_mirror_destination(root, descriptor_path)
        if ownership is not None:
            ownership.require_owned_or_absent(
                repository_root=root, mirror=mirror, snapshot=snapshot, descriptor=descriptor
            )
        mirror_no_op = content.matches(mirror)
        snapshot_no_op = _file_matches(snapshot, source_snapshot_bytes)
        descriptor_no_op = _file_matches(descriptor, descriptor_bytes)
        report = DbtProdMirrorInstallReport(
            mirror_no_op=mirror_no_op,
            snapshot_no_op=snapshot_no_op,
            descriptor_no_op=descriptor_no_op,
        )
        if report.no_op:
            return report

        journal = DbtProdMirrorJournal.begin(root)
        try:
            replacements = self._stage_replacements(
                transaction_root=journal.transaction_root,
                mirror=mirror,
                mirror_no_op=mirror_no_op,
                content=content,
                snapshot=snapshot,
                snapshot_no_op=snapshot_no_op,
                source_snapshot_bytes=source_snapshot_bytes,
                descriptor=descriptor,
                descriptor_no_op=descriptor_no_op,
                descriptor_bytes=descriptor_bytes,
            )
            journal.set_replacements(replacements)
            _commit_replacements(root, replacements, journal=journal)
            content.verify(mirror)
            if not _file_matches(
                snapshot,
                source_snapshot_bytes,
            ) or not _file_matches(descriptor, descriptor_bytes):
                raise DbtProdMirrorError("prod promotion transaction verification failed")
        except Exception as exc:
            journal.rollback()
            if isinstance(exc, DbtProdMirrorError):
                raise
            raise DbtProdMirrorError("prod promotion transaction verification failed") from exc
        journal.cleanup()
        return report

    def _stage_replacements(
        self,
        *,
        transaction_root: Path,
        mirror: Path,
        mirror_no_op: bool,
        content: DbtProdMirrorContent,
        snapshot: Path,
        snapshot_no_op: bool,
        source_snapshot_bytes: bytes,
        descriptor: Path,
        descriptor_no_op: bool,
        descriptor_bytes: bytes,
    ) -> list[MirrorReplacement]:
        replacements: list[MirrorReplacement] = []
        if not snapshot_no_op:
            replacements.append(
                _replacement(
                    snapshot,
                    _stage_file(transaction_root / "snapshot.new", source_snapshot_bytes),
                    transaction_root / "snapshot.previous",
                )
            )
        if not descriptor_no_op:
            replacements.append(
                _replacement(
                    descriptor,
                    _stage_file(transaction_root / "descriptor.new", descriptor_bytes),
                    transaction_root / "descriptor.previous",
                )
            )
        if not mirror_no_op:
            staged_mirror = transaction_root / "mirror.new"
            content.stage(staged_mirror)
            _fsync_tree_directories(staged_mirror)
            replacements.append(
                _replacement(
                    mirror,
                    staged_mirror,
                    transaction_root / "mirror.previous",
                )
            )
        return replacements


def _commit_replacements(
    root: Path,
    replacements: list[MirrorReplacement],
    *,
    journal: DbtProdMirrorJournal,
) -> None:
    created_parents: list[Path] = []
    try:
        for index, item in enumerate(replacements):
            created_parents.extend(_prepare_parent(root, item.destination.parent))
            if item.had_previous:
                if item.destination.is_symlink():
                    raise DbtProdMirrorError("prod promotion destination is a symlink")
                _replace(item.destination, item.backup)
                fsync_directory(item.destination.parent)
                journal.mark(index, "backed_up")
            else:
                journal.mark(index, "installing")
            _replace(item.staged, item.destination)
            fsync_directory(item.destination.parent)
            journal.mark(index, "installed")
    except Exception as exc:
        for parent in reversed(created_parents):
            try:
                parent.rmdir()
                fsync_directory(parent.parent)
            except OSError:
                pass
        if isinstance(exc, DbtProdMirrorError):
            raise
        raise DbtProdMirrorError("prod promotion transaction commit failed") from exc


def _prepare_parent(root: Path, parent: Path) -> list[Path]:
    created: list[Path] = []
    current = root
    for part in parent.relative_to(root).parts:
        current /= part
        if not current.exists():
            parent_directory = current.parent
            current.mkdir()
            fsync_directory(parent_directory)
            created.append(current)
        metadata = current.lstat()
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise DbtProdMirrorError("prod promotion parent is unsafe")
    return created


def _file_matches(path: Path, payload: bytes) -> bool:
    if not path.exists():
        return False
    metadata = path.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise DbtProdMirrorError("existing promotion metadata path is unsafe")
    return path.read_bytes() == payload


def _stage_file(path: Path, payload: bytes) -> Path:
    with path.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    return path


def _replacement(destination: Path, staged: Path, backup: Path) -> MirrorReplacement:
    return MirrorReplacement(
        destination=destination,
        staged=staged,
        backup=backup,
        had_previous=destination.exists(),
    )


def _replace(source: Path, destination: Path) -> None:
    os.replace(source, destination)


def _fsync_tree_directories(root: Path) -> None:
    directories = [path for path in root.rglob("*") if path.is_dir()]
    for directory in sorted(directories, key=lambda item: len(item.parts), reverse=True):
        fsync_directory(directory)
    fsync_directory(root)


__all__ = [
    "DbtProdMirrorInstallReport",
    "DbtProdMirrorTransaction",
    "DbtProjectBundleOperations",
]
