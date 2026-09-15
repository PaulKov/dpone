"""Fixed-inventory adapters over canonical confined filesystem primitives.

No transaction, recovery or ownership decision is made here. Callers retain the
operation lock and receipts; these adapters validate names, read exact snapshots
and preserve permission bits only on the inode named by a creation receipt.
"""

from __future__ import annotations

import os
import stat
from pathlib import Path
from uuid import UUID

from tools.dbt_self_service.starter_resource_journal_schema import MAX_RESOURCE_BYTES, METADATA_ROOT, RESOURCE_PATHS

from dpone.manifest.confined_files import ConfinedFileError, ConfinedFileSnapshot, read_confined_leaf
from dpone.manifest.confined_transaction_journal import transaction_journal_name
from dpone.manifest.project_root import ProjectRootIdentity
from dpone.readiness.airflow_authoring_directories import open_confined_parent
from dpone.readiness.airflow_pipeline_source import ConfinedFileCreation

_ERROR = "Starter resource file is unsafe, changed or outside the fixed inventory."


def snapshot(identity: ProjectRootIdentity, path: str) -> ConfinedFileSnapshot | None:
    validate_path(path)
    with open_confined_parent(identity.path, Path(path).parts, create=False, root_identity=identity) as parent:
        if parent.descriptor is None:
            return None
        try:
            return read_confined_leaf(parent.descriptor, Path(path).name, max_bytes=MAX_RESOURCE_BYTES)
        except ConfinedFileError as error:
            if error.code == "file_not_found":
                return None
            raise


def require_snapshot(identity: ProjectRootIdentity, path: str, expected: ConfinedFileSnapshot | None) -> None:
    if snapshot(identity, path) != expected:
        raise ValueError(_ERROR)


def verify_creation(identity: ProjectRootIdentity, created: ConfinedFileCreation) -> ConfinedFileSnapshot:
    observed = snapshot(identity, created.path.as_posix())
    if (
        observed is None
        or observed.content != created.content
        or (observed.identity.device, observed.identity.inode) != (created.device, created.inode)
    ):
        raise ValueError(_ERROR)
    return observed


def preserve_mode(identity: ProjectRootIdentity, created: ConfinedFileCreation, mode: int) -> None:
    verify_creation(identity, created)
    with open_confined_parent(identity.path, created.path.parts, create=False, root_identity=identity) as parent:
        if parent.descriptor is None:
            raise ValueError(_ERROR)
        descriptor = os.open(
            created.path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK, dir_fd=parent.descriptor
        )
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode) or (metadata.st_dev, metadata.st_ino) != (
                created.device,
                created.inode,
            ):
                raise ValueError(_ERROR)
            os.fchmod(descriptor, stat.S_IMODE(mode))
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def reject_leaf_journal(parent_fd: int, name: str) -> None:
    if name not in {Path(path).name for path in RESOURCE_PATHS}:
        raise ValueError(_ERROR)
    try:
        read_confined_leaf(parent_fd, transaction_journal_name(name), max_bytes=4096)
    except ConfinedFileError as error:
        if error.code == "file_not_found":
            return
        raise
    raise ValueError(_ERROR)


def validate_path(value: str) -> None:
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or "\\" in value or "\0" in value or path.as_posix() != value:
        raise ValueError(_ERROR)
    if value in RESOURCE_PATHS:
        return
    if (
        len(path.parts) == 4
        and path.parts[0] == METADATA_ROOT
        and _uuid(path.parts[1])
        and path.parts[2] in {"old", "new"}
        and path.name in {f"{index:03d}.bin" for index in range(len(RESOURCE_PATHS))}
    ):
        return
    for resource in RESOURCE_PATHS:
        target = Path(resource)
        prefix = f".{target.name}."
        if path.parent == target.parent and path.name.startswith(prefix):
            operation, _, suffix = path.name[len(prefix) :].rpartition(".")
            if suffix in {"new", "restore"} and _uuid(operation):
                return
    raise ValueError(_ERROR)


def _uuid(value: str) -> bool:
    try:
        return str(UUID(value)) == value
    except ValueError:
        return False
