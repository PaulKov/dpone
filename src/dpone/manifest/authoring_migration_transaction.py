"""Exact dependent-fragment state for authoring migration transactions."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.manifest.confined_files import ConfinedFileSnapshot


from dataclasses import dataclass

from dpone.manifest.confined_files import ConfinedFileError, read_confined_leaf
from dpone.manifest.confined_mutations import OwnedFile, remove_file_if_owned


@dataclass(frozen=True, slots=True)
class MigrationFragmentState:
    """Separate a planned fragment snapshot from optional operation ownership."""

    snapshot: ConfinedFileSnapshot
    owned: OwnedFile | None

    @classmethod
    def existing(cls, snapshot: ConfinedFileSnapshot) -> MigrationFragmentState:
        return cls(snapshot=snapshot, owned=None)

    @classmethod
    def created(cls, snapshot: ConfinedFileSnapshot) -> MigrationFragmentState:
        identity = snapshot.identity
        return cls(
            snapshot=snapshot,
            owned=OwnedFile(
                device=identity.device,
                inode=identity.inode,
                size=identity.size,
                sha256=snapshot.sha256,
            ),
        )


def read_migration_file(
    parent_fd: int,
    name: str,
    *,
    max_bytes: int,
) -> ConfinedFileSnapshot:
    """Read one sibling through the shared stable confined primitive."""

    return read_confined_leaf(parent_fd, name, max_bytes=max_bytes)


def same_migration_file(
    current: ConfinedFileSnapshot,
    planned: ConfinedFileSnapshot,
) -> bool:
    """Match exact planned inode, size, and content digest."""

    current_identity = current.identity
    planned_identity = planned.identity
    return (
        current_identity.device == planned_identity.device
        and current_identity.inode == planned_identity.inode
        and current_identity.size == planned_identity.size
        and current.sha256 == planned.sha256
    )


def fragment_state_is_current(
    parent_fd: int,
    name: str | None,
    state: MigrationFragmentState | None,
    *,
    max_bytes: int,
) -> bool:
    """Return false when the dependent fragment cannot match its planned state."""

    if name is None or state is None:
        return True
    try:
        current = read_migration_file(parent_fd, name, max_bytes=max_bytes)
    except ConfinedFileError:
        return False
    return same_migration_file(current, state.snapshot)


def rollback_owned_fragment(
    parent_fd: int,
    name: str | None,
    state: MigrationFragmentState | None,
    *,
    max_bytes: int,
) -> str | None:
    """Rollback only a fragment represented by an exact creation receipt."""

    if name is None or state is None or state.owned is None:
        return None
    outcome = remove_file_if_owned(
        parent_fd,
        name,
        owned=state.owned,
        max_bytes=max_bytes,
    )
    return outcome.recovery_name


__all__ = [
    "MigrationFragmentState",
    "fragment_state_is_current",
    "read_migration_file",
    "rollback_owned_fragment",
    "same_migration_file",
]
