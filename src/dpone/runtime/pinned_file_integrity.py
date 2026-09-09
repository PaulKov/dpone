"""Integrity authority for a held POSIX spool-file descriptor."""

from __future__ import annotations

import hashlib
import os
import stat
from dataclasses import dataclass

from dpone.runtime.artifact_integrity import (
    ArtifactIntegrityError,
    FileArtifactReceipt,
    FileIdentity,
    FileOwnedScope,
    FileWireContract,
)


@dataclass(frozen=True, slots=True)
class PosixDescriptorIntegrityAuthority:
    """Capture and verify receipts through the descriptor consumed by BCP."""

    descriptor: int
    owned_scope: FileOwnedScope

    @classmethod
    def for_pinned_directory(
        cls,
        descriptor: int,
        *,
        directory_path: str,
        device: int,
        inode: int,
    ) -> PosixDescriptorIntegrityAuthority:
        """Bind a held descriptor to its already-pinned work-directory scope."""

        return cls(
            descriptor=descriptor,
            owned_scope=FileOwnedScope(
                path=os.path.normcase(os.path.realpath(os.path.abspath(directory_path))),
                device=int(device),
                inode=int(inode),
            ),
        )

    def capture(
        self,
        wire_contract: FileWireContract,
        rows_exported: int | None,
    ) -> FileArtifactReceipt:
        """Freeze the bytes and row authority of the held descriptor."""

        return FileArtifactReceipt.capture_descriptor(
            self.descriptor,
            owned_scope=self.owned_scope,
            wire_contract=wire_contract,
            rows_exported=rows_exported,
        )

    def verify(
        self,
        receipt: FileArtifactReceipt,
        wire_contract: FileWireContract,
    ) -> None:
        """Rehash the exact descriptor without reopening a mutable pathname."""

        receipt.verify_descriptor(self.descriptor, wire_contract=wire_contract)


def descriptor_file_identity(descriptor: int) -> tuple[FileIdentity, str]:
    """Hash one held descriptor without changing its shared file position."""

    pread = getattr(os, "pread", None)
    if not callable(pread):
        raise ArtifactIntegrityError("artifact_integrity.descriptor_read_unsupported")
    try:
        before_details = os.fstat(descriptor)
        if not stat.S_ISREG(before_details.st_mode):
            raise ArtifactIntegrityError("artifact_integrity.regular_file_required")
        before = FileIdentity.from_stat(before_details)
        digest = hashlib.sha256()
        offset = 0
        while payload := pread(descriptor, 1024 * 1024, offset):
            digest.update(payload)
            offset += len(payload)
        after_details = os.fstat(descriptor)
    except ArtifactIntegrityError:
        raise
    except OSError as exc:
        raise ArtifactIntegrityError("artifact_integrity.file_unavailable") from exc
    if not stat.S_ISREG(after_details.st_mode):
        raise ArtifactIntegrityError("artifact_integrity.regular_file_required")
    after = FileIdentity.from_stat(after_details)
    if not after.same_object(before) or after != before:
        raise ArtifactIntegrityError("artifact_integrity.file_changed_during_hash")
    return after, digest.hexdigest()


__all__ = ["PosixDescriptorIntegrityAuthority", "descriptor_file_identity"]
