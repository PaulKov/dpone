"""Integrity authority for a held POSIX spool-file descriptor."""

from __future__ import annotations

import hashlib
import os
import stat
from dataclasses import dataclass
from typing import NoReturn

from dpone.runtime.artifact_integrity import (
    ArtifactIntegrityError,
    FileArtifactReceipt,
    FileIdentity,
    FileOwnedScope,
    FileWireContract,
)
from dpone.runtime.file_artifact_authority import FileVerificationBudget, FileVerificationTimeout


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
        *,
        verification_budget: FileVerificationBudget | None = None,
    ) -> None:
        """Rehash the exact descriptor without reopening a mutable pathname."""

        receipt.verify_descriptor(self.descriptor, wire_contract=wire_contract, verification_budget=verification_budget)


def descriptor_file_identity(
    descriptor: int, *, verification_budget: FileVerificationBudget | None = None
) -> tuple[FileIdentity, str]:
    """Hash one held descriptor without changing its shared file position."""

    if verification_budget is not None:
        verification_budget.check()
    pread = getattr(os, "pread", None)
    if not callable(pread):
        raise ArtifactIntegrityError("artifact_integrity.descriptor_read_unsupported")
    try:
        before_details = os.fstat(descriptor)
        if not stat.S_ISREG(before_details.st_mode):
            raise ArtifactIntegrityError("artifact_integrity.regular_file_required")
        before = FileIdentity.from_stat(before_details)
        if verification_budget is not None:
            verification_budget.check(before.size)
        digest = hashlib.sha256()
        offset = 0
        while True:
            if verification_budget is not None:
                verification_budget.check(offset)
            payload = pread(descriptor, 1024 * 1024, offset)
            offset += len(payload)
            if verification_budget is not None:
                verification_budget.check(offset)
            if not payload:
                break
            digest.update(payload)
        after_details = os.fstat(descriptor)
    except (ArtifactIntegrityError, FileVerificationTimeout):
        raise
    except OSError as exc:
        raise ArtifactIntegrityError("artifact_integrity.file_unavailable") from exc
    if not stat.S_ISREG(after_details.st_mode):
        raise ArtifactIntegrityError("artifact_integrity.regular_file_required")
    after = FileIdentity.from_stat(after_details)
    if not after.same_object(before) or after != before:
        raise ArtifactIntegrityError("artifact_integrity.file_changed_during_hash")
    if verification_budget is not None:
        verification_budget.check(after.size)
    return after, digest.hexdigest()


def path_file_identity(
    path: str, *, verification_budget: FileVerificationBudget | None = None
) -> tuple[FileIdentity, str]:
    """Hash one no-follow descriptor and prove its path remained attached."""

    if verification_budget is not None:
        verification_budget.check()
    try:
        path_before = os.lstat(path)
    except OSError as exc:
        raise ArtifactIntegrityError("artifact_integrity.file_unavailable") from exc
    if not stat.S_ISREG(path_before.st_mode):
        raise ArtifactIntegrityError("artifact_integrity.regular_file_required")
    path_identity = FileIdentity.from_stat(path_before)
    if verification_budget is not None:
        verification_budget.check(path_identity.size)

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
        if verification_budget is not None:
            verification_budget.check(before.size)
        with os.fdopen(descriptor, "rb", closefd=True) as handle:
            descriptor = -1
            observed = 0
            while True:
                if verification_budget is not None:
                    verification_budget.check(observed)
                chunk = handle.read(1024 * 1024)
                observed += len(chunk)
                if verification_budget is not None:
                    verification_budget.check(observed)
                if not chunk:
                    break
                digest.update(chunk)
            descriptor_after = os.fstat(handle.fileno())
    except FileVerificationTimeout:
        raise
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
    if verification_budget is not None:
        verification_budget.check(final.size)
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


__all__ = ["PosixDescriptorIntegrityAuthority", "descriptor_file_identity"]
