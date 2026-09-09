"""Port for durable approval-bound workload-index baseline promotion."""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


class WorkloadIndexRootIdentity(Protocol):
    """Stable root-directory identity required by the POSIX store."""

    @property
    def path(self) -> Path: ...

    def matches(self, metadata: os.stat_result) -> bool: ...


@dataclass(frozen=True, slots=True)
class WorkloadIndexAtomicReplaceOutcome:
    """Port-level result after the atomic replacement linearization point."""

    committed: bool
    cleanup_required: bool = False
    recovery_name: str | None = None


class WorkloadIndexAtomicReplaceError(RuntimeError):
    """Atomic replacement failed with explicit commit and cleanup state."""

    def __init__(
        self,
        message: str,
        *,
        committed: bool,
        cleanup_required: bool,
        recovery_name: str | None,
    ) -> None:
        super().__init__(message)
        self.committed = committed
        self.cleanup_required = cleanup_required
        self.recovery_name = recovery_name


WorkloadIndexAtomicReplace = Callable[
    [int, str, str, str, int],
    WorkloadIndexAtomicReplaceOutcome,
]


class WorkloadIndexBaselineStoreError(RuntimeError):
    """A baseline store could not complete with a truthful durable state."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        recovery_path: str | None = None,
        recovery_artifacts: tuple[str, ...] = (),
    ) -> None:
        super().__init__(message)
        self.code = code
        self.recovery_path = recovery_path
        self.recovery_artifacts = recovery_artifacts


class WorkloadIndexBaselineStore(Protocol):
    """Install captured candidate bytes without overwriting unreviewed state."""

    def promote(
        self,
        relative_path: str,
        *,
        desired: bytes,
        expected_sha256: str | None,
    ) -> None: ...


__all__ = [
    "WorkloadIndexAtomicReplace",
    "WorkloadIndexAtomicReplaceError",
    "WorkloadIndexAtomicReplaceOutcome",
    "WorkloadIndexBaselineStore",
    "WorkloadIndexBaselineStoreError",
    "WorkloadIndexRootIdentity",
]
