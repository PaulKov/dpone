"""Private lifecycle authorities shared by file extraction artifacts."""

from __future__ import annotations

import inspect
import math
import os
from collections.abc import Callable
from dataclasses import dataclass
from threading import RLock
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from dpone.runtime.artifact_integrity import FileArtifactReceipt, FileWireContract


class FileVerificationTimeout(TimeoutError):
    """Cooperative phase deadline expired before the next verification operation."""

    code = "artifact_integrity.verification_timeout"


class FileVerificationLimitError(RuntimeError):
    """Initial or actually observed source bytes exceed the caller's budget."""

    code = "artifact_integrity.verification_byte_limit"


def supports_verification_budget(verifier: Callable[..., None]) -> bool:
    """Admit the optional keyword before invocation, preserving errors from its body."""
    try:
        parameters = inspect.signature(verifier).parameters
    except (TypeError, ValueError):
        return False
    parameter = parameters.get("verification_budget")
    return parameter is not None and parameter.kind in {
        inspect.Parameter.KEYWORD_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    }


def verify_file_receipt_authority(
    authority: FileIntegrityAuthority | None,
    receipt: FileArtifactReceipt,
    path: str,
    wire_contract: FileWireContract,
    *,
    verification_budget: FileVerificationBudget | None = None,
) -> None:
    """Preserve legacy dispatch; admit only an explicit bounded authority when requested."""
    from dpone.runtime.artifact_integrity import ArtifactIntegrityError

    options = {} if verification_budget is None else {"verification_budget": verification_budget}
    if verification_budget is not None:
        verification_budget.check(receipt.size_bytes)
    if authority is None:
        receipt.verify(path, wire_contract=wire_contract, **options)
    else:
        verifier = authority.verify_integrity_receipt
        if verification_budget is not None and not supports_verification_budget(verifier):
            raise ArtifactIntegrityError("artifact_integrity.verification_budget_unsupported")
        verifier(receipt, wire_contract, **options)


@dataclass(frozen=True, slots=True)
class FileVerificationBudget:
    """Finite shared phase time and per-scan bytes; never a replacement verifier."""

    clock: Callable[[], float]
    deadline_monotonic: float
    max_bytes: int

    def __post_init__(self) -> None:
        if type(self.max_bytes) is not int or self.max_bytes <= 0:
            raise ValueError("max_bytes must be a positive integer")
        if isinstance(self.deadline_monotonic, bool) or not math.isfinite(self.deadline_monotonic):
            raise ValueError("deadline must be finite")

    def remaining(self) -> float:
        remaining = self.deadline_monotonic - self.clock()
        if remaining <= 0:
            raise FileVerificationTimeout("file verification deadline exceeded")
        return remaining

    def check(self, observed_bytes: int = 0) -> None:
        self.remaining()
        if observed_bytes > self.max_bytes:
            raise FileVerificationLimitError("file verification byte limit exceeded")


class FileIntegrityAuthority(Protocol):
    """Receipt boundary backed by one runtime-pinned file identity."""

    def capture_integrity_receipt(
        self,
        wire_contract: FileWireContract,
        rows_exported: int | None,
    ) -> FileArtifactReceipt: ...

    def verify_integrity_receipt(
        self,
        receipt: FileArtifactReceipt,
        wire_contract: FileWireContract,
        *,
        verification_budget: FileVerificationBudget | None = None,
    ) -> None: ...


class FileReleaseAuthority:
    """Release one physical file exactly once across immutable rebind views."""

    def __init__(self, path: str) -> None:
        self.path = path
        self._lock = RLock()
        self._released = False

    def release(self) -> None:
        with self._lock:
            if self._released:
                return
            if os.path.exists(self.path):
                try:
                    os.remove(self.path)
                except FileNotFoundError:
                    pass
            self._released = True


__all__ = ["FileIntegrityAuthority", "FileReleaseAuthority"]
