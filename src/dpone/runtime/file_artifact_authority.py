"""Private lifecycle authorities shared by file extraction artifacts."""

from __future__ import annotations

import os
from threading import RLock
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from dpone.runtime.artifact_integrity import FileArtifactReceipt, FileWireContract


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
