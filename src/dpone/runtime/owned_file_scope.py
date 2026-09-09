"""Exception-safe ownership for source-produced temporary files."""

from __future__ import annotations

import os
from pathlib import Path
from threading import RLock


class OwnedFileScope:
    """Own registered files until successful artifact handoff.

    Registration happens immediately after secure creation.  Producers release
    files only after every operation needed to construct the returned artifact
    succeeds; otherwise the scope removes all partial and sibling outputs while
    preserving the original exception.
    """

    def __init__(self) -> None:
        self._paths: set[str] = set()
        self._lock = RLock()

    def register(self, path: str | os.PathLike[str]) -> str:
        value = os.fspath(path)
        with self._lock:
            self._paths.add(value)
        return value

    def transfer(self, path: str | os.PathLike[str]) -> None:
        with self._lock:
            self._paths.discard(os.fspath(path))

    def cleanup(self) -> None:
        with self._lock:
            paths = tuple(self._paths)
            self._paths.clear()
        for path in paths:
            try:
                Path(path).unlink(missing_ok=True)
            except OSError:
                # Cleanup must never hide the producer's primary exception.
                pass


__all__ = ["OwnedFileScope"]
