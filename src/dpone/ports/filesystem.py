from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Protocol


class FileSystem(Protocol):
    """Minimal filesystem port.

    This port allows:
      - diskless tests via InMemoryFileSystem
      - decoupling services from pathlib/io
    """

    def read_text(self, path: Path, *, encoding: str = "utf-8") -> str: ...

    def read_bytes(self, path: Path) -> bytes: ...

    def write_text(self, path: Path, data: str, *, encoding: str = "utf-8") -> None: ...

    def exists(self, path: Path) -> bool: ...

    def glob(self, root: Path, pattern: str) -> Iterable[Path]: ...
