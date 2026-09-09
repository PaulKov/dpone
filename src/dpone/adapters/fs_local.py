from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path


class LocalFileSystem:
    def read_text(self, path: Path, *, encoding: str = "utf-8") -> str:
        return path.read_text(encoding=encoding)

    def read_bytes(self, path: Path) -> bytes:
        return path.read_bytes()

    def write_text(self, path: Path, data: str, *, encoding: str = "utf-8") -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(data, encoding=encoding)

    def exists(self, path: Path) -> bool:
        return path.exists()

    def glob(self, root: Path, pattern: str) -> Iterable[Path]:
        return root.glob(pattern)
