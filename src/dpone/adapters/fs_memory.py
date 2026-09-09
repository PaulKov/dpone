from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class InMemoryFileSystem:
    """Tiny in-memory FS for unit tests."""

    files: dict[str, str | bytes] = field(default_factory=dict)

    def read_text(self, path: Path, *, encoding: str = "utf-8") -> str:
        key = str(path)
        if key not in self.files:
            raise FileNotFoundError(key)
        value = self.files[key]
        return value.decode(encoding) if isinstance(value, bytes) else value

    def read_bytes(self, path: Path) -> bytes:
        key = str(path)
        if key not in self.files:
            raise FileNotFoundError(key)
        value = self.files[key]
        return value if isinstance(value, bytes) else value.encode("utf-8")

    def write_text(self, path: Path, data: str, *, encoding: str = "utf-8") -> None:
        self.files[str(path)] = data

    def exists(self, path: Path) -> bool:
        return str(path) in self.files

    def glob(self, root: Path, pattern: str) -> Iterable[Path]:
        # Minimal glob: not needed for now.
        raise NotImplementedError("InMemoryFileSystem.glob is not implemented")
