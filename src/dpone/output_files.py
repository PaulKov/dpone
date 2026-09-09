from __future__ import annotations

from pathlib import Path


def write_text_file(path: str | Path, content: str, *, encoding: str = "utf-8") -> None:
    """Write text content to a file, creating parent dirs."""

    target = Path(str(path))
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding=encoding)


__all__ = ["write_text_file"]
