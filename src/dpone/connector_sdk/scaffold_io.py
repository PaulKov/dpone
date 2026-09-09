from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path


def write_text_files(files: Mapping[Path, str]) -> list[Path]:
    written: list[Path] = []
    for path, content in files.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        written.append(path)
    return written
