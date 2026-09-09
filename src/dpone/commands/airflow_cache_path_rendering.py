"""Safe path display helpers for Airflow cache command text output."""

from __future__ import annotations

import shlex
from pathlib import Path


def cache_root_flag(cache_root: str) -> str:
    """Return a topology-safe ``--cache-root`` flag for human-readable actions."""

    display = _cache_root_display(cache_root)
    if not display or display == ".dpone-cache":
        return ""
    if display == "$DPONE_CACHE_ROOT":
        return ' --cache-root "${DPONE_CACHE_ROOT:?set DPONE_CACHE_ROOT}"'
    return f" --cache-root {shlex.quote(display)}"


def _cache_root_display(cache_root: str) -> str:
    normalized = cache_root.strip() or ".dpone-cache"
    if normalized == ".dpone-cache":
        return ""
    path = Path(normalized)
    if not path.is_absolute():
        return path.as_posix()
    try:
        return path.resolve(strict=False).relative_to(Path.cwd().resolve(strict=False)).as_posix()
    except ValueError:
        return "$DPONE_CACHE_ROOT"


__all__ = ["cache_root_flag"]
