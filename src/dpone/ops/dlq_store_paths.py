"""Path confinement primitives shared by the local DLQ repositories."""

from __future__ import annotations

import os
from pathlib import Path


class DlqStoreError(RuntimeError):
    """Base failure for durable DLQ storage."""


class DlqPathError(DlqStoreError):
    """A caller-supplied identifier or filesystem path escaped the DLQ root."""


def component(value: str) -> str:
    if (
        not value
        or value in {".", ".."}
        or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_." for char in value)
    ):
        raise DlqPathError("DPONE_DLQ_PATH_INVALID: identifier contains unsafe characters")
    return value


def confined(root: Path, path: Path) -> Path:
    resolved = path.resolve(strict=False)
    if not resolved.is_relative_to(root):
        raise DlqPathError("DPONE_DLQ_PATH_ESCAPE: path leaves configured DLQ root")
    return resolved


def mkdir(root: Path, path: Path) -> None:
    safe_path = confined(root, path)
    safe_path.mkdir(parents=True, exist_ok=True)
    confined(root, safe_path)


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


__all__ = ["DlqPathError", "DlqStoreError", "component", "confined", "fsync_directory", "mkdir"]
