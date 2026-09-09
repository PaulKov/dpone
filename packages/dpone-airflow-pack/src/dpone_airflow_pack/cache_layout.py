"""Versioned filesystem identity for Airflow cache roots."""

from __future__ import annotations

import json
import os
import stat
from pathlib import Path
from typing import Final
from uuid import uuid4

from dpone_airflow_pack.cache_permissions import SHARED_CONTROL_MODE, ensure_shared_directory

_OS_FSYNC = os.fsync

LAYOUT_MARKER_NAME: Final = ".dpone-cache-layout.json"
LAYOUT_SCHEMA: Final = "dpone.airflow-cache-layout.v1"
EXACT_DEPLOYMENT_LAYOUT: Final = "exact_deployment_v1"
LEGACY_PACK_INDEX_LAYOUT: Final = "legacy_pack_index_v1"

_EXACT_ENTRIES = frozenset({"activations", "deployments", "releases", "current-pointer.json"})
_LEGACY_ENTRIES = frozenset({"generations", "current_generation"})


class CacheLayoutError(RuntimeError):
    """Fail-closed cache-layout mismatch with stable remediation metadata."""

    def __init__(self, code: str, message: str, path: str) -> None:
        self.code = code
        self.message = message
        self.path = path
        super().__init__(f"{code}: {message}: {path}")

    def __str__(self) -> str:
        return str(self.args[0])


def ensure_cache_layout(cache_root: str | Path, *, expected_layout: str) -> dict[str, str]:
    """Validate or initialize one cache root without mixing incompatible layouts."""

    _validate_layout_name(expected_layout)
    root = _cache_root(cache_root)
    assert_cache_layout_compatible(root, expected_layout=expected_layout)
    try:
        ensure_shared_directory(root)
    except OSError as exc:
        raise CacheLayoutError(
            "DPONE_CACHE_LAYOUT_WRITE_FAILED",
            "Airflow cache root could not be initialized",
            root.as_posix(),
        ) from exc
    marker = root / LAYOUT_MARKER_NAME
    if os.path.lexists(marker):
        payload = _read_marker(marker)
        actual = payload.get("layout")
        if actual != expected_layout:
            raise _layout_mismatch(marker, expected=expected_layout, actual=actual)
        inferred = _infer_unmarked_layout(root)
        if inferred is not None and inferred != actual:
            raise _layout_mismatch(marker, expected=actual, actual=inferred)
        return payload
    inferred = _infer_unmarked_layout(root)
    if inferred is not None and inferred != expected_layout:
        raise _layout_mismatch(marker, expected=expected_layout, actual=inferred)
    payload = {"schema": LAYOUT_SCHEMA, "layout": expected_layout, "version": "1"}
    try:
        _write_marker(marker, payload)
    except OSError as exc:
        raise CacheLayoutError(
            "DPONE_CACHE_LAYOUT_WRITE_FAILED",
            "Airflow cache layout marker could not be committed durably",
            marker.as_posix(),
        ) from exc
    return payload


def read_cache_layout(cache_root: str | Path) -> str | None:
    """Return the declared layout, or infer one historical unmarked root."""

    root = _cache_root(cache_root)
    if not root.exists():
        return None
    marker = root / LAYOUT_MARKER_NAME
    if os.path.lexists(marker):
        declared = _read_marker(marker)["layout"]
        inferred = _infer_unmarked_layout(root)
        if inferred is not None and inferred != declared:
            raise _layout_mismatch(marker, expected=declared, actual=inferred)
        return declared
    return _infer_unmarked_layout(root)


def assert_cache_layout_compatible(cache_root: str | Path, *, expected_layout: str) -> str | None:
    """Read-only preflight that rejects a conflicting cache before lock mutation."""

    _validate_layout_name(expected_layout)
    root = _cache_root(cache_root)
    actual = read_cache_layout(root)
    if actual is not None and actual != expected_layout:
        raise _layout_mismatch(root / LAYOUT_MARKER_NAME, expected=expected_layout, actual=actual)
    return actual


def _cache_root(cache_root: str | Path) -> Path:
    configured = Path(cache_root)
    if os.path.lexists(configured) and configured.is_symlink():
        raise CacheLayoutError(
            "DPONE_CACHE_LAYOUT_INVALID",
            "Airflow cache root must not be a symbolic link",
            configured.as_posix(),
        )
    root = configured.resolve(strict=False)
    if root.exists() and not root.is_dir():
        raise CacheLayoutError(
            "DPONE_CACHE_LAYOUT_INVALID",
            "Airflow cache root must be a directory",
            root.as_posix(),
        )
    return root


def _validate_layout_name(expected_layout: str) -> None:
    if expected_layout not in {EXACT_DEPLOYMENT_LAYOUT, LEGACY_PACK_INDEX_LAYOUT}:
        raise ValueError(f"unsupported Airflow cache layout: {expected_layout}")


def _read_marker(path: Path) -> dict[str, str]:
    try:
        metadata = path.lstat()
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > 4096:
            raise ValueError("layout marker must be a small regular file")
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise CacheLayoutError(
            "DPONE_CACHE_LAYOUT_INVALID",
            "Airflow cache layout marker is invalid; preserve the root and follow the cache migration runbook",
            path.as_posix(),
        ) from exc
    if (
        not isinstance(payload, dict)
        or payload.get("schema") != LAYOUT_SCHEMA
        or payload.get("version") != "1"
        or payload.get("layout") not in {EXACT_DEPLOYMENT_LAYOUT, LEGACY_PACK_INDEX_LAYOUT}
    ):
        raise CacheLayoutError(
            "DPONE_CACHE_LAYOUT_INVALID",
            "Airflow cache layout marker has an unsupported contract",
            path.as_posix(),
        )
    return {str(key): str(value) for key, value in payload.items()}


def _infer_unmarked_layout(root: Path) -> str | None:
    names = {entry.name for entry in root.iterdir() if entry.name not in {".promotion.lock", LAYOUT_MARKER_NAME}}
    current = root / "current"
    exact = bool(names & _EXACT_ENTRIES) or os.path.islink(current)
    legacy = bool(names & _LEGACY_ENTRIES) or (current.exists() and not current.is_symlink())
    if exact and legacy:
        raise CacheLayoutError(
            "DPONE_CACHE_LAYOUT_AMBIGUOUS",
            "Airflow cache root contains both exact-deployment and legacy-pack entries",
            root.as_posix(),
        )
    if exact:
        return EXACT_DEPLOYMENT_LAYOUT
    if legacy:
        return LEGACY_PACK_INDEX_LAYOUT
    return None


def _write_marker(path: Path, payload: dict[str, str]) -> None:
    temporary = path.parent / f".{path.name}.{uuid4().hex}.tmp"
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        SHARED_CONTROL_MODE,
    )
    try:
        os.fchmod(descriptor, SHARED_CONTROL_MODE)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True)
            handle.write("\n")
            handle.flush()
            _OS_FSYNC(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        if os.path.lexists(temporary):
            temporary.unlink()


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        _OS_FSYNC(descriptor)
    finally:
        os.close(descriptor)


def _layout_mismatch(path: Path, *, expected: str, actual: object) -> CacheLayoutError:
    return CacheLayoutError(
        "DPONE_CACHE_LAYOUT_MISMATCH",
        f"Airflow cache root is {actual!s}, not {expected}; use a separate cache root or the migration runbook",
        path.as_posix(),
    )


__all__ = [
    "EXACT_DEPLOYMENT_LAYOUT",
    "LAYOUT_MARKER_NAME",
    "LAYOUT_SCHEMA",
    "LEGACY_PACK_INDEX_LAYOUT",
    "CacheLayoutError",
    "assert_cache_layout_compatible",
    "ensure_cache_layout",
    "read_cache_layout",
]
