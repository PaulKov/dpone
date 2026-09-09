"""Bounded durable JSON I/O for the local DLQ repositories."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

from dpone.ops.dlq_store_paths import (
    DlqPathError,
    DlqStoreError,
    component,
    confined,
    fsync_directory,
    mkdir,
)


class DlqIntegrityError(DlqStoreError):
    """A stored artifact failed its immutable checksum or schema contract."""


class DlqRecordTooLarge(DlqStoreError):
    """A canonical record exceeds its configured hard byte limit."""


def read_json(root: Path, path: Path, *, max_bytes: int) -> Mapping[str, Any]:
    safe_path = confined(root, path)
    size = safe_path.stat().st_size
    if size > max_bytes:
        raise DlqRecordTooLarge(f"DPONE_DLQ_RECORD_TOO_LARGE: artifact bytes {size} exceed {max_bytes}")
    try:
        payload = json.loads(safe_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DlqIntegrityError("DPONE_DLQ_ARTIFACT_INVALID: artifact is not valid JSON") from exc
    if not isinstance(payload, Mapping):
        raise DlqIntegrityError("DPONE_DLQ_ARTIFACT_INVALID: artifact root must be an object")
    return payload


def atomic_json(root: Path, path: Path, payload: Mapping[str, object], *, max_bytes: int) -> None:
    body = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if len(body.encode("utf-8")) > max_bytes:
        raise DlqRecordTooLarge(f"DPONE_DLQ_INDEX_TOO_LARGE: index exceeds configured {max_bytes} byte limit")
    temp: Path | None = None
    try:
        with NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, prefix=".tmp-", delete=False) as handle:
            temp = Path(handle.name)
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)
        fsync_directory(path.parent)
    finally:
        if temp is not None:
            temp.unlink(missing_ok=True)


def json_bytes(payload: Mapping[str, object]) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")


__all__ = [
    "DlqIntegrityError",
    "DlqPathError",
    "DlqRecordTooLarge",
    "DlqStoreError",
    "atomic_json",
    "component",
    "confined",
    "fsync_directory",
    "json_bytes",
    "mkdir",
    "read_json",
]
