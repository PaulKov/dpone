"""Durable content-addressed writer for public MSSQL delivery evidence."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from dpone.contracts.mssql_native_delivery_evidence import validate_mssql_native_delivery_evidence


def _canonical(payload: object) -> bytes:
    return (json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode()


def _atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, suffix=".tmp", delete=False) as temporary:
        temporary.write(payload)
        temporary.flush()
        os.fsync(temporary.fileno())
        temporary_path = Path(temporary.name)
    try:
        temporary_path.replace(path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary_path.unlink(missing_ok=True)


def write_mssql_native_delivery_evidence(root: Path, payload: dict[str, Any]) -> Path:
    """Write an immutable revision, then atomically select it with a small pointer."""
    validate_mssql_native_delivery_evidence(payload)
    encoded = _canonical(payload)
    digest = hashlib.sha256(encoded).hexdigest()
    revision = root / f"{digest}.json"
    if revision.exists():
        if revision.read_bytes() != encoded:
            raise ValueError("mssql_native.delivery_evidence.revision_collision")
    else:
        _atomic_write(revision, encoded)
    pointer = _canonical({"schema_version": 1, "sha256": digest})
    _atomic_write(root / "current.json", pointer)
    return revision


__all__ = ["write_mssql_native_delivery_evidence"]
