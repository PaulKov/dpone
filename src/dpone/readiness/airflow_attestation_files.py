"""Bounded immutable file operations for artifact-attestation commands."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

from dpone.runtime.deployment_cache_common import open_regular_file


def read_bounded_regular_file(path: Path, *, maximum: int, label: str) -> bytes:
    descriptor = open_regular_file(
        path,
        missing_code="DPONE_ARTIFACT_ATTESTATION_INPUT_INVALID",
        invalid_code="DPONE_ARTIFACT_ATTESTATION_INPUT_INVALID",
        label=label,
        root=path.absolute().parent,
    )
    chunks: list[bytes] = []
    total = 0
    try:
        while total <= maximum:
            chunk = os.read(descriptor, min(64 * 1024, maximum + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
    finally:
        os.close(descriptor)
    value = b"".join(chunks)
    if not value or len(value) > maximum:
        raise ValueError(f"{label} size is invalid")
    return value


def write_immutable_file(path: Path, payload: bytes, *, label: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        current = read_bounded_regular_file(path, maximum=len(payload), label=label)
        if current != payload:
            raise ValueError(f"{label} already contains different bytes")
        return
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            current = read_bounded_regular_file(path, maximum=len(payload), label=label)
            if current != payload:
                raise ValueError(f"{label} already contains different bytes") from None
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


__all__ = ["read_bounded_regular_file", "write_immutable_file"]
