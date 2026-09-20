"""Bounded local input adapter for immutable Airflow runtime authority."""

from __future__ import annotations

import os
import stat
from pathlib import Path

from dpone.runtime.runtime_authority_payload import (
    MAX_RUNTIME_AUTHORITY_PAYLOAD_BYTES,
    ImmutableRuntimeAuthorityPayload,
)


def immutable_runtime_authority_payload_from_file(
    path: str | Path,
    *,
    expected_sha256: str,
) -> ImmutableRuntimeAuthorityPayload:
    """Read one regular non-symlink file once with a strict decoded bound."""

    source = Path(path)
    try:
        descriptor = os.open(source, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        with os.fdopen(descriptor, "rb", closefd=True) as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                raise OSError("unsafe source")
            payload = stream.read(MAX_RUNTIME_AUTHORITY_PAYLOAD_BYTES + 1)
            if stream.read(1):
                raise OSError("source changed while reading")
    except OSError as exc:
        raise ValueError("runtime authority payload file must be a readable regular non-symlink file") from exc
    return ImmutableRuntimeAuthorityPayload.from_bytes(payload, expected_sha256=expected_sha256)


__all__ = ["immutable_runtime_authority_payload_from_file"]
