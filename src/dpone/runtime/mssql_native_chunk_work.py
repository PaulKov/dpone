"""Process-pool work values for bounded native encoding."""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any

from dpone.ports.mssql_native import EncodedNativeFile
from dpone.runtime.mssql_native_chunks_files import encode_native_frame
from dpone.runtime.mssql_native_chunks_observations import delivery_session
from dpone.runtime.native_delivery_observations import BoundedNativeDeliveryObserver


def encode_native_work(*args: Any, observed: bool = False) -> tuple[Any, dict[str, Any], dict[str, Any] | None]:
    """Encode one frame and return process-local timing evidence."""

    start = time.monotonic()
    session = delivery_session(BoundedNativeDeliveryObserver(max_observations=2) if observed else None)
    try:
        with session.recorder("encoder").phase("encode", ordinal=args[3], rows=len(args[1]), encoded_bytes=args[5]):
            value: EncodedNativeFile | Exception = encode_native_frame(*args)
    except Exception as error:
        if not observed:
            raise
        value = error
    legacy = dict(phase="encode", start=start, end=time.monotonic(), worker=os.getpid())
    return value, legacy, session.snapshot() if observed else None


def encode_observed_native_work(*args: Any) -> tuple[Any, dict[str, Any], dict[str, Any] | None]:
    """Encode one frame with bounded phase observations."""

    return encode_native_work(*args, observed=True)


@dataclass
class NativeChunkWork:
    """Parent-owned state for one bounded encode/import unit."""

    ordinal: int
    encoded_bytes: int
    file: EncodedNativeFile | None = None
    attempt: int = 0


__all__ = ["NativeChunkWork", "encode_native_work", "encode_observed_native_work"]
