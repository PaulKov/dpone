"""Pickle-stable worker values for bounded native encoding."""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any

from dpone.runtime.mssql_native_chunks_files import encode_native_frame
from dpone.runtime.mssql_native_chunks_observations import delivery_session
from dpone.runtime.native_delivery_observations import BoundedNativeDeliveryObserver


def encode(*args: Any, observed: bool = False) -> tuple[Any, dict[str, Any], dict[str, Any] | None]:
    start = time.monotonic()
    session = delivery_session(BoundedNativeDeliveryObserver(max_observations=2) if observed else None)
    try:
        with session.recorder("encoder").phase("encode", ordinal=args[3], rows=len(args[1]), encoded_bytes=args[5]):
            value: Any = encode_native_frame(*args)
    except Exception as error:
        if not observed:
            raise
        value = error
    legacy = dict(phase="encode", start=start, end=time.monotonic(), worker=os.getpid())
    return value, legacy, session.snapshot() if observed else None


def encode_observed(*args: Any) -> tuple[Any, dict[str, Any], dict[str, Any] | None]:
    return encode(*args, observed=True)


@dataclass
class Work:
    ordinal: int
    encoded_bytes: int
    file: Any | None = None
    attempt: int = 0
