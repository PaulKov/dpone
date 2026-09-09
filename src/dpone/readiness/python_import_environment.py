"""Bounded, portable environment snapshots for import-probe children."""

from __future__ import annotations

import os
from collections.abc import Mapping
from itertools import islice
from typing import cast

MAX_ENVIRONMENT_ENTRIES = 2_048
MAX_ENVIRONMENT_BYTES = 64 * 1024
MAX_ENVIRONMENT_UTF16_UNITS = 32_767

_CANONICAL_ENVIRON_TYPE = getattr(os, "_Environ", None)


def capture_bounded_environment() -> dict[str, str] | None:
    """Copy canonical ``os.environ`` within POSIX and Windows process limits."""

    if not isinstance(_CANONICAL_ENVIRON_TYPE, type):
        return None
    return _copy_bounded_environment(os.environ, _CANONICAL_ENVIRON_TYPE)


def validate_child_environment(environment: object) -> dict[str, str] | None:
    """Validate the final exact-dict environment passed to ``subprocess``."""

    return _copy_bounded_environment(environment, dict)


def _copy_bounded_environment(
    environment: object,
    expected_type: type[object],
) -> dict[str, str] | None:
    """Consume at most the contract cap without trusting mapping length."""

    if type(environment) is not expected_type:
        return None
    try:
        mapping = cast(Mapping[object, object], environment)
        items = tuple(islice(iter(mapping.items()), MAX_ENVIRONMENT_ENTRIES + 1))
    except BaseException:
        return None
    if len(items) > MAX_ENVIRONMENT_ENTRIES:
        return None

    captured: dict[str, str] = {}
    encoded_bytes = 0
    utf16_units = 1
    for item in items:
        if type(item) is not tuple or len(item) != 2:
            return None
        key, value = item
        if (
            type(key) is not str
            or type(value) is not str
            or not _environment_key_is_valid(key)
            or "\x00" in value
            or key in captured
        ):
            return None
        character_count = len(key) + len(value) + 2
        if character_count > MAX_ENVIRONMENT_UTF16_UNITS - utf16_units:
            return None
        try:
            key_bytes = os.fsencode(key)
            value_bytes = os.fsencode(value)
            key_utf16 = key.encode("utf-16-le", "surrogatepass")
            value_utf16 = value.encode("utf-16-le", "surrogatepass")
            if os.fsdecode(key_bytes) != key or os.fsdecode(value_bytes) != value:
                return None
        except BaseException:
            return None
        encoded_bytes += len(key_bytes) + len(value_bytes) + 2
        utf16_units += len(key_utf16) // 2 + len(value_utf16) // 2 + 2
        if encoded_bytes > MAX_ENVIRONMENT_BYTES or utf16_units > MAX_ENVIRONMENT_UTF16_UNITS:
            return None
        captured[key] = value
    return captured


def _environment_key_is_valid(key: str) -> bool:
    if not key or "\x00" in key:
        return False
    if "=" not in key:
        return True
    return os.name == "nt" and key.startswith("=") and len(key) > 1 and "=" not in key[1:]


__all__ = [
    "MAX_ENVIRONMENT_BYTES",
    "MAX_ENVIRONMENT_ENTRIES",
    "MAX_ENVIRONMENT_UTF16_UNITS",
    "capture_bounded_environment",
    "validate_child_environment",
]
