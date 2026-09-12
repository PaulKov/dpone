"""Exact primitive admission for PostgreSQL selected-relation schema models."""

from __future__ import annotations

import unicodedata
from collections.abc import Mapping
from typing import Any


class SourceSchemaPrimitiveError(ValueError):
    """A source-schema primitive is outside the closed V1 domain."""


def closed_mapping(value: object, keys: frozenset[str]) -> Mapping[str, Any]:
    if type(value) is not dict or set(value) != keys:
        raise SourceSchemaPrimitiveError("closed mapping required")
    return value


def exact_bool(value: object) -> bool:
    if type(value) is not bool:
        raise SourceSchemaPrimitiveError("exact bool required")
    return value


def exact_int(value: object, *, minimum: int, maximum: int) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise SourceSchemaPrimitiveError("bounded exact int required")
    return value


def exact_text(value: object, *, nfc: bool = False) -> str:
    if type(value) is not str:
        raise SourceSchemaPrimitiveError("exact text required")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError:
        raise SourceSchemaPrimitiveError("valid UTF-8 required") from None
    if not value or "\0" in value or len(encoded) > 63:
        raise SourceSchemaPrimitiveError("bounded name required")
    if nfc and (value != value.strip() or unicodedata.normalize("NFC", value) != value):
        raise SourceSchemaPrimitiveError("canonical identifier required")
    return value


def exact_digest(value: object) -> bytes:
    if type(value) is not bytes or len(value) != 32:
        raise SourceSchemaPrimitiveError("exact SHA-256 digest required")
    return value


__all__ = [
    "SourceSchemaPrimitiveError",
    "closed_mapping",
    "exact_bool",
    "exact_digest",
    "exact_int",
    "exact_text",
]
