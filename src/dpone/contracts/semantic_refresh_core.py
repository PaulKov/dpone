"""Dependency-free canonical primitives for semantic-refresh contracts."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, TypeVar

from dpone.contracts.strict_json import StrictJsonError, strict_json_object

SEMANTIC_REFRESH_CONTRACT_ERROR = "DPONE_SEMANTIC_REFRESH_CONTRACT_INVALID"
_SHA256_PREFIX = "sha256:"
_EnumT = TypeVar("_EnumT", bound=Enum)


class SemanticRefreshContractError(ValueError):
    """A closed semantic-refresh document violates its public contract."""

    def __init__(self, message: str) -> None:
        self.code = SEMANTIC_REFRESH_CONTRACT_ERROR
        super().__init__(message)


def canonical_semantic_refresh_bytes(value: Mapping[str, object]) -> bytes:
    """Serialize a semantic-refresh value using the repository canonical JSON form."""

    try:
        return json.dumps(
            dict(value),
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise SemanticRefreshContractError("semantic-refresh value is not canonical JSON") from exc


def semantic_refresh_sha256(value: Mapping[str, object]) -> str:
    """Return the canonical lowercase SHA-256 identity of one mapping."""

    return _SHA256_PREFIX + hashlib.sha256(canonical_semantic_refresh_bytes(value)).hexdigest()


def decode_semantic_refresh_json(payload: bytes | str) -> dict[str, Any]:
    """Decode one duplicate-free finite JSON object."""

    try:
        return strict_json_object(payload)
    except StrictJsonError as exc:
        raise SemanticRefreshContractError("semantic-refresh JSON is invalid") from exc


def require_closed_mapping(
    value: object,
    field: str,
    *,
    required: frozenset[str],
    optional: frozenset[str] = frozenset(),
) -> Mapping[str, Any]:
    """Return a mapping only when all and only declared fields are present."""

    if not isinstance(value, Mapping):
        raise SemanticRefreshContractError(f"{field} must be an object")
    keys = set(value)
    missing = required - keys
    unknown = keys - required - optional
    if missing or unknown:
        raise SemanticRefreshContractError(
            f"{field} fields are invalid; missing={sorted(missing)}, unknown={sorted(unknown)}"
        )
    return value


def require_text(value: object, field: str) -> str:
    """Return a non-empty, whitespace-stable text field."""

    if not isinstance(value, str) or not value or value != value.strip() or "\x00" in value:
        raise SemanticRefreshContractError(f"{field} must be canonical non-empty text")
    return value


def require_digest(value: object, field: str) -> str:
    """Return one canonical lowercase ``sha256:`` digest."""

    if (
        not isinstance(value, str)
        or len(value) != 71
        or not value.startswith(_SHA256_PREFIX)
        or any(character not in "0123456789abcdef" for character in value[7:])
    ):
        raise SemanticRefreshContractError(f"{field} must be a canonical sha256 digest")
    return value


def require_positive_int(value: object, field: str) -> int:
    """Return a positive integer, rejecting booleans."""

    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise SemanticRefreshContractError(f"{field} must be a positive integer")
    return value


def require_enum(value: object, field: str, enum_type: type[_EnumT]) -> _EnumT:
    """Parse a closed string enum without coercing non-string values."""

    if not isinstance(value, str):
        raise SemanticRefreshContractError(f"{field} must be a string")
    try:
        return enum_type(value)
    except ValueError as exc:
        raise SemanticRefreshContractError(f"{field} is unsupported") from exc


def require_sorted_unique_strings(
    value: object,
    field: str,
    *,
    allow_empty: bool = False,
) -> tuple[str, ...]:
    """Parse a canonical sorted set serialized as a JSON array."""

    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        raise SemanticRefreshContractError(f"{field} must be an array")
    items = tuple(require_text(item, field) for item in value)
    if (not allow_empty and not items) or items != tuple(sorted(set(items))):
        raise SemanticRefreshContractError(f"{field} must be a canonical sorted unique array")
    return items


def canonical_string_set(values: Sequence[str], field: str, *, allow_empty: bool = False) -> tuple[str, ...]:
    """Normalize caller-owned values before constructing a canonical document."""

    if isinstance(values, str | bytes):
        raise SemanticRefreshContractError(f"{field} must be a sequence of strings")
    items = tuple(require_text(item, field) for item in values)
    if not allow_empty and not items:
        raise SemanticRefreshContractError(f"{field} must not be empty")
    if len(items) != len(set(items)):
        raise SemanticRefreshContractError(f"{field} must contain unique values")
    return tuple(sorted(items))


def require_document_digest(payload: Mapping[str, object], digest_field: str) -> str:
    """Recompute and validate a document digest excluding only its own field."""

    declared = require_digest(payload.get(digest_field), digest_field)
    unsigned = {key: value for key, value in payload.items() if key != digest_field}
    if declared != semantic_refresh_sha256(unsigned):
        raise SemanticRefreshContractError(f"{digest_field} differs from canonical document content")
    return declared


def canonical_utc_day(value: str, field: str) -> datetime:
    """Parse a canonical midnight UTC timestamp ending in ``Z``."""

    require_text(value, field)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SemanticRefreshContractError(f"{field} must be a canonical UTC datetime") from exc
    if (
        not value.endswith("Z")
        or parsed.tzinfo is None
        or parsed.utcoffset() != timedelta(0)
        or parsed.astimezone(timezone.utc).time() != datetime.min.time()  # noqa: UP017
    ):
        raise SemanticRefreshContractError(f"{field} must be midnight UTC with a Z suffix")
    return parsed


def require_daily_scope(start: str, end: str) -> None:
    """Require one exact half-open UTC day scope."""

    if canonical_utc_day(end, "scope_end") - canonical_utc_day(start, "scope_start") != timedelta(days=1):
        raise SemanticRefreshContractError("semantic-refresh scope must be one UTC day")


__all__ = [
    "SEMANTIC_REFRESH_CONTRACT_ERROR",
    "SemanticRefreshContractError",
    "canonical_semantic_refresh_bytes",
    "canonical_string_set",
    "canonical_utc_day",
    "decode_semantic_refresh_json",
    "require_closed_mapping",
    "require_daily_scope",
    "require_digest",
    "require_document_digest",
    "require_enum",
    "require_positive_int",
    "require_sorted_unique_strings",
    "require_text",
    "semantic_refresh_sha256",
]
