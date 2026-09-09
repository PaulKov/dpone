"""Strict JSON and digest primitives for release-candidate authority."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

FULL_SHA = re.compile(r"^[0-9a-f]{40}$")
DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
REPOSITORY = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
RELEASE = re.compile(r"^v(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)$")
MAX_JSON_BYTES = 4 * 1024 * 1024


def canonical_json_bytes(value: object) -> bytes:
    """Return canonical finite JSON with a trailing LF."""

    _validate_finite(value)
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    """Return one tagged lowercase SHA-256 digest."""

    return f"sha256:{hashlib.sha256(value).hexdigest()}"


def canonical_json_sha256(value: object) -> str:
    """Digest the exact canonical JSON serialization of one finite value."""

    return sha256_bytes(canonical_json_bytes(value))


def strict_json_object(raw: bytes, *, field: str) -> dict[str, Any]:
    """Decode one bounded, duplicate-free, finite UTF-8 JSON object."""

    if len(raw) > MAX_JSON_BYTES:
        raise ValueError(f"{field} exceeds the {MAX_JSON_BYTES}-byte limit")

    def object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"{field} contains duplicate JSON key {key!r}")
            result[key] = value
        return result

    def non_finite(value: str) -> None:
        raise ValueError(f"{field} contains non-finite JSON number {value}")

    try:
        payload = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=object_pairs,
            parse_constant=non_finite,
        )
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{field} must contain one UTF-8 JSON object") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"{field} must contain one JSON object")
    _validate_finite(payload)
    return payload


def read_strict_json(path: Path, *, field: str) -> tuple[dict[str, Any], bytes]:
    """Read and decode one bounded authority input."""

    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ValueError(f"{field} is unavailable at {path}") from exc
    return strict_json_object(raw, field=field), raw


def write_canonical_json(path: Path, payload: Mapping[str, Any]) -> bytes:
    """Create one canonical JSON file without overwriting existing evidence."""

    raw = canonical_json_bytes(payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError as exc:
        raise ValueError(f"authority output already exists: {path}") from exc
    return raw


def require_exact_keys(payload: Mapping[str, Any], expected: frozenset[str], *, field: str) -> None:
    """Reject missing and unknown mapping fields."""

    actual = frozenset(payload)
    if actual != expected:
        missing = sorted(expected - actual)
        unknown = sorted(actual - expected)
        raise ValueError(f"{field} field set is invalid; missing={missing}, unknown={unknown}")


def require_string(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a non-empty string")
    return value


def require_positive_int(value: Any, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def require_nonnegative_int(value: Any, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value


def require_positive_number(value: Any, *, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"{field} must be a positive finite number")
    current = float(value)
    if not math.isfinite(current) or current <= 0:
        raise ValueError(f"{field} must be a positive finite number")
    return current


def require_digest(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or DIGEST.fullmatch(value) is None:
        raise ValueError(f"{field} must be a tagged lowercase SHA-256 digest")
    return value


def _validate_finite(value: object) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("authority JSON contains a non-finite number")
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError("authority JSON object keys must be strings")
            _validate_finite(item)
    elif isinstance(value, list | tuple):
        for item in value:
            _validate_finite(item)


__all__ = [
    "DIGEST",
    "FULL_SHA",
    "RELEASE",
    "REPOSITORY",
    "canonical_json_sha256",
    "canonical_json_bytes",
    "read_strict_json",
    "require_digest",
    "require_exact_keys",
    "require_nonnegative_int",
    "require_positive_int",
    "require_positive_number",
    "require_string",
    "sha256_bytes",
    "strict_json_object",
    "write_canonical_json",
]
