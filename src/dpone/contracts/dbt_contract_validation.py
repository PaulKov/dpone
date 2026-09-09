"""Shared dependency-light validation primitives for dbt public contracts."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import PurePosixPath
from typing import Any


class DbtPublishingError(RuntimeError):
    """Stable dbt contract failure with a public error code."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        path: str | None = None,
        remediation: str | None = None,
    ) -> None:
        self.code = code
        self.path = path
        self.remediation = remediation
        super().__init__(message)


def sha256_bytes(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def artifact_json_bytes(payload: Any) -> bytes:
    """Encode human-readable artifacts; distinct from compact identity encoding."""

    return (json.dumps(payload, allow_nan=False, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")


def contract_error(code: str, message: str) -> DbtPublishingError:
    return DbtPublishingError(code, message)


def require_text(value: object, field_name: str, code: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise contract_error(code, f"{field_name} must be a non-empty string")
    return value


def require_token(value: object, field_name: str, code: str) -> str:
    text = require_text(value, field_name, code)
    if text.startswith("-") or any(character.isspace() or character == "\x00" for character in text):
        raise contract_error(code, f"{field_name} must be one shell-free command token")
    return text


def require_digest(value: object, field_name: str, code: str) -> str:
    if (
        not isinstance(value, str)
        or not value.startswith("sha256:")
        or len(value) != 71
        or any(character not in "0123456789abcdef" for character in value[7:])
    ):
        raise contract_error(code, f"{field_name} must be a canonical sha256 digest")
    return value


def require_positive(value: object, field_name: str, code: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise contract_error(code, f"{field_name} must be a positive integer")
    return value


def require_non_negative(value: object, field_name: str, code: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise contract_error(code, f"{field_name} must be a non-negative integer")
    return value


def require_relative(
    value: object,
    field_name: str,
    code: str,
    *,
    allow_dot: bool = False,
) -> str:
    text = require_text(value, field_name, code)
    path = PurePosixPath(text)
    if not (allow_dot and text == ".") and (
        "\\" in text
        or path.is_absolute()
        or path.as_posix() != text
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise contract_error(code, f"{field_name} is unsafe")
    return text


def require_strict_mapping(
    value: object,
    field_name: str,
    keys: frozenset[str],
    code: str,
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != keys:
        raise contract_error(code, f"{field_name} must contain exactly {sorted(keys)}")
    return value


def require_strings(value: object, field_name: str, code: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise contract_error(code, f"{field_name} must be an array of strings")
    return tuple(value)


def canonical_fingerprint(value: Mapping[str, Any]) -> str:
    raw = json.dumps(
        dict(value),
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return sha256_bytes(raw)


__all__ = [
    "DbtPublishingError",
    "artifact_json_bytes",
    "canonical_fingerprint",
    "contract_error",
    "require_digest",
    "require_non_negative",
    "require_positive",
    "require_relative",
    "require_strict_mapping",
    "require_strings",
    "require_text",
    "require_token",
    "sha256_bytes",
]
