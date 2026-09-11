"""Bounded canonical authority documents and strict scalar validation."""

from __future__ import annotations

import hashlib
import re
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

MAX_DOCUMENT_BYTES = 1024 * 1024


class NonproductionAuthorityError(ValueError):
    """Stable, value-free error; never carry credential or verifier output text."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(f"DPONE_NONPRODUCTION_AUTHORITY_INVALID: {reason}")


def exact_fields(value: object, expected: set[str]) -> dict[str, Any]:
    """Require an ordinary closed object, with no coercion of keys or values."""
    if type(value) is not dict or set(value) != expected:
        raise NonproductionAuthorityError("fields")
    return value


def canonical_document(value: dict[str, Any]) -> bytes:
    """Use the existing finite UTF-8 canonicalizer with a fixed transport bound."""
    try:
        raw = canonical_json_bytes(value)
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise NonproductionAuthorityError("document") from None
    if len(raw) > MAX_DOCUMENT_BYTES:
        raise NonproductionAuthorityError("document_budget")
    return raw


def document_sha256(value: dict[str, Any]) -> str:
    """Hash exact canonical bytes, with no legacy path/string normalization."""
    return "sha256:" + hashlib.sha256(canonical_document(value)).hexdigest()


def parse_document(raw: bytes, schema: str) -> dict[str, Any]:
    """Reject duplicate/unknown schema, noncanonical bytes and oversized input."""
    if type(raw) is not bytes or not 1 <= len(raw) <= MAX_DOCUMENT_BYTES:
        raise NonproductionAuthorityError("document_budget")
    try:
        body = strict_json_object(raw)
        if body.get("schema") != schema or canonical_document(body) != raw:
            raise NonproductionAuthorityError("document_identity")
        return body
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise NonproductionAuthorityError("document") from None


def digest(value: object) -> str:
    if type(value) is not str or not is_canonical_sha256_digest(value):
        raise NonproductionAuthorityError("digest")
    return value


def text(value: object, *, maximum: int = 512) -> str:
    if (
        type(value) is not str
        or not 1 <= len(value) <= maximum
        or value != value.strip()
        or any(ord(c) < 32 for c in value)
    ):
        raise NonproductionAuthorityError("text")
    return value


def uuid_text(value: object, *, version4: bool = False) -> str:
    try:
        if (
            type(value) is not str
            or str(UUID(value)) != value
            or UUID(value).int == 0
            or (version4 and UUID(value).version != 4)
        ):
            raise ValueError
    except (ValueError, TypeError, AttributeError):
        raise NonproductionAuthorityError("uuid") from None
    return value


def repository(value: object) -> str:
    """Require one credential-free HTTPS repository spelling, with no URL options."""
    value = text(value)
    try:
        parsed = urlsplit(value)
        parts = parsed.path.split("/")[1:]
        if (
            not value.startswith("https://")
            or parsed.scheme != "https"
            or parsed.netloc != parsed.hostname
            or not parsed.hostname
            or len(parsed.hostname) > 253
            or any(
                re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label) is None
                for label in parsed.hostname.split(".")
            )
            or parsed.query
            or parsed.fragment
            or "?" in value
            or "#" in value
            or len(parts) < 2
            or any(part in {"", ".", ".."} or re.fullmatch(r"[A-Za-z0-9_.-]+", part) is None for part in parts)
        ):
            raise ValueError
    except ValueError:
        raise NonproductionAuthorityError("source_repository") from None
    return value


def sequence(value: object, *, maximum: int, allow_empty: bool = False) -> list[Any]:
    if type(value) is not list or not (0 if allow_empty else 1) <= len(value) <= maximum:
        raise NonproductionAuthorityError("array")
    return value


def ordered(values: tuple[Any, ...], *, maximum: int, allow_empty: bool = False) -> None:
    if type(values) is not tuple or not (0 if allow_empty else 1) <= len(values) <= maximum:
        raise NonproductionAuthorityError("closure")
    if values != tuple(sorted(set(values))):
        raise NonproductionAuthorityError("closure")


# Preserve historic public reflection and pickle locators.
NonproductionAuthorityError.__module__ = "dpone.contracts.nonproduction_scope"
exact_fields.__module__ = "dpone.contracts.nonproduction_scope"
canonical_document.__module__ = "dpone.contracts.nonproduction_scope"
document_sha256.__module__ = "dpone.contracts.nonproduction_scope"
parse_document.__module__ = "dpone.contracts.nonproduction_scope"
digest.__module__ = "dpone.contracts.nonproduction_scope"
text.__module__ = "dpone.contracts.nonproduction_scope"
uuid_text.__module__ = "dpone.contracts.nonproduction_scope"
repository.__module__ = "dpone.contracts.nonproduction_scope"
sequence.__module__ = "dpone.contracts.nonproduction_scope"
ordered.__module__ = "dpone.contracts.nonproduction_scope"
