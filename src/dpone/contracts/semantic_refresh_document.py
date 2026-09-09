"""Small shared codec surface for versioned semantic-refresh documents."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, ClassVar, Protocol, Self

from dpone.contracts.semantic_refresh_core import (
    SemanticRefreshContractError,
    canonical_semantic_refresh_bytes,
    canonical_string_set,
    decode_semantic_refresh_json,
    require_closed_mapping,
    require_daily_scope,
    require_digest,
    require_enum,
    require_positive_int,
    require_sorted_unique_strings,
    require_text,
    semantic_refresh_sha256,
)


class SemanticRefreshDocument(Protocol):
    """Structural public interface implemented by every canonical document."""

    schema: str
    schema_id: ClassVar[str]
    digest_field: ClassVar[str]

    @classmethod
    def from_mapping(cls, value: object) -> Self: ...

    def to_dict(self) -> dict[str, object]: ...


class SemanticRefreshDocumentCodec:
    """Mixin providing canonical JSON and common identity validation."""

    schema_id: ClassVar[str]
    digest_field: ClassVar[str]

    @classmethod
    def from_json(cls, payload: bytes | str) -> Self:
        """Decode duplicate-free JSON and validate the closed document."""

        from_mapping = getattr(cls, "from_mapping")
        return from_mapping(decode_semantic_refresh_json(payload))

    def canonical_bytes(self) -> bytes:
        """Return deterministic canonical bytes including the declared digest."""

        to_dict = getattr(self, "to_dict")
        return canonical_semantic_refresh_bytes(to_dict())


def validate_schema(schema: object, expected: str) -> str:
    """Require the exact versioned schema discriminator."""

    value = require_text(schema, "schema")
    if value != expected:
        raise SemanticRefreshContractError(f"schema must be {expected}")
    return value


def validate_digest(unsigned: Mapping[str, object], declared: object, field: str) -> str:
    """Recompute one document digest from the unsigned public mapping."""

    digest = require_digest(declared, field)
    if digest != semantic_refresh_sha256(unsigned):
        raise SemanticRefreshContractError(f"{field} differs from canonical document content")
    return digest


def parsed_document_digest(raw: Mapping[str, Any], field: str) -> str:
    """Read a digest field for a concrete document constructor."""

    return require_digest(raw.get(field), field)


__all__ = [
    "SemanticRefreshDocument",
    "SemanticRefreshDocumentCodec",
    "SemanticRefreshContractError",
    "canonical_string_set",
    "parsed_document_digest",
    "require_closed_mapping",
    "require_daily_scope",
    "require_digest",
    "require_enum",
    "require_positive_int",
    "require_sorted_unique_strings",
    "require_text",
    "semantic_refresh_sha256",
    "validate_digest",
    "validate_schema",
]
