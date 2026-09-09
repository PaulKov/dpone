"""Render-neutral immutable AST for portable relation scopes."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Literal, Protocol, TypeAlias

from dpone.contracts.portable_scope_literals import (
    RANGE_LITERAL_KINDS,
    PortableLiteral,
    PortableLiteralKind,
    PortableScopeContractError,
    blocked,
    parse_portable_literal,
    require_mapping,
)

PORTABLE_SCOPE_VERSION = 1
PORTABLE_IDENTIFIER_UTF8_LIMIT = 63


@dataclass(frozen=True, slots=True)
class PortableRangeBound:
    """One inclusive or exclusive range boundary."""

    value: PortableLiteral
    inclusive: bool

    def __post_init__(self) -> None:
        if not isinstance(self.value, PortableLiteral):
            blocked("portable_scope.range.bound_literal", "bound value must be a PortableLiteral")
        if not isinstance(self.inclusive, bool):
            blocked("portable_scope.range.bound_inclusive", "inclusive must be a boolean")

    def to_contract(self) -> dict[str, object]:
        return {"inclusive": self.inclusive, "value": self.value.to_contract()}


@dataclass(frozen=True, slots=True)
class PortableEqualityScope:
    """Equality scope over one exact catalog identifier."""

    column: str
    value: PortableLiteral
    version: int = PORTABLE_SCOPE_VERSION
    kind: Literal["equality"] = "equality"

    def __post_init__(self) -> None:
        validate_scope_header(self.column, self.version, self.kind, expected_kind="equality")
        if not isinstance(self.value, PortableLiteral):
            blocked("portable_scope.equality.value", "value must be a PortableLiteral")

    def to_contract(self) -> dict[str, object]:
        return {
            "column": self.column,
            "kind": self.kind,
            "value": self.value.to_contract(),
            "version": self.version,
        }


@dataclass(frozen=True, slots=True)
class PortableRangeScope:
    """Range scope over one exact catalog identifier."""

    column: str
    lower: PortableRangeBound | None
    upper: PortableRangeBound | None
    version: int = PORTABLE_SCOPE_VERSION
    kind: Literal["range"] = "range"

    def __post_init__(self) -> None:
        validate_scope_header(self.column, self.version, self.kind, expected_kind="range")
        if self.lower is not None and not isinstance(self.lower, PortableRangeBound):
            blocked("portable_scope.range.lower", "lower must be a PortableRangeBound")
        if self.upper is not None and not isinstance(self.upper, PortableRangeBound):
            blocked("portable_scope.range.upper", "upper must be a PortableRangeBound")
        validate_range(self.lower, self.upper)

    def to_contract(self) -> dict[str, object]:
        result: dict[str, object] = {
            "column": self.column,
            "kind": self.kind,
            "version": self.version,
        }
        if self.lower is not None:
            result["lower"] = self.lower.to_contract()
        if self.upper is not None:
            result["upper"] = self.upper.to_contract()
        return result


PortableRelationScope: TypeAlias = PortableEqualityScope | PortableRangeScope


class PortableScopeRenderBinding(Protocol):
    """Minimal catalog proof consumed by dialect-specific renderers.

    Renderers depend on this structural contract instead of the concrete
    binding implementation. Runtime admission remains the sole producer of a
    validated binding, while rendering stays independently testable.
    """

    @property
    def comparison_contract(self) -> str:
        """Return the catalog-certified cross-dialect comparison contract."""

        ...

    @property
    def literal_type(self) -> str:
        """Return the validated portable literal family."""

        ...

    @property
    def target_type(self) -> str:
        """Return the normalized SQL Server physical type."""

        ...

    def require_scope(self, scope: PortableRelationScope) -> None:
        """Reject a binding that was issued for a different AST."""


def portable_scope_contract(scope: PortableRelationScope) -> dict[str, object]:
    """Return the canonical AST contract consumed by route identities."""

    return scope.to_contract()


def portable_scope_canonical_json(scope: PortableRelationScope) -> str:
    """Serialize the AST without dialect SQL or locale-dependent formatting."""

    return json.dumps(
        portable_scope_contract(scope),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def portable_scope_sha256(scope: PortableRelationScope) -> bytes:
    """Return the canonical AST digest used by route and operation identity."""

    return hashlib.sha256(portable_scope_canonical_json(scope).encode("utf-8")).digest()


def validate_catalog_identifier(raw: Any) -> str:
    """Validate one exact logical identifier without parsing SQL syntax."""

    if not isinstance(raw, str):
        blocked("portable_scope.column", "column must be one string identifier")
    if raw == "":
        blocked("portable_scope.column_empty", "column cannot be empty")
    if "\x00" in raw:
        blocked("portable_scope.column_nul", "column cannot contain NUL")
    if len(raw.encode("utf-8")) > PORTABLE_IDENTIFIER_UTF8_LIMIT:
        blocked(
            "portable_scope.column_length",
            f"column exceeds the {PORTABLE_IDENTIFIER_UTF8_LIMIT}-byte PostgreSQL/MSSQL portable limit",
        )
    return raw


def validate_scope_version(version: Any) -> int:
    """Validate the finite AST version before parsing nested values."""

    if isinstance(version, bool) or version != PORTABLE_SCOPE_VERSION:
        blocked("portable_scope.version", f"version must be {PORTABLE_SCOPE_VERSION}")
    return version


def validate_scope_header(
    column: Any,
    version: Any,
    kind: Any,
    *,
    expected_kind: str,
) -> None:
    validate_catalog_identifier(column)
    validate_scope_version(version)
    if kind != expected_kind:
        blocked("portable_scope.kind", f"kind must be {expected_kind}")


def validate_range(
    lower: PortableRangeBound | None,
    upper: PortableRangeBound | None,
) -> None:
    """Prove bounds share one portable type and describe a nonempty range."""

    if lower is None and upper is None:
        blocked("portable_scope.range.bounds", "at least one bound is required")
    literal_kinds = {bound.value.kind for bound in (lower, upper) if bound is not None}
    if len(literal_kinds) != 1:
        blocked("portable_scope.range.type_mismatch", "both bounds must use one literal type")
    literal_kind = next(iter(literal_kinds))
    if literal_kind not in RANGE_LITERAL_KINDS:
        blocked("portable_scope.range.literal_type", f"{literal_kind} ranges are not portable")
    if lower is None or upper is None:
        return
    if lower.value.value > upper.value.value:
        blocked("portable_scope.range.order", "lower bound cannot exceed upper bound")
    if lower.value.value == upper.value.value and (not lower.inclusive or not upper.inclusive):
        blocked("portable_scope.range.empty", "exclusive equal bounds describe an empty scope")


__all__ = [
    "PORTABLE_IDENTIFIER_UTF8_LIMIT",
    "PORTABLE_SCOPE_VERSION",
    "PortableEqualityScope",
    "PortableLiteral",
    "PortableLiteralKind",
    "PortableRangeBound",
    "PortableRangeScope",
    "PortableRelationScope",
    "PortableScopeRenderBinding",
    "PortableScopeContractError",
    "blocked",
    "parse_portable_literal",
    "portable_scope_canonical_json",
    "portable_scope_contract",
    "portable_scope_sha256",
    "require_mapping",
    "validate_catalog_identifier",
    "validate_range",
    "validate_scope_version",
]
