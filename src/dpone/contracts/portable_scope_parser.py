"""Closed-object parser for the portable relation-scope AST."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.contracts.portable_scope_model import (
    PORTABLE_IDENTIFIER_UTF8_LIMIT,
    PORTABLE_SCOPE_VERSION,
    PortableEqualityScope,
    PortableLiteral,
    PortableLiteralKind,
    PortableRangeBound,
    PortableRangeScope,
    PortableRelationScope,
    PortableScopeContractError,
    PortableScopeRenderBinding,
    blocked,
    parse_portable_literal,
    portable_scope_canonical_json,
    portable_scope_contract,
    portable_scope_sha256,
    require_mapping,
    validate_catalog_identifier,
    validate_scope_version,
)


def parse_portable_relation_scope(raw: Any) -> PortableRelationScope:
    """Parse one finite mapping into the immutable portable-scope AST."""

    if isinstance(raw, PortableEqualityScope | PortableRangeScope):
        return raw
    mapping = require_mapping(raw, "portable_scope.object")
    _reject_unknown(mapping, {"version", "kind", "column", "value", "lower", "upper"})
    version = validate_scope_version(mapping.get("version", PORTABLE_SCOPE_VERSION))
    column = validate_catalog_identifier(mapping.get("column"))
    kind = mapping.get("kind")
    if kind == "equality":
        _reject_present(mapping, {"lower", "upper"}, blocker="portable_scope.equality.fields")
        if "value" not in mapping:
            blocked("portable_scope.equality.value", "value is required")
        return PortableEqualityScope(
            column=column,
            value=parse_portable_literal(mapping["value"]),
            version=version,
        )
    if kind == "range":
        _reject_present(mapping, {"value"}, blocker="portable_scope.range.fields")
        lower = _parse_bound(mapping["lower"], label="lower") if "lower" in mapping else None
        upper = _parse_bound(mapping["upper"], label="upper") if "upper" in mapping else None
        return PortableRangeScope(
            column=column,
            lower=lower,
            upper=upper,
            version=version,
        )
    blocked("portable_scope.kind", "kind must be equality or range")


def _parse_bound(raw: Any, *, label: str) -> PortableRangeBound:
    mapping = require_mapping(raw, f"portable_scope.range.{label}")
    unknown = sorted(str(key) for key in mapping if str(key) not in {"value", "inclusive"})
    if unknown:
        blocked(f"portable_scope.range.{label}.fields", f"unsupported fields: {', '.join(unknown)}")
    if "value" not in mapping:
        blocked(f"portable_scope.range.{label}.value", "bound value is required")
    inclusive = mapping.get("inclusive", True)
    if not isinstance(inclusive, bool):
        blocked(f"portable_scope.range.{label}.inclusive", "inclusive must be a boolean")
    return PortableRangeBound(parse_portable_literal(mapping["value"]), inclusive)


def _reject_unknown(raw: Mapping[str, Any], allowed: set[str]) -> None:
    unknown = sorted(str(key) for key in raw if str(key) not in allowed)
    if unknown:
        blocked("portable_scope.fields", f"unsupported fields: {', '.join(unknown)}")


def _reject_present(raw: Mapping[str, Any], fields: set[str], *, blocker: str) -> None:
    present = sorted(field for field in fields if field in raw)
    if present:
        blocked(blocker, f"fields are not valid for this kind: {', '.join(present)}")


__all__ = [
    "PORTABLE_IDENTIFIER_UTF8_LIMIT",
    "PORTABLE_SCOPE_VERSION",
    "PortableEqualityScope",
    "PortableLiteral",
    "PortableLiteralKind",
    "PortableRangeBound",
    "PortableRangeScope",
    "PortableRelationScope",
    "PortableScopeContractError",
    "PortableScopeRenderBinding",
    "parse_portable_relation_scope",
    "portable_scope_canonical_json",
    "portable_scope_contract",
    "portable_scope_sha256",
]
