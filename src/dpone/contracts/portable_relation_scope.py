"""Public identity façade for the portable relation-scope contract.

The contract is deliberately layered: literal validation, render-neutral AST,
and closed-object parsing live in focused modules. This stable façade keeps
callers independent of that internal decomposition and owns only canonical
identity helpers and ``LoadConfig`` resolution.
"""

from __future__ import annotations

from typing import Any

from dpone.contracts.portable_scope_parser import (
    PORTABLE_IDENTIFIER_UTF8_LIMIT,
    PORTABLE_SCOPE_VERSION,
    PortableEqualityScope,
    PortableLiteral,
    PortableRangeBound,
    PortableRangeScope,
    PortableRelationScope,
    PortableScopeContractError,
    parse_portable_relation_scope,
    portable_scope_canonical_json,
    portable_scope_contract,
    portable_scope_sha256,
)


def resolve_portable_relation_scope(load_config: Any) -> PortableRelationScope | None:
    """Resolve the typed scope from ``LoadConfig`` without consulting SQL."""

    raw = getattr(load_config, "portable_scope", None)
    return None if raw is None else parse_portable_relation_scope(raw)


__all__ = [
    "PORTABLE_IDENTIFIER_UTF8_LIMIT",
    "PORTABLE_SCOPE_VERSION",
    "PortableEqualityScope",
    "PortableLiteral",
    "PortableRangeBound",
    "PortableRangeScope",
    "PortableRelationScope",
    "PortableScopeContractError",
    "parse_portable_relation_scope",
    "portable_scope_canonical_json",
    "portable_scope_contract",
    "portable_scope_sha256",
    "resolve_portable_relation_scope",
]
