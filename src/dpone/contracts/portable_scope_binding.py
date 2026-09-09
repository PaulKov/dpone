"""Catalog binding and cross-database comparison proof for portable scopes."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, NoReturn

from dpone.contracts.mssql_type_contract import normalize_mssql_physical_type
from dpone.contracts.portable_scope_binding_shapes import (
    decimal_fits,
    fractional_precision,
    mssql_binary_collation,
    mssql_decimal_shape,
    mssql_integral_range,
    mssql_timestamp_precision,
    postgres_binary_collation,
    postgres_decimal_shape,
    postgres_integral_range,
    postgres_text_limit,
    postgres_timestamp_precision,
)
from dpone.contracts.portable_scope_model import (
    PortableEqualityScope,
    PortableLiteral,
    PortableRangeScope,
    PortableRelationScope,
    portable_scope_sha256,
)

PORTABLE_SCOPE_BINDING_VERSION = 1
PORTABLE_SCOPE_BINDING_OPTION = "__dpone_portable_scope_binding"

_MSSQL_NVARCHAR = re.compile(r"^nvarchar\((max|\d+)\)$")


class PortableScopeBindingError(ValueError):
    """Stable pre-state error for an unprovable source/target scope binding."""

    code = "DPONE_PORTABLE_SCOPE_BINDING_BLOCKED"

    def __init__(self, blocker: str, detail: str) -> None:
        self.blocker = blocker
        super().__init__(f"{self.code}:{blocker}: {detail}")


@dataclass(frozen=True, slots=True)
class PortableScopeColumnContract:
    """Exact source and target catalog identities selected for one AST column."""

    source_name: str
    source_type: str
    source_collation: str | None
    target_name: str
    target_type: str
    target_collation: str | None


@dataclass(frozen=True, slots=True)
class PortableScopeBinding:
    """Immutable proof that one AST has equivalent PG and MSSQL semantics."""

    ast_sha256: str
    column: str
    literal_type: str
    source_type: str
    source_collation: str | None
    target_type: str
    target_collation: str | None
    comparison_contract: str
    version: int = PORTABLE_SCOPE_BINDING_VERSION

    def __post_init__(self) -> None:
        try:
            digest = bytes.fromhex(self.ast_sha256)
        except ValueError as exc:
            raise PortableScopeBindingError("portable_scope.binding.ast_digest", "AST digest is invalid") from exc
        if len(digest) != 32:
            raise PortableScopeBindingError("portable_scope.binding.ast_digest", "AST digest is invalid")
        if any(not str(value) for value in (self.column, self.literal_type, self.source_type, self.target_type)):
            raise PortableScopeBindingError("portable_scope.binding.incomplete", "binding is incomplete")

    def to_contract(self) -> dict[str, object]:
        """Return the canonical, SQL-free binding identity."""

        return asdict(self)

    @property
    def sha256(self) -> bytes:
        encoded = json.dumps(
            self.to_contract(),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        return hashlib.sha256(encoded).digest()

    def require_scope(self, scope: PortableRelationScope) -> None:
        """Reject stale runtime binding reuse for a different AST."""

        if portable_scope_sha256(scope).hex() != self.ast_sha256 or scope.column != self.column:
            raise PortableScopeBindingError(
                "portable_scope.binding.ast_mismatch",
                "runtime binding does not belong to this portable scope",
            )


def bind_portable_scope(
    scope: PortableRelationScope,
    columns: PortableScopeColumnContract,
) -> PortableScopeBinding:
    """Prove exact value and comparison representability on PostgreSQL/MSSQL."""

    if columns.source_name != scope.column:
        _blocked("portable_scope.binding.source_identifier", "source identifier is not an exact match")
    if columns.target_name != scope.column:
        _blocked(
            "portable_scope.binding.renamed_target",
            "portable scopes do not follow a renamed target column",
        )
    source_type = _postgres_type(columns.source_type)
    try:
        target_type = normalize_mssql_physical_type(columns.target_type)
    except ValueError as exc:
        raise PortableScopeBindingError(
            "portable_scope.binding.target_type", "target physical type is unsupported"
        ) from exc
    literals = _scope_literals(scope)
    literal_type = literals[0].kind
    comparison = _comparison_contract(
        scope,
        literals,
        source_type=source_type,
        source_collation=columns.source_collation,
        target_type=target_type,
        target_collation=columns.target_collation,
    )
    return PortableScopeBinding(
        ast_sha256=portable_scope_sha256(scope).hex(),
        column=scope.column,
        literal_type=literal_type,
        source_type=source_type,
        source_collation=columns.source_collation,
        target_type=target_type,
        target_collation=columns.target_collation,
        comparison_contract=comparison,
    )


def require_exact_portable_identifier(
    requested: str,
    candidates: tuple[str, ...],
    *,
    side: str,
) -> str:
    """Bind exact catalog spelling and reject case-fold ambiguity or fallback."""

    folded = tuple(candidate for candidate in candidates if candidate.casefold() == requested.casefold())
    if len(folded) > 1:
        _blocked(
            f"portable_scope.binding.{side}_identifier_ambiguous",
            "catalog contains more than one case-equivalent identifier",
        )
    if requested in candidates:
        return requested
    if folded:
        _blocked(
            f"portable_scope.binding.{side}_identifier_case_mismatch",
            "portable scope spelling must exactly match the catalog",
        )
    _blocked(
        f"portable_scope.binding.{side}_identifier_unknown",
        "portable scope column is absent from the catalog projection",
    )


def require_portable_scope_binding(load_config: Any, scope: PortableRelationScope) -> PortableScopeBinding:
    """Load and verify the pre-admission binding frozen in runtime options."""

    options = getattr(load_config, "options", {}) or {}
    binding = options.get(PORTABLE_SCOPE_BINDING_OPTION) if isinstance(options, dict) else None
    if not isinstance(binding, PortableScopeBinding):
        _blocked("portable_scope.binding.required", "pre-admission catalog binding is required")
    binding.require_scope(scope)
    return binding


def _comparison_contract(
    scope: PortableRelationScope,
    literals: tuple[PortableLiteral, ...],
    *,
    source_type: str,
    source_collation: str | None,
    target_type: str,
    target_collation: str | None,
) -> str:
    kind = literals[0].kind
    if kind == "integer":
        _validate_integer_values(literals, source_type=source_type, target_type=target_type)
        return "ordered_exact_integer_v1" if isinstance(scope, PortableRangeScope) else "exact_integer_v1"
    if kind == "decimal":
        _validate_decimal_values(literals, source_type=source_type, target_type=target_type)
        return "ordered_exact_decimal_v1" if isinstance(scope, PortableRangeScope) else "exact_decimal_v1"
    if kind == "boolean":
        if isinstance(scope, PortableRangeScope):  # guarded by the parser, retained for direct AST construction.
            _blocked("portable_scope.binding.boolean_range", "boolean ranges are unsupported")
        if source_type not in {"boolean", "bool"} or target_type != "bit":
            _type_mismatch(kind, source_type, target_type)
        return "exact_boolean_v1"
    if kind == "date":
        if source_type != "date" or target_type != "date":
            _type_mismatch(kind, source_type, target_type)
        return "ordered_exact_date_v1" if isinstance(scope, PortableRangeScope) else "exact_date_v1"
    if kind == "timestamp":
        _validate_timestamp_values(literals, source_type=source_type, target_type=target_type, timezone=False)
        return "ordered_exact_timestamp_v1" if isinstance(scope, PortableRangeScope) else "exact_timestamp_v1"
    if kind == "timestamptz":
        _validate_timestamp_values(literals, source_type=source_type, target_type=target_type, timezone=True)
        return "ordered_exact_utc_instant_v1" if isinstance(scope, PortableRangeScope) else "exact_utc_instant_v1"
    if kind == "uuid":
        if source_type != "uuid" or target_type != "uniqueidentifier":
            _type_mismatch(kind, source_type, target_type)
        return "ordered_canonical_uuid_v1" if isinstance(scope, PortableRangeScope) else "exact_uuid_v1"
    if kind == "text":
        if not isinstance(scope, PortableEqualityScope):
            _blocked("portable_scope.binding.text_range", "text ranges are never portable")
        _validate_text_equality(
            literals[0],
            source_type=source_type,
            source_collation=source_collation,
            target_type=target_type,
            target_collation=target_collation,
        )
        return "binary_unicode_scalar_equality_v1"
    _blocked("portable_scope.binding.literal_type", f"unsupported literal type {kind!r}")


def _validate_integer_values(
    literals: tuple[PortableLiteral, ...],
    *,
    source_type: str,
    target_type: str,
) -> None:
    source_range = postgres_integral_range(source_type)
    target_range = mssql_integral_range(target_type)
    if source_range is None or target_range is None:
        _type_mismatch("integer", source_type, target_type)
    minimum = max(source_range[0], target_range[0])
    maximum = min(source_range[1], target_range[1])
    for literal in literals:
        value = literal.value
        assert isinstance(value, int) and not isinstance(value, bool)
        if not minimum <= value <= maximum:
            _blocked(
                "portable_scope.binding.integer_range",
                "integer literal is not exactly representable by both physical types",
            )


def _validate_decimal_values(
    literals: tuple[PortableLiteral, ...],
    *,
    source_type: str,
    target_type: str,
) -> None:
    source_shape = postgres_decimal_shape(source_type)
    target_shape = mssql_decimal_shape(target_type)
    if source_shape is None or target_shape is None:
        _type_mismatch("decimal", source_type, target_type)
    for literal in literals:
        value = literal.value
        assert isinstance(value, Decimal)
        if not decimal_fits(value, source_shape) or not decimal_fits(value, target_shape):
            _blocked(
                "portable_scope.binding.decimal_precision",
                "decimal literal is not exactly representable at both precision/scale boundaries",
            )


def _validate_timestamp_values(
    literals: tuple[PortableLiteral, ...],
    *,
    source_type: str,
    target_type: str,
    timezone: bool,
) -> None:
    source_precision = postgres_timestamp_precision(source_type, timezone=timezone)
    target_precision = mssql_timestamp_precision(target_type, timezone=timezone)
    if source_precision is None or target_precision is None:
        _type_mismatch("timestamptz" if timezone else "timestamp", source_type, target_type)
    supported_precision = min(source_precision, target_precision)
    for literal in literals:
        value = literal.value
        assert isinstance(value, datetime)
        if fractional_precision(value) > supported_precision:
            _blocked(
                "portable_scope.binding.timestamp_precision",
                "timestamp literal would be rounded by one endpoint",
            )


def _validate_text_equality(
    literal: PortableLiteral,
    *,
    source_type: str,
    source_collation: str | None,
    target_type: str,
    target_collation: str | None,
) -> None:
    source_limit = postgres_text_limit(source_type)
    target_match = _MSSQL_NVARCHAR.fullmatch(target_type)
    if source_limit is False or target_match is None:
        _type_mismatch("text", source_type, target_type)
    if not postgres_binary_collation(source_collation) or not mssql_binary_collation(target_collation):
        _blocked(
            "portable_scope.binding.text_collation",
            "text equality requires catalog-certified PostgreSQL binary and MSSQL BIN2 collations",
        )
    value = literal.value
    assert isinstance(value, str)
    if isinstance(source_limit, int) and len(value) > source_limit:
        _blocked("portable_scope.binding.text_length", "text literal exceeds the PostgreSQL column limit")
    raw_target_limit = target_match.group(1)
    if raw_target_limit != "max" and len(value.encode("utf-16le")) // 2 > int(raw_target_limit):
        _blocked("portable_scope.binding.text_length", "text literal exceeds the MSSQL UTF-16 limit")


def _postgres_type(value: str) -> str:
    normalized = " ".join(str(value or "").strip().lower().split())
    if not normalized:
        _blocked("portable_scope.binding.source_type", "source logical type is unavailable")
    return normalized


def _scope_literals(scope: PortableRelationScope) -> tuple[PortableLiteral, ...]:
    if isinstance(scope, PortableEqualityScope):
        return (scope.value,)
    literals = tuple(bound.value for bound in (scope.lower, scope.upper) if bound is not None)
    if not literals or len({literal.kind for literal in literals}) != 1:
        _blocked("portable_scope.binding.range_literals", "range literals are incomplete or inconsistent")
    return literals


def _type_mismatch(literal_type: str, source_type: str, target_type: str) -> NoReturn:
    _blocked(
        "portable_scope.binding.type_mismatch",
        f"{literal_type} comparison is not equivalent for source={source_type!r}, target={target_type!r}",
    )


def _blocked(blocker: str, detail: str) -> NoReturn:
    raise PortableScopeBindingError(blocker, detail)


__all__ = [
    "PORTABLE_SCOPE_BINDING_OPTION",
    "PORTABLE_SCOPE_BINDING_VERSION",
    "PortableScopeBinding",
    "PortableScopeBindingError",
    "PortableScopeColumnContract",
    "bind_portable_scope",
    "require_exact_portable_identifier",
    "require_portable_scope_binding",
]
