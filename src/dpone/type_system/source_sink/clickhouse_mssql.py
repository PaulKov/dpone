"""Canonical ClickHouse -> SQL Server type classification.

Single source of truth for the ``clickhouse -> mssql`` route, shared by:

- the runtime DDL mapper (:mod:`dpone.runtime.support.mssql_types`), which
  raises a configuration error for unsupported types;
- the diagnostic type-matrix profile
  (:mod:`dpone.type_system.source_sink.profiles`), which explains decisions in
  ``dpone schema type-matrix`` and the docs.

Policy (documented in ``docs/type-mapping-matrix.md``):

- every ClickHouse type is either **mapped** (deterministic, width/precision
  exact) or **explicitly unsupported** with an actionable source-side
  workaround — there is no silent fallback for the ClickHouse dialect;
- mapped types round-trip through
  :mod:`dpone.readiness.schema_type_compatibility` so repeated chunks never
  report fake breaking schema changes.

The classifier is pure (stdlib-only) and inspects the case-sensitive
ClickHouse spelling from ``system.columns``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

CLICKHOUSE_MSSQL_PROFILE = "clickhouse_to_mssql_landing_v1"

_WRAPPER_PATTERN = re.compile(r"\A(?:Nullable|LowCardinality)\((?P<inner>.+)\)\Z", re.IGNORECASE)
_SIMPLE_AGGREGATE_PATTERN = re.compile(r"\ASimpleAggregateFunction\(\s*[^,]+,\s*(?P<inner>.+)\)\Z", re.IGNORECASE)
_DECIMAL_PATTERN = re.compile(r"\ADecimal\(\s*(?P<precision>\d+)\s*,\s*(?P<scale>\d+)\s*\)\Z", re.IGNORECASE)
_DECIMAL_SHORT_PATTERN = re.compile(r"\ADecimal(?P<bits>32|64|128|256)\(\s*(?P<scale>\d+)\s*\)\Z", re.IGNORECASE)
_FIXED_STRING_PATTERN = re.compile(r"\AFixedString\(\s*(?P<length>\d+)\s*\)\Z", re.IGNORECASE)
_DATETIME64_PATTERN = re.compile(r"\ADateTime64(?:\(\s*(?P<precision>\d+)\s*(?:,\s*'[^']*'\s*)?\))?\Z", re.IGNORECASE)
_DATETIME_PATTERN = re.compile(r"\ADateTime(?:32)?(?:\(\s*'[^']*'\s*\))?\Z", re.IGNORECASE)
_ENUM_PATTERN = re.compile(r"\AEnum(?:8|16)?\(.*\)\Z", re.IGNORECASE)

_DECIMAL_SHORT_PRECISION = {"32": 9, "64": 18, "128": 38, "256": 76}
_MSSQL_MAX_DECIMAL_PRECISION = 38

_EXACT_SCALARS: dict[str, tuple[str, bool, str | None]] = {
    # spelling: (mssql type, lossless, note)
    "Int8": ("smallint", True, "MSSQL tinyint is unsigned; smallint keeps the sign"),
    "Int16": ("smallint", True, None),
    "Int32": ("int", True, None),
    "Int64": ("bigint", True, None),
    "UInt8": ("tinyint", True, None),
    "UInt16": ("int", True, None),
    "UInt32": ("bigint", True, None),
    "UInt64": ("decimal(20,0)", True, "bigint is signed 63-bit; decimal(20,0) holds the full unsigned range"),
    "Float32": ("real", True, None),
    "Float64": ("float", True, None),
    "BFloat16": ("real", True, "widening 16-bit float into IEEE real"),
    "String": ("nvarchar(max)", True, None),
    "Date": ("date", True, None),
    "Date32": ("date", True, None),
    "Bool": ("bit", True, None),
    "UUID": ("uniqueidentifier", True, None),
    "IPv4": ("varchar(15)", True, "canonical dotted-quad text: portable, human-readable, bcp/diff-safe"),
    "IPv6": ("varchar(45)", True, "canonical RFC 5952 text: portable, human-readable, bcp/diff-safe"),
}

_JSON_WORKAROUND = "toJSONString(col) AS col on the source"
_GEO_REASON = "geo types have no MSSQL scalar equivalent in this route"
_GEO_WORKAROUND = "toJSONString(col) or wkt(col) on the source"
_HUGE_INT_REASON = "exceeds MSSQL decimal(38,0) integer range"
_HUGE_INT_WORKAROUND = "CAST(col AS Decimal(38,0)) or toString(col) on the source"

# Case-sensitive ClickHouse spellings; entries match the exact name and the
# parameterized form `Name(...)`. `Interval` matches every Interval* unit.
_UNSUPPORTED: tuple[tuple[str, str, str], ...] = (
    ("Int128", _HUGE_INT_REASON, _HUGE_INT_WORKAROUND),
    ("Int256", _HUGE_INT_REASON, _HUGE_INT_WORKAROUND),
    ("UInt128", _HUGE_INT_REASON, _HUGE_INT_WORKAROUND),
    ("UInt256", _HUGE_INT_REASON, _HUGE_INT_WORKAROUND),
    ("Array", "MSSQL has no array type and extraction emits Python literals", _JSON_WORKAROUND),
    ("Tuple", "MSSQL has no tuple type and extraction emits Python literals", _JSON_WORKAROUND),
    ("Map", "MSSQL has no map type and extraction emits Python literals", _JSON_WORKAROUND),
    ("Nested", "nested structures need normalization or JSON landing", _JSON_WORKAROUND),
    ("JSON", "experimental JSON object type has no deterministic text form here", _JSON_WORKAROUND),
    ("Object", "experimental Object type has no deterministic text form here", _JSON_WORKAROUND),
    ("Point", _GEO_REASON, _GEO_WORKAROUND),
    ("Ring", _GEO_REASON, _GEO_WORKAROUND),
    ("Polygon", _GEO_REASON, _GEO_WORKAROUND),
    ("MultiPolygon", _GEO_REASON, _GEO_WORKAROUND),
    ("LineString", _GEO_REASON, _GEO_WORKAROUND),
    ("MultiLineString", _GEO_REASON, _GEO_WORKAROUND),
    (
        "AggregateFunction",
        "aggregate states are engine-internal binary payloads",
        "finalizeAggregation(col) on the source",
    ),
    (
        "Variant",
        "experimental Variant type is not deterministic across chunks",
        "toString(col) or toJSONString(col) on the source",
    ),
    (
        "Dynamic",
        "experimental Dynamic type is not deterministic across chunks",
        "toString(col) or toJSONString(col) on the source",
    ),
    ("Time64", "ClickHouse Time64 has no exact MSSQL mapping in this route", "toString(col) on the source"),
    ("Time", "ClickHouse Time has no exact MSSQL mapping in this route", "toString(col) on the source"),
    ("Nothing", "Nothing carries no values", "drop the column from the source projection"),
)

_UNSUPPORTED_NAME_PREFIXES = ("Interval",)
_INTERVAL_REASON = "interval values are not storable table columns"
_INTERVAL_WORKAROUND = "toString(col) on the source"


@dataclass(frozen=True, slots=True)
class ClickHouseMSSQLTypeDecision:
    """One deterministic ClickHouse -> MSSQL type decision."""

    source_type: str
    mssql_type: str | None
    lossless: bool = True
    note: str | None = None
    reason: str | None = None
    workaround: str | None = None

    @property
    def supported(self) -> bool:
        return self.mssql_type is not None


def classify_clickhouse_type(dtype: str) -> ClickHouseMSSQLTypeDecision | None:
    """Classify one ClickHouse type; ``None`` when the spelling is not ClickHouse."""

    original = str(dtype).strip()
    unwrapped = _unwrap(original)
    decision = _classify_base(unwrapped)
    if decision is None:
        return None
    return ClickHouseMSSQLTypeDecision(
        source_type=original,
        mssql_type=decision.mssql_type,
        lossless=decision.lossless,
        note=decision.note,
        reason=decision.reason,
        workaround=decision.workaround,
    )


def _classify_base(base: str) -> ClickHouseMSSQLTypeDecision | None:
    if base in _EXACT_SCALARS:
        mssql_type, lossless, note = _EXACT_SCALARS[base]
        return ClickHouseMSSQLTypeDecision(base, mssql_type, lossless=lossless, note=note)
    for spelling, reason, workaround in _UNSUPPORTED:
        if base == spelling or base.startswith(f"{spelling}("):
            return ClickHouseMSSQLTypeDecision(base, None, lossless=False, reason=reason, workaround=workaround)
    for prefix in _UNSUPPORTED_NAME_PREFIXES:
        if base.startswith(prefix):
            return ClickHouseMSSQLTypeDecision(
                base, None, lossless=False, reason=_INTERVAL_REASON, workaround=_INTERVAL_WORKAROUND
            )
    return _classify_parameterized(base)


def _classify_parameterized(base: str) -> ClickHouseMSSQLTypeDecision | None:
    if match := _DECIMAL_PATTERN.match(base):
        return _decimal_decision(base, int(match.group("precision")), int(match.group("scale")))
    if match := _DECIMAL_SHORT_PATTERN.match(base):
        return _decimal_decision(base, _DECIMAL_SHORT_PRECISION[match.group("bits")], int(match.group("scale")))
    if match := _FIXED_STRING_PATTERN.match(base):
        return ClickHouseMSSQLTypeDecision(
            base,
            f"nvarchar({match.group('length')})",
            note="text semantics; base64-encode binary FixedString payloads on the source",
        )
    if match := _DATETIME64_PATTERN.match(base):
        precision = int(match.group("precision") or 3)
        if precision <= 7:
            return ClickHouseMSSQLTypeDecision(base, f"datetime2({precision})", note=_TZ_NOTE)
        return ClickHouseMSSQLTypeDecision(
            base,
            "datetime2(7)",
            lossless=False,
            note=f"sub-100ns precision {precision} truncated to datetime2(7); {_TZ_NOTE}",
        )
    if _DATETIME_PATTERN.match(base):
        return ClickHouseMSSQLTypeDecision(base, "datetime2(0)", note=_TZ_NOTE)
    if _ENUM_PATTERN.match(base):
        return ClickHouseMSSQLTypeDecision(base, "nvarchar(max)", note="enum values land as their names")
    return None


def _decimal_decision(base: str, precision: int, scale: int) -> ClickHouseMSSQLTypeDecision:
    if precision <= _MSSQL_MAX_DECIMAL_PRECISION:
        return ClickHouseMSSQLTypeDecision(base, f"decimal({precision},{scale})")
    return ClickHouseMSSQLTypeDecision(
        base,
        None,
        lossless=False,
        reason=f"precision {precision} exceeds MSSQL decimal({_MSSQL_MAX_DECIMAL_PRECISION})",
        workaround=f"CAST(col AS Decimal({_MSSQL_MAX_DECIMAL_PRECISION}, {min(scale, _MSSQL_MAX_DECIMAL_PRECISION)})) or toString(col) on the source",
    )


def _unwrap(dtype: str) -> str:
    """Strip Nullable/LowCardinality/SimpleAggregateFunction wrappers recursively."""

    value = dtype
    while True:
        wrapper = _WRAPPER_PATTERN.match(value)
        if wrapper is not None:
            value = wrapper.group("inner").strip()
            continue
        aggregate = _SIMPLE_AGGREGATE_PATTERN.match(value)
        if aggregate is not None:
            value = aggregate.group("inner").strip()
            continue
        return value


_TZ_NOTE = "timezone metadata is not preserved; values land as rendered wall-clock text"


@dataclass(frozen=True, slots=True)
class ClickHouseMssqlTypeDecision:
    """Diagnostic decision shape used by certification payloads and matrices."""

    source_type: str
    target_type: str
    native_transport: str
    compatible: bool = True
    lossless: bool = True
    requires_explicit_contract: bool = False
    warning: str | None = None


class ClickHouseMssqlTypeMapper:
    """Diagnostic adapter over :func:`classify_clickhouse_type`.

    Runtime DDL raises for unsupported types (see
    ``dpone.runtime.support.mssql_types``); diagnostics instead report them as
    ``requires_explicit_contract`` entries with the same reason/workaround so
    certification payloads and ``dpone schema type-matrix`` stay explainable.
    """

    def resolve(self, source_type: str) -> ClickHouseMssqlTypeDecision:
        decision = classify_clickhouse_type(source_type)
        if decision is None or not decision.supported:
            reason = decision.reason if decision else "Unknown ClickHouse type"
            workaround = decision.workaround if decision else "declare an explicit schema_contract"
            return ClickHouseMssqlTypeDecision(
                source_type=str(source_type),
                target_type="nvarchar(max)",
                native_transport="BulkTextCodec text/json",
                compatible=False,
                lossless=False,
                requires_explicit_contract=True,
                warning=f"{reason}. Workaround: {workaround}.",
            )
        return ClickHouseMssqlTypeDecision(
            source_type=decision.source_type,
            target_type=decision.mssql_type or "nvarchar(max)",
            native_transport=clickhouse_mssql_transport(decision.mssql_type or ""),
            compatible=decision.lossless,
            lossless=decision.lossless,
            requires_explicit_contract=not decision.lossless,
            warning=decision.note,
        )


def clickhouse_mssql_transport(mssql_type: str) -> str:
    """Stable transport label for one mapped MSSQL DDL type."""

    if mssql_type.startswith(("decimal", "numeric")):
        return "decimal text"
    if mssql_type.startswith(("smallint", "tinyint", "int", "bigint")):
        return "integer text"
    if mssql_type.startswith(("real", "float")):
        return "float text"
    if mssql_type.startswith("datetime2"):
        return "timestamp text"
    if mssql_type == "date":
        return "ISO date text"
    if mssql_type == "bit":
        return "0/1 text"
    if mssql_type == "uniqueidentifier":
        return "uuid text"
    if mssql_type.startswith("varchar(") and mssql_type in {"varchar(15)", "varchar(45)"}:
        return "IP text"
    return "BulkTextCodec text"


def is_probable_clickhouse_type(source_type: str) -> bool:
    """Return True when a type string looks like ClickHouse metadata."""

    return classify_clickhouse_type(source_type) is not None


def normalize_clickhouse_type(source_type: str) -> str:
    """Lowercased base spelling with Nullable/LowCardinality wrappers removed."""

    normalized = " ".join(str(source_type).strip().split())
    normalized = re.sub(r"\s+nullable$", "", normalized, flags=re.IGNORECASE).strip()
    return _unwrap(normalized).lower()


__all__ = [
    "CLICKHOUSE_MSSQL_PROFILE",
    "ClickHouseMSSQLTypeDecision",
    "ClickHouseMssqlTypeDecision",
    "ClickHouseMssqlTypeMapper",
    "classify_clickhouse_type",
    "clickhouse_mssql_transport",
    "is_probable_clickhouse_type",
    "normalize_clickhouse_type",
]
