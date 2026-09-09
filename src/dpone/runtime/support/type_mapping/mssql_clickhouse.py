"""Lossless-oriented MSSQL to ClickHouse type mapping."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from typing import Literal, cast

from dpone.runtime.support.temporal_fidelity import (
    NaiveTimestampFidelityPolicy,
    TemporalFidelityPolicy,
    offset_minutes_column_name,
)

BinaryEncoding = Literal["none", "hex", "base64"]
TimeEncoding = Literal["string", "seconds_since_midnight"]


@dataclass(frozen=True, slots=True)
class MssqlClickHouseTypePolicy:
    """MSSQL -> ClickHouse fidelity policy for ambiguous source types."""

    binary_encoding: BinaryEncoding = "none"
    time_encoding: TimeEncoding = "string"
    temporal: TemporalFidelityPolicy = TemporalFidelityPolicy()
    naive_temporal: NaiveTimestampFidelityPolicy = NaiveTimestampFidelityPolicy()

    @classmethod
    def from_config(cls, raw: object | None) -> MssqlClickHouseTypePolicy:
        values = dict(raw or {}) if isinstance(raw, dict) else {}
        binary = str(values.get("binary_encoding", "none")).lower()
        time_encoding = str(values.get("time_encoding", "string")).lower()
        if binary not in {"none", "hex", "base64"}:
            raise ValueError("type_fidelity.binary_encoding must be one of: none, hex, base64")
        if time_encoding not in {"string", "seconds_since_midnight"}:
            raise ValueError("type_fidelity.time_encoding must be one of: string, seconds_since_midnight")
        return cls(
            binary_encoding=cast(BinaryEncoding, binary),
            time_encoding=cast(TimeEncoding, time_encoding),
            temporal=TemporalFidelityPolicy.from_config(values),
            naive_temporal=NaiveTimestampFidelityPolicy.from_config(values),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "binary_encoding": self.binary_encoding,
            "time_encoding": self.time_encoding,
            "temporal": {
                "offset_timestamp": self.temporal.to_dict(),
                "naive_timestamp": self.naive_temporal.to_dict(),
            },
        }

    @property
    def warnings(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys([*self.temporal.warnings, *self.naive_temporal.warnings]))


@dataclass(frozen=True, slots=True)
class MssqlClickHouseTypeDecision:
    """One MSSQL source type mapped to a ClickHouse target type."""

    column: str
    source_type: str
    clickhouse_type: str
    lossless: bool
    reason: str

    @property
    def target_type(self) -> str:
        """Generic target-type alias used by runtime physical-design resolvers."""

        return self.clickhouse_type

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class MssqlClickHouseTypeMapper:
    """Resolve MSSQL metadata types to production ClickHouse target types."""

    def __init__(self, policy: MssqlClickHouseTypePolicy | None = None) -> None:
        self.policy = policy or MssqlClickHouseTypePolicy()

    def resolve_schema(
        self, schema: list[tuple[str, str]] | tuple[tuple[str, str], ...]
    ) -> dict[str, MssqlClickHouseTypeDecision]:
        return {column: self.resolve_column(column, source_type) for column, source_type in schema}

    def resolve_column(self, column: str, source_type: str) -> MssqlClickHouseTypeDecision:
        clickhouse_type, lossless, reason = _resolve_clickhouse_type(source_type, self.policy)
        return MssqlClickHouseTypeDecision(
            column=str(column),
            source_type=str(source_type),
            clickhouse_type=clickhouse_type,
            lossless=lossless,
            reason=reason,
        )


def schema_with_temporal_companion_columns(
    schema: list[tuple[str, str]] | tuple[tuple[str, str], ...],
    policy: MssqlClickHouseTypePolicy,
) -> list[tuple[str, str]]:
    """Return schema augmented with generated offset-minute columns when requested."""
    result: list[tuple[str, str]] = [(str(column), str(dtype)) for column, dtype in schema]
    if not policy.temporal.preserves_offset_column:
        return result
    existing = {column.lower() for column, _ in result}
    for column, dtype in schema:
        normalized_dtype = str(dtype).strip().lower()
        if normalized_dtype.startswith("datetimeoffset"):
            generated = offset_minutes_column_name(str(column))
            if generated.lower() not in existing:
                generated_type = "smallint nullable" if "nullable" in normalized_dtype else "smallint"
                result.append((generated, generated_type))
                existing.add(generated.lower())
    return result


def clickhouse_type_for_mssql(source_type: str) -> str:
    """Return ClickHouse type for an MSSQL source type."""
    return _resolve_clickhouse_type(source_type, MssqlClickHouseTypePolicy())[0]


def _resolve_clickhouse_type(source_type: str, policy: MssqlClickHouseTypePolicy) -> tuple[str, bool, str]:
    normalized, nullable = _normalize_type(source_type)
    base = _strip_size(normalized)
    mapped: str
    lossless = True
    reason: str

    decimal_match = re.match(r"^(decimal|numeric)\((\d+)\s*,\s*(\d+)\)$", normalized)
    if decimal_match:
        precision = min(max(int(decimal_match.group(2)), 1), 76)
        scale = min(max(int(decimal_match.group(3)), 0), precision)
        mapped = f"Decimal({precision},{scale})"
        reason = "preserve MSSQL decimal/numeric precision and scale"
    elif base == "money":
        mapped = "Decimal(19,4)"
        reason = "preserve MSSQL money scale"
    elif base == "smallmoney":
        mapped = "Decimal(10,4)"
        reason = "preserve MSSQL smallmoney scale"
    elif base in {"decimal", "numeric"}:
        mapped = "Decimal(38,9)"
        reason = "generic decimal/numeric metadata without precision uses safe default Decimal(38,9)"
    elif base == "bigint":
        mapped = "Int64"
        reason = "MSSQL bigint maps to ClickHouse Int64"
    elif base in {"int", "integer"}:
        mapped = "Int32"
        reason = "MSSQL int maps to ClickHouse Int32"
    elif base == "smallint":
        mapped = "Int16"
        reason = "MSSQL smallint maps to ClickHouse Int16"
    elif base == "tinyint":
        mapped = "UInt8"
        reason = "MSSQL tinyint maps to ClickHouse UInt8"
    elif base in {"bit", "bool", "boolean"}:
        mapped = "Bool"
        reason = "MSSQL bit maps to ClickHouse Bool"
    elif base == "uniqueidentifier":
        mapped = "UUID"
        reason = "MSSQL uniqueidentifier maps to ClickHouse UUID"
    elif base == "date":
        mapped = "Date"
        lossless = False
        reason = "MSSQL date maps to ClickHouse Date only with an observed 1970-01-01..2149-06-06 range assurance"
    elif normalized.startswith("datetimeoffset"):
        mapped, lossless, reason = _datetimeoffset_mapping(normalized, policy)
    elif normalized.startswith("datetime2"):
        mapped = f"DateTime64({_datetime_scale(normalized)})"
        lossless = False
        reason = "preserve MSSQL datetime2 fractional precision within the governed ClickHouse DateTime64 range"
    elif base == "datetime":
        mapped = "DateTime64(3)"
        lossless = False
        reason = "preserve MSSQL datetime precision within the governed ClickHouse DateTime64 range"
    elif base == "smalldatetime":
        mapped = "DateTime64(0)"
        reason = "MSSQL smalldatetime has minute-scale precision"
    elif _is_time_type(normalized):
        if policy.time_encoding == "seconds_since_midnight":
            mapped = "UInt32"
            reason = "time encoded as seconds since midnight"
        else:
            mapped = "String"
            reason = "time encoded as canonical text"
    elif base in {"float", "double", "double precision"}:
        mapped = "Float64"
        lossless = False
        reason = "MSSQL float is approximate numeric"
    elif base in {"real"}:
        mapped = "Float32"
        lossless = False
        reason = "MSSQL real is approximate numeric"
    elif base in {"binary", "varbinary", "image", "rowversion", "timestamp"}:
        mapped = "String"
        lossless = policy.binary_encoding in {"hex", "base64"}
        reason = (
            f"binary values encoded as {policy.binary_encoding}"
            if lossless
            else "binary values require an explicit hex/base64 transfer codec for byte-exact readability"
        )
    else:
        mapped = "String"
        reason = "string-compatible MSSQL type"

    if nullable and not mapped.startswith("Nullable("):
        mapped = f"Nullable({mapped})"
    return mapped, lossless, reason


def _datetimeoffset_mapping(normalized: str, policy: MssqlClickHouseTypePolicy) -> tuple[str, bool, str]:
    scale = _datetime_scale(normalized)
    mode = policy.temporal.offset_timestamp_mode
    if mode == "utc_instant":
        return (
            f"DateTime64({scale}, 'UTC')",
            False,
            "MSSQL datetimeoffset is normalized to UTC instant; original offset is not preserved",
        )
    if mode == "fixed_timezone":
        timezone = policy.temporal.target_timezone
        return (
            f"DateTime64({scale}, '{timezone}')",
            False,
            f"MSSQL datetimeoffset is converted to configured timezone {timezone}; original offset is not preserved",
        )
    if mode == "preserve_offset":
        return (
            f"DateTime64({scale}, 'UTC')",
            False,
            "MSSQL datetimeoffset instant and offset are preserved only after proving the narrower DateTime64 target range",
        )
    return "String", True, "MSSQL datetimeoffset is preserved as offset timestamp text"


def _normalize_type(source_type: str) -> tuple[str, bool]:
    normalized = str(source_type or "").strip().lower()
    nullable = " nullable" in normalized or normalized.startswith("nullable(")
    normalized = normalized.replace(" nullable", "").strip()
    if normalized.startswith("nullable(") and normalized.endswith(")"):
        normalized = normalized[len("nullable(") : -1].strip()
    return normalized, nullable


def _strip_size(normalized: str) -> str:
    return re.sub(r"\(.*\)$", "", normalized).strip()


def _datetime_scale(normalized: str) -> int:
    match = re.search(r"\((\d+)\)", normalized)
    if not match:
        return 7 if normalized.startswith(("datetime2", "datetimeoffset")) else 6
    return min(max(int(match.group(1)), 0), 9)


def _is_time_type(normalized: str) -> bool:
    return normalized == "time" or normalized.startswith("time(")


__all__ = [
    "MssqlClickHouseTypeDecision",
    "MssqlClickHouseTypeMapper",
    "MssqlClickHouseTypePolicy",
    "clickhouse_type_for_mssql",
    "schema_with_temporal_companion_columns",
]
