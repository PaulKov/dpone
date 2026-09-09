"""MSSQL -> ClickHouse type mapping profile for planning and documentation."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from typing import Literal

BinaryEncoding = Literal["none", "hex", "base64"]
TimeEncoding = Literal["string", "seconds_since_midnight"]
OffsetTimestampMode = Literal["utc_instant", "fixed_timezone", "preserve_offset", "preserve_text"]
NaiveTimestampTransferEncoding = Literal["auto", "text", "epoch"]


@dataclass(frozen=True, slots=True)
class MssqlClickHouseMatrixPolicy:
    """MSSQL -> ClickHouse fidelity knobs needed for type-matrix planning."""

    binary_encoding: BinaryEncoding = "none"
    time_encoding: TimeEncoding = "string"
    offset_timestamp_mode: OffsetTimestampMode = "utc_instant"
    offset_timestamp_timezone: str = "UTC"
    naive_timestamp_transfer_encoding: NaiveTimestampTransferEncoding = "auto"

    @classmethod
    def from_config(cls, raw: object | None) -> MssqlClickHouseMatrixPolicy:
        values = dict(raw or {}) if isinstance(raw, dict) else {}
        temporal = values.get("temporal") if isinstance(values.get("temporal"), dict) else {}
        offset = temporal.get("offset_timestamp") if isinstance(temporal, dict) else {}
        naive = temporal.get("naive_timestamp") if isinstance(temporal, dict) else {}
        offset = offset if isinstance(offset, dict) else {}
        naive = naive if isinstance(naive, dict) else {}

        binary = _literal(
            str(values.get("binary_encoding", "none")).lower(),
            {"none", "hex", "base64"},
            "type_fidelity.binary_encoding",
        )
        time_encoding = _literal(
            str(values.get("time_encoding", "string")).lower(),
            {"string", "seconds_since_midnight"},
            "type_fidelity.time_encoding",
        )
        offset_mode = _literal(
            str(offset.get("mode", offset.get("offset_timestamp_mode", "utc_instant"))).lower(),
            {"utc_instant", "fixed_timezone", "preserve_offset", "preserve_text"},
            "type_fidelity.temporal.offset_timestamp.mode",
        )
        naive_transfer = _literal(
            str(naive.get("transfer_encoding", "auto")).lower(),
            {"auto", "text", "epoch"},
            "type_fidelity.temporal.naive_timestamp.transfer_encoding",
        )
        return cls(
            binary_encoding=binary,  # type: ignore[arg-type]
            time_encoding=time_encoding,  # type: ignore[arg-type]
            offset_timestamp_mode=offset_mode,  # type: ignore[arg-type]
            offset_timestamp_timezone=str(offset.get("timezone", offset.get("target_timezone", "UTC"))),
            naive_timestamp_transfer_encoding=naive_transfer,  # type: ignore[arg-type]
        )

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class MssqlClickHouseMatrixDecision:
    """One explainable MSSQL -> ClickHouse type-matrix decision."""

    source_type: str
    target_type: str
    native_transport: str
    schema_evolution_compatible: bool = True
    lossless: bool = True
    requires_explicit_contract: bool = False
    reason: str = ""

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class MssqlClickHouseMatrixMapper:
    """Resolve MSSQL source types for source -> sink matrix UX."""

    def __init__(self, policy: MssqlClickHouseMatrixPolicy | None = None) -> None:
        self.policy = policy or MssqlClickHouseMatrixPolicy()

    def resolve(self, source_type: str) -> MssqlClickHouseMatrixDecision:
        target_type, lossless, reason = _target_type(source_type, self.policy)
        requires_contract = _requires_explicit_contract(source_type)
        if requires_contract:
            lossless = False
            reason = "vendor-specific MSSQL type requires explicit schema_contract or physical override"
        return MssqlClickHouseMatrixDecision(
            source_type=source_type,
            target_type=target_type,
            native_transport=_native_transport(source_type),
            lossless=lossless,
            reason=reason,
            requires_explicit_contract=requires_contract,
        )


def _target_type(source_type: str, policy: MssqlClickHouseMatrixPolicy) -> tuple[str, bool, str]:
    normalized, nullable = _normalize(source_type)
    base = _strip_size(normalized)
    lossless = True

    if match := re.match(r"^(decimal|numeric)\((\d+)\s*,\s*(\d+)\)$", normalized):
        precision = min(max(int(match.group(2)), 1), 76)
        scale = min(max(int(match.group(3)), 0), precision)
        mapped = f"Decimal({precision},{scale})"
        reason = "preserve MSSQL decimal/numeric precision and scale"
    elif base == "money":
        mapped, reason = "Decimal(19,4)", "preserve MSSQL money scale"
    elif base == "smallmoney":
        mapped, reason = "Decimal(10,4)", "preserve MSSQL smallmoney scale"
    elif base in {"decimal", "numeric"}:
        mapped, reason = "Decimal(38,9)", "safe default when precision/scale metadata is absent"
    elif base == "bigint":
        mapped, reason = "Int64", "MSSQL bigint maps to ClickHouse Int64"
    elif base in {"int", "integer"}:
        mapped, reason = "Int32", "MSSQL int maps to ClickHouse Int32"
    elif base == "smallint":
        mapped, reason = "Int16", "MSSQL smallint maps to ClickHouse Int16"
    elif base == "tinyint":
        mapped, reason = "UInt8", "MSSQL tinyint maps to ClickHouse UInt8"
    elif base in {"bit", "bool", "boolean"}:
        mapped, reason = "Bool", "MSSQL bit maps to ClickHouse Bool"
    elif base == "uniqueidentifier":
        mapped, reason = "UUID", "MSSQL uniqueidentifier maps to ClickHouse UUID"
    elif base == "date":
        mapped, reason = "Date", "MSSQL date maps to ClickHouse Date"
    elif normalized.startswith("datetimeoffset"):
        mapped, lossless, reason = _datetimeoffset_type(normalized, policy)
    elif normalized.startswith("datetime2"):
        mapped, reason = f"DateTime64({_datetime_scale(normalized)})", "preserve datetime2 fractional precision"
    elif base == "datetime":
        mapped, reason = "DateTime64(3)", "MSSQL datetime has millisecond-scale precision"
    elif base == "smalldatetime":
        mapped, reason = "DateTime64(0)", "MSSQL smalldatetime has minute-scale precision"
    elif normalized == "time" or normalized.startswith("time("):
        mapped = "UInt32" if policy.time_encoding == "seconds_since_midnight" else "String"
        reason = f"time encoded as {policy.time_encoding}"
    elif base in {"float", "double", "double precision"}:
        mapped, lossless, reason = "Float64", False, "MSSQL float is approximate numeric"
    elif base == "real":
        mapped, lossless, reason = "Float32", False, "MSSQL real is approximate numeric"
    elif base in {"binary", "varbinary", "image", "rowversion", "timestamp"}:
        mapped = "String"
        lossless = policy.binary_encoding in {"hex", "base64"}
        reason = f"binary values encoded as {policy.binary_encoding}"
    else:
        mapped, reason = "String", "string-compatible MSSQL type"

    return (f"Nullable({mapped})" if nullable and not mapped.startswith("Nullable(") else mapped), lossless, reason


def _requires_explicit_contract(source_type: str) -> bool:
    normalized, _nullable = _normalize(source_type)
    base = _strip_size(normalized)
    return base in {"sql_variant", "hierarchyid", "geometry", "geography", "xml"} or "user-defined" in normalized


def _datetimeoffset_type(normalized: str, policy: MssqlClickHouseMatrixPolicy) -> tuple[str, bool, str]:
    scale = _datetime_scale(normalized)
    if policy.offset_timestamp_mode == "utc_instant":
        return f"DateTime64({scale}, 'UTC')", False, "normalize datetimeoffset to UTC instant"
    if policy.offset_timestamp_mode == "fixed_timezone":
        timezone = policy.offset_timestamp_timezone
        return f"DateTime64({scale}, '{timezone}')", False, f"convert datetimeoffset to {timezone}"
    if policy.offset_timestamp_mode == "preserve_offset":
        return f"DateTime64({scale}, 'UTC')", True, "store UTC instant plus offset-minute companion column"
    return "String", True, "preserve datetimeoffset as raw text"


def _native_transport(source_type: str) -> str:
    normalized = source_type.lower()
    if any(token in normalized for token in ("datetime", "smalldatetime")):
        return "epoch_or_text DateTime64 wire format"
    if any(token in normalized for token in ("nvarchar", "varchar", "char", "text", "xml")):
        return "escaped TSV text"
    if any(token in normalized for token in ("binary", "rowversion", "timestamp")):
        return "configured binary codec"
    if any(token in normalized for token in ("date", "time")):
        return "ClickHouse-safe temporal text"
    return "numeric_or_scalar TSV"


def _normalize(source_type: str) -> tuple[str, bool]:
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


def _literal(value: str, allowed: set[str], field_name: str) -> str:
    if value not in allowed:
        allowed_values = ", ".join(sorted(allowed))
        raise ValueError(f"{field_name} must be one of: {allowed_values}")
    return value


__all__ = ["MssqlClickHouseMatrixDecision", "MssqlClickHouseMatrixMapper", "MssqlClickHouseMatrixPolicy"]
