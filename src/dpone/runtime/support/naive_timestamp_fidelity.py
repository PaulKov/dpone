"""Policy for timezone-naive timestamp transport fidelity."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from typing import Any, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

NaiveTimestampMode = Literal["datetime64"]
NaiveTimestampTransferEncoding = Literal["auto", "text", "epoch"]

_SQL_SERVER_TZ_ALIASES = {
    "UTC": "UTC",
    "Etc/UTC": "UTC",
    "Europe/Moscow": "Russian Standard Time",
}


@dataclass(frozen=True, slots=True)
class NaiveTimestampFidelityPolicy:
    """Portable policy for timestamp values without source offset metadata."""

    mode: NaiveTimestampMode = "datetime64"
    transfer_encoding: NaiveTimestampTransferEncoding = "auto"
    timezone: str = "UTC"
    sql_server_timezone: str | None = None
    column_overrides: Mapping[str, NaiveTimestampFidelityPolicy] = field(default_factory=dict)
    warnings: tuple[str, ...] = field(default_factory=tuple)

    @classmethod
    def from_config(cls, raw: object | None) -> NaiveTimestampFidelityPolicy:
        values = dict(raw or {}) if isinstance(raw, dict) else {}
        temporal = values.get("temporal")
        naive_cfg: dict[str, Any] = {}
        if isinstance(temporal, dict) and isinstance(temporal.get("naive_timestamp"), dict):
            naive_cfg.update(temporal["naive_timestamp"])
        elif isinstance(values.get("naive_timestamp"), dict):
            naive_cfg.update(values["naive_timestamp"])
        policy = cls._from_naive_config(naive_cfg)
        overrides = _column_overrides(naive_cfg)
        if not overrides:
            return policy
        return cls(
            mode=policy.mode,
            transfer_encoding=policy.transfer_encoding,
            timezone=policy.timezone,
            sql_server_timezone=policy.sql_server_timezone,
            column_overrides=overrides,
            warnings=policy.warnings,
        )

    @classmethod
    def _from_naive_config(cls, raw: Mapping[str, Any]) -> NaiveTimestampFidelityPolicy:
        mode = str(raw.get("mode", "datetime64")).lower()
        if mode != "datetime64":
            raise ValueError("type_fidelity.temporal.naive_timestamp.mode must be: datetime64")
        transfer_encoding = str(raw.get("transfer_encoding", "auto")).lower()
        if transfer_encoding not in {"auto", "text", "epoch"}:
            raise ValueError(
                "type_fidelity.temporal.naive_timestamp.transfer_encoding must be one of: auto, text, epoch"
            )
        timezone = str(raw.get("timezone", "UTC"))
        _validate_timezone(timezone)
        sql_server_timezone = raw.get("sql_server_timezone")
        warnings = _warnings(transfer_encoding, timezone)
        return cls(
            mode="datetime64",
            transfer_encoding=transfer_encoding,  # type: ignore[arg-type]
            timezone=timezone,
            sql_server_timezone=str(sql_server_timezone) if sql_server_timezone else None,
            warnings=warnings,
        )

    def effective_transfer_encoding(self, *, native_datetime64_path: bool) -> Literal["text", "epoch"]:
        """Return concrete transfer encoding for a runtime path."""
        if self.transfer_encoding == "auto":
            return "epoch" if native_datetime64_path else "text"
        return self.transfer_encoding

    def for_column(self, column: str) -> NaiveTimestampFidelityPolicy:
        """Return effective policy for one source column."""
        return self.column_overrides.get(str(column).lower(), self)

    def sql_server_timezone_name(self) -> str:
        """Return SQL Server timezone name for `AT TIME ZONE`/`TODATETIMEOFFSET` rendering."""
        if self.sql_server_timezone:
            return self.sql_server_timezone
        return _SQL_SERVER_TZ_ALIASES.get(self.timezone, self.timezone)

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["warnings"] = list(self.warnings)
        return payload


def is_naive_timestamp_type(dtype: str) -> bool:
    """Return true for timestamp source types that do not carry offset metadata."""
    normalized = _normalize_type(dtype)
    return normalized in {"datetime", "smalldatetime"} or normalized.startswith("datetime2")


def naive_timestamp_scale(dtype: str) -> int:
    """Return the DateTime64 precision for an MSSQL naive timestamp type."""
    normalized = _normalize_type(dtype)
    if normalized == "datetime":
        return 3
    if normalized == "smalldatetime":
        return 0
    match = re.search(r"\((\d+)\)", normalized)
    return min(max(int(match.group(1)), 0), 7) if match else 7


def _column_overrides(raw: Mapping[str, Any]) -> Mapping[str, NaiveTimestampFidelityPolicy]:
    columns = raw.get("columns", {})
    if columns in (None, {}):
        return {}
    if not isinstance(columns, Mapping):
        raise ValueError("type_fidelity.temporal.naive_timestamp.columns must be an object")
    overrides: dict[str, NaiveTimestampFidelityPolicy] = {}
    for column, config in columns.items():
        if not isinstance(config, Mapping):
            raise ValueError("type_fidelity.temporal.naive_timestamp.columns.<column> must be an object")
        merged = dict(raw)
        merged.pop("columns", None)
        merged.update(config)
        overrides[str(column).lower()] = NaiveTimestampFidelityPolicy._from_naive_config(merged)
    return overrides


def _warnings(transfer_encoding: str, timezone: str) -> tuple[str, ...]:
    if transfer_encoding in {"auto", "epoch"}:
        return (
            "type_fidelity.temporal.naive_timestamp epoch transport interprets MSSQL timezone-naive "
            f"timestamps as {timezone}; use text transport for raw wall-clock audit exports.",
        )
    return ()


def _validate_timezone(value: str) -> None:
    if value in _SQL_SERVER_TZ_ALIASES.values():
        return
    if re.fullmatch(r"[+-]\d{2}:\d{2}", value):
        return
    try:
        ZoneInfo(value)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(
            "type_fidelity.temporal.naive_timestamp.timezone must be a valid IANA timezone, "
            "UTC, a SQL Server timezone alias, or a fixed offset like +03:00"
        ) from exc


def _normalize_type(dtype: str) -> str:
    normalized = str(dtype or "").strip().lower()
    normalized = normalized.replace(" nullable", "").strip()
    if normalized.startswith("nullable(") and normalized.endswith(")"):
        normalized = normalized[len("nullable(") : -1].strip()
    return normalized


__all__ = [
    "NaiveTimestampFidelityPolicy",
    "NaiveTimestampMode",
    "NaiveTimestampTransferEncoding",
    "is_naive_timestamp_type",
    "naive_timestamp_scale",
]
