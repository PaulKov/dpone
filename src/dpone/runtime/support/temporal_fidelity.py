"""Reusable temporal fidelity policy for offset-aware timestamps."""

from __future__ import annotations

import re
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from dpone.contracts.technical_columns import (
    is_offset_minutes_column,
    offset_minutes_column_name,
)
from dpone.runtime.support.naive_timestamp_fidelity import (
    NaiveTimestampFidelityPolicy,
    NaiveTimestampMode,
    NaiveTimestampTransferEncoding,
    is_naive_timestamp_type,
    naive_timestamp_scale,
)

OffsetTimestampMode = Literal["utc_instant", "fixed_timezone", "preserve_offset", "preserve_text"]
MalformedTemporalMode = Literal["fail", "preserve_text"]

_SQL_SERVER_TZ_ALIASES = {
    "UTC": "UTC",
    "Etc/UTC": "UTC",
    "Europe/Moscow": "Russian Standard Time",
}
_IANA_TZ_BY_SQL_SERVER_ALIAS = {
    "UTC": "UTC",
    "Russian Standard Time": "Europe/Moscow",
}


@dataclass(frozen=True, slots=True)
class TemporalFidelityPolicy:
    """Portable policy for timestamp values that carry an offset/timezone."""

    offset_timestamp_mode: OffsetTimestampMode = "utc_instant"
    target_timezone: str = "UTC"
    sql_server_timezone: str | None = None
    malformed: MalformedTemporalMode = "fail"
    column_overrides: Mapping[str, TemporalFidelityPolicy] = field(default_factory=dict)
    warnings: tuple[str, ...] = field(default_factory=tuple)

    @classmethod
    def from_config(cls, raw: object | None) -> TemporalFidelityPolicy:
        values = dict(raw or {}) if isinstance(raw, dict) else {}
        warnings: list[str] = []
        offset_cfg: dict[str, Any] = {}

        legacy_flat = _legacy_flat(values)
        if legacy_flat:
            offset_cfg.update(legacy_flat)
            warnings.append(
                "type_fidelity.datetimeoffset_mode/datetimeoffset_timezone are deprecated; "
                "use type_fidelity.temporal.offset_timestamp."
            )

        datetimeoffset_alias = values.get("datetimeoffset")
        if isinstance(datetimeoffset_alias, dict):
            offset_cfg.update(datetimeoffset_alias)
            warnings.append(
                "type_fidelity.datetimeoffset is a compatibility alias; use type_fidelity.temporal.offset_timestamp."
            )

        temporal = values.get("temporal")
        if isinstance(temporal, dict):
            nested = temporal.get("offset_timestamp")
            if isinstance(nested, dict):
                offset_cfg.update(nested)

        policy = cls._from_offset_config(offset_cfg, warnings=tuple(warnings))
        column_overrides = _column_overrides(offset_cfg)
        if not column_overrides:
            return policy
        return cls(
            offset_timestamp_mode=policy.offset_timestamp_mode,
            target_timezone=policy.target_timezone,
            sql_server_timezone=policy.sql_server_timezone,
            malformed=policy.malformed,
            column_overrides=column_overrides,
            warnings=policy.warnings,
        )

    @classmethod
    def _from_offset_config(
        cls, offset_cfg: Mapping[str, Any], *, warnings: tuple[str, ...] = ()
    ) -> TemporalFidelityPolicy:
        mode = str(offset_cfg.get("mode", offset_cfg.get("offset_timestamp_mode", "utc_instant"))).lower()
        if mode not in {"utc_instant", "fixed_timezone", "preserve_offset", "preserve_text"}:
            raise ValueError(
                "type_fidelity.temporal.offset_timestamp.mode must be one of: "
                "utc_instant, fixed_timezone, preserve_offset, preserve_text"
            )
        malformed = str(offset_cfg.get("malformed", "fail")).lower()
        if malformed not in {"fail", "preserve_text"}:
            raise ValueError("type_fidelity.temporal.offset_timestamp.malformed must be one of: fail, preserve_text")
        timezone = str(offset_cfg.get("timezone", offset_cfg.get("target_timezone", "UTC")))
        _validate_timezone(timezone)
        sql_server_timezone = offset_cfg.get("sql_server_timezone")
        return cls(
            offset_timestamp_mode=mode,  # type: ignore[arg-type]
            target_timezone=timezone,
            sql_server_timezone=str(sql_server_timezone) if sql_server_timezone else None,
            malformed=malformed,  # type: ignore[arg-type]
            warnings=tuple(warnings),
        )

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["warnings"] = list(self.warnings)
        return payload

    def sql_server_timezone_name(self) -> str:
        """Return SQL Server `AT TIME ZONE` name for the configured timezone."""
        if self.sql_server_timezone:
            return self.sql_server_timezone
        return _SQL_SERVER_TZ_ALIASES.get(self.target_timezone, self.target_timezone)

    def for_column(self, column: str) -> TemporalFidelityPolicy:
        """Return the effective policy for a source column."""
        return self.column_overrides.get(str(column).lower(), self)

    @property
    def preserves_offset_column(self) -> bool:
        return self.offset_timestamp_mode == "preserve_offset"

    @property
    def offset_preserving(self) -> bool:
        """Return true when the selected mode keeps the original source offset information."""
        return self.offset_timestamp_mode in {"preserve_offset", "preserve_text"}


class TemporalFidelityProjector:
    """Project offset-aware timestamp values according to a portable fidelity policy."""

    def __init__(self, policy: TemporalFidelityPolicy | None = None) -> None:
        self.policy = policy or TemporalFidelityPolicy()

    def project_payload(self, payload: Any) -> Any:
        columns = _offset_timestamp_columns(payload.schema)
        if not columns:
            return payload
        schema = _projected_schema(payload.schema, self.policy)
        artifact = self._project_artifact(payload.artifact, columns)
        if artifact is payload.artifact and schema == list(payload.schema):
            return payload
        return payload.rebind(
            artifact=artifact,
            schema=schema,
        )

    def _project_artifact(self, artifact: Any, columns: Mapping[str, str]) -> Any:
        from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
        from dpone.runtime.streaming_rows import StreamingRowsArtifact

        if isinstance(artifact, InMemoryRowsArtifact):
            return InMemoryRowsArtifact(self._project_rows(artifact._rows, columns))
        if isinstance(artifact, StreamingRowsArtifact):
            return artifact.rebind_iterator(self._project_rows(artifact._iterator, columns))
        return artifact

    def _project_rows(
        self,
        rows: Iterable[Mapping[str, object]],
        columns: Mapping[str, str],
    ) -> Iterator[Mapping[str, object]]:
        for row in rows:
            projected = dict(row)
            for column in columns:
                if column not in row:
                    continue
                policy = self.policy.for_column(column)
                value = row[column]
                projected[column] = self._project_value(column, value, policy)
                if policy.preserves_offset_column:
                    projected[offset_minutes_column_name(column)] = _offset_minutes(value)
            yield projected

    def _project_value(self, column: str, value: object, policy: TemporalFidelityPolicy) -> object:
        if value is None:
            return None
        if policy.offset_timestamp_mode == "preserve_text":
            return _offset_timestamp_text(value)
        aware = _aware_datetime(value)
        if aware is None:
            if policy.malformed == "preserve_text":
                return _offset_timestamp_text(value)
            raise TemporalProjectionError(f"Cannot parse offset timestamp value for column {column!r}: {value!r}")
        if policy.offset_timestamp_mode == "fixed_timezone":
            return aware.astimezone(_target_tzinfo(policy)).replace(tzinfo=None)
        return aware.astimezone(UTC).replace(tzinfo=None)


class TemporalProjectionError(ValueError):
    """Raised when temporal projection cannot safely parse an offset timestamp."""


def temporal_policy_warnings(
    policy: TemporalFidelityPolicy, *, generated_columns: tuple[str, ...] = ()
) -> tuple[str, ...]:
    """Return user-facing diagnostics for the selected offset timestamp policy."""
    warnings = list(policy.warnings)
    if policy.offset_timestamp_mode == "fixed_timezone":
        warnings.append(
            "type_fidelity.temporal.offset_timestamp.fixed_timezone converts instants to the configured timezone; "
            "the original source offset is not preserved."
        )
    elif policy.offset_timestamp_mode == "preserve_offset":
        columns = ", ".join(generated_columns) if generated_columns else "__dpone__tz_offset_minutes__<column>"
        warnings.append(
            "type_fidelity.temporal.offset_timestamp.preserve_offset adds framework-generated offset-minute "
            f"companion columns ({columns}); keep schema_evolution enabled or pre-create them."
        )
    elif policy.offset_timestamp_mode == "preserve_text":
        warnings.append(
            "type_fidelity.temporal.offset_timestamp.preserve_text keeps the raw offset timestamp text; "
            "downstream analytical filters should cast or materialize a typed timestamp when needed."
        )
    if policy.malformed == "preserve_text":
        warnings.append(
            "type_fidelity.temporal.offset_timestamp.malformed=preserve_text keeps malformed values as text; "
            "target type and schema can become string for affected columns."
        )
    return tuple(dict.fromkeys(warnings))


def is_offset_timestamp_type(dtype: str) -> bool:
    """Return true when a source dtype carries timestamp offset semantics."""
    normalized = str(dtype).strip().lower()
    return any(
        token in normalized
        for token in (
            "datetimeoffset",
            "timestamp with time zone",
            "timestamp with timezone",
            "timestamptz",
            "iso8601_offset_timestamp",
            "offset_timestamp",
        )
    )


def _offset_timestamp_columns(schema: Sequence[tuple[str, str]]) -> dict[str, str]:
    return {str(column): str(dtype) for column, dtype in schema if is_offset_timestamp_type(str(dtype))}


def _projected_schema(
    schema: Sequence[tuple[str, str]],
    policy: TemporalFidelityPolicy,
) -> list[tuple[str, str]]:
    projected: list[tuple[str, str]] = []
    existing = {str(column).lower() for column, _dtype in schema}
    for column, dtype in schema:
        effective = policy.for_column(column)
        if is_offset_timestamp_type(dtype):
            projected.append((column, _projected_source_type(dtype, effective)))
            companion = offset_minutes_column_name(column)
            if effective.preserves_offset_column and companion.lower() not in existing:
                projected.append((companion, _offset_minutes_dtype(dtype)))
        else:
            projected.append((column, dtype))
    return projected


def _projected_source_type(dtype: str, policy: TemporalFidelityPolicy) -> str:
    if policy.offset_timestamp_mode == "preserve_text" or policy.malformed == "preserve_text":
        return "string"
    if policy.offset_timestamp_mode in {"utc_instant", "fixed_timezone"}:
        return "timestamp"
    return dtype


def _offset_minutes_dtype(dtype: str) -> str:
    return "smallint nullable" if _is_nullable_type(dtype) else "smallint nullable"


def _is_nullable_type(dtype: str) -> bool:
    normalized = str(dtype).strip().lower()
    return "nullable" in normalized or normalized.startswith("null")


def _offset_timestamp_text(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value).strip().replace(" ", "T")


def _aware_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else None
    text = str(value).strip().replace(" ", "T")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _offset_minutes(value: object) -> int | None:
    aware = _aware_datetime(value)
    if aware is not None:
        offset = aware.utcoffset()
        return int(offset.total_seconds() // 60) if offset is not None else None
    suffix = _timezone_suffix(str(value))
    if not suffix:
        return None
    sign = -1 if suffix.startswith("-") else 1
    hours, minutes = suffix[1:].split(":", 1)
    return sign * (int(hours) * 60 + int(minutes))


def _timezone_suffix(text: str) -> str:
    match = re.search(r"([+-]\d{2}:\d{2})$", text.strip())
    return match.group(1) if match else ""


def _target_tzinfo(policy: TemporalFidelityPolicy) -> timezone | ZoneInfo:
    value = policy.target_timezone
    if re.fullmatch(r"[+-]\d{2}:\d{2}", value):
        sign = -1 if value.startswith("-") else 1
        hours, minutes = value[1:].split(":", 1)
        return timezone(sign * timedelta(hours=int(hours), minutes=int(minutes)))
    return ZoneInfo(_IANA_TZ_BY_SQL_SERVER_ALIAS.get(value, value))


def _column_overrides(offset_cfg: Mapping[str, Any]) -> Mapping[str, TemporalFidelityPolicy]:
    raw = offset_cfg.get("columns", {})
    if raw in (None, {}):
        return {}
    if not isinstance(raw, Mapping):
        raise ValueError("type_fidelity.temporal.offset_timestamp.columns must be an object")
    overrides: dict[str, TemporalFidelityPolicy] = {}
    for column, config in raw.items():
        if not isinstance(config, Mapping):
            raise ValueError("type_fidelity.temporal.offset_timestamp.columns.<column> must be an object")
        merged = dict(offset_cfg)
        merged.pop("columns", None)
        merged.update(config)
        overrides[str(column).lower()] = TemporalFidelityPolicy._from_offset_config(merged)
    return overrides


def _legacy_flat(values: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    if "datetimeoffset_mode" in values:
        result["mode"] = values["datetimeoffset_mode"]
    if "datetimeoffset_timezone" in values:
        result["timezone"] = values["datetimeoffset_timezone"]
    return result


def _validate_timezone(timezone: str) -> None:
    if timezone in _SQL_SERVER_TZ_ALIASES.values():
        return
    if re.fullmatch(r"[+-]\d{2}:\d{2}", timezone):
        return
    try:
        ZoneInfo(timezone)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(
            "type_fidelity.temporal.offset_timestamp.timezone must be a valid IANA timezone, "
            "UTC, a SQL Server timezone alias, or a fixed offset like +03:00"
        ) from exc


__all__ = [
    "MalformedTemporalMode",
    "NaiveTimestampFidelityPolicy",
    "NaiveTimestampMode",
    "NaiveTimestampTransferEncoding",
    "OffsetTimestampMode",
    "TemporalFidelityPolicy",
    "TemporalFidelityProjector",
    "TemporalProjectionError",
    "is_naive_timestamp_type",
    "is_offset_minutes_column",
    "is_offset_timestamp_type",
    "naive_timestamp_scale",
    "offset_minutes_column_name",
    "temporal_policy_warnings",
]
