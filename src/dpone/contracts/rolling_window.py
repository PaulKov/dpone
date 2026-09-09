"""Closed, clock-free rolling-window contract shared by authoring and runtime."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

from dpone._compat import UTC

_DURATION = re.compile(r"P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d{1,6})?)S)?)?")
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_MAX_CHUNKS = 100_000


def _duration(value: Any) -> timedelta:
    match = _DURATION.fullmatch(value) if isinstance(value, str) else None
    if match is None or not any(match.groups()) or value.endswith("T"):
        raise ValueError("rolling_window_duration_invalid: use a positive fixed ISO-8601 duration")
    days, hours, minutes, seconds = (Decimal(part or "0") for part in match.groups())
    micros = (days * 86400 + hours * 3600 + minutes * 60 + seconds) * 1_000_000
    if micros <= 0:
        raise ValueError("rolling_window_duration_nonpositive")
    try:
        return timedelta(microseconds=int(micros))
    except OverflowError as exc:
        raise ValueError("rolling_window_duration_out_of_range") from exc


def _anchor(value: Any) -> datetime:
    if isinstance(value, str):
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})", value):
            raise ValueError("rolling_window_anchor_invalid: require an exact timezone-aware timestamp")
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("rolling_window_anchor_invalid") from exc
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("rolling_window_anchor_required: provide timezone-aware data_interval_end")
    return value.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class FrozenRollingWindow:
    """An immutable half-open interval; UTC boundaries participate in identity."""

    column: str
    start: datetime
    end: datetime
    boundaries: tuple[datetime, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.column, str) or not _IDENTIFIER.fullmatch(self.column):
            raise ValueError("rolling_window_column_invalid")
        start, end = _anchor(self.start), _anchor(self.end)
        if not isinstance(self.boundaries, tuple | list) or not 2 <= len(self.boundaries) <= _MAX_CHUNKS + 1:
            raise ValueError("rolling_window_boundaries_invalid")
        boundaries = tuple(_anchor(value) for value in self.boundaries)
        if (
            boundaries[0] != start
            or boundaries[-1] != end
            or any(left >= right for left, right in zip(boundaries, boundaries[1:]))
        ):
            raise ValueError("rolling_window_boundaries_invalid")
        object.__setattr__(self, "start", start)
        object.__setattr__(self, "end", end)
        object.__setattr__(self, "boundaries", boundaries)

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "dpone.rolling_window.v1",
            "column": self.column,
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "boundaries": [value.isoformat() for value in self.boundaries],
        }


@dataclass(frozen=True, slots=True)
class RollingWindowSpec:
    """Declarative UTC-duration window, resolved only from caller interval context."""

    column: str
    lookback: str
    chunk_interval: str = "P1D"
    anchor: str = "data_interval_end"
    timezone: str = "UTC"

    def __post_init__(self) -> None:
        if not isinstance(self.column, str) or not _IDENTIFIER.fullmatch(self.column):
            raise ValueError("rolling_window_column_invalid: use one unqualified identifier")
        if self.anchor != "data_interval_end" or self.timezone != "UTC":
            raise ValueError("rolling_window_context_unsupported: require data_interval_end and UTC")
        _duration(self.lookback)
        _duration(self.chunk_interval)

    @classmethod
    def from_mapping(cls, raw: object) -> RollingWindowSpec:
        if not isinstance(raw, Mapping) or set(raw) - {"column", "lookback", "chunk_interval", "anchor", "timezone"}:
            raise ValueError("rolling_window_options_invalid")
        if not {"column", "anchor", "lookback"}.issubset(raw):
            raise ValueError("rolling_window_options_required: column, anchor, lookback")
        return cls(**dict(raw))

    def to_dict(self) -> dict[str, str]:
        return {
            "column": self.column,
            "anchor": self.anchor,
            "lookback": self.lookback,
            "chunk_interval": self.chunk_interval,
            "timezone": self.timezone,
        }

    def freeze(self, context: Mapping[str, Any]) -> FrozenRollingWindow:
        end = _anchor(context.get(self.anchor))
        lookback, chunk = _duration(self.lookback), _duration(self.chunk_interval)
        count = lookback // chunk + bool(lookback % chunk)
        if count > _MAX_CHUNKS:
            raise ValueError("rolling_window_chunk_count_exceeded: increase chunk_interval")
        try:
            start = end - lookback
            boundaries = tuple(start + i * chunk for i in range(count)) + (end,)
        except OverflowError as exc:
            raise ValueError("rolling_window_bounds_out_of_range") from exc
        return FrozenRollingWindow(self.column, start, end, boundaries)


__all__ = ["FrozenRollingWindow", "RollingWindowSpec"]
