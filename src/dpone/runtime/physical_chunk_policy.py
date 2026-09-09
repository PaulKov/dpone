"""Validated physical file-boundary policy for native transfers."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

PHYSICAL_CHUNK_SIZE_LITERAL_PATTERN = (
    r"^\s*(?:[1-9][0-9]*(?:\.[0-9]+)?|0\.[0-9]*[1-9][0-9]*)"
    r"\s*(?:[KkMmGgTt]i?[Bb]|[Bb])?\s*$"
)
_BYTE_UNIT_MULTIPLIERS = {
    "b": 1,
    "kb": 1000,
    "mb": 1000**2,
    "gb": 1000**3,
    "tb": 1000**4,
    "kib": 1024,
    "mib": 1024**2,
    "gib": 1024**3,
    "tib": 1024**4,
}


class PhysicalChunkLimitExceeded(RuntimeError):
    """Raised when one source row cannot fit into the configured hard limit."""

    code = "physical_chunk_row_exceeds_max_bytes"

    def __init__(self, *, chunk_index: int, row_bytes: int, max_chunk_bytes: int) -> None:
        self.chunk_index = chunk_index
        self.row_bytes = row_bytes
        self.max_chunk_bytes = max_chunk_bytes
        super().__init__(
            f"{self.code}: chunk_index={chunk_index}, row_bytes={row_bytes}, max_chunk_bytes={max_chunk_bytes}"
        )

    def to_evidence(self) -> dict[str, int]:
        """Return data-safe limit metadata without source row contents."""

        return {
            "chunk_index": self.chunk_index,
            "row_bytes": self.row_bytes,
            "max_chunk_bytes": self.max_chunk_bytes,
        }


@dataclass(frozen=True, slots=True)
class PhysicalChunkPolicy:
    """Normalized physical chunking limits for one source scan."""

    target_chunk_bytes: int = 64 * 1024 * 1024
    max_chunk_bytes: int = 128 * 1024 * 1024
    mode: str = "auto"
    spool_mode: str = "file"
    row_boundary: str = "required"
    cleanup_policy: str = "eager"

    def __post_init__(self) -> None:
        if not _is_positive_int(self.target_chunk_bytes):
            raise ValueError("physical_chunk_target_bytes_invalid")
        if not _is_positive_int(self.max_chunk_bytes):
            raise ValueError("physical_chunk_max_bytes_invalid")
        if self.target_chunk_bytes > self.max_chunk_bytes:
            raise ValueError("physical_chunk_target_exceeds_max_bytes")

    @classmethod
    def from_source_options(cls, source_options: Mapping[str, Any] | None) -> PhysicalChunkPolicy:
        native = _mapping((source_options or {}).get("native_transfer"))
        snapshot = _mapping(native.get("snapshot"))
        raw = _mapping(snapshot.get("physical_chunking"))
        return cls(
            target_chunk_bytes=_parse_policy_byte_size(
                raw.get("target_chunk_bytes", "64MiB"),
                "physical_chunk_target_bytes_invalid",
            ),
            max_chunk_bytes=_parse_policy_byte_size(
                raw.get("max_chunk_bytes", "128MiB"),
                "physical_chunk_max_bytes_invalid",
            ),
            mode=_text(raw.get("mode"), "auto"),
            spool_mode=_text(raw.get("spool_mode"), "file"),
            row_boundary=_text(raw.get("row_boundary"), "required"),
            cleanup_policy=_text(raw.get("cleanup_policy"), "eager"),
        )

    def to_evidence(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "spool_mode": self.spool_mode,
            "row_boundary": self.row_boundary,
            "cleanup_policy": self.cleanup_policy,
            "target_chunk_bytes": self.target_chunk_bytes,
            "max_chunk_bytes": self.max_chunk_bytes,
        }


def _is_positive_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _text(value: Any, default: str) -> str:
    return str(value if value is not None else default).strip().lower()


def _parse_policy_byte_size(value: Any, error_code: str) -> int:
    try:
        if isinstance(value, bool) or not isinstance(value, (int, str)):
            raise ValueError
        if isinstance(value, int):
            return value
        if re.fullmatch(PHYSICAL_CHUNK_SIZE_LITERAL_PATTERN, value) is None:
            raise ValueError
        parsed = re.fullmatch(r"\s*([0-9.]+)\s*([A-Za-z]*)\s*", value)
        if parsed is None:
            raise ValueError
        number, unit = parsed.groups()
        multiplier = _BYTE_UNIT_MULTIPLIERS[unit.lower() or "b"]
        return int(Decimal(number) * multiplier)
    except (ArithmeticError, KeyError, TypeError, ValueError):
        raise ValueError(error_code) from None


__all__ = ["PHYSICAL_CHUNK_SIZE_LITERAL_PATTERN", "PhysicalChunkLimitExceeded", "PhysicalChunkPolicy"]
