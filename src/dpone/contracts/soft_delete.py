"""Canonical soft-delete storage policy shared by runtime sinks."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from dpone._compat import StrEnum


class SoftDeleteMode(StrEnum):
    """Supported physical representations of the current delete state."""

    TIMESTAMP_ONLY = "timestamp_only"
    TIMESTAMP_AND_FLAG = "timestamp_and_flag"
    FLAG_ONLY = "flag_only"


@dataclass(frozen=True, slots=True)
class SoftDeletePolicy:
    """Resolve target columns while keeping one source of truth per mode."""

    mode: SoftDeleteMode = SoftDeleteMode.TIMESTAMP_ONLY

    @classmethod
    def from_options(cls, raw: object | None) -> SoftDeletePolicy:
        if raw is None:
            return cls()
        if isinstance(raw, SoftDeletePolicy):
            return raw
        value = raw.get("mode") if isinstance(raw, Mapping) else raw
        if value is None:
            return cls()
        try:
            return cls(SoftDeleteMode(str(value).strip().lower()))
        except ValueError as exc:
            modes = ", ".join(mode.value for mode in SoftDeleteMode)
            raise ValueError(f"soft_delete.mode must be one of: {modes}") from exc

    @property
    def timestamp_is_source_of_truth(self) -> bool:
        return self.mode in {SoftDeleteMode.TIMESTAMP_ONLY, SoftDeleteMode.TIMESTAMP_AND_FLAG}

    @property
    def flag_is_computed(self) -> bool:
        return self.mode == SoftDeleteMode.TIMESTAMP_AND_FLAG

    def target_definitions(self) -> tuple[str, ...]:
        """Return canonical SQL Server column fragments for target bootstrap."""

        if self.mode == SoftDeleteMode.TIMESTAMP_ONLY:
            return ("[__dpone__deleted_at] datetime2(7) NULL",)
        if self.mode == SoftDeleteMode.TIMESTAMP_AND_FLAG:
            return (
                "[__dpone__deleted_at] datetime2(7) NULL",
                "[__dpone__is_deleted] AS CONVERT(bit, CASE WHEN [__dpone__deleted_at] IS NULL "
                "THEN 0 ELSE 1 END) PERSISTED",
            )
        return ("[__dpone__is_deleted] bit NOT NULL DEFAULT (0)",)


__all__ = ["SoftDeleteMode", "SoftDeletePolicy"]
