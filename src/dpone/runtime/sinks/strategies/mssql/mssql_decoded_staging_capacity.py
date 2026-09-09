"""Capacity admission for the optional MSSQL decoded staging heap."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.runtime.artifact_models import StagingTableArtifact

_SQL_SERVER_DATA_PAGE_BYTES = 8_192
_SQL_SERVER_DATA_PAGES_PER_EXTENT = 8
_SQL_SERVER_ALLOCATION_PAGES_PER_EXTENT = 1
_MIN_CAPACITY_HEADROOM_BYTES = 64 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class MssqlDecodedStagingCapacityPolicy:
    """Conservative admission policy for one additional decoded heap.

    The raw heap is already allocated. Admission reserves one raw-sized copy
    for decoded values. The native reserve includes another raw-sized payload
    copy plus one complete SQL Server data page per receipt row. Data pages are
    rounded to full extents and each extent gets an additional allocation-page
    reserve, so fixed-width expansion and extent metadata cannot make the
    estimate unsafe. SQL Server remains the allocation authority; this policy
    only decides whether the optional optimization may consume additional
    space.
    """

    native_in_row_reserve_bytes: int = _SQL_SERVER_DATA_PAGE_BYTES
    headroom_bytes: int = _MIN_CAPACITY_HEADROOM_BYTES

    def __post_init__(self) -> None:
        """Reject injected policies that could understate the native peak."""

        if (
            isinstance(self.native_in_row_reserve_bytes, bool)
            or not isinstance(self.native_in_row_reserve_bytes, int)
            or self.native_in_row_reserve_bytes < _SQL_SERVER_DATA_PAGE_BYTES
            or isinstance(self.headroom_bytes, bool)
            or not isinstance(self.headroom_bytes, int)
            or self.headroom_bytes < _MIN_CAPACITY_HEADROOM_BYTES
        ):
            raise ValueError("mssql_decoded_staging.capacity_policy_invalid")

    def required_data_bytes(self, *, raw_reserved_bytes: int, row_count: int) -> int:
        """Return free data-file bytes required before decoded materialization."""

        if (
            isinstance(raw_reserved_bytes, bool)
            or not isinstance(raw_reserved_bytes, int)
            or raw_reserved_bytes < 0
            or isinstance(row_count, bool)
            or not isinstance(row_count, int)
            or row_count < 0
        ):
            raise ValueError("mssql_decoded_staging.capacity_input_invalid")
        native_data_extents = (row_count + _SQL_SERVER_DATA_PAGES_PER_EXTENT - 1) // _SQL_SERVER_DATA_PAGES_PER_EXTENT
        native_fixed_width_reserve = native_data_extents * (
            _SQL_SERVER_DATA_PAGES_PER_EXTENT * self.native_in_row_reserve_bytes
            + _SQL_SERVER_ALLOCATION_PAGES_PER_EXTENT * _SQL_SERVER_DATA_PAGE_BYTES
        )
        native_reserve = raw_reserved_bytes + native_fixed_width_reserve
        return raw_reserved_bytes + native_reserve + self.headroom_bytes


@dataclass(frozen=True, slots=True)
class _DecodedStagingAdmission:
    materialize: bool
    reason: str
    raw_reserved_bytes: int | None = None
    available_data_bytes: int | None = None
    required_data_bytes: int | None = None


class _MssqlDecodedStagingAdmissionService:
    """Probe metadata only and fail safely to the legacy inline decoder."""

    def __init__(self, strategy: Any, policy: MssqlDecodedStagingCapacityPolicy) -> None:
        self._connector = strategy.connector
        self._policy = policy

    def decide(self, raw: StagingTableArtifact, *, row_count: int) -> _DecodedStagingAdmission:
        if row_count == 0:
            return _DecodedStagingAdmission(False, "empty_raw")
        try:
            rows = self._connector.get_records(self._capacity_query(raw), as_dict=True)
        except Exception:  # Metadata permission/availability must not block the proven inline path.
            return _DecodedStagingAdmission(False, "capacity_probe_unavailable")
        if len(rows) != 1 or not isinstance(rows[0], Mapping):
            return _DecodedStagingAdmission(False, "capacity_evidence_invalid")
        raw_reserved = rows[0].get("raw_reserved_bytes")
        available = rows[0].get("available_data_bytes")
        if not _is_non_negative_int(raw_reserved) or not _is_non_negative_int(available) or raw_reserved == 0:
            return _DecodedStagingAdmission(False, "capacity_evidence_invalid")
        required = self._policy.required_data_bytes(
            raw_reserved_bytes=raw_reserved,
            row_count=row_count,
        )
        return _DecodedStagingAdmission(
            materialize=available >= required,
            reason="capacity_admitted" if available >= required else "insufficient_capacity",
            raw_reserved_bytes=raw_reserved,
            available_data_bytes=available,
            required_data_bytes=required,
        )

    def _capacity_query(self, raw: StagingTableArtifact) -> str:
        local_name = (
            f"{self._connector.quote_identifier(raw.schema)}.{self._connector.quote_identifier(raw.table)}"
        ).replace("'", "''")
        query = (
            "SELECT "
            "COALESCE((SELECT SUM(CONVERT(bigint, p.reserved_page_count)) * 8192 "
            "FROM sys.dm_db_partition_stats AS p "
            f"WHERE p.object_id = OBJECT_ID(N'{local_name}', N'U') AND p.index_id IN (0, 1)), 0) "
            "AS raw_reserved_bytes, "
            "COALESCE((SELECT SUM((CONVERT(bigint, f.size) - "
            "CONVERT(bigint, FILEPROPERTY(f.name, 'SpaceUsed'))) * 8192) "
            "FROM sys.database_files AS f "
            "INNER JOIN sys.filegroups AS fg ON fg.data_space_id = f.data_space_id "
            "WHERE f.type = 0 AND f.state = 0 AND fg.is_default = 1), 0) "
            "AS available_data_bytes"
        )
        if not raw.database:
            return query
        escaped = query.replace("'", "''")
        database = self._connector.quote_identifier(raw.database)
        return f"EXEC {database}.sys.sp_executesql N'{escaped}'"


def _is_non_negative_int(value: object) -> bool:
    return not isinstance(value, bool) and isinstance(value, int) and value >= 0


__all__ = ["MssqlDecodedStagingCapacityPolicy"]
