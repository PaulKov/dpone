"""Source-specific CDC retention probes."""

from __future__ import annotations

from typing import Any

from dpone.readiness.cdc import CDCBackend
from dpone.runtime.cdc.retention_models import CdcRetentionBounds


class StaticCdcRetentionProbe:
    """Credential-free probe for local checks, tests, and CI fixtures."""

    def __init__(self, bounds: CdcRetentionBounds) -> None:
        self._bounds = bounds

    def read_bounds(self) -> CdcRetentionBounds:
        return self._bounds


class MssqlChangeTrackingRetentionProbe:
    """Read SQL Server Change Tracking valid-version retention bounds."""

    def __init__(self, *, connector: Any, source_schema: str, source_table: str) -> None:
        self._connector = connector
        self._source_schema = source_schema
        self._source_table = source_table

    def read_bounds(self) -> CdcRetentionBounds:
        rows = self._connector.get_records(
            """
SELECT
    CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID(?)) AS min_valid_version,
    CHANGE_TRACKING_CURRENT_VERSION() AS current_version
""".strip(),
            (self._qualified_name_literal(),),
            as_dict=True,
        )
        row = _first_row(rows)
        current = str(row.get("current_version") or 0)
        return CdcRetentionBounds(
            backend=CDCBackend.MSSQL_CHANGE_TRACKING,
            min_available_offset=str(row.get("min_valid_version") or 0),
            high_watermark=current,
            current_offset=current,
            retention_seconds=_optional_int(row.get("retention_seconds")),
        )

    def _qualified_name_literal(self) -> str:
        return f"[{self._source_schema}].[{self._source_table}]"


class MssqlCdcRetentionProbe:
    """Read SQL Server native CDC LSN retention bounds."""

    def __init__(self, *, connector: Any, capture_instance: str) -> None:
        self._connector = connector
        self._capture_instance = capture_instance

    def read_bounds(self) -> CdcRetentionBounds:
        rows = self._connector.get_records(
            """
SELECT
    sys.fn_varbintohexstr(sys.fn_cdc_get_min_lsn(?)) AS min_lsn,
    sys.fn_varbintohexstr(sys.fn_cdc_get_max_lsn()) AS max_lsn
""".strip(),
            (self._capture_instance,),
            as_dict=True,
        )
        row = _first_row(rows)
        current = str(row.get("max_lsn") or "0x00000000000000000000")
        return CdcRetentionBounds(
            backend=CDCBackend.MSSQL_CDC,
            min_available_offset=str(row.get("min_lsn") or "0x00000000000000000000"),
            high_watermark=current,
            current_offset=current,
            retention_seconds=_optional_int(row.get("retention_seconds")),
        )


def _first_row(rows: object) -> dict[str, object]:
    if isinstance(rows, list | tuple) and rows and isinstance(rows[0], dict):
        return rows[0]
    return {}


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    return int(value)


__all__ = [
    "MssqlCdcRetentionProbe",
    "MssqlChangeTrackingRetentionProbe",
    "StaticCdcRetentionProbe",
]
