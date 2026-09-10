"""SQL Server catalog, volume and log headroom admission observations."""

from __future__ import annotations

from typing import Any


def require_native_target_capacity(connector: Any, limits: Any, *, required_headroom_bytes: int) -> None:
    """Require visible data/log headroom and allocation below the stop threshold.

    A dedicated target-database session must have catalog/volume/log observation
    permissions. Missing observations fail admission. Concurrent allocation can
    consume this observed headroom; callers repeat this check before publication.
    """
    if type(required_headroom_bytes) is not int or required_headroom_bytes <= 0:
        raise ValueError("mssql_native.target_headroom_required")
    files = connector.get_records(
        "SELECT f.type, CONVERT(bigint, f.size)*8192 AS allocated_bytes, "
        "v.available_bytes FROM sys.database_files f "
        "CROSS APPLY sys.dm_os_volume_stats(DB_ID(), f.file_id) v",
        as_dict=True,
    )
    log = connector.get_records(
        "SELECT total_log_size_in_bytes-used_log_space_in_bytes AS free_bytes FROM sys.dm_db_log_space_usage",
        as_dict=True,
    )
    if not files or not log or not {0, 1}.issubset({row["type"] for row in files}):
        raise ValueError("mssql_native.target_capacity_unavailable")
    if any(
        type(row[key]) is not int or row[key] < 0 for row in files for key in ("allocated_bytes", "available_bytes")
    ):
        raise ValueError("mssql_native.target_capacity_invalid")
    staging = connector.get_records(
        "SELECT COALESCE(SUM(p.reserved_page_count), 0) * 8192 AS allocated_bytes "
        "FROM sys.dm_db_partition_stats p JOIN sys.tables t ON t.object_id=p.object_id "
        "WHERE t.name LIKE 'dpone[_]native[_]%'",
        as_dict=True,
    )
    if not staging or type(staging[0]["allocated_bytes"]) is not int or staging[0]["allocated_bytes"] < 0:
        raise ValueError("mssql_native.allocation_unavailable")
    if staging[0]["allocated_bytes"] >= limits.stage_allocated_bytes_stop_threshold:
        raise ValueError("mssql_native.stage_allocation_threshold_exceeded")
    if min(row["available_bytes"] for row in files) < required_headroom_bytes:
        raise ValueError("mssql_native.target_volume_headroom_insufficient")
    free_log = log[0]["free_bytes"]
    if type(free_log) is not int or free_log < required_headroom_bytes:
        raise ValueError("mssql_native.target_log_headroom_insufficient")
