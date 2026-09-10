"""Observed native transport capacity admission; no exclusive capacity reservation.

The application calls target admission before source extraction and publication.
The executor checks local capacity itself before starting encoding workers.
"""

from __future__ import annotations

from pathlib import Path
from shutil import disk_usage
from typing import Any


def require_native_spool_capacity(work_dir: Path, limits: Any, *, overhead_bytes: int = 1048576) -> int:
    """Admit payload plus bounded metadata and a one-GiB free-space reserve."""
    if type(overhead_bytes) is not int or overhead_bytes < 0:
        raise ValueError("mssql_native.capacity_overhead_invalid")
    work_dir.mkdir(parents=True, exist_ok=True)
    if work_dir.is_symlink() or not work_dir.is_dir():
        raise ValueError("mssql_native.work_directory_invalid")
    required = limits.spool_payload_bound + overhead_bytes + 1024**3
    if disk_usage(work_dir).free < required:
        raise ValueError("mssql_native.spool_capacity_insufficient")
    return required
