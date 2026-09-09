"""Load strategy contract for dpone runtime and manifest compilation."""

from __future__ import annotations

from enum import Enum


class LoadStrategy(Enum):
    """Supported data loading strategies."""

    FULL_REFRESH = "full_refresh"
    INCREMENTAL_MERGE = "incremental_merge"
    INCREMENTAL_APPEND = "incremental_append"
    REPLACE = "replace"
    PARTITION_REPLACE = "partition_replace"
    SNAPSHOT_DIFF = "snapshot_diff"
    SCD2 = "scd2"
    CDC_APPLY = "cdc_apply"
    BACKFILL = "backfill"


__all__ = ["LoadStrategy"]
