"""Immutable models for the MSSQL load-strategy capability decision."""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True, slots=True)
class FullRefreshPolicy:
    overwrite_type: str


@dataclass(frozen=True, slots=True)
class IncrementalAppendPolicy:
    only_new_rows: bool
    unique_key: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class IncrementalMergePolicy:
    merge_policy: str
    duplicate_policy: str
    unique_key: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ReplacePolicy:
    """SQL-free scope decision for ordinary MSSQL replace."""

    scope_kind: str
    portable_scope_sha256: str | None


@dataclass(frozen=True, slots=True)
class PartitionReplacePolicy:
    column: str
    value_expression: None
    values_from_staging: bool
    max_partitions_per_run: int
    native: bool
    native_mode: str
    require_native: bool


@dataclass(frozen=True, slots=True)
class SnapshotDiffPolicy:
    compare: str
    delete_policy: str
    unique_key: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SCD2Policy:
    delete_policy: str
    unique_key: tuple[str, ...]
    valid_from_column: str
    valid_to_column: str
    current_flag_column: str
    row_hash_column: str


@dataclass(frozen=True, slots=True)
class BackfillPolicy:
    inner_mode: str
    parallel_workers: int
    publication_mode: str
    retain_backup: bool
    inner_policy: IncrementalAppendPolicy | IncrementalMergePolicy | PartitionReplacePolicy | None


@dataclass(frozen=True, slots=True)
class MSSQLLoadStrategyContract:
    """One normalized, serializable MSSQL strategy decision."""

    mode: str
    full_refresh: FullRefreshPolicy | None = None
    incremental_append: IncrementalAppendPolicy | None = None
    incremental_merge: IncrementalMergePolicy | None = None
    replace: ReplacePolicy | None = None
    partition_replace: PartitionReplacePolicy | None = None
    snapshot_diff: SnapshotDiffPolicy | None = None
    scd2: SCD2Policy | None = None
    backfill: BackfillPolicy | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


__all__ = [
    "BackfillPolicy",
    "FullRefreshPolicy",
    "IncrementalAppendPolicy",
    "IncrementalMergePolicy",
    "MSSQLLoadStrategyContract",
    "PartitionReplacePolicy",
    "ReplacePolicy",
    "SCD2Policy",
    "SnapshotDiffPolicy",
]
