"""Protocol boundaries for reconciliation runtime components."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class SnapshotStore(Protocol):
    def ensure_snapshot_table(self, target_table: str, unique_key: str | list[str]) -> bool: ...

    def insert_snapshot_from_source(
        self,
        rs_table: str,
        unique_key_list: list[str],
        source_connector: Any,
        source_schema: str,
        source_table: str,
        batch_size: int = 50000,
        batch_commit_mode: str = "whole",
        gcs_config: Any = None,
    ) -> int: ...

    def get_snapshot_count(self, rs_table: str) -> int: ...

    def prune_old_snapshots(self, rs_table: str) -> None: ...


@runtime_checkable
class DeleteLogStore(Protocol):
    def ensure_deleted_log_table(self, target_table: str, unique_key: str | list[str]) -> bool: ...


@runtime_checkable
class SnapshotLoadWriter(Protocol):
    def _load_file_to_bigquery(
        self,
        tmp_file_path: str,
        table_id: str,
        job_config: Any,
        total_rows: int,
        rs_table: str,
        connector_class: str,
        source_schema: str,
        source_table: str,
    ) -> int: ...


@runtime_checkable
class ReconciliationStore(SnapshotStore, DeleteLogStore, Protocol):
    def ensure_tech_schema(self) -> None: ...


@runtime_checkable
class SoftDeleteFinalizer(Protocol):
    def execute_soft_delete(
        self,
        target_connector: Any,
        target_schema: str,
        target_table: str,
        unique_key_list: list[str],
        deleted_keys: list[dict[str, Any]],
        tech_schema: str,
    ) -> int: ...


__all__ = [
    "DeleteLogStore",
    "ReconciliationStore",
    "SnapshotLoadWriter",
    "SnapshotStore",
    "SoftDeleteFinalizer",
]
