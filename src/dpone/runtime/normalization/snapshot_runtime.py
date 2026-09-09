"""Runtime staging/commit helpers for child snapshot stores."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from dpone.runtime.normalization.models import NormalizationResult
from dpone.runtime.normalization.snapshot_store import ChildSnapshotStore


@dataclass(frozen=True, slots=True)
class ChildSnapshotStage:
    root_table: str
    child_table: str
    load_id: str


class ChildSnapshotRuntimeService:
    """Stage, commit and rollback child snapshots for normalized load packages."""

    def stage_result(
        self,
        *,
        store: ChildSnapshotStore | None,
        result: NormalizationResult,
        root_table: str,
        table_keys: Mapping[str, Sequence[str]],
        load_id: str,
    ) -> tuple[ChildSnapshotStage, ...]:
        if store is None:
            return ()
        stages: list[ChildSnapshotStage] = []
        for table_name, unique_key in table_keys.items():
            if table_name == root_table:
                continue
            try:
                table = result.table(table_name)
            except KeyError:
                continue
            keys = [_project_key(row, unique_key) for row in table.rows]
            store.stage_snapshot(
                root_table=root_table,
                child_table=table_name,
                unique_key=unique_key,
                keys=keys,
                load_id=load_id,
            )
            stages.append(ChildSnapshotStage(root_table=root_table, child_table=table_name, load_id=load_id))
        return tuple(stages)

    def stage_rows(
        self,
        *,
        store: ChildSnapshotStore | None,
        root_table: str,
        child_table: str,
        rows: Sequence[Mapping[str, object]],
        unique_key: Sequence[str],
        load_id: str,
    ) -> tuple[ChildSnapshotStage, ...]:
        if store is None or not unique_key:
            return ()
        keys = [_project_key(row, unique_key) for row in rows]
        store.stage_snapshot(
            root_table=root_table,
            child_table=child_table,
            unique_key=unique_key,
            keys=keys,
            load_id=load_id,
        )
        return (ChildSnapshotStage(root_table=root_table, child_table=child_table, load_id=load_id),)

    def commit(self, *, store: ChildSnapshotStore | None, stages: Sequence[ChildSnapshotStage]) -> None:
        if store is None:
            return
        for stage in stages:
            store.commit_snapshot(root_table=stage.root_table, child_table=stage.child_table, load_id=stage.load_id)

    def rollback(self, *, store: ChildSnapshotStore | None, stages: Sequence[ChildSnapshotStage]) -> None:
        if store is None:
            return
        for stage in stages:
            store.rollback_staged(root_table=stage.root_table, child_table=stage.child_table, load_id=stage.load_id)


def _project_key(row: Mapping[str, object], unique_key: Sequence[str]) -> dict[str, object]:
    return {column: row.get(column) for column in unique_key}
