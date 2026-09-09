"""Child-table reconciliation planning for nested normalization."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from dpone.runtime.normalization.models import NormalizationResult, NormalizedTable
from dpone.runtime.normalization.options import NestedNormalizationOptions


@dataclass(frozen=True, slots=True)
class ChildTableDiff:
    """Physical-delete diff for one normalized child table."""

    table: str
    unique_key: tuple[str, ...]
    deleted_keys: tuple[dict[str, object], ...]
    current_keys: tuple[dict[str, object], ...]


@dataclass(frozen=True, slots=True)
class ChildReconciliationPlan:
    """All child-table reconciliation diffs for one nested payload."""

    tables: dict[str, ChildTableDiff]

    @property
    def deleted_count(self) -> int:
        return sum(len(diff.deleted_keys) for diff in self.tables.values())

    def to_dict(self) -> dict[str, object]:
        return {
            "deleted_count": self.deleted_count,
            "tables": {
                table: {
                    "unique_key": list(diff.unique_key),
                    "deleted_keys": list(diff.deleted_keys),
                    "current_keys": list(diff.current_keys),
                }
                for table, diff in self.tables.items()
            },
        }


class ChildTableReconciliationService:
    """Compute child-table physical deletes from previous/current snapshots."""

    def diff_results(
        self,
        previous: NormalizationResult,
        current: NormalizationResult,
        *,
        options: NestedNormalizationOptions,
    ) -> ChildReconciliationPlan:
        previous_tables = previous.as_mapping()
        current_tables = current.as_mapping()
        diffs: dict[str, ChildTableDiff] = {}
        for table, unique_key in options.path_policies.table_keys().items():
            if table not in previous_tables:
                continue
            current_table = current_tables.get(table)
            diff = self.diff_tables(
                previous_tables[table],
                current_table,
                unique_key=unique_key,
            )
            if diff.deleted_keys or diff.current_keys:
                diffs[table] = diff
        return ChildReconciliationPlan(tables=diffs)

    def diff_tables(
        self,
        previous: NormalizedTable,
        current: NormalizedTable | None,
        *,
        unique_key: Sequence[str],
    ) -> ChildTableDiff:
        key_tuple = tuple(unique_key)
        previous_keys = _key_map(previous.rows, key_tuple)
        current_keys = _key_map(current.rows if current is not None else (), key_tuple)
        deleted = [previous_keys[key] for key in sorted(set(previous_keys) - set(current_keys))]
        current_key_rows = [current_keys[key] for key in sorted(current_keys)]
        return ChildTableDiff(
            table=previous.name,
            unique_key=key_tuple,
            deleted_keys=tuple(deleted),
            current_keys=tuple(current_key_rows),
        )


def _key_map(
    rows: Sequence[Mapping[str, object]], unique_key: tuple[str, ...]
) -> dict[tuple[object, ...], dict[str, object]]:
    result: dict[tuple[object, ...], dict[str, object]] = {}
    for row in rows:
        key = tuple(row.get(column) for column in unique_key)
        if any(value is None for value in key):
            raise ValueError(f"Nested child row is missing key columns {unique_key}: {dict(row)}")
        result[key] = {column: row.get(column) for column in unique_key}
    return result
