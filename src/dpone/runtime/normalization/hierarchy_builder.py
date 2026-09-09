"""Reverse builder for dpone normalized nested tables."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from dpone.runtime.normalization.models import NormalizationResult

_TECHNICAL_PREFIX = "__dpone__"
_VALUE_COLUMN = "value"


class HierarchyBuilderService:
    """Rebuild nested payloads from normalized tables using row lineage ids."""

    def rebuild(self, result: NormalizationResult, *, root_table: str) -> list[dict[str, Any]]:
        tables = result.as_mapping()
        root = tables.get(root_table)
        if root is None:
            raise ValueError(f"Root normalized table `{root_table}` is missing")
        children = self._children_by_parent(result, root_table=root_table)
        rebuilt: list[dict[str, Any]] = []
        for row in root.rows:
            rebuilt.append(self._rebuild_row(row, table_name=root_table, children=children))
        return rebuilt

    def _children_by_parent(
        self,
        result: NormalizationResult,
        *,
        root_table: str,
    ) -> dict[str, list[tuple[str, dict[str, object]]]]:
        grouped: dict[str, list[tuple[str, dict[str, object]]]] = defaultdict(list)
        for table in result.tables:
            if table.name == root_table:
                continue
            for row in table.rows:
                parent_id = row.get("__dpone__parent_row_id")
                if parent_id:
                    grouped[str(parent_id)].append((table.name, row))
        for rows in grouped.values():
            rows.sort(key=lambda item: (self._child_field(item[0]), _list_index(item[1])))
        return grouped

    def _rebuild_row(
        self,
        row: dict[str, object],
        *,
        table_name: str,
        children: dict[str, list[tuple[str, dict[str, object]]]],
    ) -> dict[str, Any]:
        row_id = str(row.get("__dpone__row_id"))
        payload = {key: value for key, value in row.items() if not key.startswith(_TECHNICAL_PREFIX)}
        child_groups: dict[str, list[Any]] = defaultdict(list)
        for child_table, child_row in children.get(row_id, []):
            field = self._child_field(child_table)
            child_payload = self._rebuild_row(child_row, table_name=child_table, children=children)
            if set(child_payload) == {_VALUE_COLUMN}:
                child_groups[field].append(child_payload[_VALUE_COLUMN])
            else:
                child_groups[field].append(child_payload)
        for field, values in child_groups.items():
            payload[field] = (
                values[0]
                if len(values) == 1 and _list_index_from_group(children.get(row_id, []), field) is None
                else values
            )
        return payload

    def _child_field(self, table_name: str) -> str:
        return table_name.split("__")[-1]


def _list_index(row: dict[str, object]) -> int:
    value = row.get("__dpone__list_index")
    return int(value) if value is not None else -1


def _list_index_from_group(rows: list[tuple[str, dict[str, object]]], field: str) -> int | None:
    for table, row in rows:
        if table.split("__")[-1] == field and row.get("__dpone__list_index") is not None:
            return int(row["__dpone__list_index"])
    return None
