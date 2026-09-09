"""Table building helpers for nested normalization."""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Mapping, Sequence
from datetime import date, datetime

from dpone.runtime.lineage.identity import LineageIdentityService
from dpone.runtime.normalization.explosion import ExplosionGuard
from dpone.runtime.normalization.models import NormalizedTable
from dpone.runtime.normalization.options import NestedNormalizationOptions

_TECHNICAL_TYPES = {
    "__dpone__load_id": "string",
    "__dpone__row_id": "string",
    "__dpone__parent_row_id": "string",
    "__dpone__root_row_id": "string",
    "__dpone__list_index": "integer",
    "__dpone__loaded_at": "timestamp",
    "__dpone__extracted_at": "timestamp",
}


class NormalizedTableBuilder:
    """Append rows and render normalized table models."""

    def __init__(self, identity_service: LineageIdentityService) -> None:
        self._identity_service = identity_service

    def append_raw_row(
        self,
        builders: OrderedDict[str, list[dict[str, object]]],
        *,
        root_table: str,
        source_row: Mapping[str, object],
        root_row_id: str,
        load_id: str,
        loaded_at: str,
        guard: ExplosionGuard,
        options: NestedNormalizationOptions,
    ) -> None:
        table = f"{root_table}{options.raw_landing.table_suffix}"
        self.append_row(
            builders,
            table=table,
            row={options.raw_landing.payload_column: dict(source_row)},
            row_id=self._identity_service.row_id(
                source_type="nested",
                source_schema=root_table,
                source_table=table,
                row=dict(source_row),
                row_path=f"{root_row_id}:$.__raw",
            ),
            parent_row_id=None,
            root_row_id=root_row_id,
            list_index=None,
            load_id=load_id,
            loaded_at=loaded_at,
            guard=guard,
        )

    def append_quarantine(
        self,
        builders: OrderedDict[str, list[dict[str, object]]],
        *,
        root_table: str,
        path: str,
        payload: object,
        parent_row_id: str,
        root_row_id: str,
        load_id: str,
        loaded_at: str,
        guard: ExplosionGuard,
    ) -> None:
        table = f"{root_table}__quarantine"
        row = {"path": path, "payload": payload, "reason": "path_policy_quarantine"}
        self.append_row(
            builders,
            table=table,
            row=row,
            row_id=self._identity_service.row_id(
                source_type="nested",
                source_schema=root_table,
                source_table=table,
                row=row,
                row_path=f"{parent_row_id}:$.quarantine.{path}",
            ),
            parent_row_id=parent_row_id,
            root_row_id=root_row_id,
            list_index=None,
            load_id=load_id,
            loaded_at=loaded_at,
            guard=guard,
        )

    def append_row(
        self,
        builders: OrderedDict[str, list[dict[str, object]]],
        *,
        table: str,
        row: Mapping[str, object],
        row_id: str,
        parent_row_id: str | None,
        root_row_id: str,
        list_index: int | None,
        load_id: str,
        loaded_at: str,
        guard: ExplosionGuard,
    ) -> None:
        output = dict(row)
        output["__dpone__load_id"] = load_id
        output["__dpone__loaded_at"] = loaded_at
        output["__dpone__row_id"] = row_id
        output["__dpone__extracted_at"] = loaded_at
        output["__dpone__parent_row_id"] = parent_row_id
        output["__dpone__root_row_id"] = root_row_id
        output["__dpone__list_index"] = list_index
        builders.setdefault(table, []).append(output)
        guard.record_row(root_row_id=root_row_id, table=table)

    def build_table(self, name: str, rows: list[dict[str, object]]) -> NormalizedTable:
        return NormalizedTable(name=name, schema=tuple(infer_schema(rows)), rows=tuple(rows))


def infer_schema(rows: Sequence[Mapping[str, object]]) -> list[tuple[str, str]]:
    columns: OrderedDict[str, str] = OrderedDict()
    for row in rows:
        for name, value in row.items():
            inferred = _logical_type(str(name), value)
            if name not in columns:
                columns[str(name)] = inferred
            elif columns[str(name)] != inferred and inferred != "null":
                columns[str(name)] = _merge_type(columns[str(name)], inferred)
    return list(columns.items())


def _logical_type(name: str, value: object) -> str:
    if name in _TECHNICAL_TYPES:
        return _TECHNICAL_TYPES[name]
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, datetime):
        return "timestamp"
    if isinstance(value, date):
        return "date"
    if isinstance(value, bytes):
        return "bytes"
    if isinstance(value, (dict, list)):
        return "json"
    return "string"


def _merge_type(left: str, right: str) -> str:
    if left == "null":
        return right
    if left == right:
        return left
    if {left, right} <= {"integer", "number"}:
        return "number"
    return "string"
