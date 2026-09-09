from __future__ import annotations

from typing import Any


def has_canonical_load_workers(source_options: dict[str, Any]) -> bool:
    nested = source_options.get("partitioning")
    return isinstance(nested, dict) and nested.get("load_workers") is not None


def merge_options(*options: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for option in options:
        merged.update(option)
    return merged


def schema_from_options(source_options: dict[str, Any]) -> list[tuple[str, str]]:
    columns = source_options.get("columns")
    if isinstance(columns, dict):
        return [(str(name), str(dtype)) for name, dtype in columns.items()]
    if isinstance(columns, list):
        return [
            (str(item["name"]), str(item.get("type", item.get("dtype", "string"))))
            for item in columns
            if isinstance(item, dict) and "name" in item
        ]
    return []


def default_merge_policy(sink: str) -> str:
    if sink == "clickhouse":
        return "lightweight_delete_insert"
    if sink == "kafka":
        return "event_upsert"
    return "delete_insert"
