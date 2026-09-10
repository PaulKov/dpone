"""Deterministic authoring identity, including optional workload resources."""

from __future__ import annotations

import copy
from collections.abc import Mapping, Sequence
from typing import Any


def authoring_semantic_material(
    metadata: object, processes: Sequence[Mapping[str, Any]], resources: Mapping[str, Any] | None
) -> dict[str, Any]:
    """Bind resource-only changes to selection while preserving absent defaults."""
    material = {"metadata": _semantic_metadata(metadata), "processes": sorted(processes, key=_process_identity)}
    if resources is not None:
        material["airflow_resources"] = resources
    return material


def _semantic_metadata(raw: object) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        return {}
    metadata = {str(key): copy.deepcopy(value) for key, value in raw.items() if str(key) != "recipe"}
    tags = metadata.get("tags")
    if isinstance(tags, list) and all(isinstance(tag, str) for tag in tags):
        metadata["tags"] = sorted(set(tags))
    return metadata


def _process_identity(process: Mapping[str, Any]) -> tuple[str, str, str]:
    source = process.get("source")
    table = source.get("table") if isinstance(source, Mapping) else None
    schema_name = str(table.get("schema") or "") if isinstance(table, Mapping) else ""
    table_name = str(table.get("name") or "") if isinstance(table, Mapping) else ""
    return str(process.get("name") or ""), schema_name, table_name
