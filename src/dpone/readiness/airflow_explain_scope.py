"""Target-scoped view of one immutable Airflow deployment index."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType
from typing import Any

from dpone.readiness.airflow_active_index import (
    MAX_AIRFLOW_EXPLAIN_ARTIFACT_BYTES,
    ActiveAirflowIndexSnapshot,
)
from dpone.readiness.airflow_indexed_artifacts import (
    IndexedAirflowArtifactReadError,
    read_indexed_airflow_artifact,
)


def scope_active_index_snapshot(
    root: Path,
    snapshot: ActiveAirflowIndexSnapshot,
    *,
    pipeline_id: str,
) -> ActiveAirflowIndexSnapshot:
    """Filter a valid deployment index to one DAG/workload identity.

    Missing or invalid indexes remain unchanged so their global integrity error
    is still reported. A valid index that does not contain the requested
    pipeline is projected as a local planned state, without leaking another
    deployment's identity or workload diagnostics.
    """

    index = snapshot.index
    if index is None:
        return snapshot
    dag_specs = tuple(
        item
        for item in _mappings(index.get("dag_specs"))
        if item.get("id") == pipeline_id or pipeline_id in _dag_workload_ids(root, item)
    )
    direct_packs = tuple(item for item in _mappings(index.get("workload_packs")) if item.get("id") == pipeline_id)
    if not dag_specs and not direct_packs:
        return ActiveAirflowIndexSnapshot(source="missing")

    scoped_index = {
        **index,
        "dag_specs": dag_specs,
        "workload_packs": direct_packs,
    }
    return replace(
        snapshot,
        index=MappingProxyType(scoped_index),
    )


def _dag_workload_ids(root: Path, artifact: Mapping[str, Any]) -> set[str]:
    try:
        data = read_indexed_airflow_artifact(
            root,
            artifact,
            max_artifact_bytes=MAX_AIRFLOW_EXPLAIN_ARTIFACT_BYTES,
        )
    except IndexedAirflowArtifactReadError:
        return set()
    try:
        payload = json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return set()
    if not isinstance(payload, Mapping):
        return set()
    return {
        str(node["workload_id"])
        for node in _mappings(payload.get("nodes"))
        if isinstance(node.get("workload_id"), str) and node["workload_id"]
    }


def _mappings(raw: object) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes, bytearray)):
        return ()
    return tuple(item for item in raw if isinstance(item, Mapping))


__all__ = ["scope_active_index_snapshot"]
