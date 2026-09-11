"""Closed route classification for complete composition admission.

Recognizing a cell is necessary, never sufficient: installed backend admission
must verify its options, every physical effect, enrollment and session fencing.
No classification result is live certification or permission to execute SQL.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.contracts.composition_activation import (
    CompositionAdmissionError,
    CompositionWorkloadAdmission,
)
from dpone.contracts.composition_sources import CompositionSourceSnapshot
from dpone.contracts.dbt_relation_writes import DbtRelationWrite
from dpone.contracts.dbt_workspace_activation import dbt_relation_write_subject


def composition_transfer_cell(manifest: Mapping[str, Any]) -> str:
    """Classify one bounded table/full-refresh declaration, rejecting side effects."""
    if set(manifest) - {"name", "description", "source", "sink", "state"}:
        raise CompositionAdmissionError("transfer_manifest_capability")
    source, sink = manifest.get("source"), manifest.get("sink")
    if not isinstance(source, Mapping) or not isinstance(sink, Mapping):
        raise CompositionAdmissionError("transfer_endpoints")
    if set(source) - {"type", "connection_ref", "table", "options"}:
        raise CompositionAdmissionError("source_effects")
    if set(sink) - {"type", "connection_ref", "table", "staging", "strategy", "options"}:
        raise CompositionAdmissionError("sink_effects")
    for endpoint in (source, sink):
        _require_table_endpoint(endpoint)
    if sink.get("strategy") != {"mode": "full_refresh"}:
        raise CompositionAdmissionError("transfer_strategy")
    route = source.get("type"), sink.get("type")
    if route == ("postgres", "mssql"):
        state = manifest.get("state")
        if (
            not isinstance(state, Mapping)
            or state.get("type") != "mssql"
            or state.get("atomicity") != "target_atomic"
            or state.get("provisioning") != "external"
            or not isinstance(state.get("connection_ref"), str)
            or not state["connection_ref"].strip()
        ):
            raise CompositionAdmissionError("external_target_atomic_state_required")
        return "postgres_mssql_full_refresh_v1"
    raise CompositionAdmissionError("transfer_route_capability")


def composition_generated_transfer_cell(manifest: Mapping[str, Any]) -> str:
    """Classify original native-producer bytes, retaining their metadata/options.

    The native source verifier establishes the producer closure before this
    function runs. Runtime, quality and GitOps projections are not ordinary
    authoring escape hatches; backend admission must check their exact effects.
    No fields are removed or rewritten to imitate an ordinary transfer.
    """
    if set(manifest) - {"name", "description", "runtime", "source", "sink", "quality", "gitops", "state"}:
        raise CompositionAdmissionError("generated_manifest_capability")
    source, sink = manifest.get("source"), manifest.get("sink")
    if not isinstance(source, Mapping) or not isinstance(sink, Mapping):
        raise CompositionAdmissionError("transfer_endpoints")
    if set(source) - {"type", "connection_ref", "table", "options"}:
        raise CompositionAdmissionError("source_effects")
    if set(sink) - {"type", "connection_ref", "table", "mode", "strategy", "options", "staging"}:
        raise CompositionAdmissionError("sink_effects")
    if (source.get("type"), sink.get("type")) != ("mssql", "clickhouse"):
        raise CompositionAdmissionError("generated_transfer_route_capability")
    for endpoint in (source, sink):
        _require_table_endpoint(endpoint)
    strategy = sink.get("strategy")
    if (
        not isinstance(strategy, Mapping)
        or set(strategy) != {"mode", "max_source_bytes"}
        or strategy.get("mode") != "full_refresh"
        or type(strategy.get("max_source_bytes")) is not int
        or strategy["max_source_bytes"] <= 0
        or sink.get("mode") != "replace"
    ):
        raise CompositionAdmissionError("bounded_generated_full_refresh_required")
    if manifest.get("state"):
        raise CompositionAdmissionError("generated_state_effects_unavailable")
    for field in ("runtime", "quality", "gitops"):
        if not isinstance(manifest.get(field), Mapping):
            raise CompositionAdmissionError("generated_metadata")
    return "mssql_clickhouse_full_refresh_v1"


def _require_table_endpoint(endpoint: Mapping[str, Any]) -> None:
    table = endpoint.get("table")
    if (
        not isinstance(endpoint.get("connection_ref"), str)
        or not endpoint["connection_ref"].strip()
        or not isinstance(table, Mapping)
        or set(table) - {"database", "schema", "name"}
        or not isinstance(table.get("name"), str)
        or not table["name"].strip()
        or any(not isinstance(value, str) or not value.strip() or "\x00" in value for value in table.values())
    ):
        raise CompositionAdmissionError("table_source_required")


@dataclass(frozen=True, slots=True)
class CompositionExecutionPlan:
    """Full workload partition paired with exact source-derived relation writes."""

    sources: CompositionSourceSnapshot
    workloads: tuple[CompositionWorkloadAdmission, ...]
    writes: tuple[DbtRelationWrite, ...]

    def __post_init__(self) -> None:
        self.sources.__post_init__()
        if tuple((row.workload_id, row.pack_sha256) for row in self.workloads) != self.sources.workload_pins:
            raise CompositionAdmissionError("complete_workload_membership")
        expected = sorted(dbt_relation_write_subject(write) for write in self.sources.relation_writes)
        observed = sorted(subject for row in self.workloads for subject in row.write_subjects)
        if expected != observed or self.writes != self.sources.relation_writes:
            raise CompositionAdmissionError("complete_write_membership")
        for workload in self.workloads:
            workload.__post_init__()

    def require_installed_cells(self, cells: frozenset[str]) -> None:
        """Reject the whole plan if even one native/generated/ordinary cell is absent."""
        if any(workload.execution_cell not in cells for workload in self.workloads):
            raise CompositionAdmissionError("complete_execution_capability_unavailable")
