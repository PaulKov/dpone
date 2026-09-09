from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.runtime.bulk_wire import BulkWirePlanner
from dpone.runtime.export_optimizer_models import ExportOptimizerPolicy, parse_export_probes
from dpone.runtime.native_snapshot_models import SnapshotRouteRequest
from dpone.runtime.source_export_optimizer import SourceExportOptimizer
from dpone.runtime.source_materialization import SourceMaterializationPlanner, SourceMaterializationPolicy
from dpone.runtime.source_scan import SourceScanPlanner, SourceShape


def is_typed_native_wire(request: SnapshotRouteRequest) -> bool:
    if request.source_type.lower() != "mssql" or request.sink_type.lower() != "clickhouse":
        return False
    contract = BulkWirePlanner().plan(
        source_type=request.source_type,
        sink_type=request.sink_type,
        schema=_schema_from_options(request.source_options),
        source_options=request.source_options,
        sink_options=request.sink_options,
    )
    return contract.selected_route == "typed_binary_bcp_native" and contract.input_format == "Native"


def source_scan_evidence(source_options: Mapping[str, Any]) -> dict[str, Any] | None:
    snapshot = snapshot_mapping(source_options)
    if "scan" not in snapshot and "physical_chunking" not in snapshot:
        return None
    raw_shape = snapshot.get("source_shape")
    shape = raw_shape if isinstance(raw_shape, Mapping) else {}
    decision = SourceScanPlanner().plan(
        source_options=source_options,
        source_shape=SourceShape(
            table_kind=str(shape.get("table_kind") or "unknown"),
            has_seekable_boundary=_bool(shape.get("has_seekable_boundary"), False),
            stats_confidence=str(shape.get("stats_confidence") or "unknown"),
        ),
    )
    return decision.to_evidence()


def export_optimizer_evidence(request: SnapshotRouteRequest) -> dict[str, Any] | None:
    snapshot = snapshot_mapping(request.source_options)
    configured = "export_optimizer" in snapshot
    probes = parse_export_probes(snapshot)
    if not configured and not probes:
        return None
    decision = SourceExportOptimizer().decide(
        ExportOptimizerPolicy.from_source_options(request.source_options),
        current_default=current_export_default(request),
        probes=probes,
        cache_key=None,
    )
    return decision.to_evidence()


def source_materialization_evidence(
    request: SnapshotRouteRequest,
    source_scan: dict[str, Any] | None,
) -> dict[str, Any] | None:
    snapshot = snapshot_mapping(request.source_options)
    if "materialization" not in snapshot:
        return None
    policy = SourceMaterializationPolicy.from_source_options(request.source_options)
    decision = SourceMaterializationPlanner().plan(
        policy,
        source_shape=source_shape_from_scan(source_scan),
        provider_available=policy.provider in {"auto", "mssql_work_table"},
        permissions_ok=True,
    )
    return decision.to_evidence()


def export_optimizer_blocked(evidence: dict[str, Any] | None) -> bool:
    return bool(evidence and evidence.get("release_gate") == "blocked")


def current_export_default(request: SnapshotRouteRequest) -> str:
    source = request.source_type.lower()
    sink = request.sink_type.lower()
    if source == "mssql" and sink == "clickhouse":
        contract = BulkWirePlanner().plan(
            source_type=request.source_type,
            sink_type=request.sink_type,
            schema=_schema_from_options(request.source_options),
            source_options=request.source_options,
            sink_options=request.sink_options,
        )
        if contract.selected_route == "typed_binary_bcp_native":
            return "mssql_bcp_native"
        if contract.selected_route == "typed_binary_row_stream":
            return "mssql_driver_rowset"
        if contract.selected_route == "typed_raw_direct":
            return "mssql_bcp_character_raw"
    return str(request.source_options.get("extract_mode") or "single_scan_chunks").strip().lower()


def source_shape_from_scan(source_scan: dict[str, Any] | None) -> SourceShape:
    if not source_scan:
        return SourceShape()
    return SourceShape(
        table_kind=str(source_scan.get("table_kind") or "unknown"),
        has_seekable_boundary=False,
        stats_confidence=str(source_scan.get("stats_confidence") or "unknown"),
    )


def snapshot_mapping(source_options: Mapping[str, Any]) -> Mapping[str, Any]:
    native = source_options.get("native_transfer")
    if not isinstance(native, Mapping):
        return {}
    snapshot = native.get("snapshot")
    return snapshot if isinstance(snapshot, Mapping) else {}


def _schema_from_options(source_options: Mapping[str, Any]) -> list[tuple[str, str]]:
    columns = source_options.get("columns")
    if isinstance(columns, Mapping):
        return [(str(name), str(dtype)) for name, dtype in columns.items()]
    if isinstance(columns, list):
        return [
            (str(item["name"]), str(item.get("type", item.get("dtype", "string"))))
            for item in columns
            if isinstance(item, Mapping) and "name" in item
        ]
    return []


def _bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}
