from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.readiness.managed_utils import _source_columns
from dpone.readiness.native_snapshot_planning import build_native_transfer_snapshot_optimization
from dpone.runtime.bulk_wire import BulkWirePlanner
from dpone.runtime.columnar_fast_path_planner import ColumnarFastPathPlanner
from dpone.runtime.columnar_parquet_writer import PyArrowParquetChunkWriter, mssql_source_type_supported
from dpone.runtime.native_transfer_capabilities import NativeTransferCapabilityPlanner
from dpone.runtime.native_transfer_execution import NativeTransferExecutionPolicy
from dpone.runtime.native_transfer_route_planner import NativeTransferRoutePlanner


def native_transfer_execution(raw: Mapping[str, Any]) -> dict[str, Any]:
    execution = _native_execution(_source_options(raw))
    return NativeTransferExecutionPolicy.from_mapping(execution if isinstance(execution, dict) else {}).to_dict()


def native_transfer_transport(raw: Mapping[str, Any], *, source_type: str, sink_type: str) -> dict[str, Any]:
    source_options = _source_options(raw)
    sink_options = _sink_options(raw)
    execution = _native_execution(source_options)
    policy = NativeTransferExecutionPolicy.from_mapping(execution if isinstance(execution, dict) else {})
    return (
        NativeTransferCapabilityPlanner()
        .plan(
            source_type=source_type,
            sink_type=sink_type,
            source_options=source_options,
            sink_options=sink_options,
            transport=policy.transport,
        )
        .to_dict()
    )


def native_transfer_bulk_wire(raw: Mapping[str, Any], *, source_type: str, sink_type: str) -> dict[str, Any]:
    source_options = _source_options(raw)
    sink_options = _sink_options(raw)
    source_schema = _source_columns(source_options)
    return (
        BulkWirePlanner()
        .plan(
            source_type=source_type,
            sink_type=sink_type,
            schema=tuple(source_schema),
            source_options=source_options,
            sink_options=sink_options,
        )
        .to_evidence()
    )


def native_transfer_snapshot_optimization(
    raw: Mapping[str, Any],
    *,
    source_type: str,
    sink_type: str,
) -> dict[str, Any]:
    return build_native_transfer_snapshot_optimization(raw, source_type=source_type, sink_type=sink_type)


def columnar_fast_path_plan(
    raw: Mapping[str, Any],
    *,
    source_type: str,
    sink_type: str,
) -> dict[str, Any]:
    """Expose configured columnar intent without pretending a live preflight ran."""

    source_options = _source_options(raw)
    native_transfer = source_options.get("native_transfer")
    snapshot = native_transfer.get("snapshot") if isinstance(native_transfer, Mapping) else None
    options = snapshot.get("columnar_fast_path") if isinstance(snapshot, Mapping) else None
    if not isinstance(options, Mapping):
        return {}
    source_schema = _source_columns(source_options)
    decision = ColumnarFastPathPlanner().decide(
        columnar_options=dict(options),
        preflight=None,
        schema_supported=all(mssql_source_type_supported(declared_type) for _, declared_type in source_schema),
        parquet_writer_available=PyArrowParquetChunkWriter().is_available(),
        clickhouse_pull_available=source_type == "mssql" and sink_type == "clickhouse",
        current_provider="mssql_bcp_queryout_to_clickhouse_direct_tsv",
    )
    return decision.to_evidence()


def requests_object_storage_pull(raw: Mapping[str, Any]) -> bool:
    source_options = _source_options(raw)
    native_transfer = source_options.get("native_transfer")
    snapshot = native_transfer.get("snapshot") if isinstance(native_transfer, Mapping) else None
    options = snapshot.get("columnar_fast_path") if isinstance(snapshot, Mapping) else None
    if not isinstance(options, Mapping):
        return False
    mode = str(options.get("mode") or "auto").strip().lower()
    provider = str(options.get("provider") or "auto").strip().lower()
    return mode not in {"off", "benchmark_only"} and provider == "object_storage_pull"


def native_transfer_route_decision(
    raw: Mapping[str, Any],
    *,
    source_type: str,
    sink_type: str,
    manifest_dir: Path,
    certification_mode_override: str | None = None,
) -> dict[str, Any]:
    source_options = _source_options(raw)
    sink = raw.get("sink", {}) if isinstance(raw.get("sink"), Mapping) else {}
    sink_options = _sink_options(raw)
    execution = _native_execution(source_options)
    policy = NativeTransferExecutionPolicy.from_mapping(execution if isinstance(execution, dict) else {})
    strategy = sink.get("strategy", {}) if isinstance(sink, Mapping) else {}
    certification = policy.certification
    if certification_mode_override:
        certification = type(certification)(mode=certification_mode_override, artifact=None)
    elif certification.artifact and not Path(certification.artifact).is_absolute():
        certification = type(certification)(
            mode=certification.mode,
            artifact=str((manifest_dir / certification.artifact).resolve()),
        )
    return (
        NativeTransferRoutePlanner()
        .plan(
            source_type=source_type,
            sink_type=sink_type,
            strategy=str(strategy.get("mode", "full_refresh")) if isinstance(strategy, Mapping) else "full_refresh",
            source_options=source_options,
            sink_options=sink_options,
            execution_policy=policy,
            certification_policy=certification,
        )
        .to_evidence()
    )


def _source_options(raw: Mapping[str, Any]) -> dict[str, Any]:
    source = raw.get("source", {}) if isinstance(raw.get("source"), Mapping) else {}
    options = source.get("options", {}) if isinstance(source.get("options"), Mapping) else {}
    return dict(options) if isinstance(options, Mapping) else {}


def _sink_options(raw: Mapping[str, Any]) -> dict[str, Any]:
    sink = raw.get("sink", {}) if isinstance(raw.get("sink"), Mapping) else {}
    options = sink.get("options", {}) if isinstance(sink.get("options"), Mapping) else {}
    return dict(options) if isinstance(options, Mapping) else {}


def _native_execution(source_options: Mapping[str, Any]) -> Mapping[str, Any]:
    native_transfer = source_options.get("native_transfer") if isinstance(source_options, Mapping) else {}
    execution = native_transfer.get("execution") if isinstance(native_transfer, Mapping) else {}
    return execution if isinstance(execution, Mapping) else {}


__all__ = [
    "columnar_fast_path_plan",
    "native_transfer_bulk_wire",
    "native_transfer_execution",
    "native_transfer_route_decision",
    "native_transfer_snapshot_optimization",
    "native_transfer_transport",
    "requests_object_storage_pull",
]
