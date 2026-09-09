from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from dpone.commands.perf_cmd import (
    _render_snapshot_optimization_md,
    _render_snapshot_optimization_text,
    _snapshot_optimization_from_strategy,
)
from dpone.runtime.bulk_options import ClickHouseBulkOptionsResolver
from dpone.runtime.clickhouse_bulk_path import resolve_clickhouse_bulk_path
from dpone.runtime.native_snapshot_optimization import (
    GovernorTelemetry,
    SnapshotOptimizationPolicy,
    SnapshotRouteOptimizer,
    SnapshotRouteRequest,
)
from dpone.strategy_intelligence.native_transfer import NativeTransferPlanBuilder, NativeTransferRequest


def test_snapshot_policy_normalizes_safe_worker_profile_and_governors() -> None:
    policy = SnapshotOptimizationPolicy.from_source_options(
        {
            "native_transfer": {
                "snapshot": {
                    "execution": {
                        "mode": "parallel",
                        "profile": "safe_worker",
                        "max_parallel_exports": 3,
                        "max_parallel_loads": 2,
                    },
                    "source_governor": {"enabled": True, "max_source_cpu_pct": 55},
                    "target_governor": {"enabled": True, "max_inflight_blocks": 2},
                    "tuning": {"packet_size": "auto", "block_rows": "auto", "compression": "auto"},
                }
            }
        }
    )

    assert policy.execution.mode == "parallel"
    assert policy.execution.profile == "safe_worker"
    assert policy.execution.effective_max_parallel_exports == 1
    assert policy.execution.effective_max_parallel_loads == 1
    assert policy.source_governor.max_source_cpu_pct == 55
    assert policy.target_governor.max_inflight_blocks == 2
    assert policy.tuning.compression == "auto"


def test_snapshot_optimizer_prefers_certified_native_tcp_for_columnar_route() -> None:
    decision = SnapshotRouteOptimizer().plan(
        SnapshotRouteRequest(
            source_type="mssql",
            sink_type="clickhouse",
            source_options=_typed_native_source_options(),
            sink_options=_native_tcp_sink_options(),
            runner_policy="release",
            route_certified=True,
            available_ingest_backends=("native_tcp", "client", "http"),
            available_native_tcp_backends=("direct", "client"),
            direct_ingest_certified=True,
        )
    )

    evidence = decision.to_evidence()

    assert decision.selected_backend == "native_tcp"
    assert decision.compression == "lz4"
    assert decision.release_gate == "green"
    assert decision.warnings == ()
    assert evidence["schema_version"] == "dpone.native_transfer.snapshot_optimization.v1"
    assert evidence["selected_backend"] == "native_tcp"
    assert evidence["native_tcp_backend"] == "direct"
    assert evidence["input_format"] == "Native"
    assert evidence["fallback_chain"] == ["native_tcp", "client", "http", "python"]
    assert evidence["partition_planner"] == "statistics"


def test_snapshot_optimizer_falls_back_to_client_with_reason_when_native_tcp_unavailable() -> None:
    decision = SnapshotRouteOptimizer().plan(
        SnapshotRouteRequest(
            source_type="mssql",
            sink_type="clickhouse",
            source_options=_typed_native_source_options(),
            sink_options={"clickhouse_bulk": {"mode": "auto", "ingest_contract": "typed_binary_staging"}},
            runner_policy="dev",
            route_certified=False,
            available_ingest_backends=("client", "http"),
        )
    )

    assert decision.selected_backend == "client"
    assert decision.release_gate == "warning"
    assert "snapshot_native_tcp_backend_unavailable" in decision.reasons
    assert "snapshot_route_uncertified_advisory" in decision.warnings


def test_snapshot_optimizer_blocks_forced_direct_native_tcp_backend_before_source_io() -> None:
    decision = SnapshotRouteOptimizer().plan(
        SnapshotRouteRequest(
            source_type="mssql",
            sink_type="clickhouse",
            source_options=_typed_native_source_options(),
            sink_options=_native_tcp_sink_options(backend="direct"),
            runner_policy="release",
            route_certified=True,
            available_ingest_backends=("native_tcp", "client"),
            available_native_tcp_backends=("client",),
            direct_ingest_certified=False,
        )
    )

    assert decision.selected_backend is None
    assert decision.release_gate == "blocked"
    assert "snapshot_direct_native_tcp_backend_unavailable" in decision.blockers


def test_snapshot_optimizer_blocks_forced_native_tcp_before_source_io_when_not_eligible() -> None:
    decision = SnapshotRouteOptimizer().plan(
        SnapshotRouteRequest(
            source_type="mssql",
            sink_type="clickhouse",
            source_options={
                "native_transfer": {
                    "wire": {"mode": "source_encoded"},
                    "snapshot": {"execution": {"mode": "parallel"}},
                }
            },
            sink_options=_native_tcp_sink_options(),
            runner_policy="release",
            route_certified=True,
            available_ingest_backends=("native_tcp", "client"),
        )
    )

    assert decision.selected_backend is None
    assert decision.release_gate == "blocked"
    assert "snapshot_native_tcp_requires_typed_binary_native_wire" in decision.blockers


def test_snapshot_optimizer_reduces_limits_when_governors_report_pressure() -> None:
    decision = SnapshotRouteOptimizer().plan(
        SnapshotRouteRequest(
            source_type="mssql",
            sink_type="clickhouse",
            source_options=_typed_native_source_options(),
            sink_options=_native_tcp_sink_options(),
            runner_policy="dev",
            route_certified=True,
            available_ingest_backends=("native_tcp", "client", "http"),
            source_telemetry=GovernorTelemetry(cpu_pct=82, duration_seconds=1800),
            target_telemetry=GovernorTelemetry(parts_per_partition=75, active_merges=8),
        )
    )

    assert decision.max_parallel_exports == 1
    assert decision.max_parallel_loads == 1
    assert "snapshot_source_governor_throttled" in decision.reasons
    assert "snapshot_target_merge_pressure_throttled" in decision.reasons


def test_snapshot_optimizer_embeds_source_export_optimizer_decision() -> None:
    source_options = _typed_native_source_options()
    source_options["native_transfer"]["snapshot"]["export_optimizer"] = {
        "mode": "auto",
        "min_speedup_pct": 15,
    }
    source_options["native_transfer"]["snapshot"]["export_optimizer_probes"] = [
        {"provider_id": "mssql_bcp_native", "rows_per_second": 100000, "bytes_per_second": 40000000},
        {"provider_id": "mssql_odbc_array", "rows_per_second": 150000, "bytes_per_second": 48000000},
        {
            "provider_id": "range_partitioned",
            "rows_per_second": 240000,
            "blockers": ["source_heap_range_parallelism_blocked"],
        },
    ]

    decision = SnapshotRouteOptimizer().plan(
        SnapshotRouteRequest(
            source_type="mssql",
            sink_type="clickhouse",
            source_options=source_options,
            sink_options=_native_tcp_sink_options(),
            runner_policy="dev",
            route_certified=True,
            available_ingest_backends=("native_tcp", "client", "http"),
        )
    )

    export_optimizer = decision.to_evidence()["source_export_optimizer"]

    assert export_optimizer["schema_version"] == "dpone.native_transfer.export_optimizer.v1"
    assert export_optimizer["selected_provider"] == "mssql_odbc_array"
    assert export_optimizer["current_default"] == "mssql_bcp_native"
    assert export_optimizer["measured_speedup_pct"] == 50.0
    assert export_optimizer["rejected"]["range_partitioned"] == "source_heap_range_parallelism_blocked"


def test_snapshot_optimizer_blocks_when_required_export_provider_is_missing() -> None:
    source_options = _typed_native_source_options()
    source_options["native_transfer"]["snapshot"]["export_optimizer"] = {"mode": "required"}

    decision = SnapshotRouteOptimizer().plan(
        SnapshotRouteRequest(
            source_type="mssql",
            sink_type="clickhouse",
            source_options=source_options,
            sink_options=_native_tcp_sink_options(),
            runner_policy="release",
            route_certified=True,
            available_ingest_backends=("native_tcp", "client", "http"),
        )
    )

    assert decision.selected_backend is None
    assert decision.release_gate == "blocked"
    assert "export_optimizer_no_safe_provider" in decision.blockers
    assert decision.to_evidence()["source_export_optimizer"]["release_gate"] == "blocked"


def test_clickhouse_bulk_options_accept_native_tcp_and_path_resolver() -> None:
    options = ClickHouseBulkOptionsResolver.resolve(_native_tcp_sink_options())

    assert options.mode == "native_tcp"
    assert options.native_tcp.enabled is True
    assert options.native_tcp.port == 9000
    assert options.native_tcp.compression == "auto"
    assert options.native_tcp.backend == "auto"
    assert options.native_tcp.connection_pool_size == 2
    assert options.native_tcp.query_timeout_seconds == 3600
    assert resolve_clickhouse_bulk_path(_native_tcp_sink_options()) == "native_tcp"


def test_strategy_plan_surfaces_snapshot_optimization_evidence() -> None:
    plan = NativeTransferPlanBuilder().build(
        NativeTransferRequest(
            source_type="mssql",
            sink_type="clickhouse",
            source_table="dbo.orders",
            target_table="landing.orders",
            strategy="full_refresh",
            source_options=_typed_native_source_options(),
            sink_options=_native_tcp_sink_options(),
        )
    )

    snapshot = plan.native_ingest_settings["snapshot_optimization"]

    assert snapshot["schema_version"] == "dpone.native_transfer.snapshot_optimization.v1"
    assert snapshot["selected_backend"] == "native_tcp"
    assert snapshot["input_format"] == "Native"
    assert snapshot["compression"] == "lz4"
    assert "native_tcp" in snapshot["fallback_chain"]


def test_strategy_plan_uses_installed_direct_ingest_provider(monkeypatch) -> None:
    from dpone.runtime import direct_ingest

    provider = SimpleNamespace(
        direct_ingest_capabilities=lambda: {
            "backends": [
                {
                    "backend_id": "clickhouse_native_tcp_direct",
                    "input_format": "Native",
                    "certified": True,
                }
            ],
        },
        insert_clickhouse_native=lambda request: {"rows": 0},
    )

    def fake_import_module(name: str):
        if name == "dpone_native_accel":
            return provider
        raise ImportError(name)

    monkeypatch.setattr(direct_ingest.importlib, "import_module", fake_import_module)

    plan = NativeTransferPlanBuilder().build(
        NativeTransferRequest(
            source_type="mssql",
            sink_type="clickhouse",
            source_table="dbo.orders",
            target_table="landing.orders",
            strategy="full_refresh",
            source_options=_typed_native_source_options(),
            sink_options=_native_tcp_sink_options(),
        )
    )

    snapshot = plan.native_ingest_settings["snapshot_optimization"]

    assert snapshot["selected_backend"] == "native_tcp"
    assert snapshot["native_tcp_backend"] == "direct"
    assert snapshot["fallback_reason"] is None


def test_public_schema_exposes_snapshot_optimization_and_native_tcp_policy() -> None:
    for path in ("src/dpone/schema/etl-config.schema.json", "src/dpone/schema/etl-batch-manifest.schema.json"):
        schema = json.loads(Path(path).read_text(encoding="utf-8"))

        native_transfer = schema["definitions"]["native_transfer"]["properties"]
        root_properties = schema.get("properties", {})
        process_properties = schema["definitions"].get("process_fragment", {}).get("properties", {})
        sink = root_properties.get("sink") or process_properties["sink"]
        sink_options = sink["properties"]["options"]["properties"]
        clickhouse_bulk = sink_options["clickhouse_bulk"]["properties"]

        assert native_transfer["snapshot"]["$ref"] == "#/definitions/native_transfer_snapshot_policy"
        snapshot = schema["definitions"]["native_transfer_snapshot_policy"]["properties"]
        assert snapshot["scan"]["$ref"] == "#/definitions/native_transfer_snapshot_scan_policy"
        assert snapshot["physical_chunking"]["$ref"] == "#/definitions/native_transfer_physical_chunking_policy"
        scan = schema["definitions"]["native_transfer_snapshot_scan_policy"]["properties"]
        assert scan["mode"]["enum"] == ["auto", "single_scan", "range_partitioned"]
        assert scan["heap_policy"]["enum"] == ["single_scan_chunks", "range_if_indexed", "fail_fast"]
        chunking = schema["definitions"]["native_transfer_physical_chunking_policy"]["properties"]
        assert chunking["spool_mode"]["enum"] == ["file", "fifo"]
        assert chunking["cleanup_policy"]["default"] == "eager"
        optimizer = schema["definitions"]["native_transfer_export_optimizer_policy"]["properties"]
        assert snapshot["export_optimizer"]["$ref"] == "#/definitions/native_transfer_export_optimizer_policy"
        assert optimizer["mode"]["enum"] == ["auto", "off", "required", "benchmark_only"]
        assert optimizer["probe_rows"]["default"] == 100000
        assert optimizer["min_speedup_pct"]["default"] == 15
        assert clickhouse_bulk["native_tcp"]["$ref"] == "#/definitions/clickhouse_native_tcp_options"
        assert "native_tcp" in clickhouse_bulk["mode"]["description"]
        native_tcp = schema["definitions"]["clickhouse_native_tcp_options"]["properties"]
        assert native_tcp["backend"]["enum"] == ["auto", "direct", "client"]
        assert native_tcp["connection_pool_size"]["default"] == 2


def test_perf_advise_renders_snapshot_optimizer_decision() -> None:
    payload = _strategy_payload_with_snapshot()
    snapshot = _snapshot_optimization_from_strategy(payload)

    assert snapshot["selected_backend"] == "native_tcp"
    assert _render_snapshot_optimization_text(snapshot) == [
        "- native_transfer_snapshot_backend: native_tcp native_tcp_backend=direct compression=lz4 gate=green",
        "- native_transfer_snapshot_parallelism: exports=4 loads=2",
        "- native_transfer_snapshot_partition_planner: statistics confidence=high",
    ]
    assert _render_snapshot_optimization_md(snapshot) == [
        "- native_transfer_snapshot_backend: `native_tcp` native_tcp_backend=`direct` compression=`lz4` gate=`green`",
        "- native_transfer_snapshot_parallelism: exports=`4` loads=`2`",
        "- native_transfer_snapshot_partition_planner: `statistics` confidence=`high`",
    ]


def test_perf_advise_renders_source_scan_decision() -> None:
    snapshot = {
        "selected_backend": "native_tcp",
        "native_tcp_backend": "direct",
        "compression": "lz4",
        "release_gate": "green",
        "max_parallel_exports": 1,
        "max_parallel_loads": 1,
        "partition_planner": "single_scan_chunks",
        "stats_confidence": "low",
        "source_scan": {
            "selected_scan": "single_scan_chunks",
            "table_kind": "heap",
            "physical_chunking": {"enabled": True, "target_chunk_bytes": 67108864},
            "warnings": ["source_heap_range_parallelism_blocked"],
        },
    }

    assert _render_snapshot_optimization_text(snapshot) == [
        "- native_transfer_snapshot_backend: native_tcp native_tcp_backend=direct compression=lz4 gate=green",
        "- native_transfer_snapshot_parallelism: exports=1 loads=1",
        "- native_transfer_snapshot_partition_planner: single_scan_chunks confidence=low",
        "- native_transfer_source_scan: single_scan_chunks table=heap chunking=True target_chunk_bytes=67108864",
        "- native_transfer_source_scan_warning: source_heap_range_parallelism_blocked",
    ]


def test_perf_advise_renders_source_export_optimizer_decision() -> None:
    snapshot = {
        "selected_backend": "native_tcp",
        "native_tcp_backend": "direct",
        "compression": "lz4",
        "release_gate": "green",
        "max_parallel_exports": 1,
        "max_parallel_loads": 1,
        "partition_planner": "single_scan_chunks",
        "stats_confidence": "low",
        "source_export_optimizer": {
            "selected_provider": "mssql_odbc_array",
            "current_default": "mssql_bcp_native",
            "measured_speedup_pct": 42.0,
            "source_bottleneck": "export",
            "rejected": {"range_partitioned": "source_heap_range_parallelism_blocked"},
        },
    }

    assert _render_snapshot_optimization_text(snapshot) == [
        "- native_transfer_snapshot_backend: native_tcp native_tcp_backend=direct compression=lz4 gate=green",
        "- native_transfer_snapshot_parallelism: exports=1 loads=1",
        "- native_transfer_snapshot_partition_planner: single_scan_chunks confidence=low",
        "- native_transfer_source_export_optimizer: mssql_odbc_array default=mssql_bcp_native speedup=42.0% bottleneck=export",
        "- native_transfer_source_export_rejected: range_partitioned=source_heap_range_parallelism_blocked",
    ]


def _typed_native_source_options() -> dict:
    return {
        "columns": [{"name": "id", "type": "int"}],
        "native_transfer": {
            "wire": {
                "mode": "typed_binary",
                "source_native_format": "bcp_native",
                "binary_format": "native",
                "acceleration": {"mode": "auto"},
            },
            "snapshot": {
                "execution": {
                    "mode": "auto",
                    "profile": "balanced",
                    "max_parallel_exports": 4,
                    "max_parallel_loads": 2,
                },
                "source_governor": {"enabled": True, "max_source_cpu_pct": 60},
                "target_governor": {"enabled": True, "max_inflight_blocks": 4},
                "tuning": {"packet_size": "auto", "block_rows": "auto", "compression": "auto"},
            },
        },
    }


def _native_tcp_sink_options(*, backend: str = "auto") -> dict:
    return {
        "clickhouse_bulk": {
            "mode": "native_tcp",
            "ingest_contract": "typed_binary_staging",
            "native_tcp": {
                "enabled": True,
                "backend": backend,
                "compression": "auto",
                "port": 9000,
                "secure": False,
                "connection_pool_size": 2,
                "query_timeout_seconds": 3600,
            },
        }
    }


def _strategy_payload_with_snapshot() -> dict:
    return {
        "decision": {
            "native_transfer_plan": {
                "native_ingest_settings": {
                    "snapshot_optimization": {
                        "selected_backend": "native_tcp",
                        "native_tcp_backend": "direct",
                        "partition_planner": "statistics",
                        "stats_confidence": "high",
                        "compression": "lz4",
                        "release_gate": "green",
                        "max_parallel_exports": 4,
                        "max_parallel_loads": 2,
                    }
                }
            }
        }
    }
