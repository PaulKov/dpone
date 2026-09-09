from __future__ import annotations

import json
from pathlib import Path

from dpone.commands.perf_cmd import _render_snapshot_optimization_text


def test_public_schema_exposes_source_materialization_policy() -> None:
    for path in ("src/dpone/schema/etl-config.schema.json", "src/dpone/schema/etl-batch-manifest.schema.json"):
        schema = json.loads(Path(path).read_text(encoding="utf-8"))
        snapshot = schema["definitions"]["native_transfer_snapshot_policy"]["properties"]

        assert snapshot["materialization"]["$ref"] == "#/definitions/native_transfer_source_materialization_policy"

        materialization = schema["definitions"]["native_transfer_source_materialization_policy"]["properties"]
        assert materialization["mode"]["enum"] == ["auto", "off", "required", "benchmark_only"]
        assert materialization["allow_source_writes"]["default"] is False
        assert materialization["work_database"] == {
            "type": ["string", "null"],
            "default": None,
        }
        assert materialization["work_connection_ref"] == {
            "type": ["string", "null"],
            "minLength": 1,
            "default": None,
        }
        assert materialization["cleanup_policy"]["enum"] == ["eager", "on_success", "keep_on_failure"]
        assert (
            materialization["cleanup"]["$ref"] == "#/definitions/native_transfer_source_materialization_cleanup_policy"
        )
        cleanup = schema["definitions"]["native_transfer_source_materialization_cleanup_policy"]["properties"]
        assert cleanup["lock_timeout_ms"]["default"] == 5000
        assert cleanup["defer_on_lock_timeout"]["default"] is True
        assert cleanup["reset_lock_timeout"]["default"] is True
        assert cleanup["retry_attempts"]["default"] == 2
        assert cleanup["retry_backoff_ms"]["default"] == 250
        assert materialization["index"]["$ref"] == "#/definitions/native_transfer_source_materialization_index_policy"


def test_perf_advise_renders_source_materialization_decision() -> None:
    snapshot = {
        "selected_backend": "native_tcp",
        "native_tcp_backend": "direct",
        "compression": "lz4",
        "release_gate": "green",
        "max_parallel_exports": 1,
        "max_parallel_loads": 1,
        "partition_planner": "single_scan_chunks",
        "stats_confidence": "low",
        "source_materialization": {
            "selected": True,
            "provider": "mssql_work_table",
            "release_gate": "green",
            "work_database": "Example_System",
            "work_schema": "dpone_work",
            "cleanup_policy": "eager",
            "measured_speedup_pct": 38.5,
            "reasons": ["source_materialization_selected"],
        },
    }

    assert _render_snapshot_optimization_text(snapshot) == [
        "- native_transfer_snapshot_backend: native_tcp native_tcp_backend=direct compression=lz4 gate=green",
        "- native_transfer_snapshot_parallelism: exports=1 loads=1",
        "- native_transfer_snapshot_partition_planner: single_scan_chunks confidence=low",
        "- native_transfer_source_materialization: mssql_work_table selected=True gate=green work_database=Example_System work_schema=dpone_work cleanup=eager speedup=38.5%",
        "- native_transfer_source_materialization_reason: source_materialization_selected",
    ]
