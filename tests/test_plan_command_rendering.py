from __future__ import annotations

from dpone.commands.plan_cmd import _render_md, _render_text


def _payload() -> dict:
    return {
        "process": "orders",
        "source": {"type": "postgres", "table": "public.orders"},
        "sink": {"type": "mssql", "table": "dbo.orders"},
        "strategy": {"mode": "incremental_merge"},
        "bulk_path": "postgres_copy_to_mssql_bcp",
        "staging": {"staging_first": True},
        "schema_evolution": {"enabled": True},
        "type_inference": {"options": {"enabled": True}},
        "physical_design": {"options": {"enabled": True}},
        "runtime_storage": {
            "profile": "object_backed",
            "work_dir": "/mnt/dpone-work",
            "evidence_dir": ".dpone/runs",
            "checkpoint_dir": ".dpone/state",
            "debug_dir": ".dpone/debug",
            "min_free_bytes": 1024,
            "transfer_store": {
                "type": "s3",
                "uri": "s3://dpone-stage/native-transfer",
                "cleanup": {"temp_objects": "on_success"},
            },
        },
        "native_transfer_execution": {
            "mode": "auto",
            "profile": "balanced",
            "cleanup_policy": "eager",
            "resume_policy": "object_store_if_verified",
            "transport": {
                "mode": "auto",
                "prefer_streaming": True,
                "fallback_to_file": True,
                "stream_buffer_bytes": 16777216,
                "checksum": "rolling",
                "max_stream_seconds": 3600,
            },
            "resource_policy": {
                "max_active_files": 2,
                "max_active_bytes": 536870912,
                "target_file_bytes": 134217728,
                "max_file_bytes": 268435456,
                "adaptive_sizing": True,
            },
        },
        "native_transfer_transport": {
            "transport": "file",
            "fallback_allowed": True,
            "fallback_reason": "native_transfer_stream_fallback_file_only_source",
            "eligibility": {
                "source": False,
                "sink": True,
                "codec": True,
                "reasons": ["bcp_queryout_is_file_transport"],
            },
        },
        "native_transfer_route_decision": {
            "schema_version": "dpone.native_transfer.route_decision.v1",
            "route": {"source": "postgres", "sink": "mssql", "strategy": "incremental_merge"},
            "requested_transport": "auto",
            "selected_transport": "file",
            "certification_mode": "advisory",
            "certification_status": "uncertified",
            "release_gate": "warning",
            "fallback_chain": ["stream", "object", "file"],
            "blockers": [],
            "warnings": ["native_transfer_route_uncertified_advisory"],
            "reasons": ["native_transfer_stream_fallback_file_only_source"],
            "matrix": {
                "candidates": [
                    {
                        "transport": "stream",
                        "source": False,
                        "sink": True,
                        "codec": True,
                        "staging_safe": True,
                        "certified": False,
                        "technically_eligible": False,
                        "reasons": ["native_transfer_stream_fallback_file_only_source"],
                    },
                    {
                        "transport": "file",
                        "source": True,
                        "sink": True,
                        "codec": True,
                        "staging_safe": True,
                        "certified": False,
                        "technically_eligible": True,
                        "reasons": [],
                    },
                ],
            },
        },
        "native_transfer_bulk_wire": {
            "schema_version": "dpone.native_transfer.bulk_wire.v1",
            "selected_route": "typed_raw_direct",
            "requested_mode": "typed_raw",
            "input_format": "CustomSeparated",
            "delimiter_profile": {
                "name": "ascii_control",
                "field_delimiter": "\x1f",
                "row_after_delimiter": "\x1e",
                "row_between_delimiter": "\n",
                "source_row_terminator": "\x1e\n",
            },
            "mssql_source_escaping": False,
            "schema_hash": "sha256:test",
            "acceleration": {
                "schema_version": "dpone.native_transfer.acceleration.v1",
                "requested_mode": "auto",
                "selected_backend": "python_reference",
                "source_format": "mssql-bcp-native",
                "target_format": "Native",
                "backend_id": None,
                "accelerator_version": None,
                "available": False,
                "certified": False,
                "fallback_reason": "native_acceleration_package_missing",
                "warning_codes": ["native_acceleration_package_missing"],
                "blocker_codes": [],
            },
            "warnings": ["bulk_wire_typed_raw_requires_delimiter_safety"],
            "blockers": [],
        },
        "native_transfer_snapshot_optimization": {
            "schema_version": "dpone.native_transfer.snapshot_optimization.v1",
            "requested_backend": "auto",
            "selected_backend": "native_tcp",
            "native_tcp_backend": "direct",
            "compression": "lz4",
            "packet_size": "auto",
            "block_rows": "auto",
            "block_bytes": "auto",
            "max_parallel_exports": 4,
            "max_parallel_loads": 2,
            "release_gate": "green",
            "route_certified": True,
            "fallback_chain": ["native_tcp", "client", "http", "python"],
            "partition_planner": "statistics",
            "stats_confidence": "high",
            "fallback_reason": None,
            "blockers": [],
            "warnings": [],
            "reasons": [],
        },
        "type_matrix": {"profile": "postgres_to_mssql_native_v2", "explain_command": "dpone schema explain ..."},
        "warnings": [],
        "strategy_intelligence": {
            "decision": {
                "strategy_mode": "incremental_merge",
                "native_fast_path": "postgres_copy_to_mssql_bcp",
                "native_transfer_plan": {
                    "export_method": "copy_to_stdout",
                    "ingest_method": "mssql_bcp",
                    "finalizer": "delete_insert",
                    "partitioning": {"strategy": "range", "column": "order_id"},
                    "transport_contract": {
                        "route": "postgres_to_mssql",
                        "wire_format": "mssql-delimited",
                        "text_codec": "BulkTextCodec",
                        "null_policy": "empty_bcp_field_is_null",
                        "empty_string_policy": "encoded_marker_roundtrip",
                        "lossless": True,
                    },
                },
            }
        },
    }


def test_plan_text_renders_native_transport_contract() -> None:
    rendered = _render_text(_payload())

    assert "- runtime_transfer_store: s3 s3://dpone-stage/native-transfer" in rendered
    assert "- native_transfer_stream_transport: auto prefer_streaming=True fallback_to_file=True" in rendered
    assert "- native_transfer_stream_buffer_bytes: 16777216" in rendered
    assert "- native_transfer_resolved_transport: file" in rendered
    assert "- native_transfer_route_decision: file certification=uncertified gate=warning" in rendered
    assert "- native_transfer_route_fallback_chain: stream -> object -> file" in rendered
    assert "- native_transfer_stream_fallback_reason: native_transfer_stream_fallback_file_only_source" in rendered
    assert "- native_transfer_bulk_wire: typed_raw_direct format=CustomSeparated source_escaping=False" in rendered
    assert (
        "- native_transfer_acceleration: python_reference mode=auto fallback=native_acceleration_package_missing"
        in rendered
    )
    assert (
        "- native_transfer_snapshot_backend: native_tcp native_tcp_backend=direct compression=lz4 gate=green"
        in rendered
    )
    assert "- native_transfer_snapshot_parallelism: exports=4 loads=2" in rendered
    assert "- native_transfer_snapshot_partition_planner: statistics confidence=high" in rendered
    assert "- native_transfer_snapshot_fallback_chain: native_tcp -> client -> http -> python" in rendered
    assert "- native_transfer_bulk_wire_warning: bulk_wire_typed_raw_requires_delimiter_safety" in rendered
    assert "- native_transfer_transport: postgres_to_mssql lossless=True" in rendered
    assert "- native_transfer_wire_format: mssql-delimited" in rendered
    assert "- native_transfer_text_codec: BulkTextCodec" in rendered


def test_plan_markdown_renders_native_transport_contract() -> None:
    rendered = _render_md(_payload())

    assert "- transfer_store: `s3` `s3://dpone-stage/native-transfer`" in rendered
    assert "- transport: `auto` prefer_streaming=`True` fallback_to_file=`True`" in rendered
    assert "- stream_buffer_bytes: `16777216`" in rendered
    assert "## Native transfer resolved transport" in rendered
    assert "- fallback_reason: `native_transfer_stream_fallback_file_only_source`" in rendered
    assert "## Native transfer route decision" in rendered
    assert "- selected_transport: `file`" in rendered
    assert "- certification_status: `uncertified`" in rendered
    assert "## Native transfer bulk wire" in rendered
    assert "- selected_route: `typed_raw_direct`" in rendered
    assert "- input_format: `CustomSeparated`" in rendered
    assert "- mssql_source_escaping: `False`" in rendered
    assert "- acceleration_backend: `python_reference`" in rendered
    assert "- acceleration_fallback: `native_acceleration_package_missing`" in rendered
    assert "## Native transfer snapshot optimization" in rendered
    assert "- selected_backend: `native_tcp`" in rendered
    assert "- native_tcp_backend: `direct`" in rendered
    assert "- compression: `lz4`" in rendered
    assert "- partition_planner: `statistics`" in rendered
    assert "- parallelism: exports=`4` loads=`2`" in rendered
    assert "## Native transfer transport contract" in rendered
    assert "- route: `postgres_to_mssql`" in rendered
    assert "- lossless: `True`" in rendered
    assert "- text_codec: `BulkTextCodec`" in rendered
