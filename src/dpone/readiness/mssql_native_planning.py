"""Offline projection of the explicitly authored bounded MSSQL native route.

This describes required execution semantics, never connector availability or a
successful schema/target preflight. Legacy routes retain their existing planners.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict
from typing import Any

from dpone.manifest.mssql_native_policy import native_limits


def project_mssql_native(plan: dict[str, Any], config: Any) -> None:
    """Replace generic transfer projections only for the complete native opt-in."""
    native = config.options.get("native_transfer")
    if not isinstance(native, Mapping):
        return
    wire, execution = native.get("wire"), native.get("execution")
    chunking = execution.get("chunking") if isinstance(execution, Mapping) else None
    if not (
        plan["source"]["type"] == "clickhouse"
        and plan["sink"]["type"] == "mssql"
        and isinstance(wire, Mapping)
        and wire.get("mode") == "typed_binary"
        and wire.get("binary_format") == "mssql_native"
        and isinstance(chunking, Mapping)
        and chunking.get("mode") == "bounded_stream"
    ):
        return
    limits = native_limits(config)
    authored: dict[str, Any] = {
        "status": "composition_required",
        "live_preflight": "not_run",
        "source_query_count": 1,
        "source_projection": "catalog_columns_explicitly_named",
        "offset_pagination": False,
        "parallelism_scope": "native_encoding_and_file_import",
        "transport": "bounded_native_files",
        "limits": asdict(limits),
        "spool_payload_bound": limits.spool_payload_bound,
        "spool_bound_scope": "encoded_payload_only_excludes_driver_and_server_memory",
        "sql_capacity_policy": "observed_stop_threshold",
        "publication_scope": dict(config.options.get("mssql_native_window") or {"mode": "full_refresh"}),
        "publication": "existing_target_transaction_with_commit_receipt",
        "resume": "source_free_after_verified_eof;partial_extraction_requires_reextract",
        "required_dependencies": [
            "native_runtime_factory",
            "durable_chunk_journal",
            "lease_and_target_admission",
            "independent_guarded_import_connections",
            "quality_evidence_state_callbacks",
        ],
    }
    plan["mssql_native"] = authored
    plan["bulk_path"] = "clickhouse_bounded_mssql_native_bcp"
    # These generic stream/snapshot optimizers do not govern this executor.
    for key in ("native_transfer_execution", "native_transfer_transport", "native_transfer_snapshot_optimization"):
        plan[key] = {}
    plan["native_transfer_bulk_wire"] = {
        "selected_route": "bounded_mssql_native",
        "requested_mode": "typed_binary",
        "binary_format": "mssql_native",
        "input_format": "mssql_native",
        "schema_status": "live_preflight_required",
        "warnings": [],
        "blockers": [],
    }
    plan["native_transfer_route_decision"] = {
        "requested_transport": "bounded_native_files",
        "selected_transport": "bounded_native_files",
        "certification_mode": "unverified",
        "certification_status": "unverified",
        "release_gate": "composition_required",
        "fallback_chain": [],
        "blockers": ["composition_required"],
        "warnings": ["live_preflight_not_run"],
    }
    plan["source_impact"] = [
        {
            "code": "native_source_catalog_preflight_required",
            "severity": "info",
            "message": "One source query uses an explicit catalog-derived projection.",
            "action": "Compose the native runtime to validate source identity and lossless typed columns before extraction.",
        }
    ]
    plan["type_matrix"] = {
        **plan["type_matrix"],
        "ready": False,
        "runtime_validation": "native_schema_preflight_required",
    }
    plan["physical_design"] = {**plan["physical_design"], "ddl": [], "ddl_status": "target_preflight_required"}
    if "strategy_intelligence" in plan:
        intelligence = dict(plan["strategy_intelligence"])
        decision = dict(intelligence.get("decision") or {})
        decision["native_transfer_plan"] = {
            "export_method": "one_clickhouse_query",
            "ingest_method": "bounded_mssql_native_bcp",
            "finalizer": "existing_target_transaction_with_commit_receipt",
            "partitioning": {"strategy": "one_authored_scope", "column": authored["publication_scope"].get("column")},
        }
        intelligence["decision"] = decision
        plan["strategy_intelligence"] = intelligence
