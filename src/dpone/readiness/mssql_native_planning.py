"""Offline projection of the explicitly authored bounded MSSQL native route.

This describes required execution semantics, never connector availability or a
successful schema/target preflight. Legacy routes retain their existing planners.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.manifest.mssql_native_policy import native_limits, native_transport_policy


def project_mssql_native(plan: dict[str, Any], config: Any) -> None:
    """Replace generic transfer projections only for the complete native opt-in."""
    native = config.options.get("native_transfer")
    if not isinstance(native, Mapping):
        return
    transport = native_transport_policy(config)
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
        if transport is not None:
            raise ValueError("mssql_native.transport_requires_native_route")
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
        "limits": limits.to_dict(),
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
    if "encoding_parallelism" in authored["limits"]:
        authored["stage_concurrency"] = {
            "encoding_parallelism": limits.effective_encoding_parallelism,
            "import_parallelism": limits.effective_import_parallelism,
            "retained_work_capacity": limits.retained_work_capacity,
        }
    if transport is not None:
        authored["bulk_transport"] = transport.to_dict()
        authored["status"] = "backend_runtime_unavailable"
        authored["worker_platform"] = "linux_arm64" if transport.backend == "mssql_sqlclient" else "linux"
        authored["worker_memory_bound"] = "address_space_not_rss"
        authored["writer_authority"] = "separate_restricted_writer_and_coordinator"
        dependencies = (
            ["dpone-mssql-sqlclient", ".NET==8.0.31", "Microsoft.Data.SqlClient==7.0.2"]
            if transport.backend == "mssql_sqlclient"
            else ["mssql-python==1.13.0"]
        )
        if transport.input == "arrow":
            dependencies.append("Apache.Arrow==23.0.0" if transport.backend == "mssql_sqlclient" else "pyarrow==25.0.1")
        authored["required_dependencies"].extend([*dependencies, "fenced_worker_lifecycle"])
    plan["mssql_native"] = authored
    ingest_method = "bounded_mssql_native_bcp" if transport is None else "bounded_mssql_native_tds"
    plan["bulk_path"] = "clickhouse_" + ingest_method
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
        "release_gate": authored["status"],
        "fallback_chain": [],
        "blockers": [authored["status"]],
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
            "ingest_method": ingest_method,
            "finalizer": "existing_target_transaction_with_commit_receipt",
            "partitioning": {"strategy": "one_authored_scope", "column": authored["publication_scope"].get("column")},
        }
        intelligence["decision"] = decision
        plan["strategy_intelligence"] = intelligence
