"""Offline projection of the explicitly authored bounded MSSQL native route.

This describes required execution semantics, never connector availability or a
successful schema/target preflight. Legacy routes retain their existing planners.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.contracts.mssql_generic_transaction_names import (
    ATTEMPT_TABLE,
    ATTEMPT_TRIGGER,
    FENCE_TABLE,
    FENCE_TRIGGER,
    GENERIC_TRANSACTION_CATALOG_VERSION,
    OPERATION_TABLE,
    OPERATION_TRIGGER,
    RECEIPT_TABLE,
    RECEIPT_TRIGGER,
    TARGET_IDENTITY_REGISTRY_TABLE,
    TARGET_IDENTITY_REGISTRY_TRIGGER,
)
from dpone.manifest.clickhouse_raw_snapshot_policy import native_source_snapshot_policy
from dpone.manifest.errors import ManifestConfigurationError
from dpone.manifest.mssql_native_policy import (
    native_import_backend,
    native_limits,
    native_sqlclient_layout_version,
    native_verification_backend,
)


def validate_source_snapshot_plan(config: Any, source_type: str, sink_type: str) -> None:
    """Reject an explicit snapshot on an incompatible route before generic planning."""
    native = config.options.get("native_transfer")
    if not isinstance(native, Mapping) or "source_snapshot" not in native:
        return
    try:
        snapshot = native_source_snapshot_policy(config)
    except ValueError as error:
        raise ManifestConfigurationError(
            "mssql_native.source_snapshot_mode_invalid: source.options.native_transfer.source_snapshot "
            "requires mode=query_visible, or mode=exact_raw_rows with "
            "replica_scope=single_server|connected_replica; additional fields are not allowed."
        ) from error
    wire, execution = native.get("wire"), native.get("execution")
    chunking = execution.get("chunking") if isinstance(execution, Mapping) else None
    route_supported = (
        source_type == "clickhouse"
        and sink_type == "mssql"
        and isinstance(wire, Mapping)
        and wire.get("mode") == "typed_binary"
        and wire.get("binary_format") == "mssql_native"
        and isinstance(chunking, Mapping)
        and chunking.get("mode") == "bounded_stream"
    )
    if "source_snapshot" in native and not route_supported:
        raise ManifestConfigurationError(
            "mssql_native.source_snapshot_route_unsupported: source_snapshot requires the "
            "ClickHouse to MSSQL typed_binary/mssql_native route with bounded_stream chunking."
        )
    if snapshot.mode != "exact_raw_rows":
        return
    try:
        target_local = native_verification_backend(config).value == "target_local"
    except ValueError:
        target_local = False
    if not target_local:
        raise ManifestConfigurationError(
            "mssql_native.raw_snapshot_requires_target_local_verification: set "
            "source.options.native_transfer.execution.verification_backend=target_local "
            "to retain the durable identity and source-free recovery required by exact_raw_rows."
        )


def project_mssql_native(plan: dict[str, Any], config: Any) -> None:
    """Replace generic transfer projections only for the complete native opt-in."""
    native = config.options.get("native_transfer")
    if not isinstance(native, Mapping):
        return
    snapshot = native_source_snapshot_policy(config)
    wire, execution = native.get("wire"), native.get("execution")
    chunking = execution.get("chunking") if isinstance(execution, Mapping) else None
    route_supported = (
        plan["source"]["type"] == "clickhouse"
        and plan["sink"]["type"] == "mssql"
        and isinstance(wire, Mapping)
        and wire.get("mode") == "typed_binary"
        and wire.get("binary_format") == "mssql_native"
        and isinstance(chunking, Mapping)
        and chunking.get("mode") == "bounded_stream"
    )
    if not route_supported:
        return
    if not isinstance(execution, Mapping):
        return
    limits = native_limits(config)
    importer = native_import_backend(config)
    authored: dict[str, Any] = {
        "status": "live_preflight_required",
        "live_preflight": "not_run",
        "source_query_count": 1,
        "source_projection": "catalog_columns_explicitly_named",
        "offset_pagination": False,
        "parallelism_scope": "native_encoding_and_file_import",
        "transport": "bounded_native_files",
        "import_backend": importer.value,
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
    if importer.value == "mssql_sqlclient":
        layout_version = native_sqlclient_layout_version(config)
        authored["layout_version"] = layout_version
        authored["verification_strategy"] = (
            "persisted_hash_and_mutation_watermark" if layout_version == 2 else "canonical_target_readback"
        )
    if "source_snapshot" in native:
        authored["source_snapshot"] = {
            "mode": snapshot.mode,
            "replica_scope": snapshot.replica_scope,
            "authority": "declared",
            "live_observation": "not_run",
        }
    if "verification_backend" in execution:
        verifier = native_verification_backend(config)
        authored["verification_backend"] = verifier.value
        authored["verification_identity_version"] = verifier.identity_version
        if verifier.value == "target_local":
            authored["writer_proof_capability"] = (
                "sqlclient-session-applock-v1"
                if importer.value == "mssql_sqlclient"
                else "bcp-supervised-stage-barrier-v1"
            )
            writer_dependency = (
                "verified_sqlclient_companion" if importer.value == "mssql_sqlclient" else "supervised_bcp_writer"
            )
            authored["required_dependencies"].extend(
                ["stable_target_custody", writer_dependency, "target_local_digest"]
            )
            authored["admission"] = {
                "schema_version": 2,
                "kind": "dpone.mssql-native-admission.v2",
                "status": "blocked",
                "import_backend": importer.value,
                "verification_backend": verifier.value,
                "identity_version": verifier.identity_version,
                "capability_id": authored["writer_proof_capability"],
                "blockers": ["mssql_native.live_preflight_required"],
                "warnings": ["mssql_native.live_preflight_not_run"],
            }
    if "encoding_parallelism" in authored["limits"]:
        authored["stage_concurrency"] = {
            "encoding_parallelism": limits.effective_encoding_parallelism,
            "import_parallelism": limits.effective_import_parallelism,
            "retained_work_capacity": limits.retained_work_capacity,
        }
    plan["mssql_native"] = authored
    plan["state"] = {
        **plan["state"],
        "catalog": f"generic_mssql_transaction_v{GENERIC_TRANSACTION_CATALOG_VERSION}",
        "tables": {
            "target_identity_registry": TARGET_IDENTITY_REGISTRY_TABLE,
            "target_fence": FENCE_TABLE,
            "load_attempt": ATTEMPT_TABLE,
            "load_operation": OPERATION_TABLE,
            "load_receipt": RECEIPT_TABLE,
        },
        "triggers": {
            "target_identity_registry": TARGET_IDENTITY_REGISTRY_TRIGGER,
            "target_fence": FENCE_TRIGGER,
            "load_attempt": ATTEMPT_TRIGGER,
            "load_operation": OPERATION_TRIGGER,
            "load_receipt": RECEIPT_TRIGGER,
        },
    }
    plan["bulk_path"] = f"clickhouse_bounded_mssql_native_{importer.value}"
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
        "release_gate": "live_preflight_required",
        "fallback_chain": [],
        "blockers": [],
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
            "ingest_method": f"bounded_mssql_native_{importer.value}",
            "finalizer": "existing_target_transaction_with_commit_receipt",
            "partitioning": {"strategy": "one_authored_scope", "column": authored["publication_scope"].get("column")},
        }
        intelligence["decision"] = decision
        plan["strategy_intelligence"] = intelligence
