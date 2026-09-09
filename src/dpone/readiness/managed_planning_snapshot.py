"""Execution-plan projections for target-local key-snapshot routes."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.config.mssql_strategy_contract import normalize_mssql_authoring_strategy
from dpone.config.state import resolve_mssql_state_defaults
from dpone.contracts.connector_declarations import canonical_endpoint_type
from dpone.contracts.incremental_snapshot import MSSQL_TEXT_KEY_COLLATION, is_mssql_text_key_type

POSTGRES_XMIN_KEY_SNAPSHOT_MSSQL_ROUTE = "postgres_xmin_key_snapshot_to_mssql"


def staging_plan(load_config: Any, sink_type: str) -> dict[str, Any]:
    """Describe whether the selected strategy mutates in place or swaps."""

    strategy = str(getattr(getattr(load_config, "load_strategy", None), "value", ""))
    merge_policy = str(getattr(load_config, "merge_policy", "") or "").strip().lower()
    if sink_type == "mssql":
        contract = normalize_mssql_authoring_strategy(load_config)
        merge = contract.incremental_merge
        shadow = strategy == "replace" or bool(merge and merge.merge_policy == "shadow_swap")
    else:
        shadow = strategy in {"full_refresh", "replace"} or merge_policy == "shadow_swap"
    return {
        "staging_first": True,
        "schema": load_config.staging_schema,
        "shadow_or_swap": bool(shadow and sink_type in {"mssql", "clickhouse", "postgres"}),
        "finalization": "shadow_swap" if shadow else "in_place",
    }


def reconciliation_plan(load_config: Any) -> dict[str, Any]:
    """Project typed key-snapshot policy without enabling legacy reconciliation."""

    policy = getattr(load_config, "reconciliation_policy", None)
    if policy is not None:
        return policy.as_runtime_options()
    return {
        "enabled": bool(load_config.reconciliation),
        "mode": "legacy_snapshot" if load_config.reconciliation else "off",
    }


def state_plan(raw: Mapping[str, Any], sink_type: str) -> dict[str, Any]:
    """Project state authority and external table names without credentials."""

    state = raw.get("state", {}) if isinstance(raw.get("state"), Mapping) else {}
    connection_ref = state.get("connection_ref") or state.get("connection_id")
    state_type = canonical_endpoint_type(str(state.get("type") or ""))
    if state_type == "mssql":
        defaults = resolve_mssql_state_defaults(state)
        tables = defaults.as_table_mapping()
        atomicity = defaults.atomicity
        provisioning = defaults.provisioning
    else:
        tables = {
            key: name
            for key in (
                "table",
                "run_table",
                "receipt_table",
                "repair_authority_table",
                "repair_consumption_table",
                "audit_table",
            )
            if (name := _state_table_name(state.get(key))) is not None
        }
        atomicity = state.get("atomicity", "after_target")
        provisioning = state.get("provisioning", "runtime")
    return {
        "backend": state_type or "default",
        "connection_ref": connection_ref,
        "connection_id": connection_ref or ("sink" if sink_type in {"mssql", "postgres"} else None),
        "atomicity": atomicity,
        "provisioning": provisioning,
        "location_authority": "environment_registry" if state.get("connection_ref") else "manifest",
        "tables": tables,
    }


def is_postgres_xmin_key_snapshot_mssql(
    raw: Mapping[str, Any],
    source_type: str,
    sink_type: str,
) -> bool:
    """Return whether the process selects the typed snapshot-envelope finalizer."""

    source_raw = raw.get("source")
    source = source_raw if isinstance(source_raw, Mapping) else {}
    source_options_raw = source.get("options")
    source_options = source_options_raw if isinstance(source_options_raw, Mapping) else {}
    incremental_strategy = str(source_options.get("incremental_strategy") or "").strip().lower()
    return (
        source_type == "postgres"
        and sink_type == "mssql"
        and incremental_strategy in {"xmin", "postgres_xmin", "pg_xmin"}
        and _key_snapshot_enabled(raw)
    )


def external_schema_evolution_plan(generic_plan: Mapping[str, Any]) -> dict[str, Any]:
    """Describe the snapshot route's external schema authority without promising generic DDL."""

    configured_enabled = bool(generic_plan.get("enabled", True))
    return {
        **dict(generic_plan),
        "enabled": False,
        "configured_enabled": configured_enabled,
        "mode": "external_contract",
        "apply_safe": False,
        "ddl_preview": [],
        "runtime_execution": "bypassed",
        "provisioning": "external",
        "authority": "one_time_installer",
        "bypass_reason": POSTGRES_XMIN_KEY_SNAPSHOT_MSSQL_ROUTE,
    }


def external_physical_design_plan(
    generic_plan: Mapping[str, Any],
    *,
    unique_key: tuple[str, ...],
) -> dict[str, Any]:
    """Suppress generic CREATE DDL and expose the exact target key contract."""

    columns = generic_plan.get("columns")
    resolved_columns = columns if isinstance(columns, Mapping) else {}
    text_key_columns = tuple(
        column
        for column in unique_key
        if isinstance(resolved_columns.get(column), Mapping)
        and is_mssql_text_key_type(str(resolved_columns[column].get("target_type") or ""))
    )
    return {
        **dict(generic_plan),
        "ddl": [],
        "apply_runtime": False,
        "runtime_ddl": "disabled",
        "provisioning": "external",
        "ddl_authority": "one_time_installer",
        "generated_ddl_suppressed": True,
        "external_contract": {
            "route": POSTGRES_XMIN_KEY_SNAPSHOT_MSSQL_ROUTE,
            "target_must_exist": True,
            "schema_must_exist": True,
            "unique_index": {"required": True, "columns": list(unique_key)},
            "required_text_key_collation": MSSQL_TEXT_KEY_COLLATION,
            "text_key_collations": {column: MSSQL_TEXT_KEY_COLLATION for column in text_key_columns},
        },
    }


def bulk_wire_override(
    raw: Mapping[str, Any],
    source_type: str,
    sink_type: str,
) -> dict[str, Any] | None:
    """Return the certified PostgreSQL-to-MSSQL character wire, if selected."""

    if source_type != "postgres" or sink_type != "mssql" or not _key_snapshot_enabled(raw):
        return None
    return {
        "schema_version": "dpone.native_transfer.bulk_wire.v1",
        "selected_route": "postgres_mssql_bulk_text_codec",
        "requested_mode": "safe_character_bcp",
        "source_type": source_type,
        "sink_type": sink_type,
        "public_export_format": "csv",
        "effective_export_format": "mssql-delimited",
        "compression": "none",
        "bulk_text_codec": True,
        "lossless": True,
        "lossless_enforcement": {
            "declared_types": "reject_structural_narrowing_before_source_row_export",
            "wire": "immutable_sha256_and_row_count_receipt",
            "native_staging": "source_target_source_roundtrip_before_target_transaction",
        },
        "warnings": [],
        "blockers": [],
    }


def _key_snapshot_enabled(raw: Mapping[str, Any]) -> bool:
    reconciliation = raw.get("reconciliation")
    return bool(
        isinstance(reconciliation, Mapping)
        and reconciliation.get("enabled", True) is True
        and str(reconciliation.get("mode", "key_snapshot")).strip().lower() == "key_snapshot"
    )


def _state_table_name(value: object) -> str | None:
    if not isinstance(value, Mapping):
        return None
    name = value.get("name") or value.get("run_name")
    return str(name).strip() if name else None


__all__ = [
    "bulk_wire_override",
    "external_physical_design_plan",
    "external_schema_evolution_plan",
    "is_postgres_xmin_key_snapshot_mssql",
    "reconciliation_plan",
    "staging_plan",
    "state_plan",
]
