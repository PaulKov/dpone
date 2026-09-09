"""Universal fail-closed validation for target-local key reconciliation."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from dpone.contracts.connector_declarations import canonical_endpoint_type
from dpone.contracts.technical_columns import resolve_technical_columns
from dpone.manifest.models import ProcessSpec
from dpone.manifest.validation_models import Severity, ValidationIssue


def validate_key_snapshot_reconciliation(
    spec: ProcessSpec,
    *,
    manifest_path: Path,
    selector: str,
    load_cfg: object,
    options: Mapping[str, object],
) -> list[ValidationIssue]:
    """Validate the certified PostgreSQL XMin to MSSQL route in every profile."""

    raw = options.get("reconciliation")
    if not isinstance(raw, Mapping) or not bool(raw.get("enabled", True)):
        return []

    issues: list[ValidationIssue] = []

    def require(condition: bool, code: str, message: str) -> None:
        if not condition:
            issues.append(
                ValidationIssue(
                    severity=Severity.ERROR,
                    code=code,
                    message=message,
                    manifest_path=manifest_path,
                    selector=selector,
                )
            )

    strategy = getattr(getattr(load_cfg, "load_strategy", None), "value", None)
    unique_key = getattr(load_cfg, "unique_key", None)
    state = _mapping(options.get("state"))
    source_options = _mapping(options.get("source_options"))
    soft_delete = _mapping(options.get("soft_delete"))
    physical_design = _mapping(options.get("physical_design"))
    source_type = _endpoint_type(spec, "source")
    sink_type = _endpoint_type(spec, "sink")

    require(
        strategy == "incremental_merge",
        "RECONCILIATION_REQUIRES_INCREMENTAL_MERGE",
        "reconciliation.mode=key_snapshot requires sink.strategy.mode=incremental_merge.",
    )
    require(
        bool(unique_key),
        "RECONCILIATION_REQUIRES_UNIQUE_KEY",
        "reconciliation.mode=key_snapshot requires sink.strategy.unique_key.",
    )
    require(
        source_type == "postgres" and sink_type == "mssql",
        "RECONCILIATION_ROUTE_UNSUPPORTED",
        "key_snapshot reconciliation is certified only for source.type=postgres and sink.type=mssql.",
    )
    require(
        str(source_options.get("incremental_strategy") or "").strip().lower() in {"xmin", "postgres_xmin", "pg_xmin"},
        "RECONCILIATION_REQUIRES_XMIN",
        "key_snapshot reconciliation requires source.options.incremental_strategy=xmin.",
    )
    require(
        str(source_options.get("batch_commit_mode") or "").strip().lower() == "whole",
        "RECONCILIATION_REQUIRES_WHOLE_SNAPSHOT",
        "same_source_snapshot reconciliation requires source.options.batch_commit_mode=whole.",
    )
    require(
        source_options.get("compress_export", False) is False,
        "POSTGRES_MSSQL_RECONCILIATION_FORBIDS_COMPRESSION",
        "PostgreSQL to MSSQL key reconciliation requires source.options.compress_export=false.",
    )
    require(
        str(source_options.get("export_format") or "csv").strip().lower() == "csv",
        "POSTGRES_MSSQL_RECONCILIATION_REQUIRES_CSV_CONTRACT",
        "The public PostgreSQL to MSSQL wire contract must use source.options.export_format=csv.",
    )
    require(
        source_options.get("allow_unsafe_raw_mssql_bulk_files", False) is False
        and options.get("allow_unsafe_raw_mssql_bulk_files", False) is False,
        "POSTGRES_MSSQL_RECONCILIATION_FORBIDS_UNSAFE_BULK",
        "key_snapshot reconciliation never permits allow_unsafe_raw_mssql_bulk_files=true.",
    )
    require(
        not _mapping(source_options.get("partitioning")).get("column"),
        "RECONCILIATION_PARALLEL_SNAPSHOT_UNSUPPORTED",
        "key_snapshot reconciliation v1 requires one whole-snapshot export; source partition workers are unsupported.",
    )
    require(
        not str(source_options.get("custom_predicate") or options.get("source_custom_predicate") or "").strip(),
        "RECONCILIATION_SCOPED_PREDICATE_UNSUPPORTED",
        "key_snapshot reconciliation v1 requires the full source relation; scoped predicates are unsupported.",
    )
    require(
        physical_design.get("apply_runtime") is False,
        "RECONCILIATION_REQUIRES_EXTERNAL_TARGET_PROVISIONING",
        "key_snapshot reconciliation v1 requires physical_design.apply_runtime=false and one-time target DDL.",
    )
    require(
        canonical_endpoint_type(str(state.get("type") or "")) == "mssql"
        and str(state.get("atomicity") or "").strip().lower() == "target_atomic",
        "RECONCILIATION_REQUIRES_ATOMIC_MSSQL_STATE",
        "key_snapshot reconciliation requires state.type=mssql and state.atomicity=target_atomic.",
    )
    require(
        str(state.get("provisioning") or "external").strip().lower() == "external",
        "RECONCILIATION_REQUIRES_EXTERNAL_STATE_PROVISIONING",
        "target-atomic state tables must be provisioned once outside the DAG.",
    )
    legacy_state_fields = {"connection_id", "connection_type", "credentials_source", "vault_path"}
    require(
        not any(field in state for field in legacy_state_fields),
        "TARGET_ATOMIC_STATE_REQUIRES_CONNECTION_REF",
        "target-atomic state accepts only connection_ref/reuse authority; legacy credential fields are forbidden.",
    )
    require(
        str(soft_delete.get("mode") or "timestamp_only") in {"timestamp_only", "timestamp_and_flag", "flag_only"},
        "SOFT_DELETE_MODE_UNSUPPORTED",
        "sink.options.soft_delete.mode must be timestamp_only, timestamp_and_flag, or flag_only.",
    )
    try:
        technical_columns = resolve_technical_columns(options)
    except ValueError:
        technical_columns = None
    require(
        technical_columns is not None and technical_columns.enabled,
        "RECONCILIATION_REQUIRES_TECH_COLUMNS",
        "key_snapshot soft delete requires sink.options.technical_columns=required|optional.",
    )
    return issues


def _mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _endpoint_type(spec: ProcessSpec, endpoint: str) -> str | None:
    raw = spec.raw_config or {}
    config = raw.get(endpoint) if isinstance(raw, Mapping) else None
    if isinstance(config, Mapping):
        value = config.get("type")
        if isinstance(value, str) and value.strip():
            return canonical_endpoint_type(value)
    return None


__all__ = ["validate_key_snapshot_reconciliation"]
