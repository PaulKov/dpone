"""Focused helpers for :mod:`dpone.dag.load_config_builder`."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.config.load_config import ROUTE_IDENTITY_V1_ENDPOINT_TYPES_OPTION
from dpone.config.load_strategy import LoadStrategy
from dpone.config.reconciliation import reconciliation_policy_from_options
from dpone.contracts.api_sources import get_api_source_defaults
from dpone.contracts.incremental_snapshot import KeySnapshotReconciliationPolicy
from dpone.contracts.mssql_object_name import MSSQLObjectName
from dpone.contracts.sink_dialect import is_mssql_dialect
from dpone.dag.errors import DagConfigurationError
from dpone.dag.parse_trace import (
    LoadConfigParseTracer,
    record_load_fields,
    record_runtime_contract_fields,
)


def inject_endpoint_identity_options(
    options: dict[str, Any],
    source_type: object,
    sink_type: object,
    canonical_source_type: str,
    canonical_sink_type: str,
) -> None:
    """Install canonical runtime types and the minimal legacy identity projection."""

    authored_source = str(source_type)
    authored_sink = str(sink_type)
    options.pop(ROUTE_IDENTITY_V1_ENDPOINT_TYPES_OPTION, None)
    options["source_type"] = canonical_source_type
    options["sink_type"] = canonical_sink_type
    if authored_source != canonical_source_type or authored_sink != canonical_sink_type:
        options[ROUTE_IDENTITY_V1_ENDPOINT_TYPES_OPTION] = {
            "source_type": authored_source,
            "sink_type": authored_sink,
        }


def resolve_partition_contract(
    *,
    strategy_config: Mapping[str, Any],
    source_options: Mapping[str, Any],
    load_strategy: LoadStrategy,
) -> Any:
    """Materialize the partition authority selected by ``strategy.mode: auto``."""

    authored = strategy_config.get("partition")
    if authored is not None:
        return authored
    requested = str(strategy_config.get("mode") or LoadStrategy.FULL_REFRESH.value).strip().lower()
    column = source_options.get("partition_column")
    if requested == "auto" and load_strategy is LoadStrategy.PARTITION_REPLACE and isinstance(column, str):
        normalized = column.strip()
        if normalized:
            return {"column": normalized, "values_from_staging": True}
    return {}


def derive_api_source(
    *,
    source_cfg: dict[str, Any],
    source_table_cfg: dict[str, Any],
    source_options: dict[str, Any],
    parse_tracer: LoadConfigParseTracer | None,
) -> tuple[Any, Any]:
    api_defaults = get_api_source_defaults(source_cfg.get("api_type", "api"))
    resource = source_options.get("resource", "data")
    source_schema = source_table_cfg.get("schema") or api_defaults.source_schema()
    source_table = source_table_cfg.get("name") or api_defaults.source_table(resource)
    api_type = api_defaults.api_type

    if parse_tracer:
        parse_tracer.record(
            kind="load_config.field",
            target="load_config.source_schema",
            value=source_schema,
            sources=("source.table.schema", "source.api_type"),
            operation="derive_api",
            details={"source_type": "api", "api_type": api_type},
        )
        parse_tracer.record(
            kind="load_config.field",
            target="load_config.source_table",
            value=source_table,
            sources=("source.table.name", "source.options.resource"),
            operation="derive_api",
            details={"source_type": "api", "resource": resource},
        )
    return source_schema, source_table


def resolve_batch_size(
    *,
    source_options: dict[str, Any],
    sink_options: dict[str, Any],
    strategy_intelligence: dict[str, Any] | None,
) -> int:
    raw = source_options.get("batch_size")
    if raw is None:
        raw = sink_options.get("batch_size")
    if raw is None and strategy_intelligence is not None:
        raw = _adaptive_batch_size(strategy_intelligence)
    return int(raw or 10000)


def resolve_connection_identity(endpoint_config: Mapping[str, Any]) -> object | None:
    """Return an explicit legacy ID or a canonical logical reference."""

    if "connection_id" in endpoint_config:
        return endpoint_config.get("connection_id")
    connection_ref = endpoint_config.get("connection_ref")
    return connection_ref if isinstance(connection_ref, str) else None


def inject_manifest_context_options(
    *,
    options: dict[str, Any],
    base_path: Path | None,
    parse_tracer: LoadConfigParseTracer | None,
) -> None:
    if base_path is None:
        return
    options.setdefault("manifest_dir", str(base_path))
    options.setdefault("repo_root", str(Path.cwd()))
    if parse_tracer:
        parse_tracer.record(
            kind="option.key",
            target="load_config.options.manifest_dir",
            value=str(base_path),
            sources=("manifest.path",),
            operation="inject",
        )


def merge_load_options(
    *,
    source_options: dict[str, Any],
    sink_options: dict[str, Any],
    parse_tracer: LoadConfigParseTracer | None,
) -> dict[str, Any]:
    options = _deep_merge(source_options, sink_options)
    if parse_tracer:
        for key in sorted(set(options.keys()), key=lambda x: str(x)):
            if key in sink_options:
                src = f"sink.options.{key}"
                op = "merge_sink_overrides"
            else:
                src = f"source.options.{key}"
                op = "merge_source"
            parse_tracer.record(
                kind="option.key",
                target=f"load_config.options.{key}",
                value=options.get(key),
                sources=(src,),
                operation=op,
            )
    return options


def inject_runtime_contract_options(
    *,
    config: Mapping[str, Any],
    runtime_config: Mapping[str, Any],
    options: dict[str, Any],
    reconciliation_options: Mapping[str, Any] | None,
    parse_tracer: LoadConfigParseTracer | None,
) -> KeySnapshotReconciliationPolicy | None:
    """Inject normalized top-level contracts into canonical load options."""

    schema_contract = config.get("schema_contract")
    if schema_contract is not None:
        if not isinstance(schema_contract, Mapping):
            raise DagConfigurationError("schema_contract must be a mapping")
        sink_contract = options.get("schema_contract")
        if sink_contract is not None and sink_contract != schema_contract:
            raise DagConfigurationError(
                "schema_contract is ambiguous: top-level and sink.options.schema_contract must be identical"
            )
        options["schema_contract"] = dict(schema_contract)
        if parse_tracer:
            parse_tracer.record(
                kind="option.key",
                target="load_config.options.schema_contract",
                value=schema_contract,
                sources=("schema_contract",),
                operation="inject",
            )

    if reconciliation_options is not None:
        options["reconciliation"] = reconciliation_options
        if parse_tracer:
            parse_tracer.record(
                kind="option.key",
                target="load_config.options.reconciliation",
                value=reconciliation_options,
                sources=("reconciliation",),
                operation="normalize",
            )
    state_config = config.get("state")
    if state_config is not None:
        if not isinstance(state_config, Mapping):
            raise DagConfigurationError("state must be a mapping")
        options["state"] = dict(state_config)
        if parse_tracer:
            parse_tracer.record(
                kind="option.key",
                target="load_config.options.state",
                value=state_config,
                sources=("state",),
                operation="copy",
            )
    runtime_storage = runtime_config.get("storage")
    if isinstance(runtime_storage, dict):
        options.setdefault("runtime_storage", dict(runtime_storage))
        if parse_tracer:
            parse_tracer.record(
                kind="option.key",
                target="load_config.options.runtime_storage",
                value=runtime_storage,
                sources=("runtime.storage",),
                operation="inject",
            )
    if "quality" in config:
        quality = config["quality"]
        if not isinstance(quality, Mapping):
            raise DagConfigurationError("quality must be a mapping")
        options["quality"] = dict(quality)
        if parse_tracer:
            parse_tracer.record(
                kind="option.key",
                target="load_config.options.quality",
                value=quality,
                sources=("quality",),
                operation="inject",
            )
    return reconciliation_policy_from_options(reconciliation_options) if reconciliation_options is not None else None


def normalize_mssql_schema_label(
    *,
    dialect: object,
    database: str | None,
    schema: object,
    table: object,
) -> tuple[str | None, str]:
    schema_text = str(schema or "").strip()
    if not is_mssql(dialect) or not schema_text or table in (None, ""):
        return database, schema_text
    try:
        name = MSSQLObjectName.from_parts(
            database=database,
            schema=schema_text,
            table=str(table),
            strict=True,
        )
    except ValueError as exc:
        raise DagConfigurationError(str(exc)) from exc
    return name.database, name.schema_label


def is_mssql(value: object) -> bool:
    """Return whether a connector token uses the canonical MSSQL dialect."""

    return is_mssql_dialect(value)


def optional_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def resolve_unique_key(
    *,
    source_options: Mapping[str, object],
    strategy_config: Mapping[str, object],
    sink_options: Mapping[str, object],
) -> str | list[str] | None:
    """Resolve one unique-key authority or reject conflicting declarations."""

    candidates = {
        "source.options.unique_key": source_options.get("unique_key"),
        "sink.strategy.unique_key": strategy_config.get("unique_key"),
        "sink.options.unique_key": sink_options.get("unique_key"),
    }
    declared = {path: value for path, value in candidates.items() if value not in (None, "", [])}
    fingerprints = {_unique_key_fingerprint(value) for value in declared.values()}
    if len(fingerprints) > 1:
        paths = ", ".join(sorted(declared))
        raise DagConfigurationError(
            f"Conflicting unique_key declarations ({paths}); use sink.strategy.unique_key as the canonical authority"
        )
    selected = strategy_config.get("unique_key") or source_options.get("unique_key") or sink_options.get("unique_key")
    if selected in (None, "", []):
        return None
    fingerprint = _unique_key_fingerprint(selected)
    return fingerprint[0] if isinstance(selected, str) else list(fingerprint)


def _deep_merge(left: Mapping[str, Any], right: Mapping[str, Any]) -> dict[str, Any]:
    merged = dict(left)
    for key, value in right.items():
        current = merged.get(key)
        if isinstance(current, Mapping) and isinstance(value, Mapping):
            merged[key] = _deep_merge(current, value)
        else:
            merged[key] = value
    return merged


def _adaptive_batch_size(strategy_intelligence: dict[str, Any]) -> int:
    decision = strategy_intelligence.get("decision", {})
    batching = decision.get("adaptive_batching", {}) if isinstance(decision, dict) else {}
    return int(batching.get("initial_batch_size") or 10000)


def _unique_key_fingerprint(value: object) -> tuple[str, ...]:
    if isinstance(value, str) and value:
        return (value,)
    if isinstance(value, (list, tuple)) and value and all(isinstance(item, str) and item for item in value):
        return tuple(value)
    raise DagConfigurationError("unique_key must be a non-empty string or array of non-empty strings")


__all__ = [
    "LoadConfigParseTracer",
    "inject_endpoint_identity_options",
    "resolve_partition_contract",
    "derive_api_source",
    "inject_manifest_context_options",
    "inject_runtime_contract_options",
    "is_mssql",
    "merge_load_options",
    "normalize_mssql_schema_label",
    "optional_text",
    "record_load_fields",
    "record_runtime_contract_fields",
    "resolve_batch_size",
    "resolve_connection_identity",
    "resolve_unique_key",
]
