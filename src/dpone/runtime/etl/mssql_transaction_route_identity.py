"""Canonical route-contract normalization for governed MSSQL transactions."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, is_dataclass
from enum import Enum
from typing import Any

from dpone.backfill.execution_policy import (
    BACKFILL_RUNTIME_AUTHORITY_OPTION,
    execution_policy_from_load_config,
)
from dpone.backfill.shadow_append_authority import SHADOW_APPEND_AUTHORITY_OPTION
from dpone.config.load_config import ROUTE_IDENTITY_V1_ENDPOINT_TYPES_OPTION
from dpone.contracts.mssql_transaction_governance import (
    MssqlTransactionContractError,
    route_fingerprint,
)
from dpone.contracts.portable_relation_scope import (
    portable_scope_contract,
    resolve_portable_relation_scope,
)
from dpone.contracts.portable_scope_binding import (
    PORTABLE_SCOPE_BINDING_OPTION,
    require_portable_scope_binding,
)
from dpone.contracts.source_physical_identity import SourcePhysicalIdentity
from dpone.runtime.etl.mssql_transaction_wire_identity import (
    is_postgres_mssql_wire_route,
    mssql_codec_contract,
)
from dpone.runtime.mssql_spool_route import MSSQL_CHARACTER_SPOOL_PREFLIGHT_OPTION

_RUNTIME_OPTION_KEYS = {
    "__dpone_load_identity",
    BACKFILL_RUNTIME_AUTHORITY_OPTION,
    SHADOW_APPEND_AUTHORITY_OPTION,
    "__dpone_mssql_transaction_admission",
    "__dpone_mssql_transaction_lease",
    "__dpone_mssql_operation_scope",
    MSSQL_CHARACTER_SPOOL_PREFLIGHT_OPTION,
    PORTABLE_SCOPE_BINDING_OPTION,
    "__dpone_postgres_mssql_internal_wire",
    "_source_connector",
    "_source_conn_id",
    "_source_schema",
    "_source_table",
    "load_id",
    "run_id",
    "source_custom_predicate",
    ROUTE_IDENTITY_V1_ENDPOINT_TYPES_OPTION,
}


def invocation_route_fingerprint(
    load_config: Any,
    *,
    target_identity: bytes,
    source_identity: SourcePhysicalIdentity,
) -> bytes:
    """Bind source/target/strategy/physical/wire authority, excluding chunk scope."""

    raw_options = getattr(load_config, "options", {}) or {}
    options = _invocation_options(raw_options)
    identity_options = _v1_identity_options(raw_options, options)
    portable_scope = _route_portable_scope_identity(load_config)
    contract = {
        "source": {
            "connection_ref": load_config.source_conn_id,
            "physical": source_identity.to_dict(),
            "database": getattr(load_config, "source_database", None),
            "schema": load_config.source_schema,
            "table": load_config.source_table,
            "global_predicate": _global_source_predicate(load_config),
            "portable_scope": portable_scope,
        },
        "target": {
            "connection_ref": load_config.target_conn_id,
            "identity": target_identity.hex(),
            "database": load_config.target_database,
            "schema": load_config.target_schema,
            "table": load_config.target_table,
        },
        "strategy": {
            "mode": load_config.load_strategy.value,
            "unique_key": _canonical(load_config.unique_key),
            "only_new_rows": load_config.only_new_rows,
            "micro_batch_commit": load_config.micro_batch_commit,
            "overwrite_type": load_config.overwrite_type,
            "merge_policy": load_config.merge_policy,
            "duplicate_policy": load_config.duplicate_policy,
            "allow_non_recommended_policy": load_config.allow_non_recommended_policy,
            "mutations_sync": load_config.mutations_sync,
            "dedup_expression": load_config.dedup_expression,
            "dedup_target": load_config.dedup_target,
            "with_dedup": load_config.with_dedup,
            "custom_predicate": _global_target_predicate(load_config),
            "portable_scope": portable_scope,
            "partition": _canonical(load_config.partition),
            "reconciliation_policy": _canonical(load_config.reconciliation_policy),
            "runtime_override_policy": {
                "force_full_refresh_baseline": True,
                "backfill_inner_mode": _backfill_inner_mode(options),
                "backfill_execution_policy_sha256": _backfill_execution_policy_digest(load_config),
            },
        },
        "wire": {
            "public_export_format": load_config.export_format,
            "runtime_export_format": (
                "mssql-delimited" if is_postgres_mssql_wire_route(options) else load_config.export_format
            ),
            "compress_export": load_config.compress_export,
            "codec": mssql_codec_contract(options),
        },
        "options": _canonical(identity_options),
    }
    return route_fingerprint(contract)


def _invocation_options(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        raise MssqlTransactionContractError("mssql_transaction.options_not_mapping")
    result = {str(key): value for key, value in raw.items() if str(key) not in _RUNTIME_OPTION_KEYS}
    backfill = result.get("backfill")
    if isinstance(backfill, Mapping):
        result["backfill"] = {
            str(key): value
            for key, value in backfill.items()
            if key not in {"advisor", "chunk_context", "predicate_dialect", "state_dir"}
        }
    return result


def _v1_identity_options(raw: Any, canonical_options: Mapping[str, Any]) -> dict[str, Any]:
    """Project authored endpoint tokens only into the persisted v1 identity."""

    if not isinstance(raw, Mapping):
        raise MssqlTransactionContractError("mssql_transaction.options_not_mapping")
    result = dict(canonical_options)
    _restore_v1_authored_endpoint_types(raw, result)
    return result


def _restore_v1_authored_endpoint_types(raw: Mapping[str, Any], result: dict[str, Any]) -> None:
    """Reproduce the pre-canonicalization endpoint portion of v1 identity."""

    projection = raw.get(ROUTE_IDENTITY_V1_ENDPOINT_TYPES_OPTION)
    if projection is None:
        return
    if not isinstance(projection, Mapping):
        raise MssqlTransactionContractError("mssql_transaction.v1_endpoint_identity_invalid")
    for field in ("source_type", "sink_type"):
        value = projection.get(field)
        if not isinstance(value, str) or not value.strip():
            raise MssqlTransactionContractError("mssql_transaction.v1_endpoint_identity_invalid")
        result[field] = value


def _global_source_predicate(load_config: Any) -> Any:
    options = getattr(load_config, "options", {}) or {}
    scope = options.get("__dpone_mssql_operation_scope") if isinstance(options, Mapping) else None
    if isinstance(scope, Mapping):
        return scope.get("global_source_predicate")
    return options.get("source_custom_predicate") if isinstance(options, Mapping) else None


def _global_target_predicate(load_config: Any) -> Any:
    options = getattr(load_config, "options", {}) or {}
    scope = options.get("__dpone_mssql_operation_scope") if isinstance(options, Mapping) else None
    if isinstance(scope, Mapping):
        return scope.get("global_target_predicate")
    return load_config.custom_predicate


def _portable_scope_identity(load_config: Any) -> dict[str, Any] | None:
    scope = resolve_portable_relation_scope(load_config)
    if scope is None:
        return None
    binding = require_portable_scope_binding(load_config, scope)
    return {
        "ast": portable_scope_contract(scope),
        "binding": binding.to_contract(),
    }


def _route_portable_scope_identity(load_config: Any) -> dict[str, Any] | None:
    """Exclude runtime chunk ranges from invocation-wide route identity."""

    strategy = getattr(getattr(load_config, "load_strategy", None), "value", None)
    return None if strategy == "backfill" else _portable_scope_identity(load_config)


def _backfill_inner_mode(options: Mapping[str, Any]) -> Any:
    backfill = options.get("backfill")
    return backfill.get("inner_mode") if isinstance(backfill, Mapping) else None


def _backfill_execution_policy_digest(load_config: Any) -> str | None:
    mode = getattr(getattr(load_config, "load_strategy", None), "value", getattr(load_config, "load_strategy", None))
    if str(mode or "") != "backfill":
        return None
    try:
        return execution_policy_from_load_config(load_config).digest
    except ValueError as exc:
        raise MssqlTransactionContractError("mssql_transaction.backfill_execution_policy_invalid") from exc


def _canonical(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        if value != value or value in {float("inf"), float("-inf")}:
            raise MssqlTransactionContractError("mssql_transaction.identity_not_canonical_json")
        return value
    if isinstance(value, Enum):
        return _canonical(value.value)
    if is_dataclass(value) and not isinstance(value, type):
        return _canonical(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _canonical(item) for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_canonical(item) for item in value]
    raise MssqlTransactionContractError(f"mssql_transaction.identity_value_unsupported:{type(value).__name__}")


__all__ = ["invocation_route_fingerprint"]
