"""Route-level validation for PostgreSQL XMin execution phases."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.contracts.connector_declarations import canonical_endpoint_type
from dpone.contracts.postgres_xmin_execution import (
    PostgresXminExecutionMode,
    PostgresXminExecutionPolicy,
    postgres_xmin_execution_policy,
)


def require_postgres_xmin_execution_route(load_config: Any) -> PostgresXminExecutionPolicy:
    """Validate one explicit phase before source, state, or target I/O."""

    options = getattr(load_config, "options", None)
    policy = postgres_xmin_execution_policy(options)
    if policy.mode is PostgresXminExecutionMode.AUTO:
        return policy
    if not isinstance(options, Mapping):
        raise ValueError("postgres_xmin_handoff.options_invalid")
    source = canonical_endpoint_type(str(options.get("source_type") or ""))
    sink = canonical_endpoint_type(str(options.get("sink_type") or ""))
    if source != "postgres" or sink != "mssql":
        raise ValueError("postgres_xmin_handoff.route_unsupported")
    if str(options.get("incremental_strategy") or "").strip().casefold() != "xmin":
        _invalid(policy.mode)
    strategy = str(getattr(getattr(load_config, "load_strategy", None), "value", "") or "").strip()
    if policy.mode is PostgresXminExecutionMode.INITIAL:
        _require_initial(load_config, options, strategy)
    else:
        _require_incremental(load_config, strategy)
    return policy


def _require_initial(load_config: Any, options: Mapping[str, Any], strategy: str) -> None:
    backfill = options.get("backfill")
    chunk = backfill.get("chunk") if isinstance(backfill, Mapping) else None
    state = backfill.get("state") if isinstance(backfill, Mapping) else None
    unique_key = getattr(load_config, "unique_key", None) or options.get("unique_key")
    has_unique_key = isinstance(unique_key, (str, list, tuple)) and bool(unique_key)
    valid = (
        strategy == "backfill"
        and isinstance(backfill, Mapping)
        and isinstance(chunk, Mapping)
        and _valid_initial_chunk(chunk)
        and _valid_initial_publication_route(load_config, backfill)
        and isinstance(state, Mapping)
        and str(state.get("backend") or "").strip() == "audit_schema"
        and state.get("require_distributed_lock") is True
        and has_unique_key
    )
    if not valid:
        raise ValueError("postgres_xmin_handoff.initial_contract_invalid")


def _valid_initial_publication_route(load_config: Any, backfill: Mapping[str, Any]) -> bool:
    """Accept only the certified direct-merge or resumable shadow route."""

    inner_mode = str(backfill.get("inner_mode") or "").strip()
    if inner_mode == "incremental_merge":
        return True
    if inner_mode != "incremental_append":
        return False
    publication = backfill.get("publication")
    return (
        isinstance(publication, Mapping)
        and str(publication.get("mode") or "").strip() == "shadow_swap"
        and publication.get("retain_backup") is True
        and getattr(load_config, "only_new_rows", False) is False
    )


def _valid_initial_chunk(chunk: Mapping[str, Any]) -> bool:
    if not isinstance(chunk.get("column"), str) or not str(chunk.get("column") or "").strip():
        return False
    if str(chunk.get("kind") or "").strip().casefold() == "uuid":
        return (
            type(chunk.get("buckets")) is int
            and int(chunk["buckets"]) >= 1
            and not any(field in chunk for field in ("from", "to", "step"))
        )
    return all(field in chunk and chunk[field] is not None for field in ("from", "to", "step"))


def _require_incremental(load_config: Any, strategy: str) -> None:
    reconciliation = getattr(load_config, "reconciliation_policy", None)
    if isinstance(reconciliation, Mapping):
        enabled = reconciliation.get("enabled") is True
        mode = str(reconciliation.get("mode") or "")
    else:
        enabled = getattr(reconciliation, "enabled", None) is True
        mode = str(getattr(reconciliation, "mode", "") or "")
    if strategy != "incremental_merge" or not enabled or mode != "key_snapshot":
        raise ValueError("postgres_xmin_handoff.incremental_contract_invalid")


def _invalid(mode: PostgresXminExecutionMode) -> None:
    code = "initial_contract_invalid" if mode is PostgresXminExecutionMode.INITIAL else "incremental_contract_invalid"
    raise ValueError(f"postgres_xmin_handoff.{code}")


__all__ = ["require_postgres_xmin_execution_route"]
