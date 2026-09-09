"""Fail-closed route rules for runtime-owned backfill portable scopes."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.contracts.connector_declarations import canonical_endpoint_type
from dpone.contracts.sink_dialect import is_mssql_dialect


def is_mssql_backfill_route(load_config: Any) -> bool:
    """Return whether one backfill route targets SQL Server."""

    options = getattr(load_config, "options", {})
    return isinstance(options, Mapping) and is_mssql_dialect(options.get("sink_type"))


def is_postgres_mssql_backfill_route(load_config: Any) -> bool:
    """Return whether one config crosses PostgreSQL SQL into SQL Server."""

    options = getattr(load_config, "options", {})
    if not isinstance(options, Mapping):
        return False
    source = canonical_endpoint_type(str(options.get("source_type") or ""))
    return source == "postgres" and is_mssql_backfill_route(load_config)


def backfill_scope_authoring_violation(load_config: Any) -> tuple[str, str] | None:
    """Return one pure authoring violation shared by check, plan and runtime."""

    if not is_postgres_mssql_backfill_route(load_config):
        return None
    options = getattr(load_config, "options", {})
    if not isinstance(options, Mapping):
        return None
    backfill = options.get("backfill")
    if not isinstance(backfill, Mapping) or not backfill.get("chunk"):
        return None
    if _has_predicate(getattr(load_config, "custom_predicate", None)) or _has_predicate(
        options.get("source_custom_predicate")
    ):
        return (
            "backfill.cross_dialect_raw_predicate_unsupported",
            "use the runtime-owned portable chunk range",
        )
    if getattr(load_config, "portable_scope", None) is not None:
        return (
            "backfill.portable_scope_is_runtime_owned",
            "remove the authored portable_scope; the trusted planner binds each chunk scope",
        )
    return None


def validate_backfill_scope_authoring(load_config: Any) -> None:
    """Reject raw/composite authority before ledger or source/catalog I/O.

    A cross-database chunk is issued by the planner as one typed range AST.
    The finite v1 AST cannot safely combine an authored SQL predicate or a
    separately authored portable scope with that runtime-owned range.
    """

    violation = backfill_scope_authoring_violation(load_config)
    if violation is not None:
        blocker, detail = violation
        raise ValueError(f"{blocker}: {detail}")


def _has_predicate(value: Any) -> bool:
    return value is not None and (not isinstance(value, str) or bool(value.strip()))


__all__ = [
    "backfill_scope_authoring_violation",
    "is_mssql_backfill_route",
    "is_postgres_mssql_backfill_route",
    "validate_backfill_scope_authoring",
]
