"""Pure deployment preflight for spawned backfill process lanes.

The campaign orchestrator must reject an unusable child bootstrap before it
acquires durable leases or invokes source/target lifecycle hooks.  This module
therefore validates only the immutable spawn contract: the bootstrap must be
pickle-serializable and its top-level entrypoint must be importable and
callable in the current runtime image.  It never opens the entrypoint, creates
a process, or touches a connector.
"""

from __future__ import annotations

import pickle
from importlib import import_module
from typing import TYPE_CHECKING, Any

from dpone.backfill.portable_scope_runtime import is_postgres_mssql_backfill_route

if TYPE_CHECKING:
    from dpone.backfill.process_lane_contracts import BackfillProcessLaneBootstrap

PROCESS_BOOTSTRAP_SERIALIZATION_ERROR = "DPONE_BACKFILL_PROCESS_BOOTSTRAP_NOT_SERIALIZABLE"
PROCESS_BOOTSTRAP_FAILURE_ERROR = "DPONE_BACKFILL_PROCESS_BOOTSTRAP_FAILED"
PROCESS_ENTRYPOINT_INVALID_ERROR = "backfill.process_lane_entrypoint_invalid"
PROCESS_ENTRYPOINT_UNAVAILABLE_ERROR = "backfill.process_lane_entrypoint_unavailable"


def resolve_process_lane_entrypoint(path: str) -> Any:
    """Resolve one top-level lane opener through a stable, redacted contract."""

    module_name, separator, attribute = str(path).partition(":")
    if not separator or not module_name or not attribute or "." in attribute:
        raise RuntimeError(PROCESS_ENTRYPOINT_INVALID_ERROR)
    try:
        value = getattr(import_module(module_name), attribute, None)
    except Exception:
        raise RuntimeError(PROCESS_ENTRYPOINT_UNAVAILABLE_ERROR) from None
    if not callable(value):
        raise RuntimeError(PROCESS_ENTRYPOINT_UNAVAILABLE_ERROR)
    return value


def process_lane_bootstrap_error(bootstrap: BackfillProcessLaneBootstrap) -> str | None:
    """Return a stable public error without creating a lane or opening I/O."""

    serialization_error = process_lane_bootstrap_serialization_error(bootstrap)
    if serialization_error is not None:
        return serialization_error
    try:
        resolve_process_lane_entrypoint(bootstrap.entrypoint)
    except RuntimeError as exc:
        return f"{PROCESS_BOOTSTRAP_FAILURE_ERROR}: error={exc}"
    return None


def process_lane_bootstrap_serialization_error(
    bootstrap: BackfillProcessLaneBootstrap,
) -> str | None:
    """Retain the defensive serialization check at the lane-run boundary."""

    try:
        pickle.dumps(bootstrap)
    except Exception:
        return PROCESS_BOOTSTRAP_SERIALIZATION_ERROR
    return None


def require_process_lane_bootstrap(bootstrap: BackfillProcessLaneBootstrap) -> None:
    """Raise before campaign mutation when the immutable spawn contract fails."""

    error = process_lane_bootstrap_error(bootstrap)
    if error is not None:
        raise RuntimeError(error)


def require_process_lane_runtime(runtime: Any, load_config: Any) -> None:
    """Validate parent-only runtime capabilities before campaign mutation."""

    require_process_lane_bootstrap(runtime.bootstrap)
    if not callable(runtime.process_chunk_execution_factory):
        raise RuntimeError("backfill.process_chunk_execution_factory_unavailable")
    if not callable(getattr(runtime, "parent_control_scope", None)):
        raise RuntimeError("mssql_transaction.process_lane_control_timeout_scope_required")
    if not callable(getattr(runtime, "issue_receipt_probe", None)) or not callable(
        getattr(runtime, "validate_replay_result", None)
    ):
        raise RuntimeError("backfill.process_receipt_authority_unavailable")
    if is_postgres_mssql_backfill_route(load_config):
        if not callable(runtime.portable_scope_column_resolver):
            raise RuntimeError("backfill.process_portable_scope_column_resolver_unavailable")
        if not callable(runtime.portable_scope_history_proof):
            raise RuntimeError("backfill.process_portable_scope_history_proof_unavailable")


__all__ = [
    "PROCESS_BOOTSTRAP_FAILURE_ERROR",
    "PROCESS_BOOTSTRAP_SERIALIZATION_ERROR",
    "PROCESS_ENTRYPOINT_INVALID_ERROR",
    "PROCESS_ENTRYPOINT_UNAVAILABLE_ERROR",
    "process_lane_bootstrap_error",
    "process_lane_bootstrap_serialization_error",
    "require_process_lane_bootstrap",
    "require_process_lane_runtime",
    "resolve_process_lane_entrypoint",
]
