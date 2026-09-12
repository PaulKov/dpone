"""Process-local handoff for the selected PostgreSQL-to-MSSQL R1 runtime."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from copy import copy
from dataclasses import replace
from typing import Any

POSTGRES_MSSQL_R1_EXECUTION_OPTION = "__dpone_postgres_mssql_r1_execution"


class PostgresMssqlR1ExecutionError(RuntimeError):
    """The selected R1 route lost or contradicted its execution authority."""

    code = "DPONE_POSTGRES_MSSQL_PROFILE_WEAKER_THAN_REQUIRED"

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(f"{self.code}:{reason}")


def bind_postgres_mssql_r1_execution(load_config: Any, runtime: object) -> Any:
    """Bind one exact process-scoped runtime without mutating manifest state."""

    options = dict(getattr(load_config, "options", {}) or {})
    existing = options.get(POSTGRES_MSSQL_R1_EXECUTION_OPTION)
    if existing is not None and existing is not runtime:
        raise PostgresMssqlR1ExecutionError("execution_context_conflict")
    options[POSTGRES_MSSQL_R1_EXECUTION_OPTION] = runtime
    try:
        return replace(load_config, options=options)
    except TypeError:
        cloned = copy(load_config)
        try:
            setattr(cloned, "options", options)
        except (AttributeError, TypeError) as error:
            raise PostgresMssqlR1ExecutionError("copyable_load_config_required") from error
        return cloned


def require_postgres_mssql_r1_execution(load_config: Any, *, expected: object) -> object:
    """Require the exact runtime identity selected before endpoint/source I/O."""

    options = getattr(load_config, "options", None)
    actual = options.get(POSTGRES_MSSQL_R1_EXECUTION_OPTION) if isinstance(options, Mapping) else None
    if actual is None:
        raise PostgresMssqlR1ExecutionError("execution_context_missing")
    if actual is not expected:
        raise PostgresMssqlR1ExecutionError("execution_context_rebound")
    return actual


def r1_execution_from_load_config(load_config: Any) -> object | None:
    """Return only an in-process object; serialized manifest values are rejected."""

    options = getattr(load_config, "options", None)
    candidate = options.get(POSTGRES_MSSQL_R1_EXECUTION_OPTION) if isinstance(options, Mapping) else None
    if candidate is None:
        return None
    if isinstance(candidate, Mapping | str | bytes | int | float | bool):
        raise PostgresMssqlR1ExecutionError("serialized_execution_context_forbidden")
    return candidate


def require_r1_execution_method(execution: object, method: str) -> Callable[..., Any]:
    """Resolve one narrow mode-specific operation or fail before legacy dispatch."""

    candidate = getattr(execution, method, None)
    if not callable(candidate):
        raise PostgresMssqlR1ExecutionError(f"execution_method_missing:{method}")
    return candidate


__all__ = [
    "POSTGRES_MSSQL_R1_EXECUTION_OPTION",
    "PostgresMssqlR1ExecutionError",
    "bind_postgres_mssql_r1_execution",
    "r1_execution_from_load_config",
    "require_postgres_mssql_r1_execution",
    "require_r1_execution_method",
]
