"""Runtime-issued proof for pre-extract MSSQL character-spool admission."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

MSSQL_CHARACTER_SPOOL_REQUIREMENT = "local_character_spool_v1"
MSSQL_CHARACTER_SPOOL_PREFLIGHT_OPTION = "__dpone_mssql_character_spool_preflight_v1"
_PREFLIGHT_AUTHORITY = object()


def bind_mssql_character_spool_preflight(load_config: Any, *, source: Any) -> Any:
    """Bind preflight only when the selected source proves a row-spool route.

    Raw manifest values cannot forge this contract because the sink accepts
    only the process-local authority object issued here.
    """

    options = dict(getattr(load_config, "options", {}) or {})
    options.pop(MSSQL_CHARACTER_SPOOL_PREFLIGHT_OPTION, None)
    resolver = getattr(source, "mssql_character_spool_preflight_requirement", None)
    requirement = resolver(load_config) if callable(resolver) else None
    if requirement is not None and requirement != MSSQL_CHARACTER_SPOOL_REQUIREMENT:
        raise RuntimeError("mssql_spool_route.preflight_requirement_invalid")
    if requirement == MSSQL_CHARACTER_SPOOL_REQUIREMENT:
        options[MSSQL_CHARACTER_SPOOL_PREFLIGHT_OPTION] = _PREFLIGHT_AUTHORITY
    return replace(load_config, options=options)


def requires_mssql_character_spool_preflight(load_config: Any) -> bool:
    """Return whether runtime issued the exact physical row-spool proof."""

    options = getattr(load_config, "options", {}) or {}
    return options.get(MSSQL_CHARACTER_SPOOL_PREFLIGHT_OPTION) is _PREFLIGHT_AUTHORITY


__all__ = [
    "MSSQL_CHARACTER_SPOOL_REQUIREMENT",
    "MSSQL_CHARACTER_SPOOL_PREFLIGHT_OPTION",
    "bind_mssql_character_spool_preflight",
    "requires_mssql_character_spool_preflight",
]
