"""PostgreSQL source strategies.

Exports are lazy so importing lightweight PostgreSQL pieces does not pull the
BigQuery-backed xmin runtime unless it is actually needed.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "PostgresBaseStrategy",
    "PostgresFullExtractStrategy",
    "PostgresXMinExtractStrategy",
]

_EXPORTS: dict[str, str] = {
    "PostgresBaseStrategy": "dpone.runtime.sources.strategies.postgres.postgres_base:PostgresBaseStrategy",
    "PostgresFullExtractStrategy": "dpone.runtime.sources.strategies.postgres.postgres_full_extract:PostgresFullExtractStrategy",
    "PostgresXMinExtractStrategy": "dpone.runtime.sources.strategies.postgres.postgres_xmin_extract:PostgresXMinExtractStrategy",
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if not target:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr = target.split(":")
    mod = import_module(module_name)
    value = getattr(mod, attr)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(list(globals().keys()) + list(__all__)))
