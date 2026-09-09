# ruff: noqa: F822
"""ETL logging compatibility facade."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "ETLLogger",
    "etl_logger",
]

_EXPORTS: dict[str, str] = {
    "ETLLogger": "dpone.runtime.etl_logging.etl_logger:ETLLogger",
    "etl_logger": "dpone.runtime.etl_logging.etl_logger:etl_logger",
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr = target.split(":")
    value = getattr(import_module(module_name), attr)
    globals()[name] = value
    return value
