"""Narrow Airflow public-interface compatibility predicates."""

from __future__ import annotations

from importlib import import_module
from typing import Any


def requested_module_is_absent(exc: ModuleNotFoundError, requested: str) -> bool:
    """Return true only when the requested module itself is unavailable."""

    missing = exc.name
    return isinstance(missing, str) and (missing == requested or requested.startswith(f"{missing}."))


def python_operator_class() -> type[Any]:
    """Resolve the public Airflow 3 class with the narrow Airflow 2 fallback."""

    requested = "airflow.providers.standard.operators.python"
    try:
        module = import_module(requested)
    except ModuleNotFoundError as exc:
        if not requested_module_is_absent(exc, requested):
            raise
        module = import_module("airflow.operators.python")
    return module.PythonOperator


__all__ = ["python_operator_class", "requested_module_is_absent"]
