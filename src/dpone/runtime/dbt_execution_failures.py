"""Stable public failure-code projection for dbt execution."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.contracts.dbt_publishing import DbtPublishingError


def runtime_failure_code(error: Exception) -> str:
    """Preserve a recognized runtime code or return the generic failure."""

    code = getattr(error, "code", None)
    if isinstance(code, str) and code.startswith("DPONE_DBT_"):
        return code
    return "DPONE_DBT_EXECUTION_FAILED"


def dbt_failure_code(exit_code: int, results_error: DbtPublishingError | None) -> str:
    """Resolve command and run-results failure into one public code."""

    if exit_code != 0:
        return "DPONE_DBT_EXECUTION_FAILED"
    return results_error.code if results_error is not None else "DPONE_DBT_EXECUTION_FAILED"
