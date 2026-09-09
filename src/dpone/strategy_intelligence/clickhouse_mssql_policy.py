"""Fail-closed auto-strategy policy for ClickHouse to SQL Server loads."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol

from dpone.strategy_intelligence.models import StrategySignal


class ClickHouseMSSQLContext(Protocol):
    """Strategy hints consumed by the ClickHouse-to-MSSQL safety policy."""

    partition_column: str | None
    source_cursor: str | None
    source_options: Mapping[str, object] | None
    unique_key: Sequence[str]


def resolve_clickhouse_mssql_auto_strategy(
    context: ClickHouseMSSQLContext,
    reasons: list[StrategySignal],
    warnings: list[StrategySignal],
) -> str:
    """Use full refresh until an authored complete replacement boundary exists."""

    if context.partition_column or _has_window_hint(context.source_options):
        reasons.append(
            StrategySignal(
                code="clickhouse_mssql_partition_window_unverified",
                severity="high",
                message=(
                    "ClickHouse to MSSQL partition hints do not prove that the source slice is complete "
                    "or that an expected empty target partition is owned by the run."
                ),
                action="Request partition_replace explicitly only after certifying the complete window.",
            )
        )
        warnings.append(_complete_boundary_warning())
        return "full_refresh"
    if context.source_cursor:
        reasons.append(
            StrategySignal(
                code="clickhouse_mssql_cursor_unsupported",
                severity="high",
                message=(
                    "A target-derived ClickHouse cursor is not a durable source boundary; use deterministic "
                    "full refresh until a complete replacement window is declared."
                ),
            )
        )
        warnings.append(_complete_boundary_warning())
        return "full_refresh"
    if context.unique_key:
        reasons.append(
            StrategySignal(
                code="clickhouse_mssql_key_without_boundary",
                severity="high",
                message=(
                    "A unique key makes target DML idempotent but does not define an atomic ClickHouse "
                    "changed-row boundary."
                ),
            )
        )
        warnings.append(_complete_boundary_warning())
        return "full_refresh"
    reasons.append(
        StrategySignal(
            code="clickhouse_mssql_full_refresh_safe_default",
            severity="medium",
            message=(
                "ClickHouse to MSSQL auto mode uses full_refresh until an explicit complete-window "
                "strategy is selected and certified."
            ),
        )
    )
    return "full_refresh"


def _has_window_hint(options: Mapping[str, object] | None) -> bool:
    options = dict(options or {})
    return bool(
        options.get("source_custom_predicate")
        or options.get("custom_predicate")
        or (options.get("date_from") is not None and options.get("date_to") is not None)
    )


def _complete_boundary_warning() -> StrategySignal:
    return StrategySignal(
        code="clickhouse_mssql_complete_boundary_required",
        severity="error",
        message=(
            "ClickHouse to MSSQL incremental loading requires a complete bounded replace, partition_replace, "
            "or backfill source boundary."
        ),
        action="Declare and certify a complete window, or keep full_refresh for bootstrap and repair.",
    )


__all__ = ["ClickHouseMSSQLContext", "resolve_clickhouse_mssql_auto_strategy"]
