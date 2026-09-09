"""Target access, DDL application, and evidence helpers for schema evolution."""

from __future__ import annotations

from typing import Any

from dpone.readiness.ddl_execution import GovernedDdlExecutor
from dpone.readiness.ddl_governance import SchemaChangeLedger
from dpone.readiness.schema_evolution import ColumnDef, SchemaPlan
from dpone.runtime.schema_evolution_options import (
    SchemaEvolutionError,
    SchemaEvolutionRuntimeOptions,
    blocked_schema_evolution,
)
from dpone.runtime.schema_evolution_payload import columns_from_schema
from dpone.runtime.schema_evolution_target import qualified_target, sink_dialect


def target_schema(load_config: Any, sink: Any) -> list[ColumnDef]:
    """Read the strongest target schema exposed by a sink adapter."""

    if hasattr(sink, "get_target_columns"):
        try:
            return columns_from_schema(sink.get_target_columns(load_config))
        except Exception as exc:
            raise blocked_schema_evolution(
                "schema_evolution.target_catalog_unavailable: exact target metadata could not be verified"
            ) from exc
    if hasattr(sink, "get_target_schema"):
        return columns_from_schema(sink.get_target_schema(load_config))
    connector = getattr(sink, "connector", None)
    if connector is None:
        return []
    if hasattr(connector, "fetch_schema"):
        return columns_from_schema(connector.fetch_schema(load_config.target_schema, load_config.target_table))
    if hasattr(connector, "get_table_column_types"):
        values = connector.get_table_column_types(
            load_config.target_schema,
            load_config.target_table,
        )
        return [ColumnDef(str(column), str(dtype)) for column, dtype in values.items()]
    return []


def target_exists(load_config: Any, sink: Any, columns: list[ColumnDef]) -> bool:
    probe = getattr(sink, "target_table_exists", None)
    return bool(probe(load_config)) if callable(probe) else bool(columns)


def target_row_count(
    options: SchemaEvolutionRuntimeOptions,
    load_config: Any,
    sink: Any,
) -> int | None:
    """Read an exact configured budget metric or fail closed."""

    if options.max_table_size_for_inline_ddl is None:
        return None
    probe = getattr(sink, "get_target_row_count", None)
    if not callable(probe):
        return None
    try:
        value = probe(load_config)
    except Exception as exc:
        raise blocked_schema_evolution(
            "schema_evolution.table_size_unavailable: target row-count probe failed"
        ) from exc
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise blocked_schema_evolution(
            "schema_evolution.table_size_invalid: target row-count probe must return a non-negative integer"
        )
    return value


def apply_schema_plan(
    load_config: Any,
    sink: Any,
    plan: SchemaPlan,
    governed_plan: Any | None,
) -> None:
    """Apply only non-MSSQL-governed plans at the legacy runtime boundary."""

    if governed_plan is not None:
        if hasattr(sink, "apply_governed_schema_plan"):
            sink.apply_governed_schema_plan(load_config, governed_plan)
            return
        connector = getattr(sink, "connector", None)
        if connector is not None and hasattr(connector, "execute_query"):
            GovernedDdlExecutor().apply(connector=connector, governed_plan=governed_plan)
            return
    if hasattr(sink, "apply_schema_plan"):
        sink.apply_schema_plan(load_config, plan)
        return
    connector = getattr(sink, "connector", None)
    if connector is None or not hasattr(connector, "execute_query"):
        raise SchemaEvolutionError(f"sink {sink.__class__.__name__} cannot apply schema evolution plans")
    for statement in plan.ddl_sql(
        sink_dialect(sink),
        qualified_target(load_config, sink),
    ):
        connector.execute_query(statement)


def report_plan(logger: Any | None, plan: SchemaPlan) -> None:
    if logger is None or not hasattr(logger, "log_etl_progress"):
        return
    logger.log_etl_progress(
        "SCHEMA_EVOLUTION_PLAN",
        {
            "Changes": len(plan.changes),
            "SafeChanges": len(plan.safe_changes),
            "Breaking": plan.has_breaking_changes,
            "GeneratedColumns": plan.generated_columns,
        },
    )


def report_governance_plan(logger: Any | None, plan: Any) -> None:
    if logger is None or not hasattr(logger, "log_etl_progress"):
        return
    logger.log_etl_progress(
        "ONLINE_SCHEMA_EVOLUTION_PLAN",
        {
            "OnlineEligible": plan.online_eligible,
            "Blockers": plan.blockers,
            "Actions": len(plan.actions),
        },
    )


def record_schema_ledger(
    options: SchemaEvolutionRuntimeOptions,
    load_config: Any,
    sink: Any,
    plan: Any,
) -> None:
    if not options.ledger_path:
        return
    run_id = str(getattr(load_config, "run_id", "") or f"{load_config.target_schema}.{load_config.target_table}")
    SchemaChangeLedger(options.ledger_path).record(
        run_id=run_id,
        table=qualified_target(load_config, sink),
        dialect=sink_dialect(sink),
        plan=plan,
    )


__all__ = [
    "apply_schema_plan",
    "record_schema_ledger",
    "report_governance_plan",
    "report_plan",
    "target_exists",
    "target_row_count",
    "target_schema",
]
