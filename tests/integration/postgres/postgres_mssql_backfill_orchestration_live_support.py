"""Real-runtime support for the reviewed PostgreSQL→MSSQL backfill suite.

The helpers in this module deliberately start at :class:`DefaultProcessRunner`.
They do not call a sink strategy directly and do not manufacture observations
from connector doubles.  Every process is hydrated from the same verified
runtime context used by the production-hydration live proof.
"""

from __future__ import annotations

import json
import traceback
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from tools.route_live_certification.reviewed_cases_runtime import backfill_orchestration_suite

from dpone.config import LoadStrategy
from dpone.contracts.run_context import RunContext
from dpone.dag.config_models import ETLProcessConfig
from dpone.dag.process import ETLProcess
from dpone.runtime.bootstrap_runner import DefaultProcessRunner
from tests.integration.postgres.postgres_mssql_production_hydration_live_support import (
    SOURCE_SCHEMA,
    STAGING_SCHEMA,
    STATE_SCHEMA,
    TARGET_SCHEMA,
    ProductionHydrationLiveFixture,
    close_runtime_bindings,
    generic_state_row_counts,
)

SUITE_ID = "backfill_orchestration"
_DAG_ID = "DAG__integration__postgres_mssql__backfill_orchestration"
_PHYSICAL_DESIGN = {"indexes": {"primary_key": ["id"]}}


@dataclass(frozen=True, slots=True)
class ReviewedBackfillCase:
    """Decoded immutable reviewed case used as a pytest parameter."""

    case_id: str
    inner_mode: str
    parallel_workers: int
    lifecycle: str
    lease_ttl_minutes: int
    parameters: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ProcessInvocation:
    """Public runner result or the exact exception raised by it."""

    result: Any | None
    error: BaseException | None
    traceback: str | None


def reviewed_backfill_cases() -> tuple[ReviewedBackfillCase, ...]:
    """Return all and only the 30 reviewed backfill cases."""

    decoded: list[ReviewedBackfillCase] = []
    for reviewed in backfill_orchestration_suite().cases:
        document = json.loads(reviewed.config_json)
        parameters = dict(document["parameters"])
        decoded.append(
            ReviewedBackfillCase(
                case_id=reviewed.case_id,
                inner_mode=str(parameters["inner_mode"]),
                parallel_workers=int(parameters["parallel_workers"]),
                lifecycle=str(parameters["lifecycle"]),
                lease_ttl_minutes=int(parameters["lease_ttl_minutes"]),
                parameters=parameters,
            )
        )
    cases = tuple(decoded)
    if len(cases) != 30 or len({case.case_id for case in cases}) != 30:
        raise AssertionError("backfill live matrix must collect the exact reviewed 30-case authority")
    return cases


def apply_runtime_environment(monkeypatch: Any, live: ProductionHydrationLiveFixture) -> None:
    """Install the signed, secret-free runtime-context references for one case."""

    for name, value in live.runtime_environment.items():
        monkeypatch.setenv(name, value)


def seed_target_with_public_runner(live: ProductionHydrationLiveFixture) -> Any:
    """Create the target through a genuine full-refresh process before the case."""

    baseline = live.load_config()
    options = dict(baseline.options)
    options["physical_design"] = _PHYSICAL_DESIGN
    invocation = invoke_public_process(
        live,
        replace(baseline, options=options),
        invocation_id="backfill-baseline",
    )
    if invocation.error is not None:
        raise invocation.error
    assert invocation.result is not None
    assert invocation.result.status == "success"
    assert invocation.result.final_rows == 3
    live.postgres.execute_query(
        f'UPDATE "{SOURCE_SCHEMA}"."{live.source_table}" '
        "SET metric_value = metric_value + 1000.0, "
        "note = COALESCE(note, '') || '|bounded-backfill'"
    )
    return invocation.result


def backfill_load_config(
    live: ProductionHydrationLiveFixture,
    case: ReviewedBackfillCase,
    *,
    state_dir: Path,
) -> Any:
    """Build a complete bounded integer plan with governed MSSQL state.

    Lease duration uses the production minute-based authoring contract. Fault
    timing is injected by the test lifecycle and is never a manifest option.
    """

    baseline = live.load_config()
    options = dict(baseline.options)
    options["physical_design"] = _PHYSICAL_DESIGN
    options["backfill"] = {
        "inner_mode": case.inner_mode,
        "parallel_workers": case.parallel_workers,
        "chunk": {
            "column": "id",
            "from": "1",
            "to": "3",
            "step": "1",
            "kind": "integer",
        },
        "max_chunks": 3,
        "state": {
            "backend": "audit_schema",
            "schema": TARGET_SCHEMA,
            "require_distributed_lock": True,
        },
        "state_dir": str(state_dir),
        "retry_policy": "non_committed",
        "backfill_id": f"route-live-{case.case_id}",
        "predicate_dialect": "generic",
        "lease_ttl_minutes": case.lease_ttl_minutes,
    }
    kwargs: dict[str, Any] = {
        "load_strategy": LoadStrategy.BACKFILL,
        "options": options,
        "custom_predicate": None,
        "unique_key": None,
        "partition": {},
    }
    if case.inner_mode == "partition_replace":
        kwargs["partition"] = {
            "column": "id",
            "values_from_staging": True,
            "native_mode": "fallback",
        }
    elif case.inner_mode == "incremental_merge":
        kwargs["unique_key"] = ["id"]
        kwargs["merge_policy"] = "delete_insert"
    return replace(baseline, **kwargs)


def invoke_public_process(
    live: ProductionHydrationLiveFixture,
    load_config: Any,
    *,
    invocation_id: str,
    scheduler_run_id: str | None = None,
    backfill_orchestrator_factory: Any | None = None,
    runtime_config: dict[str, Any] | None = None,
) -> ProcessInvocation:
    """Execute and close one production-hydrated public process invocation."""

    config = ETLProcessConfig(
        name=invocation_id,
        load_config=load_config,
        load_strategy=load_config.load_strategy,
        unique_key=load_config.unique_key,
        raw_config=dict(runtime_config or live.runtime_config),
    )
    process = ETLProcess(config=config)
    result = None
    error: BaseException | None = None
    error_traceback: str | None = None
    try:
        result = DefaultProcessRunner(
            backfill_orchestrator_factory=backfill_orchestrator_factory,
        ).run(
            process,
            context=RunContext(
                run_id=scheduler_run_id or invocation_id,
                config={
                    "pipeline_id": "postgres-mssql-backfill-live",
                    "task_id": invocation_id,
                },
            ),
            dag_id=_DAG_ID,
        )
    except BaseException as exc:  # The live case asserts the exact typed boundary.
        error = exc
        error_traceback = traceback.format_exc()
    finally:
        bindings = None
        if config.source_obj is not None or config.sink_obj is not None:
            bindings = type(
                "_Bindings",
                (),
                {
                    "source_obj": config.source_obj,
                    "sink_obj": config.sink_obj,
                },
            )()
        close_runtime_bindings(bindings)
    return ProcessInvocation(result=result, error=error, traceback=error_traceback)


def semantic_image(live: ProductionHydrationLiveFixture) -> dict[str, Any]:
    """Read business rows and only durable committed checkpoint authorities."""

    receipt_rows = live.state.get_records(
        f"SELECT r.receipt_id, r.operation_key, r.attempt_key, r.scope_hash, "
        f"r.declared_rows, r.actual_native_rows, r.committed_at_utc "
        f"FROM [{STATE_SCHEMA}].[dpone_load_receipt] AS r "
        f"INNER JOIN [{STATE_SCHEMA}].[dpone_load_attempt] AS a "
        "ON a.attempt_key = r.attempt_key "
        "WHERE a.target_database = ? AND a.target_schema = ? AND a.target_table = ? "
        "ORDER BY r.committed_at_utc, r.receipt_id",
        (live.target_database, TARGET_SCHEMA, live.target_table),
        as_dict=True,
    )
    return {
        "business_rows": target_business_rows(live),
        "committed_receipts": tuple(_json_safe_row(row) for row in receipt_rows),
        "committed_chunks": tuple(
            row["chunk_index"] for row in backfill_chunk_rows(live) if str(row.get("status")) == "success"
        ),
    }


def operational_image(live: ProductionHydrationLiveFixture, transfer_root: Path) -> dict[str, Any]:
    """Read catalog, staging, receipt, ledger, session and cleanup evidence."""

    return {
        "semantic": semantic_image(live),
        "generic_state_counts": generic_state_row_counts(live.state),
        "backfill_chunks": tuple(_json_safe_row(row) for row in backfill_chunk_rows(live)),
        "backfill_chunk_journal": tuple(_json_safe_row(row) for row in backfill_chunk_journal_rows(live)),
        "backfill_campaigns": tuple(_json_safe_row(row) for row in backfill_campaign_rows(live)),
        "staging_objects": staging_objects(live),
        "target_catalog": target_catalog(live),
        "target_primary_key": target_primary_key(live),
        "target_sessions": target_sessions(live),
        "transfer_files": tuple(
            sorted(str(path.relative_to(transfer_root)) for path in transfer_root.rglob("*") if path.is_file())
        )
        if transfer_root.exists()
        else (),
    }


def target_business_rows(live: ProductionHydrationLiveFixture) -> tuple[dict[str, Any], ...]:
    rows = live.target.get_records(
        f"SELECT [id], [metric_code], [metric_value], [note] "
        f"FROM [{live.target_database}].[{TARGET_SCHEMA}].[{live.target_table}] ORDER BY [id]",
        as_dict=True,
    )
    return tuple(_json_safe_row(row) for row in rows)


def backfill_chunk_rows(live: ProductionHydrationLiveFixture) -> tuple[dict[str, Any], ...]:
    if not _table_exists(live, TARGET_SCHEMA, "__dpone__backfill_chunks"):
        return ()
    rows = live.target.get_records(
        f"SELECT journal_id, run_key, chunk_index, status, start_value, end_value, "
        f"idempotency_key, run_id, load_id, error, details_json "
        f"FROM [{live.target_database}].[{TARGET_SCHEMA}].[__dpone__backfill_chunks] "
        "ORDER BY chunk_index, journal_id",
        as_dict=True,
    )
    latest = {(str(row["run_key"]), int(row["chunk_index"])): dict(row) for row in rows}
    return tuple(latest[key] for key in sorted(latest))


def backfill_chunk_journal_rows(
    live: ProductionHydrationLiveFixture,
) -> tuple[dict[str, Any], ...]:
    """Read every append-only transition in the same order as runtime load."""

    if not _table_exists(live, TARGET_SCHEMA, "__dpone__backfill_chunks"):
        return ()
    rows = live.target.get_records(
        f"SELECT journal_id, run_key, chunk_index, status, details_json, __dpone__loaded_at, "
        "sys.fn_PhysLocFormatter(%%physloc%%) AS physical_row_locator "
        f"FROM [{live.target_database}].[{TARGET_SCHEMA}].[__dpone__backfill_chunks] "
        "ORDER BY chunk_index ASC, journal_id DESC",
        as_dict=True,
    )
    return tuple(dict(row) for row in rows)


def backfill_campaign_rows(live: ProductionHydrationLiveFixture) -> tuple[dict[str, Any], ...]:
    if not _table_exists(live, TARGET_SCHEMA, "__dpone__backfill_campaigns"):
        return ()
    rows = live.target.get_records(
        f"SELECT journal_id, run_key, dataset, inner_mode, status, plan_hash, config_hash, "
        f"chunk_config_json, details_json "
        f"FROM [{live.target_database}].[{TARGET_SCHEMA}].[__dpone__backfill_campaigns] "
        "ORDER BY journal_id",
        as_dict=True,
    )
    return tuple(dict(row) for row in rows)


def staging_objects(live: ProductionHydrationLiveFixture) -> tuple[dict[str, Any], ...]:
    rows = live.target.get_records(
        "SELECT s.name AS schema_name, o.name AS object_name, o.type_desc "
        "FROM sys.objects AS o INNER JOIN sys.schemas AS s ON s.schema_id = o.schema_id "
        "WHERE s.name = ? AND o.is_ms_shipped = 0 ORDER BY o.name",
        (STAGING_SCHEMA,),
        as_dict=True,
    )
    return tuple(dict(row) for row in rows)


def target_catalog(live: ProductionHydrationLiveFixture) -> tuple[dict[str, Any], ...]:
    rows = live.target.get_records(
        "SELECT c.column_id, c.name AS column_name, t.name AS type_name, "
        "c.max_length, c.precision, c.scale, c.is_nullable "
        "FROM sys.columns AS c "
        "INNER JOIN sys.tables AS tb ON tb.object_id = c.object_id "
        "INNER JOIN sys.schemas AS s ON s.schema_id = tb.schema_id "
        "INNER JOIN sys.types AS t ON t.user_type_id = c.user_type_id "
        "WHERE s.name = ? AND tb.name = ? ORDER BY c.column_id",
        (TARGET_SCHEMA, live.target_table),
        as_dict=True,
    )
    return tuple(dict(row) for row in rows)


def target_primary_key(live: ProductionHydrationLiveFixture) -> tuple[dict[str, Any], ...]:
    """Read the exact catalog-backed primary-key authority for the target."""

    rows = live.target.get_records(
        "SELECT i.name AS index_name, i.type_desc, i.is_unique, i.is_primary_key, "
        "c.name AS column_name, ic.key_ordinal "
        "FROM sys.tables AS tb "
        "INNER JOIN sys.schemas AS s ON s.schema_id = tb.schema_id "
        "INNER JOIN sys.indexes AS i ON i.object_id = tb.object_id AND i.is_primary_key = 1 "
        "INNER JOIN sys.index_columns AS ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id "
        "INNER JOIN sys.columns AS c ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
        "WHERE s.name = ? AND tb.name = ? AND ic.key_ordinal > 0 ORDER BY ic.key_ordinal",
        (TARGET_SCHEMA, live.target_table),
        as_dict=True,
    )
    return tuple(dict(row) for row in rows)


def target_sessions(live: ProductionHydrationLiveFixture) -> tuple[dict[str, Any], ...]:
    rows = live.target.get_records(
        "SELECT s.session_id, s.status, s.open_transaction_count, "
        "DB_NAME(s.database_id) AS database_name "
        "FROM sys.dm_exec_sessions AS s "
        "WHERE s.is_user_process = 1 AND DB_NAME(s.database_id) = ? "
        "ORDER BY s.session_id",
        (live.target_database,),
        as_dict=True,
    )
    return tuple(dict(row) for row in rows)


def _table_exists(live: ProductionHydrationLiveFixture, schema: str, table: str) -> bool:
    rows = live.target.get_records(
        "SELECT CASE WHEN OBJECT_ID(?, 'U') IS NULL THEN 0 ELSE 1 END",
        (f"{schema}.{table}",),
    )
    return bool(rows and rows[0][0])


def _json_safe_row(row: Any) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in dict(row).items():
        if isinstance(value, bytes):
            result[str(key)] = value.hex()
        elif hasattr(value, "isoformat"):
            result[str(key)] = value.isoformat()
        else:
            result[str(key)] = value
    return result


__all__ = [
    "SUITE_ID",
    "ProcessInvocation",
    "ReviewedBackfillCase",
    "apply_runtime_environment",
    "backfill_load_config",
    "invoke_public_process",
    "operational_image",
    "reviewed_backfill_cases",
    "seed_target_with_public_runner",
    "semantic_image",
    "target_primary_key",
]
