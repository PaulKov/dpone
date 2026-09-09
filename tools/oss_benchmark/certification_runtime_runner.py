"""Execution adapters for local benchmark certification scenarios."""

from __future__ import annotations

import concurrent.futures
import traceback
from collections.abc import Callable
from pathlib import Path
from time import perf_counter
from typing import Any

from tools.oss_benchmark.certification_runtime_catalog import ScenarioSpec
from tools.oss_benchmark.certification_runtime_golden import (
    run_artifact_contract_golden,
    run_cdc_replay_golden,
    run_incremental_golden,
    run_nested_lineage_golden,
    run_schema_evolution_golden,
)
from tools.oss_benchmark.certification_runtime_models import check_result, utc_now

ScenarioHandler = Callable[[ScenarioSpec, Path], dict[str, Any]]


class LocalScenarioRunner:
    """Run deterministic local scenarios with timeout and normalized errors."""

    def __init__(self, *, handler_map: dict[str, ScenarioHandler] | None = None) -> None:
        self._handler_map = handler_map or _default_handlers()

    def run(self, scenario: ScenarioSpec, run_dir: Path, *, timeout_seconds: float) -> dict[str, Any]:
        """Run one scenario and return a normalized ledger record."""

        started_at = utc_now()
        started = perf_counter()
        scenario_dir = run_dir / scenario.scenario_id
        try:
            record = self._run_with_timeout(scenario, scenario_dir, timeout_seconds=timeout_seconds)
            error_class = None
            last_error = None
        except concurrent.futures.TimeoutError:
            record = _failed_execution_record(scenario, "timeout", f"Scenario exceeded {timeout_seconds:g}s")
            error_class = "timeout"
            last_error = record["last_error"]
        except Exception as exc:  # pragma: no cover - exercised through error classification tests as needed.
            record = _failed_execution_record(scenario, exc.__class__.__name__, str(exc))
            record["traceback"] = traceback.format_exc(limit=5)
            error_class = exc.__class__.__name__
            last_error = str(exc)
        finished_at = utc_now()
        return _with_timing(
            record,
            scenario,
            started_at=started_at,
            finished_at=finished_at,
            duration_ms=int((perf_counter() - started) * 1000),
            error_class=error_class,
            last_error=last_error,
        )

    def _run_with_timeout(
        self, scenario: ScenarioSpec, scenario_dir: Path, *, timeout_seconds: float
    ) -> dict[str, Any]:
        handler = self._handler_map[scenario.scenario_id]
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        future = executor.submit(handler, scenario, scenario_dir)
        try:
            return future.result(timeout=timeout_seconds)
        finally:
            executor.shutdown(wait=False, cancel_futures=True)


def _default_handlers() -> dict[str, ScenarioHandler]:
    return {
        "nested-lineage": run_nested_lineage_golden,
        "incremental": run_incremental_golden,
        "cdc-replay": run_cdc_replay_golden,
        "schema-evolution": run_schema_evolution_golden,
        "artifact-contract": run_artifact_contract_golden,
    }


def _failed_execution_record(scenario: ScenarioSpec, error_class: str, message: str) -> dict[str, Any]:
    return {
        "scenario_id": scenario.scenario_id,
        "category": scenario.category,
        "runner": scenario.runner,
        "required": scenario.required,
        "status": "failed",
        "input_hash": None,
        "output_hash": None,
        "artifact_paths": [],
        "row_counts": {},
        "contract_checks": [
            check_result(
                "scenario_execution",
                False,
                expected="scenario completes within timeout",
                actual=error_class,
                message=message,
            )
        ],
        "error_class": error_class,
        "last_error": message,
    }


def _with_timing(
    record: dict[str, Any],
    scenario: ScenarioSpec,
    *,
    started_at: str,
    finished_at: str,
    duration_ms: int,
    error_class: str | None,
    last_error: str | None,
) -> dict[str, Any]:
    updated = {
        "scenario_id": scenario.scenario_id,
        "category": scenario.category,
        "runner": scenario.runner,
        "required": scenario.required,
        **record,
    }
    updated["started_at"] = started_at
    updated["finished_at"] = finished_at
    updated["duration_ms"] = duration_ms
    updated["error_class"] = error_class if error_class is not None else record.get("error_class")
    updated["last_error"] = last_error if last_error is not None else record.get("last_error")
    return updated
