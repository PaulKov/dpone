"""Run-state regressions for typed post-commit quality failures."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.contracts.quality_failure import QualityGateFailureOutcome
from dpone.governance.quality import QualityGateReport, QualityGateResult
from dpone.runtime.etl.processor import ETLProcessor
from dpone.runtime.governance.service import QualityGateFailure
from dpone.runtime.state import RunState


def test_post_commit_quality_outcome_populates_failed_run_state_and_result() -> None:
    failure = _quality_failure(boundary="post_commit")
    storage = _CapturingRunStateStorage()
    logger = _CapturingLogger()
    processor = ETLProcessor(
        _Source(),
        _Sink(),
        etl_logger=logger,
        run_state_storage=storage,
        extracted_payload_load_service=_FailingLoadService(failure),
    )

    with pytest.raises(QualityGateFailure) as raised:
        processor.run(
            _load_config(),
            dag_id="orders-daily",
            execution_date=datetime(2026, 7, 19, tzinfo=UTC),
        )

    assert raised.value is failure
    [failed_state] = storage.updated
    assert failed_state.rows_read == 9
    assert failed_state.rows_written == 9
    assert failed_state.rows_updated == 2
    [failed_result] = logger.ended
    assert {
        "inserted_rows": failed_result["inserted_rows"],
        "updated_rows": failed_result["updated_rows"],
        "final_rows": failed_result["final_rows"],
        "extracted_rows": failed_result["extracted_rows"],
        "attempts": failed_result["attempts"],
        "failure_context": failed_result["failure_context"],
    } == {
        "inserted_rows": 7,
        "updated_rows": 2,
        "final_rows": 11,
        "extracted_rows": 9,
        "attempts": 3,
        "failure_context": {
            "failure_boundary": "post_commit",
            "target_state": "mutation_returned_success",
            "checkpoint_state": "committed",
            "source_state": "not_advanced",
            "retry_classification": "retry_via_native_resume",
        },
    }


def test_pre_commit_quality_outcome_keeps_existing_zero_failure_metrics() -> None:
    storage = _CapturingRunStateStorage()
    logger = _CapturingLogger()
    processor = ETLProcessor(
        _Source(),
        _Sink(),
        etl_logger=logger,
        run_state_storage=storage,
        extracted_payload_load_service=_FailingLoadService(_quality_failure(boundary="pre_commit")),
    )

    with pytest.raises(QualityGateFailure):
        processor.run(
            _load_config(),
            dag_id="orders-daily",
            execution_date=datetime(2026, 7, 19, tzinfo=UTC),
        )

    [failed_state] = storage.updated
    assert failed_state.rows_read == 0
    assert failed_state.rows_written == 0
    assert failed_state.rows_updated == 0
    [failed_result] = logger.ended
    assert failed_result["inserted_rows"] == 0
    assert failed_result["updated_rows"] == 0
    assert failed_result["final_rows"] == 0
    assert failed_result["extracted_rows"] == 0
    assert "attempts" not in failed_result
    assert "failure_context" not in failed_result


def _quality_failure(*, boundary: str) -> QualityGateFailure:
    report = QualityGateReport(
        results=(
            QualityGateResult(
                gate_id="target_minimum",
                type="min_rows",
                status="failed",
                severity="error",
                metrics={"row_count": 11},
                message="target row count is below the required minimum",
            ),
        )
    )
    if boundary == "post_commit":
        outcome = QualityGateFailureOutcome(
            failure_boundary="post_commit",
            target_state="mutation_returned_success",
            checkpoint_state="committed",
            source_state="not_advanced",
            retry_classification="retry_via_native_resume",
            inserted_rows=7,
            updated_rows=2,
            final_rows=11,
            extracted_rows=9,
            attempts=3,
        )
    else:
        outcome = QualityGateFailureOutcome(
            failure_boundary="pre_commit",
            target_state="not_mutated",
            checkpoint_state="not_advanced",
            source_state="not_advanced",
            retry_classification="retry_before_target_mutation",
            inserted_rows=7,
            updated_rows=2,
            final_rows=11,
            extracted_rows=9,
            attempts=3,
        )
    return QualityGateFailure(report, outcome=outcome)


def _load_config() -> LoadConfig:
    return LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="public",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={"lineage": False},
    )


class _Source:
    connector = object()

    def extract(self, load_config: LoadConfig, state: object) -> SimpleNamespace:
        del load_config, state
        return SimpleNamespace(
            artifact=SimpleNamespace(
                lookback_partitions=(),
                incremental_partitions=(),
                new_partitions=(),
            ),
            schema=(),
            state=None,
            force_full_refresh=False,
        )


class _Sink:
    connector = object()


class _FailingLoadService:
    def __init__(self, failure: QualityGateFailure) -> None:
        self._failure = failure

    def load_extracted_payload(self, **kwargs: Any) -> tuple[Any, dict[str, Any] | None]:
        del kwargs
        raise self._failure


class _CapturingRunStateStorage:
    def __init__(self) -> None:
        self.saved: list[RunState] = []
        self.updated: list[RunState] = []

    def save_run_state(self, state: RunState) -> None:
        self.saved.append(replace(state))

    def update_run_state(self, state: RunState) -> None:
        self.updated.append(replace(state))


class _CapturingLogger:
    def __init__(self) -> None:
        self.ended: list[dict[str, Any]] = []

    def log_etl_start(self, payload: dict[str, Any]) -> None:
        del payload

    def log_etl_progress(self, event: str, payload: dict[str, Any]) -> None:
        del event, payload

    def log_etl_error(self, message: str, payload: dict[str, Any]) -> None:
        del message, payload

    def log_etl_end(self, payload: dict[str, Any]) -> None:
        self.ended.append(dict(payload))

    def info(self, message: str) -> None:
        del message

    def warning(self, message: str) -> None:
        del message
