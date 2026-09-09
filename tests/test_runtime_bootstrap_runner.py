from __future__ import annotations

import pickle
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from dpone.backfill.worker_runtime import BackfillProcessLaneRuntime
from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.run_context import RunContext
from dpone.runtime.bootstrap_runner import DefaultProcessRunner, _worker_chunk_runner_factory
from dpone.runtime.etl.backfill_process_runtime import BackfillProcessLanePayload
from dpone.runtime.route_runtime_factory import RouteCapabilityRuntimeFactory


def test_default_process_runner_exposes_runtime_evidence_details(monkeypatch) -> None:
    started_at = datetime(2026, 7, 11, 12, 0, tzinfo=UTC)

    class _Processor:
        def __init__(self, **kwargs):
            self.load_governance_service = kwargs["load_governance_service"]

    def _execute(
        processor,
        load_config,
        run_context,
        *,
        dag_id=None,
        execution_date=None,
        worker_chunk_runner_factory=None,
        worker_state_store_factory=None,
    ):
        del (
            load_config,
            run_context,
            dag_id,
            execution_date,
            worker_chunk_runner_factory,
            worker_state_store_factory,
        )
        processor.load_governance_service.record_load_step(
            load_record=SimpleNamespace(run_id="run-1", load_id="load-1"),
            step_id="staged",
            phase="load_governance",
            kind="target_preparation",
            status="succeeded",
            started_at=started_at,
            details={"staged_rows": 10, "session_token": "must-not-leak"},
        )
        return {
            "status": "success",
            "inserted_rows": 10,
            "updated_rows": 0,
            "final_rows": 10,
            "extracted_rows": 10,
            "duration_seconds": 5.0,
            "errors": [],
            "run_throughput": {
                "schema_version": "dpone.runtime.throughput.v1",
                "scope": "run",
                "duration_seconds": 5.0,
                "row_count": 10,
                "rows_per_second": 2.0,
            },
            "route_capabilities": {"summary": {"selected_route_id": "typed_binary_streaming"}},
        }

    import dpone.runtime.etl.backfill_orchestrator as backfill_orchestrator
    import dpone.runtime.etl.processor as processor_module
    import dpone.runtime.governance.service as governance_service

    monkeypatch.setattr(processor_module, "ETLProcessor", _Processor)
    monkeypatch.setattr(backfill_orchestrator, "execute_process_with_backfill", _execute)
    monkeypatch.setattr(governance_service, "_utc_now", lambda: started_at + timedelta(seconds=4))

    process = SimpleNamespace(
        config=SimpleNamespace(
            name="orders",
            source_obj=object(),
            sink_obj=object(),
            load_config=SimpleNamespace(log_sample_rows=0),
            etl_logger=object(),
            run_state_storage=None,
            partition_checkpoint_store=None,
            credential_resolution_receipts=(
                {
                    "connection_ref": "source-main",
                    "resolver": "vault_kv",
                    "resolved_version": 17,
                    "token": "must-not-leak",
                },
            ),
            ensure_runtime_bindings=lambda: None,
        ),
        current_state=None,
    )

    result = DefaultProcessRunner(route_capability_factory=_NoopRouteFactory()).run(
        process,
        context=RunContext(run_id="run-1"),
    )

    assert result.details is not None
    assert result.details["run_throughput"]["rows_per_second"] == 2.0
    assert result.details["route_capabilities"]["summary"]["selected_route_id"] == "typed_binary_streaming"
    assert result.details["credential_resolution"] == [
        {
            "connection_ref": "source-main",
            "resolver": "vault_kv",
            "resolved_version": 17,
            "token": "[REDACTED]",
        }
    ]
    assert result.details["load_steps"] == [
        {
            "run_id": "run-1",
            "load_id": "load-1",
            "step_id": "staged",
            "phase": "load_governance",
            "kind": "target_preparation",
            "status": "succeeded",
            "started_at": started_at.isoformat(),
            "finished_at": (started_at + timedelta(seconds=4)).isoformat(),
            "duration_seconds": 4.0,
            "error_message": None,
            "details_json": {
                "staged_rows": 10,
                "session_token": "[REDACTED]",
                "throughput": {
                    "schema_version": "dpone.runtime.throughput.v1",
                    "scope": "load_step",
                    "duration_seconds": 4.0,
                    "rate_type": "counter_over_wall_clock",
                    "row_count": 10,
                    "row_count_source": "staged_rows",
                    "rows_per_second": 2.5,
                    "confidence": "measured",
                },
            },
        }
    ]
    assert "must-not-leak" not in str(result.to_dict())


def test_default_process_runner_resolves_governance_service_at_run_time(monkeypatch) -> None:
    backfill_orchestrator_factory = object()

    class _GovernanceService:
        created = False

        def __init__(self, *, audit_storage):
            _GovernanceService.created = True
            self.audit_storage = audit_storage

    class _Processor:
        def __init__(self, **kwargs):
            assert isinstance(kwargs["load_governance_service"], _GovernanceService)

    def _execute(
        processor,
        load_config,
        run_context,
        *,
        dag_id=None,
        execution_date=None,
        worker_chunk_runner_factory=None,
        worker_state_store_factory=None,
        orchestrator_factory=None,
    ):
        del (
            processor,
            load_config,
            run_context,
            dag_id,
            execution_date,
            worker_chunk_runner_factory,
            worker_state_store_factory,
        )
        assert orchestrator_factory is backfill_orchestrator_factory
        return {
            "status": "success",
            "inserted_rows": 0,
            "updated_rows": 0,
            "final_rows": 0,
            "extracted_rows": 0,
            "duration_seconds": 0.0,
            "errors": [],
        }

    import dpone.runtime.etl.backfill_orchestrator as backfill_orchestrator
    import dpone.runtime.etl.processor as processor_module
    import dpone.runtime.governance.service as governance_service

    monkeypatch.setattr(processor_module, "ETLProcessor", _Processor)
    monkeypatch.setattr(backfill_orchestrator, "execute_process_with_backfill", _execute)
    monkeypatch.setattr(governance_service, "LoadGovernanceService", _GovernanceService)

    process = SimpleNamespace(
        config=SimpleNamespace(
            name="orders",
            source_obj=object(),
            sink_obj=object(),
            load_config=SimpleNamespace(log_sample_rows=0),
            etl_logger=object(),
            run_state_storage=None,
            partition_checkpoint_store=None,
            ensure_runtime_bindings=lambda: None,
        ),
        current_state=None,
    )

    DefaultProcessRunner(
        route_capability_factory=_NoopRouteFactory(),
        backfill_orchestrator_factory=backfill_orchestrator_factory,
    ).run(
        process,
        context=RunContext(run_id="run-1"),
    )

    assert _GovernanceService.created is True


class _NoopRouteFactory:
    def build(self, **kwargs):
        del kwargs
        return None


def test_target_atomic_worker_factory_projects_only_spawn_safe_raw_inputs() -> None:
    load_config = LoadConfig(
        source_conn_id="source-ref",
        target_conn_id="target-ref",
        source_schema="public",
        source_table="orders",
        target_schema="dbo",
        target_table="orders",
        load_strategy=LoadStrategy.BACKFILL,
        options={
            "backfill": {
                "parallel_workers": 4,
                "chunk": {"column": "id", "from": 1, "to": 10, "step": 1},
            }
        },
    )
    raw_config = {
        "source": {"connection_ref": "source-ref"},
        "sink": {"connection_ref": "target-ref"},
    }
    live_connector = object()
    process_config = SimpleNamespace(
        load_config=load_config,
        raw_config=raw_config,
        source_obj=SimpleNamespace(connector=live_connector),
        sink_obj=SimpleNamespace(
            connector=object(),
            state_storage=SimpleNamespace(
                atomicity="target_atomic",
                connector=live_connector,
                database="DWH",
                schema="audit",
            ),
        ),
    )
    run_context = RunContext("scheduled-1", config={"process": "orders"})

    runtime = _worker_chunk_runner_factory(
        process_config,
        route_capability_factory=RouteCapabilityRuntimeFactory(),
        run_context=run_context,
        dag_id="DAG__orders",
        execution_date=None,
    )

    assert isinstance(runtime, BackfillProcessLaneRuntime)
    payload = runtime.bootstrap.payload
    assert isinstance(payload, BackfillProcessLanePayload)
    assert payload.raw_config == raw_config
    assert payload.raw_config is not raw_config
    assert payload.run_context is run_context
    assert live_connector is not payload.raw_config
    assert callable(runtime.portable_scope_history_proof)
    pickle.dumps(runtime.bootstrap)


def test_target_atomic_process_bootstrap_rejects_unserializable_custom_route_factory() -> None:
    load_config = LoadConfig(
        source_conn_id="source-ref",
        target_conn_id="target-ref",
        source_schema="public",
        source_table="orders",
        target_schema="dbo",
        target_table="orders",
        load_strategy=LoadStrategy.BACKFILL,
        options={"backfill": {"parallel_workers": 2}},
    )
    process_config = SimpleNamespace(
        load_config=load_config,
        raw_config={"source": {}, "sink": {}},
        sink_obj=SimpleNamespace(state_storage=SimpleNamespace(atomicity="target_atomic")),
    )

    with pytest.raises(
        RuntimeError,
        match="mssql_transaction.parallel_backfill_custom_route_factory_unsupported",
    ):
        _worker_chunk_runner_factory(
            process_config,
            route_capability_factory=_NoopRouteFactory(),
            run_context=RunContext("scheduled-1"),
            dag_id=None,
            execution_date=None,
        )
