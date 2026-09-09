"""Real-vendor generic MSSQL transaction fault and authority matrix.

Every case starts from the public, CLI-provisioned production hydration
composition.  Faults are injected only at named runtime boundaries after the
real vendor assertion for that boundary has completed; target DML, generic
state DML, rollback, receipt probing, and application locks remain SQL Server
operations.
"""

from __future__ import annotations

import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from tools.route_live_certification.recorder import RouteLiveObservationRecorder

from dpone.backfill.execution_policy import normalize_backfill_execution_policy, plan_hash
from dpone.backfill.planner import plan_chunks
from dpone.backfill.runtime_execution import BackfillChunkExecutor
from dpone.config import LoadStrategy
from dpone.contracts.run_context import RunContext
from dpone.runtime.bootstrap_hydrator import DefaultRuntimeHydrator
from dpone.runtime.etl.mssql_transaction_admission import (
    ADMISSION_OPTION,
    MssqlTransactionAdmissionService,
)
from dpone.runtime.etl.processor import ETLProcessor
from dpone.runtime.sinks.load_result import AtomicCommitOutcome
from dpone.runtime.sinks.mssql_target_catalog_fingerprint import (
    assert_target_catalog_expectations,
)
from dpone.runtime.sinks.strategies.mssql.mssql_transaction_finalizer import (
    MssqlGenericCommitOutcomeUnknown,
    MssqlGenericTransactionFinalizer,
)
from dpone.runtime.sinks.strategies.mssql.mssql_transaction_lock import (
    acquire_target_lock,
)
from dpone.runtime.state.mssql_generic_transaction import (
    MssqlGenericTransactionState,
    MssqlOperationClaimRejected,
)
from dpone.runtime.state.mssql_generic_transaction_names import (
    ATTEMPT_TABLE,
    FENCE_TABLE,
    OPERATION_TABLE,
    OPERATION_TRIGGER,
    RECEIPT_TABLE,
)
from tests.integration.postgres.postgres_live_support import postgres_mssql_enabled
from tests.integration.postgres.postgres_mssql_production_hydration_live_support import (
    STAGING_SCHEMA,
    STATE_SCHEMA,
    TARGET_SCHEMA,
    close_runtime_bindings,
    generic_state_row_counts,
    production_hydration_live_fixture,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_live,
    pytest.mark.integration_postgres,
    pytest.mark.integration_mssql,
    pytest.mark.route_live_wide,
]

_DAG_ID = "DAG__integration__postgres_mssql__transaction_governance"
_FAULT_PHASES = ("admit", "stage", "mutate", "catalog_verify", "checkpoint", "receipt")


@dataclass(slots=True)
class _FaultProbe:
    phase: str
    boundary: str | None = None
    staging_rows: int | None = None
    state_assertions: int = 0
    receipt_inserted_in_transaction: bool = False

    @property
    def code(self) -> str:
        return f"route_live.transaction_fault_after_{self.phase}"

    def fail(self, boundary: str) -> None:
        self.boundary = boundary
        raise RuntimeError(self.code)


class _AdmissionHook:
    """Run one test-only callback after real production admission."""

    def __init__(self, callback: Any) -> None:
        self._delegate = MssqlTransactionAdmissionService()
        self._callback = callback

    def prepare(self, *args: Any, **kwargs: Any) -> Any:
        prepared = self._delegate.prepare(*args, **kwargs)
        self._callback(prepared)
        return prepared

    def replay_result(self, load_config: Any) -> Any:
        return self._delegate.replay_result(load_config)


class _FaultingTransactionState:
    """Inject after a real owner checkpoint or transactional receipt insert."""

    def __init__(self, delegate: MssqlGenericTransactionState, probe: _FaultProbe) -> None:
        self._delegate = delegate
        self._probe = probe

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)

    def assert_current(
        self,
        executor: Any,
        operation: Any,
        *,
        require_unexpired_lease: bool = True,
    ) -> None:
        self._delegate.assert_current(
            executor,
            operation,
            require_unexpired_lease=require_unexpired_lease,
        )
        self._probe.state_assertions += 1
        if self._probe.phase == "checkpoint" and not require_unexpired_lease:
            self._probe.fail("post_dml_owner_epoch_checkpoint")

    def insert_receipt(self, *args: Any, **kwargs: Any) -> Any:
        receipt = self._delegate.insert_receipt(*args, **kwargs)
        self._probe.receipt_inserted_in_transaction = True
        if self._probe.phase == "receipt":
            self._probe.fail("transactional_receipt_insert_verified")
        return receipt


class _FaultingFinalizer(MssqlGenericTransactionFinalizer):
    """Place deterministic faults around the real generic finalizer."""

    def __init__(self, strategy: Any, state_storage: Any, probe: _FaultProbe) -> None:
        self._probe = probe
        state: Any = MssqlGenericTransactionState.from_state_storage(state_storage)
        if probe.phase in {"checkpoint", "receipt"}:
            state = _FaultingTransactionState(state, probe)

        def catalog_revalidator(*args: Any, **kwargs: Any) -> None:
            assert_target_catalog_expectations(*args, **kwargs)
            if probe.phase == "catalog_verify" and kwargs.get("boundary") == "after":
                probe.fail("catalog_after_image_verified")

        super().__init__(
            strategy,
            state_storage,
            transaction_state=state,
            catalog_revalidator=catalog_revalidator,
        )

    def finalize(
        self,
        load_config: Any,
        admission: Any,
        handler: Any,
        staging: Any,
        **kwargs: Any,
    ) -> Any:
        if self._probe.phase == "stage":
            self._probe.staging_rows = int(staging.row_count)
            if self._probe.staging_rows <= 0:
                raise AssertionError("fault-after-stage requires a materialized vendor staging table")
            self._probe.fail("native_staging_materialized")
        if self._probe.phase == "mutate":
            real_handler = handler

            def faulting_handler(artifact: Any) -> Any:
                result = real_handler(artifact)
                self._probe.fail("business_dml_executed_inside_target_transaction")
                return result  # pragma: no cover - fail always raises.

            handler = faulting_handler
        return super().finalize(
            load_config,
            admission,
            handler,
            staging,
            **kwargs,
        )


class _BarrierFinalizer(MssqlGenericTransactionFinalizer):
    """Make two real finalizers request the same target fence together."""

    def __init__(self, strategy: Any, state_storage: Any, barrier: threading.Barrier) -> None:
        def acquire_together(connector: Any, resource: str, **kwargs: Any) -> None:
            barrier.wait(timeout=60)
            acquire_target_lock(connector, resource, **kwargs)

        super().__init__(
            strategy,
            state_storage,
            target_lock_acquirer=acquire_together,
        )


@pytest.mark.skipif(
    not postgres_mssql_enabled(),
    reason="PostgreSQL/MSSQL Docker integration is not configured",
)
def test_production_hydrated_transaction_fault_matrix_live(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    """Rollback all six finite fault phases without target mutation."""

    for phase in _FAULT_PHASES:
        with production_hydration_live_fixture(tmp_path / phase) as live:
            _apply_runtime_environment(monkeypatch, live)
            load_config = live.load_config()
            bindings = DefaultRuntimeHydrator().build(
                config=live.runtime_config,
                load_config=load_config,
            )
            probe = _FaultProbe(phase)
            try:
                admission_service = None
                if phase == "admit":
                    admission_service = _AdmissionHook(lambda _prepared: probe.fail("admission_persisted"))
                else:
                    _bind_fault_finalizer(bindings, probe)
                processor = _processor(bindings, admission_service=admission_service)
                before = _target_business_image(live)
                with pytest.raises(RuntimeError, match=probe.code):
                    processor.run(
                        load_config,
                        run_context=_run_context(f"fault-{phase}"),
                        dag_id=_DAG_ID,
                    )
                after = _target_business_image(live)
                assert after == before
                assert _staging_image(live) == ()
                state_counts = generic_state_row_counts(live.state)
                assert state_counts == {
                    FENCE_TABLE: 1,
                    ATTEMPT_TABLE: 1,
                    OPERATION_TABLE: 1,
                    RECEIPT_TABLE: 0,
                }
                assert not live.transfer_root.exists() or not tuple(live.transfer_root.iterdir())
                route_live_recorder.observe_case(
                    "transaction_governance",
                    f"fault_after_{phase}",
                    before_image=before,
                    after_image=after,
                    observations={
                        "fault_phase": phase,
                        "observed_boundary": probe.boundary,
                        "staging_rows_at_fault": probe.staging_rows,
                        "owner_epoch_assertions": probe.state_assertions,
                        "receipt_inserted_before_rollback": probe.receipt_inserted_in_transaction,
                        "state_counts_after_rollback": state_counts,
                        "staging_after_rollback": [],
                        "owned_source_artifacts_after_rollback": [],
                    },
                )
            finally:
                close_runtime_bindings(bindings)


@pytest.mark.skipif(
    not postgres_mssql_enabled(),
    reason="PostgreSQL/MSSQL Docker integration is not configured",
)
def test_production_hydrated_transaction_authority_matrix_live(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    """Prove owner, epoch, lease, receipt, and commit-ACK authority."""

    _duplicate_receipt(tmp_path / "duplicate", monkeypatch, route_live_recorder)
    _conflicting_receipt(tmp_path / "conflict", monkeypatch, route_live_recorder)
    _stale_operation_epoch(tmp_path / "stale", monkeypatch, route_live_recorder)
    _expired_owner_lease(tmp_path / "expired", monkeypatch, route_live_recorder)
    _fresh_session_ack_probe(tmp_path / "ack", monkeypatch, route_live_recorder)
    _commit_outcome_unknown(tmp_path / "unknown", monkeypatch, route_live_recorder)
    _parallel_owner_applock(tmp_path / "parallel", monkeypatch, route_live_recorder)


def _duplicate_receipt(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    recorder: RouteLiveObservationRecorder,
) -> None:
    with production_hydration_live_fixture(root) as live:
        _apply_runtime_environment(monkeypatch, live)
        load_config, bindings, processor = _runtime(live)
        try:
            context = _run_context("duplicate-receipt")
            committed = processor.run(load_config, run_context=context, dag_id=_DAG_ID)
            assert committed["commit_outcome"] == AtomicCommitOutcome.COMMITTED
            before = _target_business_image(live)
            state_before = generic_state_row_counts(live.state)
            replay = processor.run(load_config, run_context=context, dag_id=_DAG_ID)
            after = _target_business_image(live)
            state_after = generic_state_row_counts(live.state)
            assert replay["commit_outcome"] == AtomicCommitOutcome.REPLAY_SUPPRESSED
            assert replay["commit_receipt_id"] == committed["commit_receipt_id"]
            assert after == before
            assert state_after == state_before
            recorder.observe_case(
                "transaction_governance",
                "duplicate_receipt",
                before_image=before,
                after_image=after,
                observations={
                    "commit_outcome": str(replay["commit_outcome"]),
                    "receipt_id": replay["commit_receipt_id"],
                    "state_counts_before": state_before,
                    "state_counts_after": state_after,
                    "source_reextracted": False,
                },
            )
        finally:
            close_runtime_bindings(bindings)


def _conflicting_receipt(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    recorder: RouteLiveObservationRecorder,
) -> None:
    with production_hydration_live_fixture(root) as live:
        _apply_runtime_environment(monkeypatch, live)
        load_config, bindings, processor = _runtime(live)
        try:
            context = _run_context("conflicting-receipt")
            processor.run(load_config, run_context=context, dag_id=_DAG_ID)
            before = _target_business_image(live)
            state_before = generic_state_row_counts(live.state)
            conflicting = replace(
                load_config,
                options={**load_config.options, "mssql_transaction_lock_timeout_ms": 123_457},
            )
            with pytest.raises(Exception) as captured:  # noqa: B017 - vendor error is normalized below.
                processor.run(conflicting, run_context=context, dag_id=_DAG_ID)
            blocker = str(captured.value)
            assert blocker == "mssql_transaction.attempt_identity_collision"
            after = _target_business_image(live)
            state_after = generic_state_row_counts(live.state)
            assert after == before
            assert state_after == state_before
            assert _staging_image(live) == ()
            recorder.observe_case(
                "transaction_governance",
                "conflicting_receipt",
                before_image=before,
                after_image=after,
                observations={
                    "blocker": blocker,
                    "state_counts_before": state_before,
                    "state_counts_after": state_after,
                    "failed_before_source_staging": True,
                },
            )
        finally:
            close_runtime_bindings(bindings)


def _stale_operation_epoch(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    recorder: RouteLiveObservationRecorder,
) -> None:
    with production_hydration_live_fixture(root) as live:
        _apply_runtime_environment(monkeypatch, live)
        load_config = live.load_config()
        bindings = DefaultRuntimeHydrator().build(config=live.runtime_config, load_config=load_config)

        def make_stale(prepared: Any) -> None:
            admission = prepared.options[ADMISSION_OPTION]
            operation = admission.operation
            assert operation is not None
            trigger = f"[{STATE_SCHEMA}].[{OPERATION_TRIGGER}]"
            table = f"[{STATE_SCHEMA}].[{OPERATION_TABLE}]"
            live.state.execute_query(f"DISABLE TRIGGER {trigger} ON {table}")
            try:
                live.state.execute_query(
                    f"UPDATE {table} SET current_epoch = current_epoch + 1, current_owner_digest = ? "
                    "WHERE operation_key = ?",
                    (hashlib.sha256(b"route-live-stale-owner").digest(), operation.operation_key),
                )
            finally:
                live.state.execute_query(f"ENABLE TRIGGER {trigger} ON {table}")

        processor = _processor(bindings, admission_service=_AdmissionHook(make_stale))
        try:
            before = _target_business_image(live)
            with pytest.raises(RuntimeError, match="stale_source_or_operation_generation") as captured:
                processor.run(
                    load_config,
                    run_context=_run_context("stale-operation-epoch"),
                    dag_id=_DAG_ID,
                )
            after = _target_business_image(live)
            assert after == before
            assert _staging_image(live) == ()
            recorder.observe_case(
                "transaction_governance",
                "stale_operation_epoch",
                before_image=before,
                after_image=after,
                observations={
                    "blocker": str(captured.value),
                    "fault": "vendor_operation_epoch_advanced_after_admission",
                    "state_counts": generic_state_row_counts(live.state),
                },
            )
        finally:
            close_runtime_bindings(bindings)


def _expired_owner_lease(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    recorder: RouteLiveObservationRecorder,
) -> None:
    with production_hydration_live_fixture(root) as live:
        _apply_runtime_environment(monkeypatch, live)
        load_config, bindings, processor = _runtime(live)
        try:
            backfill_options = {
                "inner_mode": "incremental_merge",
                "parallel_workers": 1,
                "chunk": {
                    "column": "id",
                    "from": "1",
                    "to": "1",
                    "step": "1",
                    "kind": "integer",
                },
                "max_chunks": 1,
                "retry_policy": "non_committed",
                "backfill_id": "route-live-expired-owner",
                "predicate_dialect": "generic",
                "lease_ttl_minutes": 1,
            }
            authored = replace(
                load_config,
                load_strategy=LoadStrategy.BACKFILL,
                unique_key=["id"],
                options={**load_config.options, "backfill": backfill_options},
            )
            policy = normalize_backfill_execution_policy(backfill_options)
            assert policy.chunk is not None
            chunks = plan_chunks(policy.chunk, run_key="route-live-expired-owner")
            load_config = BackfillChunkExecutor._chunk_load_config(
                authored,
                chunks[0],
                run_key="route-live-expired-owner",
                plan_hash=plan_hash(chunks, execution_policy=policy),
                owner="expired-owner",
                lease_expires_at_utc=datetime.now(UTC) - timedelta(seconds=1),
            )
            before = _target_business_image(live)
            with pytest.raises(MssqlOperationClaimRejected) as captured:
                processor.run(
                    load_config,
                    run_context=_run_context("expired-owner-lease"),
                    dag_id=_DAG_ID,
                )
            after = _target_business_image(live)
            assert captured.value.code == "mssql_transaction.operation_lease_expired"
            assert captured.value.vendor_token == "DPONE_LOAD_OPERATION_LEASE_EXPIRED"
            assert after == before
            assert _staging_image(live) == ()
            recorder.observe_case(
                "transaction_governance",
                "expired_owner_lease",
                before_image=before,
                after_image=after,
                observations={
                    "blocker": str(captured.value),
                    "failed_before_source_staging": True,
                    "state_counts": generic_state_row_counts(live.state),
                },
            )
        finally:
            close_runtime_bindings(bindings)


def _fresh_session_ack_probe(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    recorder: RouteLiveObservationRecorder,
) -> None:
    with production_hydration_live_fixture(root) as live:
        _apply_runtime_environment(monkeypatch, live)
        load_config, bindings, processor = _runtime(live)
        target = bindings.sink_obj.connector
        real_commit = target.commit_transaction

        def commit_then_lose_ack() -> None:
            real_commit()
            raise OSError("route-live injected commit ACK loss")

        target.commit_transaction = commit_then_lose_ack
        try:
            before = _target_business_image(live)
            committed = processor.run(
                load_config,
                run_context=_run_context("fresh-session-ack-probe"),
                dag_id=_DAG_ID,
            )
            after = _target_business_image(live)
            assert committed["commit_outcome"] == AtomicCommitOutcome.COMMITTED_AFTER_RECEIPT_PROBE
            assert after != before
            recorder.observe_case(
                "transaction_governance",
                "fresh_session_ack_probe",
                before_image=before,
                after_image=after,
                observations={
                    "commit_outcome": str(committed["commit_outcome"]),
                    "receipt_id": committed["commit_receipt_id"],
                    "fresh_session_receipt_proved_commit": True,
                    "state_counts": generic_state_row_counts(live.state),
                },
            )
        finally:
            close_runtime_bindings(bindings)


def _commit_outcome_unknown(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    recorder: RouteLiveObservationRecorder,
) -> None:
    with production_hydration_live_fixture(root) as live:
        _apply_runtime_environment(monkeypatch, live)
        load_config, bindings, processor = _runtime(live)

        def lose_before_commit() -> None:
            raise OSError("route-live injected pre-ACK transport loss")

        bindings.sink_obj.connector.commit_transaction = lose_before_commit
        try:
            before = _target_business_image(live)
            with pytest.raises(MssqlGenericCommitOutcomeUnknown) as captured:
                processor.run(
                    load_config,
                    run_context=_run_context("commit-outcome-unknown"),
                    dag_id=_DAG_ID,
                )
            after = _target_business_image(live)
            staging = _staging_image(live)
            assert after == before
            assert staging
            state_counts = generic_state_row_counts(live.state)
            assert state_counts[RECEIPT_TABLE] == 0
            recorder.observe_case(
                "transaction_governance",
                "commit_outcome_unknown",
                before_image=before,
                after_image=after,
                observations={
                    "blocker": str(captured.value),
                    "fresh_session_receipt": None,
                    "staging_preserved": staging,
                    "state_counts": state_counts,
                    "operator_resolution_required": True,
                },
            )
        finally:
            close_runtime_bindings(bindings)


def _parallel_owner_applock(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    recorder: RouteLiveObservationRecorder,
) -> None:
    with production_hydration_live_fixture(root) as live:
        _apply_runtime_environment(monkeypatch, live)
        baseline_config, baseline_bindings, baseline_processor = _runtime(live)
        bindings_a = None
        bindings_b = None
        try:
            baseline_processor.run(
                baseline_config,
                run_context=_run_context("parallel-baseline"),
                dag_id=_DAG_ID,
            )
            before = _target_business_image(live)
            config_a = live.load_config()
            config_b = live.load_config()
            bindings_a = DefaultRuntimeHydrator().build(config=live.runtime_config, load_config=config_a)
            bindings_b = DefaultRuntimeHydrator().build(config=live.runtime_config, load_config=config_b)
            barrier = threading.Barrier(2)
            _bind_barrier_finalizer(bindings_a, barrier)
            _bind_barrier_finalizer(bindings_b, barrier)
            processor_a = _processor(bindings_a)
            processor_b = _processor(bindings_b)
            context = _run_context("parallel-owner-applock")
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = (
                    pool.submit(processor_a.run, config_a, run_context=context, dag_id=_DAG_ID),
                    pool.submit(processor_b.run, config_b, run_context=context, dag_id=_DAG_ID),
                )
                outcomes: list[dict[str, Any]] = []
                blockers: list[str] = []
                for future in futures:
                    try:
                        outcomes.append(future.result(timeout=180))
                    except RuntimeError as exc:
                        blockers.append(str(exc))
            commit_outcomes = {str(value["commit_outcome"]) for value in outcomes}
            assert commit_outcomes == {str(AtomicCommitOutcome.COMMITTED)}
            assert blockers == ["mssql_transaction.concurrent_receipt_payload_mismatch"]
            after = _target_business_image(live)
            assert after == before
            assert _staging_image(live) == ()
            recorder.observe_case(
                "transaction_governance",
                "parallel_owner_applock",
                before_image=before,
                after_image=after,
                observations={
                    "contenders": 2,
                    "same_operation_identity": True,
                    "commit_outcomes": sorted(commit_outcomes),
                    "serialized_loser_blockers": blockers,
                    "state_counts": generic_state_row_counts(live.state),
                    "target_rows_unchanged": True,
                },
            )
        finally:
            close_runtime_bindings(bindings_b)
            close_runtime_bindings(bindings_a)
            close_runtime_bindings(baseline_bindings)


def _runtime(live: Any) -> tuple[Any, Any, ETLProcessor]:
    load_config = live.load_config()
    bindings = DefaultRuntimeHydrator().build(config=live.runtime_config, load_config=load_config)
    return load_config, bindings, _processor(bindings)


def _processor(bindings: Any, *, admission_service: Any | None = None) -> ETLProcessor:
    return ETLProcessor(
        source=bindings.source_obj,
        sink=bindings.sink_obj,
        etl_logger=bindings.etl_logger,
        run_state_storage=bindings.run_state_storage,
        load_identity_service=bindings.load_identity_service,
        mssql_transaction_admission_service=admission_service,
    )


def _bind_fault_finalizer(bindings: Any, probe: _FaultProbe) -> None:
    strategy = bindings.sink_obj._strategy_map[LoadStrategy.FULL_REFRESH]
    strategy.transaction_finalizer_factory = lambda value, storage: _FaultingFinalizer(value, storage, probe)


def _bind_barrier_finalizer(bindings: Any, barrier: threading.Barrier) -> None:
    strategy = bindings.sink_obj._strategy_map[LoadStrategy.FULL_REFRESH]
    strategy.transaction_finalizer_factory = lambda value, storage: _BarrierFinalizer(value, storage, barrier)


def _run_context(case_id: str) -> RunContext:
    return RunContext(
        run_id=f"route-live-{case_id}",
        config={
            "pipeline_id": "route-live-transaction-governance",
            "task_id": case_id,
        },
    )


def _apply_runtime_environment(monkeypatch: pytest.MonkeyPatch, live: Any) -> None:
    for name, value in live.runtime_environment.items():
        monkeypatch.setenv(name, value)


def _target_business_image(live: Any) -> dict[str, Any]:
    exists = bool(
        live.target.get_records(
            "SELECT CASE WHEN OBJECT_ID(?, N'U') IS NULL THEN 0 ELSE 1 END AS exists_flag",
            (f"{TARGET_SCHEMA}.{live.target_table}",),
            as_dict=True,
        )[0]["exists_flag"]
    )
    rows: list[dict[str, Any]] = []
    if exists:
        rows = list(
            live.target.get_records(
                f"SELECT [id], [metric_code], [metric_value], [note] "
                f"FROM [{TARGET_SCHEMA}].[{live.target_table}] ORDER BY [id]",
                as_dict=True,
            )
        )
    return {"exists": exists, "rows": rows}


def _staging_image(live: Any) -> tuple[dict[str, Any], ...]:
    return tuple(
        live.target.get_records(
            "SELECT t.name AS table_name, SUM(p.rows) AS row_count "
            "FROM sys.tables AS t INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
            "INNER JOIN sys.partitions AS p ON p.object_id = t.object_id AND p.index_id IN (0, 1) "
            "WHERE s.name = ? GROUP BY t.name ORDER BY t.name",
            (STAGING_SCHEMA,),
            as_dict=True,
        )
    )
