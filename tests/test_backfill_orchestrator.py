from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier, Event
from types import SimpleNamespace

import pytest

import dpone.backfill.process_lane_parent as process_lane_parent
from dpone.backfill import (
    BackfillChunkRecord,
    BackfillLedger,
    backfill_run_key,
    chunk_spec_from_options,
    config_hash,
    inner_mode_from_options,
    normalize_backfill_execution_policy,
    plan_chunks,
    plan_hash,
)
from dpone.backfill.campaign_contract import campaign_identity_hashes
from dpone.backfill.campaign_lease import BackfillCampaignLease, BackfillCampaignLeaseError
from dpone.backfill.chunk_lifecycle import (
    BackfillChunkLifecycleContext,
    DefaultBackfillChunkLifecycle,
)
from dpone.backfill.execution_contract import BACKFILL_OPERATION_SCOPE_OPTION
from dpone.backfill.lease_heartbeat import BackfillLeaseHeartbeat, BackfillLeaseHeartbeatError
from dpone.backfill.mapping import (
    AIRFLOW_MAPPING_SELECTION_CONTEXT_KEY,
    AirflowBackfillMappingViolation,
    build_airflow_mapping_plan,
    parse_airflow_mapping_item_json,
    serialize_airflow_mapping_item,
)
from dpone.backfill.portable_scope_campaign import CampaignPortableScopeBinding
from dpone.backfill.process_chunk_execution import BackfillProcessChunkExecution
from dpone.backfill.process_lane_contracts import (
    BackfillProcessLaneBootstrap,
    parent_issued_operation_lease_expiry,
)
from dpone.backfill.progress import backfill_progress
from dpone.backfill.state import BackfillPublicationRecord, FileBackfillStateStore
from dpone.backfill.worker_runtime import BackfillProcessLaneRuntime
from dpone.backfill.xmin_handoff import PostgresXminInitialHandoffLifecycle
from dpone.backfill.xmin_handoff_models import BackfillXminHandoffRecord
from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.config.mssql_strategy_contract import MSSQLStrategyContractError
from dpone.contracts.mssql_transaction_governance import (
    InvocationIdentity,
    MssqlAttemptRequest,
    MssqlOperationRequest,
)
from dpone.contracts.portable_scope_binding import (
    PORTABLE_SCOPE_BINDING_OPTION,
    PortableScopeBinding,
    PortableScopeColumnContract,
)
from dpone.contracts.run_context import RunContext
from dpone.runtime.etl.backfill_orchestrator import (
    BackfillOrchestrator,
    execute_process_with_backfill,
    is_chunked_backfill,
)
from dpone.runtime.etl.portable_scope_preflight import PortableScopeColumnResolver


def _config(tmp_path: Path, **backfill) -> LoadConfig:
    options = {
        "source_type": "mssql",
        "sink_type": "clickhouse",
        "backfill": {
            "inner_mode": "partition_replace",
            "state_dir": str(tmp_path / "state"),
            "chunk": {"column": "d", "from": "2025-01-01", "to": "2025-01-04", "step": "1d"},
            **backfill,
        },
    }
    return LoadConfig(
        source_conn_id="src",
        target_conn_id="tgt",
        source_schema="dbo",
        source_table="orders",
        target_schema="analytics",
        target_table="orders",
        load_strategy=LoadStrategy.BACKFILL,
        options=options,
    )


def _mapped_config(tmp_path: Path) -> LoadConfig:
    config = _config(tmp_path)
    config.options["sink_type"] = "postgres"
    config.options["backfill"]["state"] = {
        "backend": "audit_schema",
        "require_distributed_lock": True,
    }
    return config


@contextmanager
def _open_spawn_executor_test_lane(worker_id, payload, operation_lease_factory):
    events_path = Path(str(payload["events_path"]))

    def record(**event):
        with events_path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(event, sort_keys=True) + "\n")

    record(event="opened", worker_id=worker_id)

    def run(load_config):
        operation_lease_factory.authorize_receipt_probe(load_config, load_id="spawn-test-load")
        index = load_config.options["backfill"]["chunk_context"]["index"]
        binding = load_config.options.get(PORTABLE_SCOPE_BINDING_OPTION)
        operation_scope = load_config.options.get(BACKFILL_OPERATION_SCOPE_OPTION) or {}
        record(
            event="chunk",
            worker_id=worker_id,
            chunk_index=index,
            portable_scope_binding=isinstance(binding, PortableScopeBinding),
            portable_scope_ast_sha256=getattr(binding, "ast_sha256", None),
            operation_lease_expires_at_utc=operation_scope.get("lease_expires_at_utc"),
        )
        if payload.get("fail") is True:
            raise RuntimeError("reviewed chunk failure after dispatch")
        return {"status": "success", "extracted_rows": 1, "loaded_rows": 1, "load_id": "spawn-test-load"}

    try:
        yield run
    finally:
        record(event="closed", worker_id=worker_id)


def _test_receipt_authority(_dispatch, load_id, _portable_binding):
    attempt = MssqlAttemptRequest(
        invocation=InvocationIdentity("dpone-backfill:test", "test", "test:test"),
        target_identity=b"t" * 32,
        route_fingerprint=b"r" * 32,
        load_id=load_id,
        target_database="DWH",
        target_schema="dbo",
        target_table="orders",
        strategy="backfill",
    )
    return attempt, MssqlOperationRequest(b"s" * 32, b"o" * 32)


def _test_replay_result(*_args) -> bool:
    return True


@contextmanager
def _test_parent_control_scope(_store):
    yield


class _RecordingRunner:
    def __init__(self, fail_on: set[int] | None = None) -> None:
        self.calls: list[LoadConfig] = []
        self.fail_on = fail_on or set()

    def __call__(self, chunk_config: LoadConfig):
        self.calls.append(chunk_config)
        index = chunk_config.options["backfill"]["chunk_context"]["index"]
        if index in self.fail_on:
            raise RuntimeError(f"chunk {index} exploded")
        return {"extracted_rows": 10, "loaded_rows": 10, "run_id": f"run-{index}", "load_id": f"load-{index}"}


def test_backfill_ledger_publication_is_backward_compatible_and_round_trips() -> None:
    ledger = BackfillLedger(
        run_key="campaign-a",
        dataset="dbo.orders",
        inner_mode="incremental_append",
        chunk_config={},
    )
    legacy = ledger.to_jsonable()
    legacy.pop("publication")
    legacy.pop("portable_scope_column_contract")

    assert BackfillLedger.from_dict(legacy).publication is None
    assert BackfillLedger.from_dict(legacy).portable_scope_column_contract is None

    for corrupt in ("not-a-contract", ["not", "a", "contract"]):
        malformed = dict(legacy)
        malformed["portable_scope_column_contract"] = corrupt
        with pytest.raises(ValueError, match="portable_scope.binding.campaign_contract_invalid"):
            BackfillLedger.from_dict(malformed)

    ledger.publication = BackfillPublicationRecord(
        mode="shadow_swap",
        target_table="orders",
        shadow_table="orders__dpone_initial_shadow",
        backup_table="orders__dpone_initial_backup",
        phase="published",
        expected_rows=42,
        actual_rows=42,
        duplicate_keys=0,
        receipt_id="receipt-a",
    )
    restored = BackfillLedger.from_dict(ledger.to_jsonable())
    assert restored.publication == ledger.publication


def test_legacy_campaign_binding_upgrade_is_durable_and_dispatches_exact_proof(tmp_path: Path) -> None:
    """A 0.74.26 ledger is upgraded once without changing its campaign identity."""

    config = _config(tmp_path, parallel_workers=4)
    config.source_schema = "public"
    config.target_database = "DWH"
    config.options["source_type"] = "postgres"
    config.options["sink_type"] = "mssql"
    config.partition = {"column": "d", "values_from_staging": True}
    config.options["backfill"]["chunk"]["to"] = "2025-01-01"
    policy = normalize_backfill_execution_policy(config.options["backfill"])
    assert policy.chunk is not None
    dataset = f"{config.target_schema}.{config.target_table}"
    run_key = backfill_run_key(dataset=dataset, spec=policy.chunk, inner_mode=policy.inner_mode)
    chunks = plan_chunks(policy.chunk, run_key=run_key)
    legacy_plan_hash, legacy_config_hash = campaign_identity_hashes(
        chunks=chunks,
        dataset=dataset,
        execution_policy=policy,
        campaign_contract=None,
    )
    state = FileBackfillStateStore(tmp_path / "state")
    state.save(
        BackfillLedger(
            run_key=run_key,
            dataset=dataset,
            inner_mode=policy.inner_mode,
            plan_hash=legacy_plan_hash,
            config_hash=legacy_config_hash,
            chunk_config=policy.chunk.to_jsonable(),
            chunks=[
                BackfillChunkRecord(
                    index=chunk.index,
                    start=chunk.start,
                    end=chunk.end,
                    idempotency_key=chunk.idempotency_key,
                )
                for chunk in chunks
            ],
        )
    )
    resolver_calls: list[str] = []
    columns = PortableScopeColumnContract("d", "date", None, "d", "date", None)

    def resolve_legacy_columns(_config, column):
        leased = state.load(run_key)
        assert leased is not None
        assert leased.lock_owner is not None
        resolver_calls.append(column)
        return columns

    @contextmanager
    def unused_worker_state(_worker_id):
        yield state

    def runtime(*, fail: bool, resolver):
        return BackfillProcessLaneRuntime(
            bootstrap=BackfillProcessLaneBootstrap(
                "tests.test_backfill_orchestrator:_open_spawn_executor_test_lane",
                {"events_path": str(tmp_path / "process-events.jsonl"), "fail": fail},
            ),
            renew_operation_lease=lambda _operation, _expiry: True,
            validate_operation_binding=lambda _dispatch, _operation, _binding: True,
            issue_receipt_probe=_test_receipt_authority,
            validate_replay_result=_test_replay_result,
            receipt_recovery=object(),
            parent_control_scope=_test_parent_control_scope,
            portable_scope_column_resolver=resolver,
            portable_scope_history_proof=lambda **_kwargs: None,
            process_chunk_execution_factory=BackfillProcessChunkExecution,
        )

    first = BackfillOrchestrator(
        chunk_runner=lambda _config: pytest.fail("parent runner must remain unused"),
        state_store=state,
        worker_state_store_factory=unused_worker_state,
        worker_chunk_runner_factory=runtime(
            fail=True,
            resolver=resolve_legacy_columns,
        ),
    ).run(config)
    migrated = state.load(run_key)
    assert migrated is not None
    migrated_binding = CampaignPortableScopeBinding.from_jsonable(migrated.portable_scope_column_contract)

    assert first["status"] == "error"
    assert resolver_calls == ["d"]
    assert migrated_binding == CampaignPortableScopeBinding(columns=columns, identity_bound=False)
    assert migrated.plan_hash == legacy_plan_hash
    assert migrated.config_hash == legacy_config_hash

    second = BackfillOrchestrator(
        chunk_runner=lambda _config: pytest.fail("parent runner must remain unused"),
        state_store=state,
        worker_state_store_factory=unused_worker_state,
        worker_chunk_runner_factory=runtime(
            fail=False,
            resolver=lambda *_args: pytest.fail("durable resume must not query the catalog"),
        ),
    ).run(config)
    chunk_events = [
        json.loads(line)
        for line in (tmp_path / "process-events.jsonl").read_text(encoding="utf-8").splitlines()
        if json.loads(line)["event"] == "chunk"
    ]

    assert second["status"] == "success"
    assert [event["portable_scope_binding"] for event in chunk_events] == [True, True]
    assert all(event["portable_scope_ast_sha256"] for event in chunk_events)


def test_single_worker_campaign_persists_portable_scope_contract(tmp_path: Path) -> None:
    """Campaign authority must not depend on the configured lane count."""

    config = _config(tmp_path, parallel_workers=1)
    config.source_schema = "public"
    config.target_database = "DWH"
    config.options["source_type"] = "postgres"
    config.options["sink_type"] = "mssql"
    config.partition = {"column": "d", "values_from_staging": True}
    state = FileBackfillStateStore(tmp_path / "state")
    runner = _RecordingRunner()
    resolver_calls: list[str] = []

    def resolve_columns(_config, column):
        resolver_calls.append(column)
        return PortableScopeColumnContract("d", "date", None, "d", "date", None)

    result = BackfillOrchestrator(
        chunk_runner=runner,
        state_store=state,
        portable_scope_column_resolver=resolve_columns,
    ).run(config)
    ledger = state.load(result["backfill"]["run_key"])

    assert result["status"] == "success"
    assert resolver_calls == ["d"]
    assert ledger is not None
    assert CampaignPortableScopeBinding.from_jsonable(ledger.portable_scope_column_contract) == (
        CampaignPortableScopeBinding(
            columns=PortableScopeColumnContract("d", "date", None, "d", "date", None),
            identity_bound=True,
        )
    )
    assert all(
        isinstance(call.options.get(PORTABLE_SCOPE_BINDING_OPTION), PortableScopeBinding) for call in runner.calls
    )


def test_target_publication_completes_before_xmin_handoff(tmp_path: Path) -> None:
    events: list[str] = []

    class Lifecycle:
        def require_unmapped(self, *, selection):
            assert selection is None

        def campaign_contract(self, load_config):
            return {"kind": "xmin-test"}

        def before_chunks(self, load_config, ledger, store):
            events.append("xmin_before")
            return ledger

        def after_chunks(self, load_config, ledger, store):
            events.append("xmin_after")
            return {"phase": "committed"}

    class Publisher:
        def require_unmapped(self, *, selection):
            assert selection is None

        def campaign_contract(self, load_config):
            return {"kind": "publication-test"}

        def before_chunks(self, load_config, ledger, store):
            raise AssertionError("fenced publisher preparation must be preferred")

        def before_chunks_fenced(self, load_config, ledger, store, *, campaign_owner):
            assert campaign_owner.startswith("dpone-backfill-campaign-lease-v2:")
            events.append("publish_prepare_fenced")
            return replace(load_config, target_table="orders__shadow")

        def after_chunks(self, load_config, ledger, store):
            events.append("publish_commit")
            return {"phase": "published"}

    def run_chunk(load_config):
        events.append(f"chunk:{load_config.target_table}")
        return {"extracted_rows": 1, "loaded_rows": 1}

    result = BackfillOrchestrator(
        chunk_runner=run_chunk,
        campaign_lifecycle=Lifecycle(),
        target_publisher=Publisher(),
    ).run(_config(tmp_path))

    assert events[:2] == ["xmin_before", "publish_prepare_fenced"]
    assert events[2:6] == ["chunk:orders__shadow"] * 4
    assert events[-2:] == ["publish_commit", "xmin_after"]
    assert result["target_publication"] == {"phase": "published"}
    assert result["xmin_handoff"] == {"phase": "committed"}


def test_target_publication_result_is_rejected_after_campaign_owner_replacement(tmp_path: Path) -> None:
    """A completed vendor phase cannot report success under a stale owner."""

    class Publisher:
        def require_unmapped(self, *, selection):
            assert selection is None

        def campaign_contract(self, load_config):
            return {"kind": "publication-fence-test"}

        def before_chunks(self, load_config, ledger, store):
            return load_config

        def after_chunks(self, load_config, ledger, store):
            current = store.load(ledger.run_key)
            assert current is not None
            current.lock_owner = "dpone-backfill-campaign-lease-v2:replacement"
            current.lock_expires_at = (datetime.now(UTC) + timedelta(minutes=5)).isoformat()
            store.save(current)
            return {"phase": "published"}

    with pytest.raises(BackfillCampaignLeaseError, match="DPONE_BACKFILL_CAMPAIGN_LEASE_LOST"):
        BackfillOrchestrator(
            chunk_runner=_RecordingRunner(),
            target_publisher=Publisher(),
        ).run(_config(tmp_path))


def test_campaign_lease_policy_is_dependency_injected(tmp_path: Path) -> None:
    built: list[tuple[object, str]] = []

    def campaign_lease_factory(store, run_key):
        built.append((store, run_key))
        return BackfillCampaignLease(
            store,
            run_key,
            ttl=timedelta(seconds=3),
            renew_interval=timedelta(seconds=1),
        )

    result = BackfillOrchestrator(
        chunk_runner=_RecordingRunner(),
        campaign_lease_factory=campaign_lease_factory,
    ).run(_config(tmp_path))

    assert result["status"] == "success"
    assert len(built) == 1
    assert built[0][1] == result["backfill"]["run_key"]


def test_backfill_progress_projects_rate_and_eta_from_durable_transitions() -> None:
    ledger = BackfillLedger(
        run_key="campaign-a",
        dataset="dbo.orders",
        inner_mode="incremental_append",
        chunk_config={},
        created_at="2026-08-22T10:00:00+00:00",
        chunks=[
            BackfillChunkRecord(
                index=1,
                start="1",
                end="1",
                idempotency_key="1",
                status="success",
                rows_extracted=1_000,
                rows_loaded=1_000,
                started_at="2026-08-22T10:00:00+00:00",
                finished_at="2026-08-22T10:00:10+00:00",
            ),
            BackfillChunkRecord(index=2, start="2", end="2", idempotency_key="2"),
        ],
    )

    progress = backfill_progress(ledger, now=datetime(2026, 8, 22, 10, 0, 20, tzinfo=UTC))

    assert progress == {
        "pending": 1,
        "running": 0,
        "committed": 1,
        "failed": 0,
        "rows_extracted": 1_000,
        "rows_loaded": 1_000,
        "rows_per_second": 50.0,
        "eta_seconds": 20.0,
        "elapsed_seconds": 20.0,
        "last_transition_at": "2026-08-22T10:00:10+00:00",
    }


class _DistributedFileStateStore(FileBackfillStateStore):
    def state_capabilities(self):
        return {
            **super().state_capabilities(),
            "distributed_lock": True,
            "distributed_chunk_lease": True,
            "compare_and_set_completion": True,
            "lock_scope": "test_distributed",
        }


class _LeaseRecordingFileStateStore(FileBackfillStateStore):
    def __init__(self, root_dir: Path) -> None:
        super().__init__(root_dir)
        self.campaign_expiries: list[datetime] = []
        self.campaign_renewals: list[datetime] = []
        self.chunk_expiries: list[datetime] = []

    def acquire_campaign_lock(self, run_key, *, owner, lease_expires_at):
        self.campaign_expiries.append(lease_expires_at)
        return super().acquire_campaign_lock(
            run_key,
            owner=owner,
            lease_expires_at=lease_expires_at,
        )

    def renew_campaign_lock(self, run_key, *, owner, lease_expires_at):
        self.campaign_renewals.append(lease_expires_at)
        return super().renew_campaign_lock(
            run_key,
            owner=owner,
            lease_expires_at=lease_expires_at,
        )

    def acquire_chunk_lease(self, run_key, index, *, owner, lease_expires_at):
        self.chunk_expiries.append(lease_expires_at)
        return super().acquire_chunk_lease(
            run_key,
            index,
            owner=owner,
            lease_expires_at=lease_expires_at,
        )


class _WorkerStateSession:
    def __init__(
        self,
        authority: FileBackfillStateStore,
        *,
        session_id: int,
        barrier: Barrier | None = None,
        entered: list[tuple[int, int]] | None = None,
    ) -> None:
        self._authority = authority
        self.session_id = session_id
        self._barrier = barrier
        self._entered = entered
        self._barrier_used = False

    def __getattr__(self, name: str):
        return getattr(self._authority, name)

    def acquire_chunk_lease(self, run_key, index, *, owner, lease_expires_at):
        if self._entered is not None:
            self._entered.append((self.session_id, index))
        if self._barrier is not None and not self._barrier_used:
            self._barrier_used = True
            self._barrier.wait(timeout=2)
        return self._authority.acquire_chunk_lease(
            run_key,
            index,
            owner=owner,
            lease_expires_at=lease_expires_at,
        )


def _worker_store_factory(
    authority: FileBackfillStateStore,
    *,
    opened: list[int] | None = None,
    closed: list[int] | None = None,
    barrier: Barrier | None = None,
    entered: list[tuple[int, int]] | None = None,
):
    @contextmanager
    def build(worker_id: int):
        session_id = 700 + worker_id
        if opened is not None:
            opened.append(session_id)
        try:
            yield _WorkerStateSession(
                authority,
                session_id=session_id,
                barrier=barrier,
                entered=entered,
            )
        finally:
            if closed is not None:
                closed.append(session_id)

    return build


def test_chunk_heartbeat_proves_ownership_synchronously() -> None:
    now = datetime(2026, 8, 16, tzinfo=UTC)
    calls = 0

    def rejected_renewal() -> bool:
        nonlocal calls
        calls += 1
        return False

    heartbeat = BackfillLeaseHeartbeat(
        rejected_renewal,
        initial_expiry=now + timedelta(minutes=1),
        now=lambda: now,
    )

    with pytest.raises(BackfillLeaseHeartbeatError, match="DPONE_BACKFILL_CHUNK_LEASE_HEARTBEAT_LOST"):
        heartbeat.prove_ownership()

    assert calls == 1


def test_is_chunked_backfill_detects_only_chunked_configs(tmp_path: Path) -> None:
    assert is_chunked_backfill(_config(tmp_path))
    single_shot = _config(tmp_path)
    single_shot.options["backfill"].pop("chunk")
    assert not is_chunked_backfill(single_shot)


def test_orchestrator_runs_all_chunks_with_bounded_predicates(tmp_path: Path) -> None:
    runner = _RecordingRunner()

    result = BackfillOrchestrator(chunk_runner=runner).run(_config(tmp_path))

    assert result["status"] == "success"
    assert len(runner.calls) == 4
    first = runner.calls[0]
    assert first.custom_predicate == "d >= '2025-01-01' AND d < '2025-01-02'"
    assert first.options["source_custom_predicate"] == first.custom_predicate
    assert first.options["backfill"]["chunk_context"]["index"] == 1
    assert result["backfill"]["chunks_committed"] == 4
    assert result["backfill"]["retry_policy"] == "non_committed"
    assert result["extracted_rows"] == 40


def test_mapped_orchestrator_executes_only_selected_range_and_reports_item_success(tmp_path: Path) -> None:
    config = _mapped_config(tmp_path)
    plan = build_airflow_mapping_plan(
        config,
        {"mode": "summary", "max_items": 2, "max_active": 2, "pool": "history"},
    )
    runtime_selection = parse_airflow_mapping_item_json(serialize_airflow_mapping_item(plan, plan.items[0]))
    runner = _RecordingRunner()

    result = BackfillOrchestrator(
        chunk_runner=runner,
        state_store=_DistributedFileStateStore(tmp_path / "state"),
    ).run(config, selection=runtime_selection)

    assert [call.options["backfill"]["chunk_context"]["index"] for call in runner.calls] == [1, 2]
    assert result["status"] == "success"
    assert result["extracted_rows"] == 20
    assert result["backfill"]["chunks_committed"] == 2
    assert result["backfill"]["mapping"] == {
        "mode": "summary",
        "plan_fingerprint": plan.plan_fingerprint,
        "backfill_plan_hash": plan.backfill_plan_hash,
        "item_index": 0,
        "first_chunk_index": 1,
        "last_chunk_index": 2,
        "chunks_count": 2,
        "item_status": "success",
    }
    assert "verification_execution_path" not in result["backfill"]


@pytest.mark.parametrize(
    "proof_error",
    [
        "backfill.portable_scope_history.operation_scope_mismatch",
        "backfill.portable_scope_history.committed_receipt_missing",
    ],
    ids=["schema-drift", "history-tamper"],
)
def test_mapped_legacy_portable_scope_upgrade_fails_closed_before_source(
    tmp_path: Path,
    proof_error: str,
) -> None:
    config = _mapped_config(tmp_path)
    config.source_schema = "public"
    config.target_database = "DWH"
    config.options.update({"source_type": "postgres", "sink_type": "mssql"})
    config.partition = {"column": "d", "values_from_staging": True}
    policy = normalize_backfill_execution_policy(config.options["backfill"])
    assert policy.chunk is not None
    dataset = f"{config.target_schema}.{config.target_table}"
    run_key = backfill_run_key(dataset=dataset, spec=policy.chunk, inner_mode=policy.inner_mode)
    chunks = plan_chunks(policy.chunk, run_key=run_key)
    legacy_plan_hash, legacy_config_hash = campaign_identity_hashes(
        chunks=chunks,
        dataset=dataset,
        execution_policy=policy,
        campaign_contract=None,
    )
    store = _DistributedFileStateStore(tmp_path / "state")
    store.save(
        BackfillLedger(
            run_key=run_key,
            dataset=dataset,
            inner_mode=policy.inner_mode,
            plan_hash=legacy_plan_hash,
            config_hash=legacy_config_hash,
            chunk_config=policy.chunk.to_jsonable(),
            chunks=[
                BackfillChunkRecord(
                    index=chunk.index,
                    start=chunk.start,
                    end=chunk.end,
                    idempotency_key=chunk.idempotency_key,
                )
                for chunk in chunks
            ],
        )
    )
    plan = build_airflow_mapping_plan(
        config,
        {"mode": "summary", "max_items": 2, "max_active": 2, "pool": "history"},
    )
    selection = parse_airflow_mapping_item_json(serialize_airflow_mapping_item(plan, plan.items[0]))
    proof_calls: list[str] = []

    def reject_history(**_kwargs):
        current = store.load(run_key)
        assert current is not None and current.lock_owner is not None
        proof_calls.append(proof_error)
        raise RuntimeError(proof_error)

    runner = _RecordingRunner()
    with pytest.raises(RuntimeError, match=proof_error):
        BackfillOrchestrator(
            chunk_runner=runner,
            state_store=store,
            portable_scope_column_resolver=lambda *_args: PortableScopeColumnContract(
                "d", "date", None, "d", "date", None
            ),
            portable_scope_history_proof=reject_history,
        ).run(config, selection=selection)

    current = store.load(run_key)
    assert current is not None
    assert current.portable_scope_column_contract is None
    assert current.lock_owner is None
    assert proof_calls == [proof_error]
    assert runner.calls == []


def test_mapped_orchestrator_replans_before_source_io_and_blocks_plan_drift(tmp_path: Path) -> None:
    config = _mapped_config(tmp_path)
    plan = build_airflow_mapping_plan(
        config,
        {"mode": "visible", "max_items": 4, "max_active": 2, "pool": "history"},
    )
    selection = parse_airflow_mapping_item_json(serialize_airflow_mapping_item(plan, plan.items[0]))
    drifted = replace(selection, backfill_plan_hash="sha256:" + "0" * 64)
    runner = _RecordingRunner()

    with pytest.raises(AirflowBackfillMappingViolation) as raised:
        BackfillOrchestrator(
            chunk_runner=runner,
            state_store=_DistributedFileStateStore(tmp_path / "state"),
        ).run(config, selection=drifted)

    assert raised.value.code == "DPONE_AIRFLOW_MAPPING_PLAN_MISMATCH"
    assert runner.calls == []


def test_mapped_orchestrator_requires_runtime_distributed_cas_capabilities(tmp_path: Path) -> None:
    config = _mapped_config(tmp_path)
    plan = build_airflow_mapping_plan(
        config,
        {"mode": "visible", "max_items": 4, "max_active": 2, "pool": "history"},
    )
    selection = parse_airflow_mapping_item_json(serialize_airflow_mapping_item(plan, plan.items[0]))
    runner = _RecordingRunner()

    with pytest.raises(AirflowBackfillMappingViolation) as raised:
        BackfillOrchestrator(
            chunk_runner=runner,
            state_store=FileBackfillStateStore(tmp_path / "state"),
        ).run(config, selection=selection)

    assert raised.value.code == "DPONE_AIRFLOW_MAPPING_STATE_UNSUPPORTED"
    assert runner.calls == []


def test_orchestrator_combines_user_predicate_with_chunk_window(tmp_path: Path) -> None:
    runner = _RecordingRunner()
    config = _config(tmp_path)
    config.custom_predicate = "region = 'emea'"

    BackfillOrchestrator(chunk_runner=runner).run(config)

    assert runner.calls[0].custom_predicate == "(region = 'emea') AND (d >= '2025-01-01' AND d < '2025-01-02')"


def test_orchestrator_uses_dialect_renderer_for_generated_predicate(tmp_path: Path) -> None:
    runner = _RecordingRunner()
    config = _config(tmp_path)
    config.options["backfill"]["predicate_dialect"] = "mssql"

    BackfillOrchestrator(chunk_runner=runner).run(config)

    assert runner.calls[0].custom_predicate == "[d] >= CAST('2025-01-01' AS date) AND [d] < CAST('2025-01-02' AS date)"


def test_orchestrator_stops_on_first_failure_and_resumes(tmp_path: Path) -> None:
    failing = _RecordingRunner(fail_on={3})

    first = BackfillOrchestrator(chunk_runner=failing).run(_config(tmp_path))

    assert first["status"] == "error"
    assert first["backfill"]["chunks_committed"] == 2
    assert first["backfill"]["chunks_failed"] == 1
    assert len(failing.calls) == 3  # chunk 4 never started
    assert "chunk 3" in first["errors"][0]

    resuming = _RecordingRunner()
    second = BackfillOrchestrator(chunk_runner=resuming).run(_config(tmp_path))

    assert second["status"] == "success"
    assert [call.options["backfill"]["chunk_context"]["index"] for call in resuming.calls] == [3, 4]
    assert second["backfill"]["chunks_skipped_resume"] == 2


def test_post_commit_owner_cas_loss_resumes_same_campaign(tmp_path: Path) -> None:
    class _StealFirstCompletion(DefaultBackfillChunkLifecycle):
        fired = False

        def before_ledger_completion(
            self,
            context: BackfillChunkLifecycleContext,
            result: dict[str, object] | object,
        ) -> None:
            del result
            if self.fired:
                return
            self.fired = True
            current = context.store.load(context.run_key)
            assert current is not None
            record = current.chunk(context.chunk_index)
            record.lease_owner = "stale-owner"
            record.lease_expires_at = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
            context.store.update_chunk(current, record)

    store = FileBackfillStateStore(tmp_path / "state")
    first_runner = _RecordingRunner()
    first = BackfillOrchestrator(
        chunk_runner=first_runner,
        state_store=store,
        lifecycle_port=_StealFirstCompletion(),
    ).run(_config(tmp_path))

    assert first["status"] == "error"
    assert first["backfill"]["chunks_committed"] == 0
    assert first_runner.calls[0].options["backfill"]["chunk_context"]["index"] == 1
    assert "DPONE_BACKFILL_CHUNK_LEASE_LOST" in first["errors"][0]

    retry_runner = _RecordingRunner()
    second = BackfillOrchestrator(chunk_runner=retry_runner, state_store=store).run(_config(tmp_path))

    assert second["status"] == "success"
    assert second["backfill"]["chunks_committed"] == 4
    assert [call.options["backfill"]["chunk_context"]["index"] for call in retry_runner.calls] == [1, 2, 3, 4]


def test_orchestrator_fails_before_source_io_when_campaign_lock_is_active(tmp_path: Path) -> None:
    config = _config(tmp_path, backfill_id="locked-campaign")
    store = FileBackfillStateStore(tmp_path / "state")
    spec = chunk_spec_from_options(config.options["backfill"])
    assert spec is not None
    inner_mode = inner_mode_from_options(config.options["backfill"])
    chunks = plan_chunks(spec, run_key="locked-campaign")
    execution_policy = normalize_backfill_execution_policy(config.options["backfill"])
    store.save(
        BackfillLedger(
            run_key="locked-campaign",
            dataset="analytics.orders",
            inner_mode=inner_mode,
            plan_hash=plan_hash(chunks, execution_policy=execution_policy),
            config_hash=config_hash(dataset="analytics.orders", execution_policy=execution_policy),
            chunk_config={
                "column": spec.column,
                "from": spec.start,
                "to": spec.end,
                "step": spec.step,
                "kind": spec.kind,
            },
            chunks=[
                BackfillChunkRecord(
                    index=chunk.index, start=chunk.start, end=chunk.end, idempotency_key=chunk.idempotency_key
                )
                for chunk in chunks
            ],
        )
    )
    assert store.acquire_campaign_lock(
        "locked-campaign",
        owner="other-worker",
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=30),
    )
    runner = _RecordingRunner()

    with pytest.raises(BackfillCampaignLeaseError, match="DPONE_BACKFILL_CAMPAIGN_LEASE_LOST"):
        BackfillOrchestrator(chunk_runner=runner, state_store=store).run(config)

    assert runner.calls == []


def test_retry_policy_is_frozen_for_an_existing_campaign(tmp_path: Path) -> None:
    store = FileBackfillStateStore(tmp_path / "state")
    first = BackfillOrchestrator(chunk_runner=_RecordingRunner(fail_on={2}), state_store=store).run(_config(tmp_path))
    assert first["backfill"]["chunks_committed"] == 1
    assert first["backfill"]["chunks_failed"] == 1
    retry_config = _config(tmp_path, retry_policy="failed_only")
    runner = _RecordingRunner()

    with pytest.raises(ValueError, match="config hash changed"):
        BackfillOrchestrator(chunk_runner=runner, state_store=store).run(retry_config)

    assert runner.calls == []


def test_unknown_retry_policy_fails_before_source_io(tmp_path: Path) -> None:
    runner = _RecordingRunner()

    with pytest.raises(ValueError, match="backfill.retry_policy"):
        BackfillOrchestrator(chunk_runner=runner).run(_config(tmp_path, retry_policy="failed-only"))

    assert runner.calls == []


def test_orchestrator_blocks_resume_when_pinned_campaign_shape_changes(tmp_path: Path) -> None:
    runner = _RecordingRunner()
    config = _config(tmp_path, backfill_id="stable-campaign")
    BackfillOrchestrator(chunk_runner=runner).run(config)
    changed = _config(tmp_path, backfill_id="stable-campaign")
    changed.options["backfill"]["chunk"]["to"] = "2025-01-05"

    with pytest.raises(ValueError, match="config hash changed"):
        BackfillOrchestrator(chunk_runner=_RecordingRunner()).run(changed)


def test_orchestrator_does_not_start_new_chunks_after_cancel(tmp_path: Path) -> None:
    config = _config(tmp_path, backfill_id="cancelled-campaign")
    store = FileBackfillStateStore(tmp_path / "state")
    BackfillOrchestrator(chunk_runner=_RecordingRunner(), state_store=store).run(config)
    store.request_cancel("cancelled-campaign", reason="operator requested stop", requested_by="qa")
    runner = _RecordingRunner()

    result = BackfillOrchestrator(chunk_runner=runner, state_store=store).run(config)

    assert result["status"] == "cancelled"
    assert runner.calls == []
    assert result["backfill"]["cancel_reason"] == "operator requested stop"


def test_orchestrator_finalizes_legacy_cancel_after_published_target(tmp_path: Path) -> None:
    events: list[str] = []

    class Lifecycle:
        def require_unmapped(self, *, selection):
            assert selection is None

        def campaign_contract(self, load_config):
            return {"kind": "legacy-post-publication-finalize-test"}

        def before_chunks(self, load_config, ledger, store):
            events.append("before")
            return ledger

        def after_chunks(self, load_config, ledger, store):
            events.append("after")
            return {"phase": "committed"}

    config = _config(tmp_path, backfill_id="published-then-cancelled")
    store = FileBackfillStateStore(tmp_path / "state")
    lifecycle = Lifecycle()
    BackfillOrchestrator(
        chunk_runner=_RecordingRunner(),
        state_store=store,
        campaign_lifecycle=lifecycle,
    ).run(config)
    ledger = store.load("published-then-cancelled")
    assert ledger is not None
    ledger.status = "cancel_requested"
    ledger.cancel_reason = "legacy race after target commit"
    ledger.cancel_requested_by = "qa"
    ledger.publication = BackfillPublicationRecord(
        mode="shadow_swap",
        target_table="analytics.orders",
        shadow_table="analytics.orders__shadow",
        backup_table="analytics.orders__backup",
        phase="published",
        receipt_id="receipt-a",
    )
    store.save(ledger)
    events.clear()
    runner = _RecordingRunner()

    result = BackfillOrchestrator(
        chunk_runner=runner,
        state_store=store,
        campaign_lifecycle=lifecycle,
    ).run(config)

    current = store.load("published-then-cancelled")
    assert current is not None
    assert result["status"] == "success"
    assert result["xmin_handoff"] == {"phase": "committed"}
    assert events == ["before", "after"]
    assert runner.calls == []
    assert current.status == "active"
    assert current.cancel_reason is None
    assert current.cancel_requested_by is None


def test_orchestrator_normalizes_published_cancel_after_committed_handoff(tmp_path: Path) -> None:
    events: list[str] = []

    class Lifecycle:
        def require_unmapped(self, *, selection):
            assert selection is None

        def campaign_contract(self, load_config):
            return {"kind": "committed-handoff-normalization-test"}

        def before_chunks(self, load_config, ledger, store):
            events.append("before")
            return ledger

        def after_chunks(self, load_config, ledger, store):
            events.append("after")
            return {"phase": "committed"}

    config = _config(tmp_path, backfill_id="published-committed-then-cancelled")
    store = FileBackfillStateStore(tmp_path / "state")
    lifecycle = Lifecycle()
    BackfillOrchestrator(
        chunk_runner=_RecordingRunner(),
        state_store=store,
        campaign_lifecycle=lifecycle,
    ).run(config)
    ledger = store.load("published-committed-then-cancelled")
    assert ledger is not None
    ledger.status = "cancel_requested"
    ledger.cancel_reason = "legacy post-publication race"
    ledger.cancel_requested_by = "qa"
    ledger.publication = BackfillPublicationRecord(
        mode="shadow_swap",
        target_table="analytics.orders",
        shadow_table="analytics.orders__shadow",
        backup_table="analytics.orders__backup",
        phase="published",
        receipt_id="publication-receipt",
    )
    ledger.xmin_handoff = BackfillXminHandoffRecord(
        handoff_id="handoff-a",
        status="committed",
        anchor_xmin=42,
        snapshot_token="sha256:" + "a" * 64,
        state_key_sha256="b" * 64,
        source_authority_sha256="c" * 64,
        plan_hash=str(ledger.plan_hash),
        seed_load_id="seed-a",
        receipt_id="handoff-receipt",
        candidate_revision=7,
    )
    store.save(ledger)
    events.clear()
    runner = _RecordingRunner()

    result = BackfillOrchestrator(
        chunk_runner=runner,
        state_store=store,
        campaign_lifecycle=lifecycle,
    ).run(config)

    current = store.load("published-committed-then-cancelled")
    assert current is not None
    assert result["status"] == "success"
    assert result["xmin_handoff"] == {"phase": "committed"}
    assert runner.calls == []
    assert events == ["before", "after"]
    assert current.status == "active"
    assert current.cancel_reason is None
    assert current.cancel_requested_by is None
    assert current.xmin_handoff is not None
    assert current.xmin_handoff.status == "committed"


def test_orchestrator_requires_injected_store_for_audit_schema_backend(tmp_path: Path) -> None:
    config = _config(tmp_path)
    config.options["backfill"]["state"] = {"backend": "audit_schema", "schema": "DWH_Tech"}

    with pytest.raises(ValueError, match="audit_schema requires sink_connector"):
        BackfillOrchestrator(chunk_runner=_RecordingRunner()).run(config)


def test_execute_process_with_backfill_builds_audit_schema_state_store(tmp_path: Path) -> None:
    class _Sink:
        def __init__(self) -> None:
            self.connector = _AuditConnector()

    class _Processor:
        def __init__(self) -> None:
            self.sink = _Sink()

        def run(self, load_config, run_context=None, *, dag_id=None, execution_date=None):
            index = load_config.options["backfill"]["chunk_context"]["index"]
            return {"status": "success", "extracted_rows": 5, "loaded_rows": 5, "load_id": f"load-{index}"}

    config = _config(tmp_path)
    config.options["backfill"]["state"] = {"backend": "audit_schema", "schema": "DWH_Tech"}
    processor = _Processor()

    result = execute_process_with_backfill(processor, config)

    assert result["status"] == "success"
    assert result["backfill"]["state_backend"] == "audit_schema"
    rendered = "\n".join(str(sql) for sql, _ in processor.sink.connector.calls)
    assert "`DWH_Tech`.`__dpone__backfill_campaigns`" in rendered


def test_orchestrator_ledger_records_lineage_ids(tmp_path: Path) -> None:
    result = BackfillOrchestrator(chunk_runner=_RecordingRunner()).run(_config(tmp_path))

    ledger = json.loads(Path(result["backfill"]["state_path"]).read_text(encoding="utf-8"))
    assert ledger["kind"] == "dpone.backfill_ledger"
    assert [chunk["load_id"] for chunk in ledger["chunks"]] == ["load-1", "load-2", "load-3", "load-4"]
    assert all(chunk["idempotency_key"] for chunk in ledger["chunks"])


def test_orchestrator_rejects_multi_chunk_full_refresh(tmp_path: Path) -> None:
    config = _config(tmp_path, inner_mode="full_refresh")

    with pytest.raises(ValueError, match="single-chunk plan"):
        BackfillOrchestrator(chunk_runner=_RecordingRunner()).run(config)


def test_orchestrator_supports_parallel_workers(tmp_path: Path) -> None:
    runner = _RecordingRunner()
    config = _config(tmp_path, parallel_workers=3)
    store = FileBackfillStateStore(tmp_path / "state")
    opened: list[int] = []
    closed: list[int] = []

    result = BackfillOrchestrator(
        chunk_runner=runner,
        state_store=store,
        worker_state_store_factory=_worker_store_factory(store, opened=opened, closed=closed),
    ).run(config)

    assert result["status"] == "success"
    assert sorted(call.options["backfill"]["chunk_context"]["index"] for call in runner.calls) == [1, 2, 3, 4]
    assert len(set(opened)) == 3
    assert sorted(closed) == sorted(opened)


def test_parallel_orchestrator_rejects_singleton_state_store(tmp_path: Path) -> None:
    runner = _RecordingRunner()

    with pytest.raises(RuntimeError, match="parallel_worker_state_store_factory_required"):
        BackfillOrchestrator(chunk_runner=runner).run(_config(tmp_path, parallel_workers=2))

    assert runner.calls == []


@pytest.mark.parametrize("fail", [False, True], ids=["success", "worker_error"])
def test_parallel_worker_state_sessions_overlap_and_close(tmp_path: Path, fail: bool) -> None:
    store = FileBackfillStateStore(tmp_path / "state")
    opened: list[int] = []
    closed: list[int] = []
    entered: list[tuple[int, int]] = []
    barrier = Barrier(2)
    runner = _RecordingRunner(fail_on={1} if fail else None)

    result = BackfillOrchestrator(
        chunk_runner=runner,
        state_store=store,
        worker_state_store_factory=_worker_store_factory(
            store,
            opened=opened,
            closed=closed,
            barrier=barrier,
            entered=entered,
        ),
    ).run(_config(tmp_path, parallel_workers=2))

    assert result["status"] == ("error" if fail else "success")
    assert {session_id for session_id, _index in entered} == {700, 701}
    entered_indexes = sorted(index for _session_id, index in entered)
    if fail:
        assert entered_indexes[:2] == [1, 2]
    else:
        assert entered_indexes == [1, 2, 3, 4]
    assert opened == [700, 701]
    assert sorted(closed) == [700, 701]
    assert barrier.broken is False


def test_parallel_orchestrator_stops_new_claims_and_resumes_durable_pending_chunks(tmp_path: Path) -> None:
    failure_durable = Event()

    class _FailureAwareStore(FileBackfillStateStore):
        def complete_chunk_if_owned(self, run_key, record, *, owner):
            completed = super().complete_chunk_if_owned(run_key, record, owner=owner)
            if completed and record.status == "failed":
                failure_durable.set()
            return completed

    class _FailingRunner(_RecordingRunner):
        def __call__(self, chunk_config: LoadConfig):
            self.calls.append(chunk_config)
            index = chunk_config.options["backfill"]["chunk_context"]["index"]
            if index == 1:
                raise RuntimeError("chunk 1 exploded")
            if index == 2:
                assert failure_durable.wait(timeout=2)
            return {"extracted_rows": 10, "loaded_rows": 10}

    store = _FailureAwareStore(tmp_path / "state")
    runner = _FailingRunner()
    barrier = Barrier(2)

    result = BackfillOrchestrator(
        chunk_runner=runner,
        state_store=store,
        worker_state_store_factory=_worker_store_factory(store, barrier=barrier),
    ).run(_config(tmp_path, parallel_workers=2))

    assert result["status"] == "error"
    assert sorted(call.options["backfill"]["chunk_context"]["index"] for call in runner.calls) == [1, 2]
    ledger = store.load(result["backfill"]["run_key"])
    assert ledger is not None
    assert [ledger.chunk(index).status for index in (3, 4)] == ["pending", "pending"]

    resumed = _RecordingRunner()
    resumed_result = BackfillOrchestrator(
        chunk_runner=resumed,
        state_store=store,
        worker_state_store_factory=_worker_store_factory(store),
    ).run(_config(tmp_path, parallel_workers=2))

    assert resumed_result["status"] == "success"
    assert sorted(call.options["backfill"]["chunk_context"]["index"] for call in resumed.calls) == [1, 3, 4]
    resumed_ledger = store.load(resumed_result["backfill"]["run_key"])
    assert resumed_ledger is not None
    assert [resumed_ledger.chunk(index).status for index in (1, 2, 3, 4)] == ["success"] * 4


def test_orchestrator_exports_verification_execution_document(tmp_path: Path) -> None:
    result = BackfillOrchestrator(chunk_runner=_RecordingRunner()).run(_config(tmp_path))

    execution_path = Path(result["backfill"]["verification_execution_path"])
    payload = json.loads(execution_path.read_text(encoding="utf-8"))
    assert payload["route"] == {"source": "mssql", "sink": "clickhouse", "strategy": "backfill"}
    assert payload["status"] == "succeeded"
    assert payload["passed"] is True
    assert payload["executed"] is True
    assert [chunk["ordinal"] for chunk in payload["chunks"]] == [1, 2, 3, 4]
    assert result["backfill"]["verification"] == {
        "check": "row_count_parity",
        "status": "passed",
        "mismatched_chunks": [],
    }


def test_verification_warns_on_row_count_mismatch(tmp_path: Path) -> None:
    class _LossyRunner(_RecordingRunner):
        def __call__(self, chunk_config: LoadConfig):
            result = super().__call__(chunk_config)
            return {**result, "loaded_rows": 9}

    result = BackfillOrchestrator(chunk_runner=_LossyRunner()).run(_config(tmp_path))

    verification = result["backfill"]["verification"]
    assert verification["status"] == "warning"
    assert len(verification["mismatched_chunks"]) == 4


def test_execute_process_with_backfill_falls_back_to_single_run(tmp_path: Path) -> None:
    class _FakeProcessor:
        def __init__(self) -> None:
            self.configs: list[LoadConfig] = []

        def run(self, load_config, run_context=None, *, dag_id=None, execution_date=None):
            self.configs.append(load_config)
            return {"status": "success", "extracted_rows": 1, "loaded_rows": 1}

    processor = _FakeProcessor()
    single_shot = _config(tmp_path)
    single_shot.options["backfill"].pop("chunk")

    result = execute_process_with_backfill(processor, single_shot)

    assert result["status"] == "success"
    assert processor.configs == [single_shot]


def test_execute_process_with_backfill_rejects_mapped_context_for_single_run(tmp_path: Path) -> None:
    class _FakeProcessor:
        def __init__(self) -> None:
            self.calls = 0

        def run(self, load_config, run_context=None, *, dag_id=None, execution_date=None):
            self.calls += 1
            return {"status": "success"}

    config = _mapped_config(tmp_path)
    plan = build_airflow_mapping_plan(
        config,
        {"mode": "visible", "max_items": 4, "max_active": 2, "pool": "history"},
    )
    selection = parse_airflow_mapping_item_json(serialize_airflow_mapping_item(plan, plan.items[0]))
    config.options["backfill"].pop("chunk")
    processor = _FakeProcessor()

    with pytest.raises(AirflowBackfillMappingViolation) as raised:
        execute_process_with_backfill(
            processor,
            config,
            RunContext(run_id="mapped-single-run-guard", config={AIRFLOW_MAPPING_SELECTION_CONTEXT_KEY: selection}),
        )

    assert raised.value.code == "DPONE_AIRFLOW_MAPPING_REQUIRES_CHUNKED_BACKFILL"
    assert processor.calls == 0


def test_execute_process_with_backfill_orchestrates_chunks(tmp_path: Path) -> None:
    class _FakeProcessor:
        def __init__(self) -> None:
            self.count = 0

        def run(self, load_config, run_context=None, *, dag_id=None, execution_date=None):
            self.count += 1
            return {"status": "success", "extracted_rows": 5, "loaded_rows": 5}

    processor = _FakeProcessor()

    result = execute_process_with_backfill(processor, _config(tmp_path))

    assert processor.count == 4
    assert result["backfill"]["chunks_total"] == 4


@pytest.mark.parametrize(
    "sink_type",
    (
        "mssql",
        "MSSQL",
        "microsoft mssql",
        "microsoft_mssql",
        "odbc",
        "sqlserver",
        "sql_server",
        "sql-server",
    ),
)
def test_mssql_campaign_contract_fails_before_state_or_catalog_io(tmp_path: Path, sink_type: str) -> None:
    config = _config(tmp_path, inner_mode="replace")
    config.options.update({"source_type": "postgres", "sink_type": sink_type})
    config.only_new_rows = True
    calls: list[str] = []

    orchestrator = BackfillOrchestrator(
        chunk_runner=lambda _config: pytest.fail("invalid campaign must not run a chunk"),
        portable_scope_column_resolver=lambda *_args: calls.append("catalog"),
    )

    with pytest.raises(MSSQLStrategyContractError) as raised:
        orchestrator.run(config)

    assert raised.value.blocker == "mssql.strategy.backfill.irrelevant_only_new_rows"
    assert calls == []
    assert not (tmp_path / "state").exists()


def test_execute_process_with_backfill_composes_initial_xmin_lifecycle(tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    class Source:
        def build_xmin_initial_handoff_source(self, storage):
            captured["source_storage"] = storage
            return object()

    processor = SimpleNamespace(
        source=Source(),
        sink=SimpleNamespace(
            connector=object(),
            state_storage=SimpleNamespace(
                atomicity="target_atomic",
                connector=object(),
                database="DWH_Tech",
                schema="system",
            ),
        ),
        run=lambda *_args, **_kwargs: {"status": "success"},
    )
    config = _config(tmp_path)
    config.options.update(
        {
            "source_type": "postgres",
            "sink_type": "mssql",
            "incremental_strategy": "xmin",
            "xmin_execution": {"mode": "initial", "handoff_id": "orders_v1"},
            "unique_key": ["id"],
        }
    )
    config.options["backfill"].update(
        {
            "inner_mode": "incremental_merge",
            "state": {"backend": "audit_schema", "require_distributed_lock": True},
        }
    )
    storage = object()

    class Orchestrator:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def run(self, _config, *, selection=None):
            assert selection is None
            return {"status": "success"}

    result = execute_process_with_backfill(
        processor,
        config,
        xmin_handoff_state_storage=storage,
        orchestrator_factory=Orchestrator,
    )

    assert result == {"status": "success"}
    assert captured["source_storage"] is storage
    assert isinstance(captured["campaign_lifecycle"], PostgresXminInitialHandoffLifecycle)
    assert isinstance(captured["portable_scope_column_resolver"], PortableScopeColumnResolver)


def test_target_atomic_parallel_backfill_rejects_thread_factory(tmp_path: Path) -> None:
    class ParentProcessor:
        sink = SimpleNamespace(
            connector=object(),
            state_storage=SimpleNamespace(atomicity="target_atomic"),
        )

        run = None

    @contextmanager
    def factory(_worker_id: int):
        yield lambda _load_config: {"status": "success"}

    with pytest.raises(
        RuntimeError,
        match="mssql_transaction.parallel_backfill_process_bootstrap_required",
    ):
        execute_process_with_backfill(
            ParentProcessor(),
            _config(tmp_path, parallel_workers=2),
            RunContext("scheduled-backfill", config={"process": "p"}),
            worker_chunk_runner_factory=factory,
        )


def test_target_atomic_parallel_backfill_accepts_process_runtime_marker(tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    class ParentProcessor:
        sink = SimpleNamespace(
            connector=object(),
            state_storage=SimpleNamespace(atomicity="target_atomic"),
        )

        run = None

    class Orchestrator:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def run(self, _config, *, selection=None):
            assert selection is None
            return {"status": "success"}

    marker = BackfillProcessLaneRuntime(
        bootstrap=BackfillProcessLaneBootstrap("module:entrypoint", {}),
        renew_operation_lease=lambda _operation, _expiry: True,
        validate_operation_binding=lambda _dispatch, _operation, _binding: True,
        issue_receipt_probe=_test_receipt_authority,
        validate_replay_result=_test_replay_result,
        receipt_recovery=object(),
        parent_control_scope=_test_parent_control_scope,
        process_chunk_execution_factory=BackfillProcessChunkExecution,
    )
    result = execute_process_with_backfill(
        ParentProcessor(),
        _config(tmp_path, parallel_workers=2),
        RunContext("scheduled-backfill", config={"process": "p"}),
        worker_chunk_runner_factory=marker,
        orchestrator_factory=Orchestrator,
    )

    assert result == {"status": "success"}
    assert captured["worker_chunk_runner_factory"] is marker


def test_process_runtime_spawns_for_one_remaining_chunk_without_worker_state_lane(tmp_path: Path) -> None:
    events_path = tmp_path / "process-events.jsonl"
    prepared_chunks: list[int] = []
    config = _config(tmp_path, parallel_workers=4)
    config.options["backfill"]["chunk"]["to"] = "2025-01-01"
    state = _LeaseRecordingFileStateStore(tmp_path / "state")

    @contextmanager
    def forbidden_worker_state(_worker_id):
        raise AssertionError("process lanes must keep MSSQL ledger ownership in the parent")
        yield  # pragma: no cover

    def prepare_dispatch(dispatch):
        prepared_chunks.append(dispatch.binding.chunk_index)
        return dispatch

    marker = BackfillProcessLaneRuntime(
        bootstrap=BackfillProcessLaneBootstrap(
            "tests.test_backfill_orchestrator:_open_spawn_executor_test_lane",
            {"events_path": str(events_path)},
        ),
        renew_operation_lease=lambda _operation, _expiry: True,
        validate_operation_binding=lambda _dispatch, _operation, _binding: True,
        issue_receipt_probe=_test_receipt_authority,
        validate_replay_result=_test_replay_result,
        receipt_recovery=object(),
        parent_control_scope=_test_parent_control_scope,
        prepare_dispatch=prepare_dispatch,
        process_chunk_execution_factory=BackfillProcessChunkExecution,
    )
    result = BackfillOrchestrator(
        chunk_runner=lambda _config: (_ for _ in ()).throw(
            AssertionError("parent processor must not execute a target-atomic chunk")
        ),
        state_store=state,
        worker_state_store_factory=forbidden_worker_state,
        worker_chunk_runner_factory=marker,
    ).run(config)

    events = [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines()]
    assert result["status"] == "success"
    assert [event["event"] for event in events] == ["opened", "chunk", "closed"]
    assert [event.get("chunk_index") for event in events if event["event"] == "chunk"] == [1]
    # Initial authority warm is followed by a fresh-deadline reprojection at
    # the exact pre-send boundary.
    assert prepared_chunks == [1, 1]
    now = datetime.now(UTC)
    assert state.campaign_expiries
    assert state.campaign_renewals
    assert state.chunk_expiries
    assert all(timedelta(seconds=30) < expiry - now < timedelta(minutes=2) for expiry in state.campaign_expiries)
    assert all(timedelta(seconds=30) < expiry - now < timedelta(minutes=2) for expiry in state.campaign_renewals)
    assert all(timedelta(seconds=30) < expiry - now < timedelta(minutes=2) for expiry in state.chunk_expiries)


def test_process_dispatch_refreshes_child_deadline_after_slow_batch_preflight(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events_path = tmp_path / "fresh-deadline-events.jsonl"
    config = _config(tmp_path, parallel_workers=4)
    config.options["backfill"]["chunk"]["to"] = "2025-01-01"
    state = _LeaseRecordingFileStateStore(tmp_path / "state")
    lease_clock = {"now": datetime.now(UTC)}
    issued_expiries: list[datetime] = []
    lease_ttls = iter((timedelta(seconds=1), *([timedelta(seconds=90)] * 20)))

    real_await_ready = process_lane_parent.await_ready

    def await_ready_after_virtual_startup_delay(lanes, *, timeout_seconds):
        # Simulate arbitrarily slow process bootstrap without a wall-clock
        # sleep. A chunk lease must not exist until every lane is READY.
        assert issued_expiries == []
        errors = real_await_ready(lanes, timeout_seconds=timeout_seconds)
        assert issued_expiries == []
        lease_clock["now"] += timedelta(minutes=10)
        return errors

    def issue_call_relative_expiry() -> datetime:
        expiry = lease_clock["now"] + next(lease_ttls)
        issued_expiries.append(expiry)
        return expiry

    monkeypatch.setattr(process_lane_parent, "await_ready", await_ready_after_virtual_startup_delay)
    monkeypatch.setattr(
        "dpone.backfill.process_chunk_execution.process_lane_lease_expires_at",
        issue_call_relative_expiry,
    )
    prepared_expiries: list[datetime] = []

    def prepare_dispatch(dispatch):
        prepared_expiries.append(parent_issued_operation_lease_expiry(dispatch.load_config))
        return dispatch

    @contextmanager
    def forbidden_worker_state(_worker_id):
        raise AssertionError("process lanes must keep ledger ownership in the parent")
        yield  # pragma: no cover

    marker = BackfillProcessLaneRuntime(
        bootstrap=BackfillProcessLaneBootstrap(
            "tests.test_backfill_orchestrator:_open_spawn_executor_test_lane",
            {"events_path": str(events_path)},
        ),
        renew_operation_lease=lambda _operation, _expiry: True,
        validate_operation_binding=lambda _dispatch, _operation, _binding: True,
        issue_receipt_probe=_test_receipt_authority,
        validate_replay_result=_test_replay_result,
        receipt_recovery=object(),
        parent_control_scope=_test_parent_control_scope,
        prepare_dispatch=prepare_dispatch,
        process_chunk_execution_factory=BackfillProcessChunkExecution,
    )
    result = BackfillOrchestrator(
        chunk_runner=lambda _config: pytest.fail("parent processor must not execute a target-atomic chunk"),
        state_store=state,
        worker_state_store_factory=forbidden_worker_state,
        worker_chunk_runner_factory=marker,
    ).run(config)

    child_event = next(
        event
        for event in (json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines())
        if event["event"] == "chunk"
    )
    child_expiry = datetime.fromisoformat(child_event["operation_lease_expires_at_utc"])

    assert result["status"] == "success"
    assert len(prepared_expiries) == 2
    assert prepared_expiries == issued_expiries[:2]
    assert prepared_expiries[0] == lease_clock["now"] + timedelta(seconds=1)
    assert prepared_expiries[1] == lease_clock["now"] + timedelta(seconds=90)
    assert child_expiry == prepared_expiries[1]


def test_process_dispatch_preflight_failure_releases_owned_chunk(tmp_path: Path) -> None:
    """A parent catalog failure must not orphan a RUNNING chunk until TTL."""

    config = _config(tmp_path, parallel_workers=4)
    config.options["backfill"]["chunk"]["to"] = "2025-01-01"
    state = _LeaseRecordingFileStateStore(tmp_path / "state")

    def reject_dispatch(_dispatch):
        raise RuntimeError("catalog preflight rejected password=must-not-leak")

    @contextmanager
    def forbidden_worker_state(_worker_id):
        raise AssertionError("process lanes must keep MSSQL ledger ownership in the parent")
        yield  # pragma: no cover

    marker = BackfillProcessLaneRuntime(
        bootstrap=BackfillProcessLaneBootstrap(
            "tests.test_backfill_orchestrator:_open_spawn_executor_test_lane",
            {"events_path": str(tmp_path / "preflight-events.jsonl")},
        ),
        renew_operation_lease=lambda _operation, _expiry: True,
        validate_operation_binding=lambda _dispatch, _operation: True,
        issue_receipt_probe=_test_receipt_authority,
        validate_replay_result=_test_replay_result,
        receipt_recovery=object(),
        parent_control_scope=_test_parent_control_scope,
        prepare_dispatch=reject_dispatch,
        process_chunk_execution_factory=BackfillProcessChunkExecution,
    )
    result = BackfillOrchestrator(
        chunk_runner=lambda _config: (_ for _ in ()).throw(
            AssertionError("parent processor must not execute a target-atomic chunk")
        ),
        state_store=state,
        worker_state_store_factory=forbidden_worker_state,
        worker_chunk_runner_factory=marker,
    ).run(config)

    assert result["status"] == "error"
    ledger = state.load(result["backfill"]["run_key"])
    assert ledger is not None
    record = ledger.chunk(1)
    assert record.status == "failed"
    assert record.lease_owner is None
    assert record.lease_expires_at is None
    assert "catalog preflight rejected" in str(record.error)
    assert "must-not-leak" not in str(record.error)
    assert "[REDACTED]" in str(record.error)


def test_process_dispatch_signal_during_claim_releases_owned_chunk_before_reraise(
    tmp_path: Path,
) -> None:
    """Process-control signals cannot leave a freshly acquired claim RUNNING."""

    class _ProcessSignal(BaseException):
        pass

    config = _config(tmp_path, parallel_workers=4)
    config.options["backfill"]["chunk"]["to"] = "2025-01-01"
    state = _LeaseRecordingFileStateStore(tmp_path / "state")

    def interrupt_dispatch(_dispatch):
        raise _ProcessSignal("operator stop")

    @contextmanager
    def forbidden_worker_state(_worker_id):
        raise AssertionError("process lanes must keep MSSQL ledger ownership in the parent")
        yield  # pragma: no cover

    marker = BackfillProcessLaneRuntime(
        bootstrap=BackfillProcessLaneBootstrap(
            "tests.test_backfill_orchestrator:_open_spawn_executor_test_lane",
            {"events_path": str(tmp_path / "signal-events.jsonl")},
        ),
        renew_operation_lease=lambda _operation, _expiry: True,
        validate_operation_binding=lambda _dispatch, _operation: True,
        issue_receipt_probe=_test_receipt_authority,
        validate_replay_result=_test_replay_result,
        receipt_recovery=object(),
        parent_control_scope=_test_parent_control_scope,
        prepare_dispatch=interrupt_dispatch,
        process_chunk_execution_factory=BackfillProcessChunkExecution,
    )
    orchestrator = BackfillOrchestrator(
        chunk_runner=lambda _config: pytest.fail("parent processor must not execute a target-atomic chunk"),
        state_store=state,
        worker_state_store_factory=forbidden_worker_state,
        worker_chunk_runner_factory=marker,
    )

    with pytest.raises(_ProcessSignal, match="operator stop"):
        orchestrator.run(config)

    ledgers = list(state.root_dir.glob("*.json"))
    assert len(ledgers) == 1
    ledger = state.load(ledgers[0].stem)
    assert ledger is not None
    record = ledger.chunk(1)
    assert record.status == "failed"
    assert record.lease_owner is None
    assert record.lease_expires_at is None


def test_process_dispatch_reproves_chunk_lease_after_slow_preflight(tmp_path: Path) -> None:
    """An expired preflight claim must fail before any child receives work."""

    events_path = tmp_path / "expired-preflight-events.jsonl"
    config = _config(tmp_path, parallel_workers=4)
    config.options["backfill"]["chunk"]["to"] = "2025-01-01"
    state = _LeaseRecordingFileStateStore(tmp_path / "state")

    def expire_claim(dispatch):
        ledger = state.load(dispatch.binding.run_key)
        assert ledger is not None
        record = ledger.chunk(dispatch.binding.chunk_index)
        record.lease_expires_at = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
        state.update_chunk(ledger, record)
        return dispatch

    @contextmanager
    def forbidden_worker_state(_worker_id):
        raise AssertionError("process lanes must keep MSSQL ledger ownership in the parent")
        yield  # pragma: no cover

    marker = BackfillProcessLaneRuntime(
        bootstrap=BackfillProcessLaneBootstrap(
            "tests.test_backfill_orchestrator:_open_spawn_executor_test_lane",
            {"events_path": str(events_path)},
        ),
        renew_operation_lease=lambda _operation, _expiry: True,
        validate_operation_binding=lambda _dispatch, _operation: True,
        issue_receipt_probe=_test_receipt_authority,
        validate_replay_result=_test_replay_result,
        receipt_recovery=object(),
        parent_control_scope=_test_parent_control_scope,
        prepare_dispatch=expire_claim,
        process_chunk_execution_factory=BackfillProcessChunkExecution,
    )
    result = BackfillOrchestrator(
        chunk_runner=lambda _config: (_ for _ in ()).throw(
            AssertionError("parent processor must not execute a target-atomic chunk")
        ),
        state_store=state,
        worker_state_store_factory=forbidden_worker_state,
        worker_chunk_runner_factory=marker,
    ).run(config)

    events = [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines()]
    assert result["status"] == "error"
    assert not any(event["event"] == "chunk" for event in events)
    ledger = state.load(result["backfill"]["run_key"])
    assert ledger is not None
    record = ledger.chunk(1)
    assert record.status == "failed"
    assert record.lease_owner is None
    assert record.lease_expires_at is None
    assert "DPONE_BACKFILL_CHUNK_LEASE_HEARTBEAT_LOST" in str(record.error)


@pytest.mark.parametrize(
    ("bootstrap", "expected_error"),
    [
        (
            BackfillProcessLaneBootstrap(
                "tests.test_backfill_orchestrator:_open_spawn_executor_test_lane",
                {"not_serializable": lambda: None},
            ),
            "DPONE_BACKFILL_PROCESS_BOOTSTRAP_NOT_SERIALIZABLE",
        ),
        (
            BackfillProcessLaneBootstrap("dpone.does_not_exist:open_lane", {}),
            ("DPONE_BACKFILL_PROCESS_BOOTSTRAP_FAILED: error=backfill.process_lane_entrypoint_unavailable"),
        ),
    ],
    ids=["not-serializable", "entrypoint-unavailable"],
)
def test_process_bootstrap_preflight_precedes_all_campaign_mutation(
    tmp_path: Path,
    bootstrap: BackfillProcessLaneBootstrap,
    expected_error: str,
) -> None:
    events: list[str] = []
    state = FileBackfillStateStore(tmp_path / "state")

    class Lifecycle:
        def require_unmapped(self, *, selection):
            events.append("lifecycle.require_unmapped")

        def campaign_contract(self, load_config):
            events.append("lifecycle.campaign_contract")
            return {}

        def before_chunks(self, load_config, ledger, store):
            events.append("lifecycle.before_chunks")
            return ledger

        def after_chunks(self, load_config, ledger, store):
            events.append("lifecycle.after_chunks")
            return {}

    class Publisher:
        def require_unmapped(self, *, selection):
            events.append("publisher.require_unmapped")

        def campaign_contract(self, load_config):
            events.append("publisher.campaign_contract")
            return {}

        def before_chunks(self, load_config, ledger, store):
            events.append("publisher.before_chunks")
            return load_config

        def after_chunks(self, load_config, ledger, store):
            events.append("publisher.after_chunks")
            return {}

    @contextmanager
    def worker_state_store(_worker_id):
        events.append("worker_state_store.open")
        yield state

    runtime = BackfillProcessLaneRuntime(
        bootstrap=bootstrap,
        renew_operation_lease=lambda _operation, _expiry: True,
        validate_operation_binding=lambda _dispatch, _operation, _binding: True,
        issue_receipt_probe=_test_receipt_authority,
        validate_replay_result=_test_replay_result,
        receipt_recovery=object(),
        parent_control_scope=_test_parent_control_scope,
        process_chunk_execution_factory=BackfillProcessChunkExecution,
    )
    orchestrator = BackfillOrchestrator(
        chunk_runner=lambda _config: events.append("source.read") or {"status": "success"},
        state_store=state,
        worker_state_store_factory=worker_state_store,
        worker_chunk_runner_factory=runtime,
        campaign_lifecycle=Lifecycle(),
        target_publisher=Publisher(),
    )

    with pytest.raises(RuntimeError) as raised:
        orchestrator.run(_config(tmp_path, parallel_workers=4))

    assert str(raised.value) == expected_error
    assert events == []
    assert not state.root_dir.exists()


def test_process_history_proof_preflight_precedes_campaign_mutation(tmp_path: Path) -> None:
    config = _config(tmp_path, parallel_workers=4)
    config.options["source_type"] = "postgres"
    config.options["sink_type"] = "mssql"
    config.partition = {"column": "d", "values_from_staging": True}
    state = FileBackfillStateStore(tmp_path / "state")

    @contextmanager
    def worker_state_store(_worker_id):
        yield state

    runtime = BackfillProcessLaneRuntime(
        bootstrap=BackfillProcessLaneBootstrap(
            "tests.test_backfill_orchestrator:_open_spawn_executor_test_lane",
            {"events_path": str(tmp_path / "events.jsonl")},
        ),
        renew_operation_lease=lambda _operation, _expiry: True,
        validate_operation_binding=lambda _dispatch, _operation, _binding: True,
        issue_receipt_probe=_test_receipt_authority,
        validate_replay_result=_test_replay_result,
        receipt_recovery=object(),
        parent_control_scope=_test_parent_control_scope,
        portable_scope_column_resolver=lambda *_args: PortableScopeColumnContract("d", "date", None, "d", "date", None),
        process_chunk_execution_factory=BackfillProcessChunkExecution,
    )

    with pytest.raises(RuntimeError, match="backfill.process_portable_scope_history_proof_unavailable"):
        BackfillOrchestrator(
            chunk_runner=lambda _config: pytest.fail("preflight must reject before chunk execution"),
            state_store=state,
            worker_state_store_factory=worker_state_store,
            worker_chunk_runner_factory=runtime,
        ).run(config)

    assert not state.root_dir.exists()


def test_process_runtime_preserves_parent_heartbeat_policy_without_sql_thread(tmp_path: Path) -> None:
    events_path = tmp_path / "process-events.jsonl"
    config = _config(tmp_path, parallel_workers=4)
    config.options["backfill"]["chunk"]["to"] = "2025-01-01"
    state = FileBackfillStateStore(tmp_path / "state")
    observed_renewals: list[bool] = []

    @contextmanager
    def forbidden_worker_state(_worker_id):
        raise AssertionError("process lanes must keep MSSQL ledger ownership in the parent")
        yield  # pragma: no cover

    def reject_after_real_renewal(renew, *, initial_expiry, now):
        def reject() -> bool:
            observed_renewals.append(bool(renew()))
            return False

        return BackfillLeaseHeartbeat(reject, initial_expiry=initial_expiry, now=now)

    marker = BackfillProcessLaneRuntime(
        bootstrap=BackfillProcessLaneBootstrap(
            "tests.test_backfill_orchestrator:_open_spawn_executor_test_lane",
            {"events_path": str(events_path)},
        ),
        renew_operation_lease=lambda _operation, _expiry: True,
        validate_operation_binding=lambda _dispatch, _operation, _binding: True,
        issue_receipt_probe=_test_receipt_authority,
        validate_replay_result=_test_replay_result,
        receipt_recovery=object(),
        parent_control_scope=_test_parent_control_scope,
        process_chunk_execution_factory=BackfillProcessChunkExecution,
    )
    result = BackfillOrchestrator(
        chunk_runner=lambda _config: (_ for _ in ()).throw(AssertionError("parent chunk runner is forbidden")),
        state_store=state,
        heartbeat_factory=reject_after_real_renewal,
        worker_state_store_factory=forbidden_worker_state,
        worker_chunk_runner_factory=marker,
    ).run(config)

    events = [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines()]
    assert result["status"] == "error"
    assert observed_renewals == [True]
    assert not any(event["event"] == "chunk" for event in events)


def test_state_store_env_override(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DPONE_BACKFILL_STATE_DIR", str(tmp_path / "custom"))

    store = FileBackfillStateStore()

    assert store.root_dir == tmp_path / "custom"


class _AuditConnector:
    def __init__(self) -> None:
        self.calls: list[tuple[object, object | None]] = []

    def execute_query(self, sql, params=None):
        self.calls.append((sql, params))

    def get_records(self, sql, params=None, as_dict=False):
        self.calls.append((sql, params))
        if "dpone_backfill_journal_shape:clickhouse" not in str(sql):
            return []
        rows = [
            {
                "column_name": "journal_id",
                "data_type": "UInt64",
                "default_kind": "DEFAULT",
                "default_expression": "generateSnowflakeID()",
                "engine_full": "ReplacingMergeTree(journal_id) ORDER BY run_key",
            }
        ]
        return rows if as_dict else [tuple(rows[0].values())]
