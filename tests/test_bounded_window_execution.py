"""Synthetic fault boundaries for durable bounded-window execution."""

import hashlib
import threading
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from dpone.adapters.bounded_window_journal import WindowJournal
from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.contracts.bounded_window import ChunkReceipt, WindowPlan
from dpone.contracts.process_errors import WindowContractError, WindowOutcomeUnknown, WindowTransientError
from dpone.runtime.bounded_window_execution import BoundedWindowExecutor


def plan(workers=2):
    start = datetime(2026, 1, 1, tzinfo=UTC)
    return WindowPlan(
        "route",
        "target",
        start,
        start + timedelta(days=2),
        (start, start + timedelta(days=1), start + timedelta(days=2)),
        "schema",
        "immutable-v1",
        "params",
        workers,
        window_column="at",
    )


class Source:
    def __init__(self):
        self.closed = 0
        self.expired = False

    def validate(self, plan):
        if self.expired:
            raise WindowContractError("expired source version")

    def read(self, plan, chunk):
        try:
            yield (chunk.start.isoformat(), 1)
            yield (chunk.start.isoformat(), 1)
        finally:
            self.closed += 1


class Target:
    def __init__(self):
        self.receipts = {}
        self.rows = {}
        self.published = False
        self.calls = 0
        self.exchanges = 0
        self.fail_stage = None
        self.fail_publish = None
        self.reject = False
        self.discards = 0
        self.lock = threading.Lock()

    def validate(self, plan):
        if self.reject:
            raise WindowContractError("unsupported topology")

    def stage(self, plan, chunk, attempt_id, rows, lease):
        with self.lock:
            self.calls += 1
        if self.fail_stage == "before":
            raise WindowTransientError("network")
        if self.fail_stage == "terminal":
            raise WindowContractError("schema")
        values = list(rows)
        receipt = ChunkReceipt(
            chunk.chunk_id, attempt_id, len(values), hashlib.sha256(repr(sorted(values)).encode()).hexdigest()
        )
        self.receipts[attempt_id] = receipt
        self.rows[attempt_id] = values
        if self.fail_stage == "after":
            raise WindowTransientError("ack lost")
        return receipt

    def inspect_attempt(self, plan, chunk, attempt_id, lease):
        return self.receipts.get(attempt_id)

    def discard_attempt(self, plan, chunk, attempt_id, lease):
        self.discards += 1
        self.receipts.pop(attempt_id, None)
        self.rows.pop(attempt_id, None)

    def prepare(self, plan, receipts, lease):
        assert sum(x.row_count for x in receipts) == 4
        return plan.run_id

    def publish(self, plan, generation, lease):
        self.exchanges += 1
        if self.fail_publish == "before":
            raise WindowTransientError("unknown")
        self.published = True
        if self.fail_publish == "after":
            raise WindowTransientError("ack lost")

    def inspect_publication(self, plan, generation):
        return "published" if self.published else "unknown"


def setup(tmp_path, *, evidence=None, state=None):
    source, target = Source(), Target()
    store = SQLiteWindowStore(tmp_path / "journal.sqlite", clock=lambda: 1.0)
    events = []
    executor = BoundedWindowExecutor(
        source=source,
        target=target,
        store=store,
        journal_factory=lambda lease, run_id: WindowJournal(store, lease, run_id),
        evidence=evidence or (lambda *args: events.append("evidence")),
        advance_state=state or (lambda *args: events.append("state")),
        sleeper=lambda _: None,
    )
    return executor, source, target, events


def test_duplicate_multiplicity_and_repeat(tmp_path):
    executor, source, target, events = setup(tmp_path)
    result = executor.execute(plan(), "owner")
    assert sum(r.row_count for r in result.receipts) == 4
    assert events == ["evidence", "state"]
    assert source.closed == 2
    executor.execute(plan(), "owner")
    assert target.calls == 2 and target.exchanges == 1
    assert events == ["evidence", "state"]


@pytest.mark.parametrize("phase", ["before", "after"])
def test_transient_stage_reconciliation(tmp_path, phase):
    executor, _, target, _ = setup(tmp_path)
    target.fail_stage = phase
    if phase == "before":
        with pytest.raises(WindowTransientError):
            executor.execute(plan(workers=1), "owner")
        assert target.calls == 3
        assert target.exchanges == 0
    else:
        executor.execute(plan(), "owner")
        assert target.calls == 2 and target.exchanges == 1


def test_terminal_error_is_not_retried(tmp_path):
    executor, _, target, events = setup(tmp_path)
    target.fail_stage = "terminal"
    with pytest.raises(WindowContractError):
        executor.execute(plan(workers=1), "owner")
    assert target.calls == 1 and target.exchanges == 0 and events == []


@pytest.mark.parametrize("phase", ["before", "after"])
def test_publication_ack_and_unknown_never_exchange_twice(tmp_path, phase):
    executor, _, target, events = setup(tmp_path)
    target.fail_publish = phase
    if phase == "before":
        for _ in range(2):
            with pytest.raises(WindowOutcomeUnknown):
                executor.execute(plan(), "owner")
        assert events == []
    else:
        executor.execute(plan(), "owner")
        executor.execute(plan(), "owner")
        assert events == ["evidence", "state"]
    assert target.exchanges == 1


def test_evidence_failure_recovers_after_expired_snapshot(tmp_path):
    calls = []

    def evidence(*args):
        calls.append("evidence")
        if len(calls) == 1:
            raise OSError("disk full")

    executor, source, target, events = setup(tmp_path, evidence=evidence)
    with pytest.raises(OSError):
        executor.execute(plan(), "owner")
    assert target.published and events == []
    source.expired = True
    executor.execute(plan(), "owner")
    assert calls == ["evidence", "evidence"] and events == ["state"]
    assert target.exchanges == 1


def test_state_failure_retries_idempotent_state_after_evidence(tmp_path):
    calls = []

    def state(*args):
        calls.append("state")
        if len(calls) == 1:
            raise OSError("state unavailable")

    executor, _, target, events = setup(tmp_path, state=state)
    with pytest.raises(OSError):
        executor.execute(plan(), "owner")
    executor.execute(plan(), "owner")
    assert calls == ["state", "state"] and events == ["evidence", "evidence"]
    assert target.exchanges == 1


def test_capability_failure_before_writes(tmp_path):
    executor, _, target, _ = setup(tmp_path)
    target.reject = True
    with pytest.raises(WindowContractError):
        executor.execute(plan(), "owner")
    assert not target.calls


@pytest.mark.parametrize(
    "field,value",
    [
        ("schema_fingerprint", "other"),
        ("source_version", "other"),
        ("parameters_fingerprint", "other"),
        ("target_id", "other"),
    ],
)
def test_identity_binds_contract(field, value):
    assert replace(plan(), **{field: value}).run_id != plan().run_id
    assert replace(plan(), workers=1).run_id == plan().run_id


@pytest.mark.parametrize("workers", [0, -1, 65, True, 1.5])
def test_invalid_workers(workers):
    with pytest.raises(WindowContractError):
        plan(workers)


def test_half_open_chunks_and_aware_boundaries():
    p = plan()
    assert p.chunks[0].end == p.chunks[1].start
    with pytest.raises(WindowContractError):
        replace(p, boundaries=(p.end, p.start))
    with pytest.raises(WindowContractError):
        replace(p, start=p.start.replace(tzinfo=None))


def test_retry_exhaustion_survives_restart(tmp_path):
    executor, _, target, _ = setup(tmp_path)
    target.fail_stage = "before"
    with pytest.raises(WindowTransientError):
        executor.execute(plan(workers=1), "owner")
    with pytest.raises(WindowContractError, match="terminally"):
        executor.execute(plan(workers=1), "owner")
    assert target.calls == 3


def test_checkpoint_failure_reconciles_applied_attempt(tmp_path):
    executor, _, target, _ = setup(tmp_path)
    original = executor.store.save
    failed = []

    def save(key, expected, payload, lease):
        if '"phase": "verified"' in payload and not failed:
            failed.append(True)
            raise OSError("checkpoint unavailable")
        return original(key, expected, payload, lease)

    executor.store.save = save
    with pytest.raises(OSError):
        executor.execute(plan(workers=1), "owner")
    executor.execute(plan(workers=1), "owner")
    assert target.calls == 2 and target.exchanges == 1


def test_checkpointed_staging_drift_refuses_resume(tmp_path):
    executor, _, target, _ = setup(tmp_path)
    original = target.prepare
    target.prepare = lambda *args: (_ for _ in ()).throw(OSError("prepare interrupted"))
    with pytest.raises(OSError):
        executor.execute(plan(workers=1), "owner")
    target.prepare = original
    key = next(iter(target.receipts))
    target.receipts[key] = replace(target.receipts[key], row_count=99)
    with pytest.raises(WindowContractError, match="changed"):
        executor.execute(plan(workers=1), "owner")
    assert target.exchanges == 0


def test_lease_loss_during_stream_closes_source_and_never_publishes(tmp_path):
    executor, source, target, events = setup(tmp_path)
    original = source.read

    def read(plan, chunk):
        for row in original(plan, chunk):
            active = executor.store.acquire("other", "owner", 10)
            # A competing writer cannot acquire the live target. Expire this test lease.
            with executor.store._connect() as db:
                db.execute("UPDATE window_leases SET expires=0 WHERE target=?", ("target",))
            executor.store.release(active)
            yield row

    source.read = read
    with pytest.raises(WindowContractError):
        executor.execute(plan(workers=1), "owner")
    assert source.closed == 1 and target.exchanges == 0 and events == []


def test_crash_restarts_consume_attempt_budget(tmp_path):
    executor, _, target, _ = setup(tmp_path)
    attempts = []

    def crash(plan, chunk, attempt_id, rows, lease):
        attempts.append(attempt_id)
        raise OSError("process interrupted after admission")

    target.stage = crash
    for _ in range(3):
        with pytest.raises(OSError):
            executor.execute(plan(workers=1), "owner")
    with pytest.raises(WindowContractError, match="exhausted"):
        executor.execute(plan(workers=1), "owner")
    assert len(attempts) == len(set(attempts)) == 3


def test_boundary_list_is_snapshotted():
    boundaries = list(plan().boundaries)
    frozen = replace(plan(), boundaries=boundaries)
    identity = frozen.run_id
    boundaries.clear()
    assert frozen.run_id == identity and len(frozen.chunks) == 2


@pytest.mark.parametrize(
    "field,value",
    [
        ("route_id", 1),
        ("target_id", []),
        ("schema_fingerprint", True),
        ("source_version", ["v1"]),
        ("parameters_fingerprint", {}),
        ("start", "2026-01-01"),
    ],
)
def test_identity_fields_reject_mutable_and_wrong_types(field, value):
    with pytest.raises(WindowContractError):
        replace(plan(), **{field: value})


@pytest.mark.parametrize(
    "bad",
    [
        {"version": True, "phase": "planned"},
        {"version": 1, "phase": "surprise"},
        {"version": 1, "phase": "staging", "attempt": True},
        {"version": 1, "phase": "staging", "attempt": -1},
        {"version": 1, "phase": "staging", "attempt": 3},
    ],
)
def test_invalid_journal_refused_before_stage(tmp_path, bad):
    import json

    executor, _, target, _ = setup(tmp_path)
    p = plan(workers=1)
    lease = executor.store.acquire(p.target_id, "seed", 30)
    name = "run" if "attempt" not in bad else "chunk/" + p.chunks[0].chunk_id
    executor.store.save(p.run_id + "/" + name, None, json.dumps(bad), lease)
    executor.store.release(lease)
    with pytest.raises(WindowContractError):
        executor.execute(p, "owner")
    assert target.calls == 0


@pytest.mark.parametrize("mutation", ["empty", "duplicate", "wrong_attempt", "bool_count", "bad_generation"])
def test_corrupt_success_receipts_cannot_report_success(tmp_path, mutation):
    import json

    executor, _, target, events = setup(tmp_path)
    p = plan()
    executor.execute(p, "owner")
    record = executor.store.load(p.run_id + "/run")
    data = json.loads(record.payload)
    if mutation == "empty":
        data["receipts"] = []
    elif mutation == "duplicate":
        data["receipts"][1] = data["receipts"][0]
    elif mutation == "wrong_attempt":
        data["receipts"][0]["attempt_id"] = "alien"
    elif mutation == "bool_count":
        data["receipts"][0]["row_count"] = True
    else:
        data["generation"] = 123
    lease = executor.store.acquire(p.target_id, "seed", 30)
    executor.store.save(p.run_id + "/run", record.revision, json.dumps(data), lease)
    executor.store.release(lease)
    with pytest.raises(WindowContractError):
        executor.execute(p, "owner")
    assert target.exchanges == 1 and events == ["evidence", "state"]


def test_bounded_pending_observes_later_failure_without_waiting_for_first(tmp_path, monkeypatch):
    import dpone.runtime.bounded_window_execution as module

    submitted = []
    original = module.ThreadPoolExecutor

    class TrackingPool(original):
        def submit(self, fn, *args, **kwargs):
            submitted.append(args[1])
            return super().submit(fn, *args, **kwargs)

    monkeypatch.setattr(module, "ThreadPoolExecutor", TrackingPool)
    executor, _, _, _ = setup(tmp_path)
    first_started = threading.Event()

    def chunk(plan, chunk, lease, journal, cancelled):
        if chunk.start == plan.start:
            first_started.set()
            assert cancelled.wait(2), "later failure was not observed promptly"
            raise WindowContractError("first cancelled")
        assert first_started.wait(1)
        raise WindowContractError("second failed")

    executor._chunk = chunk
    p = plan()
    p = replace(p, end=p.start + timedelta(days=10), boundaries=tuple(p.start + timedelta(days=i) for i in range(11)))
    with pytest.raises(WindowContractError, match="second failed"):
        executor.execute(p, "owner")
    assert len(submitted) == p.workers


def test_stalled_state_callback_rechecks_fence_at_actual_mutation(tmp_path):
    executor, _, _, _ = setup(tmp_path)
    writes = []

    def state(plan, lease):
        # Simulate expiration and a newer run while the callback was paused.
        with executor.store._connect() as db:
            db.execute("UPDATE window_leases SET expires=0 WHERE target=?", (plan.target_id,))
        new_lease = executor.store.acquire(plan.target_id, "new-writer", 30)
        try:
            # The callback owns a CAS-protected mutation, not a prechecked boolean.
            executor.store.save("source-state", None, plan.run_id, lease)
            writes.append(plan.run_id)
        finally:
            executor.store.release(new_lease)

    executor.advance_state = state
    with pytest.raises(WindowContractError):
        executor.execute(plan(), "owner")
    assert not writes and executor.store.load("source-state") is None


def test_cleanup_failure_preserves_primary_stage_error(tmp_path):
    executor, _, target, _ = setup(tmp_path)
    target.fail_stage = "terminal"

    def release(lease):
        raise OSError("release failed")

    executor.store.release = release
    with pytest.raises(WindowContractError, match="schema") as error:
        executor.execute(plan(workers=1), "owner")
    assert any("release failed" in note for note in error.value.__notes__)


def test_lease_queries_are_bounded_per_row_stream(tmp_path):
    executor, source, target, _ = setup(tmp_path)

    def read(plan, chunk):
        yield from ((1,) for _ in range(3000))

    source.read = read
    target.prepare = lambda *args: "generation"
    checks = []
    original = executor.store.assert_lease

    def check(lease):
        checks.append(lease)
        original(lease)

    executor.store.assert_lease = check
    executor.execute(plan(), "owner")
    assert len(checks) < 40


def test_interrupted_cleanup_reconciles_all_older_attempts(tmp_path):
    executor, _, target, _ = setup(tmp_path)
    original = target.discard_attempt
    attempts = []

    def crash(plan, chunk, attempt_id, rows, lease):
        attempts.append(attempt_id)
        raise OSError("crash in stage")

    target.stage = crash
    with pytest.raises(OSError):
        executor.execute(plan(workers=1), "owner")
    discarded = []

    def discard(plan, chunk, attempt_id, lease):
        discarded.append(attempt_id)
        if len(discarded) == 1:
            raise OSError("cleanup interrupted")
        original(plan, chunk, attempt_id, lease)

    target.discard_attempt = discard
    with pytest.raises(OSError):
        executor.execute(plan(workers=1), "owner")
    with pytest.raises(OSError):
        executor.execute(plan(workers=1), "owner")
    assert [x.rsplit("-", 1)[1] for x in attempts] == ["0", "2"]
    assert [x.rsplit("-", 1)[1] for x in discarded] == ["0", "0", "1"]


def test_row_close_failure_keeps_primary_stage_error(tmp_path):
    executor, source, target, _ = setup(tmp_path)

    def read(plan, chunk):
        try:
            yield (1,)
        finally:
            raise OSError("cursor close failed")

    def stage(plan, chunk, attempt_id, rows, lease):
        next(rows)
        raise WindowContractError("stage invalid")

    source.read, target.stage = read, stage
    with pytest.raises(WindowContractError, match="stage invalid") as error:
        executor.execute(plan(workers=1), "owner")
    assert any("cursor close failed" in note for note in error.value.__notes__)


def test_independent_lease_heartbeat_during_blocked_source(tmp_path):
    executor, source, _, _ = setup(tmp_path)
    executor.lease_ttl = 0.03
    renewed = threading.Event()
    original_renew = executor.store.renew
    original_read = source.read

    def renew(lease, ttl):
        original_renew(lease, ttl)
        renewed.set()

    def read(plan, chunk):
        assert renewed.wait(1), "independent renewal did not run"
        yield from original_read(plan, chunk)

    executor.store.renew, source.read = renew, read
    executor.execute(plan(), "owner")
    assert renewed.is_set()


def test_heartbeat_shutdown_timeout_is_explicit_failure(tmp_path, monkeypatch):
    import dpone.runtime.bounded_window_execution as module

    joins = []

    class StuckThread:
        def __init__(self, **kwargs):
            pass

        def start(self):
            pass

        def join(self, timeout):
            joins.append(timeout)

        def is_alive(self):
            return True

    monkeypatch.setattr(module, "Thread", StuckThread)
    executor, _, _, _ = setup(tmp_path)
    with pytest.raises(WindowContractError, match="did not stop"):
        executor.execute(plan(), "owner")
    assert joins == [35.0]


def test_execute_leased_keeps_caller_ownership_and_renews(tmp_path):
    executor, source, _, _ = setup(tmp_path)
    executor.lease_ttl = 0.03
    p = plan()
    lease = executor.store.acquire(p.target_id, "caller", executor.lease_ttl)
    original_release = executor.store.release
    original_renew = executor.store.renew
    renewed = threading.Event()

    def forbidden(*args):
        raise AssertionError("caller owns acquire/release")

    def renew(lease, ttl):
        original_renew(lease, ttl)
        renewed.set()

    original_read = source.read

    def read(plan, chunk):
        assert renewed.wait(1)
        yield from original_read(plan, chunk)

    executor.store.acquire = forbidden
    executor.store.release = forbidden
    executor.store.renew = renew
    source.read = read
    result = executor.execute_leased(p, lease)
    assert result.run_id == p.run_id
    executor.store.assert_lease(lease)
    original_release(lease)


def test_execute_leased_rejects_mismatched_target(tmp_path):
    executor, _, target, _ = setup(tmp_path)
    lease = executor.store.acquire("other-target", "caller", 30)
    with pytest.raises(WindowContractError, match="target"):
        executor.execute_leased(plan(), lease)
    executor.store.assert_lease(lease)
    assert target.calls == 0
    executor.store.release(lease)


def test_execute_leased_failure_does_not_release_caller(tmp_path):
    executor, _, target, _ = setup(tmp_path)
    target.fail_stage = "terminal"
    p = plan(workers=1)
    lease = executor.store.acquire(p.target_id, "caller", 30)
    with pytest.raises(WindowContractError):
        executor.execute_leased(p, lease)
    executor.store.assert_lease(lease)
    executor.store.release(lease)


def test_window_column_changes_identity_and_rejects_sql_fragment():
    from dataclasses import replace

    frozen = plan()
    assert replace(frozen, window_column="other_at").run_id != frozen.run_id
    with pytest.raises(WindowContractError, match="column"):
        replace(frozen, window_column="at; DROP TABLE x")
