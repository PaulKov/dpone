"""Manifest-to-runtime admission, interval and result contracts."""

import json
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import jsonschema
import pytest

from dpone.adapters.bounded_window_journal import WindowJournal
from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.contracts.bounded_window import ChunkReceipt, WindowResult
from dpone.contracts.process_errors import ETLProcessError
from dpone.dag.errors import DagConfigurationError
from dpone.dag.load_config_scope import resolve_load_scopes
from dpone.runtime.bootstrap_runner import DefaultProcessRunner
from dpone.runtime.rolling_window_runtime import RollingWindowRuntime

WINDOW = {"column": "observed_at", "anchor": "data_interval_end", "lookback": "P2D"}


def test_manifest_compiles_window_without_inventing_raw_sql():
    options = {}
    scope, sql = resolve_load_scopes(
        strategy_config={"mode": "replace", "atomicity": "target_atomic", "window": WINDOW},
        source_options={},
        options=options,
        parse_tracer=None,
    )
    assert scope is None and sql is None
    assert options["rolling_window"]["column"] == "observed_at"


@pytest.mark.parametrize(
    "update",
    [{"mode": "full_refresh"}, {"atomicity": "best_effort"}, {"custom_predicate": "1=1"}, {"portable_scope": {}}],
)
def test_manifest_rejects_conflicting_window_policy(update):
    with pytest.raises(DagConfigurationError):
        resolve_load_scopes(
            strategy_config={"mode": "replace", "atomicity": "target_atomic", "window": WINDOW, **update},
            source_options={},
            options={},
            parse_tracer=None,
        )


@pytest.mark.parametrize("name", ["etl-config.schema.json", "etl-batch-manifest.schema.json"])
def test_public_schema_admits_only_atomic_replace_windows(name):
    schema = json.loads((Path(__file__).parents[1] / "src/dpone/schema" / name).read_text())
    fragment = schema if name.startswith("etl-config") else schema["definitions"]["process_fragment"]
    strategy = fragment["properties"]["sink"]["properties"]["strategy"]
    validator = jsonschema.Draft7Validator({**strategy, "definitions": schema["definitions"]})
    valid = {"mode": "replace", "atomicity": "target_atomic", "window": WINDOW}
    assert list(validator.iter_errors(valid)) == []
    for bad in [
        {**valid, "mode": "full_refresh"},
        {"mode": "replace", "window": WINDOW},
        {**valid, "custom_predicate": "1=1"},
        {"mode": "replace", "atomicity": "target_atomic"},
    ]:
        assert list(validator.iter_errors(bad))


def test_default_runner_refuses_missing_authority_before_hydration():
    def forbidden():
        pytest.fail("No source or target may be opened without window authority")

    process = SimpleNamespace(
        config=SimpleNamespace(
            name="window",
            load_config=SimpleNamespace(options={"rolling_window": WINDOW}),
            ensure_runtime_bindings=forbidden,
        )
    )
    with pytest.raises(ETLProcessError, match="capability_required"):
        DefaultProcessRunner().run(process)


def test_runtime_uses_interval_and_closes_snapshot_after_publication(tmp_path):
    events = []

    @contextmanager
    def source_factory(window):
        events.append("open")
        assert window.end.isoformat() == "2026-09-09T00:00:00+00:00"
        try:
            yield SimpleNamespace(source_version="snapshot", parameters_fingerprint="query")
        finally:
            events.append("close")

    class Executor:
        def execute_leased(self, plan, lease):
            assert lease.owner == "invocation" and plan.workers == 4
            events.append("published")
            return WindowResult(
                plan.run_id,
                "generation",
                tuple(ChunkReceipt(chunk.chunk_id, chunk.chunk_id + "-0", 1, "digest") for chunk in plan.chunks),
            )

    runtime = RollingWindowRuntime(
        source_factory=source_factory,
        executor_factory=lambda source: Executor(),
        route_id="route",
        target_id="target",
        schema_fingerprint="schema",
        generation_total=lambda plan, result: 42,
        store=SQLiteWindowStore(tmp_path / "journal.sqlite", clock=lambda: 1),
    )
    result = runtime.run(
        SimpleNamespace(
            options={
                "rolling_window": WINDOW,
                "interval": {"interval_end": "2026-09-09T03:00:00+03:00"},
                "native_transfer": {"execution": {"chunking": {"parallelism": 4}}},
            }
        ),
        owner="invocation",
    )
    assert result.status == "success" and result.inserted_rows == 2 and result.final_rows == 42
    assert events == ["open", "published", "close"]


@pytest.mark.parametrize(
    "chunking",
    [
        {"parallelism": True},
        {"parallelism": 0},
        {"parallelism": 65},
        {"parallelism": "4"},
        {"mode": "unbounded"},
        {"unknown": 1},
    ],
)
def test_invalid_parallelism_fails_before_opening_snapshot(chunking, tmp_path):
    def forbidden(_):
        pytest.fail("Snapshot opened for invalid configuration")

    runtime = RollingWindowRuntime(
        source_factory=forbidden,
        executor_factory=forbidden,
        route_id="route",
        target_id="target",
        schema_fingerprint="schema",
        generation_total=forbidden,
        store=SQLiteWindowStore(tmp_path / "journal.sqlite", clock=lambda: 1),
    )
    with pytest.raises(ValueError, match="rolling_window"):
        runtime.run(
            SimpleNamespace(
                options={
                    "rolling_window": WINDOW,
                    "interval": {"interval_end": "2026-09-09T00:00:00Z"},
                    "native_transfer": {"execution": {"chunking": chunking}},
                }
            ),
            owner="owner",
        )


class _VersionedSource:
    parameters_fingerprint = "query"

    def __init__(self, version):
        self.source_version = version

    def validate(self, plan):
        assert plan.source_version == self.source_version

    def read(self, plan, chunk):
        yield (1,)


class _Target:
    def __init__(self):
        self.receipts = {}
        self.published = False
        self.stage_calls = 0
        self.exchange_calls = 0
        self.crash = False

    def validate(self, plan):
        pass

    def stage(self, plan, chunk, attempt_id, rows, lease):
        self.stage_calls += 1
        if self.crash:
            raise OSError("crash before stage acknowledgment")
        receipt = ChunkReceipt(chunk.chunk_id, attempt_id, sum(1 for _ in rows), "digest")
        self.receipts[attempt_id] = receipt
        return receipt

    def inspect_attempt(self, plan, chunk, attempt_id, lease):
        return self.receipts.get(attempt_id)

    def discard_attempt(self, plan, chunk, attempt_id, lease):
        self.receipts.pop(attempt_id, None)

    def prepare(self, plan, receipts, lease):
        return plan.run_id

    def publish(self, plan, generation, lease):
        self.exchange_calls += 1
        self.published = True

    def inspect_publication(self, plan, generation):
        return "published" if self.published else "unknown"


def _runtime(tmp_path, *, evidence=None):
    from dpone.runtime.bounded_window_execution import BoundedWindowExecutor

    store = SQLiteWindowStore(tmp_path / "wrapper.sqlite", clock=lambda: 1)
    events, versions = [], ["snapshot-1"]
    target = _Target()

    @contextmanager
    def source_factory(window):
        # Source opens only after the target lease has been acquired.
        from dpone.contracts.process_errors import WindowLeaseLost

        with pytest.raises(WindowLeaseLost):
            store.acquire("target", "competing", 30)
        events.append("open")
        try:
            yield _VersionedSource(versions[0])
        finally:
            with pytest.raises(WindowLeaseLost):
                store.acquire("target", "competing", 30)
            events.append("close")

    def executor_factory(source):
        return BoundedWindowExecutor(
            source=source,
            target=target,
            store=store,
            journal_factory=lambda lease, run_id: WindowJournal(store, lease, run_id),
            evidence=evidence or (lambda *args: events.append("evidence")),
            advance_state=lambda *args: events.append("state"),
            sleeper=lambda _: None,
        )

    runtime = RollingWindowRuntime(
        source_factory=source_factory,
        executor_factory=executor_factory,
        route_id="route",
        target_id="target",
        schema_fingerprint="schema",
        store=store,
        generation_total=lambda plan, result: sum(r.row_count for r in result.receipts),
    )
    return runtime, store, events, versions, target


def _load():
    return SimpleNamespace(options={"rolling_window": WINDOW, "interval": {"interval_end": "2026-09-09T00:00:00Z"}})


def test_wrapper_postpublication_retry_never_reopens_source(tmp_path):
    evidence_calls = []

    def evidence(*args):
        evidence_calls.append(args)
        if len(evidence_calls) == 1:
            raise OSError("evidence disk full")

    runtime, store, events, _, target = _runtime(tmp_path, evidence=evidence)
    with pytest.raises(OSError, match="disk full"):
        runtime.run(_load(), owner="invocation")
    assert target.published and events == ["open", "close"]
    runtime._source_factory = lambda _: pytest.fail("Postpublication recovery reopened source")
    result = runtime.run(_load(), owner="invocation")
    assert result.status == "success" and target.exchange_calls == 1
    assert evidence_calls[0][0].run_id == evidence_calls[1][0].run_id
    assert events == ["open", "close", "state"]
    lease = store.acquire("target", "after-cleanup", 30)
    store.release(lease)


def test_wrapper_success_repeat_never_reopens_source(tmp_path):
    runtime, _, events, _, target = _runtime(tmp_path)
    first = runtime.run(_load(), owner="invocation")
    runtime._source_factory = lambda _: pytest.fail("Succeeded retry reopened source")
    second = runtime.run(_load(), owner="invocation")
    assert first.details == second.details and target.exchange_calls == 1
    assert events == ["open", "evidence", "state", "close"]


def test_wrapper_planned_snapshot_change_requires_new_invocation(tmp_path):
    from dpone.contracts.process_errors import WindowContractError

    runtime, _, events, versions, target = _runtime(tmp_path)
    target.crash = True
    with pytest.raises(OSError):
        runtime.run(_load(), owner="invocation")
    versions[0] = "snapshot-2"
    with pytest.raises(WindowContractError, match="full re-extraction requires a new invocation"):
        runtime.run(_load(), owner="invocation")
    assert target.stage_calls == 1 and events == ["open", "close", "open", "close"]


def test_wrapper_planned_expired_snapshot_has_explicit_recovery_error(tmp_path):
    from dpone.contracts.process_errors import WindowContractError

    runtime, _, _, _, target = _runtime(tmp_path)
    target.crash = True
    with pytest.raises(OSError):
        runtime.run(_load(), owner="invocation")

    def expired(_):
        raise OSError("snapshot expired")

    runtime._source_factory = expired
    with pytest.raises(WindowContractError, match="full re-extraction requires a new invocation"):
        runtime.run(_load(), owner="invocation")
    assert target.stage_calls == 1


def test_wrapper_request_change_rejected_before_source(tmp_path):
    from dpone.contracts.process_errors import WindowContractError

    runtime, _, _, _, _ = _runtime(tmp_path)
    runtime.run(_load(), owner="invocation")
    runtime._source_factory = lambda _: pytest.fail("Changed request reopened source")
    load = _load()
    load.options["interval"]["interval_end"] = "2026-09-10T00:00:00Z"
    with pytest.raises(WindowContractError, match="request_changed"):
        runtime.run(load, owner="invocation")


@pytest.mark.parametrize("mutation", ["version", "run_id", "target", "empty_plan", "unknown_field"])
def test_wrapper_corrupt_invocation_registry_refused(tmp_path, mutation):
    from dpone.contracts.bounded_window import invocation_fingerprint
    from dpone.contracts.process_errors import WindowContractError

    runtime, store, _, _, target = _runtime(tmp_path)
    runtime.run(_load(), owner="invocation")
    key = "rolling-invocation-v1/" + invocation_fingerprint("target", "invocation")
    record = store.load(key)
    payload = json.loads(record.payload)
    if mutation == "version":
        payload["version"] = True
    elif mutation == "run_id":
        payload["plan"]["run_id"] = "wrong"
    elif mutation == "target":
        payload["plan"]["target_id"] = "other"
    elif mutation == "empty_plan":
        payload["plan"] = {}
    else:
        payload["unknown"] = 1
    lease = store.acquire("target", "seed", 30)
    store.save(key, record.revision, json.dumps(payload), lease)
    store.release(lease)
    runtime._source_factory = lambda _: pytest.fail("Corrupt registry reopened source")
    with pytest.raises(WindowContractError):
        runtime.run(_load(), owner="invocation")
    assert target.exchange_calls == 1


def test_wrapper_snapshot_close_failure_preserves_primary(tmp_path):
    runtime, _, _, _, target = _runtime(tmp_path)

    @contextmanager
    def source_factory(_):
        try:
            yield _VersionedSource("snapshot-1")
        finally:
            raise OSError("close failed")

    runtime._source_factory = source_factory
    target.crash = True
    with pytest.raises(OSError, match="crash before stage") as error:
        runtime.run(_load(), owner="invocation")
    assert any("close failed" in note for note in error.value.__notes__)


def test_wrapper_crash_retries_keep_original_budget(tmp_path):
    from dpone.contracts.process_errors import WindowContractError

    runtime, _, _, _, target = _runtime(tmp_path)
    target.crash = True
    for _ in range(3):
        with pytest.raises(OSError):
            runtime.run(_load(), owner="invocation")
    with pytest.raises(WindowContractError, match="retry budget exhausted"):
        runtime.run(_load(), owner="invocation")
    assert target.stage_calls == 3


def test_wrapper_unknown_publication_recovery_never_reopens_source(tmp_path):
    from dpone.contracts.process_errors import WindowOutcomeUnknown

    runtime, _, _, _, target = _runtime(tmp_path)

    def publish(plan, generation, lease):
        target.exchange_calls += 1
        raise OSError("publication outcome unknown")

    target.publish = publish
    with pytest.raises(WindowOutcomeUnknown):
        runtime.run(_load(), owner="invocation")
    runtime._source_factory = lambda _: pytest.fail("Unknown publication reopened source")
    with pytest.raises(WindowOutcomeUnknown):
        runtime.run(_load(), owner="invocation")
    assert target.exchange_calls == 1


def test_wrapper_source_state_failure_recovers_without_snapshot(tmp_path):
    runtime, _, _, _, target = _runtime(tmp_path)
    factory = runtime._executor_factory
    calls = []

    def state(plan, lease):
        calls.append(plan.run_id)
        if len(calls) == 1:
            raise OSError("source state unavailable")

    def executor_factory(source):
        executor = factory(source)
        executor.advance_state = state
        return executor

    runtime._executor_factory = executor_factory
    with pytest.raises(OSError, match="source state unavailable"):
        runtime.run(_load(), owner="invocation")
    runtime._source_factory = lambda _: pytest.fail("State recovery reopened source")
    runtime.run(_load(), owner="invocation")
    assert len(calls) == 2 and calls[0] == calls[1] and target.exchange_calls == 1


def test_wrapper_expired_source_validation_requires_new_invocation(tmp_path):
    from dpone.contracts.process_errors import WindowContractError

    runtime, _, _, _, target = _runtime(tmp_path)
    target.crash = True
    with pytest.raises(OSError):
        runtime.run(_load(), owner="invocation")

    @contextmanager
    def expired(_):
        source = _VersionedSource("snapshot-1")

        def validate(plan):
            raise WindowContractError("immutable version unavailable")

        source.validate = validate
        yield source

    runtime._source_factory = expired
    with pytest.raises(WindowContractError, match="full re-extraction requires a new invocation"):
        runtime.run(_load(), owner="invocation")
    assert target.stage_calls == 1
