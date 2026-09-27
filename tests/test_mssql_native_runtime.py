"""Publication/evidence/state order and source-free retry under real lease storage."""

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_native_chunks_journal_v2 import NativeChunkJournalV2
from dpone.adapters.mssql_native_custody import NativeTargetCustody
from dpone.contracts.mssql_native_chunks import NativeChunkPlan
from dpone.contracts.mssql_native_verification import NativeVerificationBackend, NativeVerificationIdentityV2
from dpone.runtime.governance.ports import StagedLoadHandle
from dpone.runtime.mssql_native_runtime import NativeMssqlRuntime, NativeRuntimeBindings
from dpone.runtime.sinks.load_result import LoadResult
from tests.test_mssql_native_policy import config


def runtime(tmp_path, *, recovered=False, evidence_fails=False, quality_fails=False, observer=None):
    events = []

    class Journal(NativeChunkJournalV2):
        # This interaction fake keeps publication callbacks observable while
        # satisfying the runtime's concrete v2 journal admission check.
        identity = None

        def __init__(self):
            pass

        @property
        def publication(self):
            return self

        phase = "published"

        def state(self):
            return {"phase": self.phase}

        def evidence_complete(self):
            self.phase = "evidence-complete"
            events.append("evidence-marker")

        def succeeded(self):
            self.phase = "succeeded"
            events.append("state-marker")

        def completed(self):
            return SimpleNamespace(rows=2)

    journal = Journal()
    handle = StagedLoadHandle(None, (), 2)
    result = LoadResult(inserted_rows=2, updated_rows=0, total_rows=2, staging_rows=2, commit_receipt_id="synthetic")

    class Service:
        def resume(self, *args):
            events.append("resume")
            return result if recovered else None

        def stage(self, *args):
            events.append("stage")
            return handle

        def finalize(self, *args):
            events.append("publish")
            return result

        def cleanup(self, *args):
            events.append("cleanup")

        def cleanup_recovered(self, *args):
            events.append("cleanup-recovered")

        def abort(self, *args):
            events.append("abort")

    service = Service()

    def bindings(cfg, owner, lease, cancelled):
        context = SimpleNamespace(
            plan=SimpleNamespace(target_id="target"), lease=lease, journal_factory=lambda: journal, cancelled=cancelled
        )
        return NativeRuntimeBindings(service, context, None)

    @contextmanager
    def source(*args):
        events.append("source")
        yield object()

    def quality(*args):
        events.append("quality")
        if quality_fails:
            raise ValueError("quality")

    def evidence(*args):
        events.append("evidence")
        if evidence_fails:
            raise ValueError("evidence")

    value = NativeMssqlRuntime(
        observer=observer,
        store=SQLiteWindowStore(tmp_path / "state.sqlite", clock=lambda: 1),
        target_id="target",
        bindings=bindings,
        source=source,
        preflight=lambda _: events.append("preflight"),
        quality=quality,
        evidence=evidence,
        advance_state=lambda *args: events.append("state"),
    )
    return value, events, journal


def test_order_and_complete_result(tmp_path):
    value, events, journal = runtime(tmp_path)
    assert value.run(config(), owner="invocation").status == "success"
    assert events == [
        "preflight",
        "resume",
        "source",
        "stage",
        "quality",
        "publish",
        "evidence",
        "evidence-marker",
        "state",
        "state-marker",
        "cleanup",
    ]


def test_published_recovery_never_opens_source_or_runs_quality(tmp_path):
    value, events, _ = runtime(tmp_path, recovered=True)
    value.run(config(), owner="invocation")
    assert "source" not in events and "quality" not in events and "publish" not in events
    assert events[-1] == "cleanup-recovered"


def test_evidence_failure_does_not_advance_or_abort_publication(tmp_path):
    value, events, journal = runtime(tmp_path, evidence_fails=True)
    with pytest.raises(ValueError, match="evidence"):
        value.run(config(), owner="invocation")
    assert "publish" in events and "state" not in events and "abort" not in events
    assert journal.phase == "published"


def test_quality_failure_aborts_before_target_mutation(tmp_path):
    value, events, _ = runtime(tmp_path, quality_fails=True)
    with pytest.raises(ValueError, match="quality"):
        value.run(config(), owner="invocation")
    assert "abort" in events and "publish" not in events


@pytest.mark.parametrize("recovered", [False, True])
def test_optional_observer_records_actual_runtime_boundaries(tmp_path, recovered):
    from dpone.runtime.native_delivery_observations import BoundedNativeDeliveryObserver

    observer = BoundedNativeDeliveryObserver()
    value, events, _ = runtime(tmp_path, recovered=recovered, observer=observer)
    assert value.run(config(), owner="invocation").status == "success"
    report = observer.snapshot()
    phases = [item["phase"] for item in report["observations"]]
    assert phases == (["evidence", "checkpoint"] if recovered else ["quality", "publish", "evidence", "checkpoint"])
    assert report["status"] == "PASS"
    assert all(item["durations"]["delivery"]["value"] is None for item in report["recorders"])
    assert "source" not in phases


@pytest.mark.parametrize("quality_fails", [False, True])
def test_observer_failure_cannot_change_business_outcome(tmp_path, quality_fails):
    class BrokenObserver:
        def record(self, observation):
            raise RuntimeError("observer failure must not replace business failure")

    value, events, _ = runtime(tmp_path, observer=BrokenObserver(), quality_fails=quality_fails)
    if quality_fails:
        with pytest.raises(ValueError, match="^quality$"):
            value.run(config(), owner="invocation")
        assert "publish" not in events
    else:
        assert value.run(config(), owner="invocation").status == "success"
        assert "state" in events
    assert value.observations.snapshot()["status"] == "UNVERIFIED"


def test_runtime_observations_identify_the_executing_thread(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    from threading import get_ident

    from dpone.runtime.native_delivery_observations import BoundedNativeDeliveryObserver

    observer = BoundedNativeDeliveryObserver()
    value, _, _ = runtime(tmp_path, observer=observer)

    def execute():
        value.run(config(), owner="invocation")
        return get_ident()

    with ThreadPoolExecutor(max_workers=1) as pool:
        worker = pool.submit(execute).result()
    assert worker != get_ident()
    assert {item["worker_id"] for item in observer.snapshot()["observations"]} == {f"runtime:{worker}"}


def test_v1_runtime_blocks_held_v2_custody_before_resume_or_source(tmp_path):
    value, events, _ = runtime(tmp_path)
    lease = value.store.acquire("target", "prior", 60)
    NativeTargetCustody(value.store, "target").claim(lease, "a" * 64)
    value.store.release(lease)
    with pytest.raises(Exception, match="custody_held"):
        value.run(config(), owner="new")
    assert "resume" not in events and "source" not in events


def test_target_local_claims_before_resume_and_releases_after_cleanup(tmp_path):
    value, events, journal = runtime(tmp_path)
    cfg = config()
    cfg.options["native_transfer"]["execution"]["verification_backend"] = "target_local"
    plan = NativeChunkPlan("run", "target", "query", "window", "schema", "wire")
    identity = NativeVerificationIdentityV2(
        plan,
        "bcp",
        NativeVerificationBackend.TARGET_LOCAL,
        "a" * 64,
        "b" * 64,
        "c" * 64,
        "mssql-native-sha256-sum-v1",
        "d" * 64,
    )
    journal.identity = identity
    original_bindings = value.bindings

    def bindings(*args):
        bound = original_bindings(*args)
        bound.stage_context.plan = plan
        return NativeRuntimeBindings(bound.service, bound.stage_context, bound.admission, identity)

    value.bindings = bindings
    original_resume = value.bindings
    custody = NativeTargetCustody(value.store, "target")

    def asserting_bindings(*args):
        bound = original_resume(*args)
        original = bound.service.resume

        def resume(*resume_args):
            assert custody.inspect(bound.stage_context.lease).holder_invocation_key == identity.invocation_key
            events.append("custody-claimed")
            return original(*resume_args)

        bound.service.resume = resume
        return bound

    value.bindings = asserting_bindings
    assert value.run(cfg, owner="invocation").status == "success"
    assert events.index("custody-claimed") < events.index("resume") < events.index("source")
    assert custody.inspect(value.store.acquire("target", "inspect", 60)).release_reason == "published_cleanup"
