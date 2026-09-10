"""Publication/evidence/state order and source-free retry under real lease storage."""

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.runtime.governance.ports import StagedLoadHandle
from dpone.runtime.mssql_native_runtime import NativeMssqlRuntime, NativeRuntimeBindings
from dpone.runtime.sinks.load_result import LoadResult
from tests.test_mssql_native_policy import config


def runtime(tmp_path, *, recovered=False, evidence_fails=False, quality_fails=False):
    events = []

    class Journal:
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
