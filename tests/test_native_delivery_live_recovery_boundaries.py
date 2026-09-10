"""Independent negative boundaries: safe final rows cannot hide an unsafe transition."""

import json
from dataclasses import replace

import pytest
from tools.native_delivery_live_support.artifacts import read_artifact
from tools.native_delivery_live_support.correctness import failure_recovery
from tools.native_delivery_live_support.hermetic import HermeticRouteFactory, HermeticRouteSession, produce_fixture
from tools.native_delivery_live_support.profiles import Dataset
from tools.native_delivery_live_support.validation import validate_run


class SessionFactory(HermeticRouteFactory):
    def __init__(self, session_type):
        super().__init__()
        self.session_type = session_type

    def open(self, dataset, *, case, clock):
        session = self.session_type(dataset, case, clock)
        self.sessions.append(session)
        return session


def recovery_results(session_type, strategy="partition_replace"):
    return {
        check["id"]: check["status"]
        for check in failure_recovery(SessionFactory(session_type), Dataset("narrow", 16), strategy)
    }


@pytest.mark.parametrize("strategy", ["partition_replace", "full_refresh"])
@pytest.mark.parametrize(
    "fault,mutation,expected",
    [
        ("unknown_commit", "partial_rows", "receipt_first_recovery"),
        ("unknown_commit", "complete", "receipt_first_recovery"),
        ("unknown_commit", "publications", "receipt_first_recovery"),
        ("before_commit", "complete", "rollback"),
        ("after_eof", "complete", "source_free_resume"),
        ("after_eof", "outside", "source_free_resume"),
        ("after_eof", "publications", "source_free_resume"),
        ("lost_ack", "partial_rows", "receipt_first_recovery"),
        ("lost_ack", "outside", "receipt_first_recovery"),
    ],
)
def test_recovery_cannot_hide_an_invalid_initial_boundary(strategy, fault, mutation, expected):
    class BrokenBoundary(HermeticRouteSession):
        def run(self):
            try:
                super().run()
            finally:
                if self.fault == fault:
                    if mutation == "partial_rows":
                        self.rows.pop()
                    elif mutation == "outside":
                        self.outside[0]["outside"] = "corrupt"
                    elif mutation == "complete":
                        self.complete = True
                    else:
                        self.publications += 1

        def recover(self, *, source_allowed):
            if self.fault == fault and fault in {"after_eof", "lost_ack"}:
                self.outside = [{"outside": "unchanged"}]
                if fault == "after_eof":
                    self.publications = 0
                else:
                    self.rows = list(self.dataset.generate())
            super().recover(source_allowed=source_allowed)

    assert recovery_results(BrokenBoundary, strategy)[expected] == "FAIL"


@pytest.mark.parametrize("phase", ["type-fidelity", "trial-"])
@pytest.mark.parametrize("counter,value", [("queries", 0), ("queries", 2), ("publications", 0), ("publications", 2)])
def test_ordinary_delivery_requires_one_source_query_and_one_publication(tmp_path, phase, counter, value):
    class InvalidCount(HermeticRouteSession):
        def run(self):
            super().run()
            if self.case.startswith(phase):
                setattr(self, counter, value)

    result = produce_fixture(tmp_path, SessionFactory(InvalidCount))
    assert result["status"] == "FAIL"
    if phase == "type-fidelity":
        assert result["fidelity_receipt"]["status"] == "FAIL" and result["samples"] == []
    else:
        assert len(result["samples"]) == 4 and all(sample["status"] == "FAIL" for sample in result["samples"])


@pytest.mark.parametrize("state,receipt_available", [("old", False), ("new", False), ("new", True)])
def test_unknown_outcome_accepts_atomic_state_without_claiming_commit_authority(state, receipt_available):
    class AtomicUnknown(HermeticRouteSession):
        def run(self):
            if self.fault == "unknown_commit" and state == "old":
                self.clock.source_acquired()
                self.queries += 1
                self.events += (self.fault,)
                self.known = False
                raise RuntimeError("deliberately unavailable outcome")
            super().run()

        def snapshot(self):
            snapshot = super().snapshot()
            if self.fault == "unknown_commit" and not receipt_available:
                snapshot = replace(snapshot, receipt_observed=None)
            return snapshot

    assert recovery_results(AtomicUnknown)["receipt_first_recovery"] == "PASS"


def test_known_commit_may_finish_pipeline_during_receipt_first_recovery():
    class PendingPipeline(HermeticRouteSession):
        def run(self):
            super().run()
            if self.fault == "lost_ack":
                self.complete = False

        def recover(self, *, source_allowed):
            super().recover(source_allowed=source_allowed)
            if self.fault == "lost_ack":
                self.complete = True

    assert recovery_results(PendingPipeline)["receipt_first_recovery"] == "PASS"


def test_recovery_observations_accept_single_use_row_iterators():
    class IteratorRows(HermeticRouteSession):
        def snapshot(self):
            return replace(super().snapshot(), rows=iter(self.rows), outside_rows=iter(self.outside))

    assert set(recovery_results(IteratorRows).values()) == {"PASS"}


@pytest.mark.parametrize("field", ["metadata_observed", "receipt_observed"])
@pytest.mark.parametrize(
    "fault,expected",
    [
        ("before_commit", "rollback"),
        ("after_eof", "source_free_resume"),
        ("lost_ack", "receipt_first_recovery"),
        ("unknown_commit", "receipt_first_recovery"),
    ],
)
def test_recovery_cannot_hide_initial_metadata_or_receipt_damage(field, fault, expected):
    class BrokenBinding(HermeticRouteSession):
        healed = False

        def snapshot(self):
            snapshot = super().snapshot()
            if self.fault == fault and self.queries and not self.healed:
                snapshot = replace(snapshot, **{field: "f" * 64})
            return snapshot

        def recover(self, *, source_allowed):
            if self.fault != "unknown_commit":
                self.healed = True
            super().recover(source_allowed=source_allowed)

    assert recovery_results(BrokenBinding)[expected] == "FAIL"


def test_unknown_outcome_without_metadata_authority_is_unverified():
    class MissingMetadata(HermeticRouteSession):
        def snapshot(self):
            snapshot = super().snapshot()
            return replace(snapshot, metadata_observed=None) if self.case == "unknown_commit" else snapshot

    assert recovery_results(MissingMetadata)["receipt_first_recovery"] == "UNVERIFIED"


@pytest.mark.parametrize("field,value", [("queries", 1), ("publications", 1), ("complete", True)])
def test_fresh_invocation_cannot_reset_existing_progress(tmp_path, field, value):
    class ExistingProgress(HermeticRouteSession):
        def __init__(self, *args):
            super().__init__(*args)
            setattr(self, field, value)

        def run(self):
            self.queries, self.publications, self.complete = 0, 0, False
            super().run()

    result = produce_fixture(tmp_path, SessionFactory(ExistingProgress))
    assert result["fidelity_receipt"]["status"] != "PASS" and result["samples"] == []


@pytest.mark.parametrize("strategy", ["partition_replace", "full_refresh"])
def test_unknown_old_target_cannot_gain_an_operation_receipt(strategy):
    class PhantomReceipt(HermeticRouteSession):
        def snapshot(self):
            value = super().snapshot()
            if self.case == "unknown_commit":
                value = replace(value, receipt_observed="b" * 64 if self.queries else None)
            return value

        def run(self):
            if self.fault == "unknown_commit":
                self.clock.source_acquired()
                self.queries += 1
                self.events += (self.fault,)
                self.known = False
                raise RuntimeError("unknown outcome with receipt-only publication")
            super().run()

    assert recovery_results(PhantomReceipt, strategy)["receipt_first_recovery"] == "FAIL"


@pytest.mark.parametrize(
    "fault,check_id",
    [
        ("before_commit", "rollback"),
        ("after_eof", "source_free_resume"),
        ("lost_ack", "receipt_first_recovery"),
        ("unknown_commit", "receipt_first_recovery"),
    ],
)
def test_retained_failure_preserves_metadata_mismatch(tmp_path, fault, check_id):
    class BadMetadata(HermeticRouteSession):
        def snapshot(self):
            value = super().snapshot()
            return replace(value, metadata_observed="f" * 64) if self.fault == fault and self.queries else value

    result = produce_fixture(tmp_path, SessionFactory(BadMetadata))
    proof = read_artifact(tmp_path, result["recovery_receipt"])
    failed = next(item for item in proof["checks"] if item["id"] == check_id)
    assert failed["status"] == "FAIL" and failed["expected"] != failed["observed"]
    assert "a" * 64 in json.dumps(failed["expected"]) and "f" * 64 in json.dumps(failed["observed"])
    raw = read_artifact(tmp_path, failed["evidence"])
    assert {**failed, "evidence": None} in raw["checks"]
    assert validate_run(result, tmp_path) == []
