"""Worker aggregate orchestration doubles do not certify gate closure."""

from types import SimpleNamespace

import pytest

from dpone.app.composition_clickhouse_terminal import ClickHouseTerminalWorkerGate
from dpone.contracts.composition_identity import CompositionAdmissionError


def test_worker_closes_both_existing_purposes_before_aggregate():
    events = []

    class Gate:
        def __init__(self, name):
            self.name = name

        def issue_once(self, attempt):
            events.append("issue:" + self.name)
            return "credentials"

        def close(self, attempt):
            events.append("close:" + self.name)

        def prove_quiescence(self, attempt):
            events.append("quiet:" + self.name)

    class Store:
        def require_issued(self, attempt):
            events.append("issued")

        def persist_success(self, attempt):
            events.append("persist")
            return SimpleNamespace(closed_gates="closed")

        def read_proofs(self, attempt):
            return SimpleNamespace(quiescence="quiet", outcome="outcome")

    worker = ClickHouseTerminalWorkerGate(ingest=Gate("ingest"), publisher=Gate("publisher"), terminal=Store())
    assert worker.issue_once("attempt") == "credentials"
    assert worker.close("attempt") == "closed"
    assert worker.prove_quiescence("attempt") == "quiet"
    assert worker.observe("attempt") == "outcome"
    assert events == [
        "issue:ingest",
        "close:ingest",
        "quiet:ingest",
        "issued",
        "close:publisher",
        "quiet:publisher",
        "persist",
    ]


def test_missing_publisher_never_issues_or_fabricates_aggregate():
    events = []

    class Gate:
        def close(self, attempt):
            events.append("closed ingest")

        def prove_quiescence(self, attempt):
            events.append("quiet ingest")

    class Store:
        def require_issued(self, attempt):
            raise CompositionAdmissionError("missing publisher")

    worker = ClickHouseTerminalWorkerGate(ingest=Gate(), publisher=object(), terminal=Store())
    with pytest.raises(CompositionAdmissionError):
        worker.close("attempt")
    assert events == ["closed ingest", "quiet ingest"]


def test_only_outer_worker_closure_begins_cleanup_before_observation():
    events = []
    gate = SimpleNamespace(
        issue_once=lambda attempt: events.append("issue"),
        close=lambda attempt: events.append("close"),
        prove_quiescence=lambda attempt: events.append("quiet"),
    )

    def missing(attempt):
        raise CompositionAdmissionError("missing publisher")

    worker = ClickHouseTerminalWorkerGate(
        ingest=gate,
        publisher=gate,
        terminal=SimpleNamespace(require_issued=missing),
        begin_cleanup=lambda: events.append("cleanup"),
    )
    worker.issue_once("attempt")
    assert events == ["issue"]
    with pytest.raises(CompositionAdmissionError):
        worker.close("attempt")
    assert events == ["issue", "cleanup", "close", "quiet"]
