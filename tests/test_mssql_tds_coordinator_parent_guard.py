"""Parent fault closes forward CREATE work while preserving actual capabilities."""

import pytest

from dpone.app.mssql_tds_coordinator_supervision import TdsCoordinatorSupervisionUnknown
from dpone.app.mssql_tds_coordinator_supervisor import run_tds_coordinator
from tests.test_mssql_tds_coordinator_supervisor import Harness


def run(h, guard):
    return run_tds_coordinator(
        h.writer,
        h.evidence,
        h.launcher,
        h.request,
        h.admission,
        h.supply,
        pool=h.pool,
        operation_deadline=100.0,
        startup_timeout=10.0,
        termination_timeout=5.0,
        clock=lambda: h.now,
        _parent_assertion=guard,
    )


def test_admitted_parent_preserves_create_outcome():
    h = Harness()
    deadlines = []
    outcome = run(h, deadlines.append)
    assert outcome.provenance is not None
    assert deadlines and all(0 < d <= 100 for d in deadlines)


@pytest.mark.parametrize(
    "boundary,forbidden",
    [
        ("evidence:create_request", "evidence:admission"),
        ("evidence:admission", "spawn"),
        ("spawn", "startup"),
        ("startup", "evidence:registration"),
        ("ack:CoordinatorCredentialIntent", "supplier"),
        ("supplier", "credentials"),
        ("credentials", "authority"),
        ("authority", "evidence:authority"),
        ("ack:CoordinatorGrantIntent", "grant"),
        ("grant", "result"),
        ("result", "evidence:result"),
        ("wait", "ack:CoordinatorResultReceived"),
    ],
)
def test_parent_invalidated_by_callback_blocks_next_effect(boundary, forbidden):
    h = Harness()
    poisoned = False

    def hook(label):
        nonlocal poisoned
        if label == boundary:
            poisoned = True

    def guard(deadline):
        if poisoned:
            raise ValueError("parent authority unavailable")

    h.hook = hook
    with pytest.raises(TdsCoordinatorSupervisionUnknown) as caught:
        run(h, guard)
    assert forbidden not in h.events
    assert caught.value.retained.pool is h.pool
    assert "evidence.close" in h.events and "writer.close" in h.events
    if "spawn" in h.events:
        assert caught.value.retained.child is not None
        assert "child.close" in h.events
    if boundary == "result":
        assert caught.value.retained.raw_result is not None


def test_initial_parent_failure_prevents_forward_effects():
    h = Harness()

    def guard(deadline):
        raise ValueError("parent unavailable")

    with pytest.raises(TdsCoordinatorSupervisionUnknown):
        run(h, guard)
    assert "spawn" not in h.events
    assert not h.saved


def test_final_parent_assertion_cannot_extend_operation_deadline():
    h = Harness()

    def guard(deadline):
        if "writer.close" in h.events:
            h.now = 101.0

    with pytest.raises(TdsCoordinatorSupervisionUnknown):
        run(h, guard)
    assert h.events.count("evidence.close") == 1
    assert h.events.count("writer.close") == 1
