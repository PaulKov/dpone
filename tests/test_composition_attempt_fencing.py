"""Parent attempt identity and conflict policy; no mocked live certification."""

from dataclasses import replace

import pytest

from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_attempt import (
    CompositionAttemptIdentity,
    CompositionAttemptReceipt,
    require_composition_attempt_admission,
)
from tests.test_composition_activation_contract import digest, occurrence


def attempt(workload_id="a_native", try_number=1):
    active = occurrence()
    workload = next(row for row in active.request.workloads if row.workload_id == workload_id)
    return CompositionAttemptIdentity(
        active.request.request_sha256,
        workload.workload_id,
        workload.constituent_id,
        workload.pack_sha256,
        digest("verified runtime plan"),
        "scheduled__synthetic",
        "execute",
        try_number,
        -1,
        active.receipt.guard_epochs,
    )


def test_first_exact_parent_attempt_can_be_admitted():
    require_composition_attempt_admission(occurrence(), attempt(), ())


@pytest.mark.parametrize("state", ["RUNNING", "COMMIT_UNKNOWN"])
def test_native_and_ordinary_attempts_share_physical_parent_fence(state):
    running = CompositionAttemptReceipt(attempt(), state)
    with pytest.raises(CompositionAdmissionError, match="attempt_conflict"):
        require_composition_attempt_admission(occurrence(), attempt("b_ordinary"), (running,))


def test_running_replay_never_reissues_executor_authority():
    original = attempt()
    with pytest.raises(CompositionAdmissionError, match="attempt_replay"):
        require_composition_attempt_admission(occurrence(), original, (CompositionAttemptReceipt(original, "RUNNING"),))


def test_retiring_parent_and_stale_epoch_rejected_before_admission():
    with pytest.raises(CompositionAdmissionError, match="occurrence_state"):
        require_composition_attempt_admission(occurrence("RETIRING"), attempt(), ())
    value = attempt()
    stale = replace(value, guard_epochs=((value.guard_epochs[0][0], 2),))
    with pytest.raises(CompositionAdmissionError, match="attempt_guard_epochs"):
        require_composition_attempt_admission(occurrence(), stale, ())


@pytest.mark.parametrize(
    "field,value",
    [
        ("constituent_id", "standalone"),
        ("pack_sha256", digest("foreign")),
        ("activation_request_sha256", digest("old parent")),
    ],
)
def test_foreign_workload_or_parent_never_matches_fence(field, value):
    forged = replace(attempt(), **{field: value})
    with pytest.raises(CompositionAdmissionError):
        require_composition_attempt_admission(occurrence(), forged, ())


def test_success_requires_closed_gate_quiescence_and_durable_evidence():
    with pytest.raises(CompositionAdmissionError, match="terminal_evidence"):
        CompositionAttemptReceipt(attempt(), "SUCCEEDED")
    completed = CompositionAttemptReceipt(
        attempt(), "SUCCEEDED", digest("closed gates"), digest("server quiescence"), digest("durable outcome")
    )
    require_composition_attempt_admission(occurrence(), attempt(try_number=2), (completed,))


def test_closed_gate_does_not_resolve_unknown_commit():
    unknown = CompositionAttemptReceipt(
        attempt(), "COMMIT_UNKNOWN", digest("closed"), digest("quiescent"), digest("unknown outcome")
    )
    with pytest.raises(CompositionAdmissionError, match="attempt_conflict"):
        require_composition_attempt_admission(occurrence(), attempt(try_number=2), (unknown,))


def test_mutable_guard_pair_cannot_change_attempt_identity_after_admission():
    value = attempt()
    with pytest.raises(CompositionAdmissionError, match="attempt_guard_epochs"):
        replace(value, guard_epochs=([value.guard_epochs[0][0], 1],))
