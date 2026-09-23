"""Real SQLite terminal ordering with explicitly corrupted acknowledgement values."""

from copy import deepcopy
from time import monotonic
from types import SimpleNamespace

import pytest

from dpone.contracts.mssql_sqlclient_stage_identity import stage_object_identity
from dpone.contracts.mssql_tds_coordinator import coordinator_identity_digest
from dpone.contracts.mssql_tds_coordinator_evidence import (
    TdsCoordinatorEvidenceKind,
    TdsCoordinatorEvidenceObservation,
    TdsCoordinatorEvidenceReceipt,
)
from dpone.contracts.mssql_tds_worker import TdsAttemptPhase
from dpone.services.mssql_tds_original_continuation import PreparationTransition
from tests.test_mssql_sqlclient_observe_composition import observe_parent as observe_parent
from tests.test_mssql_sqlclient_observe_composition import retained_parent
from tests.test_mssql_sqlclient_preparation_composition import acknowledged_preparation


@pytest.mark.parametrize("boundary", ["post_ack", "sequence_exit"])
@pytest.mark.parametrize(
    "subject",
    [
        "returned",
        "expected",
        "original",
        "pinned",
        "cached",
        "registration_current",
        "registration_original",
        "registration_pinned",
    ],
)
@pytest.mark.parametrize("alias", [float, bool])
def test_terminal_rejects_noncanonical_coordinator_ack(observe_parent, tmp_path, monkeypatch, boundary, subject, alias):
    parent = observe_parent
    handle = retained_parent(parent)
    transition = PreparationTransition(parent.attempt, handle)
    parent.attempt._preparation = transition
    continuation = handle.continuation
    if subject.startswith("registration_"):
        operation = coordinator_identity_digest(handle.identity)
        receipt = TdsCoordinatorEvidenceReceipt(
            operation,
            TdsCoordinatorEvidenceKind.REGISTRATION,
            f"tds-coordinator-{operation}-registration-{'f' * 64}.json",
            "f" * 64,
            1,
        )
        registration = TdsCoordinatorEvidenceObservation(operation, receipt)
        # Scripted evidence port; no artifact or live authority is claimed here.
        continuation._evidence = SimpleNamespace(observation=registration)
        transition.registration = registration
        transition.registration_snapshot = deepcopy(registration)
    execute = handle.writer.execute
    advance = parent.attempt._lifecycle.advance
    armed = False

    def corrupt(value):
        result = deepcopy(value)
        assert result.revision == 1, "the scalar alias must remain equal to its original"
        object.__setattr__(result, "revision", alias(result.revision))
        with pytest.raises(ValueError):
            result.__post_init__()
        return result

    def authority(request, *, deadline):
        result = execute(request, deadline=deadline)
        if armed:
            if subject == "returned":
                return corrupt(result)
            if subject == "expected":
                continuation._expected = corrupt(continuation._expected)
            elif subject == "original":
                transition.coordinator = corrupt(transition.coordinator)
            elif subject == "pinned":
                transition.coordinator_snapshot = corrupt(transition.coordinator_snapshot)
            elif subject == "cached":
                canonical = deepcopy(result)
                continuation._expected = deepcopy(continuation._expected)
                transition.coordinator = deepcopy(transition.coordinator)
                object.__setattr__(handle.writer.observation.snapshot, "revision", alias(result.revision))
                return canonical
            elif subject.startswith("registration_"):
                registration = deepcopy(continuation._evidence.observation)
                object.__setattr__(registration.receipt, "byte_count", alias(1))
                registration.__post_init__()  # Outer validation alone misses the corrupt receipt.
                with pytest.raises(ValueError):
                    registration.receipt.__post_init__()
                if subject == "registration_current":
                    continuation._evidence.observation = registration
                elif subject == "registration_original":
                    transition.registration = registration
                else:
                    transition.registration_snapshot = registration
        return result

    def committed(event, **kwargs):
        nonlocal armed
        result = advance(event, **kwargs)
        if boundary == "post_ack":
            armed = True
        return result

    monkeypatch.setattr(handle.writer, "execute", authority)
    monkeypatch.setattr(parent.attempt._lifecycle, "advance", committed)
    try:
        with pytest.raises(ValueError):
            with transition.sequence():
                receipt = acknowledged_preparation(transition, tmp_path)
                transition.advance(stage_object_identity(parent.request.selected_stage), receipt)
                armed = True
        assert parent.attempt._lifecycle.snapshot.state.phase is TdsAttemptPhase.PREPARED
        assert continuation._failed and transition.failed and parent.attempt._poisoned
        assert parent.attempt._preparation is transition
        assert not transition.cleaned
    finally:
        from dpone.app.mssql_sqlclient_preparation_composition import _cleanup

        try:
            _cleanup(transition, monotonic() + 2)
        finally:
            # Release only this synthetic fixture after checking retained UNKNOWN.
            parent.attempt._preparation = None
