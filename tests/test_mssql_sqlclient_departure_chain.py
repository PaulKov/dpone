"""Pure reconstruction against actual SQLite/evidence actors; SQL/process ports are scripted."""

from copy import deepcopy
from dataclasses import replace
from uuid import uuid4

import pytest

from dpone.contracts.mssql_sqlclient_departure_evidence_types import SqlClientDepartureEvidenceKind as Kind
from tests.test_mssql_sqlclient_create_departure_composition import Harness as V1Harness
from tests.test_mssql_sqlclient_stage_locator_wiring import TracedHarness as V2Harness


@pytest.fixture(params=[V1Harness, V2Harness], ids=["v1", "v2"])
def completed_departure(request, tmp_path):
    h = request.param(tmp_path)
    try:
        outcome = h.run().helper_outcome
        yield h, outcome
    finally:
        h.cleanup()


def reconstruct(h, outcome):
    from dpone.contracts.mssql_sqlclient_departure_chain import reconstruct_departure_chain

    return reconstruct_departure_chain(
        outcome.plan, outcome.request, h.helper_startup, outcome.result, outcome.local_exit
    )


def test_reconstruction_matches_all_six_original_acked_file_bytes(completed_departure):
    h, outcome = completed_departure
    before = list(h.events)
    records = reconstruct(h, outcome)
    assert type(records) is tuple
    assert [r.kind for r in records] == list(Kind)
    assert tuple(r.receipt for r in records) == outcome.receipts
    for record in records:
        paths = list(h.evidence_root.rglob(record.receipt.relative_name))
        assert len(paths) == 1
        assert paths[0].read_bytes() == record.payload
        assert (record.result_context is outcome.request) is (record.kind is Kind.RESULT)
    assert h.events == before
    assert reconstruct(h, outcome) == records
    assert h.events == before


def test_equal_request_copy_reconstructs_bytes_but_cannot_be_ack_authority(completed_departure):
    h, outcome = completed_departure
    copied = deepcopy(outcome)
    records = reconstruct(h, copied)
    assert tuple(r.receipt for r in records) == outcome.receipts
    assert all(r.receipt is not original for r, original in zip(records, outcome.receipts, strict=True))
    # The codec returns predicted records only, not registration, settlement or grant.
    assert all(not hasattr(r, "acknowledged") and not hasattr(r, "settled") for r in records)


@pytest.mark.parametrize("fault", ["plan_subject", "request_startup", "nonzero_exit", "unreaped", "pid_alias"])
def test_mismatched_chain_inputs_reject_without_process_or_artifact_effects(completed_departure, fault):
    h, original = completed_departure
    outcome = deepcopy(original)
    if fault == "plan_subject":
        object.__setattr__(outcome, "plan", replace(outcome.plan, helper_id=uuid4()))
    elif fault == "request_startup":
        object.__setattr__(
            outcome,
            "request",
            replace(outcome.request, startup=replace(outcome.request.startup, launch_nonce=b"z" * 32)),
        )
    elif fault == "nonzero_exit":
        object.__setattr__(outcome, "local_exit", replace(outcome.local_exit, exit_code=7))
    elif fault == "unreaped":
        object.__setattr__(outcome, "local_exit", replace(outcome.local_exit, reaped=False))
    else:
        object.__setattr__(outcome.local_exit.identity, "pid", True)
    before_events = list(h.events)
    before_files = {p: p.read_bytes() for p in h.evidence_root.rglob("*.json")}
    with pytest.raises((ValueError, TypeError)):
        reconstruct(h, outcome)
    assert h.events == before_events
    assert {p: p.read_bytes() for p in h.evidence_root.rglob("*.json")} == before_files
