"""Actual CREATE application path with synthetic ports; not live ACK authority."""

import pytest

from dpone.app.mssql_tds_coordinator_supervisor import TdsCoordinatorCreateOutcome
from tests.test_mssql_tds_coordinator_supervisor import Harness


def test_legacy_positional_outcome_keeps_default_absent_provenance():
    outcome = Harness().run()
    legacy = TdsCoordinatorCreateOutcome(outcome.response, outcome.local_exit, outcome.receipts)
    assert legacy.provenance is None


def test_successful_application_retains_exact_nonsecret_originals():
    h = Harness()
    outcome = h.run()
    provenance = outcome.provenance
    assert provenance is not None
    assert provenance.request == h.request
    assert provenance.admission == h.admission
    assert provenance.startup == h.startup
    assert provenance.authority == h.authority
    assert provenance.grant == h.grant
    assert provenance.snapshot == h.current
    assert provenance.local_proof.exit == outcome.local_exit
    assert h.events[-2:] == ["evidence.close", "writer.close"]
    assert "privatepassword" not in repr(provenance)


def test_outcome_rejects_untyped_optional_provenance():
    outcome = Harness().run()
    with pytest.raises(ValueError):
        TdsCoordinatorCreateOutcome(outcome.response, outcome.local_exit, outcome.receipts, provenance={})


def test_provenance_validation_after_teardown_cannot_extend_deadline(monkeypatch):
    from dpone.app import mssql_tds_coordinator_supervisor as module
    from dpone.app.mssql_tds_coordinator_supervision import TdsCoordinatorSupervisionUnknown

    h = Harness()
    original = module.validate_create_provenance

    def late(*args, **kwargs):
        assert h.events[-2:] == ["evidence.close", "writer.close"]
        original(*args, **kwargs)
        h.now = 101.0

    monkeypatch.setattr(module, "validate_create_provenance", late)
    with pytest.raises(TdsCoordinatorSupervisionUnknown):
        h.run()
    assert h.events.count("child.close") == 1
    assert h.events.count("evidence.close") == 1
    assert h.events.count("writer.close") == 1


def test_final_outcome_constructor_cannot_return_after_deadline(monkeypatch):
    from dpone.app.mssql_tds_coordinator_supervision import TdsCoordinatorSupervisionUnknown
    from dpone.app.mssql_tds_create_provenance import TdsCoordinatorCreateProvenance

    h = Harness()
    original = TdsCoordinatorCreateProvenance.__post_init__
    calls = []

    def late(value):
        original(value)
        calls.append(value)
        if len(calls) == 3:
            h.now = 101.0

    monkeypatch.setattr(TdsCoordinatorCreateProvenance, "__post_init__", late)
    with pytest.raises(TdsCoordinatorSupervisionUnknown):
        h.run()
    assert len(calls) == 3
    assert h.events.count("child.close") == 1
    assert h.events.count("evidence.close") == 1
    assert h.events.count("writer.close") == 1
