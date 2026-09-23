"""Original pending association boundaries through real SQLite actors, scripted SQL/process ports."""

from copy import deepcopy
from time import monotonic

import pytest

from dpone.app.mssql_sqlclient_departure_supervision import SqlClientCreateDepartureUnknown
from tests.test_mssql_sqlclient_stage_locator_wiring import TracedHarness


def captured_policy(monkeypatch, action=None):
    from dpone.services.mssql_tds_original_continuation import CreateSettlement

    pending = []
    original = CreateSettlement.__init__

    def capture(self, attempt, outcome, retained, factory, parent, directory, deadline):
        assert attempt._departure_baseline is not None
        assert attempt._create_settlement is None
        original(self, attempt, outcome, retained, factory, parent, directory, deadline)
        pending.append((self, deepcopy(parent), deepcopy(directory), deadline))
        if action is not None:
            action(self)

    monkeypatch.setattr(CreateSettlement, "__init__", capture)
    return CreateSettlement, pending


def test_same_pending_record_activates_after_context_without_gateway_refresh(tmp_path, monkeypatch):
    cls, pending = captured_policy(monkeypatch)
    original = cls.activate_after_context
    activated = []

    def activate(self):
        assert pending[0][0] is self
        assert self.attempt._departure_baseline is None
        assert self.attempt._create_settlement is None

        def forbidden(*args, **kwargs):
            raise AssertionError("activation must retain original snapshots, not refresh gateways")

        with monkeypatch.context() as patch:
            patch.setattr(self.attempt._lifecycle, "assert_authority", forbidden)
            patch.setattr(self.attempt._directory, "execute", forbidden)
            patch.setattr(type(self.attempt._lifecycle), "snapshot", property(forbidden))
            patch.setattr(type(self.attempt._directory), "observation", property(forbidden))
            result = original(self)
        activated.append(self)
        return result

    monkeypatch.setattr(cls, "activate_after_context", activate)
    h = TracedHarness(tmp_path)
    try:
        outcome = h.run()
        association, parent, directory, deadline = pending[0]
        assert activated == [association]
        assert h.attempt._create_settlement is association
        assert association.outcome is outcome
        assert association.parent == parent and association.directory == directory
        assert association.deadline == deadline == h.now + 100.0
        assert h.attempt._create_departure_outcome is outcome
        assert h.attempt._create_departure_registered is outcome
        assert h.attempt._create_departure_snapshot == outcome
        assert h.attempt._create_departure_snapshot is not outcome
        assert not h.attempt.directory.state.slots[0].settled
    finally:
        h.cleanup()


@pytest.mark.parametrize("fault", ["exit_failure", "caught_reentry"])
def test_failed_original_context_cannot_activate_captured_record(tmp_path, monkeypatch, fault):
    _, pending = captured_policy(monkeypatch)
    h = TracedHarness(tmp_path)
    authority = h.attempt._assert_create_departure

    def assert_current(*args, **kwargs):
        if pending:
            if fault == "exit_failure":
                raise OSError("synthetic original final-context assertion failure")
            with pytest.raises(Exception):
                _ = h.attempt.directory
        return authority(*args, **kwargs)

    monkeypatch.setattr(h.attempt, "_assert_create_departure", assert_current)
    try:
        with pytest.raises(SqlClientCreateDepartureUnknown):
            h.run()
        assert len(pending) == 1
        assert h.attempt._create_settlement is None
        assert h.attempt._poisoned
        assert h.attempt._departure_baseline is None
        with pytest.raises(Exception):
            pending[0][0].activate_after_context()
        assert h.attempt._create_settlement is None
    finally:
        h.cleanup()


@pytest.mark.parametrize("field", ["outcome_alias", "factory", "retained"])
def test_mutated_pending_original_rejected_before_activation(tmp_path, monkeypatch, field):
    restore = []

    def mutate(association):
        if field == "outcome_alias":
            identity = association.outcome.helper_outcome.local_exit.identity
            restore.append((identity, identity.pid))
            object.__setattr__(identity, "pid", True)
        else:
            setattr(association, field, object())

    _, pending = captured_policy(monkeypatch, mutate)
    h = TracedHarness(tmp_path)
    try:
        with pytest.raises(SqlClientCreateDepartureUnknown):
            h.run()
        assert len(pending) == 1
        assert h.attempt._create_settlement is None
        assert h.attempt._poisoned
    finally:
        for identity, pid in restore:
            object.__setattr__(identity, "pid", pid)
        h.cleanup()


def test_activation_replay_never_registers_again_or_repeats_sqlite_ack(tmp_path, monkeypatch):
    from dpone.app.mssql_sqlclient_create_settlement import settle_sqlclient_create_departure

    _, pending = captured_policy(monkeypatch)
    h = TracedHarness(tmp_path)
    try:
        h.run()
        association = pending[0][0]
        before = list(h.trace)
        with pytest.raises(Exception):
            association.activate_after_context()
        assert h.trace == before
        assert h.attempt._create_settlement is association
        # Replay cannot substitute a new association or initiate any proof write.
        assert association.local_ack is None and association.remote_ack is None
        assert not h.attempt._poisoned
        settled = settle_sqlclient_create_departure(
            h.attempt, admitted_factory=h.factory, pool=h.pool, deadline=monotonic() + 5
        )
        assert settled.state.slots[0].settled
    finally:
        h.cleanup()
