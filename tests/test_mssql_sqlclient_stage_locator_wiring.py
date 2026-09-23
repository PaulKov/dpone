"""V2 uses the original admitted store and actual acknowledged locator actors."""

import pytest

from dpone.app.mssql_sqlclient_departure_supervision import SqlClientCreateDepartureUnknown
from tests.test_mssql_sqlclient_create_departure_composition_v2 import Harness


def test_raw_factory_rejected_before_create_effects(tmp_path):
    h = Harness(tmp_path)
    try:
        with pytest.raises(SqlClientCreateDepartureUnknown):
            h.run(store_factory=type(h).factory.__get__(h))
        assert h.events == []
        assert list(h.evidence_root.rglob("*.json")) == []
    finally:
        h.cleanup()


class TracedHarness(Harness):
    def __init__(self, root, fault=None):
        from threading import Event

        self.trace = []
        self.contexts = 0
        self.fault = fault
        self.release = Event()
        self.waiting = Event()
        super().__init__(root)

    def prepare_factory(self, factory):
        from contextlib import contextmanager
        from dataclasses import replace
        from threading import get_ident

        original_save, original_load = self.store.save, self.store.load

        def save(key, *args):
            label = "coordinator" if "/operation/" in key else key.split("/")[0]
            self.trace.append(("save", label, get_ident()))
            if label == "sqlclient-state-domain" and self.fault == "domain_save":
                raise OSError("domain acknowledgement unavailable")
            if label == "sqlclient-stage-locator":
                self.locator_key = key
            if label == "sqlclient-stage-locator" and self.fault == "before_save":
                raise OSError("before locator persistence")
            if label == "sqlclient-stage-locator" and self.fault == "blocked_before_save":
                self.waiting.set()
                self.release.wait(5)
            result = original_save(key, *args)
            if label == "sqlclient-stage-locator":
                if self.fault == "blocked_save":
                    self.waiting.set()
                    self.release.wait(5)
                if self.fault == "after_save":
                    raise OSError("lost locator acknowledgement")
                if self.fault == "bad_ack":
                    result = replace(result, payload="{}")
                if self.fault == "deadline":
                    self.now += 200.0
            self.trace.append(("ack", label, get_ident()))
            return result

        def load(key):
            result = original_load(key)
            self.trace.append(("read", key.split("/")[0], get_ident()))
            if key == "sqlclient-state-domain/v1" and self.contexts == {
                "drift_parent": 2,
                "drift_coordinator": 4,
                "drift_locator": 5,
            }.get(self.fault):
                return None
            return result

        self.store.save, self.store.load = save, load

        @contextmanager
        def traced():
            self.contexts += 1
            index = self.contexts
            self.trace.append(("enter", index, get_ident()))
            try:
                with factory() as store:
                    yield store
            finally:
                if index == 5:
                    if self.fault == "blocked_exit":
                        self.waiting.set()
                        self.release.wait(5)
                    if self.fault == "exit_failure":
                        raise OSError("locator context exit failed")
                self.trace.append(("exit", index, get_ident()))

        self.raw_factory = traced
        return super().prepare_factory(traced)


def test_actual_sqlite_ack_and_actor_teardown_order_before_process(tmp_path, monkeypatch):
    h = TracedHarness(tmp_path)
    try:
        from dpone.app import mssql_sqlclient_create_departure_composition as composition

        original = composition.open_tds_coordinator_evidence

        def open_evidence(*args, **kwargs):
            h.trace.append(("open", "create_evidence", None))
            return original(*args, **kwargs)

        monkeypatch.setattr(composition, "open_tds_coordinator_evidence", open_evidence)
        h.hook = lambda label: h.trace.append(("process", label, None))
        h.run()
        events = [(a, b) for a, b, _ in h.trace]
        domain_ack = events.index(("ack", "sqlclient-state-domain"))
        domain_readback = events.index(("read", "sqlclient-state-domain"), domain_ack)
        parent = next(i for i, v in enumerate(events) if v[0] == "save" and v[1].startswith("mssql-tds-attempt"))
        coordinator = next(i for i, v in enumerate(events) if v[0] == "ack" and "coordinator" in v[1])
        locator = events.index(("save", "sqlclient-stage-locator"))
        ack = events.index(("ack", "sqlclient-stage-locator"))
        exit_index = events.index(("exit", 5))
        assert (
            domain_ack < domain_readback < events.index(("exit", 1)) < parent < coordinator < locator < ack < exit_index
        )
        assert exit_index < events.index(("open", "create_evidence")) < events.index(("process", "spawn"))
        assert h.pool.live_count == 2
        assert not any(a == "save" and b == "sqlclient-stage-locator" for a, b, _ in h.trace[ack + 1 :])
    finally:
        h.cleanup()


@pytest.mark.parametrize("origin", ["raw_then_admitted", "other_admitted"])
def test_late_or_equivalent_admission_cannot_adopt_attempt(tmp_path, origin):
    from dpone.app.mssql_sqlclient_stage_locator_composition import admit_sqlclient_state_domain

    class RawHarness(Harness):
        def prepare_factory(self, factory):
            return factory

    h = (RawHarness if origin == "raw_then_admitted" else Harness)(tmp_path)
    try:
        supplied = admit_sqlclient_state_domain(
            store_factory=type(h).factory.__get__(h), lease=h.lease, pool=h.pool, deadline=h.now + 100.0
        )
        if origin == "other_admitted":
            assert supplied.domain_id == h.factory.domain_id
        with pytest.raises(SqlClientCreateDepartureUnknown):
            h.run(store_factory=supplied)
        assert h.events == []
        assert list(h.evidence_root.rglob("*.json")) == []
    finally:
        h.cleanup()


@pytest.mark.parametrize("fault", ["before_save", "after_save", "bad_ack", "deadline", "exit_failure"])
def test_locator_unknown_prevents_create_and_retains_actual_gateway(tmp_path, fault):
    h = TracedHarness(tmp_path, fault)
    try:
        with pytest.raises(SqlClientCreateDepartureUnknown) as caught:
            h.run()
        r = caught.value.retained
        assert r.locator_gateway is not None
        assert type(r.locator_gateway).__name__ == "SqlClientStageLocatorActor"
        assert r.create_writer is not r.locator_gateway
        assert r.create_evidence is None
        assert h.events == []
        assert list(h.evidence_root.rglob("*.json")) == []
        saves = [v for v in h.trace if v[:2] == ("save", "sqlclient-stage-locator")]
        assert len(saves) == 1
        with pytest.raises(SqlClientCreateDepartureUnknown):
            h.run()
        assert len([v for v in h.trace if v[:2] == ("save", "sqlclient-stage-locator")]) == 1
    finally:
        if fault == "exit_failure":
            from dpone.adapters.mssql_tds_actor_core import TdsJournalActorUnknown

            with pytest.raises(TdsJournalActorUnknown):
                h.cleanup()
            assert h.pool.live_count == 0
        else:
            h.cleanup()


@pytest.mark.parametrize("fault", ["blocked_exit", "blocked_save", "blocked_before_save"])
def test_blocked_locator_keeps_actual_pool_reservation(tmp_path, fault):
    from time import monotonic

    h = TracedHarness(tmp_path, fault)
    try:
        with pytest.raises(SqlClientCreateDepartureUnknown) as caught:
            h.run(operation_deadline=h.now + 0.04, termination_timeout=0.04)
        r = caught.value.retained
        assert h.waiting.is_set()
        assert r.locator_gateway is not None and not r.locator_gateway_closed
        assert h.pool.live_count == 1
        assert h.events == []
        original_budget = r.containment_deadline
        h.release.set()
        # A released original actor can be contained; the budget is never renewed.
        caught.value.close(deadline=monotonic() + 1000.0)
        assert r.containment_deadline == original_budget
        assert r.locator_gateway_closed and h.pool.live_count == 0
    finally:
        h.release.set()
        h.cleanup()


@pytest.mark.parametrize("fault", ["domain_save", "drift_parent"])
def test_domain_failure_prevents_original_attempt_journal(tmp_path, fault):
    from dpone.adapters.mssql_tds_actor_core import TdsJournalActorUnknown
    from dpone.services.mssql_tds_attempt import TdsAttemptUnknown

    h = TracedHarness.__new__(TracedHarness)
    try:
        with pytest.raises((TdsAttemptUnknown, TdsJournalActorUnknown)):
            h.__init__(tmp_path, fault)
        assert not any(a == "save" and b.startswith("mssql-tds") for a, b, _ in h.trace)
        assert h.events == []
    finally:
        h.pool.close(deadline=h.now + 100.0)


@pytest.mark.parametrize("fault", ["drift_coordinator", "drift_locator"])
def test_later_domain_drift_prevents_that_context_effects(tmp_path, fault):
    h = TracedHarness(tmp_path, fault)
    try:
        with pytest.raises(SqlClientCreateDepartureUnknown):
            h.run()
        assert h.events == []
        assert not any(a == "save" and b == "sqlclient-stage-locator" for a, b, _ in h.trace)
        if fault == "drift_coordinator":
            assert not any(a == "save" and b == "coordinator" for a, b, _ in h.trace)
    finally:
        h.cleanup()


@pytest.mark.parametrize("mutation", ["server", "database", "nonce", "operation", "domain"])
def test_locator_captures_original_values_before_actor_effects(tmp_path, monkeypatch, mutation):
    from dpone.app import mssql_sqlclient_create_departure_composition as composition
    from dpone.contracts.mssql_sqlclient_stage_locator import decode_stage_locator_record

    h = TracedHarness(tmp_path)
    expected = {
        "server": h.departure.admission.server.server_name,
        "database": h.departure.admission.database.database_name,
        "nonce": h.request.object_nonce.int,
        "operation": h.identity.operation_id.int,
        "domain": h.factory.domain_id.int,
    }
    original = composition.create_tds_coordinator

    def mutate(*args, **kwargs):
        result = original(*args, **kwargs)
        if mutation == "server":
            object.__setattr__(h.departure.admission.server, "server_name", "changed-server")
        elif mutation == "database":
            object.__setattr__(h.departure.admission.database, "database_name", "changed-db")
        else:
            value = {
                "nonce": h.request.object_nonce,
                "operation": h.identity.operation_id,
                "domain": h.factory.domain_id,
            }[mutation]
            object.__setattr__(value, "int", 997)
        return result

    def stop_before_evidence(*args, **kwargs):
        raise OSError("stop after real locator; no synthetic process execution")

    monkeypatch.setattr(composition, "create_tds_coordinator", mutate)
    monkeypatch.setattr(composition, "open_tds_coordinator_evidence", stop_before_evidence)
    try:
        with pytest.raises(SqlClientCreateDepartureUnknown):
            h.run()
        if mutation == "operation":
            # The caller UUID also belongs to the original reservation snapshot.
            # Its mutation must invalidate that original before discovery writes.
            assert not hasattr(h, "locator_key")
            assert h.events == []
            return
        actual = decode_stage_locator_record(h.store.load(h.locator_key)).locator
        assert actual.server.server_name == expected["server"]
        assert actual.database.name == expected["database"]
        assert actual.object_nonce.int == expected["nonce"]
        assert actual.create_operation.operation_id.int == expected["operation"]
        assert actual.state_domain_id.int == expected["domain"]
        assert h.events == []
    finally:
        h.cleanup()


def test_lost_ack_remnant_cannot_authorize_replay_or_be_overwritten(tmp_path):
    from dpone.adapters.mssql_tds_actor_core import TdsJournalActorUnknown
    from dpone.app.mssql_sqlclient_stage_locator_composition import create_sqlclient_stage_locator
    from dpone.contracts.mssql_sqlclient_stage_locator import decode_stage_locator_record

    h = TracedHarness(tmp_path, "after_save")
    try:
        with pytest.raises(SqlClientCreateDepartureUnknown):
            h.run()
        before = h.store.load(h.locator_key)
        locator = decode_stage_locator_record(before).locator
        h.fault = None
        with pytest.raises(TdsJournalActorUnknown):
            create_sqlclient_stage_locator(
                admitted_factory=h.factory,
                locator=locator,
                request=h.request,
                lease=h.lease,
                pool=h.pool,
                deadline=h.now + 100.0,
            )
        assert h.store.load(h.locator_key) == before
        assert len([v for v in h.trace if v[:2] == ("save", "sqlclient-stage-locator")]) == 1
        assert h.events == []
    finally:
        h.cleanup()


def test_locator_cleanup_runs_despite_sibling_failure_and_never_renews_budget(tmp_path, monkeypatch):
    from dpone.app import mssql_sqlclient_create_departure_composition as composition
    from dpone.app.mssql_sqlclient_stage_locator_composition import SqlClientStageLocatorActor

    h = TracedHarness(tmp_path, "blocked_exit")
    original_create = composition.create_tds_coordinator
    original_close = SqlClientStageLocatorActor.close
    closes = []

    def close(actor, *, deadline):
        closes.append(deadline)
        return original_close(actor, deadline=deadline)

    def coordinator(*args, **kwargs):
        actor = original_create(*args, **kwargs)
        close_writer = actor.close

        def fail_close(*, deadline):
            close_writer(deadline=deadline)
            raise OSError("sibling cleanup unavailable")

        actor.close = fail_close
        return actor

    monkeypatch.setattr(composition, "create_tds_coordinator", coordinator)
    monkeypatch.setattr(SqlClientStageLocatorActor, "close", close)
    try:
        with pytest.raises(SqlClientCreateDepartureUnknown) as caught:
            h.run(operation_deadline=h.now + 0.04, termination_timeout=0.04)
        r = caught.value.retained
        assert len(closes) == 2 and closes[-1] == r.containment_deadline
        assert not r.locator_gateway_closed and h.pool.live_count == 1
        h.release.set()
        with pytest.raises(SqlClientCreateDepartureUnknown):
            caught.value.close(deadline=h.now + 1000.0)
        assert r.locator_gateway_closed and h.pool.live_count == 0
        count = len(closes)
        with pytest.raises(SqlClientCreateDepartureUnknown):
            caught.value.close(deadline=h.now + 2000.0)
        assert len(closes) == count
        assert all(end == r.containment_deadline for end in closes[1:])
        assert h.events == []
    finally:
        h.release.set()
        h.cleanup()


@pytest.mark.parametrize("field", ["request_nonce", "operation_id", "domain_id"])
@pytest.mark.parametrize("bad", [True, 0, -1, 2**128])
def test_mutated_uuid_scalars_reject_before_coordinator(tmp_path, field, bad):
    h = TracedHarness(tmp_path)
    try:
        value = {
            "request_nonce": h.request.object_nonce,
            "operation_id": h.identity.operation_id,
            "domain_id": h.factory.domain_id,
        }[field]
        object.__setattr__(value, "int", bad)
        with pytest.raises(SqlClientCreateDepartureUnknown):
            h.run()
        assert h.events == []
        assert not any(a == "save" and b in ("coordinator", "sqlclient-stage-locator") for a, b, _ in h.trace)
    finally:
        h.cleanup()
