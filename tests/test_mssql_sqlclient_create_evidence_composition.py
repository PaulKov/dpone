"""Actual CREATE saves its six acknowledged receipts before departure."""

from hashlib import sha256

import pytest

from dpone.contracts.mssql_sqlclient_create_evidence import decode_create_seal
from dpone.contracts.mssql_tds_coordinator import coordinator_key
from tests.test_mssql_sqlclient_stage_locator_wiring import TracedHarness


def test_create_evidence_actor_is_the_adapter_gateway_class():
    from dpone.adapters.mssql_sqlclient_create_evidence_gateway import SqlClientCreateEvidenceActor as AdapterActor
    from dpone.app.mssql_sqlclient_create_evidence_composition import SqlClientCreateEvidenceActor as AppActor

    assert AppActor is AdapterActor


def test_create_evidence_app_delegates_actor_core_to_adapter_gateway():
    import ast
    from pathlib import Path

    source = Path("src/dpone/app/mssql_sqlclient_create_evidence_composition.py").read_text(encoding="utf-8")
    imports = {
        node.module
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("dpone.adapters")
    }

    assert "dpone.adapters.mssql_tds_actor_core" not in imports
    assert "dpone.adapters.mssql_sqlclient_create_evidence_gateway" in imports


def test_actual_producer_seals_original_before_departure(tmp_path):
    from dpone.contracts.mssql_sqlclient_stage_locator import decode_stage_locator_record

    h = TracedHarness(tmp_path)
    try:

        def observe(label):
            if label == "helper.spawn":
                assert h.store.load(coordinator_key(h.identity) + "/create-evidence/v1") is not None

        h.hook = observe
        outcome = h.run()
        locator = decode_stage_locator_record(h.store.load(h.locator_key)).locator
        record = h.store.load(coordinator_key(h.identity) + "/create-evidence/v1")
        seal = decode_create_seal(record.payload.encode(), locator)
        assert seal.original == outcome.create_outcome.provenance.snapshot
        assert set(seal.receipts) == set(outcome.create_outcome.receipts)
        for receipt in seal.receipts:
            assert sha256((h.evidence_root / receipt.relative_name).read_bytes()).hexdigest() == receipt.payload_sha256
    finally:
        h.cleanup()


@pytest.mark.parametrize("fault", ["before_save", "lost_ack", "bad_ack"])
def test_seal_failure_retains_original_and_prohibits_departure(tmp_path, fault):
    from dataclasses import replace

    from dpone.app.mssql_sqlclient_departure_supervision import SqlClientCreateDepartureUnknown

    h = TracedHarness(tmp_path)
    original = h.store.save
    count = []

    def save(key, *args):
        if not key.endswith("/create-evidence/v1"):
            return original(key, *args)
        count.append(key)
        if fault == "before_save":
            raise OSError("synthetic uncommitted seal")
        record = original(key, *args)
        if fault == "lost_ack":
            raise OSError("synthetic lost seal acknowledgement")
        return replace(record, payload="{}")

    h.store.save = save
    try:
        with pytest.raises(SqlClientCreateDepartureUnknown) as caught:
            h.run()
        assert caught.value.retained.create_outcome is not None
        assert type(caught.value.retained.seal_gateway).__name__ == "SqlClientCreateEvidenceActor"
        assert "helper.spawn" not in h.events
        assert len(count) == 1
        with pytest.raises(SqlClientCreateDepartureUnknown):
            h.run()
        assert len(count) == 1
        if fault != "before_save":
            from time import monotonic

            from dpone.adapters.filesystem_evidence import PinnedEvidenceReadFactory
            from dpone.adapters.mssql_tds_actor_core import TdsActorPool
            from dpone.app.mssql_sqlclient_grant_inventory_composition import SqlClientGrantInventoryCollector
            from tests.test_mssql_sqlclient_source_free_inventory import consumer

            pool = TdsActorPool(capacity=1)
            sql, kwargs = consumer(h, pool)
            try:
                result = SqlClientGrantInventoryCollector(sql, **kwargs).collect_authenticated(
                    reader_factory=PinnedEvidenceReadFactory(h.evidence_root)
                )
                assert len(result.creates) == 1
                assert "helper.spawn" not in h.events
            finally:
                sql.close()
                pool.close(deadline=monotonic() + 5)
    finally:
        h.cleanup()


@pytest.mark.parametrize("fault", ["save", "exit"])
def test_late_seal_ack_or_teardown_retains_original_gateway(tmp_path, fault):
    from contextlib import contextmanager
    from threading import Event, current_thread

    from dpone.app.mssql_sqlclient_departure_supervision import SqlClientCreateDepartureUnknown

    h = TracedHarness(tmp_path)
    blocked, release = Event(), Event()
    original_save = h.store.save
    original_factory = h.factory._factory
    seal_thread = None

    def block():
        blocked.set()
        release.wait(5)

    def save(key, *args):
        nonlocal seal_thread
        record = original_save(key, *args)
        if key.endswith("/create-evidence/v1"):
            seal_thread = current_thread()
            if fault == "save":
                block()
        return record

    @contextmanager
    def factory():
        with original_factory() as store:
            yield store
            if current_thread() is seal_thread and fault == "exit":
                block()

    h.store.save = save
    h.factory._factory = factory
    try:
        with pytest.raises(SqlClientCreateDepartureUnknown) as caught:
            h.run(operation_deadline=h.now + 0.1, termination_timeout=0.04)
        r = caught.value.retained
        assert blocked.is_set()
        assert r.create_outcome is not None
        assert r.seal_gateway is not None and not r.seal_gateway_closed
        assert h.pool.live_count == 1
        assert "helper.spawn" not in h.events
        deadline = r.containment_deadline
        release.set()
        caught.value.close(deadline=h.now + 1000)
        assert r.seal_gateway_closed and h.pool.live_count == 0
        assert r.containment_deadline == deadline
    finally:
        release.set()
        h.cleanup()
