"""Actual producer, six files, fresh actors and an actual inventory consumer."""

from dataclasses import replace
from time import monotonic
from types import SimpleNamespace

import pytest

from dpone.adapters.filesystem_evidence import PinnedEvidenceReadFactory
from dpone.adapters.mssql_tds_actor_core import TdsActorPool
from dpone.adapters.mssql_tds_coordinator_connection import TdsSqlConnection
from dpone.adapters.mssql_tds_coordinator_sql import TdsCoordinatorSql
from dpone.app.mssql_sqlclient_grant_inventory_composition import (
    SqlClientGrantInventoryCollector,
    SqlClientGrantInventoryUnknown,
)
from dpone.contracts.mssql_sqlclient_create_evidence import encode_authenticated_inventory
from dpone.contracts.mssql_sqlclient_grant_inventory import SqlClientGrantInventoryLimits
from dpone.contracts.mssql_sqlclient_observation import SqlClientPrincipalResolution, SqlClientSessionAuthority
from dpone.contracts.mssql_sqlclient_session_control import SqlClientDatabasePrincipal
from dpone.contracts.mssql_sqlclient_stage_identity import stage_identity_from_create
from dpone.contracts.mssql_tds_directory import TdsCoordinatorCommand
from tests.test_mssql_sqlclient_grant_catalog import Cursor, vars_server
from tests.test_mssql_sqlclient_stage_locator_wiring import TracedHarness
from tests.test_mssql_tds_session import NONCE


def consumer(h, pool):
    """Build independent SQL catalog fixture; receives no producer outcome/receipts."""
    cursor = Cursor()
    cursor.stage = stage_identity_from_create(h.proof)
    db, server = h.departure.database, h.departure.admission.server
    cursor.writer = h.departure.admission
    cursor.management = replace(cursor.writer, login=replace(cursor.management.login))
    cursor.own = replace(
        cursor.own,
        authority=SqlClientSessionAuthority(
            server,
            cursor.management.database,
            cursor.management.login,
            cursor.management.transport,
            SqlClientPrincipalResolution("sysadmin_dbo", 1, "dbo", cursor.management.database.owner_sid),
        ),
        visibility=replace(cursor.own.visibility, database_id=db.database_id),
    )
    cursor.database = [db.name, db.database_id, db.database_guid, cursor.stage.schema_id, cursor.stage.schema_name]
    cursor.session_row[13:20] = [*vars_server(server), db.name, db.database_id, db.database_guid]
    cursor.permissions = [(1, cursor.stage.object_id, 0, 5, 1, "SL", "SELECT", "G")]
    sql = TdsCoordinatorSql(
        TdsSqlConnection(SimpleNamespace(close=lambda: None), cursor),
        replace(h.identity, command=TdsCoordinatorCommand.OBSERVE),
        h.owner,
        h.startup.process,
    )
    sql.acquire(NONCE, deadline=monotonic() + 5)
    kwargs = dict(
        management_admission=cursor.management,
        writer_admission=cursor.writer,
        writer_principal=SqlClientDatabasePrincipal(5, "writer_user", "aa"),
        admitted_factory=h.factory,
        pool=pool,
        limits=SqlClientGrantInventoryLimits(),
        deadline=monotonic() + 5,
    )
    return sql, kwargs


def test_actual_producer_to_fresh_authenticated_inventory(tmp_path):
    h = TracedHarness(tmp_path)
    pool = TdsActorPool(capacity=1)
    sql = None
    try:
        h.run()
        h.cleanup()
        sql, kwargs = consumer(h, pool)
        collector = SqlClientGrantInventoryCollector(sql, **kwargs)
        before = list(h.events)
        result = collector.collect_authenticated(reader_factory=PinnedEvidenceReadFactory(h.evidence_root))
        assert len(result.creates) == 1
        assert result.creates[0].original.state.sequence == 6
        assert result.creates[0].current == result.creates[0].original
        assert result.inventory.members[0].stage == sql.cursor.stage
        assert encode_authenticated_inventory(result)
        assert h.events == before
        assert pool.live_count == 0
        assert not hasattr(result, "prepared")
        with pytest.raises(SqlClientGrantInventoryUnknown):
            collector.collect()
    finally:
        pool.close(deadline=monotonic() + 5)
        if sql is not None:
            sql.close()
        h.cleanup()


@pytest.mark.parametrize(
    "fault", ["missing_seal", "missing_file", "modified_file", "stage_changed", "current_error", "closing_drift"]
)
def test_source_free_rejects_unavailable_or_changed_history(tmp_path, fault, monkeypatch):
    from dpone.app import mssql_sqlclient_create_evidence_composition as composition
    from dpone.contracts.bounded_window import WindowRecord
    from dpone.contracts.mssql_tds_coordinator import coordinator_key
    from dpone.contracts.mssql_tds_coordinator_codec import decode_coordinator_state, encode_coordinator_state
    from dpone.contracts.mssql_tds_worker import TdsAttemptError

    h = TracedHarness(tmp_path)
    pool = TdsActorPool(capacity=1)
    sql = None
    try:
        h.run()
        h.cleanup()
        key = coordinator_key(h.identity)
        original_load = h.store.load
        state_record = original_load(key)
        state = decode_coordinator_state(state_record.payload.encode(), identity=h.identity)
        drifted = WindowRecord(state_record.revision + 1, state_record.payload)
        if fault == "current_error":
            changed = replace(state, error=next(iter(TdsAttemptError)), sequence=7)
            drifted = WindowRecord(state_record.revision + 1, encode_coordinator_state(changed).decode())
        active = fault == "current_error"

        def load(candidate):
            if fault == "missing_seal" and candidate.endswith("/create-evidence/v1"):
                return None
            if active and candidate == key:
                return drifted
            return original_load(candidate)

        h.store.load = load
        files = list(h.evidence_root.glob("tds-coordinator-*-create_request-*.json"))
        if fault == "missing_file":
            files[0].unlink()
        if fault == "modified_file":
            files[0].write_bytes(b"x" * files[0].stat().st_size)
        if fault == "closing_drift":
            acquire = composition.acquire_authenticated_create

            def drift(**kwargs):
                nonlocal active
                result = acquire(**kwargs)
                active = True
                return result

            monkeypatch.setattr(
                "dpone.app.mssql_sqlclient_grant_inventory_composition.acquire_authenticated_create", drift
            )
        sql, kwargs = consumer(h, pool)
        if fault == "stage_changed":
            sql.cursor.stage = replace(sql.cursor.stage, object_id=sql.cursor.stage.object_id + 1)
            sql.cursor.permissions = [(1, sql.cursor.stage.object_id, 0, 5, 1, "SL", "SELECT", "G")]
        collector = SqlClientGrantInventoryCollector(sql, **kwargs)
        before = list(h.trace)
        with pytest.raises(SqlClientGrantInventoryUnknown) as caught:
            collector.collect_authenticated(reader_factory=PinnedEvidenceReadFactory(h.evidence_root))
        assert not any(row[0] == "save" for row in h.trace[len(before) :])
        if fault in ("missing_file", "modified_file", "missing_seal", "current_error"):
            assert caught.value.gateway is not None
        with pytest.raises(SqlClientGrantInventoryUnknown):
            collector.collect()
    finally:
        pool.close(deadline=monotonic() + 5)
        if sql is not None:
            sql.close()
        h.cleanup()


@pytest.mark.parametrize("fault", ["read", "reader_exit", "store_exit"])
def test_blocked_actor_keeps_actual_gateway_and_pool_reservation(tmp_path, fault, monkeypatch):
    from contextlib import contextmanager
    from threading import Event

    from dpone.adapters.filesystem_evidence import PinnedEvidenceReader
    from dpone.adapters.mssql_tds_actor_core import TdsJournalActorUnknown

    h = TracedHarness(tmp_path)
    pool = TdsActorPool(capacity=1)
    release, blocked = Event(), Event()
    sql = None
    try:
        h.run()
        h.cleanup()
        sql, kwargs = consumer(h, pool)
        kwargs["deadline"] = monotonic() + 0.1

        def block():
            blocked.set()
            release.wait(5)

        if fault == "read":
            original_read = PinnedEvidenceReader.read

            def read(self, *args):
                block()
                return original_read(self, *args)

            monkeypatch.setattr(PinnedEvidenceReader, "read", read)
        elif fault == "reader_exit":
            original_factory = PinnedEvidenceReadFactory.__call__

            @contextmanager
            def factory(self):
                with original_factory(self) as reader:
                    yield reader
                    block()

            monkeypatch.setattr(PinnedEvidenceReadFactory, "__call__", factory)
        else:
            original_factory = h.factory._factory

            @contextmanager
            def store_factory():
                with original_factory() as store:
                    yield store
                    block()

            h.factory._factory = store_factory
        collector = SqlClientGrantInventoryCollector(sql, **kwargs)
        with pytest.raises(SqlClientGrantInventoryUnknown) as caught:
            collector.collect_authenticated(reader_factory=PinnedEvidenceReadFactory(h.evidence_root))
        assert blocked.is_set()
        assert caught.value.gateway is not None
        assert pool.live_count == 1
        before = len(sql.cursor.calls)
        with pytest.raises(SqlClientGrantInventoryUnknown):
            collector.collect_authenticated(reader_factory=PinnedEvidenceReadFactory(h.evidence_root))
        assert len(sql.cursor.calls) == before
        release.set()
        # External containment can close the retained actual pool, never resume collection.
        try:
            pool.close(deadline=monotonic() + 5)
        except TdsJournalActorUnknown:
            pass
        assert pool.live_count == 0
    finally:
        release.set()
        pool.close(deadline=monotonic() + 5)
        if sql is not None:
            sql.close()
        h.cleanup()


def test_exact_enclosing_authenticated_output_budget(tmp_path):
    from dpone.contracts.mssql_sqlclient_create_evidence import SqlClientAuthenticatedInventory

    h = TracedHarness(tmp_path)
    pool = TdsActorPool(capacity=1)
    sql = None
    try:
        h.run()
        h.cleanup()
        sql, kwargs = consumer(h, pool)
        result = SqlClientGrantInventoryCollector(sql, **kwargs).collect_authenticated(
            reader_factory=PinnedEvidenceReadFactory(h.evidence_root)
        )
        # The encoded limit itself changes decimal width; solve the exact boundary.
        cap = len(encode_authenticated_inventory(result))
        for _ in range(3):
            bounded = replace(
                result,
                inventory=replace(result.inventory, limits=replace(result.inventory.limits, observation_bytes=cap)),
            )
            # Size independent of admission: retain a larger same-width value temporarily.
            body = encode_authenticated_inventory(
                replace(
                    bounded,
                    inventory=replace(
                        bounded.inventory, limits=replace(bounded.inventory.limits, observation_bytes=999999)
                    ),
                )
            )
            cap = len(body) + len(str(cap)) - 6
        exact = replace(
            result, inventory=replace(result.inventory, limits=replace(result.inventory.limits, observation_bytes=cap))
        )
        assert len(encode_authenticated_inventory(exact)) == cap
        with pytest.raises(ValueError):
            encode_authenticated_inventory(
                SqlClientAuthenticatedInventory(
                    replace(exact.inventory, limits=replace(exact.inventory.limits, observation_bytes=cap - 1)),
                    exact.creates,
                )
            )
        sql.close()
        sql, kwargs = consumer(h, pool)
        kwargs["limits"] = replace(kwargs["limits"], observation_bytes=cap - 1)
        with pytest.raises(SqlClientGrantInventoryUnknown):
            SqlClientGrantInventoryCollector(sql, **kwargs).collect_authenticated(
                reader_factory=PinnedEvidenceReadFactory(h.evidence_root)
            )
    finally:
        pool.close(deadline=monotonic() + 5)
        if sql is not None:
            sql.close()
        h.cleanup()


def test_current_remote_fact_is_preserved_without_settlement_claim(tmp_path):
    from dpone.contracts.bounded_window import WindowRecord
    from dpone.contracts.mssql_tds_coordinator import (
        TdsCoordinatorRemoteKind,
        TdsCoordinatorRemoteObservation,
        coordinator_identity_digest,
        coordinator_key,
    )
    from dpone.contracts.mssql_tds_coordinator_codec import decode_coordinator_state, encode_coordinator_state

    h = TracedHarness(tmp_path)
    pool = TdsActorPool(capacity=1)
    sql = None
    try:
        h.run()
        h.cleanup()
        key = coordinator_key(h.identity)
        original_load = h.store.load
        record = original_load(key)
        state = decode_coordinator_state(record.payload.encode(), identity=h.identity)
        remote = TdsCoordinatorRemoteObservation(
            coordinator_identity_digest(h.identity),
            TdsCoordinatorRemoteKind.SETTLED,
            state.session,
            state.authority_sha256,
            "f" * 64,
        )
        current = replace(state, sequence=7, remote=remote)
        # Synthetic journal observation tests retention; it does not establish remote truth.
        h.store.load = lambda selected: (
            WindowRecord(record.revision + 1, encode_coordinator_state(current).decode())
            if selected == key
            else original_load(selected)
        )
        sql, kwargs = consumer(h, pool)
        result = SqlClientGrantInventoryCollector(sql, **kwargs).collect_authenticated(
            reader_factory=PinnedEvidenceReadFactory(h.evidence_root)
        )
        assert result.creates[0].current.state.remote == remote
        assert result.creates[0].original.state.remote is None
        assert not any(hasattr(result, name) for name in ("prepared", "settled", "safe"))
    finally:
        pool.close(deadline=monotonic() + 5)
        if sql is not None:
            sql.close()
        h.cleanup()
