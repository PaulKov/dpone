"""Local SQL Server proof for the authority, not a ClickHouse route certificate.

Opt in with DPONE_RUN_MSSQL_PUBLICATION_LIVE=1 and local test credentials.
Creates one uniquely named isolated database, retained for evidence. Never
touches an existing catalog, production endpoint, ClickHouse table or principal.
"""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import UUID, uuid4

import pytest

from dpone.adapters.mssql_publication_catalog_ddl import render_publication_catalog_ddl
from dpone.contracts.clickhouse_cluster_publication import AuthorityMutationStatus as Status
from dpone.contracts.clickhouse_cluster_publication import AuthorityPhase as Phase
from dpone.contracts.clickhouse_cluster_publication import AuthorityRecord, GenerationIdentity, digest_payload
from dpone.contracts.publication_authority_binding import PublicationAuthorityBinding
from dpone.runtime.state.mssql_publication_admission import require_publication_catalog
from dpone.runtime.state.mssql_publication_authority import MssqlPublicationAuthority
from dpone.runtime.state.mssql_publication_transaction import (
    PublicationTransactionUnknown,
    execute_publication_transaction,
)

pytestmark = [
    pytest.mark.integration_live,
    pytest.mark.skipif(
        os.environ.get("DPONE_RUN_MSSQL_PUBLICATION_LIVE") != "1", reason="explicit local SQL Server opt-in required"
    ),
]


def connect(database="master"):
    from dpone.runtime.connectors.mssql import MSSQLConnector

    host = os.environ["DPONE_IT_MSSQL_HOST"]
    if host not in {"localhost", "127.0.0.1", "host.docker.internal"}:
        raise ValueError("this authority fault suite admits only local disposable SQL Server")
    return MSSQLConnector(
        host=host,
        port=int(os.environ["DPONE_IT_MSSQL_PORT"]),
        database=database,
        user=os.environ["DPONE_IT_MSSQL_USER"],
        password=os.environ["DPONE_IT_MSSQL_PASSWORD"],
        trust_server_certificate="yes",
        connect_timeout=10,
        query_timeout=30,
    )


@pytest.fixture(scope="module")
def catalog():
    database = "dpone_pub_it_" + uuid4().hex[:16]
    admin = connect()
    admin.execute_query(f"CREATE DATABASE [{database}]")
    admin.close()
    connector = connect(database)
    for batch in render_publication_catalog_ddl(database=database, schema="dbo").split("\nGO\n"):
        if batch.strip():
            connector.execute_query(batch)
    binding = PublicationAuthorityBinding("mssql", "test_metadata", database, "dbo", "local-authority-test", "isolated")
    identity = connector.get_records(
        "SELECT CONVERT(nvarchar(128),SERVERPROPERTY('ServerName')),"
        "CONVERT(varchar(36),database_guid) FROM sys.database_recovery_status WHERE database_id=DB_ID()"
    )
    assert len(identity) == 1 and all(identity[0])
    require_publication_catalog(connector, binding=binding)
    yield connector, binding, digest_payload(identity)
    connector.close()


def authority(catalog, *, factory=None, write_id_factory=uuid4):
    connector, binding, endpoint = catalog
    return MssqlPublicationAuthority(
        catalog_connector=connector,
        binding=binding,
        endpoint_identity=endpoint,
        session_factory=factory or (lambda: connect(binding.database).connection),
        write_id_factory=write_id_factory,
    )


def record():
    target = "target_" + uuid4().hex
    generation = GenerationIdentity(
        str(uuid4()), "ReplicatedMergeTree('/synthetic','r')", "a" * 64, "default", "/synthetic"
    )
    return AuthorityRecord(
        target_key=digest_payload(target),
        operation_id=uuid4().hex,
        fence_token=uuid4().hex,
        phase=Phase.PREPARED,
        dispatch_epoch=0,
        inventory_digest="b" * 64,
        plan_digest="c" * 64,
        database="synthetic",
        target=target,
        candidate=target + "_candidate",
        desired=generation,
        predecessor=None,
        staged_rows=2,
    )


def test_concurrent_create_and_dispatch_have_one_acknowledged_winner(catalog):
    # Separate adapters and DBAPI sessions model independent cooperating writers.
    stores = [authority(catalog) for _ in range(6)]
    original = record()
    barrier = Barrier(len(stores))

    def create(store):
        barrier.wait(timeout=20)
        return store.create_if_absent(original)

    with ThreadPoolExecutor(max_workers=len(stores)) as executor:
        created = list(executor.map(create, stores))
    winners = [item for item in created if item.status is Status.VERIFIED]
    assert len(winners) == 1
    assert all(item.permit is None for item in created)
    assert all(item.status in {Status.VERIFIED, Status.CONFLICT} for item in created)
    current = winners[0].observed
    desired = current.record.dispatching(token="synthetic-intent", query_digest="d" * 64)
    barrier = Barrier(len(stores))

    def dispatch(store):
        barrier.wait(timeout=20)
        return store.compare_and_swap(current, desired)

    with ThreadPoolExecutor(max_workers=len(stores)) as executor:
        results = list(executor.map(dispatch, stores))
    assert sum(item.status is Status.VERIFIED for item in results) == 1
    assert sum(item.permit is not None for item in results) == 1
    assert stores[0].read_versioned(original.target_key).version == 2
    assert stores[0].compare_and_swap(current, desired).status is Status.CONFLICT


@pytest.mark.parametrize("transition", ["native_dispatch", "fresh_prepared"])
def test_lost_commit_ack_keeps_intent_but_cannot_grant_permit(catalog, transition):
    store = authority(catalog)
    if transition == "fresh_prepared":
        from tests.test_mssql_publication_fresh_cas import preparation

        plan = retirement_plan(catalog)
        assert retirement_store(catalog).retire_if_absent(plan) == "acknowledged"
        current = store.read_versioned(plan.original.record.target_key)
        desired = preparation(current.record)
    else:
        current = store.create_if_absent(record()).observed
        desired = current.record.dispatching(token="commit-ack-loss", query_digest="d" * 64)

    class LostAck:
        def __init__(self):
            self.connection = connect(catalog[1].database).connection

        @property
        def autocommit(self):
            return self.connection.autocommit

        @autocommit.setter
        def autocommit(self, value):
            self.connection.autocommit = value

        def cursor(self):
            return self.connection.cursor()

        def commit(self):
            self.connection.commit()
            raise TimeoutError("synthetic ACK loss after real server commit")

        def rollback(self):
            self.connection.rollback()

        def close(self):
            self.connection.close()

    result = authority(catalog, factory=LostAck).compare_and_swap(current, desired)
    assert result.status is Status.OUTCOME_UNKNOWN and result.permit is None
    readback = store.read_versioned(current.record.target_key)
    assert readback.version == 2 and readback.record.phase is desired.phase
    assert store.compare_and_swap(current, desired).permit is None


def test_real_composition_uses_admitted_registry_endpoint_for_every_session(catalog):
    from dpone.contracts.runtime_connection import ResolvedBindingConnection, ResolvedConnectionDescriptor
    from dpone.runtime.credentials.config import CredentialsConfig
    from dpone.runtime.publication_authority_composition import build_publication_authority

    connector, binding, _ = catalog
    server, database, guid = connector.get_records(
        "SELECT CONVERT(nvarchar(128),SERVERPROPERTY('ServerName')),DB_NAME(),"
        "CONVERT(varchar(36),database_guid) FROM sys.database_recovery_status WHERE database_id=DB_ID()"
    )[0]
    pin = digest_payload(
        {
            "contract": "dpone.mssql-publication-endpoint.v1",
            "server": server,
            "database": database,
            "database_guid": str(UUID(guid)),
        }
    )
    resolved = ResolvedBindingConnection(
        CredentialsConfig(
            host=os.environ["DPONE_IT_MSSQL_HOST"],
            port=int(os.environ["DPONE_IT_MSSQL_PORT"]),
            database=database,
            username=os.environ["DPONE_IT_MSSQL_USER"],
            password=os.environ["DPONE_IT_MSSQL_PASSWORD"],
            trust_server_certificate="yes",
            query_timeout=30,
        ),
        {},
        ResolvedConnectionDescriptor(
            "mssql",
            {
                "database": database,
                "schema": "dbo",
                "publication_authority": {
                    "service_id": binding.service_id,
                    "environment": binding.environment,
                    "endpoint_identity_sha256": pin,
                },
            },
        ),
    )
    store = build_publication_authority(connection=resolved, binding=binding, environment=binding.environment)
    created = store.create_if_absent(record())
    assert created.status is Status.VERIFIED
    assert store.read_versioned(created.observed.record.target_key) == created.observed
    dispatching = created.observed.record.dispatching(token="real-composition", query_digest="a" * 64)
    dispatched = store.compare_and_swap(created.observed, dispatching)
    assert dispatched.status is Status.VERIFIED and dispatched.permit is not None
    assert store.read_versioned(dispatching.target_key) == dispatched.observed


def test_event_unique_write_id_rolls_back_second_slot(catalog):
    write_id = uuid4()
    store = authority(catalog, write_id_factory=lambda: write_id)
    first, second = record(), record()
    assert store.create_if_absent(first).status is Status.VERIFIED
    result = store.create_if_absent(second)
    assert result.status is Status.OUTCOME_UNKNOWN and result.permit is None
    assert store.read_versioned(second.target_key) is None


def test_emitted_row_followed_by_throw_is_rolled_back(catalog):
    connector, binding, _ = catalog
    connector.execute_query("CREATE TABLE dbo.publication_ack_probe (id int NOT NULL PRIMARY KEY)")
    with pytest.raises(PublicationTransactionUnknown):
        execute_publication_transaction(
            lambda: connect(binding.database).connection,
            "INSERT dbo.publication_ack_probe VALUES (1); SELECT 1; THROW 51079, 'synthetic rollback', 1;",
            (),
            validate=lambda rows: rows,
        )
    assert connector.get_records("SELECT count(*) FROM dbo.publication_ack_probe") == [(0,)]


@pytest.mark.parametrize(
    "verb",
    [
        "UPDATE dbo.dpone_cluster_publication_events SET origin='modified'",
        "DELETE FROM dbo.dpone_cluster_publication_events",
    ],
)
def test_immutable_event_trigger_rejects_mutation(catalog, verb):
    connector = catalog[0]
    store = authority(catalog)
    value = record()
    assert store.create_if_absent(value).status is Status.VERIFIED
    with pytest.raises(Exception):
        connector.execute_query(verb)
    assert store.read_versioned(value.target_key).record.operation_id == value.operation_id


def test_live_trigger_drift_is_rejected(catalog):
    connector, binding, _ = catalog
    connector.execute_query(
        "DISABLE TRIGGER dbo.dpone_publication_events_immutable ON dbo.dpone_cluster_publication_events"
    )
    try:
        with pytest.raises(ValueError, match="publication catalog"):
            require_publication_catalog(connector, binding=binding)
    finally:
        connector.execute_query(
            "ENABLE TRIGGER dbo.dpone_publication_events_immutable ON dbo.dpone_cluster_publication_events"
        )
    require_publication_catalog(connector, binding=binding)


def retirement_plan(catalog):
    """Synthetic CH observations: this suite proves only the real SQL boundary."""
    from dataclasses import replace

    from dpone.contracts.clickhouse_cluster_publication import VersionedAuthorityRecord
    from dpone.contracts.publication_authority_binding import publication_binding_digest
    from dpone.contracts.publication_retirement import plan_retirement
    from tests.test_publication_retirement import observation

    value = observation()
    legacy = replace(value.replicas[0].authority.record, target="retire_" + uuid4().hex, operation_id=uuid4().hex)
    legacy = replace(
        legacy,
        target_key=digest_payload(
            {
                "cluster": value.inventory.cluster,
                "database": legacy.database,
                "target": legacy.target,
            }
        ),
    )
    binding_digest = publication_binding_digest(catalog[1], endpoint_identity=catalog[2])
    value = replace(
        value,
        binding_digest=binding_digest,
        freeze=replace(value.freeze, target_key=legacy.target_key, binding_digest=binding_digest),
        replicas=tuple(
            replace(item, authority=VersionedAuthorityRecord(legacy, 1), original_payload=legacy.payload.encode())
            for item in value.replicas
        ),
    )
    return plan_retirement(value, now=220)


def retirement_store(catalog, factory=None):
    from dpone.runtime.state.mssql_publication_retirement import MssqlPublicationRetirement

    return MssqlPublicationRetirement(
        catalog_connector=catalog[0],
        binding=catalog[1],
        endpoint_identity=catalog[2],
        clock=lambda: 220,
        session_factory=factory or (lambda: connect(catalog[1].database).connection),
    )


def test_concurrent_retirement_has_one_sql_winner_and_preserved_original_bytes(catalog):
    from dpone.contracts.publication_authority_binding import publication_slot_key

    plan = retirement_plan(catalog)
    stores = [retirement_store(catalog) for _ in range(6)]
    barrier = Barrier(len(stores))

    def retire(store):
        barrier.wait(timeout=20)
        return store.retire_if_absent(plan)

    with ThreadPoolExecutor(max_workers=len(stores)) as executor:
        results = list(executor.map(retire, stores))
    assert results.count("acknowledged") == 1
    assert results.count("conflict") == 5
    assert stores[0].inspect(plan) == "exact"
    rows = catalog[0].get_records(
        "SELECT origin,provenance,phase,revision FROM dbo.dpone_cluster_publication_events WHERE slot_key=?",
        (publication_slot_key(catalog[1], plan.original.record.target_key),),
    )
    assert rows == [("legacy_retired", plan.payload.encode(), "RETIRED_UNPUBLISHED", 1)]
    assert plan.original_payload.hex() in plan.payload
    observed = authority(catalog).read_versioned(plan.original.record.target_key)
    assert observed.record.phase.value == "RETIRED_UNPUBLISHED"
    assert observed.record.operation_id == plan.original.record.operation_id
    assert observed.record.quality_evidence is None
    assert observed.version == 1
    with pytest.raises(ValueError):
        authority(catalog).compare_and_swap(observed, observed.record)


def test_retirement_lost_commit_ack_resolves_by_read_only_inspection(catalog):
    plan = retirement_plan(catalog)

    class LostAck:
        def __init__(self):
            self.connection = connect(catalog[1].database).connection

        @property
        def autocommit(self):
            return self.connection.autocommit

        @autocommit.setter
        def autocommit(self, value):
            self.connection.autocommit = value

        def cursor(self):
            return self.connection.cursor()

        def commit(self):
            self.connection.commit()
            raise TimeoutError("synthetic retirement ACK loss after commit")

        def rollback(self):
            self.connection.rollback()

        def close(self):
            self.connection.close()

    assert retirement_store(catalog, LostAck).retire_if_absent(plan) == "unknown"
    assert retirement_store(catalog).inspect(plan) == "exact"


def test_retirement_refuses_existing_native_slot(catalog):
    plan = retirement_plan(catalog)
    assert authority(catalog).create_if_absent(plan.original.record).status is Status.VERIFIED
    store = retirement_store(catalog)
    assert store.retire_if_absent(plan) == "conflict"
    assert store.inspect(plan) == "conflict"


def test_retirement_orphan_history_is_not_absence_and_cannot_be_recreated(catalog):
    from dpone.contracts.clickhouse_cluster_publication import ClusterPublicationError
    from dpone.contracts.publication_authority_binding import publication_slot_key

    plan = retirement_plan(catalog)
    store = retirement_store(catalog)
    assert store.retire_if_absent(plan) == "acknowledged"
    key = publication_slot_key(catalog[1], plan.original.record.target_key)
    # Deliberately corrupt only our disposable test database; retain the immutable
    # event. This is a fault injection, never a migration/recovery technique.
    catalog[0].execute_query("DELETE FROM dbo.dpone_cluster_publication_authority WHERE slot_key=?", (key,))
    assert store.inspect(plan) == "unknown"
    assert store.retire_if_absent(plan) == "unknown"
    with pytest.raises(ClusterPublicationError, match="READ_UNKNOWN"):
        authority(catalog).read_for_operation(plan.original.record.target_key, "fresh-operation")
    assert authority(catalog).create_if_absent(plan.original.record).status is Status.OUTCOME_UNKNOWN
    assert catalog[0].get_records(
        "SELECT count(*) FROM dbo.dpone_cluster_publication_events WHERE slot_key=?", (key,)
    ) == [(1,)]


def test_operation_read_validates_real_retirement_root_and_rejects_original_id(catalog):
    from dpone.contracts.clickhouse_cluster_publication import ClusterPublicationError

    plan = retirement_plan(catalog)
    assert retirement_store(catalog).retire_if_absent(plan) == "acknowledged"
    store = authority(catalog)
    observed = store.read_for_operation(plan.original.record.target_key, "fresh-operation")
    assert observed.version == 1
    assert observed.record.phase is Phase.RETIRED_UNPUBLISHED
    with pytest.raises(ClusterPublicationError, match="RETIRED_OPERATION"):
        store.read_for_operation(plan.original.record.target_key, plan.original.record.operation_id)
    assert retirement_store(catalog).inspect(plan) == "exact"


def test_operation_read_validates_real_native_root_without_mutating_it(catalog):
    store = authority(catalog)
    original = record()
    assert store.read_for_operation(original.target_key, original.operation_id) is None
    result = store.create_if_absent(original)
    assert result.status is Status.VERIFIED
    assert store.read_for_operation(original.target_key, original.operation_id) == result.observed
    assert store.read_versioned(original.target_key) == result.observed


def test_one_fresh_cas_winner_keeps_retired_id_banned_after_completion(catalog):
    from dataclasses import replace

    from dpone.contracts.clickhouse_cluster_publication import ClusterPublicationError
    from dpone.contracts.publication_authority_binding import publication_slot_key
    from tests.test_mssql_publication_fresh_cas import preparation

    plan = retirement_plan(catalog)
    assert retirement_store(catalog).retire_if_absent(plan) == "acknowledged"
    stores = [authority(catalog) for _ in range(6)]
    current = stores[0].read_versioned(plan.original.record.target_key)
    desired = preparation(current.record)
    barrier = Barrier(len(stores))

    def acquire(store):
        barrier.wait(timeout=20)
        return store.compare_and_swap(current, desired)

    with ThreadPoolExecutor(max_workers=len(stores)) as executor:
        results = list(executor.map(acquire, stores))
    winners = [item for item in results if item.status is Status.VERIFIED]
    assert len(winners) == 1
    assert sum(item.status is Status.CONFLICT for item in results) == 5
    assert all(item.permit is None for item in results)
    prepared = winners[0].observed
    store = stores[0]
    dispatch = store.compare_and_swap(prepared, prepared.record.dispatching(token="synthetic", query_digest="1" * 64))
    assert dispatch.status is Status.VERIFIED
    committed = store.compare_and_swap(
        dispatch.observed, replace(dispatch.observed.record, phase=Phase.COMMITTED, ddl_entry="synthetic-entry")
    )
    assert committed.status is Status.VERIFIED
    completed = store.compare_and_swap(committed.observed, replace(committed.observed.record, phase=Phase.COMPLETED))
    assert completed.status is Status.VERIFIED
    with pytest.raises(ClusterPublicationError, match="RETIRED_OPERATION"):
        store.read_for_operation(current.record.target_key, current.record.operation_id)
    blocked = store.compare_and_swap(
        completed.observed,
        replace(preparation(completed.observed.record), operation_id=current.record.operation_id),
    )
    assert blocked.status is Status.OUTCOME_UNKNOWN
    assert blocked.permit is None
    assert store.read_versioned(current.record.target_key) == completed.observed
    key = publication_slot_key(catalog[1], current.record.target_key)
    assert catalog[0].get_records(
        "SELECT origin,provenance FROM dbo.dpone_cluster_publication_events WHERE slot_key=? AND revision=1",
        (key,),
    ) == [("legacy_retired", plan.payload.encode())]
