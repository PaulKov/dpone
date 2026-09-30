"""Local SQL Server proof for the authority, not a ClickHouse route certificate.

Opt in with DPONE_RUN_MSSQL_PUBLICATION_LIVE=1 and local test credentials.
Creates one uniquely named isolated database, retained for evidence. Never
touches an existing catalog, production endpoint, ClickHouse table or principal.
"""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

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


def test_lost_commit_ack_keeps_intent_but_cannot_grant_permit(catalog):
    store = authority(catalog)
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
    assert readback.version == 2 and readback.record.phase is Phase.DISPATCHING
    assert store.compare_and_swap(current, desired).permit is None


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
