"""Isolated local SQL Server schema transactions; no corporate catalog or DDL."""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import pytest

from dpone.contracts.publication_authority_binding import PublicationAuthorityBinding
from dpone.runtime.state.mssql_publication_admission import require_publication_catalog
from dpone.runtime.state.mssql_publication_schema import MssqlPublicationSchema
from tests.integration.clickhouse_cluster.test_mssql_publication_authority_live import connect
from tests.integration.clickhouse_cluster.test_mssql_publication_authority_live import pytestmark as pytestmark


@pytest.fixture
def empty_catalog():
    database = "dpone_schema_it_" + uuid4().hex[:16]
    admin = connect()
    admin.execute_query(f"CREATE DATABASE [{database}]")
    admin.close()
    connector = connect(database)
    binding = PublicationAuthorityBinding("mssql", "local_metadata", database, "dbo", "schema-test", "isolated")
    yield connector, binding
    connector.close()  # disposable test database retained for evidence


def operator(catalog, factory=None):
    return MssqlPublicationSchema(
        session_factory=factory or (lambda: connect(catalog[1].database).connection),
        binding=catalog[1],
        endpoint_identity="a" * 64,  # synthetic pin: this suite covers DDL, not deployment admission
    )


def test_real_schema_apply_and_exact_readback_preserve_catalog_rows(empty_catalog):
    store = operator(empty_catalog)
    plan = store.plan()
    assert store.inspect(plan).status == "ready"
    result = store.apply(plan, confirmation_digest=plan.digest)
    assert result.status == "completed", result
    assert result.reason_code == "catalog_created"
    require_publication_catalog(empty_catalog[0], binding=empty_catalog[1])
    assert store.inspect(plan).reason_code == "catalog_exact"
    assert store.apply(plan, confirmation_digest=plan.digest).reason_code == "catalog_exact"
    assert empty_catalog[0].get_records("SELECT count(*) FROM dbo.dpone_cluster_publication_authority") == [(0,)]
    assert empty_catalog[0].get_records("SELECT count(*) FROM dbo.dpone_cluster_publication_events") == [(0,)]


def test_concurrent_schema_apply_provisions_once(empty_catalog):
    stores = [operator(empty_catalog) for _ in range(5)]
    barrier = Barrier(len(stores))

    def apply(store):
        barrier.wait(timeout=20)
        return store.apply(store.plan(), confirmation_digest=store.plan().digest)

    with ThreadPoolExecutor(max_workers=len(stores)) as executor:
        outcomes = list(executor.map(apply, stores))
    assert sum(result.reason_code == "catalog_created" for result in outcomes) == 1, outcomes
    assert all(result.status in {"completed", "blocked"} for result in outcomes), outcomes
    assert stores[0].inspect(stores[0].plan()).reason_code == "catalog_exact"


class FaultSession:
    def __init__(self, database, *, after_commit=False):
        self.connection = connect(database).connection
        self.after_commit = after_commit

    @property
    def autocommit(self):
        return self.connection.autocommit

    @autocommit.setter
    def autocommit(self, value):
        self.connection.autocommit = value

    def cursor(self):
        cursor = self.connection.cursor()
        return cursor if self.after_commit else BeforeTriggerFailure(cursor)

    def commit(self):
        self.connection.commit()
        if self.after_commit:
            raise TimeoutError("synthetic schema ACK loss after actual commit")

    def rollback(self):
        self.connection.rollback()

    def close(self):
        self.connection.close()


class BeforeTriggerFailure:
    def __init__(self, cursor):
        self.cursor = cursor

    @property
    def description(self):
        return self.cursor.description

    def execute(self, sql, params):
        if sql.startswith("CREATE TRIGGER"):
            raise ConnectionError("synthetic failure after both CREATE TABLE statements")
        return self.cursor.execute(sql, params)

    def fetchall(self):
        return self.cursor.fetchall()

    def nextset(self):
        return self.cursor.nextset()

    def close(self):
        self.cursor.close()


@pytest.mark.parametrize("after_commit", [False, True])
def test_real_schema_unknown_resolves_without_repeating_ddl(empty_catalog, after_commit):
    store = operator(empty_catalog, lambda: FaultSession(empty_catalog[1].database, after_commit=after_commit))
    result = store.apply(store.plan(), confirmation_digest=store.plan().digest)
    assert result.status == "outcome_unknown", result
    reader = operator(empty_catalog)
    observed = reader.inspect(reader.plan())
    assert observed.status == ("completed" if after_commit else "ready"), observed
    if not after_commit:
        assert empty_catalog[0].get_records(
            "SELECT count(*) FROM sys.objects WHERE name IN "
            "('dpone_cluster_publication_authority','dpone_cluster_publication_events','dpone_publication_events_immutable')"
        ) == [(0,)]


def test_partial_existing_catalog_is_not_repaired_or_extended(empty_catalog):
    empty_catalog[0].execute_query("CREATE TABLE dbo.dpone_cluster_publication_authority (sentinel int)")
    empty_catalog[0].execute_query("INSERT dbo.dpone_cluster_publication_authority VALUES (7)")
    store = operator(empty_catalog)
    result = store.apply(store.plan(), confirmation_digest=store.plan().digest)
    assert result.status == "blocked"
    assert empty_catalog[0].get_records("SELECT sentinel FROM dbo.dpone_cluster_publication_authority") == [(7,)]
    assert empty_catalog[0].get_records(
        "SELECT count(*) FROM sys.tables WHERE name='dpone_cluster_publication_events'"
    ) == [(0,)]
