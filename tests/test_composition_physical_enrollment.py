"""Protected physical catalog boundaries, with synthetic driver rows only."""

from types import SimpleNamespace
from uuid import UUID

import pytest

from dpone.adapters.composition_clickhouse_enrollment import (
    ClickHouseCompositionEnrollmentReader,
    clickhouse_physical_domain,
)
from dpone.adapters.composition_mssql_enrollment import mssql_physical_domain
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.dbt_relation_writes import DbtRelationWrite
from dpone.contracts.mssql_database_authority import MssqlDatabaseAuthorityPin
from dpone.contracts.runtime_connection import ResolvedBindingConnection, ResolvedConnectionDescriptor

SERVICE = "10000000-0000-4000-8000-000000000001"
DATABASE = "20000000-0000-4000-8000-000000000002"
WRITE = DbtRelationWrite("native", "flow", "transfer", "transfer", "clickhouse", "ch", None, "data", "target")


def connection():
    return ResolvedBindingConnection(
        SimpleNamespace(database="data"),
        {},
        ResolvedConnectionDescriptor(
            "clickhouse",
            {
                "database": "data",
                "composition_service_id": SERVICE,
                "database_authorities": {"data": {"database_uuid": DATABASE}},
            },
        ),
    )


class Catalog:
    def __init__(self):
        self.identity = ((SERVICE, "data", DATABASE, "Atomic"),)
        self.tables = (
            (
                "data",
                "target",
                DATABASE,
                "MergeTree",
                "CREATE TABLE data.target (id Int64) ENGINE=MergeTree ORDER BY id",
                (),
                (),
            ),
        )
        self.calls = []
        self.enrolled = []

    def require(self, domain, context):
        self.enrolled.append(domain)

    def query(self, connection, statement):
        self.calls.append(statement)
        if "serverUUID()" in statement:
            return self.identity
        if "FROM system.tables" in statement:
            return self.tables
        return ((0,),)

    def reader(self):
        return ClickHouseCompositionEnrollmentReader(query=self.query, require_enrollment=self.require)


def test_clickhouse_identity_observed_and_full_catalog_bound():
    catalog = Catalog()
    reader = catalog.reader()
    domain = reader.resolve(connection(), WRITE, None)
    assert domain == clickhouse_physical_domain(SERVICE, DATABASE)
    observation = reader.observe(domain, (WRITE,), connection(), None)
    assert observation.domain == domain
    assert len(observation.slots) == 1
    assert catalog.enrolled
    assert sum("FROM system.tables" in sql for sql in catalog.calls) == 1


@pytest.mark.parametrize(
    "identity",
    [
        (),
        ((SERVICE, "data", DATABASE, "Ordinary"),),
        (("30000000-0000-4000-8000-000000000003", "data", DATABASE, "Atomic"),),
        ((SERVICE, "data", "30000000-0000-4000-8000-000000000003", "Atomic"),),
    ],
)
def test_clickhouse_identity_mismatch_rejects(identity):
    catalog = Catalog()
    catalog.identity = identity
    with pytest.raises(CompositionAdmissionError):
        catalog.reader().resolve(connection(), WRITE, None)


@pytest.mark.parametrize("change", ["engine", "dependencies", "ttl", "foreign_view", "overflow"])
def test_clickhouse_side_effects_or_partial_budget_reject(change):
    catalog = Catalog()
    row = list(catalog.tables[0])
    if change == "engine":
        row[3] = "ReplicatedMergeTree"
    elif change == "dependencies":
        row[5] = ("foreign",)
    elif change == "ttl":
        row[4] += " TTL id + INTERVAL 1 DAY"
    elif change == "foreign_view":
        row[:4] = ["foreign", "dependent", DATABASE, "MaterializedView"]
    catalog.tables = (tuple(row),) if change != "overflow" else (tuple(row),) * 8193
    with pytest.raises(CompositionAdmissionError):
        catalog.reader().observe(clickhouse_physical_domain(SERVICE, DATABASE), (WRITE,), connection(), None)


def test_missing_protected_enrollment_never_queries_catalog():
    catalog = Catalog()

    def reject(domain, context):
        raise CompositionAdmissionError("domain_enrollment")

    reader = ClickHouseCompositionEnrollmentReader(query=catalog.query, require_enrollment=reject)
    with pytest.raises(CompositionAdmissionError, match="domain_enrollment"):
        reader.resolve(connection(), WRITE, None)
    assert not catalog.calls


def test_physical_subjects_exclude_alias_principal_and_reject_nil_uuid():
    pin = MssqlDatabaseAuthorityPin("data", 8, "2026-01-01T00:00:00", UUID(DATABASE))
    assert mssql_physical_domain(SERVICE, pin) == mssql_physical_domain(
        SERVICE, MssqlDatabaseAuthorityPin("DATA", 8, pin.create_token, pin.database_guid)
    )
    with pytest.raises(CompositionAdmissionError):
        clickhouse_physical_domain(SERVICE, str(UUID(int=0)))


@pytest.fixture
def mssql_environment(monkeypatch):
    """Synthetic DB-API rows; existing ledger/gate policy suites own their SQL."""
    from dpone.adapters import composition_mssql_enrollment as module

    pin = MssqlDatabaseAuthorityPin("data", 8, "2026-01-01T00:00:00", UUID(DATABASE))
    domain = mssql_physical_domain(SERVICE, pin)
    bound = ResolvedBindingConnection(
        SimpleNamespace(database="data"),
        {},
        ResolvedConnectionDescriptor(
            "mssql",
            {
                "database": "data",
                "composition_service_id": SERVICE,
                "database_authorities": {
                    "data": {"database_id": 8, "database_guid": DATABASE, "create_token": pin.create_token}
                },
            },
        ),
    )
    target = DbtRelationWrite("native", "flow", "model.test", "model", "mssql", "db", "data", "managed", "target")
    events = []

    class Cursor:
        def __init__(self):
            self.statement = ""
            self.records = [
                (
                    "data",
                    8,
                    DATABASE,
                    pin.create_token,
                    "writer",
                    8,
                    DATABASE,
                    pin.create_token,
                    0,
                    0,
                    0,
                    0,
                    6,
                    b"controller",
                    b"controller",
                    "mssql",
                    SERVICE,
                    domain.physical_subject_sha256,
                )
            ]
            self.schemas = [("managed",)]
            self.cascade = (0,)

        def execute(self, statement, *values):
            self.statement = statement
            events.append(("sql", statement, values))
            return self

        def fetchone(self):
            return ("control",) if "DB_NAME" in self.statement else self.cascade

        def fetchall(self):
            return self.schemas if "SELECT TOP (8193) schema_name" in self.statement else self.records

        def close(self):
            events.append("cursor_close")

    cursor = Cursor()

    class Connection:
        autocommit = True

        def cursor(self):
            return cursor

        def rollback(self):
            events.append("rollback")

        def close(self):
            events.append("connection_close")

    class Ledger:
        def __init__(self, cursor, schema):
            self.cursor, self.schema = cursor, schema

        def begin(self, service):
            events.append(("begin", service))
            return 1

        def require_transaction(self, transaction):
            events.append(("check", transaction))

        def table(self, name):
            return "[dpone_control].[composition_" + name + "]"

    monkeypatch.setattr(module, "CompositionMssqlLedger", Ledger)
    monkeypatch.setattr(
        module, "require_gate_policy", lambda ledger, database: events.append(("gate_policy", database))
    )
    monkeypatch.setattr(
        module, "require_database_policy", lambda ledger, **kwargs: events.append(("database_policy", kwargs))
    )
    reader = module.MssqlCompositionEnrollmentReader(
        connection_factory=lambda context: Connection(),
        require_target_service=lambda connection, service, context: events.append(("target_service", service)),
    )
    return reader, bound, target, cursor, events, domain


def test_mssql_protected_enrollment_checks_physical_identity_and_cleanup(mssql_environment):
    reader, bound, target, _, events, expected = mssql_environment
    assert reader.resolve(bound, target, None) == expected
    assert events[0] == events[-4] == ("target_service", SERVICE)
    assert events[-3:] == ["rollback", "cursor_close", "connection_close"]
    assert ("begin", SERVICE) in events and ("gate_policy", "control") in events


@pytest.mark.parametrize(
    "mutation", ["missing", "wrong_actual_guid", "wrong_service", "containment", "wrong_owner", "unmanaged", "cascade"]
)
def test_mssql_enrollment_rejects_incomplete_or_unprotected_database(mssql_environment, mutation):
    reader, bound, target, cursor, events, _ = mssql_environment
    record = list(cursor.records[0])
    if mutation == "missing":
        cursor.records = []
    elif mutation == "unmanaged":
        cursor.schemas = [("other",)]
    elif mutation == "cascade":
        cursor.cascade = (1,)
    else:
        index, value = {
            "wrong_actual_guid": (6, SERVICE),
            "wrong_service": (16, DATABASE),
            "containment": (10, 1),
            "wrong_owner": (13, b"other"),
        }[mutation]
        record[index] = value
        cursor.records = [tuple(record)]
    with pytest.raises(CompositionAdmissionError):
        reader.resolve(bound, target, None)
    assert events[-3:] == ["rollback", "cursor_close", "connection_close"]


def test_mssql_endpoint_clone_cannot_enter_controller_enrollment(mssql_environment):
    reader, bound, target, _, events, _ = mssql_environment

    def reject(connection, service, context):
        raise CompositionAdmissionError("target_service_identity")

    reader._require_target_service = reject
    with pytest.raises(CompositionAdmissionError, match="target_service_identity"):
        reader.resolve(bound, target, None)
    assert not events


def test_clickhouse_duplicate_targets_share_equivalence_but_case_is_preserved():
    from dataclasses import replace

    catalog = Catalog()
    domain = clickhouse_physical_domain(SERVICE, DATABASE)
    alias = replace(WRITE, resource_id="other", connection_ref="other_alias")
    same = catalog.reader().observe(domain, (WRITE, alias), connection(), None)
    distinct = catalog.reader().observe(domain, (WRITE, replace(alias, relation="TARGET")), connection(), None)
    assert same.slots[0][1] == same.slots[1][1]
    assert distinct.slots[0][1] != distinct.slots[1][1]


@pytest.mark.parametrize("fault", ["null_dependencies", "false_counter", "changed_after_catalog"])
def test_clickhouse_incomplete_or_changed_observation_rejects(fault):
    catalog = Catalog()
    original = catalog.query

    def query(bound, statement):
        if fault == "false_counter" and "SELECT count()" in statement:
            return ((False,),)
        if fault == "null_dependencies" and "FROM system.tables" in statement:
            return ((*catalog.tables[0][:5], None, None),)
        result = original(bound, statement)
        if fault == "changed_after_catalog" and "FROM system.tables" in statement:
            catalog.identity = ((SERVICE, "data", SERVICE, "Atomic"),)
        return result

    reader = ClickHouseCompositionEnrollmentReader(query=query, require_enrollment=catalog.require)
    with pytest.raises(CompositionAdmissionError):
        reader.observe(clickhouse_physical_domain(SERVICE, DATABASE), (WRITE,), connection(), None)


def test_mssql_cleanup_uncertainty_rejects_and_closes_remaining_resources(mssql_environment):
    reader, bound, target, cursor, events, _ = mssql_environment

    def uncertain():
        events.append("cursor_close_failed")
        raise OSError("SENSITIVE_SENTINEL")

    cursor.close = uncertain
    with pytest.raises(CompositionAdmissionError, match="mssql_enrollment_cleanup") as failure:
        reader.resolve(bound, target, None)
    assert events[-1] == "connection_close"
    assert "SENSITIVE_SENTINEL" not in str(failure.value)
