"""Operator schema behavior against a fault-scripted DBAPI/catalog boundary."""

from dataclasses import replace
from importlib import import_module

import pytest

from tests.test_mssql_publication_admission import BINDING, EVENTS, SLOT, Catalog


def api():
    return import_module("dpone.runtime.state.mssql_publication_schema")


class SchemaSession(Catalog):
    def __init__(self, *, existing=False, fail=None):
        super().__init__()
        self.objects = [(SLOT, "U"), (EVENTS, "U"), ("dpone_publication_events_immutable", "TR")] if existing else []
        self.events = []
        self.fail = fail
        self.autocommit = True
        self.description = None
        self.rows = []
        self.visibility = [(BINDING.database, 1, 1)]
        self.lock_result = 0

    def cursor(self):
        self.events.append("cursor")
        return self

    def execute(self, sql, params):
        assert not self.autocommit
        self.events.append((sql, params))
        self.description = None
        self.rows = []
        if sql.startswith("SET XACT_ABORT") or sql.startswith("USE "):
            return
        if sql.startswith("CREATE "):
            if self.fail == "ddl":
                raise RuntimeError("synthetic-private-driver-detail")
            if sql.startswith("CREATE TRIGGER"):
                self.objects = [(SLOT, "U"), (EVENTS, "U"), ("dpone_publication_events_immutable", "TR")]
                if self.fail == "catalog":
                    self.column_rows = []
            return
        self.description = (("result",),)
        if "sp_getapplock" in sql:
            self.rows = [(self.lock_result,)]
        elif sql.startswith("SELECT DB_NAME()"):
            self.rows = self.visibility
        elif sql.startswith("SELECT o.name,"):
            self.rows = self.objects
        else:
            self.rows = self.get_records(sql, params)

    def fetchall(self):
        return self.rows

    def nextset(self):
        return False

    def commit(self):
        self.events.append("commit")
        if self.fail == "commit":
            raise RuntimeError("synthetic-private-driver-detail")

    def rollback(self):
        self.events.append("rollback")

    def close(self):
        self.events.append("close")

    @property
    def ddl(self):
        return [event[0] for event in self.events if isinstance(event, tuple) and event[0].startswith("CREATE ")]


def service(session):
    return api().MssqlPublicationSchema(session_factory=lambda: session, binding=BINDING, endpoint_identity="a" * 64)


def test_schema_plan_has_no_database_side_effects_and_pins_actual_ddl():
    session = SchemaSession()
    operator = service(session)
    plan = operator.plan()
    assert session.events == []
    assert plan.binding == BINDING
    assert plan.endpoint_identity == "a" * 64
    assert len(plan.ddl_sha256) == 64


@pytest.mark.parametrize("existing,expected", [(False, "ready"), (True, "completed")])
def test_inspection_is_read_only_for_absent_and_exact_catalog(existing, expected):
    session = SchemaSession(existing=existing)
    operator = service(session)
    result = operator.inspect(operator.plan())
    assert result.status == expected
    assert session.ddl == []
    assert not any("sp_getapplock" in event[0] for event in session.events if isinstance(event, tuple))


def test_apply_creates_only_three_catalog_objects_and_admits_before_commit():
    session = SchemaSession()
    operator = service(session)
    plan = operator.plan()
    result = operator.apply(plan, confirmation_digest=plan.digest)
    assert result.status == "completed"
    assert result.reason_code == "catalog_created"
    assert len(session.ddl) == 3
    assert all("\nGO\n" not in sql for sql in session.ddl)
    assert session.ddl[-1].startswith("CREATE TRIGGER [dbo].[dpone_publication_events_immutable]")
    assert len(session.calls) == 4  # exact column/index/trigger/unsupported-object admission
    assert session.events.count("commit") == 1
    assert session.events[-1] == "close"


def test_existing_exact_catalog_is_verified_without_reprovisioning():
    session = SchemaSession(existing=True)
    operator = service(session)
    result = operator.apply(operator.plan(), confirmation_digest=operator.plan().digest)
    assert result.status == "completed"
    assert result.reason_code == "catalog_exact"
    assert session.ddl == []


@pytest.mark.parametrize("change", ["digest", "database", "endpoint", "sql"])
def test_unconfirmed_or_substituted_plan_never_opens_session(change):
    session = SchemaSession()
    operator = service(session)
    original = operator.plan()
    plan = original
    if change == "database":
        plan = replace(plan, binding=replace(BINDING, database="Other_System"))
    elif change == "endpoint":
        plan = replace(plan, endpoint_identity="c" * 64)
    elif change == "sql":
        plan = replace(plan, ddl_sha256="c" * 64)
    with pytest.raises(ValueError):
        operator.apply(plan, confirmation_digest="d" * 64 if change == "digest" else plan.digest)
    assert session.events == []


@pytest.mark.parametrize(
    "issue", ["partial", "wrong_kind", "drift", "no_visibility", "wrong_database", "no_schema", "lock"]
)
def test_unsafe_catalog_is_blocked_without_any_create(issue):
    session = SchemaSession(existing=issue == "drift")
    if issue == "partial":
        session.objects = [(SLOT, "U")]
    elif issue == "wrong_kind":
        session.objects = [(SLOT, "V")]
    elif issue == "drift":
        session.column_rows = []
    elif issue == "no_visibility":
        session.visibility = [(BINDING.database, 0, 1)]
    elif issue == "wrong_database":
        session.visibility = [("Other_System", 1, 1)]
    elif issue == "no_schema":
        session.visibility = [(BINDING.database, 1, None)]
    else:
        session.lock_result = -1
    operator = service(session)
    result = operator.apply(operator.plan(), confirmation_digest=operator.plan().digest)
    assert result.status == "blocked"
    assert session.ddl == []


@pytest.mark.parametrize("failure", ["ddl", "catalog", "commit"])
def test_failure_never_returns_success_or_retries_ddl(failure):
    session = SchemaSession(fail=failure)
    operator = service(session)
    result = operator.apply(operator.plan(), confirmation_digest=operator.plan().digest)
    assert result.status == "outcome_unknown"
    assert "synthetic-private" not in repr(result)
    assert len(session.ddl) <= 3
    assert "rollback" in session.events
    assert session.events.count("commit") == (1 if failure == "commit" else 0)


def test_lost_commit_ack_is_resolved_by_explicit_read_only_inspection():
    session = SchemaSession(fail="commit")
    operator = service(session)
    plan = operator.plan()
    assert operator.apply(plan, confirmation_digest=plan.digest).status == "outcome_unknown"
    ddl_before = list(session.ddl)
    session.fail = None
    result = operator.inspect(plan)
    assert result.status == "completed"
    assert result.reason_code == "catalog_exact"
    assert session.ddl == ddl_before
