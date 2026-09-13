"""Immutable external enrollment contracts; offline SQL is not live proof."""

from copy import deepcopy
from types import SimpleNamespace

import pytest

from dpone.adapters.composition_clickhouse_supervisor_enrollment import (
    ClickHouseSupervisorEnrollment,
    SupervisorEnrollmentReader,
)
from dpone.adapters.composition_clickhouse_supervisor_linux import digest
from dpone.adapters.composition_clickhouse_supervisor_schema import render_clickhouse_supervisor_schema
from dpone.adapters.composition_mssql_schema import COMPOSITION_MSSQL_SCHEMA_VERSION
from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.strict_json import canonical_json_bytes

CH, DISPATCHER, NETWORK = "a" * 64, "b" * 64, "c" * 64


def enrollment_body(facts=None):
    return {
        "schema": "dpone.composition-clickhouse-supervisor-enrollment.v1",
        "service_id": "10000000-0000-4000-8000-000000000001",
        "database_uuid": "10000000-0000-4000-8000-000000000002",
        "boot_id": "10000000-0000-4000-8000-000000000003",
        "isolation_id": "10000000-0000-4000-8000-000000000004",
        "target_enrollment_sha256": "sha256:" + "d" * 64,
        "policy": {
            "roles": {CH: "clickhouse", DISPATCHER: "dispatcher"},
            "network_id": NETWORK,
            "frontend_port": 9080,
            "config_roots": {CH: ["/etc/clickhouse-server"], DISPATCHER: ["/app"]},
        },
        "facts": facts or {"docker": {}, "linux": {}},
    }


def enrolled(body=None):
    document = canonical_json_bytes(body or enrollment_body())
    return ClickHouseSupervisorEnrollment(digest(document), document)


def test_enrollment_retains_exact_original_without_exposing_it_in_repr():
    value = enrolled()
    assert value.body == enrollment_body()
    assert "clickhouse-server" not in repr(value)
    body = value.body
    body["policy"]["roles"].clear()
    assert len(value.policy["roles"]) == 2


@pytest.mark.parametrize(
    "mutation",
    [
        lambda b: b.update(extra="unreviewed"),
        lambda b: b["policy"].update(accept_unknown=True),
        lambda b: b["policy"]["roles"].update({CH: "control"}),
        lambda b: b["policy"].update(frontend_port=8123),
        lambda b: b["policy"].update(frontend_port=True),
        lambda b: b["policy"]["config_roots"].update({CH: ["/etc/../etc/clickhouse-server"]}),
        lambda b: b["policy"]["config_roots"].update({CH: ["/app"]}),
        lambda b: b.update(service_id="00000000-0000-0000-0000-000000000000"),
    ],
)
def test_enrollment_closed_shape_rejects_unreviewed_policy(mutation):
    body = deepcopy(enrollment_body())
    mutation(body)
    with pytest.raises((CompositionAdmissionError, ValueError)):
        enrolled(body)


def test_enrollment_hash_and_canonical_bytes_are_both_required():
    value = enrolled()
    with pytest.raises(CompositionAdmissionError):
        ClickHouseSupervisorEnrollment(value.enrollment_sha256, value.document + b" ")
    changed = value.document + b" "
    with pytest.raises(CompositionAdmissionError):
        ClickHouseSupervisorEnrollment(digest(changed), changed)


def test_schema_has_immutable_hash_guard_and_no_runtime_adoption_ddl():
    sql = render_clickhouse_supervisor_schema("control")
    assert "CREATE TABLE [control].[composition_ch_supervisor_enrollments]" in sql
    assert "DPONE_CH_SUPERVISOR_IMMUTABLE" in sql and "HASHBYTES" in sql
    assert "deleted" in sql and "Exclusive" in sql
    assert "DROP " not in sql and "ALTER TABLE" not in sql


class EnrollmentCursor:
    """Supplies literal SQL results; production transaction pinning runs unchanged."""

    def __init__(self, value):
        self.value, self.transaction, self.rows = value, 71, []
        self.service = "10000000-0000-4000-8000-000000000006"
        self.on_enrollment = None
        self.statements = []

    def execute(self, sql, *args):
        self.statements.append(sql)
        if "CURRENT_TRANSACTION_ID" in sql:
            self.rows = [(1, 1, "Exclusive", self.transaction)]
        elif "composition_authority]" in sql:
            self.rows = [(1, COMPOSITION_MSSQL_SCHEMA_VERSION, self.service)]
        elif "composition_ch_supervisor_enrollments]" in sql:
            b = self.value.body
            self.rows = [
                tuple(b[key] for key in ("service_id", "database_uuid", "boot_id", "isolation_id"))
                + (self.value.document,)
            ]
            if self.on_enrollment:
                self.on_enrollment(self)
        else:
            raise AssertionError(sql)
        return self

    def fetchall(self):
        return self.rows


def enrollment_reader(monkeypatch):
    import dpone.adapters.composition_clickhouse_supervisor_enrollment as module

    value = enrolled()
    cursor = EnrollmentCursor(value)
    ledger = CompositionMssqlLedger(cursor, "control")
    ledger._expected_service_id = cursor.service
    target = SimpleNamespace(
        guard_id="g",
        service_id=value.body["service_id"],
        database_id=value.body["database_uuid"],
        enrollment_sha256=value.body["target_enrollment_sha256"],
        physical_subject_sha256="p",
        write_subject_sha256="w",
    )
    attempt = SimpleNamespace(workload_id="work", guard_epochs=(("g", 1),))
    workload = SimpleNamespace(
        workload_id="work", write_subjects=("w",), execution_cell="mssql_clickhouse_full_refresh_v1"
    )
    resource = SimpleNamespace(
        guard_id="g",
        connector="clickhouse",
        service_id=target.service_id,
        physical_subject_sha256="p",
        write_subjects=("w",),
    )
    occurrence = SimpleNamespace(
        receipt=SimpleNamespace(state="ACTIVE"), request=SimpleNamespace(workloads=(workload,), resources=(resource,))
    )
    monkeypatch.setattr(module, "require_clickhouse_supervisor_schema", lambda *args: None)
    monkeypatch.setattr(
        module, "require_existing_execution_in", lambda *args, **kwargs: (occurrence, SimpleNamespace(state="RUNNING"))
    )
    reader = SupervisorEnrollmentReader(ledger, value.enrollment_sha256, attempt, target)
    return reader, cursor, workload


def test_reader_never_opens_connection_or_mutates_supplied_sql_transaction(monkeypatch):
    reader, cursor, _ = enrollment_reader(monkeypatch)
    assert reader.read().document == cursor.value.document
    assert not any(
        word in sql for sql in cursor.statements for word in ("INSERT ", "UPDATE ", "BEGIN TRANSACTION", "COMMIT")
    )


def test_reader_rejects_transaction_switched_during_enrollment_read(monkeypatch):
    reader, cursor, _ = enrollment_reader(monkeypatch)
    cursor.on_enrollment = lambda c: setattr(c, "transaction", 72)
    with pytest.raises(CompositionAdmissionError, match="transaction_identity"):
        reader.read()


def test_reader_rejects_cursor_identity_replacement(monkeypatch):
    reader, cursor, _ = enrollment_reader(monkeypatch)
    reader.context.cursor = EnrollmentCursor(cursor.value)
    with pytest.raises(CompositionAdmissionError, match="sql_context_changed"):
        reader.check()


def test_reader_rejects_different_execution_cell(monkeypatch):
    reader, _, workload = enrollment_reader(monkeypatch)
    workload.execution_cell = "different"
    with pytest.raises(CompositionAdmissionError):
        reader.read()
