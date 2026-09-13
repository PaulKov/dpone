"""Public composition root wiring and fail-closed capability selection."""

import inspect
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.adapters.composition_clickhouse_enrollment import clickhouse_physical_domain
from dpone.adapters.composition_mssql_enrollment import mssql_target_pin
from dpone.app.composition_activation import (
    _compose_composition_activation_coordinator,
    build_composition_activation_coordinator,
)
from dpone.app.composition_clickhouse_admission import require_protected_clickhouse_enrollment
from dpone.app.composition_clickhouse_execution import CompositionClickHouseExecutionRoot
from dpone.app.composition_dbt_execution import CompositionDbtExecutionRoot
from dpone.app.composition_materialization_seams import build_composition_materialization_seams
from dpone.app.composition_transfer_execution import CompositionTransferExecutionRoot
from dpone.contracts.composition_activation import CompositionAdmissionError, CompositionOccurrenceContext
from dpone.contracts.dbt_relation_writes import DbtRelationWrite
from dpone.contracts.runtime_connection import ResolvedBindingConnection, ResolvedConnectionDescriptor
from dpone.runtime.credentials.config import CredentialsConfig
from dpone.services.composition_activation_coordinator import CompositionActivationCoordinator
from tests.test_composition_clickhouse_supervisor_enrollment import enrolled, enrollment_body

SERVICE = "10000000-0000-4000-8000-000000000001"
OTHER_SERVICE = "30000000-0000-4000-8000-000000000003"
DATABASE = "20000000-0000-4000-8000-000000000002"
OTHER_DATABASE = "40000000-0000-4000-8000-000000000004"
CONTEXT = CompositionOccurrenceContext(
    "10000000-0000-4000-8000-000000000003",
    "test",
    "sha256:" + "a" * 64,
    "sha256:" + "b" * 64,
    None,
    "sha256:" + "c" * 64,
)

COMPLETE_CELLS = frozenset(
    {
        "sqlserver_dbt_v1",
        "postgres_mssql_full_refresh_v1",
        "mssql_clickhouse_full_refresh_v1",
    }
)


class ResolverFactory:
    def build(self, **_kwargs):
        raise AssertionError("factory construction must not resolve credentials")


def compose_activation_coordinator(cache_root: Path, **kwargs):
    kwargs.setdefault("authority_connection_ref", "composition_control")
    kwargs.setdefault("resolver_factory", ResolverFactory())
    return _compose_composition_activation_coordinator(cache_root=cache_root, **kwargs)


def test_public_factory_constructs_coordinator_without_eager_io(tmp_path: Path) -> None:
    coordinator = build_composition_activation_coordinator(
        cache_root=tmp_path,
        authority_connection_ref="composition_control",
    )

    assert isinstance(coordinator, CompositionActivationCoordinator)


def test_public_factory_advertises_the_complete_installed_cell_set(tmp_path: Path) -> None:
    coordinator = compose_activation_coordinator(tmp_path)

    assert coordinator.execution_cells == COMPLETE_CELLS


def test_public_factory_owns_callable_factories_for_every_installed_cell(tmp_path: Path) -> None:
    coordinator = compose_activation_coordinator(tmp_path)

    factories = {
        cell: coordinator.execution_factory(cell)
        for cell in (
            "sqlserver_dbt_v1",
            "postgres_mssql_full_refresh_v1",
            "mssql_clickhouse_full_refresh_v1",
        )
    }

    assert coordinator.execution_cells == frozenset(factories)
    assert all(callable(factory) for factory in factories.values())
    assert factories["sqlserver_dbt_v1"].root_type is CompositionDbtExecutionRoot
    assert factories["postgres_mssql_full_refresh_v1"].root_type is CompositionTransferExecutionRoot
    assert factories["mssql_clickhouse_full_refresh_v1"].root_type is CompositionClickHouseExecutionRoot


def test_public_factory_supplies_materialization_seams_without_opening_sql(tmp_path: Path) -> None:
    coordinator = compose_activation_coordinator(tmp_path)

    seams = coordinator.materialization_seams()

    assert callable(seams["open_target"])
    assert callable(seams["require_target"])
    assert callable(seams["observe_undispatched_closure"])


def test_public_factory_exposes_only_the_approved_parameters() -> None:
    parameters = inspect.signature(build_composition_activation_coordinator).parameters

    assert list(parameters) == ["cache_root", "authority_connection_ref", "control_schema"]


def test_public_factory_cannot_substitute_clickhouse_enrollment(tmp_path: Path) -> None:
    with pytest.raises(TypeError):
        build_composition_activation_coordinator(
            cache_root=tmp_path,
            authority_connection_ref="composition_control",
            require_clickhouse_enrollment=lambda domain, context: None,
        )


def test_require_target_fail_closes_when_service_callback_is_missing() -> None:
    target, write, pin = mssql_materialization_target()
    seams = build_composition_materialization_seams(
        target=target,
        expected_service_id=SERVICE,
        context=CONTEXT,
    )

    with pytest.raises(CompositionAdmissionError) as failure:
        seams["require_target"](LiveConnection(), object(), write, pin)

    assert failure.value.reason in {"materialization_target", "target_service_pin"}


def test_require_target_inspects_the_live_connection() -> None:
    target, write, pin = mssql_materialization_target()
    seams = build_composition_materialization_seams(
        target=target,
        expected_service_id=SERVICE,
        context=CONTEXT,
        require_target_service=lambda *_args: None,
    )

    with pytest.raises(CompositionAdmissionError) as failure:
        seams["require_target"](None, object(), write, pin)

    assert failure.value.reason in {"materialization_target", "target_service_pin"}


def test_require_target_rejects_mismatched_target_service(tmp_path: Path) -> None:
    target, write, pin = mssql_materialization_target(service_id=OTHER_SERVICE)

    def require_service(tgt, expected, _context) -> None:
        if tgt.descriptor.properties.get("composition_service_id") != expected:
            raise CompositionAdmissionError("target_service_pin")

    coordinator = compose_activation_coordinator(tmp_path, require_mssql_target_service=require_service)
    seams = coordinator.materialization_seams(
        target=target,
        expected_service_id=SERVICE,
        context=CONTEXT,
    )

    with pytest.raises(CompositionAdmissionError) as failure:
        seams["require_target"](LiveConnection(), object(), write, pin)

    assert failure.value.reason in {"materialization_target", "target_service_pin"}


def test_same_service_other_database_enrollment_is_clickhouse_enrollment(monkeypatch: pytest.MonkeyPatch) -> None:
    import dpone.app.composition_clickhouse_admission as admission
    from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger

    domain = clickhouse_physical_domain(SERVICE, DATABASE)
    body = enrollment_body()
    body["database_uuid"] = OTHER_DATABASE
    value = enrolled(body)
    cursor = EnrollmentCursor(domain, value)
    control = SimpleNamespace(
        autocommit=True,
        cursor=lambda: cursor,
        rollback=lambda: None,
        close=lambda: None,
    )
    monkeypatch.setattr(admission, "require_clickhouse_supervisor_schema", lambda *_args: None)
    monkeypatch.setattr(CompositionMssqlLedger, "begin", lambda self, service_id: 71)
    monkeypatch.setattr(CompositionMssqlLedger, "require_transaction", lambda self, transaction=None: 71)

    with pytest.raises(CompositionAdmissionError) as failure:
        require_protected_clickhouse_enrollment(
            domain,
            CONTEXT,
            connections=SimpleNamespace(control_connection_with_service=lambda _context: (control, OTHER_SERVICE)),
            control_schema="dpone_control",
        )

    assert failure.value.reason == "clickhouse_enrollment"


class LiveConnection:
    def cursor(self) -> None:
        raise AssertionError("require_target must inspect the live handle, not open catalog SQL")


class EnrollmentCursor:
    def __init__(self, domain, value) -> None:
        self.domain = domain
        self.value = value
        self.rows: tuple = ()

    def execute(self, sql: str, *params: object) -> None:
        if "domains" in sql:
            self.rows = ((self.domain.connector, self.domain.service_id, self.domain.physical_subject_sha256),)
            return
        if "ch_supervisor_enrollments" in sql:
            if "database_uuid" in sql or "enrollment_document" in sql:
                body = self.value.body
                self.rows = (
                    (
                        body["service_id"],
                        body["database_uuid"],
                        self.value.enrollment_sha256,
                        self.value.document,
                    ),
                )
                return
            self.rows = ((self.domain.service_id,),)
            return
        self.rows = ()

    def fetchall(self) -> tuple:
        return self.rows

    def close(self) -> None:
        return None


@pytest.fixture
def protected_clickhouse_case(monkeypatch):
    """Exercise real ledger identity checks with fixed, distinct service markers."""
    import dpone.adapters.composition_mssql_store_queries as queries
    import dpone.app.composition_clickhouse_admission as admission

    domain = clickhouse_physical_domain(SERVICE, DATABASE)
    body = enrollment_body()
    body.update(service_id=SERVICE, database_uuid=DATABASE)
    events = []

    class Cursor(EnrollmentCursor):
        sql_service = OTHER_SERVICE

        def execute(self, sql, *parameters):
            events.append(("sql", sql, parameters))
            if "sp_getapplock" in sql:
                self.rows = ((0,),)
            elif "DECLARE @count" in sql:
                self.rows = ((1, 1, "Exclusive", 71),)
            elif "composition_authority" in sql:
                # This observed marker never learns a caller's expected UUID.
                self.rows = ((1, 2, self.sql_service),)
            else:
                super().execute(sql, *parameters)

        def fetchone(self):
            return self.rows[0] if self.rows else None

        def close(self):
            events.append("cursor.close")

    cursor = Cursor(domain, enrolled(body))
    control = SimpleNamespace(
        autocommit=True,
        cursor=lambda: cursor,
        rollback=lambda: events.append("rollback"),
        close=lambda: events.append("control.close"),
    )
    connections = SimpleNamespace(
        control_connection=lambda context: control,
        control_connection_with_service=lambda context: (control, OTHER_SERVICE),
    )
    monkeypatch.setattr(admission, "require_clickhouse_supervisor_schema", lambda *args: None)
    monkeypatch.setattr(queries, "require_composition_mssql_schema", lambda *args: None)
    return SimpleNamespace(
        domain=domain, body=body, cursor=cursor, control=control, connections=connections, events=events
    )


def test_clickhouse_enrollment_uses_distinct_verified_control_service(protected_clickhouse_case):
    case = protected_clickhouse_case
    assert case.domain.service_id != OTHER_SERVICE
    require_protected_clickhouse_enrollment(
        case.domain, CONTEXT, connections=case.connections, control_schema="dpone_control"
    )
    assert case.events[-3:] == ["rollback", "cursor.close", "control.close"]
    enrollment_queries = [
        event for event in case.events if isinstance(event, tuple) and "ch_supervisor_enrollments" in event[1]
    ]
    assert len(enrollment_queries) == 1 and enrollment_queries[0][2] == (SERVICE,)


def test_clickhouse_enrollment_rejects_ch_uuid_as_sql_marker(protected_clickhouse_case):
    case = protected_clickhouse_case
    case.cursor.sql_service = SERVICE
    with pytest.raises(CompositionAdmissionError, match="control_authority"):
        require_protected_clickhouse_enrollment(
            case.domain, CONTEXT, connections=case.connections, control_schema="dpone_control"
        )
    assert case.events[-3:] == ["rollback", "cursor.close", "control.close"]
    assert not any(isinstance(event, tuple) and "ch_supervisor_enrollments" in event[1] for event in case.events)


@pytest.mark.parametrize("field", ["service_id", "database_uuid"])
def test_distinct_control_does_not_weaken_clickhouse_identity(protected_clickhouse_case, field):
    case = protected_clickhouse_case
    case.body[field] = OTHER_SERVICE if field == "service_id" else OTHER_DATABASE
    case.cursor.value = enrolled(case.body)
    with pytest.raises(CompositionAdmissionError, match="clickhouse_enrollment"):
        require_protected_clickhouse_enrollment(
            case.domain, CONTEXT, connections=case.connections, control_schema="dpone_control"
        )
    assert case.events[-3:] == ["rollback", "cursor.close", "control.close"]


def mssql_materialization_target(*, service_id: str = SERVICE):
    credentials = CredentialsConfig(database="data")
    target = ResolvedBindingConnection(
        credentials,
        {},
        ResolvedConnectionDescriptor(
            "mssql",
            {
                "database": "data",
                "composition_service_id": service_id,
                "database_authorities": {
                    "data": {
                        "database_id": 8,
                        "database_guid": DATABASE,
                        "create_token": "2026-01-01T00:00:00",
                    }
                },
            },
        ),
    )
    write = DbtRelationWrite("native", "flow", "model.test", "model", "mssql", "db", "data", "managed", "target")
    return target, write, mssql_target_pin(target, write)
