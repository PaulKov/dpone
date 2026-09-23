"""P9b management adapter performs the exact bounded settlement observations."""

from dataclasses import astuple, replace
from types import SimpleNamespace

import pytest

from dpone.adapters import mssql_sqlclient_restricted_writer_settlement as adapter
from dpone.adapters.mssql_sqlclient_grant_catalog import _permission_row
from dpone.adapters.mssql_tds_coordinator_connection import TdsSqlConnection
from dpone.contracts.mssql_sqlclient_restricted_writer_settlement import RestrictedWriterSettlementOperations
from dpone.contracts.mssql_sqlclient_restricted_writer_settlement_codec import (
    encode_request,
    remote_settlement_payload,
    validate_result,
)
from tests.test_mssql_sqlclient_restricted_writer_settlement_codec import settlement_fixture

OPERATIONS = RestrictedWriterSettlementOperations(encode_request, validate_result, remote_settlement_payload)


@pytest.mark.parametrize(("count", "public_sid"), [(0, "010500000000000904000000"), (1, "00")])
def test_adapter_reobserves_principals_grants_stage_and_exact_count(monkeypatch, count, public_sid):
    _, request, expected = settlement_fixture()
    expected = replace(
        expected,
        principals=(expected.principals[0], replace(expected.principals[1], sid=public_sid)),
    )
    cursor, calls = object(), []
    connection = TdsSqlConnection(SimpleNamespace(close=lambda: None), cursor)

    class Sql:
        def __init__(self, value, *args):
            assert value is connection

        def acquire(self, nonce, *, deadline):
            calls.append(("acquire", nonce))

        def check_deadline(self, *, deadline):
            calls.append(("deadline", deadline))

    class Exclusion:
        def __init__(self, value, **kwargs):
            assert value is cursor
            assert kwargs["admission"] is request.plan.writer_admission
            assert kwargs["observer_admission"] is request.plan.management_admission

        def observe(self, **kwargs):
            assert kwargs["original"] == request.plan.verify_result.opening.session
            calls.append(("absence", kwargs["original"]))
            return expected.absence

    class Session:
        def begin(self):
            calls.append(("count_begin", None))
            return self

        def rows(self, lease, statement, parameters, **kwargs):
            assert lease is self and "COUNT_BIG(*)" in statement and parameters == ()
            calls.append(("count", statement))
            return [(count,)]

        def finish(self, lease):
            calls.append(("count_finish", None))

        def fail(self, lease):
            calls.append(("count_fail", None))

    session = Session()

    class Catalog:
        @classmethod
        def _sharing(cls, sql, *, deadline, session):
            return cls()

        def read_own_incarnation(self):
            return ("incarnation",)

        def writer_admission(self, *args):
            admission = request.plan.writer_admission
            return (
                (
                    admission.server.server_name,
                    admission.server.machine_name,
                    admission.server.instance_name,
                    admission.server.physical_machine_name,
                    admission.database.database_id,
                    admission.database.database_name,
                    request.plan.verify_request.stage.database_guid,
                    bytes.fromhex(admission.database.owner_sid),
                    admission.login.principal_id,
                    admission.login.name,
                    bytes.fromhex(admission.login.sid),
                    int(admission.login.is_sysadmin),
                    "SQL_LOGIN",
                ),
            )

        def principals(self, *args):
            return tuple(
                (
                    row.principal_id,
                    row.name,
                    bytes.fromhex(row.sid),
                    row.type_desc,
                    row.authentication_type_desc,
                )
                for row in expected.principals
            )

        def permissions(self, *args, **kwargs):
            padded = []
            for row in expected.direct_permissions:
                raw = list(astuple(row))
                raw[5] = raw[5].ljust(4)
                padded.append(_permission_row(tuple(raw)))
            return tuple(padded)

    class Stage:
        @classmethod
        def _sharing(cls, current, **kwargs):
            assert current is session
            return cls()

        def observe(self, value):
            calls.append(("stage", value))
            return expected.stage

    monkeypatch.setattr(adapter, "TdsCoordinatorSql", Sql)
    monkeypatch.setattr(adapter, "SqlClientRestrictedSessionDepartureObserver", Exclusion)
    monkeypatch.setattr(adapter, "ObservationCursor", lambda value, **kwargs: session)
    monkeypatch.setattr(adapter, "SqlClientGrantCatalog", Catalog)
    monkeypatch.setattr(adapter, "SqlClientStageObserver", Stage)
    monkeypatch.setattr(adapter, "parse_observer_incarnation_rows", lambda *args, **kwargs: expected.catalog_observer)
    if count:
        with pytest.raises(RuntimeError):
            adapter.SqlClientRestrictedWriterSettlementObserver(connection, request, OPERATIONS).observe(b"n" * 32)
        assert all(name != "stage" for name, _ in calls)
    else:
        observed = adapter.SqlClientRestrictedWriterSettlementObserver(connection, request, OPERATIONS).observe(
            b"n" * 32
        )
        assert observed == expected
        assert [name for name, _ in calls if name in {"absence", "count", "stage"}] == [
            "absence",
            "count",
            "stage",
        ]
