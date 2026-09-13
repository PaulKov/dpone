"""Offline fixed-command principal checks; no live ClickHouse certification."""

from unittest.mock import Mock

import pytest

from dpone.adapters.composition_clickhouse_principal import ClickHousePrincipalAdmin, IssuedClickHouseCredentials
from dpone.contracts.composition_identity import CompositionAdmissionError
from tests.composition_snapshot_helpers import intent


def test_credentials_hide_secret():
    value = IssuedClickHouseCredentials("dpone_ch_" + "a" * 64, "11111111-1111-1111-1111-111111111111", "secret")
    assert "secret" not in repr(value)


def test_creation_error_is_sanitized_and_never_retried():
    client = Mock()
    client.command.side_effect = RuntimeError("sensitive password")
    admin = ClickHousePrincipalAdmin(client)
    with pytest.raises(CompositionAdmissionError, match="principal_operation_unknown") as error:
        admin.create_disabled("dpone_ch_" + "a" * 64, "secret")
    assert "sensitive" not in str(error.value)
    assert client.command.call_count == 1


def test_create_starts_disabled_and_uses_no_replacement():
    client = Mock()
    client.query.return_value = (("11111111-1111-1111-1111-111111111111",),)
    admin = ClickHousePrincipalAdmin(client)
    assert admin.create_disabled("dpone_ch_" + "a" * 64, "secret") == "11111111-1111-1111-1111-111111111111"
    sql = client.command.call_args.args[0]
    assert "HOST NONE" in sql and "IF NOT EXISTS" not in sql and "REPLACE" not in sql
    assert "secret" not in str(client.command.call_args.kwargs["parameters"])


USER = "dpone_ch_" + "a" * 64
USER_ID = "11111111-1111-1111-1111-111111111111"


class Client:
    def __init__(self, target, purpose="ingest", host=None, revoked=False):
        self.commands = []
        self.user = (USER_ID, "sha256_password", [] if host is None else [host], [], [], [], 0, [], [], 0, [], [])
        self.roles = ()
        self.rights = (
            () if revoked else tuple((*row, None, 0, 0) for row in ClickHousePrincipalAdmin.grants(target, purpose))
        )
        self.counts = {}
        self.fail = None

    def command(self, sql, **kwargs):
        self.commands.append((sql, kwargs))

    def query(self, sql, **kwargs):
        if self.fail:
            raise RuntimeError("sensitive secret")
        if sql.startswith("SELECT toString(id) FROM"):
            return ((self.user[0],),)
        if "FROM system.users" in sql:
            return (self.user,)
        if "FROM system.role_grants" in sql:
            return self.roles
        if "FROM system.grants" in sql:
            return self.rights
        return ((self.counts.get(sql, 0),),)


@pytest.mark.parametrize("purpose", ["ingest", "publisher"])
@pytest.mark.parametrize("host", [None, "10.1.0.2", "2001:db8::1"])
@pytest.mark.parametrize("revoked", [False, True])
def test_exact_catalog_subjects(purpose, host, revoked):
    target = intent().target
    client = Client(target, purpose, host, revoked)
    admin = ClickHousePrincipalAdmin(client)
    observed = admin.observe(USER, USER_ID, target, purpose, host=host, revoked=revoked)
    assert observed["user_id"] == USER_ID and observed["host"] == host


@pytest.mark.parametrize(
    "column,value",
    [
        (0, "22222222-2222-2222-2222-222222222222"),
        (1, ["no_password"]),
        (1, ["sha256_password", "plaintext_password"]),
        (2, ["0.0.0.0/0"]),
        (3, ["localhost"]),
        (4, [".*"]),
        (5, ["%"]),
        (6, 1),
        (7, ["admin"]),
        (8, ["admin"]),
        (9, 1),
        (10, ["admin"]),
        (11, ["admin"]),
    ],
)
def test_changed_identity_auth_host_or_inherited_access_rejects(column, value):
    target = intent().target
    client = Client(target)
    data = list(client.user)
    data[column] = value
    client.user = tuple(data)
    with pytest.raises(CompositionAdmissionError):
        ClickHousePrincipalAdmin(client).observe(USER, USER_ID, target, "ingest", host=None, revoked=False)


@pytest.mark.parametrize("change", ["role", "grant_option", "partial", "column", "scope", "extra", "missing"])
def test_any_unexpected_grant_rejects(change):
    target = intent().target
    client = Client(target)
    if change == "role":
        client.roles = (("admin",),)
    elif change == "extra":
        client.rights = (*client.rights, ("ALL", None, None, None, 0, 0))
    elif change == "missing":
        client.rights = client.rights[:-1]
    else:
        row = list(client.rights[0])
        index = {"grant_option": 5, "partial": 4, "column": 3, "scope": 2}[change]
        row[index] = 1 if index >= 4 else "unowned"
        client.rights = (tuple(row), *client.rights[1:])
    with pytest.raises(CompositionAdmissionError):
        ClickHousePrincipalAdmin(client).observe(USER, USER_ID, target, "ingest", host=None, revoked=False)


@pytest.mark.parametrize(
    "query",
    [
        "SELECT count() FROM system.processes WHERE user={user:String} OR initial_user={user:String}",
        "SELECT count() FROM system.transactions",
        "SELECT count() FROM system.asynchronous_inserts",
        "SELECT count() FROM system.mutations WHERE NOT is_done",
        "SELECT count() FROM system.distribution_queue",
    ],
)
def test_each_unresolved_server_work_class_blocks_quiescence(query):
    client = Client(intent().target)
    client.counts[query] = 1
    with pytest.raises(CompositionAdmissionError, match="not_quiescent"):
        ClickHousePrincipalAdmin(client).require_quiescence(USER)


def test_catalog_failure_is_sanitized():
    client = Client(intent().target)
    client.fail = True
    with pytest.raises(CompositionAdmissionError, match="observation_unknown") as error:
        ClickHousePrincipalAdmin(client).identity(USER)
    assert "secret" not in str(error.value)


@pytest.mark.parametrize("name", ["", "default", "dpone_ch_" + "g" * 64, USER + "; DROP USER default"])
def test_unbounded_names_never_reach_admin_client(name):
    client = Mock()
    with pytest.raises(CompositionAdmissionError):
        ClickHousePrincipalAdmin(client).create_disabled(name, "secret")
    client.command.assert_not_called()


def test_grants_are_exact_generation_or_pair():
    target = intent().target
    ingest = ClickHousePrincipalAdmin.grants(target, "ingest")
    publisher = ClickHousePrincipalAdmin.grants(target, "publisher")
    assert {table for _, _, table in ingest} == {target.generation_table}
    assert {table for _, _, table in publisher} == {target.generation_table, target.target_table}
    assert {privilege for privilege, _, _ in publisher} == {"SELECT", "DROP TABLE", "CREATE TABLE", "INSERT"}


@pytest.mark.parametrize(
    "rows", [(), (("00000000-0000-0000-0000-000000000001", "Atomic"),), ((intent().target.database_id, "Ordinary"),)]
)
def test_admin_endpoint_rejects_other_database_uuid_or_engine(rows):
    client = Mock()
    client.query.return_value = rows
    with pytest.raises(CompositionAdmissionError):
        ClickHousePrincipalAdmin(client).require_target(intent().target)


@pytest.mark.parametrize("version", ["24.8.14.39", "25.1.1.1", "26.8.1.1"])
def test_actual_admin_endpoint_and_pinned_catalog_version(version):
    target = intent().target
    client = Mock()
    client.query.return_value = ((version, target.service_id, target.database_id, "Atomic"),)
    if version.startswith("24.8."):
        ClickHousePrincipalAdmin(client).require_target(target)
    else:
        with pytest.raises(CompositionAdmissionError):
            ClickHousePrincipalAdmin(client).require_target(target)


def test_actual_other_server_uuid_rejects():
    target = intent().target
    client = Mock()
    client.query.return_value = (("24.8.14.39", USER_ID, target.database_id, "Atomic"),)
    with pytest.raises(CompositionAdmissionError):
        ClickHousePrincipalAdmin(client).require_target(target)


def test_access_ddl_renders_only_closed_literals_for_24_8_parser():
    target = intent().target
    client = Client(target)
    admin = ClickHousePrincipalAdmin(client)
    admin.create_disabled(USER, "secret")
    admin.grant_disabled(USER, USER_ID, target, "ingest")
    admin.enable(USER, USER_ID, "10.1.0.2")
    admin.revoke(USER, USER_ID)
    assert all("{" not in sql and not args["parameters"] for sql, args in client.commands)
    assert all("secret" not in sql for sql, _ in client.commands)


@pytest.mark.parametrize("host", ["127.0.0.1", "::1"])
def test_loopback_ip_mode_never_enables_localhost_authority(host):
    client = Mock()
    client.query.return_value = ((USER_ID,),)
    with pytest.raises(CompositionAdmissionError, match="host"):
        ClickHousePrincipalAdmin(client).enable(USER, USER_ID, host)
    client.command.assert_not_called()


def test_explicit_local_enable_requires_exact_named_uuid():
    client = Client(intent().target)
    ClickHousePrincipalAdmin(client).enable_local(USER, USER_ID)
    assert client.commands == [(f"ALTER USER `{USER}` HOST LOCAL", {"parameters": {}, "redactions": ()})]


@pytest.mark.parametrize("purpose", ["ingest", "publisher"])
def test_explicit_local_catalog(purpose):
    target = intent().target
    client = Client(target, purpose)
    row = list(client.user)
    row[3] = ["localhost"]
    client.user = tuple(row)
    observed = ClickHousePrincipalAdmin(client).observe_local(USER, USER_ID, target, purpose)
    assert observed["host"] == "LOCAL"


@pytest.mark.parametrize(
    "column,value", [(2, ["127.0.0.1"]), (3, []), (3, ["localhost", "other"]), (4, [".*"]), (5, ["%"]), (7, ["admin"])]
)
def test_local_catalog_rejects_broader_or_missing_host_policy(column, value):
    target = intent().target
    client = Client(target)
    row = list(client.user)
    row[3] = ["localhost"]
    row[column] = value
    client.user = tuple(row)
    with pytest.raises(CompositionAdmissionError):
        ClickHousePrincipalAdmin(client).observe_local(USER, USER_ID, target, "ingest")
