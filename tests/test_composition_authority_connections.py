"""Sealed endpoints retain credential boundaries; these are offline driver tests."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from dpone.app.composition_authority_connections import CompositionAuthorityConnections
from dpone.contracts.composition_activation import CompositionOccurrenceContext
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.runtime_connection import ResolvedBindingConnection, ResolvedConnectionDescriptor
from dpone.runtime.credentials.config import CredentialsConfig

SERVICE = "10000000-0000-4000-8000-000000000001"
GUID = "20000000-0000-4000-8000-000000000002"
CONTEXT = CompositionOccurrenceContext(
    "10000000-0000-4000-8000-000000000003",
    "test",
    "sha256:" + "a" * 64,
    "sha256:" + "b" * 64,
    None,
    "sha256:" + "c" * 64,
)
PIN = {"database_id": 8, "database_guid": GUID, "create_token": "2026-01-01T00:00:00"}


def bound(*, target=False):
    database = "target" if target else "control"
    credentials = CredentialsConfig(
        host="target-host" if target else "control-host",
        port=1433,
        database=database,
        username="reader" if target else "controller",
        password="TARGET_SECRET" if target else "CONTROL_SECRET",
        encrypt="yes",
        trust_server_certificate="no",
    )
    descriptor = ResolvedConnectionDescriptor(
        "mssql",
        {
            "database": database,
            "composition_service_id": SERVICE,
            "database_authorities": {"control": PIN, "target": {**PIN, "database_id": 9}},
        },
    )
    return ResolvedBindingConnection(credentials, {}, descriptor)


class Cursor:
    def __init__(self, events):
        self.events = events
        self.header = (1, 1, 77, 8, "control", GUID, PIN["create_token"], 1)
        self.marker = ((1, 2, SERVICE),)
        self.sql = ""

    def execute(self, sql, *params):
        self.sql = sql
        self.events.append(("sql", sql, params))
        return self

    def fetchall(self):
        return self.marker if "composition_authority" in self.sql else (self.header,)

    def close(self):
        self.events.append("cursor.close")


class Raw:
    autocommit = False

    def __init__(self):
        self.events = []
        self.handle = Cursor(self.events)

    def cursor(self):
        return self.handle

    def rollback(self):
        self.events.append("rollback")

    def close(self):
        self.events.append("raw.close")


@pytest.fixture
def authority(monkeypatch):
    import dpone.app.composition_authority_connections as module

    calls = []
    raw = Raw()

    def factory(resolved, *, autocommit):
        calls.append(resolved)
        return SimpleNamespace(connection=raw, close=lambda: raw.close())

    monkeypatch.setattr(
        module, "require_composition_mssql_schema", lambda cursor, schema: raw.events.append(("catalog", schema))
    )
    authority = CompositionAuthorityConnections(
        inputs=SimpleNamespace(resolve_connection=lambda context, ref: bound()),
        authority_connection_ref="authority",
        connector_factory=factory,
    )
    return authority, raw, calls


def test_target_probe_never_forwards_control_secret_or_endpoint(authority):
    authority, raw, calls = authority
    target = bound(target=True)
    authority.require_mssql_target_service(target, SERVICE, CONTEXT)
    observed = calls[0].credentials
    assert (observed.host, observed.username, observed.password, observed.database) == (
        "target-host",
        "reader",
        "TARGET_SECRET",
        "control",
    )
    assert (observed.encrypt, observed.trust_server_certificate) == ("yes", "no")
    assert target.credentials.database == "target"
    assert raw.events[-3:] == ["rollback", "cursor.close", "raw.close"]
    assert all("CONTROL_SECRET" not in str(event) for event in raw.events)


def test_control_connection_returns_open_owned_handle_after_verified_rollback(authority):
    authority, raw, calls = authority
    assert authority.control_connection(CONTEXT) is raw
    assert calls[0].credentials.password == "CONTROL_SECRET"
    assert raw.events[-2:] == ["rollback", "cursor.close"]
    assert "raw.close" not in raw.events


@pytest.mark.parametrize("mutation", ["service", "missing_control_pin", "different_control_pin", "connector"])
def test_target_mismatch_rejects_before_connect(authority, mutation):
    authority, _, calls = authority
    target = bound(target=True)
    properties = dict(target.descriptor.properties)
    if mutation == "service":
        properties["composition_service_id"] = GUID
    elif mutation == "missing_control_pin":
        properties["database_authorities"] = {"target": PIN}
    elif mutation == "different_control_pin":
        properties["database_authorities"] = {"control": {**PIN, "database_id": 10}, "target": PIN}
    target = replace(
        target, descriptor=ResolvedConnectionDescriptor("postgres" if mutation == "connector" else "mssql", properties)
    )
    with pytest.raises(CompositionAdmissionError):
        authority.require_mssql_target_service(target, SERVICE, CONTEXT)
    assert not calls


@pytest.mark.parametrize("fault", ["wrong_marker", "wrong_guid", "no_transaction", "no_catalog"])
def test_actual_target_proof_failures_close_every_resource(authority, monkeypatch, fault):
    authority, raw, _ = authority
    if fault == "wrong_marker":
        raw.handle.marker = ((1, 2, GUID),)
    elif fault == "wrong_guid":
        raw.handle.header = (*raw.handle.header[:5], SERVICE, *raw.handle.header[6:])
    elif fault == "no_transaction":
        raw.handle.header = (0, *raw.handle.header[1:])
    else:

        def fail(cursor, schema):
            raise RuntimeError("SENSITIVE_SENTINEL")

        monkeypatch.setattr("dpone.app.composition_authority_connections.require_composition_mssql_schema", fail)
    with pytest.raises(CompositionAdmissionError) as failure:
        authority.require_mssql_target_service(bound(target=True), SERVICE, CONTEXT)
    assert "SENSITIVE_SENTINEL" not in str(failure.value)
    assert raw.events[-3:] == ["rollback", "cursor.close", "raw.close"]


@pytest.fixture
def clickhouse():
    calls, events = [], []
    payload = [["data", ["dependent"], 1]]

    class Connector:
        def get_records(self, statement):
            events.append(statement)
            return payload

        def close(self):
            events.append("close")

    def factory(connection, *, autocommit):
        calls.append(connection)
        assert autocommit is True
        return Connector()

    connection = ResolvedBindingConnection(
        CredentialsConfig(
            host="ch-target",
            database="data",
            username="observer",
            password="CH_SECRET",
            secure=True,
            connect_timeout=200,
            send_receive_timeout=300,
            settings={"readonly": 0, "max_result_rows": 0, "skip_unavailable_shards": 1},
        ),
        {},
        ResolvedConnectionDescriptor("clickhouse", {"database": "data"}),
    )
    instance = CompositionAuthorityConnections(
        inputs=SimpleNamespace(), authority_connection_ref="authority", connector_factory=factory
    )
    return instance, connection, calls, events, payload


def test_clickhouse_detaches_rows_and_enforces_readonly_transport_limits(clickhouse):
    instance, connection, calls, events, payload = clickhouse
    rows = instance.query_clickhouse(connection, "SELECT count() FROM system.row_policies")
    payload[0][1].append("changed")
    assert rows == (("data", ("dependent",), 1),)
    sent = calls[0].credentials
    assert sent.host == "ch-target" and sent.password == "CH_SECRET" and sent.secure is True
    assert sent.connect_timeout == sent.send_receive_timeout == 10
    assert sent.settings["readonly"] == 1 and sent.settings["max_result_rows"] == 8193
    assert sent.settings["max_result_bytes"] == 8 * 1024 * 1024 and sent.settings["result_overflow_mode"] == "throw"
    assert sent.settings["skip_unavailable_shards"] == 0
    assert connection.credentials.settings["readonly"] == 0
    assert events[-1] == "close"


@pytest.mark.parametrize(
    "statement",
    [
        "DROP TABLE data.target",
        "SELECT * FROM data.target",
        "SELECT count() FROM system.row_policies; DROP TABLE data.target",
        "SELECT count() FROM system.row_policies SETTINGS readonly=0",
        "SELECT toString(serverUUID()),name,toString(uuid),engine FROM system.databases WHERE name='data'; SELECT 1 LIMIT 2",
    ],
)
def test_clickhouse_free_sql_or_override_rejected_before_connection(clickhouse, statement):
    instance, connection, calls, _, _ = clickhouse
    with pytest.raises(CompositionAdmissionError, match="clickhouse_catalog_statement"):
        instance.query_clickhouse(connection, statement)
    assert not calls


@pytest.mark.parametrize("fault", ["rows", "bytes", "nonfinite"])
def test_clickhouse_invalid_result_is_not_partial_success(clickhouse, fault):
    instance, connection, _, events, payload = clickhouse
    payload[:] = (
        [[0]] * 8193 if fault == "rows" else [["x" * (8 * 1024 * 1024)]] if fault == "bytes" else [[float("nan")]]
    )
    with pytest.raises(CompositionAdmissionError, match="clickhouse_catalog_query"):
        instance.query_clickhouse(connection, "SELECT count() FROM system.row_policies")
    assert events[-1] == "close"


@pytest.mark.parametrize(
    "statement",
    [
        "SELECT toString(serverUUID()),name,toString(uuid),engine FROM system.databases WHERE name='data' LIMIT 2",
        "SELECT count() FROM system.columns WHERE database='data' AND default_kind != ''",
        "SELECT count() FROM system.data_skipping_indices WHERE database='data'",
    ],
)
def test_exact_dynamic_catalog_shape_accepts_only_bound_settings(clickhouse, statement):
    instance, connection, _, events, _ = clickhouse
    suffix = (
        " SETTINGS max_execution_time=10,max_result_rows=8193,max_result_bytes=8388608,result_overflow_mode='throw'"
    )
    instance.query_clickhouse(connection, statement + suffix)
    assert events == [statement, "close"]


def test_control_cleanup_failure_never_returns_uncertain_handle(authority):
    authority, raw, _ = authority

    def fail():
        raw.events.append("rollback.failed")
        raise OSError("SENSITIVE_SENTINEL")

    raw.rollback = fail
    with pytest.raises(CompositionAdmissionError, match="authority_endpoint_cleanup") as failure:
        authority.control_connection(CONTEXT)
    assert raw.events[-2:] == ["cursor.close", "raw.close"]
    assert "SENSITIVE_SENTINEL" not in str(failure.value)


def test_missing_control_pin_rejects_without_io(authority):
    authority, _, calls = authority
    missing = replace(
        bound(),
        descriptor=ResolvedConnectionDescriptor("mssql", {"database": "control", "composition_service_id": SERVICE}),
    )
    authority._inputs.resolve_connection = lambda context, ref: missing
    with pytest.raises(CompositionAdmissionError, match="authority_connection"):
        authority.control_connection(CONTEXT)
    assert not calls
