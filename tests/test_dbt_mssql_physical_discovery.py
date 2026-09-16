"""P-only discovery representation and settlement, without live certification."""

from dataclasses import replace

import pytest

from dpone.contracts.dbt_mssql_physical_discovery import (
    DiscoveryObject,
    PhysicalDiscoveryRequest,
    decode_discovery_request,
    encode_discovery_request,
)
from dpone.contracts.dbt_workspace_activation import DbtWorkspaceGuardEpoch
from dpone.contracts.dbt_workspace_attempt import DbtWorkspaceAttemptRequest
from tests.support.dbt_mssql_physical_registration import registration_inputs


def request():
    return PhysicalDiscoveryRequest(
        registration_inputs()["platform_subject"],
        DbtWorkspaceAttemptRequest.build(
            activation_id="10000000-0000-0000-0000-000000000001",
            attempt_id="sha256:" + "a" * 64,
            workflow_id="orders",
            write_subjects=("sha256:" + "b" * 64,),
        ),
        DbtWorkspaceGuardEpoch("guard", 1),
        "20000000-0000-0000-0000-000000000001",
        "30000000-0000-0000-0000-000000000001",
        "DATA",
        tuple(
            DiscoveryObject("model.example.orders", role, name)
            for role, name in [("TARGET", "orders"), ("CANDIDATE", "candidate"), ("HELPER", "helper")]
        ),
    )


def test_request_is_complete_canonical_and_bounded():
    value = request()
    payload = encode_discovery_request(value, max_bytes=65536, max_objects=3)
    assert decode_discovery_request(payload, max_bytes=len(payload), max_objects=3) == value
    with pytest.raises(ValueError):
        encode_discovery_request(value, max_bytes=len(payload) - 1, max_objects=3)
    with pytest.raises(ValueError):
        encode_discovery_request(value, max_bytes=65536, max_objects=2)


@pytest.mark.parametrize(
    "mutation",
    [lambda v: v.objects[:2], lambda v: (v.objects[1], v.objects[0], v.objects[2]), lambda v: v.objects + v.objects],
)
def test_exact_distinct_ordered_model_triples(mutation):
    value = request()
    with pytest.raises(ValueError):
        replace(value, objects=mutation(value))


@pytest.mark.parametrize(
    "change",
    [
        lambda raw: raw.update(schema=None),
        lambda raw: raw.update(database="override"),
        lambda raw: raw["guard"].update(fencing_epoch=True),
        lambda raw: raw["objects"][0].update(name="bad\x00name"),
        lambda raw: raw["workspace_attempt"].update(extra="forbidden"),
    ],
)
def test_closed_nested_request_rejects(change):
    from dpone.contracts.native_delivery_json import encode_native_delivery_json

    raw = request().to_dict()
    change(raw)
    with pytest.raises(ValueError):
        decode_discovery_request(encode_native_delivery_json(raw), max_bytes=65536, max_objects=3)


class Cursor:
    def __init__(self, rows, extra=False):
        self.rows = list(rows)
        self.extra = extra
        self.calls = []
        self.closed = False

    def execute(self, sql, *args):
        self.calls.append((sql, args))
        return self

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None

    def nextset(self):
        return self.extra if len(self.calls) > 1 else None

    def close(self):
        self.closed = True


class Connection:
    def __init__(self, cursor, failure=None):
        self._cursor = cursor
        self.failure = failure
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commits += 1
        if self.failure:
            raise self.failure

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


def row():
    from dpone.contracts.dbt_mssql_physical_discovery import discovery_request_digest
    from dpone.contracts.dbt_mssql_physical_registration_codec import physical_runtime_registration_digest
    from tests.test_dbt_mssql_physical_catalog_binding import binding, registration

    r, b = registration(), binding()
    payload = encode_discovery_request(request(), max_bytes=r.limits.max_metadata_bytes, max_objects=3)
    return (
        1,
        r.registration_id,
        physical_runtime_registration_digest(r).encode(),
        discovery_request_digest(payload).encode(),
        r.model_database.database_id,
        r.model_database.database_guid,
        b.model_schema_id,
        b.model_schema,
        1,
        1,
        3,
        2,
        "DATA",
        "FG",
        r.principals.metadata.model.principal_id,
        bytes.fromhex(r.principals.metadata.model.sid_hex),
        r.principals.metadata.control.principal_id,
        bytes.fromhex(r.principals.metadata.control.sid_hex),
    )


def reader(connection, clock=lambda: 0):
    from dpone.adapters.dbt_mssql_physical_discovery import MssqlPhysicalDiscoveryReader
    from tests.test_dbt_mssql_physical_catalog_binding import binding, registration

    return MssqlPhysicalDiscoveryReader(
        connection_factory=lambda: connection,
        registration=registration(),
        binding=binding(),
        operation_timeout_seconds=10,
        clock=clock,
    )


def test_reader_requires_acknowledged_commit_and_closes_owned_resources():
    cursor = Cursor([row()])
    connection = Connection(cursor)
    result = reader(connection).read(request())
    assert result.filegroup_name == "DATA"
    assert connection.commits == 1 and connection.rollbacks == 0
    assert cursor.closed and connection.closed
    assert len(cursor.calls) == 2
    assert "physical_discover_absent_v1" in cursor.calls[1][0]


@pytest.mark.parametrize("mode", ["empty", "extra_row", "extra_set", "identity", "bool", "binary", "commit", "cancel"])
def test_reader_rejects_incomplete_or_unsettled_observations(mode):
    from dpone.adapters.dbt_mssql_physical_discovery import PhysicalDiscoveryReadError

    facts = list(row())
    if mode == "identity":
        facts[9] = 2
    if mode == "bool":
        facts[10] = True
    if mode == "binary":
        facts[2] = b"a" * 1000
    cursor = Cursor([] if mode == "empty" else [tuple(facts)] * (2 if mode == "extra_row" else 1), mode == "extra_set")
    failure = RuntimeError("commit unknown") if mode == "commit" else KeyboardInterrupt() if mode == "cancel" else None
    connection = Connection(cursor, failure)
    with pytest.raises(KeyboardInterrupt if mode == "cancel" else PhysicalDiscoveryReadError):
        reader(connection).read(request())
    assert connection.rollbacks == 1 and connection.closed and cursor.closed
    assert connection.commits == (1 if mode in ("commit", "cancel") else 0)


def test_expired_connection_deadline_never_executes_or_commits():
    from dpone.adapters.dbt_mssql_physical_discovery import PhysicalDiscoveryReadError

    cursor = Cursor([row()])
    connection = Connection(cursor)
    times = iter((0, 11))
    with pytest.raises(PhysicalDiscoveryReadError):
        reader(connection, lambda: next(times)).read(request())
    assert not cursor.calls and connection.commits == 0 and connection.rollbacks == 1


@pytest.mark.parametrize("subject_count,accepted", [(1211, True), (1212, False)])
def test_native_token_ceiling_matches_closed_shape_at_nearest_valid_boundaries(subject_count, accepted):
    import re

    from dpone.contracts.native_delivery_json import MAX_NATIVE_JSON_TOKENS, NativeJsonError
    from dpone.contracts.strict_json import canonical_json_bytes

    value = request()
    attempt = DbtWorkspaceAttemptRequest.build(
        activation_id=value.workspace_attempt.activation_id,
        attempt_id=value.workspace_attempt.attempt_id,
        workflow_id=value.workspace_attempt.workflow_id,
        write_subjects=tuple("sha256:" + format(i, "064x") for i in range(subject_count)),
    )
    objects = tuple(
        DiscoveryObject(f"model.example.m{i:04d}", role, f"n{i}_{role}")
        for i in range(1500)
        for role in ("TARGET", "CANDIDATE", "HELPER")
    )
    value = replace(value, workspace_attempt=attempt, objects=objects)
    canonical = canonical_json_bytes(value.to_dict())
    lexical = len(re.findall(r'"(?:\\.|[^"\\])*"|[{}\[\],:]|-?\d+|true|false|null', canonical.decode()))
    assert lexical == 113 + 2 * subject_count + 14 * len(objects)
    # This closed shape always has odd token cardinality: the nearest valid
    # requests straddle the even native ceiling at65535 and65537.
    assert lexical == MAX_NATIVE_JSON_TOKENS + (-1 if accepted else 1)
    assert len(canonical) < 1048576
    if accepted:
        assert encode_discovery_request(value, max_bytes=1048576, max_objects=4500) == canonical
        assert decode_discovery_request(canonical, max_bytes=1048576, max_objects=4500) == value
    else:
        with pytest.raises(NativeJsonError, match="lexical token"):
            encode_discovery_request(value, max_bytes=1048576, max_objects=4500)
