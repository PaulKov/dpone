"""Synthetic query, metadata, descriptor and exact observation contracts."""

import time
from dataclasses import asdict, replace

import pytest

from dpone.adapters.target_acceptance import worker
from dpone.adapters.target_acceptance.native import NativeSession, metric_plan, parse_metrics, require_binding
from dpone.adapters.target_acceptance.reader import BoundedClickHouseTargetAcceptanceReader
from dpone.contracts.clickhouse_cluster_publication import ClusterInventory, ClusterReplica
from dpone.contracts.quality_replay import (
    MAX_UINT64,
    TargetAcceptanceError,
    TargetAcceptanceRequest,
    unavailable_observation,
    validate_target_observation,
)


def request():
    return TargetAcceptanceRequest(
        "cluster",
        "db",
        "table",
        "db.table",
        (("a", "UInt64"), ("null_a", "String")),
        "a" * 64,
        "b" * 64,
        {},
        "reader",
        "c" * 64,
        True,
        ("a", "null_a"),
        ("a",),
    )


@pytest.mark.parametrize("value", [True, -1, MAX_UINT64 + 1, "0", None, 1.0])
def test_counts_are_exact_uint64(value):
    _, plan = metric_plan(request())
    with pytest.raises(TargetAcceptanceError):
        parse_metrics([(value, 0, 0, 0)], [(f"__dpone_m{i}", "UInt64") for i in range(4)], plan)


def test_ordinal_aliases_cannot_collide_and_empty_is_real_zero():
    query, plan = metric_plan(request())
    assert query.count(" AS __dpone_m") == 4
    result = parse_metrics([(0, 0, 0, 0)], [(f"__dpone_m{i}", "UInt64") for i in range(4)], plan)
    assert result == {"row_count": 0, "null_counts": {"a": 0, "null_a": 0}, "distinct_counts": {"a": 0}}
    for rows, aliases in [([], []), ([(0,)], [("missing", "UInt64")]), ([(0, 0, 0, 0)], [("same", "UInt64")] * 4)]:
        with pytest.raises(TargetAcceptanceError):
            parse_metrics(rows, aliases, plan)


def test_unsupported_selected_types_and_unknown_columns_are_rejected():
    for req in [
        replace(request(), columns=(("a", "Array(UInt64)"), ("null_a", "String"))),
        replace(request(), null_columns=("missing",)),
    ]:
        with pytest.raises(TargetAcceptanceError):
            metric_plan(req)


def test_exact_observation_and_explicit_unavailable():
    req = request()
    observation = unavailable_observation(req, replica="replica", attempt_id="f" * 32)
    assert validate_target_observation(req, observation, allow_unavailable=True) == observation
    with pytest.raises(TargetAcceptanceError):
        validate_target_observation(req, observation)
    for bad in [
        {**observation, "row_count": 0},
        {**observation, "unexpected": 1},
        {**observation, "binding": {"forged": True}},
        {**observation, "attempt_id": "bad"},
    ]:
        with pytest.raises(TargetAcceptanceError):
            validate_target_observation(req, bad, allow_unavailable=True)


def test_no_connector_client_or_settings_are_used_at_construction():
    class Connector:
        @property
        def client(self):
            pytest.fail("connected client was touched")

    reader = BoundedClickHouseTargetAcceptanceReader.from_connector(Connector())
    with pytest.raises(TargetAcceptanceError, match="UNSUPPORTED"):
        reader.require_ready(cluster="cluster", database="db", table="table")


@pytest.mark.parametrize(
    "outcome",
    [
        "success",
        "unavailable",
        "drift",
        "verify",
        "socket_timeout",
        "server_timeout",
        "server_cancel",
        "protocol",
        "network",
        "unknown",
    ],
)
def test_one_replica_scan_and_before_after_inventory(monkeypatch, outcome):
    replicas = tuple(ClusterReplica(host, host, 9000, 1, i, True) for i, host in enumerate(("b", "a"), 1))
    inventory = ClusterInventory("cluster", replicas)
    desired = {
        "uuid": "u",
        "engine_full": "ReplicatedMergeTree('/x', 'r')",
        "schema_digest": "s",
        "keeper_name": "k",
        "keeper_path": "/x",
    }
    facts = {host: {**desired, "columns": list(request().columns)} for host in inventory.hosts}
    req = replace(request(), binding={"inventory_digest": inventory.digest, "desired": desired})
    reads = []

    def inspect(*args, **kwargs):
        reads.append("metadata")
        if outcome == "drift" and reads.count("metadata") > 1:
            return inventory, {**facts, "a": {**facts["a"], "uuid": "stale"}}
        return inventory, facts

    class Session:
        def __init__(self, *args, host, port):
            assert host == "a"

        def require_settings(self):
            pass

        def read(self, query, typed):
            reads.append("scan")
            if outcome in {"unavailable", "socket_timeout", "server_timeout", "server_cancel", "protocol", "network"}:
                errors = pytest.importorskip("clickhouse_driver.errors")
                failures = {
                    "unavailable": errors.ServerException("private endpoint", code=48),
                    "socket_timeout": errors.SocketTimeoutError("private endpoint"),
                    "server_timeout": errors.ServerException("private endpoint", code=159),
                    "server_cancel": errors.ServerException("private endpoint", code=394),
                    "protocol": errors.UnexpectedPacketFromServerError("private endpoint"),
                    "network": errors.NetworkError("private endpoint"),
                }
                raise failures[outcome]
            if outcome == "unknown":
                raise ValueError("private protocol corruption")
            return [(7, 0, 0, 7)], [(f"__dpone_m{i}", "UInt64") for i in range(4)]

        def close(self):
            pass

    monkeypatch.setattr(worker, "inspect_replicas", inspect)
    monkeypatch.setattr(worker, "NativeSession", Session)
    message = {
        "mode": "verify" if outcome == "verify" else "collect",
        "descriptor": {},
        "deadline": 1,
        "request": asdict(req),
        "nonce": "a" * 32,
    }
    if outcome == "drift":
        with pytest.raises(TargetAcceptanceError, match="MISMATCH"):
            worker.execute(message)
        return
    if outcome in {"socket_timeout", "server_timeout", "server_cancel", "protocol", "network", "unknown"}:
        # These failures happen before the global deadline; none is warn-only evidence.
        message["deadline"] = time.monotonic() + 60
        reason = "INVALID" if outcome in {"protocol", "unknown"} else "INCOMPLETE"
        with pytest.raises(TargetAcceptanceError, match=reason) as caught:
            worker.execute(message)
        assert not caught.value.probe_unavailable
        assert reads == ["metadata", "scan"]
        return
    result = worker.execute(message)
    if outcome == "verify":
        assert result == {}
        assert reads == ["metadata"]
        return
    if outcome == "unavailable":
        assert result["row_count"] is None
        assert result["null_counts"] == result["distinct_counts"] == {}
        assert result["warnings"] == ["target_acceptance_metric_probe_unavailable"]
    else:
        assert result["row_count"] == 7
    assert result["replica"] == "a"
    assert reads == ["metadata", "scan", "metadata"]


def test_missing_replica_and_stale_schema_fail_closed():
    inventory = ClusterInventory(
        "cluster", (ClusterReplica("a", "a", 9000, 1, 1, True), ClusterReplica("b", "b", 9000, 1, 2, True))
    )
    with pytest.raises(TargetAcceptanceError):
        require_binding(
            inventory, {"a": {"columns": []}}, {"inventory_digest": inventory.digest, "desired": {}}, request().columns
        )


def test_admission_requires_every_forced_setting():
    session = object.__new__(NativeSession)
    session.read = lambda *args: []
    with pytest.raises(TargetAcceptanceError, match="UNSUPPORTED"):
        session.require_settings()


def descriptor():
    return dict(
        host="localhost",
        port=9000,
        driver="native",
        database="default",
        user="default",
        password="SECRET",
        secure=False,
        compression=False,
        connect_timeout=10,
        send_receive_timeout=300,
        ca_cert=None,
        settings={"readonly": 0, "skip_unavailable_shards": 1},
    )


def test_full_private_request_is_reserved_before_publication():
    value = descriptor()
    value["password"] = "secret" * 50000
    reader = BoundedClickHouseTargetAcceptanceReader(value)
    with pytest.raises(TargetAcceptanceError, match="UNSUPPORTED") as caught:
        reader.validate_plan(request())
    assert "secret" not in str(caught.value)


def test_nonce_and_extra_response_fields_are_rejected(monkeypatch):
    from dpone.adapters.target_acceptance import reader as module

    reader = BoundedClickHouseTargetAcceptanceReader(descriptor())
    for response in ({"nonce": "forged", "error": None, "result": {}}, {"unexpected": True}):
        monkeypatch.setattr(module, "supervise", lambda *args, **kwargs: response)
        with pytest.raises(TargetAcceptanceError):
            reader.verify_generation(request(), deadline=999999999999)


@pytest.mark.parametrize(
    "engine,ddl",
    [
        ("ReplicatedReplacingMergeTree('/x', 'r')", "CREATE TABLE t"),
        ("ReplicatedMergeTree('/x', 'r')", "CREATE TABLE t TTL day + 1"),
    ],
)
def test_mutating_engine_or_ttl_never_admitted(engine, ddl):
    session = object.__new__(NativeSession)
    session.read = lambda *args: [("a", "uuid", engine, ddl)]
    with pytest.raises(TargetAcceptanceError, match="UNSUPPORTED"):
        session.generation("db", "table", "a", 2)


def test_forced_settings_override_all_connector_settings(monkeypatch):
    import sys
    from types import SimpleNamespace

    from dpone.adapters.target_acceptance.native import FORCED_SETTINGS

    calls = []
    monkeypatch.setitem(sys.modules, "clickhouse_driver", SimpleNamespace(Client=lambda **kwargs: calls.append(kwargs)))
    NativeSession(descriptor(), time.monotonic() + 5)
    assert calls[0]["settings"]["readonly"] == 1
    assert calls[0]["settings"]["skip_unavailable_shards"] == 0
    assert 0 < calls[0]["settings"]["max_execution_time"] <= 5
    assert calls[0]["send_receive_timeout"] <= 5
    assert set(calls[0]["settings"]) == set(FORCED_SETTINGS)


@pytest.mark.parametrize("present", [[], ["a"], ["a", "b"]])
def test_initial_admission_all_absent_or_all_present_only(monkeypatch, present):
    from dpone.adapters.target_acceptance import native

    inventory = ClusterInventory(
        "cluster", (ClusterReplica("a", "a", 9000, 1, 1, True), ClusterReplica("b", "b", 9000, 1, 2, True))
    )

    class Session:
        def __init__(self, *args, **kwargs):
            pass

        def require_settings(self):
            pass

        def inventory(self, cluster):
            return inventory

        def generation(self, database, table, host, count, *, allow_absent):
            assert allow_absent
            return {"uuid": "u"} if host in present else None

        def close(self):
            pass

    monkeypatch.setattr(native, "NativeSession", Session)
    if len(present) == 1:
        with pytest.raises(TargetAcceptanceError, match="MISMATCH"):
            native.inspect_replicas({}, "cluster", "db", "table", 1, allow_absent=True)
    else:
        actual, facts = native.inspect_replicas({}, "cluster", "db", "table", 1, allow_absent=True)
        assert actual == inventory
        assert set(facts) == set(present)


def test_absent_target_requires_explicit_admission_mode():
    session = object.__new__(NativeSession)
    session.read = lambda *args: []
    assert session.generation("db", "table", "a", 2, allow_absent=True) is None
    with pytest.raises(TargetAcceptanceError, match="MISMATCH"):
        session.generation("db", "table", "a", 2)
