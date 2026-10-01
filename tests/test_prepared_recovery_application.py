"""Real verified context and private plans; no permissive observer fallback."""

import json
from importlib import import_module

import pytest

from dpone.adapters.publication_plan_file import write_private_plan
from tests.prepared_recovery_operator_fixtures import operator_rig
from tests.test_prepared_recovery_plan_codec import operator_plan
from tests.test_publication_schema_application import operator_context, projected_credentials  # noqa: F401


def application(**kwargs):
    return import_module("dpone.app.prepared_recovery_application").PreparedRecoveryApplication(**kwargs)


def request(path):
    return dict(
        connection_ref="source-main",
        sink_connection_ref="sink-main",
        environment="prod",
        cluster="cluster",
        database="analytics",
        target="target",
        operation_id="operation",
        expected_version=1,
        path=path,
    )


def test_missing_observer_blocks_before_context_credentials_or_connectors(tmp_path):
    result = application(environ={}, connector_factory=lambda _: pytest.fail("connector opened")).plan(
        **request(tmp_path / "plan")
    )
    assert result["status"] == "blocked"
    assert result["reason_code"] == "held_recovery_observer_required"
    assert not (tmp_path / "plan").exists()


@pytest.mark.parametrize("confirmation", [None, "", "a" * 64, "é" * 64, 1])
def test_confirmation_precedes_context_and_observer(tmp_path, confirmation):
    saved = operator_plan()
    path = tmp_path / "plan"
    write_private_plan(path, saved.payload.encode())
    result = application(environ={}).execute(path=path, environment="test", confirmation_digest=confirmation)
    assert result["status"] == "blocked"
    assert result["reason_code"] == "plan_confirmation_required"


def test_saved_environment_precedes_observer_and_credentials(tmp_path):
    saved = operator_plan()
    path = tmp_path / "plan"
    write_private_plan(path, saved.payload.encode())
    result = application(environ={}).execute(path=path, environment="prod", confirmation_digest=saved.digest)
    assert result["reason_code"] == "plan_environment_differs"


@pytest.mark.parametrize("damage", ["missing", "tampered", "unbound", "environment"])
def test_unverified_context_never_reaches_connector(tmp_path, damage):
    environ, root = operator_context(tmp_path)
    args = request(tmp_path / "plan")
    if damage == "missing":
        environ = {}
    elif damage == "tampered":
        (root / "binding-set.json").write_bytes(b"{}")
    elif damage == "unbound":
        args["connection_ref"] = "unbound"
    else:
        args["environment"] = "dev"
    result = application(
        environ=environ, safety=object(), connector_factory=lambda _: pytest.fail("connector opened")
    ).plan(**args)
    assert result["status"] not in {"ready", "completed"}
    assert not args["path"].exists()
    assert "synthetic-private" not in json.dumps(result)


def prepared(tmp_path, monkeypatch):
    r = operator_rig(tmp_path, monkeypatch)
    app = application(environ=r.environ, safety=r.safety, connector_factory=r.server.connect, clock=lambda: 130)
    path = tmp_path / "native-plan"
    args = request(path)
    args["operation_id"] = r.authority.current.record.operation_id
    result = app.plan(**args)
    assert result["status"] == "ready", result
    return r, app, path, result


def test_real_adapters_plan_execute_and_replay_without_second_dispatch(tmp_path, monkeypatch):
    r, app, path, planned = prepared(tmp_path, monkeypatch)
    assert r.server.effects == [] and r.authority.mutations == 0
    assert len(r.server.handles) == 1 and r.server.handles[0].closed == 1
    for _ in range(2):
        result = app.execute(path=path, environment="prod", confirmation_digest=planned["plan_digest"])
        assert result["status"] == "completed", result
        assert len(r.server.effects) == 2
        assert all(handle.closed == 1 for handle in r.server.handles)
    assert len(r.server.handles) == 3
    assert len(r.builds) == 3
    assert "synthetic-private" not in path.read_text() + json.dumps(result)


def test_lost_ddl_replies_use_real_readback_not_redispatch(tmp_path, monkeypatch):
    r, app, path, planned = prepared(tmp_path, monkeypatch)
    r.server.lose_reply = True
    result = app.execute(path=path, environment="prod", confirmation_digest=planned["plan_digest"])
    assert result["status"] == "completed", result
    assert len(r.server.effects) == 2


@pytest.mark.parametrize("field", ["server_uuid", "database_uuid", "driver"])
def test_changed_physical_endpoint_or_transport_never_mutates(tmp_path, monkeypatch, field):
    from uuid import uuid4

    r, app, path, planned = prepared(tmp_path, monkeypatch)
    setattr(r.server, field, "http" if field == "driver" else str(uuid4()))
    result = app.execute(path=path, environment="prod", confirmation_digest=planned["plan_digest"])
    assert result["status"] == "blocked", result
    assert r.server.effects == [] and r.authority.mutations == 0
    assert len(r.builds) == 1
    assert all(handle.closed == 1 for handle in r.server.handles)


def test_close_failure_after_completed_effect_is_unknown_and_keeps_plan(tmp_path, monkeypatch):
    r, app, path, planned = prepared(tmp_path, monkeypatch)
    before = path.read_bytes()
    r.server.fail_close = True
    result = app.execute(path=path, environment="prod", confirmation_digest=planned["plan_digest"])
    assert result["status"] == "outcome_unknown"
    assert len(r.server.effects) == 2
    assert path.read_bytes() == before
    assert all(handle.closed == 1 for handle in r.server.handles)
    assert "synthetic-private" not in json.dumps(result)


def test_close_failure_during_plan_never_publishes_ready_file(tmp_path, monkeypatch):
    r = operator_rig(tmp_path, monkeypatch)
    r.server.fail_close = True
    args = request(tmp_path / "plan")
    args["operation_id"] = r.authority.current.record.operation_id
    result = application(
        environ=r.environ, safety=r.safety, connector_factory=r.server.connect, clock=lambda: 130
    ).plan(**args)
    assert result["status"] == "outcome_unknown"
    assert not args["path"].exists()
    assert r.server.effects == []
    assert r.server.handles[0].closed == 1


def test_existing_plan_is_preserved_and_connection_closed(tmp_path, monkeypatch):
    r, app, path, _ = prepared(tmp_path, monkeypatch)
    before = path.read_bytes()
    args = request(path)
    args["operation_id"] = r.authority.current.record.operation_id
    result = app.plan(**args)
    assert result["status"] == "outcome_unknown"
    assert path.read_bytes() == before
    assert r.server.effects == []
    assert all(handle.closed == 1 for handle in r.server.handles)


@pytest.mark.parametrize("damage", ["tampered", "changed_context"])
def test_context_drift_is_rejected_before_new_connectors(tmp_path, monkeypatch, damage):
    from tests.test_runtime_connection_context_loader import _replace_plan_descriptor

    r, _, path, planned = prepared(tmp_path, monkeypatch)
    registry_path = r.root / "connection-registry.json"
    registry = json.loads(registry_path.read_bytes())
    registry["connections"]["sink-registry"]["connection"]["host"] = "other-clickhouse"
    content = json.dumps(registry, sort_keys=True, separators=(",", ":")).encode()
    registry_path.write_bytes(content)
    environ = r.environ
    if damage == "changed_context":
        environ = _replace_plan_descriptor(environ, name="connection_registry", content=content)
    result = application(environ=environ, safety=r.safety, connector_factory=r.server.connect).execute(
        path=path, environment="prod", confirmation_digest=planned["plan_digest"]
    )
    assert result["status"] not in {"completed", "ready"}
    assert len(r.server.handles) == 1 and len(r.builds) == 1
    assert r.server.effects == []


def test_lost_hold_stops_before_effect_and_closes_target(tmp_path, monkeypatch):
    r, app, path, planned = prepared(tmp_path, monkeypatch)
    r.safety.fail_at = r.safety.checks + 1
    result = app.execute(path=path, environment="prod", confirmation_digest=planned["plan_digest"])
    assert result["status"] == "outcome_unknown"
    assert r.server.effects == [] and r.authority.mutations == 0
    assert all(handle.closed == 1 for handle in r.server.handles)


@pytest.mark.parametrize(
    "field,value", [("server_uuid", "not-a-uuid"), ("database_uuid", "00000000-0000-0000-0000-000000000000")]
)
def test_unusable_endpoint_still_closes_once_without_authority_access(tmp_path, monkeypatch, field, value):
    r = operator_rig(tmp_path, monkeypatch)
    setattr(r.server, field, value)
    result = application(environ=r.environ, safety=r.safety, connector_factory=r.server.connect).plan(
        **request(tmp_path / "plan")
    )
    assert result["status"] == "outcome_unknown"
    assert r.builds == [] and r.server.effects == []
    assert len(r.server.handles) == 1 and r.server.handles[0].closed == 1


@pytest.mark.parametrize("status", ["outcome_unknown", "conflict"])
def test_unacknowledged_cas_does_not_dispatch_or_retry(tmp_path, monkeypatch, status):
    from dpone.contracts.clickhouse_cluster_publication import AuthorityMutationStatus

    r, app, path, planned = prepared(tmp_path, monkeypatch)
    r.authority.outcome = AuthorityMutationStatus(status)
    result = app.execute(path=path, environment="prod", confirmation_digest=planned["plan_digest"])
    assert result["status"] == "outcome_unknown"
    assert r.authority.mutations == 1 and r.server.effects == []
    assert all(handle.closed == 1 for handle in r.server.handles)


@pytest.mark.parametrize("subject", [None, "", "a" * 64, "sha256:" + "A" * 64, "sha256:" + "a" * 63])
def test_subject_conversion_is_strict_not_permissive(subject):
    from types import SimpleNamespace

    from dpone.app.publication_operator_context import OperatorScopeBlocked, context_subject

    with pytest.raises(OperatorScopeBlocked, match="^verified_context_subject_required$"):
        context_subject(SimpleNamespace(authority_subject_sha256=subject))
