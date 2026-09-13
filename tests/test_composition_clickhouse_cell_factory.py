"""Shared concrete graph preserves explicit storage and observer authority."""

from types import SimpleNamespace

import pytest

from dpone.app import composition_clickhouse_cell_factory as common
from dpone.app.composition_dbt_execution_factory import CompositionDbtControlAuthority
from dpone.contracts.composition_identity import CompositionAdmissionError
from tests.test_composition_clickhouse_capture_factory import arguments
from tests.test_composition_clickhouse_supervisor_enrollment import enrolled


def inputs(tmp_path, monkeypatch):
    args = arguments(tmp_path)
    parent = args["parent"]
    control = parent["control"]
    parent["control"] = CompositionDbtControlAuthority(
        control.connection_factory, control.expected_service_id, "control", "custom"
    )
    parent["read_active"] = lambda: pytest.fail("construction read current SQL")
    parent["target"] = SimpleNamespace(
        credentials=SimpleNamespace(
            host="127.0.0.1", port=8123, username="admin", password="secret", secure=False, additional_params={}
        )
    )
    monkeypatch.setattr(common, "snapshot_target_for_write", lambda *_: args["target"])
    monkeypatch.setattr(common, "snapshot_limits_from_manifest", lambda *_: args["limits"])
    files = SimpleNamespace(write_once=lambda *_: None, read=lambda *_: None)
    supervisor = SimpleNamespace(observe=lambda *_: pytest.fail("construction observed host"))
    return dict(
        parent=parent,
        manifest=args["manifest"],
        plan=args["plan"],
        attempt=args["attempt"],
        write=object(),
        enrollment=enrolled(),
        supervisor=supervisor,
        files=files,
        root=args["root"],
    )


def test_actual_graph_shares_capture_publication_and_terminal_with_explicit_custody(tmp_path, monkeypatch):
    args = inputs(tmp_path, monkeypatch)
    calls = []

    def host():
        calls.append("host")

    def same_ledger(ledger, subject):
        calls.append((ledger, subject))

    cell = common.build_protected_clickhouse_cell(**args, require_custody=host, require_enrollment_in=same_ledger)
    deps = cell.dependencies
    assert deps.capture is cell.capture
    assert cell.capture._files is args["files"]
    assert cell.terminal._capture is cell.capture._store
    assert cell.terminal._generation == cell.capture.load_generation_in
    assert cell.terminal._target == deps.target
    assert deps.worker_gate._terminal is cell.terminal and deps.outcome_observer is deps.worker_gate
    assert deps.worker_gate._ingest is deps.gate and deps.worker_gate._publisher is deps.publisher_gate
    assert deps.gate._supervisor is args["supervisor"]
    assert deps.publisher_gate._supervisor is args["supervisor"]
    assert cell.capture._store._enrollment is same_ledger
    cell.capture._check_custody()
    ledger, subject = object(), object()
    cell.capture._store._enrollment(ledger, subject)
    assert calls == ["host", (ledger, subject)]
    assert not args["root"].exists()
    assert "secret" not in repr(cell)


@pytest.mark.parametrize("field", ["files", "supervisor", "require_custody", "require_enrollment_in"])
def test_invalid_collaborator_rejects_before_credentials_or_source(tmp_path, monkeypatch, field):
    args = inputs(tmp_path, monkeypatch)
    args["parent"]["resolver"].resolve = lambda *_: pytest.fail("invalid inputs resolved credentials")
    monkeypatch.setattr(common, "clickhouse_http_endpoint", lambda *_: pytest.fail("invalid inputs read target"))
    args[field] = object()
    with pytest.raises(CompositionAdmissionError):
        common.build_protected_clickhouse_cell(**args)


def test_legacy_builder_imports_are_identical_reexports():
    from dpone.app import composition_clickhouse_execution_factory as legacy

    assert (
        legacy.build_composition_clickhouse_execution_dependencies
        is common.build_composition_clickhouse_execution_dependencies
    )
    assert legacy.build_composition_clickhouse_execution_root is common.build_composition_clickhouse_execution_root


@pytest.mark.parametrize("field", ["absolute_deadline", "require_effect", "source_connector_factory"])
def test_execution_collaborators_validate_before_resolving_secrets(tmp_path, monkeypatch, field):
    args = inputs(tmp_path, monkeypatch)
    args["parent"]["resolver"].resolve = lambda *_: pytest.fail("resolved source")
    monkeypatch.setattr(common, "clickhouse_http_endpoint", lambda *_: pytest.fail("read target secrets"))
    with pytest.raises(CompositionAdmissionError):
        common.build_protected_clickhouse_cell(**args, **{field: True})


def test_entire_http_graph_shares_deadline_and_effect_guard(tmp_path, monkeypatch):
    from dpone.adapters.composition_clickhouse_http import ClickHouseTransportCredentials

    calls = []

    def deadline():
        return 123.0

    def guard():
        calls.append("guard")

    connection = object()

    def factory(resolved, *, autocommit):
        assert autocommit is False
        calls.append("open")
        return SimpleNamespace(connection=connection)

    cell = common.build_protected_clickhouse_cell(
        **inputs(tmp_path, monkeypatch),
        absolute_deadline=deadline,
        require_effect=guard,
        source_connector_factory=factory,
    )
    deps = cell.dependencies
    for gate in (deps.gate, deps.publisher_gate):
        assert gate._admin._client._http._absolute_deadline is deadline
        assert gate._admin._client._require_effect is guard
    assert cell.capture._catalog._http._absolute_deadline is deadline
    transport = deps.bind_transport(ClickHouseTransportCredentials("worker", "test-secret"), object())
    assert transport._http._absolute_deadline is deadline
    assert transport._require_effect is guard
    assert calls == []
    assert deps.read_source._open() is connection
    assert calls == ["guard", "open"]
