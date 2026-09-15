"""Real callback/lookup composition stays lazy and validates both observations."""

from types import SimpleNamespace

import pytest

from dpone.app.native_originals_activation import build_native_originals_activation_callback
from dpone.contracts.dbt_workspace_activation import DbtWorkspaceActivationRequest, DbtWorkspaceActiveActivation
from dpone.contracts.runtime_connection import ResolvedBindingConnection, ResolvedConnectionDescriptor
from dpone.runtime.credentials.config import CredentialsConfig
from tests.test_dbt_workspace_activation_contract import _receipt, _resource
from tests.test_native_activation_lookup import UUID, Connection, D, coordinates, record


def setup_callback(tmp_path, monkeypatch, *, change=None, rows=None):
    identity, context = coordinates()
    calls = []
    credentials = CredentialsConfig(connect_timeout=0, query_timeout=0, additional_params={"LoginTimeout": 0})
    resolved = ResolvedBindingConnection(credentials, {}, ResolvedConnectionDescriptor("mssql", {}))

    class Inputs:
        def load_sources(self, **kwargs):
            calls.append("sources")
            return SimpleNamespace(release_id=D, inventory=SimpleNamespace(snapshot_sha256=D))

        def load_runtime_authority(self, **kwargs):
            calls.append("authority")
            return context

        def resolve_connection(self, authority, ref):
            calls.append("credentials")
            assert authority == context and ref == "control"
            return resolved

    connection = Connection([record()] if rows is None else rows)

    def create(binding, **kwargs):
        calls.append("connect")
        assert binding.credentials.connect_timeout == 7
        assert binding.credentials.query_timeout == 11
        assert binding.credentials.additional_params == {}
        assert kwargs == {"autocommit": False}
        return SimpleNamespace(connection=connection)

    monkeypatch.setattr("dpone.app.native_originals_activation.ResolvedConnectorFactory.create", create)

    class Coordinator:
        def require_active(self, **kwargs):
            calls.append("active")
            assert kwargs["previous_deployment_id"] == D
            resource = _resource()
            args = dict(
                activation_id=UUID,
                environment=context.environment,
                release_id=D,
                deployment_id=D,
                previous_deployment_id=D,
                source_inventory_sha256=D,
                runtime_context_sha256=context.authority_subject_sha256,
                write_subjects=resource.write_subjects,
                resources=(resource,),
            )
            if change:
                args[change[0]] = change[1]
            request = DbtWorkspaceActivationRequest.build(**args)
            return DbtWorkspaceActiveActivation(request, _receipt(request, "ACTIVE"))

    callback = build_native_originals_activation_callback(
        inputs=Inputs(),
        coordinator=Coordinator(),
        invocation_identity=identity,
        projection_root=tmp_path,
        environment=context.environment,
        authority_connection_ref="control",
        control_schema="dpone_control",
        connect_timeout_seconds=7,
        statement_timeout_seconds=11,
    )
    return callback, calls, credentials, connection


def test_production_callback_uses_real_lookup_and_independent_active_readback(tmp_path, monkeypatch):
    callback, calls, credentials, connection = setup_callback(tmp_path, monkeypatch)
    assert calls == []
    result = callback()
    assert result.request.activation_id == UUID
    assert calls == ["sources", "authority", "credentials", "connect", "active"]
    assert credentials.connect_timeout == 0 and credentials.query_timeout == 0
    assert credentials.additional_params == {"LoginTimeout": 0}
    assert connection.closed == 2


@pytest.mark.parametrize(
    "change",
    [
        ("activation_id", "22222222-2222-4222-8222-222222222222"),
        ("environment", "other"),
        ("release_id", "sha256:" + "b" * 64),
        ("deployment_id", "sha256:" + "b" * 64),
        ("previous_deployment_id", None),
        ("source_inventory_sha256", "sha256:" + "b" * 64),
        ("runtime_context_sha256", "sha256:" + "b" * 64),
    ],
)
def test_independent_readback_cannot_switch_pinned_or_preflight_subject(tmp_path, monkeypatch, change):
    callback, calls, _, _ = setup_callback(tmp_path, monkeypatch, change=change)
    with pytest.raises(ValueError, match="readback differs"):
        callback()
    assert calls[-1] == "active"


def test_missing_pinned_occurrence_cannot_call_coordinator(tmp_path, monkeypatch):
    callback, calls, _, _ = setup_callback(tmp_path, monkeypatch, rows=[])
    with pytest.raises(ValueError, match="missing"):
        callback()
    assert "active" not in calls
