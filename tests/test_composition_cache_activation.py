"""A distinct full-parent coordinator is required before v3 current activation."""

import argparse
from types import SimpleNamespace

import pytest

from dpone.commands import airflow_cache_sync_cmd
from dpone.contracts.composition_activation import CompositionActivationOccurrence, CompositionActivationReceipt
from dpone.contracts.release_composition import COMPOSITION_ADMISSION
from dpone.readiness import airflow_desired_state_reconcile, airflow_self_service_cache_sync
from dpone.readiness.airflow_self_service_models import SelfServiceResult
from dpone.runtime.deployment_cache_common import DeploymentCacheError
from dpone.runtime.deployment_cache_workspace_activation import DeploymentCacheWorkspaceActivation
from tests.test_composition_activation_contract import request


def coordinates(tmp_path):
    value = request()
    return dict(
        projection_root=tmp_path,
        dbt_wire=COMPOSITION_ADMISSION,
        activation_id=value.activation_id,
        environment=value.environment,
        release_id=value.release_id,
        deployment_id=value.deployment_id,
        previous_deployment_id=value.previous_deployment_id,
    )


def acknowledged(state):
    value = request()
    return CompositionActivationOccurrence(
        value, CompositionActivationReceipt(value.request_sha256, state, ((value.resources[0].guard_id, 1),))
    )


def test_parent_saga_uses_separate_coordinator_and_exact_active_receipt(tmp_path):
    calls = []

    def prepare(**kwargs):
        calls.append("parent_prepare")
        return acknowledged("PREPARED")

    def activate(prepared, **kwargs):
        calls.append("parent_activate")
        return acknowledged("ACTIVE")

    parent = SimpleNamespace(prepare=prepare, activate=activate, require_active=lambda **kwargs: acknowledged("ACTIVE"))
    saga = DeploymentCacheWorkspaceActivation(None, composition_coordinator=parent)
    prepared = saga.prepare_occurrence(**coordinates(tmp_path))
    saga.activate_occurrence(prepared, projection_root=tmp_path)
    saga.require_existing(**coordinates(tmp_path))
    assert calls == ["parent_prepare", "parent_activate"]


@pytest.mark.parametrize("result", [None, object(), SimpleNamespace(request=request(), __post_init__=lambda: None)])
def test_parent_admission_cannot_acknowledge_native_or_untyped_result(tmp_path, result):
    parent = SimpleNamespace(prepare=lambda **kwargs: result)
    saga = DeploymentCacheWorkspaceActivation(None, composition_coordinator=parent)
    with pytest.raises(DeploymentCacheError) as failure:
        saga.prepare_occurrence(**coordinates(tmp_path))
    assert failure.value.code == "DPONE_COMPOSITION_ADMISSION_UNAVAILABLE"


def test_post_pointer_failure_is_explicit_parent_commit_unknown(tmp_path):
    parent = SimpleNamespace(activate=lambda *args, **kwargs: acknowledged("PREPARED"))
    saga = DeploymentCacheWorkspaceActivation(None, composition_coordinator=parent)
    with pytest.raises(DeploymentCacheError) as failure:
        saga.activate_occurrence(acknowledged("PREPARED"), projection_root=tmp_path)
    assert failure.value.code == "DPONE_COMPOSITION_ACTIVATION_COMMIT_UNKNOWN"
    assert failure.value.details["state_may_have_changed"] is True


def test_cache_sync_builds_both_authority_coordinators_and_propagates_reference(tmp_path, monkeypatch):
    observed = {}
    workspace = object()
    composition = object()

    def build_workspace(**kwargs):
        observed["workspace_factory"] = kwargs
        return workspace

    def build_composition(**kwargs):
        observed["composition_factory"] = kwargs
        return composition

    monkeypatch.setattr(
        airflow_self_service_cache_sync,
        "build_deployment_cache_workspace_activation_coordinator",
        build_workspace,
    )
    monkeypatch.setattr(
        airflow_self_service_cache_sync,
        "build_composition_activation_coordinator",
        build_composition,
    )

    class Materializer:
        def __init__(self, cache_root, **kwargs):
            observed["materializer"] = (cache_root, kwargs)

        def promote(self, deployment_dir, **kwargs):
            observed["promote"] = (deployment_dir, kwargs)
            return SimpleNamespace(to_dict=lambda: {"deployment_id": "deployment"})

    monkeypatch.setattr(airflow_self_service_cache_sync, "DeploymentCacheMaterializer", Materializer)

    result = airflow_self_service_cache_sync.cache_sync_result(
        cache_root=tmp_path,
        deployment_dir=tmp_path / "deployment",
        environment="dev",
        promoted_by="ci",
        confirm_promote=True,
        allowed_promoters=("ci",),
        expect_current_absent=True,
        workspace_authority_connection_ref="composition_control",
    )

    assert result.passed is True
    assert observed["composition_factory"] == {
        "cache_root": tmp_path,
        "authority_connection_ref": "composition_control",
    }
    assert observed["materializer"][1] == {
        "allowed_promoters": ("ci",),
        "workspace_activation": workspace,
        "composition_activation_coordinator": composition,
    }
    assert observed["promote"][1]["workspace_authority_connection_ref"] == "composition_control"


def test_cache_sync_cli_accepts_and_propagates_authority_reference(monkeypatch):
    parser = argparse.ArgumentParser()
    command = parser.add_subparsers(dest="command", required=True)
    airflow_cache_sync_cmd.register_cache_sync_parser(command)
    args = parser.parse_args(
        [
            "cache-sync",
            "--deployment-dir",
            "deployment",
            "--environment",
            "dev",
            "--promoted-by",
            "ci",
            "--allowed-promoter",
            "ci",
            "--expect-current-absent",
            "--confirm-promote",
            "--workspace-authority-connection-ref",
            "composition_control",
        ]
    )
    observed = {}
    monkeypatch.setattr(
        airflow_cache_sync_cmd,
        "cache_sync_result",
        lambda **kwargs: observed.update(kwargs) or SelfServiceResult(passed=True),
    )
    monkeypatch.setattr(airflow_cache_sync_cmd, "emit_self_service_result", lambda *_args, **_kwargs: None)

    assert airflow_cache_sync_cmd.cmd_airflow_cache_sync(args, ctx=object(), logger=SimpleNamespace()) == 0
    assert observed["workspace_authority_connection_ref"] == "composition_control"


def test_cache_sync_rejects_invalid_authority_reference_before_materialization(tmp_path, monkeypatch):
    monkeypatch.setattr(
        airflow_self_service_cache_sync,
        "DeploymentCacheMaterializer",
        lambda *_args, **_kwargs: pytest.fail("invalid authority must not mutate cache"),
    )

    result = airflow_self_service_cache_sync.cache_sync_result(
        cache_root=tmp_path,
        deployment_dir=tmp_path / "deployment",
        environment="dev",
        promoted_by="ci",
        confirm_promote=True,
        allowed_promoters=("ci",),
        expect_current_absent=True,
        workspace_authority_connection_ref="",
    )

    assert result.passed is False
    assert result.errors[0]["code"] == "DPONE_DBT_WORKSPACE_ADMISSION_UNAVAILABLE"


def test_desired_state_activator_propagates_separate_parent_coordinator(tmp_path, monkeypatch):
    observed = {}
    workspace = object()
    composition = object()

    class Materializer:
        def __init__(self, cache_root, **kwargs):
            observed["materializer"] = (cache_root, kwargs)

    monkeypatch.setattr(airflow_desired_state_reconcile, "DeploymentCacheMaterializer", Materializer)

    airflow_desired_state_reconcile.DeploymentCacheDesiredDeploymentActivator(
        cache_root=tmp_path,
        promoted_by="watcher",
        workspace_activation=workspace,
        composition_activation=composition,
        workspace_authority_connection_ref="composition_control",
    )

    assert observed["materializer"][1] == {
        "allowed_promoters": ("watcher",),
        "workspace_activation": workspace,
        "composition_activation_coordinator": composition,
    }
