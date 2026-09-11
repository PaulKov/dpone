"""A distinct full-parent coordinator is required before v3 current activation."""

from types import SimpleNamespace

import pytest

from dpone.contracts.composition_activation import CompositionActivationOccurrence, CompositionActivationReceipt
from dpone.contracts.release_composition import COMPOSITION_ADMISSION
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
