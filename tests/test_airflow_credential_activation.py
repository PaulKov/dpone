"""Protected watcher authority is checked against actual sealed bytes."""

import json
from dataclasses import replace

import pytest

from dpone.readiness.airflow_credential_activation import require_activation_credential_authority
from dpone.runtime.deployment_cache_common import DeploymentCacheError
from tests.test_airflow_credential_projection_delivery import build_native_deployment


@pytest.mark.parametrize("drift", [None, "ref", "authority", "missing"])
def test_activation_requires_exact_projection_authority(tmp_path, drift):
    projection, cache, authority = build_native_deployment(tmp_path)
    if drift == "ref":
        authority = replace(authority, workspace_authority_connection_ref="another_control")
    if drift == "authority":
        authority = replace(authority, watcher_identity="another-watcher")
    kwargs = dict(
        deployment_dir=projection.deployment_dir,
        cache_root=cache,
        control_ref=authority.workspace_authority_connection_ref,
        authority_sha256=None if drift == "missing" else authority.publish_authority_sha256,
    )
    if drift is None:
        require_activation_credential_authority(projection.deployment, **kwargs)
    else:
        with pytest.raises(DeploymentCacheError, match="credentials"):
            require_activation_credential_authority(projection.deployment, **kwargs)


def test_workspace_lane_cannot_activate_legacy_deployment(tmp_path):
    with pytest.raises(DeploymentCacheError, match="credentials"):
        require_activation_credential_authority(
            {"schema": "dpone.deployment-set.v2"},
            deployment_dir=tmp_path,
            cache_root=tmp_path,
            control_ref="workspace_control",
            authority_sha256="sha256:" + "a" * 64,
        )


def test_non_workspace_legacy_activation_remains_compatible(tmp_path):
    require_activation_credential_authority(
        {"schema": "dpone.deployment-set.v2"},
        deployment_dir=tmp_path,
        cache_root=tmp_path,
        control_ref=None,
        authority_sha256=None,
    )


def test_current_pointer_control_drift_never_reports_healthy(tmp_path):
    from dpone.ports.airflow_desired_state import DesiredStateReconcilePortError
    from dpone.readiness.airflow_desired_state_reconcile import DeploymentCacheDesiredDeploymentActivator
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer
    from tests.test_dbt_workspace_cache_activation_saga import _Coordinator

    projection, cache, authority = build_native_deployment(tmp_path)
    coordinator = _Coordinator()
    DeploymentCacheMaterializer(cache, workspace_activation=coordinator).promote(
        projection.deployment_dir,
        environment="prod",
        workspace_authority_connection_ref=authority.workspace_authority_connection_ref,
    )
    activator = DeploymentCacheDesiredDeploymentActivator(
        cache_root=cache,
        promoted_by=authority.watcher_identity,
        workspace_activation=coordinator,
        workspace_authority_connection_ref=authority.workspace_authority_connection_ref,
        publish_authority_sha256=authority.publish_authority_sha256,
    )
    assert activator.current(environment="prod") is not None
    pointer_path = cache / "current-pointer.json"
    pointer = json.loads(pointer_path.read_bytes())
    pointer["workspace_authority_connection_ref"] = "another_control"
    pointer_path.write_text(json.dumps(pointer))
    with pytest.raises(DesiredStateReconcilePortError) as error:
        activator.current(environment="prod")
    assert error.value.code == "DPONE_RUNTIME_CREDENTIAL_PROJECTION_MISMATCH"
