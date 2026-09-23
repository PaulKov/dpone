"""Native deployment -> init ready -> base admission, without database access."""

from __future__ import annotations

import pytest
from dpone_airflow_pack.credential_projection_contract import CredentialProjectionError
from dpone_airflow_pack.deployment_index_contract import load_airflow_deployment_index

from dpone.contracts.dbt_runtime import DBT_WORKSPACE_AUTHORITY_CONNECTION_REF_ENV
from dpone.runtime.init_fetch_contract import InitFetchError, cache_relative_path
from dpone.runtime.runtime_init_fetch_plan_codec import decode_runtime_init_fetch_plan
from dpone.runtime.runtime_init_fetch_service import RuntimeInitFetchExecutor
from dpone.runtime.verified_pack_launcher import VerifiedPackLauncher
from tests.test_airflow_credential_projection_delivery import build_native_deployment
from tests.test_airflow_runtime_init_fetch_cli import RecordingRegistry


@pytest.fixture
def native_ready(tmp_path):
    projection, cache, _authority = build_native_deployment(tmp_path)
    index = load_airflow_deployment_index(projection.deployment_dir / "airflow-index.json", cache_root=cache)
    context = index.delivery_context
    encoded = context.encode_plan(
        workload_id=context.workload_packs[0].id,
        execution_kind="runtime",
        execution_scope="workload",
        hook_execution="externalized",
    )
    plan, digest = decode_runtime_init_fetch_plan(encoded.base64, encoded.sha256)
    objects = {}
    deployment_files = {
        plan.credential_projection.artifact_ref: "credential-projection.json",
        plan.binding_set.artifact_ref: "binding-set.json",
        plan.connection_registry.artifact_ref: "connection-registry.ref",
        plan.credential_runtime.artifact_ref: "credential-runtime.ref",
    }
    for descriptor in plan.artifacts:
        key = cache_relative_path(descriptor.artifact_ref)
        path = (
            projection.deployment_dir / deployment_files[descriptor.artifact_ref]
            if descriptor.artifact_ref in deployment_files
            else cache / key
        )
        objects[key] = path.read_bytes()
    artifacts, worktree = tmp_path / "artifacts", tmp_path / "worktree"
    registry = RecordingRegistry(objects)
    executor = RuntimeInitFetchExecutor(
        registry=registry,
        artifact_root=artifacts,
        worktree_root=worktree,
    )
    ready = executor.execute(plan, plan_sha256=digest)
    calls = len(registry.calls)
    assert executor.execute(plan, plan_sha256=digest) == ready
    assert len(registry.calls) == calls
    return plan, digest, artifacts, VerifiedPackLauncher(artifact_root=artifacts, worktree_root=worktree)


def test_base_pins_verified_control_ref_after_init_success(native_ready):
    plan, digest, _artifacts, launcher = native_ready
    environment = {DBT_WORKSPACE_AUTHORITY_CONNECTION_REF_ENV: "workspace_control"}
    command = launcher.prepare(plan, plan_sha256=digest, runtime_environment=environment)
    environment[DBT_WORKSPACE_AUTHORITY_CONNECTION_REF_ENV] = "changed_after_prepare"
    assert command.env[DBT_WORKSPACE_AUTHORITY_CONNECTION_REF_ENV] == "workspace_control"
    assert command.argv[:3] == ("dpone", "dbt", "execute-pack")


def test_public_prepare_cannot_omit_v6_base_admission(native_ready):
    plan, digest, _artifacts, launcher = native_ready
    with pytest.raises(CredentialProjectionError):
        launcher.prepare(plan, plan_sha256=digest)
    assert launcher.validate_ready(plan, plan_sha256=digest) is None


@pytest.mark.parametrize("control_ref", [None, "other_control"])
def test_base_refuses_missing_or_mismatched_pointer_control(native_ready, control_ref):
    plan, digest, _artifacts, launcher = native_ready
    environment = {} if control_ref is None else {DBT_WORKSPACE_AUTHORITY_CONNECTION_REF_ENV: control_ref}
    with pytest.raises(CredentialProjectionError):
        launcher.prepare(plan, plan_sha256=digest, runtime_environment=environment)


def test_modified_projection_after_ready_fails_before_child(native_ready):
    plan, digest, artifacts, launcher = native_ready
    payload = artifacts / "payload" / cache_relative_path(plan.credential_projection.artifact_ref)
    payload.write_bytes(payload.read_bytes() + b" ")
    with pytest.raises(InitFetchError):
        launcher.prepare(
            plan,
            plan_sha256=digest,
            runtime_environment={DBT_WORKSPACE_AUTHORITY_CONNECTION_REF_ENV: "workspace_control"},
        )
