"""Offline source-provenance and bounded-closure tests for native deployment."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from copy import deepcopy

import pytest
from dpone_airflow_pack.credential_projection_contract import (
    CredentialProjectionError,
    canonical_projection_bytes,
    projection_descriptor,
)

from dpone.readiness.airflow_credential_projection import build_credential_projection, native_workload_requirements
from dpone.readiness.airflow_desired_state_authority import AirflowDesiredStateAuthority
from tests.test_native_deployment_credential_projection_gap import _native_case, _registry, _snapshots


def projection_case(*, shared: bool = False):
    """Return only synthetic metadata and exact snapshot bytes; no credentials."""
    registry = _registry(shared_connection=shared)
    snapshots = _snapshots(registry)
    authority = AirflowDesiredStateAuthority(
        environment="prod",
        desired_state_uri="s3://artifacts/desired/current.json",
        certified_s3_endpoint_url="https://objects.example.test",
        artifact_registry_uri="s3://artifacts/immutable",
        artifact_registry_ref="registry",
        watcher_identity="watcher",
        source_project="example/project",
        source_ref="main",
        workspace_authority_connection_ref="workspace_control",
    )
    return dict(
        environment="prod",
        release_id="sha256:" + "a" * 64,
        artifact_registry_ref="registry",
        requirements={"dbt__example": ("warehouse",)},
        binding_set=json.loads(snapshots["binding_set"]),
        source_registry=registry,
        snapshots=snapshots,
        authority=authority,
    )


@pytest.mark.parametrize("shared", [False, True])
def test_exact_source_keys_and_control_closure(shared):
    inputs = projection_case(shared=shared)
    before = deepcopy(inputs["source_registry"])
    result = build_credential_projection(**inputs)
    assert result is not None
    assert {row.connection_ref for row in result.membership("dbt__example")} == {"warehouse", "workspace_control"}
    assert {source.secret_key for source in result.sources} == (
        {"AIRFLOW_CONN_SQL_TARGET_LOGIN"}
        if shared
        else {"AIRFLOW_CONN_SQL_TARGET_LOGIN", "AIRFLOW_CONN_SQL_CONTROL_LOGIN"}
    )
    assert len({source.mount_path for source in result.sources}) == 2
    assert inputs["source_registry"] == before
    assert "password" not in canonical_projection_bytes(result.to_dict()).decode()


@pytest.mark.parametrize("ref", ["warehouse_runtime", "control_runtime"])
def test_exact_source_change_changes_projection_identity(ref):
    inputs = projection_case()
    original = build_credential_projection(**inputs)
    inputs["source_registry"]["connections"][ref]["credentials"]["connection_id"] = "replacement_login"
    changed = build_credential_projection(**inputs)
    assert projection_descriptor(canonical_projection_bytes(original.to_dict())) != projection_descriptor(
        canonical_projection_bytes(changed.to_dict())
    )


def test_no_native_requirements_preserve_legacy_lane():
    inputs = projection_case()
    inputs.update(requirements={}, authority=None)
    assert build_credential_projection(**inputs) is None


def test_native_requires_protected_authority():
    inputs = projection_case()
    inputs["authority"] = None
    with pytest.raises(CredentialProjectionError, match="required"):
        build_credential_projection(**inputs)


def test_native_source_reader_uses_execution_profile():
    pack, _kwargs, _context = _native_case()
    assert native_workload_requirements({pack["workload"]["workload_id"]: pack}) == {
        pack["workload"]["workload_id"]: ("warehouse",),
    }


@pytest.mark.parametrize("flow", [False, True])
def test_mixed_native_and_ordinary_closure_uses_canonical_processes(tmp_path, flow):
    from tests.test_release_composition_ordinary import ordinary_root

    root = ordinary_root(tmp_path, flow=flow, second_process=flow, sql_file=not flow)
    ordinary = json.loads((root / "orders/airflow-pack.json").read_bytes())
    native, _kwargs, _context = _native_case()
    refs = native_workload_requirements({native["workload"]["workload_id"]: native, "orders": ordinary})
    assert refs["orders"] == ("source", "target")
    assert refs[native["workload"]["workload_id"]] == ("warehouse",)


def test_mixed_flow_closure_ignores_ambient_source_registry(tmp_path):
    from tests.test_release_composition_ordinary import ordinary_root

    root = ordinary_root(tmp_path, flow=True)
    ordinary = json.loads((root / "orders/airflow-pack.json").read_bytes())
    native, _kwargs, _context = _native_case()
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import json,sys; from dpone.readiness.airflow_credential_projection import native_workload_requirements; print(json.dumps(native_workload_requirements(json.load(sys.stdin))))",
        ],
        input=json.dumps({native["workload"]["workload_id"]: native, "orders": ordinary}),
        capture_output=True,
        text=True,
        timeout=30,
        env={**os.environ, "DPONE_SOURCES_REGISTRY": str(tmp_path / "unavailable-registry.yaml")},
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["orders"] == ["source", "target"]


@pytest.mark.parametrize(
    "locator",
    [
        "registry: /unavailable.yaml",
        "registries: [/unavailable.yaml]",
        "convention: /unavailable.yaml",
        "conventions: [/unavailable.yaml]",
    ],
)
def test_resealed_runtime_manifest_cannot_add_external_metadata_locators(tmp_path, locator):
    from dpone_airflow_pack.pack_identity import compute_pack_fingerprint

    from tests.test_release_composition_ordinary import _replace_archive, ordinary_root

    root = ordinary_root(tmp_path)
    ordinary = json.loads((root / "orders/airflow-pack.json").read_bytes())
    payload = (tmp_path / "author/transfer.yaml").read_bytes() + (locator + "\n").encode()
    _replace_archive(ordinary, {"transfer.yaml": payload})
    ordinary["runtime_manifest"]["sha256"] = "sha256:" + hashlib.sha256(payload).hexdigest()
    ordinary["pack_fingerprint"] = compute_pack_fingerprint(ordinary)
    native, _kwargs, _context = _native_case()
    with pytest.raises(CredentialProjectionError, match="unsupported"):
        native_workload_requirements({native["workload"]["workload_id"]: native, "orders": ordinary})


@pytest.mark.parametrize("state", ["disabled", "reuse"])
def test_mixed_closure_does_not_add_disabled_or_reused_state(tmp_path, state):
    from tests.test_release_composition_ordinary import ordinary_root

    extra = "state:\n  type: disabled\n  connection_ref: unused\n" if state == "disabled" else "state:\n  reuse: sink\n"
    root = ordinary_root(tmp_path, extra_manifest=extra)
    ordinary = json.loads((root / "orders/airflow-pack.json").read_bytes())
    native, _kwargs, _context = _native_case()
    assert native_workload_requirements({native["workload"]["workload_id"]: native, "orders": ordinary})["orders"] == (
        "source",
        "target",
    )


def test_unused_registry_entry_not_mounted():
    inputs = projection_case()
    inputs["source_registry"]["connections"]["unused"] = deepcopy(
        inputs["source_registry"]["connections"]["warehouse_runtime"]
    )
    result = build_credential_projection(**inputs)
    assert {row.registry_ref for row in result.sources} == {"warehouse_runtime", "control_runtime"}


def test_normalized_source_key_collision_is_rejected():
    inputs = projection_case()
    inputs["source_registry"]["connections"]["warehouse_runtime"]["credentials"]["connection_id"] = "sql-login"
    inputs["source_registry"]["connections"]["control_runtime"]["credentials"]["connection_id"] = "sql_login"
    with pytest.raises(CredentialProjectionError):
        build_credential_projection(**inputs)
