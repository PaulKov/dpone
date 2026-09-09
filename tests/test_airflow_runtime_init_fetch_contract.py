from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import replace

import pytest

from dpone.runtime.init_fetch_contract import InitFetchError
from dpone.runtime.runtime_init_fetch_plan import (
    RuntimeArtifactDescriptor,
    RuntimeExecutionSelection,
    RuntimeInitFetchPlan,
    RuntimePayloadDescriptor,
    RuntimeWorkloadPackRef,
    canonical_runtime_init_fetch_plan_bytes,
    runtime_init_fetch_plan_sha256,
)
from dpone.runtime.runtime_init_fetch_plan_codec import decode_runtime_init_fetch_plan
from dpone.runtime.verified_pack_launcher import _selected_command

_RELEASE_ID = "sha256:" + "a" * 64
_DEPLOYMENT_ID = "sha256:" + "b" * 64
_IMAGE_DIGEST = "sha256:" + "c" * 64
_CONTEXT_DIR = "sha256-" + "d" * 64


def test_runtime_plan_carries_exact_six_artifacts_and_rejects_missing_or_extra_context() -> None:
    plan = _plan()

    assert plan.artifacts == (
        plan.deployment,
        plan.release,
        plan.workload_pack,
        plan.binding_set,
        plan.connection_registry,
        plan.credential_runtime,
    )
    payload = canonical_runtime_init_fetch_plan_bytes(plan)
    decoded, digest = decode_runtime_init_fetch_plan(
        base64.b64encode(payload).decode("ascii"),
        _sha(payload),
    )
    assert decoded == plan
    assert digest == runtime_init_fetch_plan_sha256(plan)

    for mutation in ("missing", "extra"):
        raw = plan.to_dict()
        if mutation == "missing":
            raw.pop("credential_runtime")
        else:
            raw["credential_runtime_body"] = {"vault": {"path": "must-not-cross-wire"}}
        with pytest.raises(InitFetchError) as exc:
            _decode(raw)
        assert exc.value.code == "DPONE_INIT_FETCH_PLAN_INVALID"


@pytest.mark.parametrize(
    ("artifact_ref", "expected_code"),
    [
        (
            "cache://deployments/dev/" + _DEPLOYMENT_ID.replace(":", "-") + "/connection-registry.ref",
            "DPONE_INIT_FETCH_PLAN_INVALID",
        ),
        (
            "cache://runtime-connection-contexts/current/connection-registry.json",
            "DPONE_CACHE_UNPINNED_REFERENCE",
        ),
    ],
)
def test_runtime_plan_rejects_legacy_or_mutable_connection_registry_authority(
    artifact_ref: str,
    expected_code: str,
) -> None:
    raw = _plan().to_dict()
    raw["connection_registry"]["artifact_ref"] = artifact_ref

    with pytest.raises(InitFetchError) as exc:
        _decode(raw)

    assert exc.value.code == expected_code


def test_ready_plan_digest_binds_each_runtime_connection_descriptor() -> None:
    plan = _plan()
    changed = replace(
        plan,
        connection_registry=replace(
            plan.connection_registry,
            sha256="sha256:" + "e" * 64,
        ),
    )

    assert runtime_init_fetch_plan_sha256(changed) != runtime_init_fetch_plan_sha256(plan)


@pytest.mark.parametrize("schema_version", ("v1", "v2"))
def test_decoded_legacy_plan_selects_the_only_verified_process(
    schema_version: str,
) -> None:
    plan = _decoded_legacy_plan(schema_version)
    pack = {
        "runtime_bootstrap": {
            "schema": "dpone.airflow-runtime-bootstrap.v1",
            "commands": {
                "customers": {
                    "argv": [
                        "dpone",
                        "run",
                        "runtime/orders.yaml",
                        "--format",
                        "json",
                        "--selector",
                        "customers",
                    ],
                    "env": {"DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS": "1"},
                }
            },
        },
        "process_plans": {
            "customers": {
                "selector": "customers",
                "dag_node": {"visibility": "inline"},
            }
        },
    }

    argv, environment = _selected_command(pack, plan)

    assert argv[-2:] == ("--selector", "customers")
    assert environment == {}


@pytest.mark.parametrize("schema_version", ("v1", "v2"))
def test_decoded_legacy_pre_hook_selects_the_only_verified_process(
    schema_version: str,
) -> None:
    plan = replace(
        _decoded_legacy_plan(schema_version),
        execution=RuntimeExecutionSelection(
            kind="pre_hook",
            selector="orders",
            hook_name="pre_hook_refresh_orders",
        ),
    )
    pack = {
        "steps": [],
        "process_plans": {
            "orders": {
                "selector": "orders",
                "steps": [_legacy_hook_step("orders")],
            }
        },
    }

    argv, environment = _selected_command(pack, plan)

    assert argv[-2:] == ("--selector", "orders")
    assert environment == {}


@pytest.mark.parametrize("schema_version", ("v1", "v2"))
def test_decoded_legacy_plan_rejects_every_multi_process_pack(
    schema_version: str,
) -> None:
    plan = _decoded_legacy_plan(schema_version)
    pack = {
        "runtime_bootstrap": {
            "schema": "dpone.airflow-runtime-bootstrap.v1",
            "commands": {},
        },
        "process_plans": {
            "orders": {
                "selector": "orders",
                "dag_node": {"visibility": "inline"},
            },
            "customers": {
                "selector": "customers",
                "dag_node": {"visibility": "inline"},
            },
        },
    }

    with pytest.raises(InitFetchError) as exc_info:
        _selected_command(pack, plan)

    assert exc_info.value.code == "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED"


@pytest.mark.parametrize("schema_version", ("v1", "v2"))
def test_decoded_legacy_pre_hook_rejects_unique_hook_in_multi_process_pack(
    schema_version: str,
) -> None:
    plan = replace(
        _decoded_legacy_plan(schema_version),
        execution=RuntimeExecutionSelection(
            kind="pre_hook",
            selector="orders",
            hook_name="pre_hook_refresh_orders",
        ),
    )
    pack = {
        "steps": [],
        "process_plans": {
            "orders": {
                "selector": "orders",
                "steps": [_legacy_hook_step("orders")],
            },
            "customers": {
                "selector": "customers",
                "steps": [_legacy_hook_step("customers", hook_id="refresh_customers")],
            },
        },
    }

    with pytest.raises(InitFetchError) as exc_info:
        _selected_command(pack, plan)

    assert exc_info.value.code == "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED"


def _plan() -> RuntimeInitFetchPlan:
    release_dir = _RELEASE_ID.replace(":", "-")
    deployment_dir = _DEPLOYMENT_ID.replace(":", "-")
    context_root = f"cache://runtime-connection-contexts/{_CONTEXT_DIR}"
    return RuntimeInitFetchPlan(
        environment="dev",
        trust_tier="non_production",
        release_id=_RELEASE_ID,
        deployment_id=_DEPLOYMENT_ID,
        runtime_image_ref=f"registry.example/dpone/runtime@{_IMAGE_DIGEST}",
        runtime_image_digest=_IMAGE_DIGEST,
        artifact_registry_ref="dpone-artifacts",
        registry_config_ref={
            "kind": "kubernetes_config_map",
            "name": "dpone-artifact-registry",
            "key": "registry.json",
            "sha256": "sha256:" + "1" * 64,
        },
        trust_policy_ref=None,
        identity={
            "method": "kubernetes_workload_identity",
            "service_account": "dpone-runtime",
            "namespace": "data-platform",
        },
        release=_artifact(
            f"cache://releases/{release_dir}/release-set.json",
            "2",
        ),
        deployment=_artifact(
            f"cache://deployments/dev/{deployment_dir}/deployment.json",
            "3",
        ),
        binding_set=_artifact(f"{context_root}/binding-set.json", "4"),
        connection_registry=_artifact(f"{context_root}/connection-registry.json", "5"),
        credential_runtime=_artifact(f"{context_root}/credential-runtime.json", "6"),
        workload_pack=RuntimeWorkloadPackRef(
            id="orders",
            artifact_ref=f"cache://releases/{release_dir}/packs/orders.json",
            sha256="sha256:" + "7" * 64,
            bytes=128,
            pack_fingerprint="sha256:" + "8" * 64,
        ),
        execution=RuntimeExecutionSelection(
            kind="runtime",
            selector="orders",
        ),
        verify={"checksums": "required", "attestations": "optional"},
    )


def _decoded_legacy_plan(schema_version: str) -> RuntimeInitFetchPlan:
    plan = _plan()
    if schema_version == "v2":
        release_dir = _RELEASE_ID.replace(":", "-")
        plan = replace(
            plan,
            runtime_payloads=(
                RuntimePayloadDescriptor(
                    id="dbt-manifest",
                    kind="dbt_manifest",
                    artifact_ref=(f"cache://releases/{release_dir}/payload/dbt-manifest.json"),
                    sha256="sha256:" + "9" * 64,
                    bytes=128,
                    media_type="application/json",
                ),
            ),
        )
    payload = canonical_runtime_init_fetch_plan_bytes(plan)
    decoded, _ = decode_runtime_init_fetch_plan(
        base64.b64encode(payload).decode("ascii"),
        _sha(payload),
    )
    assert decoded.to_dict()["schema"].endswith(f".{schema_version}")
    return decoded


def _artifact(artifact_ref: str, digest_character: str) -> RuntimeArtifactDescriptor:
    return RuntimeArtifactDescriptor(
        artifact_ref=artifact_ref,
        sha256="sha256:" + digest_character * 64,
        bytes=128,
    )


def _legacy_hook_step(
    selector: str,
    *,
    hook_id: str = "refresh_orders",
) -> dict[str, object]:
    argv = [
        "dpone",
        "hooks",
        "execute",
        "runtime/orders.yaml",
        "--phase",
        "pre_hook",
        "--hook-id",
        hook_id,
        "--selector",
        selector,
    ]
    return {
        "name": f"pre_hook_{hook_id}",
        "phase": "pre_hook",
        "command": " ".join(argv),
        "runtime_command": {
            "schema": "dpone.airflow-pre-hook-command.v1",
            "hook_id": hook_id,
            "process_selector": selector,
            "argv": argv,
        },
    }


def _decode(raw: dict[str, object]) -> None:
    payload = json.dumps(raw, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
    decode_runtime_init_fetch_plan(
        base64.b64encode(payload).decode("ascii"),
        _sha(payload),
    )


def _sha(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()
