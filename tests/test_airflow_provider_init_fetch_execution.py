from __future__ import annotations

import base64
import hashlib
import inspect
import json
import os
from dataclasses import FrozenInstanceError
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import dpone_airflow_pack.dag_loader as dag_loader
import dpone_airflow_pack.pack_tasks as pack_tasks
import dpone_airflow_pack.provider as provider
import pytest
from dpone_airflow_pack.deployment_index import (
    AirflowDeploymentIndex,
    AirflowDeploymentIndexError,
    AirflowIndexArtifact,
    load_airflow_deployment_index,
)
from dpone_airflow_pack.init_fetch_contract import (
    InitFetchDeliveryContext,
    InitFetchProviderError,
    init_fetch_context_from_payload,
)
from dpone_airflow_pack.init_fetch_pod import compose_init_fetch_operator_kwargs
from dpone_airflow_pack.init_fetch_pod_contract import ALLOWED_PACK_ENV
from dpone_airflow_pack.pack_identity import (
    PACK_IDENTITY_SCHEMA,
    compute_pack_fingerprint,
)

RELEASE_ID = "sha256:" + "a" * 64
DEPLOYMENT_ID = "sha256:" + "b" * 64
IMAGE_DIGEST = "sha256:" + "c" * 64
PACK_SHA256 = "sha256:" + "d" * 64
LEGACY_PACK_FINGERPRINT = "sha256:" + "e" * 64
XCOM_IMAGE = "registry.example/airflow/xcom@sha256:" + "f" * 64
RELEASE_DIR = RELEASE_ID.replace(":", "-")
DEPLOYMENT_DIR = DEPLOYMENT_ID.replace(":", "-")
IMAGE_REF = f"registry.example/dpone/runtime@{IMAGE_DIGEST}"
RUNTIME_CONNECTION_CONTEXT_DIR = "sha256-" + "9" * 64


def _config_map_ref(name: str) -> dict[str, str]:
    return {
        "kind": "kubernetes_config_map",
        "name": f"dpone-{name}",
        "key": f"{name}.json",
        "sha256": "sha256:" + ("1" if name == "registry" else "2") * 64,
    }


def _runtime_connection_artifact(name: str, digest_character: str) -> dict[str, str | int]:
    return {
        "artifact_ref": (f"cache://runtime-connection-contexts/{RUNTIME_CONNECTION_CONTEXT_DIR}/{name}.json"),
        "sha256": "sha256:" + digest_character * 64,
        "bytes": 1024,
    }


def _v2_payload(*, trust_policy: bool = True) -> dict[str, Any]:
    delivery: dict[str, Any] = {
        "mode": "init_fetch",
        "trust_tier": "production",
        "artifact_registry_ref": "dpone-prod-artifacts",
        "identity": {
            "method": "kubernetes_workload_identity",
            "service_account": "dpone-runtime",
            "namespace": "data-platform",
        },
        "registry_config_ref": _config_map_ref("registry"),
        "source": {"artifact_registry_ref": "dpone-prod-artifacts"},
        "verify": {"checksums": "required", "attestations": "required_for_prod"},
    }
    if trust_policy:
        delivery["trust_policy_ref"] = _config_map_ref("policy")
    else:
        delivery["trust_tier"] = "non_production"
        delivery["verify"]["attestations"] = "optional"
    payload = {
        "schema": "dpone.airflow-deployment-index.v2",
        "trust_tier": delivery["trust_tier"],
        "release_id": RELEASE_ID,
        "deployment_id": DEPLOYMENT_ID,
        "runtime_image_ref": IMAGE_REF,
        "runtime_image_digest": IMAGE_DIGEST,
        "binding_set_ref": "sha256:" + "6" * 64,
        "connection_registry_ref": "sha256:" + "7" * 64,
        "credential_runtime_ref": "sha256:" + "8" * 64,
        "binding_set": _runtime_connection_artifact("binding-set", "5"),
        "connection_registry": _runtime_connection_artifact("connection-registry", "6"),
        "credential_runtime": _runtime_connection_artifact("credential-runtime", "7"),
        "airflow_bundle_ref": "git:7ac31f2",
        "runtime_artifact_delivery": delivery,
        "release": {
            "artifact_ref": f"cache://releases/{RELEASE_DIR}/release-set.json",
            "sha256": "sha256:" + "3" * 64,
            "bytes": 4096,
        },
        "deployment": {
            "artifact_ref": f"cache://deployments/prod/{DEPLOYMENT_DIR}/deployment.json",
            "sha256": "sha256:" + "4" * 64,
            "bytes": 2048,
        },
        "dag_specs": [],
        "workload_packs": [
            {
                "id": "orders",
                "artifact_ref": f"cache://releases/{RELEASE_DIR}/packs/orders.json",
                "sha256": PACK_SHA256,
                "bytes": 8192,
                "pack_fingerprint": PACK_FINGERPRINT,
            }
        ],
    }
    return payload


def _legacy_pack() -> dict[str, Any]:
    return {
        "kind": "gitops.airflow_pack",
        "pack_fingerprint": LEGACY_PACK_FINGERPRINT,
        "workload": {"workload_id": "orders"},
        "runtime_command": "scheduler-shell-command --must-not-run",
        "kpo_kwargs": {
            "task_id": "orders__dpone_runtime",
            "name": "dpone-orders",
            "namespace": "pack-namespace",
            "image": "registry.example/pack:mutable",
            "cmds": ["/bin/sh", "-ec"],
            "arguments": ["scheduler-kpo-command --must-not-run"],
            "env_vars": {"DPONE_DAG_ID": "{{ dag.dag_id }}"},
        },
        "pod_spec": {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "dpone-orders", "namespace": "pack-namespace"},
            "spec": {
                "restartPolicy": "Never",
                "serviceAccountName": "pack-service-account",
                "nodeSelector": {"workload": "etl"},
                "containers": [
                    {
                        "name": "base",
                        "image": "registry.example/pack:mutable",
                        "command": ["/bin/sh", "-ec"],
                        "args": ["scheduler-pod-command --must-not-run"],
                        "resources": {"requests": {"cpu": "1"}},
                        "volumeMounts": [
                            {
                                "name": "dpone-worktree",
                                "mountPath": "/workspace",
                                "readOnly": False,
                            }
                        ],
                    }
                ],
                "initContainers": [
                    {
                        "name": "dpone-inline-workload-bootstrap",
                        "command": ["/bin/sh", "-ec"],
                        "args": ["inline-bootstrap --must-not-run"],
                    }
                ],
                "volumes": [{"name": "dpone-worktree", "emptyDir": {}}],
            },
        },
        "steps": [],
    }


def _strict_pack() -> dict[str, Any]:
    pack = _legacy_pack()
    pack["provider_execution"] = {
        "schema": "dpone.airflow-provider-execution.v1",
        "kpo_kwargs": {
            "task_id": "orders__dpone_runtime",
            "name": "dpone-orders",
            "labels": {"dpone.dev/workload-id": "orders"},
            "env_vars": {"DPONE_DAG_ID": "{{ dag.dag_id }}"},
        },
        "pod_spec": {
            "spec": {
                "nodeSelector": {"workload": "etl"},
                "containers": [
                    {
                        "name": "base",
                        "resources": {"requests": {"cpu": "1"}},
                    }
                ],
            },
        },
    }
    pack["xcom"] = {"sidecar_image": XCOM_IMAGE}
    pack["pack_identity"] = {"schema": PACK_IDENTITY_SCHEMA}
    pack["pack_fingerprint"] = compute_pack_fingerprint(pack)
    return pack


PACK_FINGERPRINT = str(_strict_pack()["pack_fingerprint"])


def _decoded_plan(env_vars: dict[str, Any]) -> tuple[dict[str, Any], bytes]:
    payload = base64.b64decode(env_vars["DPONE_INIT_FETCH_PLAN_B64"], validate=True)
    return json.loads(payload), payload


def _write_index(tmp_path: Path, payload: dict[str, Any]) -> Path:
    path = tmp_path / ".dpone-cache/deployments/prod/sha256-deployment/airflow-index.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    (tmp_path / ".dpone-cache/.promotion.lock").touch(mode=0o644, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_wire_v1_preserves_local_preview_and_rejects_init_fetch_before_dag_install(
    tmp_path: Path,
) -> None:
    local_preview = {
        "schema": "dpone.airflow-deployment-index.v1",
        "release_id": RELEASE_ID,
        "deployment_id": DEPLOYMENT_ID,
        "dag_specs": [],
        "workload_packs": [],
        "runtime_artifact_delivery": {"mode": "local_preview"},
    }

    index = load_airflow_deployment_index(_write_index(tmp_path, local_preview))

    assert index.schema == "dpone.airflow-deployment-index.v1"
    assert index.delivery_context is None

    local_preview["runtime_artifact_delivery"] = {"mode": "init_fetch"}
    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_airflow_deployment_index(_write_index(tmp_path, local_preview))

    assert exc_info.value.code == "DPONE_RUNTIME_ARTIFACT_DELIVERY_MIGRATION_REQUIRED"


def test_v1_init_fetch_loader_is_fatal_before_globals_mutation(tmp_path: Path) -> None:
    payload = {
        "schema": "dpone.airflow-deployment-index.v1",
        "release_id": RELEASE_ID,
        "deployment_id": DEPLOYMENT_ID,
        "dag_specs": [],
        "workload_packs": [],
        "runtime_artifact_delivery": {"mode": "init_fetch"},
    }
    index_path = _write_index(tmp_path, payload)
    sentinel = object()
    globals_dict = {"sentinel": sentinel}

    report = dag_loader.load_dpone_dags(globals_dict, index_path=index_path)

    assert report.fatal is True
    assert report.loaded == ()
    assert report.errors[0]["code"] == ("DPONE_RUNTIME_ARTIFACT_DELIVERY_MIGRATION_REQUIRED")
    assert globals_dict == {"sentinel": sentinel}


@pytest.mark.parametrize("mode", ["local_preview", "shared_pvc", "embedded_bundle", "csi_volume", "inline"])
def test_wire_v2_rejects_every_known_non_init_fetch_mode(tmp_path: Path, mode: str) -> None:
    payload = _v2_payload()
    payload["runtime_artifact_delivery"] = {"mode": mode}

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_airflow_deployment_index(_write_index(tmp_path, payload))

    assert exc_info.value.code == "DPONE_RUNTIME_ARTIFACT_DELIVERY_MODE_UNSUPPORTED"


def test_wire_v2_rejects_unknown_delivery_mode(tmp_path: Path) -> None:
    payload = _v2_payload()
    payload["runtime_artifact_delivery"] = {"mode": "teleport"}

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_airflow_deployment_index(_write_index(tmp_path, payload))

    assert exc_info.value.code == "DPONE_AIRFLOW_INDEX_FIELD_INVALID"


@pytest.mark.parametrize("mutation", ["tier_mismatch", "secret_ref", "unknown_field"])
def test_wire_v2_rejects_noncanonical_or_secret_projection(mutation: str) -> None:
    payload = _v2_payload()
    delivery = payload["runtime_artifact_delivery"]
    if mutation == "tier_mismatch":
        payload["trust_tier"] = "non_production"
    elif mutation == "secret_ref":
        delivery["artifact_registry_ref"] = "sk-proj-abcdefghijklmnop"
        delivery["source"]["artifact_registry_ref"] = "sk-proj-abcdefghijklmnop"
    else:
        delivery["credential"] = "must-not-serialize"

    with pytest.raises(InitFetchProviderError) as exc_info:
        init_fetch_context_from_payload(payload)

    assert exc_info.value.code == "DPONE_AIRFLOW_INDEX_FIELD_INVALID"


@pytest.mark.parametrize("port", [1, 443, 65535])
def test_wire_v2_accepts_valid_oci_registry_ports(port: int) -> None:
    payload = _v2_payload()
    payload["runtime_image_ref"] = f"registry.example:{port}/dpone/runtime@{IMAGE_DIGEST}"

    context = init_fetch_context_from_payload(payload)

    assert context.runtime_image_ref == payload["runtime_image_ref"]


@pytest.mark.parametrize("port", [0, 65536, 99999])
def test_wire_v2_rejects_out_of_range_oci_registry_ports(port: int) -> None:
    payload = _v2_payload()
    payload["runtime_image_ref"] = f"registry.example:{port}/dpone/runtime@{IMAGE_DIGEST}"

    with pytest.raises(InitFetchProviderError) as exc_info:
        init_fetch_context_from_payload(payload)

    assert exc_info.value.code == "DPONE_AIRFLOW_INDEX_FIELD_INVALID"


@pytest.mark.parametrize("trust_policy", [True, False])
def test_v2_plan_is_canonical_bounded_hash_pinned_and_matches_config_mounts(
    trust_policy: bool,
) -> None:
    context = init_fetch_context_from_payload(_v2_payload(trust_policy=trust_policy))
    pack = _strict_pack()
    pack["provider_execution"]["kpo_kwargs"]["labels"].update(
        {
            "dpone.dev/managed-by": "legacy-author",
            "dpone.dev/runtime-contract": "legacy-v1",
        }
    )
    kwargs = compose_init_fetch_operator_kwargs(
        pack=pack,
        kwargs=pack["provider_execution"]["kpo_kwargs"],
        context=context,
        workload_id="orders",
        execution_kind="runtime",
        execution_scope="workload",
        hook_execution="externalized",
    )

    plan, canonical = _decoded_plan(kwargs["env_vars"])
    assert (
        canonical
        == json.dumps(
            plan,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    )
    assert len(canonical) <= 16 * 1024
    assert kwargs["env_vars"]["DPONE_INIT_FETCH_PLAN_SHA256"] == ("sha256:" + hashlib.sha256(canonical).hexdigest())
    assert plan["trust_tier"] == ("production" if trust_policy else "non_production")
    assert plan["trust_policy"] == (_config_map_ref("policy") if trust_policy else None)
    assert plan["registry"]["configuration"] == _config_map_ref("registry")
    assert {field: plan[field] for field in ("binding_set", "connection_registry", "credential_runtime")} == {
        field: _v2_payload(trust_policy=trust_policy)[field]
        for field in ("binding_set", "connection_registry", "credential_runtime")
    }
    assert b"vault" not in canonical
    assert b"connections" not in canonical
    assert plan["execution"] == {
        "kind": "runtime",
        "selector": "orders",
        "scope": "workload",
        "process_selector": None,
        "hook_name": None,
        "hook_execution": "externalized",
    }
    if os.environ.get("DPONE_AIRFLOW_SCHEDULER_ONLY") != "1":
        from dpone.runtime.runtime_init_fetch_plan_codec import (
            decode_runtime_init_fetch_plan,
        )

        decoded, decoded_sha256 = decode_runtime_init_fetch_plan(
            kwargs["env_vars"]["DPONE_INIT_FETCH_PLAN_B64"],
            kwargs["env_vars"]["DPONE_INIT_FETCH_PLAN_SHA256"],
        )
        assert decoded.to_dict() == plan
        assert decoded_sha256 == kwargs["env_vars"]["DPONE_INIT_FETCH_PLAN_SHA256"]

    pod = kwargs["full_pod_spec"]
    base = pod["spec"]["containers"][0]
    init = pod["spec"]["initContainers"][0]
    assert kwargs["image"] == IMAGE_REF
    assert kwargs["cmds"] == ["dpone", "airflow", "runtime-pack-exec"]
    assert kwargs["arguments"] == []
    assert base["image"] == init["image"] == IMAGE_REF
    assert base["command"] == ["dpone", "airflow", "runtime-pack-exec"]
    assert init["command"] == ["dpone", "airflow", "runtime-init-fetch"]
    assert base["args"] == init["args"] == []
    base_env = {item["name"]: item["value"] for item in base["env"]}
    init_env = {item["name"]: item["value"] for item in init["env"]}
    assert init_env["DPONE_INIT_FETCH_PLAN_B64"] == base_env["DPONE_INIT_FETCH_PLAN_B64"]
    assert init_env["DPONE_INIT_FETCH_PLAN_SHA256"] == base_env["DPONE_INIT_FETCH_PLAN_SHA256"]
    assert init_env == base_env
    assert kwargs["namespace"] == "data-platform"
    assert kwargs["service_account_name"] == "dpone-runtime"
    assert kwargs["labels"] == {
        "dpone.dev/managed-by": "airflow-provider",
        "dpone.dev/runtime-contract": "init-fetch-v2",
        "dpone.dev/workload-id": "orders",
    }
    assert pod["metadata"]["labels"] == kwargs["labels"]
    assert pod["metadata"]["namespace"] == "data-platform"
    assert pod["spec"]["serviceAccountName"] == "dpone-runtime"
    assert pod["spec"]["nodeSelector"] == {"workload": "etl"}
    assert base["resources"] == {"requests": {"cpu": "1"}}
    serialized = json.dumps(kwargs, default=str)
    assert "scheduler-" not in serialized
    assert "inline-bootstrap" not in serialized

    config_volumes = {item["name"]: item["configMap"] for item in pod["spec"]["volumes"] if "configMap" in item}
    assert config_volumes["dpone-artifact-registry-config"]["name"] == (plan["registry"]["configuration"]["name"])
    init_mounts = {item["name"]: item for item in init["volumeMounts"]}
    base_mounts = {item["name"]: item for item in base["volumeMounts"]}
    assert init_mounts["dpone-artifact-registry-config"]["mountPath"] == ("/etc/dpone/artifact-registry")
    assert config_volumes["dpone-artifact-registry-config"]["items"] == [
        {"key": plan["registry"]["configuration"]["key"], "path": "registry.json"}
    ]
    assert init_mounts["dpone-fetched-artifacts"]["mountPath"] == "/var/lib/dpone/artifacts"
    assert init_mounts["dpone-worktree"]["mountPath"] == "/workspace/repo"
    assert base_mounts["dpone-fetched-artifacts"] == {
        "name": "dpone-fetched-artifacts",
        "mountPath": "/var/lib/dpone/artifacts",
        "readOnly": True,
    }
    assert base_mounts["dpone-worktree"] == {
        "name": "dpone-worktree",
        "mountPath": "/workspace/repo",
        "readOnly": True,
    }
    assert base_mounts["dpone-run-output"]["mountPath"] == "/var/lib/dpone/run"
    if trust_policy:
        assert config_volumes["dpone-artifact-trust-policy"]["name"] == (plan["trust_policy"]["name"])
        assert init_mounts["dpone-artifact-trust-policy"]["mountPath"] == ("/etc/dpone/artifact-trust")
        assert "items" not in config_volumes["dpone-artifact-trust-policy"]
        assert init_mounts["dpone-artifact-trust-policy"]["readOnly"] is True
    else:
        assert "dpone-artifact-trust-policy" not in config_volumes

    with pytest.raises(FrozenInstanceError):
        context.trust_tier = "non_production"  # type: ignore[misc]


def test_wire_v2_requires_canonical_trust_policy_config_map_key() -> None:
    payload = _v2_payload()
    payload["runtime_artifact_delivery"]["trust_policy_ref"]["key"] = "trust.json"

    with pytest.raises(InitFetchProviderError) as exc:
        init_fetch_context_from_payload(payload)

    assert exc.value.code == "DPONE_AIRFLOW_INDEX_FIELD_INVALID"
    assert "policy.json" in str(exc.value)


def test_dbt_runtime_payloads_are_selected_and_pinned_in_plan_v3() -> None:
    """Provider-side projection must pin dbt payloads without importing core dpone.

    Airflow pack compatibility CI installs only the scheduler-light provider and
    asserts ``dpone`` is absent; round-trip decode of the plan lives in core
    runtime tests.
    """

    payload = _v2_payload()
    payload["runtime_payloads"] = [
        {
            "id": "daily_marts_project",
            "kind": "dbt_project_bundle",
            "artifact_ref": (f"cache://releases/{RELEASE_DIR}/runtime/dbt/daily_marts.project.tar.gz"),
            "sha256": "sha256:" + "8" * 64,
            "bytes": 65_536,
            "media_type": "application/vnd.dpone.dbt-project-bundle+gzip",
        },
        {
            "id": "daily_marts_selection",
            "kind": "dbt_selection_lock",
            "artifact_ref": (f"cache://releases/{RELEASE_DIR}/runtime/dbt/daily_marts.selection-lock.json"),
            "sha256": "sha256:" + "9" * 64,
            "bytes": 512,
            "media_type": "application/vnd.dpone.dbt-selection-lock+json",
        },
    ]
    payload["workload_packs"][0]["runtime_payload_ids"] = [
        "daily_marts_project",
        "daily_marts_selection",
    ]

    context = init_fetch_context_from_payload(payload)
    encoded = context.encode_plan(
        workload_id="orders",
        execution_kind="runtime",
        execution_scope="workload",
        hook_execution="externalized",
    )
    plan = json.loads(encoded.payload)

    assert plan["schema"] == "dpone.airflow-runtime-init-fetch-plan.v3"
    assert plan["execution"]["hook_execution"] == "externalized"
    assert [item["id"] for item in plan["runtime_payloads"]] == [
        "daily_marts_project",
        "daily_marts_selection",
    ]
    assert [item["kind"] for item in plan["runtime_payloads"]] == [
        "dbt_project_bundle",
        "dbt_selection_lock",
    ]
    assert all(str(item["sha256"]).startswith("sha256:") for item in plan["runtime_payloads"])
    payload_bytes = encoded.payload if isinstance(encoded.payload, bytes) else encoded.payload.encode("utf-8")
    assert hashlib.sha256(payload_bytes).hexdigest() == encoded.sha256.removeprefix("sha256:")
    assert base64.b64decode(encoded.base64) == payload_bytes


def test_provider_plan_v3_carries_exact_inline_process_hook_ownership() -> None:
    context = init_fetch_context_from_payload(_v2_payload())

    encoded = context.encode_plan(
        workload_id="orders",
        execution_kind="runtime",
        execution_scope="process",
        process_selector="dbo.orders",
        hook_execution="inline",
    )
    plan = json.loads(encoded.payload)

    assert plan["schema"] == "dpone.airflow-runtime-init-fetch-plan.v3"
    assert plan["execution"] == {
        "kind": "runtime",
        "selector": "orders",
        "scope": "process",
        "process_selector": "dbo.orders",
        "hook_name": None,
        "hook_execution": "inline",
    }


def test_provider_contract_requires_explicit_hook_ownership() -> None:
    encode_parameters = inspect.signature(
        InitFetchDeliveryContext.encode_plan,
    ).parameters
    compose_parameters = inspect.signature(
        compose_init_fetch_operator_kwargs,
    ).parameters

    for field in ("execution_scope", "hook_execution"):
        assert encode_parameters[field].default is inspect.Parameter.empty
        assert compose_parameters[field].default is inspect.Parameter.empty


def test_v2_pod_forces_zero_retries_when_no_retry_is_configured() -> None:
    context = init_fetch_context_from_payload(_v2_payload())
    pack = _strict_pack()

    kwargs = compose_init_fetch_operator_kwargs(
        pack=pack,
        kwargs=pack["provider_execution"]["kpo_kwargs"],
        context=context,
        workload_id="orders",
        execution_kind="runtime",
        execution_scope="workload",
        hook_execution="externalized",
    )

    assert kwargs["retries"] == 0


def test_dbt_dev_evidence_delivery_is_deployment_owned_and_bounded() -> None:
    payload = _v2_payload(trust_policy=False)
    payload["dev_evidence_delivery"] = {
        "mode": "shared_pvc",
        "claim_name": "dpone-dbt-dev-evidence",
        "mount_path": "/var/lib/dpone/dev-evidence",
        "worker_queue": "dpone_evidence_export",
    }
    payload["workload_packs"][0]["id"] = "dbt__daily_marts"
    pack = _strict_pack()
    pack["workload"]["workload_id"] = "dbt__daily_marts"
    pack["provider_execution"]["kpo_kwargs"]["task_id"] = "dbt__daily_marts__dpone_runtime"
    pack["provider_execution"]["kpo_kwargs"]["name"] = "dpone-dbt--daily-marts"
    pack["provider_execution"]["kpo_kwargs"]["labels"] = {"dpone.dev/workload-id": "dbt__daily_marts"}
    pack["kpo_kwargs"]["task_id"] = "dbt__daily_marts__dpone_runtime"
    pack["workload"]["workload_id"] = "dbt__daily_marts"
    pack["pack_fingerprint"] = compute_pack_fingerprint(pack)
    payload["workload_packs"][0]["pack_fingerprint"] = pack["pack_fingerprint"]

    context = init_fetch_context_from_payload(payload)
    kwargs = compose_init_fetch_operator_kwargs(
        pack=pack,
        kwargs=pack["provider_execution"]["kpo_kwargs"],
        context=context,
        workload_id="dbt__daily_marts",
        execution_kind="runtime",
        execution_scope="workload",
        hook_execution="externalized",
    )

    assert context.dev_evidence_delivery is not None
    assert context.dev_evidence_delivery.worker_queue == "dpone_evidence_export"
    assert kwargs["env_vars"]["DPONE_DBT_EVIDENCE_EXPORT_ROOT"] == ("/var/lib/dpone/dev-evidence-dbt")
    assert "dpone_evidence_authority" in kwargs["env_vars"]["DPONE_DBT_EVIDENCE_SET_ID"]
    pod = kwargs["full_pod_spec"]
    volume = next(item for item in pod["spec"]["volumes"] if item["name"] == "dpone-dev-evidence")
    assert volume["persistentVolumeClaim"]["claimName"] == ("dpone-dbt-dev-evidence")
    base_mounts = {item["name"]: item for item in pod["spec"]["containers"][0]["volumeMounts"]}
    assert base_mounts["dpone-dev-evidence"] == {
        "name": "dpone-dev-evidence",
        "mountPath": "/var/lib/dpone/dev-evidence-dbt",
        "readOnly": False,
        "subPath": "dbt-spool",
    }
    init_container = pod["spec"]["initContainers"][0]
    init_mounts = {item["name"]: item for item in init_container["volumeMounts"]}
    assert init_mounts["dpone-dev-evidence"] == {
        "name": "dpone-dev-evidence",
        "mountPath": "/var/lib/dpone/dev-evidence-bootstrap",
        "readOnly": False,
    }
    init_env = {item["name"]: item["value"] for item in init_container["env"]}
    assert init_env["DPONE_DBT_EVIDENCE_BOOTSTRAP_ROOT"] == ("/var/lib/dpone/dev-evidence-bootstrap")


def test_init_fetch_injects_repair_authority_ref_from_dag_run_conf() -> None:
    """Scheduler compose owns repair-authority env; packs cannot bake the ID."""

    context = init_fetch_context_from_payload(_v2_payload())
    pack = _strict_pack()
    kwargs = compose_init_fetch_operator_kwargs(
        pack=pack,
        kwargs=pack["provider_execution"]["kpo_kwargs"],
        context=context,
        workload_id="orders",
        execution_kind="runtime",
        execution_scope="workload",
        hook_execution="externalized",
    )

    template = kwargs["env_vars"]["DPONE_REPAIR_AUTHORITY_REF"]
    assert "DPONE_REPAIR_AUTHORITY_REFS" in template
    assert "ti.task_id" in template
    assert "dag_run.conf.get('DPONE_REPAIR_AUTHORITY_REF'" in template
    assert "DPONE_REPAIR_AUTHORITY_REF" not in ALLOWED_PACK_ENV

    base_env = {item["name"]: item["value"] for item in kwargs["full_pod_spec"]["spec"]["containers"][0]["env"]}
    assert base_env["DPONE_REPAIR_AUTHORITY_REF"] == template


def test_init_fetch_rejects_pack_provided_repair_authority_ref() -> None:
    pack = _strict_pack()
    kwargs = dict(pack["provider_execution"]["kpo_kwargs"])
    kwargs["env_vars"] = {
        **kwargs["env_vars"],
        "DPONE_REPAIR_AUTHORITY_REF": "baked-standing-id",
    }

    with pytest.raises(InitFetchProviderError) as raised:
        compose_init_fetch_operator_kwargs(
            pack=pack,
            kwargs=kwargs,
            context=init_fetch_context_from_payload(_v2_payload()),
            workload_id="orders",
            execution_kind="runtime",
            execution_scope="workload",
            hook_execution="externalized",
        )

    assert raised.value.code == "DPONE_INIT_FETCH_RESERVED_COLLISION"


def test_dev_evidence_delivery_is_rejected_for_production() -> None:
    payload = _v2_payload()
    payload["dev_evidence_delivery"] = {
        "mode": "shared_pvc",
        "claim_name": "dpone-dbt-dev-evidence",
        "mount_path": "/var/lib/dpone/dev-evidence",
        "worker_queue": "dpone_evidence_export",
    }

    with pytest.raises(InitFetchProviderError) as raised:
        init_fetch_context_from_payload(payload)
    assert raised.value.code == "DPONE_AIRFLOW_INDEX_FIELD_INVALID"


@pytest.mark.parametrize("execution_kind", ["runtime", "pre_hook"])
def test_v2_pod_requests_provider_native_success_cleanup(
    execution_kind: str,
) -> None:
    """KPO streams base logs after pod start and requests provider-native cleanup."""

    context = init_fetch_context_from_payload(_v2_payload())
    pack = _strict_pack()

    kwargs = compose_init_fetch_operator_kwargs(
        pack=pack,
        kwargs=pack["provider_execution"]["kpo_kwargs"],
        context=context,
        workload_id="orders",
        execution_kind=execution_kind,
        execution_scope="workload",
        hook_execution="externalized",
        hook_name="prepare_source" if execution_kind == "pre_hook" else None,
    )

    assert kwargs["get_logs"] is True
    assert kwargs["on_finish_action"] == "delete_succeeded_pod"


def test_strict_init_fetch_enables_live_base_container_log_streaming() -> None:
    """Regression guard: get_logs must stay True so Airflow follows `base` stdout.

    With get_logs=False, cncf-kubernetes KPO calls await_container_completion and
    the Airflow UI only shows repeating
    ``Waiting for container 'base' state to be completed`` instead of colorful
    streamed runtime logs.
    """

    context = init_fetch_context_from_payload(_v2_payload())
    pack = _strict_pack()

    kwargs = compose_init_fetch_operator_kwargs(
        pack=pack,
        kwargs=pack["provider_execution"]["kpo_kwargs"],
        context=context,
        workload_id="orders",
        execution_kind="runtime",
        execution_scope="workload",
        hook_execution="externalized",
    )

    assert kwargs["get_logs"] is True
    assert kwargs.get("startup_timeout_seconds") == 900
    assert "get_logs" in kwargs
    assert kwargs.get("logging_interval") is None  # non-deferrable trusted path
    containers = kwargs["full_pod_spec"]["spec"]["containers"]
    assert containers, "strict pod must materialize the runtime base container"
    assert containers[0]["name"] == "base"
    assert containers[0]["command"] == ["dpone", "airflow", "runtime-pack-exec"]

    fn_source = inspect.getsource(compose_init_fetch_operator_kwargs)
    assert '"get_logs": True' in fn_source


def test_strict_init_fetch_ignores_pack_attempts_to_disable_live_logs() -> None:
    """Pack-owned get_logs=false must not survive into operator kwargs."""

    pack = _strict_pack()
    pack["airflow"] = {"execution": {"get_logs": False, "logging_interval_seconds": 60}}

    with pytest.raises(InitFetchProviderError) as raised:
        compose_init_fetch_operator_kwargs(
            pack=pack,
            kwargs=pack["provider_execution"]["kpo_kwargs"],
            context=init_fetch_context_from_payload(_v2_payload()),
            workload_id="orders",
            execution_kind="runtime",
            execution_scope="workload",
            hook_execution="externalized",
        )

    assert raised.value.code == "DPONE_INIT_FETCH_RESERVED_COLLISION"


def test_v2_pod_requires_explicit_provider_execution_projection() -> None:
    context = init_fetch_context_from_payload(_v2_payload())
    pack = _legacy_pack()

    with pytest.raises(InitFetchProviderError) as exc_info:
        compose_init_fetch_operator_kwargs(
            pack=pack,
            kwargs={"task_id": "orders__dpone_runtime"},
            context=context,
            workload_id="orders",
            execution_kind="runtime",
            execution_scope="workload",
            hook_execution="externalized",
        )

    assert exc_info.value.code == "DPONE_INIT_FETCH_PACK_MIGRATION_REQUIRED"


def test_v2_pod_rejects_task_retries_without_target_fence() -> None:
    context = init_fetch_context_from_payload(_v2_payload())
    pack = _strict_pack()
    kwargs = {**pack["provider_execution"]["kpo_kwargs"], "retries": 1}

    with pytest.raises(InitFetchProviderError) as exc_info:
        compose_init_fetch_operator_kwargs(
            pack=pack,
            kwargs=kwargs,
            context=context,
            workload_id="orders",
            execution_kind="runtime",
            execution_scope="workload",
            hook_execution="externalized",
        )

    assert exc_info.value.code == "DPONE_AIRFLOW_RETRIES_REQUIRE_TARGET_FENCE"


def test_v2_pod_preserves_bounded_retries_with_compiler_target_fence() -> None:
    context = init_fetch_context_from_payload(_v2_payload())
    pack = _strict_pack()
    pack["provider_execution"]["retry_authority"] = {
        "schema": "dpone.airflow-retry-authority.v1",
        "mode": "postgres_xmin_initial_mssql_target_atomic_v1",
        "max_task_retries": 3,
    }
    kwargs = {**pack["provider_execution"]["kpo_kwargs"], "retries": 2}

    composed = compose_init_fetch_operator_kwargs(
        pack=pack,
        kwargs=kwargs,
        context=context,
        workload_id="orders",
        execution_kind="runtime",
        execution_scope="workload",
        hook_execution="externalized",
    )

    assert composed["retries"] == 2


def test_v2_pod_keeps_externalized_hooks_at_zero_retries() -> None:
    context = init_fetch_context_from_payload(_v2_payload())
    pack = _strict_pack()
    pack["provider_execution"]["retry_authority"] = {
        "schema": "dpone.airflow-retry-authority.v1",
        "mode": "postgres_xmin_initial_mssql_target_atomic_v1",
        "max_task_retries": 3,
    }
    kwargs = {**pack["provider_execution"]["kpo_kwargs"], "retries": 2}

    composed = compose_init_fetch_operator_kwargs(
        pack=pack,
        kwargs=kwargs,
        context=context,
        workload_id="orders",
        execution_kind="pre_hook",
        execution_scope="process",
        process_selector="public.orders",
        hook_execution="externalized",
        hook_name="prepare_target",
    )

    assert composed["retries"] == 0


@pytest.mark.parametrize(
    "retry_authority,retries,expected_code",
    [
        (
            {
                "schema": "dpone.airflow-retry-authority.v1",
                "mode": "postgres_xmin_initial_mssql_target_atomic_v1",
                "max_task_retries": 3,
            },
            4,
            "DPONE_AIRFLOW_RETRIES_REQUIRE_TARGET_FENCE",
        ),
        (
            {
                "schema": "dpone.airflow-retry-authority.v1",
                "mode": "postgres_xmin_initial_mssql_target_atomic_v1",
                "max_task_retries": True,
            },
            1,
            "DPONE_INIT_FETCH_PROVIDER_EXECUTION_INVALID",
        ),
        (
            {
                "schema": "dpone.airflow-retry-authority.v1",
                "mode": "forged",
                "max_task_retries": 3,
            },
            1,
            "DPONE_INIT_FETCH_PROVIDER_EXECUTION_INVALID",
        ),
    ],
)
def test_v2_pod_rejects_invalid_or_over_limit_retry_authority(
    retry_authority: dict[str, object],
    retries: int,
    expected_code: str,
) -> None:
    context = init_fetch_context_from_payload(_v2_payload())
    pack = _strict_pack()
    pack["provider_execution"]["retry_authority"] = retry_authority
    kwargs = {**pack["provider_execution"]["kpo_kwargs"], "retries": retries}

    with pytest.raises(InitFetchProviderError) as exc_info:
        compose_init_fetch_operator_kwargs(
            pack=pack,
            kwargs=kwargs,
            context=context,
            workload_id="orders",
            execution_kind="runtime",
            execution_scope="workload",
            hook_execution="externalized",
        )

    assert exc_info.value.code == expected_code


@pytest.mark.parametrize(
    "mutation",
    [
        "pod_security",
        "sidecar",
        "reserved_mount",
        "kpo_security",
        "plan_env",
        "image",
        "command",
        "arguments",
        "namespace",
        "service_account",
        "init_container",
        "volume",
    ],
)
def test_v2_pod_rejects_security_and_reserved_override_surfaces(mutation: str) -> None:
    context = init_fetch_context_from_payload(_v2_payload())
    pack = _strict_pack()
    projection = pack["provider_execution"]
    kwargs = dict(projection["kpo_kwargs"])
    spec = projection["pod_spec"]["spec"]
    if mutation == "pod_security":
        spec["securityContext"] = {"runAsUser": 0}
    elif mutation == "sidecar":
        spec["containers"].append({"name": "sidecar", "image": "attacker"})
    elif mutation == "reserved_mount":
        spec["containers"][0]["volumeMounts"] = [{"name": "attacker", "mountPath": "/var/lib/dpone/run"}]
    elif mutation == "kpo_security":
        kwargs["secrets"] = ["airflow-secret"]
    elif mutation == "plan_env":
        kwargs["env_vars"] = {"DPONE_INIT_FETCH_PLAN_B64": "attacker"}
    elif mutation == "image":
        projection["kpo_kwargs"]["image"] = "attacker"
    elif mutation == "command":
        spec["containers"][0]["command"] = ["attacker"]
    elif mutation == "arguments":
        projection["kpo_kwargs"]["arguments"] = ["attacker"]
    elif mutation == "namespace":
        projection["kpo_kwargs"]["namespace"] = "attacker"
    elif mutation == "service_account":
        spec["serviceAccountName"] = "attacker"
    elif mutation == "init_container":
        spec["initContainers"] = [{"name": "attacker"}]
    else:
        spec["volumes"] = [{"name": "attacker"}]

    with pytest.raises(InitFetchProviderError) as exc_info:
        compose_init_fetch_operator_kwargs(
            pack=pack,
            kwargs=kwargs,
            context=context,
            workload_id="orders",
            execution_kind="runtime",
            execution_scope="workload",
            hook_execution="externalized",
        )

    assert exc_info.value.code == "DPONE_INIT_FETCH_RESERVED_COLLISION"


def _mapping_plan() -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema": "dpone.airflow-mapping-plan.v1",
        "mode": "visible",
        "backfill_plan_hash": "sha256:" + "8" * 64,
        "chunks_total": 1,
        "items_total": 1,
        "limits": {"max_items": 1, "max_active": 1, "pool": "dpone_backfill"},
        "items": [
            {
                "item_index": 0,
                "first_chunk_index": 1,
                "last_chunk_index": 1,
                "chunks_count": 1,
            }
        ],
    }
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
    return {**payload, "plan_fingerprint": "sha256:" + hashlib.sha256(encoded).hexdigest()}


def test_runtime_pre_hook_and_mapped_operator_share_one_immutable_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pack = _strict_pack()
    pack["steps"] = [
        _owned_hook_step(
            name="prepare_orders",
            hook_id="refresh_orders",
            selector="dbo.orders",
        )
    ]
    pack["mapping_plan"] = _mapping_plan()
    pack["pack_fingerprint"] = compute_pack_fingerprint(pack)
    deployment_payload = _v2_payload()
    deployment_payload["workload_packs"][0]["pack_fingerprint"] = pack["pack_fingerprint"]
    context = init_fetch_context_from_payload(deployment_payload)

    class RecordingOperator:
        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = kwargs
            self.downstream: list[Any] = []

        def __rshift__(self, other: Any) -> Any:
            self.downstream.append(other)
            return other

        @classmethod
        def partial(cls, **kwargs: Any) -> Any:
            return SimpleNamespace(expand=lambda **expanded: cls(**kwargs, expanded_env_vars=expanded["env_vars"]))

    monkeypatch.setattr(pack_tasks, "_operator_class", lambda *_args, **_kwargs: RecordingOperator)
    tasks = pack_tasks._build_dpone_gitops_task_group_from_loaded_pack(
        pack,
        provenance={
            "pack_sha256": PACK_SHA256,
            "verified_pack_fingerprint": pack["pack_fingerprint"],
        },
        dag=SimpleNamespace(),
        operator_overrides=None,
        node=None,
        task_group=None,
        delivery_context=context,
    )

    pre_hook = tasks["prepare_orders"]
    mapped_runtime = tasks["dpone_runtime"]
    assert pre_hook._dpone_init_fetch_context is context
    assert mapped_runtime._dpone_init_fetch_context is context
    pre_plan, _ = _decoded_plan(pre_hook.kwargs["env_vars"])
    mapped_plan, _ = _decoded_plan(mapped_runtime.kwargs["expanded_env_vars"][0])
    assert pre_plan["execution"] == {
        "kind": "pre_hook",
        "selector": "orders",
        "scope": "process",
        "process_selector": "dbo.orders",
        "hook_name": "prepare_orders",
        "hook_execution": "externalized",
    }
    assert mapped_plan["execution"] == {
        "kind": "runtime",
        "selector": "orders",
        "scope": "workload",
        "process_selector": None,
        "hook_name": None,
        "hook_execution": "externalized",
    }


@pytest.mark.parametrize(
    ("visibility", "expected_hook_execution", "expected_task_keys"),
    (
        ("inline", "inline", {"dpone_runtime"}),
        (
            "task",
            "externalized",
            {"pre_hook_refresh_orders", "dpone_runtime", "outcome_gate"},
        ),
        (
            "group",
            "externalized",
            {"pre_hook_refresh_orders", "dpone_runtime", "outcome_gate"},
        ),
    ),
)
def test_node_materialization_binds_hook_ownership_to_runtime_plan(
    monkeypatch: pytest.MonkeyPatch,
    visibility: str,
    expected_hook_execution: str,
    expected_task_keys: set[str],
) -> None:
    from dpone_airflow_pack.node_materialization import PackNodeMaterialization

    pack = _strict_pack()
    process_steps = [
        _owned_hook_step(
            name="pre_hook_refresh_orders",
            hook_id="refresh_orders",
            selector="dbo.orders",
        ),
        {
            "name": "dpone_runtime",
            "phase": "runtime",
            "depends_on": ["pre_hook_refresh_orders"],
        },
        {
            "name": "outcome_gate",
            "phase": "evidence",
            "depends_on": ["dpone_runtime"],
        },
    ]
    pack["runtime_selection"] = {
        "mode": "process_plan",
        "required_for_selected_nodes": True,
    }
    pack["process_plans"] = {
        "dbo.orders": {
            "selector": "dbo.orders",
            "dag_node": {
                "process_name": "orders",
                "visibility": visibility,
                "task_group": "orders" if visibility == "group" else None,
                "estimated_visible_tasks": 1 if visibility == "inline" else 3,
                "depends_on_process_selectors": [],
            },
            "runtime_commands": {
                "inline": "dpone run runtime/orders.yaml --selector dbo.orders",
                "expanded": ("DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS=1 dpone run runtime/orders.yaml --selector dbo.orders"),
            },
            "mapping_plan": {},
            "steps": process_steps,
        }
    }
    pack["pack_fingerprint"] = compute_pack_fingerprint(pack)
    deployment_payload = _v2_payload()
    deployment_payload["workload_packs"][0]["pack_fingerprint"] = pack["pack_fingerprint"]
    context = init_fetch_context_from_payload(deployment_payload)
    node = PackNodeMaterialization(
        node_id="orders__load_orders",
        workload_id="orders",
        selector="dbo.orders",
        visibility=visibility,
        task_group="orders" if visibility == "group" else None,
    )

    class RecordingOperator:
        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = kwargs
            self.downstream: list[Any] = []

        def __rshift__(self, other: Any) -> Any:
            self.downstream.append(other)
            return other

    monkeypatch.setattr(pack_tasks, "_operator_class", lambda *_args, **_kwargs: RecordingOperator)
    monkeypatch.setattr(
        pack_tasks,
        "build_pack_outcome_task",
        lambda **_kwargs: RecordingOperator(task_id="outcome_gate"),
    )
    monkeypatch.setattr(
        pack_tasks,
        "build_pack_launch_pin_cleanup_task",
        lambda **_kwargs: RecordingOperator(task_id="launch_pin_cleanup"),
    )

    tasks = pack_tasks._build_dpone_gitops_task_group_from_loaded_pack(
        pack,
        provenance={
            "pack_sha256": PACK_SHA256,
            "verified_pack_fingerprint": pack["pack_fingerprint"],
        },
        dag=SimpleNamespace(),
        operator_overrides=None,
        node=node,
        task_group=None,
        delivery_context=context,
    )

    assert set(tasks) == expected_task_keys
    runtime_plan, _ = _decoded_plan(tasks["dpone_runtime"].kwargs["env_vars"])
    assert runtime_plan["execution"] == {
        "kind": "runtime",
        "selector": "orders",
        "scope": "process",
        "process_selector": "dbo.orders",
        "hook_name": None,
        "hook_execution": expected_hook_execution,
    }
    if visibility == "inline":
        return
    hook_plan, _ = _decoded_plan(
        tasks["pre_hook_refresh_orders"].kwargs["env_vars"],
    )
    assert hook_plan["execution"] == {
        "kind": "pre_hook",
        "selector": "orders",
        "scope": "process",
        "process_selector": "dbo.orders",
        "hook_name": "pre_hook_refresh_orders",
        "hook_execution": "externalized",
    }


def test_default_process_hook_keeps_process_scope_with_null_selector(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone_airflow_pack.node_materialization import PackNodeMaterialization

    pack = _strict_pack()
    process_hook = _owned_hook_step(
        name="pre_hook_refresh_orders",
        hook_id="refresh_orders",
        selector=None,
        manifest_path="runtime/default-process.yaml",
    )
    pack["steps"] = [
        _owned_hook_step(
            name="pre_hook_refresh_orders",
            hook_id="refresh_orders",
            selector=None,
            manifest_path="runtime/workload.yaml",
        )
    ]
    pack["runtime_selection"] = {
        "mode": "process_plan",
        "required_for_selected_nodes": True,
    }
    pack["process_plans"] = {
        "__default__": {
            "selector": None,
            "dag_node": {
                "process_name": "orders",
                "visibility": "task",
                "task_group": None,
                "estimated_visible_tasks": 3,
                "depends_on_process_selectors": [],
            },
            "runtime_commands": {
                "inline": "dpone run runtime/default-process.yaml",
                "expanded": ("DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS=1 dpone run runtime/default-process.yaml"),
            },
            "mapping_plan": {},
            "steps": [
                process_hook,
                {
                    "name": "dpone_runtime",
                    "phase": "runtime",
                    "depends_on": ["pre_hook_refresh_orders"],
                },
                {
                    "name": "outcome_gate",
                    "phase": "evidence",
                    "depends_on": ["dpone_runtime"],
                },
            ],
        }
    }
    pack["pack_fingerprint"] = compute_pack_fingerprint(pack)
    deployment_payload = _v2_payload()
    deployment_payload["workload_packs"][0]["pack_fingerprint"] = pack["pack_fingerprint"]
    context = init_fetch_context_from_payload(deployment_payload)
    node = PackNodeMaterialization(
        node_id="orders__load_orders",
        workload_id="orders",
        selector=None,
        visibility="task",
        task_group=None,
    )

    class RecordingOperator:
        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = kwargs
            self.downstream: list[Any] = []

        def __rshift__(self, other: Any) -> Any:
            self.downstream.append(other)
            return other

    monkeypatch.setattr(pack_tasks, "_operator_class", lambda *_args, **_kwargs: RecordingOperator)
    monkeypatch.setattr(
        pack_tasks,
        "build_pack_outcome_task",
        lambda **_kwargs: RecordingOperator(task_id="outcome_gate"),
    )
    monkeypatch.setattr(
        pack_tasks,
        "build_pack_launch_pin_cleanup_task",
        lambda **_kwargs: RecordingOperator(task_id="launch_pin_cleanup"),
    )

    tasks = pack_tasks._build_dpone_gitops_task_group_from_loaded_pack(
        pack,
        provenance={
            "pack_sha256": PACK_SHA256,
            "verified_pack_fingerprint": pack["pack_fingerprint"],
        },
        dag=SimpleNamespace(),
        operator_overrides=None,
        node=node,
        task_group=None,
        delivery_context=context,
    )

    hook_plan, _ = _decoded_plan(
        tasks["pre_hook_refresh_orders"].kwargs["env_vars"],
    )
    assert hook_plan["execution"] == {
        "kind": "pre_hook",
        "selector": "orders",
        "scope": "process",
        "process_selector": None,
        "hook_name": "pre_hook_refresh_orders",
        "hook_execution": "externalized",
    }


def test_whole_workload_rejects_incomplete_process_hook_ownership() -> None:
    pack = _strict_pack()
    pack["steps"] = []
    process_hook = _owned_hook_step(
        name="pre_hook_refresh_orders",
        hook_id="refresh_orders",
        selector="dbo.orders",
    )
    pack["process_plans"] = {
        "dbo.orders": {
            "selector": "dbo.orders",
            "dag_node": {
                "process_name": "orders",
                "visibility": "task",
                "task_group": None,
                "estimated_visible_tasks": 3,
                "depends_on_process_selectors": [],
            },
            "runtime_commands": {
                "inline": "dpone run runtime/orders.yaml --selector dbo.orders",
                "expanded": ("DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS=1 dpone run runtime/orders.yaml --selector dbo.orders"),
            },
            "mapping_plan": {},
            "steps": [process_hook],
        }
    }
    pack["pack_fingerprint"] = compute_pack_fingerprint(pack)
    deployment_payload = _v2_payload()
    deployment_payload["workload_packs"][0]["pack_fingerprint"] = pack["pack_fingerprint"]
    context = init_fetch_context_from_payload(deployment_payload)

    with pytest.raises(
        InitFetchProviderError,
        match="whole-workload hook ownership is incomplete",
    ) as exc_info:
        pack_tasks._build_dpone_gitops_task_group_from_loaded_pack(
            pack,
            provenance={
                "pack_sha256": PACK_SHA256,
                "verified_pack_fingerprint": pack["pack_fingerprint"],
            },
            dag=SimpleNamespace(),
            operator_overrides=None,
            node=None,
            task_group=None,
            delivery_context=context,
        )

    assert exc_info.value.code == "DPONE_INIT_FETCH_HOOK_OWNERSHIP_INCOMPLETE"


def test_whole_workload_accepts_complete_process_hook_ownership() -> None:
    from dpone_airflow_pack.pack_hook_ownership import (
        require_complete_workload_hook_ownership,
    )

    hook_step = _owned_hook_step(
        name="pre_hook_refresh_orders",
        hook_id="refresh_orders",
        selector="dbo.orders",
    )
    require_complete_workload_hook_ownership(
        {
            "steps": [hook_step],
            "process_plans": {
                "dbo.orders": {
                    "selector": "dbo.orders",
                    "steps": [dict(hook_step)],
                }
            },
        }
    )


def test_whole_workload_rejects_one_missing_hook_from_multi_process_inventory() -> None:
    from dpone_airflow_pack.pack_hook_ownership import (
        require_complete_workload_hook_ownership,
    )

    orders = _owned_hook_step(
        name="pre_hook_refresh_orders",
        hook_id="refresh_orders",
        selector="dbo.orders",
    )
    customers = _owned_hook_step(
        name="pre_hook_refresh_customers",
        hook_id="refresh_customers",
        selector="dbo.customers",
    )

    with pytest.raises(
        InitFetchProviderError,
        match="pre_hook_refresh_customers",
    ) as exc_info:
        require_complete_workload_hook_ownership(
            {
                "steps": [orders],
                "process_plans": {
                    "dbo.orders": {
                        "selector": "dbo.orders",
                        "steps": [orders],
                    },
                    "dbo.customers": {
                        "selector": "dbo.customers",
                        "steps": [customers],
                    },
                },
            }
        )

    assert exc_info.value.code == "DPONE_INIT_FETCH_HOOK_OWNERSHIP_INCOMPLETE"


def test_whole_workload_does_not_conflate_repeated_hook_names_across_selectors() -> None:
    from dpone_airflow_pack.pack_hook_ownership import (
        require_complete_workload_hook_ownership,
    )

    orders = _owned_hook_step(
        name="pre_hook_refresh_source",
        hook_id="refresh_source",
        selector="dbo.orders",
    )
    customers = _owned_hook_step(
        name="pre_hook_refresh_source",
        hook_id="refresh_source",
        selector="dbo.customers",
    )

    with pytest.raises(
        InitFetchProviderError,
        match="dbo.customers",
    ):
        require_complete_workload_hook_ownership(
            {
                "steps": [orders],
                "process_plans": {
                    "dbo.orders": {
                        "selector": "dbo.orders",
                        "steps": [orders],
                    },
                    "dbo.customers": {
                        "selector": "dbo.customers",
                        "steps": [customers],
                    },
                },
            }
        )


def test_whole_workload_rejects_hook_with_different_executable_argv() -> None:
    from dpone_airflow_pack.pack_hook_ownership import (
        require_complete_workload_hook_ownership,
    )

    required = _owned_hook_step(
        name="pre_hook_refresh_orders",
        hook_id="refresh_orders",
        selector="dbo.orders",
    )
    externalized = _owned_hook_step(
        name="pre_hook_refresh_orders",
        hook_id="refresh_orders",
        selector="dbo.orders",
        manifest_path="runtime/customers.yaml",
    )

    with pytest.raises(
        InitFetchProviderError,
        match="pre_hook_refresh_orders",
    ):
        require_complete_workload_hook_ownership(
            {
                "steps": [externalized],
                "process_plans": {
                    "dbo.orders": {
                        "selector": "dbo.orders",
                        "steps": [required],
                    }
                },
            }
        )


@pytest.mark.parametrize(
    ("field", "different_value"),
    (
        ("depends_on", ["pre_hook_other"]),
        ("required", False),
        ("credential_required", False),
        ("produces", ["different_evidence"]),
        ("reason", "hook:different"),
    ),
)
def test_whole_workload_rejects_hook_with_different_execution_policy(
    field: str,
    different_value: object,
) -> None:
    from dpone_airflow_pack.pack_hook_ownership import (
        require_complete_workload_hook_ownership,
    )

    required = _owned_hook_step(
        name="pre_hook_refresh_orders",
        hook_id="refresh_orders",
        selector="dbo.orders",
    )
    externalized = dict(required)
    externalized[field] = different_value

    with pytest.raises(
        InitFetchProviderError,
        match="pre_hook_refresh_orders",
    ):
        require_complete_workload_hook_ownership(
            {
                "steps": [externalized],
                "process_plans": {
                    "dbo.orders": {
                        "selector": "dbo.orders",
                        "steps": [required],
                    }
                },
            }
        )


def test_whole_workload_rejects_human_command_that_differs_from_runtime_argv() -> None:
    from dpone_airflow_pack.pack_hook_ownership import (
        require_complete_workload_hook_ownership,
    )

    required = _owned_hook_step(
        name="pre_hook_refresh_orders",
        hook_id="refresh_orders",
        selector="dbo.orders",
    )
    externalized = dict(required)
    externalized["command"] = (
        "dpone hooks execute runtime/customers.yaml --phase pre_hook --hook-id refresh_orders --selector dbo.orders"
    )

    with pytest.raises(
        InitFetchProviderError,
        match="command differs from runtime argv",
    ):
        require_complete_workload_hook_ownership(
            {
                "steps": [externalized],
                "process_plans": {
                    "dbo.orders": {
                        "selector": "dbo.orders",
                        "steps": [required],
                    }
                },
            }
        )


@pytest.mark.parametrize(
    ("plan_key", "plan_selector"),
    (
        ("__default__", None),
        ("dbo.orders", "dbo.customers"),
    ),
)
def test_whole_workload_rejects_process_plan_selector_mismatch(
    plan_key: str,
    plan_selector: str | None,
) -> None:
    from dpone_airflow_pack.pack_hook_ownership import (
        require_complete_workload_hook_ownership,
    )

    hook = _owned_hook_step(
        name="pre_hook_refresh_orders",
        hook_id="refresh_orders",
        selector="dbo.orders",
    )

    with pytest.raises(
        InitFetchProviderError,
        match="process selector is inconsistent",
    ):
        require_complete_workload_hook_ownership(
            {
                "steps": [hook],
                "process_plans": {
                    plan_key: {
                        "selector": plan_selector,
                        "steps": [hook],
                    }
                },
            }
        )


def test_node_hook_execution_rejects_structured_selector_mismatch() -> None:
    from dpone_airflow_pack.pack_hook_ownership import (
        pre_hook_execution_selection,
    )

    hook = _owned_hook_step(
        name="pre_hook_refresh_orders",
        hook_id="refresh_orders",
        selector="dbo.customers",
    )

    with pytest.raises(
        InitFetchProviderError,
        match="process selector is inconsistent",
    ):
        pre_hook_execution_selection(
            hook,
            node_selector="dbo.orders",
            node_scoped=True,
        )


def _owned_hook_step(
    *,
    name: str,
    hook_id: str,
    selector: str | None,
    manifest_path: str = "runtime/orders.yaml",
) -> dict[str, object]:
    argv = [
        "dpone",
        "hooks",
        "execute",
        manifest_path,
        "--phase",
        "pre_hook",
        "--hook-id",
        hook_id,
        *(("--selector", selector) if selector is not None else ()),
    ]
    return {
        "name": name,
        "phase": "pre_hook",
        "command": " ".join(argv),
        "required": True,
        "credential_required": True,
        "produces": ["hook_evidence"],
        "reason": "hook:source_refresh",
        "depends_on": [],
        "runtime_command": {
            "schema": "dpone.airflow-pre-hook-command.v1",
            "hook_id": hook_id,
            "process_selector": selector,
            "argv": argv,
        },
    }


def test_indexed_task_group_threads_the_same_v2_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = init_fetch_context_from_payload(_v2_payload())
    captured: dict[str, Any] = {}

    monkeypatch.setattr(provider, "_airflow_task_group_class", lambda: None)
    monkeypatch.setattr(
        provider,
        "_resolve_pack_ref",
        lambda *_args, **_kwargs: (
            tmp_path / "orders.json",
            PACK_SHA256,
            {"schema": "dpone.airflow-run-identity.v1"},
            tmp_path,
            context,
        ),
    )

    def _build(_pack_ref: Path, **kwargs: Any) -> dict[str, object]:
        captured.update(kwargs)
        return {"dpone_runtime": object()}

    monkeypatch.setattr(provider, "build_dpone_gitops_task_group_from_pack", _build)

    provider.DponeTaskGroup.from_pack(
        "cached://workloads/orders",
        index_path=tmp_path / "airflow-index.json",
    )

    assert captured["delivery_context"] is context


def test_v2_all_index_preflight_leaves_globals_unchanged_on_late_contract_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = init_fetch_context_from_payload(_v2_payload())
    artifacts = tuple(
        AirflowIndexArtifact(
            id=dag_id,
            artifact_ref=f"cache://releases/{RELEASE_DIR}/dags/{dag_id}.json",
            sha256="sha256:" + digit * 64,
            path=tmp_path / f"{dag_id}.json",
            bytes=1,
            cache_root=tmp_path,
        )
        for dag_id, digit in (("first", "5"), ("second", "6"))
    )
    index = AirflowDeploymentIndex(
        schema="dpone.airflow-deployment-index.v2",
        path=tmp_path / "airflow-index.json",
        cache_root=tmp_path,
        release_id=RELEASE_ID,
        deployment_id=DEPLOYMENT_ID,
        dag_specs=artifacts,
        workload_packs=(),
        runtime_artifact_delivery=_v2_payload()["runtime_artifact_delivery"],
        delivery_context=context,
    )
    monkeypatch.setattr(
        dag_loader,
        "load_dag_spec_file",
        lambda path, **_kwargs: (
            {"dag_id": path.stem, "nodes": [], "edges": [], "schedule": None},
            (),
        ),
    )
    calls: list[str] = []

    def _materialize(spec: dict[str, Any], **_kwargs: Any) -> object:
        calls.append(spec["dag_id"])
        if spec["dag_id"] == "second":
            raise InitFetchProviderError(
                "DPONE_INIT_FETCH_RESERVED_COLLISION",
                "strict init-fetch pod contract is invalid",
            )
        return object()

    monkeypatch.setattr(dag_loader, "_materialize_dag_spec", _materialize)
    globals_dict: dict[str, object] = {"sentinel": object()}
    original = dict(globals_dict)

    report = dag_loader.load_dpone_dags_from_index(
        globals_dict,
        index=index,
        operator_overrides=None,
        duplicate_policy="skip_and_report",
        invalid_dag_policy="skip_and_report",
    )

    assert calls == ["first", "second"]
    assert report.fatal is True
    assert report.loaded == ()
    assert globals_dict == original


def test_v2_preflight_defers_dag_retries_to_each_pack_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = init_fetch_context_from_payload(_v2_payload())
    artifact = AirflowIndexArtifact(
        id="orders",
        artifact_ref=f"cache://releases/{RELEASE_DIR}/dags/orders.json",
        sha256="sha256:" + "5" * 64,
        path=tmp_path / "orders.json",
        bytes=1,
        cache_root=tmp_path,
    )
    index = AirflowDeploymentIndex(
        schema="dpone.airflow-deployment-index.v2",
        path=tmp_path / "airflow-index.json",
        cache_root=tmp_path,
        release_id=RELEASE_ID,
        deployment_id=DEPLOYMENT_ID,
        dag_specs=(artifact,),
        workload_packs=(),
        runtime_artifact_delivery=_v2_payload()["runtime_artifact_delivery"],
        delivery_context=context,
    )
    monkeypatch.setattr(
        dag_loader,
        "load_dag_spec_file",
        lambda _path, **_kwargs: (
            {
                "dag_id": "orders",
                "default_args": {"retries": 1},
                "nodes": [],
                "edges": [],
                "schedule": None,
            },
            (),
        ),
    )
    materialized: list[str] = []
    monkeypatch.setattr(
        dag_loader,
        "_materialize_dag_spec",
        lambda payload, **_kwargs: materialized.append(str(payload["dag_id"])),
    )
    globals_dict: dict[str, object] = {"sentinel": object()}
    report = dag_loader.load_dpone_dags_from_index(
        globals_dict,
        index=index,
        operator_overrides=None,
        duplicate_policy="skip_and_report",
        invalid_dag_policy="skip_and_report",
    )

    assert report.fatal is False
    assert report.loaded == ("orders",)
    assert materialized == ["orders"]


@pytest.mark.parametrize("corrupt_artifact", ["dag_spec", "workload_pack"])
def test_v2_duplicate_cannot_hide_corrupt_index_artifact(
    corrupt_artifact: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = init_fetch_context_from_payload(_v2_payload())
    dag_artifact = AirflowIndexArtifact(
        id="orders",
        artifact_ref=f"cache://releases/{RELEASE_DIR}/dags/orders.json",
        sha256="sha256:" + "5" * 64,
        path=tmp_path / "orders.json",
        bytes=128,
        cache_root=tmp_path,
    )
    pack_artifact = AirflowIndexArtifact(
        id="orders",
        artifact_ref=f"cache://releases/{RELEASE_DIR}/packs/orders.json",
        sha256=PACK_SHA256,
        path=tmp_path / "missing-pack.json",
        bytes=8192,
        cache_root=tmp_path,
        pack_fingerprint=PACK_FINGERPRINT,
    )
    index = AirflowDeploymentIndex(
        schema="dpone.airflow-deployment-index.v2",
        path=tmp_path / "airflow-index.json",
        cache_root=tmp_path,
        release_id=RELEASE_ID,
        deployment_id=DEPLOYMENT_ID,
        dag_specs=(dag_artifact,),
        workload_packs=(pack_artifact,),
        runtime_artifact_delivery=_v2_payload()["runtime_artifact_delivery"],
        delivery_context=context,
    )
    calls: list[Path] = []

    def _load(path: Path, **_kwargs: Any) -> tuple[dict[str, Any], tuple[Any, ...]]:
        calls.append(path)
        if corrupt_artifact == "dag_spec":
            raise AirflowDeploymentIndexError(
                "DPONE_CACHE_CHECKSUM_MISMATCH",
                "corrupt duplicate dag-spec",
                path=path.as_posix(),
            )
        return (
            {
                "dag_id": "orders",
                "nodes": [
                    {
                        "node_id": "orders",
                        "workload_id": "orders",
                        "pack_ref": "cached://workloads/orders",
                    }
                ],
                "edges": [],
                "schedule": None,
            },
            (),
        )

    monkeypatch.setattr(dag_loader, "load_dag_spec_file", _load)
    materialized: list[str] = []
    monkeypatch.setattr(
        dag_loader,
        "_materialize_dag_spec",
        lambda payload, **_kwargs: materialized.append(str(payload["dag_id"])),
    )
    existing = object()
    globals_dict: dict[str, object] = {"orders": existing}

    report = dag_loader.load_dpone_dags_from_index(
        globals_dict,
        index=index,
        operator_overrides=None,
        duplicate_policy="skip_and_report",
        invalid_dag_policy="skip_and_report",
    )

    assert calls == [dag_artifact.path]
    assert report.fatal is True
    assert report.loaded == ()
    assert materialized == []
    assert globals_dict == {"orders": existing}


DBT_IMAGE_DIGEST = "sha256:" + "9" * 64
DBT_IMAGE_REF = f"registry.example/dpone/runtime-dbt@{DBT_IMAGE_DIGEST}"


def _v2_payload_with_dbt_digest() -> dict[str, Any]:
    payload = _v2_payload()
    payload["runtime_image_dbt_ref"] = DBT_IMAGE_REF
    payload["runtime_image_dbt_digest"] = DBT_IMAGE_DIGEST
    payload["workload_packs"] = [
        {
            "id": "dbt__orders",
            "artifact_ref": f"cache://releases/{RELEASE_DIR}/packs/dbt__orders.json",
            "sha256": PACK_SHA256,
            "bytes": 8192,
            "pack_fingerprint": PACK_FINGERPRINT,
        }
    ]
    return payload


def _strict_dbt_pack() -> dict[str, Any]:
    pack = _strict_pack()
    pack["workload"] = {"workload_id": "dbt__orders"}
    pe = pack["provider_execution"]
    pe["kpo_kwargs"]["task_id"] = "dbt__orders__dpone_runtime"
    pe["kpo_kwargs"]["name"] = "dpone-dbt--orders"
    pe["kpo_kwargs"]["labels"] = {"dpone.dev/workload-id": "dbt__orders"}
    pack["pack_fingerprint"] = compute_pack_fingerprint(pack)
    return pack


def test_dual_digest_dbt_workload_uses_dbt_image_for_operator_and_pod() -> None:
    """dbt__* workloads must select runtime_image_dbt_* for KPO image and pod.

    Regression: dual-digest cutover built the pod from runtime_image_for_workload
    but left strict_runtime_image_ref on the full runtime image, so init-fetch
    validation failed with base/dbt image mismatch.
    """

    from dpone_airflow_pack.pack_task_runtime import operator_init_kwargs
    from dpone_airflow_pack.xcom_sidecar import validate_strict_xcom_pod

    context = init_fetch_context_from_payload(_v2_payload_with_dbt_digest())
    pack = _strict_dbt_pack()
    kwargs = compose_init_fetch_operator_kwargs(
        pack=pack,
        kwargs=pack["provider_execution"]["kpo_kwargs"],
        context=context,
        workload_id="dbt__orders",
        execution_kind="runtime",
        execution_scope="workload",
        hook_execution="externalized",
    )

    assert kwargs["image"] == DBT_IMAGE_REF
    containers = kwargs["full_pod_spec"]["spec"]["containers"]
    init_containers = kwargs["full_pod_spec"]["spec"]["initContainers"]
    assert containers[0]["image"] == DBT_IMAGE_REF
    assert init_containers[0]["image"] == DBT_IMAGE_REF

    selected = context.runtime_image_for_workload("dbt__orders")[0]
    assert selected == DBT_IMAGE_REF
    init_kwargs = operator_init_kwargs(
        pack,
        kwargs,
        strict_runtime_image_ref=selected,
    )
    assert init_kwargs["xcom_sidecar"].strict_runtime_image == DBT_IMAGE_REF
    # Final pod topology must validate against the selected dbt image.
    pod = {
        "spec": {
            "containers": [
                {"name": "base", "image": DBT_IMAGE_REF},
                {"name": "airflow-xcom-sidecar", "image": XCOM_IMAGE},
            ],
            "initContainers": [
                {"name": "dpone-runtime-init-fetch", "image": DBT_IMAGE_REF},
            ],
        }
    }
    validate_strict_xcom_pod(pod, sidecar_image=XCOM_IMAGE, runtime_image_ref=selected)
