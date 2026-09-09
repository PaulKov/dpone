"""Real-Airflow contract tests for the declarative dag-spec loader.

Runs only where Apache Airflow (with the cncf.kubernetes provider) is
importable — locally inside ``.venv-airflow`` and in the
``airflow-pack-compat`` CI matrix. Verifies ``load_dpone_dags`` materializes
scheduler-safe DAG objects, quarantines invalid specs, and preserves
collision-safe coexistence with hand-written DAG modules.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

airflow = pytest.importorskip("airflow", reason="Apache Airflow is required for dag-spec loader contract tests")
pytest.importorskip(
    "airflow.providers.cncf.kubernetes",
    reason="cncf.kubernetes provider is required for dag-spec loader contract tests",
)

from airflow import DAG  # noqa: E402
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator  # noqa: E402
from dpone_airflow_pack.cache_activation_contract import cache_write_lease  # noqa: E402
from dpone_airflow_pack.connection_secret_lifecycle import (  # noqa: E402
    ATTEMPT_REF_ANNOTATION,
    LIFECYCLE_VERSION_LABEL,
    MANAGED_BY_LABEL,
    POD_RESOURCE_KIND,
    RESOURCE_KIND_LABEL,
    SECRET_REF_ANNOTATION,
    AirflowConnectionSecretLifecycle,
)
from dpone_airflow_pack.dag_loader import load_dpone_dags  # noqa: E402
from dpone_airflow_pack.dag_spec_loader import compute_dag_spec_fingerprint  # noqa: E402
from dpone_airflow_pack.operator_runtime import materialize_airflow_connection_secret_volume  # noqa: E402
from dpone_airflow_pack.operators import (  # noqa: E402
    AirflowConnectionSecretVolumeKubernetesPodOperator,
    PinnedXComSidecarKubernetesPodOperator,
)
from dpone_airflow_pack.pack_identity import (  # noqa: E402
    PACK_IDENTITY_SCHEMA,
    compute_pack_fingerprint,
)
from kubernetes.client import models as k8s  # noqa: E402

RELEASE_ID = "sha256:" + "a" * 64
DEPLOYMENT_ID = "sha256:" + "b" * 64
PACK_FINGERPRINT = "sha256:" + "c" * 64


def test_supported_kpo_deferrable_callback_validates_returned_or_pushed_xcom(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Validate both KPO contracts used across the exact compatibility matrix."""

    failed = {"kind": "gitops.airflow_xcom_summary", "status": "failed", "blockers": []}

    class TaskInstance:
        def __init__(self) -> None:
            self.value: object = None

        def xcom_push(self, _key: str, value: object) -> None:
            self.value = value

        def xcom_pull(self, *, task_ids: str) -> object:
            assert task_ids == "load_orders"
            return self.value

    task_instance = TaskInstance()
    from dpone_airflow_pack.xcom_sidecar import XComSidecarRuntimeConfig

    operator = PinnedXComSidecarKubernetesPodOperator(
        task_id="load_orders",
        name="load-orders",
        namespace="airflow",
        image="dpone-runtime:test",
        do_xcom_push=True,
        deferrable=True,
        get_logs=False,
        inline_outcome_required_status="passed",
        xcom_sidecar=XComSidecarRuntimeConfig(
            image="registry.example/dpone/xcom-sidecar@sha256:" + "a" * 64,
        ),
    )
    completed_pod = k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(name="load-orders", namespace="airflow"),
        status=k8s.V1PodStatus(
            phase="Succeeded",
            container_statuses=[
                k8s.V1ContainerStatus(
                    name=operator.base_container_name,
                    image="dpone-runtime:test",
                    image_id="dpone-runtime:test",
                    ready=False,
                    restart_count=0,
                    state=k8s.V1ContainerState(
                        terminated=k8s.V1ContainerStateTerminated(exit_code=0),
                    ),
                )
            ],
        ),
    )
    operator.__dict__["hook"] = SimpleNamespace(get_pod=lambda _name, _namespace: completed_pod)
    monkeypatch.setattr(operator, "extract_xcom", lambda **_kwargs: failed)
    monkeypatch.setattr(operator, "_clean", lambda *_args, **_kwargs: None)
    # Dpone KPO re-asserts get_logs=True before super().trigger_reentry; stub the
    # cncf base so this unit test does not need a live Kubernetes client.
    monkeypatch.setattr(
        KubernetesPodOperator,
        "trigger_reentry",
        lambda self, context, event: failed,
    )

    with pytest.raises(RuntimeError, match="DPONE_AIRFLOW_INLINE_OUTCOME_FAILED"):
        operator.trigger_reentry(
            {"ti": task_instance},
            {"name": "load-orders", "namespace": "airflow", "status": "success"},
        )

    assert PinnedXComSidecarKubernetesPodOperator.trigger_reentry is not KubernetesPodOperator.trigger_reentry


def test_real_kpo_preserves_validated_lifecycle_metadata_at_highest_precedence() -> None:
    lifecycle = AirflowConnectionSecretLifecycle(
        secret_ref="sha256:" + "c" * 64,
        attempt_ref="sha256:" + "d" * 64,
        cleanup_policy="retain",
    )
    projection = {
        "mode": "kubernetes_secret_volume",
        "secret_name": "dpone-attempt-secret",
        "mount_path": "/run/secrets/dpone/airflow-connections",
        "connections": [
            {
                "connection_id": "mssql_prod",
                "secret_key": "AIRFLOW_CONN_MSSQL_PROD",
                "mount_path": "/run/secrets/dpone/airflow-connections/mssql",
                "fields": {"uri": "uri"},
            }
        ],
    }
    operator = AirflowConnectionSecretVolumeKubernetesPodOperator(
        task_id="load_orders",
        name="load-orders",
        namespace="airflow-example",
        image="dpone-runtime:test",
        full_pod_spec=k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(labels={"base": "kept"}),
            spec=k8s.V1PodSpec(containers=[k8s.V1Container(name="base", image="dpone-runtime:test")]),
        ),
        labels={"team": "sales", MANAGED_BY_LABEL: "not-dpone"},
        annotations={"runbook": "orders", SECRET_REF_ANNOTATION: "sha256:" + "0" * 64},
        airflow_connection_projection=projection,
    )

    materialize_airflow_connection_secret_volume(operator, projection, lifecycle=lifecycle)
    # Keep the real KPO merge path while isolating kube-config and Airflow metadata DB access.
    operator.__dict__["hook"] = SimpleNamespace(is_in_cluster=False)
    pod = operator.build_pod_request_obj(context={})

    assert pod.metadata.labels["base"] == "kept"
    assert pod.metadata.labels["team"] == "sales"
    assert pod.metadata.labels[MANAGED_BY_LABEL] == "dpone"
    assert pod.metadata.labels[RESOURCE_KIND_LABEL] == POD_RESOURCE_KIND
    assert pod.metadata.labels[LIFECYCLE_VERSION_LABEL] == "v1"
    assert pod.metadata.annotations["runbook"] == "orders"
    assert pod.metadata.annotations[SECRET_REF_ANNOTATION] == lifecycle.secret_ref
    assert pod.metadata.annotations[ATTEMPT_REF_ANNOTATION] == lifecycle.attempt_ref


def _sha256(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _minimal_pack(*, task_id: str) -> dict[str, object]:
    return {
        "kind": "gitops.airflow_pack",
        "schema_version": "3",
        "producer": "test",
        "kpo_kwargs": {
            "task_id": task_id,
            "name": task_id.replace("_", "-"),
            "namespace": "airflow",
            "image": "dpone-runtime:test",
            "cmds": ["dpone"],
            "arguments": ["run", "manifests/orders.yaml", "--format", "json"],
        },
        "runtime_command": "dpone run manifests/orders.yaml --format json",
        "pack_fingerprint": PACK_FINGERPRINT,
        "steps": [],
    }


def _strict_init_fetch_pack(*, workload_id: str) -> dict[str, object]:
    task_id = f"{workload_id}__dpone_runtime"
    name = f"dpone-{workload_id.replace('_', '-')}"
    payload: dict[str, object] = {
        "kind": "gitops.airflow_pack",
        "schema_version": "3",
        "producer": "test",
        "pack_identity": {"schema": PACK_IDENTITY_SCHEMA},
        "workload": {"workload_id": workload_id},
        "airflow": {"execution": {}},
        "connection_projection": {},
        "kpo_kwargs": {
            "task_id": task_id,
            "name": name,
            "namespace": "airflow",
            "image": "dpone-runtime:test",
            "cmds": ["dpone"],
            "arguments": ["run", "manifests/orders.yaml", "--format", "json"],
        },
        "runtime_command": "dpone run manifests/orders.yaml --format json",
        "provider_execution": {
            "schema": "dpone.airflow-provider-execution.v1",
            "kpo_kwargs": {
                "task_id": task_id,
                "name": name,
                "labels": {"dpone.dev/workload-id": workload_id},
                "env_vars": {},
            },
            "pod_spec": {"spec": {"containers": [{"name": "base"}]}},
        },
        "xcom": {"sidecar_image": "registry.example/airflow/xcom@sha256:" + "d" * 64},
        "steps": [],
    }
    payload["pack_fingerprint"] = compute_pack_fingerprint(payload)
    return payload


def _valid_spec_payload(*, dag_id: str, pack_ref: str) -> dict[str, object]:
    payload: dict[str, object] = {
        "kind": "gitops.airflow_dag_spec",
        "schema_version": "1",
        "producer": "test",
        "dag_id": dag_id,
        "schedule": None,
        "start_date": "2026-07-07",
        "catchup": False,
        "tags": ["dpone"],
        "nodes": [
            {
                "node_id": "load_orders",
                "workload_id": "load_orders",
                "pack_ref": pack_ref,
            }
        ],
        "edges": [],
        "topological_order": ["load_orders"],
    }
    payload["spec_fingerprint"] = compute_dag_spec_fingerprint(payload)
    return payload


def _write_deployment_index_fixture(
    tmp_path: Path,
    *,
    include_invalid: bool = False,
    runtime_mode: str = "local_preview",
) -> Path:
    cache = tmp_path / ".dpone-cache"
    with cache_write_lease(cache):
        pass
    release_dir_name = RELEASE_ID.replace(":", "-")
    deployment_dir_name = DEPLOYMENT_ID.replace(":", "-")
    dags_dir = cache / "releases" / release_dir_name / "dags"
    packs_dir = cache / "releases" / release_dir_name / "packs"
    deployment_dir = cache / "deployments" / "prod" / "sha256-deployment"
    dags_dir.mkdir(parents=True)
    packs_dir.mkdir(parents=True)
    deployment_dir.mkdir(parents=True)

    pack_path = packs_dir / "load_orders.airflow-pack.json"
    if runtime_mode == "init_fetch":
        pack_payload = _strict_init_fetch_pack(workload_id="load_orders")
        pack_fingerprint = str(pack_payload["pack_fingerprint"])
    else:
        pack_payload = _minimal_pack(task_id="load_orders__dpone_runtime")
        pack_fingerprint = PACK_FINGERPRINT
    pack_path.write_text(json.dumps(pack_payload, sort_keys=True), encoding="utf-8")
    pack_ref = f"cached://deployments/{DEPLOYMENT_ID}/workloads/load_orders"

    valid_path = dags_dir / "orders_daily.dag-spec.json"
    valid_path.write_text(
        json.dumps(_valid_spec_payload(dag_id="orders_daily", pack_ref=pack_ref), sort_keys=True),
        encoding="utf-8",
    )
    dag_specs = [
        {
            "id": "orders_daily",
            "artifact_ref": f"cache://releases/{release_dir_name}/dags/orders_daily.dag-spec.json",
            "sha256": _sha256(valid_path),
            "bytes": valid_path.stat().st_size,
        }
    ]
    if include_invalid:
        invalid_path = dags_dir / "broken.dag-spec.json"
        invalid_path.write_text(
            json.dumps(
                {
                    "kind": "gitops.airflow_dag_spec",
                    "schema_version": "1",
                    "producer": "test",
                    "dag_id": "broken",
                    "nodes": [],
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        dag_specs.append(
            {
                "id": "broken",
                "artifact_ref": f"cache://releases/{release_dir_name}/dags/broken.dag-spec.json",
                "sha256": _sha256(invalid_path),
                "bytes": invalid_path.stat().st_size,
            }
        )

    runtime_delivery: dict[str, object] = {"mode": runtime_mode}
    schema = "dpone.airflow-deployment-index.v1"
    if runtime_mode == "init_fetch":
        schema = "dpone.airflow-deployment-index.v2"
        registry_ref = "dpone-prod-artifacts"
        runtime_delivery = {
            "mode": "init_fetch",
            "trust_tier": "production",
            "artifact_registry_ref": registry_ref,
            "identity": {
                "method": "kubernetes_workload_identity",
                "service_account": "dpone-runtime",
                "namespace": "airflow",
            },
            "registry_config_ref": {
                "kind": "kubernetes_config_map",
                "name": "dpone-artifact-registry",
                "key": "registry.json",
                "sha256": "sha256:" + "5" * 64,
            },
            "trust_policy_ref": {
                "kind": "kubernetes_config_map",
                "name": "dpone-artifact-trust",
                "key": "policy.json",
                "sha256": "sha256:" + "6" * 64,
            },
            "source": {"artifact_registry_ref": registry_ref},
            "verify": {
                "checksums": "required",
                "attestations": "required_for_prod",
            },
        }
    index_payload: dict[str, object] = {
        "schema": schema,
        "release_id": RELEASE_ID,
        "deployment_id": DEPLOYMENT_ID,
        "dag_specs": dag_specs,
        "workload_packs": [
            {
                "id": "load_orders",
                "artifact_ref": f"cache://releases/{release_dir_name}/packs/load_orders.airflow-pack.json",
                "sha256": _sha256(pack_path),
                "bytes": pack_path.stat().st_size,
                **({"pack_fingerprint": pack_fingerprint} if runtime_mode == "init_fetch" else {}),
            }
        ],
        "binding_set_ref": "sha256:" + "1" * 64,
        "connection_registry_ref": "sha256:" + "2" * 64,
        "credential_runtime_ref": "sha256:" + "3" * 64,
        "runtime_image_digest": "sha256:" + "4" * 64,
        "airflow_bundle_ref": "git:7ac31f2",
        "runtime_artifact_delivery": runtime_delivery,
    }
    if runtime_mode == "init_fetch":
        context_dir = "sha256-" + "9" * 64
        context_root = cache / "runtime-connection-contexts" / context_dir
        context_root.mkdir(parents=True)
        binding_path = context_root / "binding-set.json"
        registry_path = context_root / "connection-registry.json"
        credential_path = context_root / "credential-runtime.json"
        for path in (binding_path, registry_path, credential_path):
            path.write_text("{}", encoding="utf-8")

        def _runtime_connection_artifact(name: str, path: Path) -> dict[str, str | int]:
            return {
                "artifact_ref": f"cache://runtime-connection-contexts/{context_dir}/{name}.json",
                "sha256": _sha256(path),
                "bytes": path.stat().st_size,
            }

        index_payload.update(
            {
                "trust_tier": "production",
                "runtime_image_ref": "registry.example/dpone/runtime@sha256:" + "4" * 64,
                "binding_set": _runtime_connection_artifact("binding-set", binding_path),
                "connection_registry": _runtime_connection_artifact(
                    "connection-registry",
                    registry_path,
                ),
                "credential_runtime": _runtime_connection_artifact(
                    "credential-runtime",
                    credential_path,
                ),
                "release": {
                    "artifact_ref": f"cache://releases/{release_dir_name}/release-set.json",
                    "sha256": "sha256:" + "7" * 64,
                    "bytes": 4096,
                },
                "deployment": {
                    "artifact_ref": (f"cache://deployments/prod/{deployment_dir_name}/deployment.json"),
                    "sha256": "sha256:" + "8" * 64,
                    "bytes": 2048,
                },
            }
        )
    index_path = deployment_dir / "airflow-index.json"
    index_path.write_text(
        json.dumps(index_payload, sort_keys=True),
        encoding="utf-8",
    )
    return index_path


def test_load_dpone_dags_materializes_real_airflow_dag_from_deployment_index(tmp_path: Path) -> None:
    index_path = _write_deployment_index_fixture(tmp_path)
    globals_dict: dict[str, object] = {}

    report = load_dpone_dags(globals_dict, index_path=index_path)

    assert report.errors == ()
    assert report.loaded == ("orders_daily",)
    dag = globals_dict["orders_daily"]
    assert isinstance(dag, DAG)
    assert dag.dag_id == "orders_daily"
    assert dag.get_task("load_orders__dpone_runtime") is not None


def test_index_backed_runtime_task_carries_verified_composite_identity(tmp_path: Path) -> None:
    index_path = _write_deployment_index_fixture(tmp_path, runtime_mode="init_fetch")
    globals_dict: dict[str, object] = {}

    report = load_dpone_dags(globals_dict, index_path=index_path)

    assert report.errors == ()
    dag = globals_dict["orders_daily"]
    runtime_task = dag.get_task("load_orders__dpone_runtime")
    rendered = runtime_task.env_vars
    env = (
        {str(key): str(value) for key, value in rendered.items()}
        if isinstance(rendered, dict)
        else {str(item.name): str(item.value) for item in rendered}
    )
    identity = json.loads(env["DPONE_AIRFLOW_RUN_IDENTITY"])
    assert identity["release_id"] == RELEASE_ID
    assert identity["deployment_id"] == DEPLOYMENT_ID
    assert identity["dag_spec"] == {
        "id": "orders_daily",
        "sha256": _sha256(
            index_path.parents[3] / f"releases/{RELEASE_ID.replace(':', '-')}/dags/orders_daily.dag-spec.json"
        ),
    }
    assert identity["workload_pack"] == {
        "id": "load_orders",
        "sha256": _sha256(
            index_path.parents[3] / f"releases/{RELEASE_ID.replace(':', '-')}/packs/load_orders.airflow-pack.json"
        ),
    }
    assert identity["airflow_bundle"] == {
        "backend": "git",
        "ref": "git:7ac31f2",
        "snapshot_ref": None,
        "version": "7ac31f2",
        "versioned": True,
    }
    assert runtime_task.params["dpone_run_identity"] == identity
    assert getattr(dag, "_dpone_run_identity_context")["dag_spec"] == identity["dag_spec"]


def test_load_dpone_dags_quarantines_invalid_spec_with_real_airflow(tmp_path: Path) -> None:
    index_path = _write_deployment_index_fixture(tmp_path, include_invalid=True)
    globals_dict: dict[str, object] = {}

    report = load_dpone_dags(
        globals_dict,
        index_path=index_path,
        invalid_dag_policy="create_diagnostic_dag",
    )

    assert report.loaded == ("orders_daily",)
    assert any(error.get("code") == "DPONE_AIRFLOW_DAG_SPEC_NODES_MISSING" for error in report.errors)
    diagnostic = globals_dict["broken"]
    assert isinstance(diagnostic, DAG)
    assert diagnostic.is_paused_upon_creation is True
    assert "dpone_spec_error" in diagnostic.tags
    assert diagnostic.get_task("dpone_spec_error") is not None


def test_load_dpone_dags_skips_duplicate_without_overwriting_real_dag(tmp_path: Path) -> None:
    import pendulum

    index_path = _write_deployment_index_fixture(tmp_path)
    existing = DAG(
        dag_id="orders_daily",
        schedule=None,
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        catchup=False,
    )
    globals_dict: dict[str, object] = {"orders_daily": existing}

    report = load_dpone_dags(globals_dict, index_path=index_path)

    assert report.loaded == ()
    assert report.skipped == ({"dag_id": "orders_daily", "reason": "duplicate_dag_id"},)
    assert globals_dict["orders_daily"] is existing


def test_load_dpone_dags_repo_root_discovery_materializes_real_airflow_dag(tmp_path: Path) -> None:
    repo = tmp_path
    pack_dir = repo / ".dpone/gitops/airflow/load_orders"
    pack_dir.mkdir(parents=True)
    (pack_dir / "airflow-pack.json").write_text(
        json.dumps(_minimal_pack(task_id="load_orders__dpone_runtime"), sort_keys=True),
        encoding="utf-8",
    )
    payload = _valid_spec_payload(
        dag_id="DAG__repo_root",
        pack_ref=str(pack_dir / "airflow-pack.json"),
    )
    spec_dir = repo / ".dpone/gitops/airflow/_dags"
    spec_dir.mkdir(parents=True)
    (spec_dir / "DAG__repo_root.dag-spec.json").write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")

    globals_dict: dict[str, object] = {}
    report = load_dpone_dags(globals_dict, repo)

    assert report.errors == ()
    assert "DAG__repo_root" in globals_dict
    assert isinstance(globals_dict["DAG__repo_root"], DAG)


def test_provider_namespace_exports_load_dpone_dags() -> None:
    from airflow.providers.dpone import load_dpone_dags as namespace_loader
    from dpone_airflow_pack.provider import load_dpone_dags as provider_loader

    assert namespace_loader is provider_loader
    assert namespace_loader.__name__ == "load_dpone_dags"
