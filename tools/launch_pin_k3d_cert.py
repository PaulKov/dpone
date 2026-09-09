#!/usr/bin/env python3
"""Live K3d/Kind launch-pin ConfigMap CAS + RBAC certification harness.

Exercises the production ``KubernetesConfigMapLaunchPinStore`` against a real
Kubernetes API with namespace-scoped worker and runtime service accounts.

Evidence is written as JSON (default:
``test_artifacts/launch-pin-k3d-cert/receipt.json``).
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]


@dataclass
class ScenarioResult:
    name: str
    status: str
    detail: str = ""
    duration_ms: int = 0


@dataclass
class CertReceipt:
    schema: str = "dpone.launch-pin-k3d-cert.v1"
    status: str = "FAIL"
    certified_at: str = ""
    commit_sha: str = ""
    cluster: str = ""
    namespace: str = "airflow"
    kubernetes_version: str = ""
    scenarios: list[ScenarioResult] = field(default_factory=list)
    limitations: list[str] = field(default_factory=list)

    def to_json(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["scenarios"] = [asdict(item) for item in self.scenarios]
        return payload


def _git_head_sha() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"


def _run_identity() -> dict[str, object]:
    return {
        "schema": "dpone.airflow-run-identity.v1",
        "release_id": "sha256:" + "a" * 64,
        "deployment_id": "sha256:" + "b" * 64,
        "dag_spec": {"id": "orders_daily", "sha256": "sha256:" + "c" * 64},
        "workload_pack": {"id": "orders", "sha256": "sha256:" + "d" * 64},
        "runtime_image_digest": "sha256:" + "e" * 64,
        "binding_set_ref": "sha256:" + "1" * 64,
        "connection_registry_ref": "sha256:" + "2" * 64,
        "credential_runtime_ref": "sha256:" + "3" * 64,
        "airflow_bundle": {
            "backend": "git",
            "ref": "git:7ac31f2",
            "versioned": True,
            "version": "7ac31f2",
            "snapshot_ref": None,
        },
    }


def _deployment_identity() -> dict[str, object]:
    return {
        "schema": "dpone.airflow-deployment-identity.v1",
        "release_id": "sha256:" + "a" * 64,
        "deployment_id": "sha256:" + "b" * 64,
        "activation_id": "3f60628e-ef48-48b0-84c3-a9e27a82a7f2",
    }


def _load_clients(*, namespace: str) -> tuple[Any, Any, Any, Any]:
    from kubernetes import client, config
    from kubernetes.client import ApiException

    kubeconfig = os.environ.get("KUBECONFIG")
    if kubeconfig:
        config.load_kube_config(config_file=kubeconfig)
    else:
        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()

    admin = client.ApiClient()
    rbac = client.RbacAuthorizationV1Api(admin)
    core = client.CoreV1Api(admin)

    worker_sa = "dpone-lp-worker"
    runtime_sa = "dpone-lp-runtime"
    for sa_name in (worker_sa, runtime_sa):
        try:
            core.read_namespaced_service_account(name=sa_name, namespace=namespace)
        except ApiException as exc:
            if exc.status != 404:
                raise
            core.create_namespaced_service_account(
                namespace=namespace,
                body={"metadata": {"name": sa_name}},
            )

    role_specs = {
        "dpone-lp-worker": [
            {
                "apiGroups": [""],
                "resources": ["configmaps"],
                "verbs": ["get", "list", "watch", "create", "update", "patch", "delete"],
            },
            {
                "apiGroups": [""],
                "resources": ["pods"],
                "verbs": ["get", "list", "watch", "delete"],
            },
        ],
        "dpone-lp-runtime": [
            {
                "apiGroups": [""],
                "resources": ["configmaps"],
                "verbs": ["get", "list", "watch"],
            },
        ],
    }
    for role_name, rules in role_specs.items():
        sa_name = role_name
        try:
            rbac.read_namespaced_role(name=role_name, namespace=namespace)
        except ApiException as exc:
            if exc.status != 404:
                raise
            rbac.create_namespaced_role(
                namespace=namespace,
                body={"metadata": {"name": role_name}, "rules": rules},
            )
        binding_name = f"{role_name}-binding"
        try:
            rbac.read_namespaced_role_binding(name=binding_name, namespace=namespace)
        except ApiException as exc:
            if exc.status != 404:
                raise
            rbac.create_namespaced_role_binding(
                namespace=namespace,
                body={
                    "metadata": {"name": binding_name},
                    "roleRef": {
                        "apiGroup": "rbac.authorization.k8s.io",
                        "kind": "Role",
                        "name": role_name,
                    },
                    "subjects": [
                        {
                            "kind": "ServiceAccount",
                            "name": sa_name,
                            "namespace": namespace,
                        }
                    ],
                },
            )

    def _client_for_sa(sa_name: str) -> client.CoreV1Api:
        token = subprocess.check_output(
            [
                "kubectl",
                "create",
                "token",
                sa_name,
                "-n",
                namespace,
                "--duration=3600s",
            ],
            text=True,
        ).strip()
        cfg = client.Configuration()
        if kubeconfig:
            config.load_kube_config(config_file=kubeconfig, client_configuration=cfg)
        else:
            config.load_kube_config(client_configuration=cfg)
        cfg.api_key = {"authorization": f"Bearer {token}"}
        cfg.api_key_prefix = {}
        cfg.username = None
        cfg.password = None
        cfg.cert_file = None
        cfg.key_file = None
        cfg.token = None
        return client.CoreV1Api(client.ApiClient(configuration=cfg))

    worker_core = _client_for_sa(worker_sa)
    runtime_core = _client_for_sa(runtime_sa)
    time.sleep(2)  # RBAC propagation buffer for token-bound clients.
    return admin, core, worker_core, runtime_core


def _store_for_core(core_api: Any, *, namespace: str) -> Any:
    from dpone_airflow_pack.launch_pin import build_launch_pin
    from dpone_airflow_pack.launch_pin_k8s_store import KubernetesConfigMapLaunchPinStore

    return KubernetesConfigMapLaunchPinStore(namespace=namespace, api=core_api), build_launch_pin


def _timed(fn: Any) -> ScenarioResult:
    started = time.perf_counter()
    try:
        detail = fn()
        elapsed = int((time.perf_counter() - started) * 1000)
        return ScenarioResult(
            name=fn.__name__.removeprefix("_scenario_"), status="PASS", detail=detail or "", duration_ms=elapsed
        )
    except Exception as exc:  # noqa: BLE001 - certification boundary
        elapsed = int((time.perf_counter() - started) * 1000)
        return ScenarioResult(
            name=fn.__name__.removeprefix("_scenario_"),
            status="FAIL",
            detail=f"{type(exc).__name__}: {exc}",
            duration_ms=elapsed,
        )


def _scenario_worker_configmap_cas(store: Any, build_launch_pin: Any, *, run_suffix: str) -> str:
    pin = build_launch_pin(
        attempt={
            "dag_id": f"cert-{run_suffix}",
            "run_id": "run",
            "task_id": "orders__runtime",
            "map_index": -1,
            "try_number": 1,
        },
        pod_namespace="airflow",
        pod_name=f"pod-a-{run_suffix}",
        pod_uid=str(uuid.uuid4()),
        run_identity=_run_identity(),
        deployment_identity=_deployment_identity(),
        expected_runtime_evidence_sha256=None,
        kubernetes_conn_id="kubernetes_default",
    )
    created = store.create_once(pin)
    assert created["state"] == "ACTIVE"
    read_back = store.get(
        dag_id=f"cert-{run_suffix}",
        run_id="run",
        task_id="orders__runtime",
        map_index=-1,
    )
    assert read_back is not None
    assert read_back["pod_uid"] == pin["pod_uid"]
    return f"ACTIVE pin uid={pin['pod_uid']}"


def _scenario_worker_pod_delete(worker_core: Any, admin_core: Any, *, run_suffix: str) -> str:
    from kubernetes import client

    name = f"dpone-lp-cert-pod-{run_suffix}"
    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(name=name, namespace="airflow"),
        spec=client.V1PodSpec(
            restart_policy="Never",
            containers=[
                client.V1Container(name="pause", image="rancher/mirrored-pause:3.6", command=["/pause"]),
            ],
        ),
    )
    # Platform/KPO creates retained pods; cleanup worker SA only needs get/delete.
    admin_core.create_namespaced_pod(namespace="airflow", body=pod)
    worker_core.delete_namespaced_pod(name=name, namespace="airflow")
    return f"deleted pod {name} (created by platform, deleted by worker SA)"


def _scenario_runtime_configmap_get_only(runtime_core: Any, worker_core: Any, *, run_suffix: str) -> str:
    from kubernetes import client
    from kubernetes.client import ApiException

    cm_name = f"dpone-lp-runtime-get-{run_suffix}"
    worker_core.create_namespaced_config_map(
        namespace="airflow",
        body=client.V1ConfigMap(
            metadata=client.V1ObjectMeta(name=cm_name, namespace="airflow"),
            data={"probe": "1"},
        ),
    )
    runtime_core.read_namespaced_config_map(name=cm_name, namespace="airflow")
    try:
        runtime_core.create_namespaced_config_map(
            namespace="airflow",
            body=client.V1ConfigMap(
                metadata=client.V1ObjectMeta(name=f"dpone-lp-runtime-deny-{run_suffix}", namespace="airflow"),
                data={"probe": "1"},
            ),
        )
    except ApiException as exc:
        if exc.status in {403, 401}:
            worker_core.delete_namespaced_config_map(name=cm_name, namespace="airflow")
            return "runtime get ok; create denied as expected"
        raise
    raise AssertionError("runtime SA must not create configmaps")


def _scenario_multi_worker_cas_single_winner(store_factory: Any, build_launch_pin: Any, *, run_suffix: str) -> str:

    store_a, _ = store_factory()
    store_b, _ = store_factory()
    subject = f"cert-mw-{run_suffix}"
    pin1 = build_launch_pin(
        attempt={"dag_id": subject, "run_id": "run", "task_id": "t", "map_index": -1, "try_number": 1},
        pod_namespace="airflow",
        pod_name=f"pod-a-{run_suffix}",
        pod_uid=str(uuid.uuid4()),
        run_identity=_run_identity(),
        deployment_identity=_deployment_identity(),
        expected_runtime_evidence_sha256=None,
        kubernetes_conn_id="kubernetes_default",
    )
    pin2 = build_launch_pin(
        attempt={"dag_id": subject, "run_id": "run", "task_id": "t", "map_index": -1, "try_number": 2},
        pod_namespace="airflow",
        pod_name=f"pod-b-{run_suffix}",
        pod_uid=str(uuid.uuid4()),
        run_identity=_run_identity(),
        deployment_identity=_deployment_identity(),
        expected_runtime_evidence_sha256=None,
        kubernetes_conn_id="kubernetes_default",
    )
    results: list[Any] = []
    barrier = threading.Barrier(2)

    def _attempt(store: Any, pin: dict[str, object]) -> None:
        barrier.wait(timeout=5)
        try:
            results.append(store.create_once(pin))
        except BaseException as exc:  # noqa: BLE001
            results.append(exc)

    threads = [
        threading.Thread(target=_attempt, args=(store_a, pin1)),
        threading.Thread(target=_attempt, args=(store_b, pin2)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
    winners = [item for item in results if isinstance(item, dict)]
    losers = [item for item in results if not isinstance(item, dict)]
    assert len(winners) == 1, results
    assert len(losers) == 1, results
    assert winners[0]["state"] == "ACTIVE"
    head = store_a.get(dag_id=subject, run_id="run", task_id="t", map_index=-1)
    assert head is not None and head["state"] == "ACTIVE"
    return f"single ACTIVE winner try={winners[0]['try_number']}"


def _scenario_runtime_reads_active_barrier(
    runtime_core: Any, store: Any, build_launch_pin: Any, *, run_suffix: str
) -> str:
    pin = build_launch_pin(
        attempt={
            "dag_id": f"cert-barrier-{run_suffix}",
            "run_id": "run",
            "task_id": "t",
            "map_index": -1,
            "try_number": 1,
        },
        pod_namespace="airflow",
        pod_name=f"pod-barrier-{run_suffix}",
        pod_uid=str(uuid.uuid4()),
        run_identity=_run_identity(),
        deployment_identity=_deployment_identity(),
        expected_runtime_evidence_sha256=None,
        kubernetes_conn_id="kubernetes_default",
    )
    active = store.create_once(pin)
    from dpone_airflow_pack.launch_pin_k8s_validate import configmap_name_for_subject

    per_try = configmap_name_for_subject(
        dag_id=f"cert-barrier-{run_suffix}",
        run_id="run",
        task_id="t",
        map_index=-1,
        try_number=1,
    )
    runtime_core.read_namespaced_config_map(name=per_try, namespace="airflow")
    assert active["state"] == "ACTIVE"
    return f"runtime read ACTIVE per-try {per_try}"


def run_certification(*, namespace: str, cluster: str) -> CertReceipt:
    receipt = CertReceipt(
        certified_at=datetime.now(tz=UTC).isoformat(),
        commit_sha=_git_head_sha(),
        cluster=cluster,
        namespace=namespace,
        limitations=[
            "Single-node k3d cluster with two logical worker clients (not separate Airflow worker pods).",
            "Does not exercise full Airflow KPO/barrier init container or outcome_gate task graph.",
            "Gate/cleanup workers reuse worker RBAC profile (same verbs as documented worker SA).",
        ],
    )
    try:
        version_json = subprocess.check_output(["kubectl", "version", "--output=json"], text=True)
        receipt.kubernetes_version = json.loads(version_json).get("serverVersion", {}).get("gitVersion", "unknown")
    except (subprocess.CalledProcessError, json.JSONDecodeError, FileNotFoundError):
        receipt.kubernetes_version = "unknown"

    _admin, core, worker_core, runtime_core = _load_clients(namespace=namespace)
    run_suffix = uuid.uuid4().hex[:8]
    worker_store, build_launch_pin = _store_for_core(worker_core, namespace=namespace)

    def store_factory() -> tuple[Any, Any]:
        return _store_for_core(worker_core, namespace=namespace)

    scenarios = [
        lambda: _scenario_worker_configmap_cas(worker_store, build_launch_pin, run_suffix=run_suffix),
        lambda: _scenario_worker_pod_delete(worker_core, core, run_suffix=run_suffix),
        lambda: _scenario_runtime_configmap_get_only(runtime_core, worker_core, run_suffix=run_suffix),
        lambda: _scenario_multi_worker_cas_single_winner(store_factory, build_launch_pin, run_suffix=run_suffix),
        lambda: _scenario_runtime_reads_active_barrier(
            runtime_core, worker_store, build_launch_pin, run_suffix=run_suffix
        ),
    ]
    for scenario in scenarios:
        started = time.perf_counter()
        try:
            detail = scenario()
            receipt.scenarios.append(
                ScenarioResult(
                    name=scenario.__name__ if hasattr(scenario, "__name__") else "scenario",
                    status="PASS",
                    detail=detail,
                    duration_ms=int((time.perf_counter() - started) * 1000),
                )
            )
        except Exception as exc:  # noqa: BLE001
            receipt.scenarios.append(
                ScenarioResult(
                    name=getattr(scenario, "__name__", "scenario"),
                    status="FAIL",
                    detail=f"{type(exc).__name__}: {exc}",
                    duration_ms=int((time.perf_counter() - started) * 1000),
                )
            )

    # Name scenarios explicitly (lambdas lose names).
    names = [
        "worker_configmap_cas",
        "worker_pod_delete",
        "runtime_configmap_get_only",
        "multi_worker_cas_single_winner",
        "runtime_reads_active_barrier",
    ]
    for idx, name in enumerate(names):
        if idx < len(receipt.scenarios):
            receipt.scenarios[idx].name = name

    receipt.status = "PASS" if all(item.status == "PASS" for item in receipt.scenarios) else "FAIL"
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", default="airflow")
    parser.add_argument("--cluster", default=os.environ.get("LAUNCH_PIN_CERT_CLUSTER", "unknown"))
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "test_artifacts" / "launch-pin-k3d-cert" / "receipt.json",
    )
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    receipt = run_certification(namespace=args.namespace, cluster=args.cluster)
    args.output.write_text(json.dumps(receipt.to_json(), indent=2) + "\n", encoding="utf-8")
    print(json.dumps(receipt.to_json(), indent=2))
    return 0 if receipt.status == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
