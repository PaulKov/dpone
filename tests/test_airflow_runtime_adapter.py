from __future__ import annotations

import json
import sys
import types
from pathlib import Path

import pytest

from dpone.airflow.pack_tasks import build_dpone_gitops_task_group_from_pack, load_dpone_airflow_pack
from dpone.airflow.runtime_adapter import (
    DponeAirflowContractError,
    build_dpone_gitops_task_from_artifacts,
    load_dpone_kpo_kwargs,
    validate_dpone_artifacts,
)
from dpone.airflow.step_tasks import build_dpone_gitops_step_tasks_from_artifacts


def _artifact_dir(tmp_path: Path) -> Path:
    artifact_dir = tmp_path / ".dpone/gitops/airflow"
    artifact_dir.mkdir(parents=True)
    pod_spec_path = ".dpone/gitops/airflow/pod-spec.yaml"
    (artifact_dir / "kpo-kwargs.json").write_text(
        json.dumps(
            {
                "task_id": "generated",
                "name": "generated",
                "namespace": "dpone-runners",
                "pod_template_file": pod_spec_path,
                "do_xcom_push": True,
                "cmds": ["/bin/sh", "-ec"],
                "arguments": ["dpone gitops airflow run-spec-exec .dpone/gitops/airflow/run-spec.json"],
                "labels": {"app.kubernetes.io/name": "dpone"},
                "annotations": {"dpone.dev/runner": "airflow"},
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (artifact_dir / "pod-spec.yaml").write_text(
        "\n".join(
            [
                "apiVersion: v1",
                "kind: Pod",
                "metadata:",
                "  name: dpone-runtime",
                "spec:",
                "  containers:",
                "    - name: base",
                "      image: ghcr.io/acme/dpone:2026.06.16",
            ]
        ),
        encoding="utf-8",
    )
    (artifact_dir / "pod-contract.json").write_text(
        json.dumps(
            {
                "kind": "gitops.airflow_pod_contract",
                "pod_spec_path": pod_spec_path,
                "xcom": {"enabled": True, "return_path": "/airflow/xcom/return.json"},
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (artifact_dir / "run-spec.json").write_text(
        json.dumps(
            {
                "steps": [
                    {
                        "name": "pre_hook_refresh_orders",
                        "kind": "source_refresh",
                        "command": "dpone hooks execute manifests/orders.yaml --phase pre_hook --hook-id refresh_orders",
                    },
                    {
                        "name": "dpone_run_orders",
                        "kind": "dpone_run",
                        "command": "dpone run manifests/orders.yaml",
                        "depends_on": ["pre_hook_refresh_orders"],
                    },
                ]
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return artifact_dir


def _install_fake_airflow(monkeypatch: pytest.MonkeyPatch) -> None:
    airflow = types.ModuleType("airflow")
    providers = types.ModuleType("airflow.providers")
    standard = types.ModuleType("airflow.providers.standard")
    standard_operators = types.ModuleType("airflow.providers.standard.operators")
    python = types.ModuleType("airflow.providers.standard.operators.python")
    cncf = types.ModuleType("airflow.providers.cncf")
    kubernetes = types.ModuleType("airflow.providers.cncf.kubernetes")
    operators = types.ModuleType("airflow.providers.cncf.kubernetes.operators")
    pod = types.ModuleType("airflow.providers.cncf.kubernetes.operators.pod")

    class DependencyOperator:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs
            self.downstream: list[object] = []

        def __rshift__(self, downstream: object) -> object:
            self.downstream.append(downstream)
            return downstream

    class KubernetesPodOperator(DependencyOperator):
        def build_pod_request_obj(self, context: object | None = None) -> object:
            return self.kwargs.get("full_pod_spec") or types.SimpleNamespace(spec=types.SimpleNamespace(containers=[]))

    class PythonOperator(DependencyOperator):
        pass

    pod.KubernetesPodOperator = KubernetesPodOperator
    python.PythonOperator = PythonOperator
    for name, module in {
        "airflow": airflow,
        "airflow.providers": providers,
        "airflow.providers.standard": standard,
        "airflow.providers.standard.operators": standard_operators,
        "airflow.providers.standard.operators.python": python,
        "airflow.providers.cncf": cncf,
        "airflow.providers.cncf.kubernetes": kubernetes,
        "airflow.providers.cncf.kubernetes.operators": operators,
        "airflow.providers.cncf.kubernetes.operators.pod": pod,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)


def test_runtime_adapter_validates_and_loads_dag_owned_kpo_kwargs(tmp_path: Path) -> None:
    artifact_dir = _artifact_dir(tmp_path)

    report = validate_dpone_artifacts(artifact_dir)
    kwargs = load_dpone_kpo_kwargs(
        artifact_dir,
        task_id="orders_to_clickhouse",
        name="orders-runtime",
        labels={"team": "dwh"},
        annotations={"owner": "analytics"},
        operator_overrides={"pool": "dpone", "retries": 2},
    )

    assert report["kind"] == "dpone.airflow_runtime_artifacts"
    assert [entry["kind"] for entry in report["entries"]] == ["kpo_kwargs", "pod_spec", "pod_contract"]
    assert kwargs["task_id"] == "orders_to_clickhouse"
    assert kwargs["name"] == "orders-runtime"
    assert kwargs["pod_template_file"] == str(artifact_dir / "pod-spec.yaml")
    assert kwargs["do_xcom_push"] is True
    assert kwargs["labels"] == {"app.kubernetes.io/name": "dpone", "team": "dwh"}
    assert kwargs["annotations"] == {"dpone.dev/runner": "airflow", "owner": "analytics"}
    assert kwargs["pool"] == "dpone"
    assert kwargs["retries"] == 2


def test_runtime_adapter_builds_kpo_lazily_with_fake_airflow(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact_dir = _artifact_dir(tmp_path)
    _install_fake_airflow(monkeypatch)

    task = build_dpone_gitops_task_from_artifacts(
        dag=object(),
        artifact_dir=artifact_dir,
        task_id="orders_to_clickhouse",
    )

    assert task.kwargs["task_id"] == "orders_to_clickhouse"
    assert task.kwargs["pod_template_file"] == str(artifact_dir / "pod-spec.yaml")
    assert task.kwargs["do_xcom_push"] is True


def test_runtime_adapter_builds_visible_step_kpos_from_run_spec(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact_dir = _artifact_dir(tmp_path)
    _install_fake_airflow(monkeypatch)

    tasks = build_dpone_gitops_step_tasks_from_artifacts(dag=object(), artifact_dir=artifact_dir)

    assert list(tasks) == ["pre_hook_refresh_orders", "dpone_run_orders"]
    assert tasks["pre_hook_refresh_orders"].kwargs["task_id"] == "pre_hook_refresh_orders"
    assert tasks["pre_hook_refresh_orders"].kwargs["arguments"] == [
        "dpone hooks execute manifests/orders.yaml --phase pre_hook --hook-id refresh_orders"
    ]
    assert tasks["pre_hook_refresh_orders"].kwargs["do_xcom_push"] is False
    assert tasks["dpone_run_orders"].kwargs["arguments"] == ["dpone run manifests/orders.yaml"]
    assert tasks["dpone_run_orders"].kwargs["do_xcom_push"] is True
    assert tasks["pre_hook_refresh_orders"].downstream == [tasks["dpone_run_orders"]]


def test_pack_tasks_build_runtime_task_from_compact_pack(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pack_path = tmp_path / ".dpone/gitops/airflow/orders/airflow-pack.json"
    pack_path.parent.mkdir(parents=True)
    pack_path.write_text(
        json.dumps(
            {
                "kind": "gitops.airflow_pack",
                "workload": {"workload_id": "orders"},
                "airflow": {
                    "execution": {
                        "task_executor": "KubernetesExecutor",
                        "deferrable": False,
                        "on_finish_action": "delete_succeeded_pod",
                        "get_logs": True,
                        "logging_interval_seconds": 60,
                    }
                },
                "kpo_kwargs": {
                    "task_id": "orders__dpone_runtime",
                    "name": "dpone-orders",
                    "namespace": "airflow-dev",
                    "image": "registry.example/dpone:dev",
                    "cmds": ["dpone"],
                    "arguments": ["run", "manifests/orders.yaml", "--format", "json"],
                    "labels": {"dpone.dev/workload-id": "orders"},
                    "resources_profile": "throughput",
                    "outcome_mode": "xcom_then_gate",
                },
            }
        ),
        encoding="utf-8",
    )
    _install_fake_airflow(monkeypatch)

    pack = load_dpone_airflow_pack(pack_path)
    tasks = build_dpone_gitops_task_group_from_pack(pack_path, dag=object(), operator_overrides={"retries": 1})

    assert pack["kind"] == "gitops.airflow_pack"
    assert list(tasks) == ["dpone_runtime"]
    assert tasks["dpone_runtime"].kwargs["task_id"] == "orders__dpone_runtime"
    assert tasks["dpone_runtime"].kwargs["image"] == "registry.example/dpone:dev"
    assert tasks["dpone_runtime"].kwargs["do_xcom_push"] is True
    assert tasks["dpone_runtime"].kwargs["executor"] == "KubernetesExecutor"
    assert tasks["dpone_runtime"].kwargs["deferrable"] is False
    assert tasks["dpone_runtime"].kwargs["on_finish_action"] == "delete_succeeded_pod"
    assert tasks["dpone_runtime"].kwargs["get_logs"] is True
    assert tasks["dpone_runtime"].kwargs["logging_interval"] == 60
    assert tasks["dpone_runtime"].kwargs["retries"] == 1
    assert "task_executor" not in tasks["dpone_runtime"].kwargs
    assert "logging_interval_seconds" not in tasks["dpone_runtime"].kwargs
    assert "resources_profile" not in tasks["dpone_runtime"].kwargs
    assert "outcome_mode" not in tasks["dpone_runtime"].kwargs


def test_pack_tasks_build_runtime_and_outcome_gate_from_self_contained_pack(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pack_path = tmp_path / ".dpone/gitops/airflow/orders/airflow-pack.json"
    pack_path.parent.mkdir(parents=True)
    pack_path.write_text(
        json.dumps(
            {
                "kind": "gitops.airflow_pack",
                "schema_version": "3",
                "workload": {"workload_id": "orders"},
                "runtime_command": "status=0; dpone run manifests/orders.yaml || status=$?; exit 0",
                "pod_spec": {
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "dpone-orders", "namespace": "airflow-dev"},
                    "spec": {
                        "serviceAccountName": "airflow-sa",
                        "containers": [{"name": "base", "image": "registry.example/dpone:dev"}],
                        "initContainers": [{"name": "dpone-inline-workload-bootstrap"}],
                    },
                },
                "connection_projection": {
                    "mode": "unsafe_airflow_env",
                    "connection_ids": ["mssql_prod", "ClickHouse"],
                    "database_overrides": {"ClickHouse": "DWH_Raw"},
                    "scheme_overrides": {"ClickHouse": "clickhouse"},
                    "query_overrides": {"mssql_prod": {"trust_server_certificate": "yes"}},
                },
                "xcom": {"sidecar_image": "harbor.example/alpine:3.23.4"},
                "outcome_gate": {"task_id": "orders__dpone_outcome_gate", "required_status": "passed"},
                "kpo_kwargs": {
                    "task_id": "orders__dpone_runtime",
                    "name": "dpone-orders",
                    "namespace": "airflow-dev",
                    "image": "registry.example/dpone:dev",
                    "cmds": ["dpone"],
                    "arguments": ["run", "manifests/orders.yaml", "--format", "json"],
                    "labels": {"dpone.dev/workload-id": "orders"},
                },
            }
        ),
        encoding="utf-8",
    )
    _install_fake_airflow(monkeypatch)

    tasks = build_dpone_gitops_task_group_from_pack(pack_path, dag=object(), operator_overrides={"retries": 1})
    runtime = tasks["dpone_runtime"]
    outcome_gate = tasks["outcome_gate"]

    assert sorted(tasks) == ["dpone_runtime", "outcome_gate"]
    assert runtime.kwargs["task_id"] == "orders__dpone_runtime"
    assert runtime.kwargs["cmds"] == ["/bin/sh", "-ec"]
    assert runtime.kwargs["arguments"] == ["status=0; dpone run manifests/orders.yaml || status=$?; exit 0"]
    assert runtime.kwargs["do_xcom_push"] is True
    assert runtime.kwargs["retries"] == 1
    assert runtime.unsafe_airflow_connection_ids == ("mssql_prod", "ClickHouse")
    assert runtime.unsafe_runtime_database_overrides == {"ClickHouse": "DWH_Raw"}
    assert runtime.unsafe_runtime_scheme_overrides == {"ClickHouse": "clickhouse"}
    assert runtime.unsafe_runtime_query_overrides == {"mssql_prod": {"trust_server_certificate": "yes"}}
    assert runtime.xcom_sidecar.image == "harbor.example/alpine:3.23.4"
    full_pod_spec = runtime.kwargs["full_pod_spec"]
    if isinstance(full_pod_spec, dict):
        init_container_name = full_pod_spec["spec"]["initContainers"][0]["name"]
    else:
        init_container_name = full_pod_spec.spec.init_containers[0].name
    assert init_container_name == "dpone-inline-workload-bootstrap"
    assert outcome_gate.kwargs["task_id"] == "orders__dpone_outcome_gate"
    assert outcome_gate.kwargs["op_kwargs"]["upstream_task_id"] == "orders__dpone_runtime"


def test_lightweight_outcome_gate_preserves_non_blocking_xcom_warnings() -> None:
    from dpone_airflow_pack.outcome_gate import GitOpsAirflowOutcomeGateEvaluator

    report = GitOpsAirflowOutcomeGateEvaluator().evaluate(
        xcom_summary_path="xcom://orders__dpone_runtime",
        required_status="passed",
        xcom_summary={
            "kind": "gitops.airflow_xcom_summary",
            "schema_version": "1",
            "producer": "dpone gitops airflow xcom-from-evidence",
            "status": "passed",
            "run_spec_path": "",
            "runtime_evidence_path": ".dpone/runs/orders/runtime-evidence.json",
            "runtime_evidence_sha256": "sha256:" + ("a" * 64),
            "warnings": [
                {
                    "code": "runtime_evidence_xcom_build_unavailable",
                    "message": "Runtime evidence used fallback status hint",
                    "path": ".dpone/runs/orders/runtime-evidence.json",
                    "source": "dpone gitops airflow xcom-from-evidence",
                }
            ],
            "blockers": [],
        },
    )

    assert report.passed is True
    assert report.blockers == ()
    assert report.warnings[0].code == "runtime_evidence_xcom_build_unavailable"


def test_pack_tasks_build_visible_pre_hook_task_from_compact_pack(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pack_path = tmp_path / ".dpone/gitops/airflow/orders/airflow-pack.json"
    pack_path.parent.mkdir(parents=True)
    pack_path.write_text(
        json.dumps(
            {
                "kind": "gitops.airflow_pack",
                "schema_version": "3",
                "workload": {"workload_id": "orders"},
                "runtime_command": "DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS=1 dpone run manifests/orders.yaml",
                "steps": [
                    {
                        "name": "pre_hook_refresh_orders",
                        "phase": "pre_hook",
                        "command": "dpone hooks execute manifests/orders.yaml --phase pre_hook --hook-id refresh_orders",
                        "depends_on": [],
                    },
                    {"name": "dpone_runtime", "phase": "runtime", "depends_on": ["pre_hook_refresh_orders"]},
                ],
                "kpo_kwargs": {
                    "task_id": "orders__dpone_runtime",
                    "name": "dpone-orders",
                    "namespace": "airflow-dev",
                    "image": "registry.example/dpone:dev",
                    "labels": {"dpone.dev/workload-id": "orders"},
                },
            }
        ),
        encoding="utf-8",
    )
    _install_fake_airflow(monkeypatch)

    tasks = build_dpone_gitops_task_group_from_pack(pack_path, dag=object(), operator_overrides={"retries": 1})

    assert list(tasks) == ["pre_hook_refresh_orders", "dpone_runtime"]
    assert tasks["pre_hook_refresh_orders"].kwargs["task_id"] == "pre_hook_refresh_orders"
    assert tasks["pre_hook_refresh_orders"].kwargs["arguments"] == [
        "dpone hooks execute manifests/orders.yaml --phase pre_hook --hook-id refresh_orders"
    ]
    assert tasks["pre_hook_refresh_orders"].kwargs["do_xcom_push"] is False
    assert tasks["pre_hook_refresh_orders"].kwargs["retries"] == 1


@pytest.mark.parametrize("field", ["pod_template_file", "do_xcom_push", "cmds", "arguments"])
def test_runtime_adapter_rejects_contract_owned_overrides(tmp_path: Path, field: str) -> None:
    artifact_dir = _artifact_dir(tmp_path)

    with pytest.raises(ValueError, match=field):
        load_dpone_kpo_kwargs(artifact_dir, operator_overrides={field: "unsafe"})


def test_runtime_adapter_blocks_missing_or_drifted_artifacts(tmp_path: Path) -> None:
    artifact_dir = _artifact_dir(tmp_path)
    (artifact_dir / "pod-spec.yaml").unlink()

    with pytest.raises(DponeAirflowContractError) as exc_info:
        validate_dpone_artifacts(artifact_dir)

    assert any(blocker["code"] == "pod_spec_missing" for blocker in exc_info.value.blockers)


@pytest.mark.parametrize(
    "module_name",
    [
        "dpone_airflow_pack.pack_tasks",
        "dpone_airflow_pack.step_tasks",
    ],
)
def test_airflow_dependency_wiring_does_not_silently_drop_operator_type_errors(module_name: str) -> None:
    module = __import__(module_name, fromlist=["_chain"])

    class BrokenOperator:
        def __rshift__(self, downstream: object) -> object:
            raise TypeError("dependency wiring rejected")

    with pytest.raises(TypeError, match="dependency wiring rejected"):
        module._chain(BrokenOperator(), object())
