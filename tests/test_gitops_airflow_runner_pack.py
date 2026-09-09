from __future__ import annotations

import json
import logging
import subprocess
import sys
import types
from argparse import Namespace
from pathlib import Path

from dpone.adapters.fs_local import LocalFileSystem
from dpone.adapters.yaml_pyyaml import PyYamlCodec
from dpone.app.context import AppContext
from dpone.app.settings import Settings
from dpone.services.gitops.airflow_doctor_service import GitOpsAirflowDoctorService
from dpone.services.gitops.airflow_image_contract_service import GitOpsAirflowImageContractService
from dpone.services.gitops.airflow_render_service import GitOpsAirflowRenderService
from dpone.services.gitops.bundle_service import GitOpsBundleService


def _ctx(tmp_path: Path) -> AppContext:
    settings = Settings(repo_root=tmp_path, project_dir=tmp_path, manifest_dir=tmp_path, sources_registry_paths=())
    return AppContext(settings=settings, logger=logging.getLogger("test"), fs=LocalFileSystem(), yaml=PyYamlCodec())


def _bundle_args(**overrides: object) -> Namespace:
    data = {
        "changed_files": ["dpone_workloads/manifests/seed.yaml"],
        "changed_files_file": None,
        "from_ref": None,
        "to_ref": None,
        "workload_root": None,
        "manifest_glob": "manifests/**/*.yaml",
        "include_global_overrides": False,
        "include_env_overrides": [],
        "include_registry": False,
        "registry": [],
        "support_path": [],
        "runner": "kubernetes_pod_operator",
        "worktree": ".",
        "verify_lock": True,
        "fail_on_empty_impact": False,
        "fail_on_warnings": False,
        "require_lock": True,
        "policy_profile": "release",
        "attest": True,
        "output_dir": ".dpone/gitops/bundle",
        "output": None,
        "format": "json",
    }
    data.update(overrides)
    return Namespace(**data)


def _render_args(**overrides: object) -> Namespace:
    data = {
        "bundle_path": ".dpone/gitops/bundle/bundle.json",
        "output_dir": ".dpone/gitops/airflow",
        "image": "ghcr.io/acme/dpone:2026.06.16",
        "image_digest": None,
        "dpone_version": None,
        "python_version": None,
        "airflow_provider_version": None,
        "tool": [],
        "user": None,
        "workdir": None,
        "entrypoint": None,
        "image_contract": ".dpone/gitops/airflow/image-contract.json",
        "namespace": "dpone-runners",
        "service_account": "dpone-runner",
        "dag_id": "dpone_gitops",
        "task_id": "dpone_orders",
        "require_attestation": True,
        "format": "json",
        "output": None,
    }
    data.update(overrides)
    return Namespace(**data)


def _doctor_args(**overrides: object) -> Namespace:
    data = {
        "bundle_path": ".dpone/gitops/bundle/bundle.json",
        "pod_template": ".dpone/gitops/airflow/pod_template.yaml",
        "image_contract": ".dpone/gitops/airflow/image-contract.json",
        "image": "ghcr.io/acme/dpone:2026.06.16",
        "require_attestation": True,
        "runner_policy": "advisory",
        "format": "json",
        "output": None,
    }
    data.update(overrides)
    return Namespace(**data)


def _image_contract_args(**overrides: object) -> Namespace:
    data = {
        "image": "ghcr.io/acme/dpone:2026.06.16",
        "image_digest": "sha256:" + "a" * 64,
        "dpone_version": "0.11.0",
        "python_version": "3.12",
        "airflow_provider_version": "10.18.0",
        "tool": ["dpone", "bcp", "clickhouse-client"],
        "user": "10001",
        "workdir": "/workspace",
        "entrypoint": "/opt/dpone/entrypoint.sh",
        "output_path": ".dpone/gitops/airflow/image-contract.json",
        "format": "json",
        "output": None,
    }
    data.update(overrides)
    return Namespace(**data)


def _run_spec_args(**overrides: object) -> Namespace:
    data = {
        "bundle_path": ".dpone/gitops/bundle/bundle.json",
        "output_path": ".dpone/gitops/airflow/run-spec.json",
        "evidence_output": ".dpone/gitops/airflow/runtime-evidence.json",
        "image": "ghcr.io/acme/dpone:2026.06.16",
        "image_digest": "sha256:" + "d" * 64,
        "worktree": ".",
        "require_attestation": True,
        "format": "json",
        "output": None,
    }
    data.update(overrides)
    return Namespace(**data)


def _runtime_profile_args(**overrides: object) -> Namespace:
    data = {
        "bundle_path": ".dpone/gitops/bundle/bundle.json",
        "run_spec_path": ".dpone/gitops/airflow/run-spec.json",
        "runtime_evidence_path": ".dpone/gitops/airflow/runtime-evidence.json",
        "output_path": ".dpone/gitops/airflow/runtime-profile.json",
        "xcom_summary_path": ".dpone/gitops/airflow/xcom-summary.json",
        "dag_factory_path": ".dpone/gitops/airflow/airflow_dag_factory.py",
        "outcome_gate_path": ".dpone/gitops/airflow/outcome_gate.py",
        "image": "ghcr.io/acme/dpone:2026.06.16",
        "image_digest": "sha256:" + "f" * 64,
        "namespace": "dpone-runners",
        "service_account": "dpone-runner",
        "cpu_request": "250m",
        "memory_request": "512Mi",
        "cpu_limit": "2",
        "memory_limit": "2Gi",
        "artifact_sink_kind": "local",
        "artifact_sink_path": ".dpone/gitops/airflow",
        "label": ["app.kubernetes.io/name=dpone"],
        "annotation": ["dpone.dev/runner=airflow"],
        "env": ["DPONE_PROFILE=release"],
        "runner_policy": "release",
        "outcome_mode": "strict_fail",
        "format": "json",
        "output": None,
    }
    data.update(overrides)
    return Namespace(**data)


def _pod_contract_args(**overrides: object) -> Namespace:
    data = {
        "bundle_path": ".dpone/gitops/bundle/bundle.json",
        "run_spec_path": ".dpone/gitops/airflow/run-spec.json",
        "runtime_profile_path": ".dpone/gitops/airflow/runtime-profile.json",
        "output_path": ".dpone/gitops/airflow/pod-contract.json",
        "pod_spec_path": ".dpone/gitops/airflow/pod-spec.yaml",
        "kpo_kwargs_path": ".dpone/gitops/airflow/kpo-kwargs.json",
        "image_pull_secret": ["regcred"],
        "volume": ["dpone-artifacts=.dpone/gitops/airflow"],
        "volume_mount": ["dpone-artifacts=/workspace/.dpone/gitops/airflow:ro"],
        "env_from_configmap": ["dpone-runner-config"],
        "env_secret": ["DPONE_TOKEN=dpone-token:token"],
        "node_selector": ["workload=dpone"],
        "toleration": ["dedicated=dpone:NoSchedule"],
        "label": ["app.kubernetes.io/component=dpone-runner"],
        "annotation": ["dpone.dev/pod-contract=enabled"],
        "on_finish_action": "delete_pod",
        "get_logs": True,
        "deferrable": False,
        "outcome_mode": "strict_fail",
        "format": "json",
        "output": None,
    }
    data.update(overrides)
    return Namespace(**data)


def _pod_doctor_args(**overrides: object) -> Namespace:
    data = {
        "pod_contract_path": ".dpone/gitops/airflow/pod-contract.json",
        "pod_spec_path": ".dpone/gitops/airflow/pod-spec.yaml",
        "kpo_kwargs_path": ".dpone/gitops/airflow/kpo-kwargs.json",
        "runner_policy": "release",
        "format": "json",
        "output": None,
    }
    data.update(overrides)
    return Namespace(**data)


def _run_spec_exec_args(**overrides: object) -> Namespace:
    data = {
        "run_spec_path": ".dpone/gitops/airflow/run-spec.json",
        "evidence_output": ".dpone/gitops/airflow/runtime-evidence.json",
        "xcom_output": ".dpone/gitops/airflow/xcom-summary.json",
        "format": "json",
        "output": None,
    }
    data.update(overrides)
    return Namespace(**data)


def _evidence_verify_args(**overrides: object) -> Namespace:
    data = {
        "run_spec_path": ".dpone/gitops/airflow/run-spec.json",
        "evidence_path": ".dpone/gitops/airflow/runtime-evidence.json",
        "require_all_steps": True,
        "format": "json",
        "output": None,
    }
    data.update(overrides)
    return Namespace(**data)


def _outcome_gate_args(**overrides: object) -> Namespace:
    data = {
        "xcom_summary_path": ".dpone/gitops/airflow/xcom-summary.json",
        "required_status": "passed",
        "format": "json",
        "output": None,
    }
    data.update(overrides)
    return Namespace(**data)


def _write(path: Path, text: str = "source: {}\nsink: {}\n") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _workload(tmp_path: Path) -> None:
    workload = tmp_path / "dpone_workloads"
    _write(workload / "manifests" / "orders.yaml", "depends_on:\n  - path: seed.yaml\nsource: {}\nsink: {}\n")
    _write(workload / "manifests" / "seed.yaml")


def _git_baseline(tmp_path: Path) -> None:
    subprocess.run(["git", "init"], cwd=tmp_path, check=True, capture_output=True, text=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.name", "Test User"], cwd=tmp_path, check=True)
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True, capture_output=True, text=True)
    subprocess.run(["git", "commit", "-m", "baseline"], cwd=tmp_path, check=True, capture_output=True, text=True)


def _build_attested_bundle(tmp_path: Path) -> None:
    _workload(tmp_path)
    _git_baseline(tmp_path)
    view = GitOpsBundleService(ctx=_ctx(tmp_path)).build_view(_bundle_args())
    assert view.exit_code == 0


def _install_fake_airflow_modules(monkeypatch) -> None:
    airflow = types.ModuleType("airflow")
    exceptions = types.ModuleType("airflow.exceptions")
    providers = types.ModuleType("airflow.providers")
    standard = types.ModuleType("airflow.providers.standard")
    standard_operators = types.ModuleType("airflow.providers.standard.operators")
    python_operator = types.ModuleType("airflow.providers.standard.operators.python")
    cncf = types.ModuleType("airflow.providers.cncf")
    kubernetes = types.ModuleType("airflow.providers.cncf.kubernetes")
    pod_operator = types.ModuleType("airflow.providers.cncf.kubernetes.operators.pod")
    kubernetes_operators = types.ModuleType("airflow.providers.cncf.kubernetes.operators")

    class DAG:
        pass

    class AirflowException(Exception):
        pass

    class PythonOperator:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs

    class KubernetesPodOperator:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs

    airflow.DAG = DAG
    exceptions.AirflowException = AirflowException
    python_operator.PythonOperator = PythonOperator
    pod_operator.KubernetesPodOperator = KubernetesPodOperator
    module_map = {
        "airflow": airflow,
        "airflow.exceptions": exceptions,
        "airflow.providers": providers,
        "airflow.providers.standard": standard,
        "airflow.providers.standard.operators": standard_operators,
        "airflow.providers.standard.operators.python": python_operator,
        "airflow.providers.cncf": cncf,
        "airflow.providers.cncf.kubernetes": kubernetes,
        "airflow.providers.cncf.kubernetes.operators": kubernetes_operators,
        "airflow.providers.cncf.kubernetes.operators.pod": pod_operator,
    }
    for name, module in module_map.items():
        monkeypatch.setitem(sys.modules, name, module)


def test_airflow_render_service_writes_kubernetes_runner_artifacts(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _build_attested_bundle(tmp_path)

    view = GitOpsAirflowRenderService(ctx=_ctx(tmp_path)).build_view(_render_args())
    payload = view.report.to_jsonable()

    assert view.exit_code == 0
    assert payload["kind"] == "gitops.airflow_render"
    assert payload["bundle_path"] == ".dpone/gitops/bundle/bundle.json"
    assert payload["output_dir"] == ".dpone/gitops/airflow"
    assert [artifact["path"] for artifact in payload["artifacts"]] == [
        ".dpone/gitops/airflow/pod_template.yaml",
        ".dpone/gitops/airflow/executor_config.json",
        ".dpone/gitops/airflow/airflow_task.py",
        ".dpone/gitops/airflow/entrypoint.sh",
        ".dpone/gitops/airflow/run-spec.json",
        ".dpone/gitops/airflow/runtime-profile.json",
        ".dpone/gitops/airflow/xcom-summary.json",
        ".dpone/gitops/airflow/airflow_dag_factory.py",
        ".dpone/gitops/airflow/outcome_gate.py",
        ".dpone/gitops/airflow/runtime-evidence.json",
        ".dpone/gitops/airflow/image-contract.json",
    ]
    assert payload["commands"] == [
        "dpone gitops airflow run-spec-exec .dpone/gitops/airflow/run-spec.json "
        "--evidence-output .dpone/gitops/airflow/runtime-evidence.json "
        "--xcom-output .dpone/gitops/airflow/xcom-summary.json",
    ]
    assert payload["blockers"] == []
    assert str(tmp_path) not in json.dumps(payload)

    pod_template = (tmp_path / ".dpone/gitops/airflow/pod_template.yaml").read_text(encoding="utf-8")
    entrypoint = (tmp_path / ".dpone/gitops/airflow/entrypoint.sh").read_text(encoding="utf-8")
    airflow_task = (tmp_path / ".dpone/gitops/airflow/airflow_task.py").read_text(encoding="utf-8")

    assert "name: dpone-airflow-worker" in pod_template
    assert "name: base" in pod_template
    assert "image: ghcr.io/acme/dpone:2026.06.16" in pod_template
    assert "dpone gitops airflow run-spec-exec .dpone/gitops/airflow/run-spec.json" in entrypoint
    assert "KubernetesPodOperator" in airflow_task
    assert "pod_template_file" in airflow_task

    run_spec = json.loads((tmp_path / ".dpone/gitops/airflow/run-spec.json").read_text(encoding="utf-8"))
    runtime_profile = json.loads((tmp_path / ".dpone/gitops/airflow/runtime-profile.json").read_text(encoding="utf-8"))
    assert run_spec["kind"] == "gitops.airflow_run_spec"
    assert run_spec["bundle_path"] == ".dpone/gitops/bundle/bundle.json"
    assert run_spec["evidence_output"] == ".dpone/gitops/airflow/runtime-evidence.json"
    assert run_spec["image"] == "ghcr.io/acme/dpone:2026.06.16"
    assert [step["kind"] for step in run_spec["steps"]] == ["bundle_verify", "gitops_verify", "dpone_run"]
    assert any(step["command"] == "dpone run dpone_workloads/manifests/orders.yaml" for step in run_spec["steps"])
    assert runtime_profile["kind"] == "gitops.airflow_runtime_profile"
    assert runtime_profile["run_spec_path"] == ".dpone/gitops/airflow/run-spec.json"
    assert runtime_profile["xcom_summary_path"] == ".dpone/gitops/airflow/xcom-summary.json"
    assert str(tmp_path) not in json.dumps(run_spec)


def test_airflow_runtime_profile_service_writes_profile_xcom_and_dag_factory(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from dpone.services.gitops.airflow_runtime_profile_service import GitOpsAirflowRuntimeProfileService
    from dpone.services.gitops.airflow_runtime_service import GitOpsAirflowRunSpecService

    monkeypatch.chdir(tmp_path)
    _build_attested_bundle(tmp_path)
    GitOpsAirflowRunSpecService(ctx=_ctx(tmp_path)).build_view(_run_spec_args())

    view = GitOpsAirflowRuntimeProfileService(ctx=_ctx(tmp_path)).build_view(_runtime_profile_args())
    payload = view.report.to_jsonable()
    profile_file = json.loads((tmp_path / ".dpone/gitops/airflow/runtime-profile.json").read_text(encoding="utf-8"))
    xcom_summary = json.loads((tmp_path / ".dpone/gitops/airflow/xcom-summary.json").read_text(encoding="utf-8"))
    dag_factory = (tmp_path / ".dpone/gitops/airflow/airflow_dag_factory.py").read_text(encoding="utf-8")

    assert view.exit_code == 0
    assert payload == profile_file
    assert payload["kind"] == "gitops.airflow_runtime_profile"
    assert payload["runner_policy"] == "release"
    assert payload["image"] == "ghcr.io/acme/dpone:2026.06.16"
    assert payload["image_digest"] == "sha256:" + "f" * 64
    assert payload["namespace"] == "dpone-runners"
    assert payload["service_account"] == "dpone-runner"
    assert payload["run_spec_path"] == ".dpone/gitops/airflow/run-spec.json"
    assert payload["runtime_evidence_path"] == ".dpone/gitops/airflow/runtime-evidence.json"
    assert payload["xcom_summary_path"] == ".dpone/gitops/airflow/xcom-summary.json"
    assert payload["outcome_mode"] == "strict_fail"
    assert payload["outcome_gate_path"] == ".dpone/gitops/airflow/outcome_gate.py"
    assert payload["resources"] == {
        "requests": {"cpu": "250m", "memory": "512Mi"},
        "limits": {"cpu": "2", "memory": "2Gi"},
    }
    assert payload["artifact_sink"] == {"kind": "local", "path": ".dpone/gitops/airflow"}
    assert payload["env"] == [{"name": "DPONE_PROFILE", "value": "release"}]
    assert payload["labels"] == {"app.kubernetes.io/name": "dpone"}
    assert payload["annotations"] == {"dpone.dev/runner": "airflow"}
    assert payload["blockers"] == []
    assert xcom_summary["kind"] == "gitops.airflow_xcom_summary"
    assert xcom_summary["status"] == "planned"
    assert xcom_summary["runtime_profile_path"] == ".dpone/gitops/airflow/runtime-profile.json"
    assert xcom_summary["run_spec_path"] == ".dpone/gitops/airflow/run-spec.json"
    assert xcom_summary["runtime_evidence_path"] == ".dpone/gitops/airflow/runtime-evidence.json"
    assert xcom_summary["failed_step"] is None
    assert xcom_summary["blockers"] == []
    assert "KubernetesPodOperator" in dag_factory
    assert "from airflow.providers.standard.operators.python import PythonOperator" in dag_factory
    assert "from airflow.operators.python import PythonOperator" not in dag_factory
    assert "do_xcom_push=True" in dag_factory
    assert "runtime-profile.json" in dag_factory
    assert "xcom-summary.json" in dag_factory
    assert "build_dpone_gitops_outcome_gate" in dag_factory
    assert "AirflowException" in dag_factory
    compile(dag_factory, "airflow_dag_factory.py", "exec")
    compile((tmp_path / ".dpone/gitops/airflow/outcome_gate.py").read_text(encoding="utf-8"), "outcome_gate.py", "exec")
    assert str(tmp_path) not in json.dumps(payload)
    assert str(tmp_path) not in dag_factory


def test_airflow_dag_factory_loads_kpo_kwargs_and_pod_spec_artifacts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from dpone.services.gitops.airflow_pod_contract_service import GitOpsAirflowPodContractService
    from dpone.services.gitops.airflow_runtime_profile_service import GitOpsAirflowRuntimeProfileService
    from dpone.services.gitops.airflow_runtime_service import GitOpsAirflowRunSpecService

    monkeypatch.chdir(tmp_path)
    _install_fake_airflow_modules(monkeypatch)
    _build_attested_bundle(tmp_path)
    GitOpsAirflowRunSpecService(ctx=_ctx(tmp_path)).build_view(_run_spec_args())
    GitOpsAirflowRuntimeProfileService(ctx=_ctx(tmp_path)).build_view(_runtime_profile_args())
    GitOpsAirflowPodContractService(ctx=_ctx(tmp_path)).build_view(_pod_contract_args())

    dag_factory = (tmp_path / ".dpone/gitops/airflow/airflow_dag_factory.py").read_text(encoding="utf-8")
    namespace: dict[str, object] = {}
    exec(compile(dag_factory, "airflow_dag_factory.py", "exec"), namespace)

    loader = namespace["load_dpone_kpo_kwargs"]
    build_task = namespace["build_dpone_gitops_task_from_artifacts"]
    artifact_dir = tmp_path / ".dpone/gitops/airflow"

    kwargs = loader(
        artifact_dir,
        task_id="orders_to_clickhouse",
        name="orders-runtime",
        labels={"team": "dwh"},
        annotations={"owner": "analytics"},
        operator_overrides={"pool": "dpone", "retries": 2},
    )

    assert kwargs["task_id"] == "orders_to_clickhouse"
    assert kwargs["name"] == "orders-runtime"
    assert kwargs["pod_template_file"] == str(artifact_dir / "pod-spec.yaml")
    assert kwargs["do_xcom_push"] is True
    assert kwargs["cmds"] == ["/bin/sh", "-ec"]
    assert kwargs["arguments"]
    assert kwargs["labels"] == {
        "app.kubernetes.io/name": "dpone",
        "app.kubernetes.io/component": "dpone-runner",
        "team": "dwh",
    }
    assert kwargs["annotations"] == {
        "dpone.dev/runner": "airflow",
        "dpone.dev/pod-contract": "enabled",
        "owner": "analytics",
    }
    assert kwargs["pool"] == "dpone"
    assert kwargs["retries"] == 2

    task = build_task(dag=object(), artifact_dir=artifact_dir, task_id="orders_to_clickhouse")
    assert task.kwargs["pod_template_file"] == str(artifact_dir / "pod-spec.yaml")
    assert task.kwargs["do_xcom_push"] is True

    for forbidden in ("pod_template_file", "do_xcom_push", "cmds", "arguments"):
        try:
            loader(artifact_dir, task_id="bad_override", operator_overrides={forbidden: "bad"})
        except ValueError as exc:
            assert forbidden in str(exc)
        else:
            raise AssertionError(f"{forbidden} override should be rejected")


def test_airflow_runtime_profile_xcom_then_gate_factory_exits_zero_and_adds_gate(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from dpone.services.gitops.airflow_runtime_profile_service import GitOpsAirflowRuntimeProfileService
    from dpone.services.gitops.airflow_runtime_service import GitOpsAirflowRunSpecService

    monkeypatch.chdir(tmp_path)
    _build_attested_bundle(tmp_path)
    GitOpsAirflowRunSpecService(ctx=_ctx(tmp_path)).build_view(_run_spec_args())

    view = GitOpsAirflowRuntimeProfileService(ctx=_ctx(tmp_path)).build_view(
        _runtime_profile_args(outcome_mode="xcom_then_gate")
    )
    payload = view.report.to_jsonable()
    dag_factory = (tmp_path / ".dpone/gitops/airflow/airflow_dag_factory.py").read_text(encoding="utf-8")
    outcome_gate = (tmp_path / ".dpone/gitops/airflow/outcome_gate.py").read_text(encoding="utf-8")

    assert view.exit_code == 0
    assert payload["outcome_mode"] == "xcom_then_gate"
    assert "exit 0" in dag_factory
    assert "exit $status" not in dag_factory
    assert "PythonOperator" in dag_factory
    assert "build_dpone_gitops_outcome_gate" in dag_factory
    assert "gitops.airflow_outcome_gate" in outcome_gate
    compile(dag_factory, "airflow_dag_factory.py", "exec")
    compile(outcome_gate, "outcome_gate.py", "exec")
    assert str(tmp_path) not in dag_factory
    assert str(tmp_path) not in outcome_gate


def test_airflow_pod_contract_service_writes_contract_spec_and_kpo_kwargs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from dpone.services.gitops.airflow_pod_contract_service import GitOpsAirflowPodContractService
    from dpone.services.gitops.airflow_runtime_profile_service import GitOpsAirflowRuntimeProfileService
    from dpone.services.gitops.airflow_runtime_service import GitOpsAirflowRunSpecService

    monkeypatch.chdir(tmp_path)
    _build_attested_bundle(tmp_path)
    GitOpsAirflowRunSpecService(ctx=_ctx(tmp_path)).build_view(_run_spec_args())
    GitOpsAirflowRuntimeProfileService(ctx=_ctx(tmp_path)).build_view(_runtime_profile_args())

    view = GitOpsAirflowPodContractService(ctx=_ctx(tmp_path)).build_view(_pod_contract_args())
    payload = view.report.to_jsonable()
    contract = json.loads((tmp_path / ".dpone/gitops/airflow/pod-contract.json").read_text(encoding="utf-8"))
    pod_spec = PyYamlCodec().load((tmp_path / ".dpone/gitops/airflow/pod-spec.yaml").read_text(encoding="utf-8"))
    kpo_kwargs = json.loads((tmp_path / ".dpone/gitops/airflow/kpo-kwargs.json").read_text(encoding="utf-8"))

    assert view.exit_code == 0
    assert payload == contract
    assert payload["kind"] == "gitops.airflow_pod_contract"
    assert payload["bundle_path"] == ".dpone/gitops/bundle/bundle.json"
    assert payload["run_spec_path"] == ".dpone/gitops/airflow/run-spec.json"
    assert payload["runtime_profile_path"] == ".dpone/gitops/airflow/runtime-profile.json"
    assert payload["pod_spec_path"] == ".dpone/gitops/airflow/pod-spec.yaml"
    assert payload["kpo_kwargs_path"] == ".dpone/gitops/airflow/kpo-kwargs.json"
    assert payload["xcom"] == {
        "enabled": True,
        "return_path": "/airflow/xcom/return.json",
        "summary_path": ".dpone/gitops/airflow/xcom-summary.json",
        "mode": "final_outcome",
        "outcome_mode": "strict_fail",
    }
    assert payload["blockers"] == []
    assert pod_spec["kind"] == "Pod"
    assert pod_spec["metadata"]["namespace"] == "dpone-runners"
    assert pod_spec["metadata"]["labels"]["app.kubernetes.io/component"] == "dpone-runner"
    assert pod_spec["metadata"]["annotations"]["dpone.dev/pod-contract"] == "enabled"
    assert pod_spec["spec"]["serviceAccountName"] == "dpone-runner"
    assert pod_spec["spec"]["imagePullSecrets"] == [{"name": "regcred"}]
    assert pod_spec["spec"]["nodeSelector"] == {"workload": "dpone"}
    assert pod_spec["spec"]["tolerations"] == [
        {"key": "dedicated", "operator": "Equal", "value": "dpone", "effect": "NoSchedule"}
    ]
    assert pod_spec["spec"]["volumes"] == [{"name": "dpone-artifacts", "emptyDir": {}}]
    base_container = pod_spec["spec"]["containers"][0]
    assert base_container["name"] == "base"
    assert base_container["image"] == "ghcr.io/acme/dpone:2026.06.16"
    assert base_container["resources"] == {
        "requests": {"cpu": "250m", "memory": "512Mi"},
        "limits": {"cpu": "2", "memory": "2Gi"},
    }
    assert {"configMapRef": {"name": "dpone-runner-config"}} in base_container["envFrom"]
    assert {"name": "DPONE_TOKEN", "valueFrom": {"secretKeyRef": {"name": "dpone-token", "key": "token"}}} in (
        base_container["env"]
    )
    assert base_container["volumeMounts"] == [
        {"name": "dpone-artifacts", "mountPath": "/workspace/.dpone/gitops/airflow", "readOnly": True}
    ]
    assert kpo_kwargs["pod_template_file"] == ".dpone/gitops/airflow/pod-spec.yaml"
    assert kpo_kwargs["do_xcom_push"] is True
    assert kpo_kwargs["get_logs"] is True
    assert kpo_kwargs["on_finish_action"] == "delete_pod"
    assert kpo_kwargs["deferrable"] is False
    assert "exit $status" in kpo_kwargs["arguments"][0]
    assert str(tmp_path) not in json.dumps(payload)
    assert str(tmp_path) not in json.dumps(kpo_kwargs)
    assert str(tmp_path) not in PyYamlCodec().dump(pod_spec)


def test_airflow_pod_contract_xcom_then_gate_mode_preserves_xcom_before_downstream_gate(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from dpone.services.gitops.airflow_pod_contract_service import GitOpsAirflowPodContractService
    from dpone.services.gitops.airflow_runtime_profile_service import GitOpsAirflowRuntimeProfileService
    from dpone.services.gitops.airflow_runtime_service import GitOpsAirflowRunSpecService

    monkeypatch.chdir(tmp_path)
    _build_attested_bundle(tmp_path)
    GitOpsAirflowRunSpecService(ctx=_ctx(tmp_path)).build_view(_run_spec_args())
    GitOpsAirflowRuntimeProfileService(ctx=_ctx(tmp_path)).build_view(
        _runtime_profile_args(outcome_mode="xcom_then_gate")
    )

    view = GitOpsAirflowPodContractService(ctx=_ctx(tmp_path)).build_view(
        _pod_contract_args(outcome_mode="xcom_then_gate")
    )
    payload = view.report.to_jsonable()
    pod_spec = PyYamlCodec().load((tmp_path / ".dpone/gitops/airflow/pod-spec.yaml").read_text(encoding="utf-8"))
    kpo_kwargs = json.loads((tmp_path / ".dpone/gitops/airflow/kpo-kwargs.json").read_text(encoding="utf-8"))
    command = kpo_kwargs["arguments"][0]
    env = pod_spec["spec"]["containers"][0]["env"]

    assert view.exit_code == 0
    assert payload["xcom"]["outcome_mode"] == "xcom_then_gate"
    assert "exit 0" in command
    assert "exit $status" not in command
    assert {"name": "DPONE_AIRFLOW_OUTCOME_MODE", "value": "xcom_then_gate"} in env
    assert str(tmp_path) not in json.dumps(payload)


def test_airflow_pod_doctor_blocks_xcom_and_base_container_drift(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from dpone.services.gitops.airflow_pod_contract_service import GitOpsAirflowPodContractService
    from dpone.services.gitops.airflow_pod_doctor_service import GitOpsAirflowPodDoctorService
    from dpone.services.gitops.airflow_runtime_profile_service import GitOpsAirflowRuntimeProfileService
    from dpone.services.gitops.airflow_runtime_service import GitOpsAirflowRunSpecService

    monkeypatch.chdir(tmp_path)
    _build_attested_bundle(tmp_path)
    GitOpsAirflowRunSpecService(ctx=_ctx(tmp_path)).build_view(_run_spec_args())
    GitOpsAirflowRuntimeProfileService(ctx=_ctx(tmp_path)).build_view(_runtime_profile_args())
    GitOpsAirflowPodContractService(ctx=_ctx(tmp_path)).build_view(_pod_contract_args())

    pod_spec_path = tmp_path / ".dpone/gitops/airflow/pod-spec.yaml"
    pod_spec = PyYamlCodec().load(pod_spec_path.read_text(encoding="utf-8"))
    pod_spec["spec"]["containers"][0]["name"] = "runner"
    pod_spec_path.write_text(PyYamlCodec().dump(pod_spec), encoding="utf-8")
    kpo_kwargs_path = tmp_path / ".dpone/gitops/airflow/kpo-kwargs.json"
    kpo_kwargs = json.loads(kpo_kwargs_path.read_text(encoding="utf-8"))
    kpo_kwargs["do_xcom_push"] = False
    kpo_kwargs_path.write_text(json.dumps(kpo_kwargs, indent=2), encoding="utf-8")

    view = GitOpsAirflowPodDoctorService(ctx=_ctx(tmp_path)).build_view(_pod_doctor_args())
    payload = view.report.to_jsonable()
    blocker_codes = {blocker["code"] for blocker in payload["blockers"]}

    assert view.exit_code == 2
    assert payload["kind"] == "gitops.airflow_pod_doctor"
    assert {
        "pod_contract_base_container_missing",
        "pod_contract_xcom_push_required",
    }.issubset(blocker_codes)
    assert any(check["name"] == "pod_contract_base_container" and not check["passed"] for check in payload["checks"])
    assert any(check["name"] == "pod_contract_xcom_push" and not check["passed"] for check in payload["checks"])


def test_airflow_runtime_profile_release_policy_blocks_unsafe_profile(tmp_path: Path, monkeypatch) -> None:
    from dpone.services.gitops.airflow_runtime_profile_service import GitOpsAirflowRuntimeProfileService
    from dpone.services.gitops.airflow_runtime_service import GitOpsAirflowRunSpecService

    monkeypatch.chdir(tmp_path)
    _build_attested_bundle(tmp_path)
    GitOpsAirflowRunSpecService(ctx=_ctx(tmp_path)).build_view(_run_spec_args())

    view = GitOpsAirflowRuntimeProfileService(ctx=_ctx(tmp_path)).build_view(
        _runtime_profile_args(
            image_digest=None,
            service_account="default",
            cpu_request=None,
            memory_request=None,
            cpu_limit=None,
            memory_limit=None,
            artifact_sink_path=None,
        )
    )
    payload = view.report.to_jsonable()
    blocker_codes = {blocker["code"] for blocker in payload["blockers"]}

    assert view.exit_code == 2
    assert {
        "runtime_profile_image_digest_required",
        "runtime_profile_service_account_required",
        "runtime_profile_resources_required",
        "runtime_profile_artifact_sink_required",
    }.issubset(blocker_codes)
    assert not (tmp_path / ".dpone/gitops/airflow/runtime-profile.json").exists()


def test_airflow_run_spec_service_writes_public_runtime_contract(tmp_path: Path, monkeypatch) -> None:
    from dpone.services.gitops.airflow_runtime_service import GitOpsAirflowRunSpecService

    monkeypatch.chdir(tmp_path)
    _build_attested_bundle(tmp_path)

    view = GitOpsAirflowRunSpecService(ctx=_ctx(tmp_path)).build_view(_run_spec_args())
    payload = view.report.to_jsonable()
    run_spec_file = json.loads((tmp_path / ".dpone/gitops/airflow/run-spec.json").read_text(encoding="utf-8"))

    assert view.exit_code == 0
    assert payload["kind"] == "gitops.airflow_run_spec"
    assert payload == run_spec_file
    assert payload["bundle_digest"].startswith("sha256:")
    assert payload["image_digest"] == "sha256:" + "d" * 64
    assert payload["worktree"] == "."
    assert payload["airflow_context_env"] == [
        "AIRFLOW_CTX_DAG_ID",
        "AIRFLOW_CTX_TASK_ID",
        "AIRFLOW_CTX_RUN_ID",
        "AIRFLOW_CTX_TRY_NUMBER",
        "DPONE_DAG_ID",
        "DPONE_DAG_RUN_ID",
        "DPONE_TRY_NUMBER",
        "DPONE_LOGICAL_DATE",
        "DPONE_INTERVAL_START",
        "DPONE_INTERVAL_END",
        "DPONE_PARTITION_KEY",
        "DPONE_PARTITION_DIMENSION",
        "DPONE_PARTITION_MODE",
    ]
    assert [entry["manifest"] for entry in payload["entries"]] == ["dpone_workloads/manifests/orders.yaml"]
    assert [step["kind"] for step in payload["steps"]] == ["bundle_verify", "gitops_verify", "dpone_run"]
    assert str(tmp_path) not in json.dumps(payload)


def test_airflow_run_spec_executor_writes_runtime_evidence_for_success(tmp_path: Path, monkeypatch) -> None:
    from dpone.gitops.airflow_runtime_executor import StaticAirflowCommandRunner
    from dpone.services.gitops.airflow_runtime_exec_service import GitOpsAirflowRunSpecExecService
    from dpone.services.gitops.airflow_runtime_service import GitOpsAirflowRunSpecService

    monkeypatch.chdir(tmp_path)
    _build_attested_bundle(tmp_path)
    GitOpsAirflowRunSpecService(ctx=_ctx(tmp_path)).build_view(_run_spec_args())

    view = GitOpsAirflowRunSpecExecService(
        ctx=_ctx(tmp_path),
        runner=StaticAirflowCommandRunner(exit_codes={}),
    ).build_view(
        Namespace(
            run_spec_path=".dpone/gitops/airflow/run-spec.json",
            evidence_output=".dpone/gitops/airflow/runtime-evidence.json",
            format="json",
            output=None,
        )
    )
    payload = view.report.to_jsonable()
    evidence_file = json.loads((tmp_path / ".dpone/gitops/airflow/runtime-evidence.json").read_text(encoding="utf-8"))

    assert view.exit_code == 0
    assert payload == evidence_file
    assert payload["kind"] == "gitops.airflow_runtime_evidence"
    assert payload["run_spec_path"] == ".dpone/gitops/airflow/run-spec.json"
    assert payload["status"] == "passed"
    assert [step["status"] for step in payload["steps"]] == ["passed", "passed", "passed"]
    assert all(step["exit_code"] == 0 for step in payload["steps"])
    assert payload["blockers"] == []
    assert str(tmp_path) not in json.dumps(payload)


def test_airflow_run_spec_executor_writes_final_xcom_outcome_for_success(tmp_path: Path, monkeypatch) -> None:
    from dpone.gitops.airflow_runtime_executor import StaticAirflowCommandRunner
    from dpone.services.gitops.airflow_runtime_exec_service import GitOpsAirflowRunSpecExecService
    from dpone.services.gitops.airflow_runtime_service import GitOpsAirflowRunSpecService

    monkeypatch.chdir(tmp_path)
    _build_attested_bundle(tmp_path)
    GitOpsAirflowRunSpecService(ctx=_ctx(tmp_path)).build_view(_run_spec_args())

    view = GitOpsAirflowRunSpecExecService(
        ctx=_ctx(tmp_path),
        runner=StaticAirflowCommandRunner(exit_codes={}),
    ).build_view(_run_spec_exec_args())
    payload = view.report.to_jsonable()
    xcom_summary = json.loads((tmp_path / ".dpone/gitops/airflow/xcom-summary.json").read_text(encoding="utf-8"))

    assert view.exit_code == 0
    assert payload["status"] == "passed"
    assert xcom_summary["kind"] == "gitops.airflow_xcom_summary"
    assert xcom_summary["status"] == "passed"
    assert xcom_summary["run_spec_path"] == ".dpone/gitops/airflow/run-spec.json"
    assert xcom_summary["runtime_evidence_path"] == ".dpone/gitops/airflow/runtime-evidence.json"
    assert xcom_summary["runtime_evidence_sha256"].startswith("sha256:")
    assert xcom_summary["failed_step"] is None
    assert xcom_summary["step_counts"] == {"total": 3, "passed": 3, "failed": 0}
    assert xcom_summary["artifact_paths"] == {"runtime_evidence": ".dpone/gitops/airflow/runtime-evidence.json"}
    assert xcom_summary["blockers"] == []
    assert str(tmp_path) not in json.dumps(xcom_summary)


def test_airflow_run_spec_executor_writes_final_xcom_outcome_for_failed_step(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from dpone.gitops.airflow_runtime_executor import StaticAirflowCommandRunner
    from dpone.services.gitops.airflow_runtime_exec_service import GitOpsAirflowRunSpecExecService
    from dpone.services.gitops.airflow_runtime_service import GitOpsAirflowRunSpecService

    monkeypatch.chdir(tmp_path)
    _build_attested_bundle(tmp_path)
    GitOpsAirflowRunSpecService(ctx=_ctx(tmp_path)).build_view(_run_spec_args())

    view = GitOpsAirflowRunSpecExecService(
        ctx=_ctx(tmp_path),
        runner=StaticAirflowCommandRunner(exit_codes={"dpone run dpone_workloads/manifests/orders.yaml": 7}),
    ).build_view(_run_spec_exec_args())
    xcom_summary = json.loads((tmp_path / ".dpone/gitops/airflow/xcom-summary.json").read_text(encoding="utf-8"))

    assert view.exit_code == 2
    assert xcom_summary["status"] == "failed"
    assert xcom_summary["failed_step"] == "dpone_run_dpone_workloads_manifests_orders_yaml"
    assert xcom_summary["runtime_evidence_sha256"].startswith("sha256:")
    assert xcom_summary["step_counts"] == {"total": 3, "passed": 2, "failed": 1}
    assert any(blocker["code"] == "runtime_step_failed" for blocker in xcom_summary["blockers"])


def test_airflow_outcome_gate_service_blocks_failed_xcom_summary(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from dpone.gitops.airflow_runtime_executor import StaticAirflowCommandRunner
    from dpone.services.gitops.airflow_outcome_gate_service import GitOpsAirflowOutcomeGateService
    from dpone.services.gitops.airflow_runtime_exec_service import GitOpsAirflowRunSpecExecService
    from dpone.services.gitops.airflow_runtime_service import GitOpsAirflowRunSpecService

    monkeypatch.chdir(tmp_path)
    _build_attested_bundle(tmp_path)
    GitOpsAirflowRunSpecService(ctx=_ctx(tmp_path)).build_view(_run_spec_args())
    GitOpsAirflowRunSpecExecService(
        ctx=_ctx(tmp_path),
        runner=StaticAirflowCommandRunner(exit_codes={"dpone run dpone_workloads/manifests/orders.yaml": 7}),
    ).build_view(_run_spec_exec_args())

    view = GitOpsAirflowOutcomeGateService(ctx=_ctx(tmp_path)).build_view(_outcome_gate_args())
    payload = view.report.to_jsonable()

    assert view.exit_code == 2
    assert payload["kind"] == "gitops.airflow_outcome_gate"
    assert payload["xcom_summary_path"] == ".dpone/gitops/airflow/xcom-summary.json"
    assert payload["required_status"] == "passed"
    assert payload["status"] == "failed"
    assert payload["failed_step"] == "dpone_run_dpone_workloads_manifests_orders_yaml"
    assert not payload["passed"]
    assert {blocker["code"] for blocker in payload["blockers"]}.issuperset(
        {"airflow_outcome_failed", "runtime_step_failed"}
    )
    assert str(tmp_path) not in json.dumps(payload)


def test_airflow_evidence_verify_blocks_failed_runtime_step(tmp_path: Path, monkeypatch) -> None:
    from dpone.gitops.airflow_runtime_executor import StaticAirflowCommandRunner
    from dpone.services.gitops.airflow_runtime_exec_service import GitOpsAirflowRunSpecExecService
    from dpone.services.gitops.airflow_runtime_service import (
        GitOpsAirflowEvidenceVerifyService,
        GitOpsAirflowRunSpecService,
    )

    monkeypatch.chdir(tmp_path)
    _build_attested_bundle(tmp_path)
    GitOpsAirflowRunSpecService(ctx=_ctx(tmp_path)).build_view(_run_spec_args())
    GitOpsAirflowRunSpecExecService(
        ctx=_ctx(tmp_path),
        runner=StaticAirflowCommandRunner(exit_codes={"dpone run dpone_workloads/manifests/orders.yaml": 19}),
    ).build_view(
        Namespace(
            run_spec_path=".dpone/gitops/airflow/run-spec.json",
            evidence_output=".dpone/gitops/airflow/runtime-evidence.json",
            format="json",
            output=None,
        )
    )

    view = GitOpsAirflowEvidenceVerifyService(ctx=_ctx(tmp_path)).build_view(_evidence_verify_args())
    payload = view.report.to_jsonable()
    blocker_codes = {blocker["code"] for blocker in payload["blockers"]}

    assert view.exit_code == 2
    assert payload["kind"] == "gitops.airflow_runtime_evidence"
    assert payload["status"] == "failed"
    assert "runtime_step_failed" in blocker_codes
    assert any(step["kind"] == "dpone_run" and step["exit_code"] == 19 for step in payload["steps"])


def test_airflow_doctor_service_passes_generated_runner_contracts(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _build_attested_bundle(tmp_path)
    GitOpsAirflowRenderService(ctx=_ctx(tmp_path)).build_view(_render_args())

    view = GitOpsAirflowDoctorService(ctx=_ctx(tmp_path)).build_view(_doctor_args())
    payload = view.report.to_jsonable()

    assert view.exit_code == 0
    assert payload["kind"] == "gitops.airflow_doctor"
    assert payload["blockers"] == []
    check_names = {check["name"] for check in payload["checks"]}
    assert {
        "bundle_json",
        "bundle_attestation",
        "pod_template_base_container",
        "pod_template_image",
        "image_contract_image",
        "image_contract_tools",
    }.issubset(check_names)
    assert all(check["passed"] for check in payload["checks"])
    assert str(tmp_path) not in json.dumps(payload)


def test_airflow_release_policy_passes_generated_hardened_runner_contracts(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _build_attested_bundle(tmp_path)
    GitOpsAirflowRenderService(ctx=_ctx(tmp_path)).build_view(
        _render_args(
            image_digest="sha256:" + "b" * 64,
            tool=["dpone", "bcp", "clickhouse-client"],
        )
    )

    view = GitOpsAirflowDoctorService(ctx=_ctx(tmp_path)).build_view(_doctor_args(runner_policy="release"))
    payload = view.report.to_jsonable()

    assert view.exit_code == 0
    assert payload["runner_policy"] == "release"
    check_names = {check["name"] for check in payload["checks"]}
    assert {
        "runner_policy_image_digest",
        "runner_policy_service_account",
        "runner_policy_resources",
        "runner_policy_non_root",
    }.issubset(check_names)
    assert payload["blockers"] == []
    assert all(check["passed"] for check in payload["checks"])


def test_airflow_release_policy_blocks_missing_hardening_contracts(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _build_attested_bundle(tmp_path)
    GitOpsAirflowRenderService(ctx=_ctx(tmp_path)).build_view(_render_args())

    image_contract_path = tmp_path / ".dpone/gitops/airflow/image-contract.json"
    image_contract = json.loads(image_contract_path.read_text(encoding="utf-8"))
    image_contract.pop("image_digest", None)
    image_contract_path.write_text(json.dumps(image_contract, indent=2), encoding="utf-8")

    pod_template_path = tmp_path / ".dpone/gitops/airflow/pod_template.yaml"
    pod_template = PyYamlCodec().load(pod_template_path.read_text(encoding="utf-8"))
    pod_template["spec"].pop("serviceAccountName", None)
    pod_template["spec"].pop("securityContext", None)
    pod_template["spec"]["containers"][0].pop("resources", None)
    pod_template_path.write_text(PyYamlCodec().dump(pod_template), encoding="utf-8")

    view = GitOpsAirflowDoctorService(ctx=_ctx(tmp_path)).build_view(_doctor_args(runner_policy="release"))
    payload = view.report.to_jsonable()
    blocker_codes = {blocker["code"] for blocker in payload["blockers"]}

    assert view.exit_code == 2
    assert payload["runner_policy"] == "release"
    assert {
        "image_digest_required",
        "pod_template_service_account_required",
        "pod_template_resources_required",
        "pod_template_non_root_required",
    }.issubset(blocker_codes)
    assert any(check["name"] == "runner_policy_image_digest" and not check["passed"] for check in payload["checks"])


def test_airflow_doctor_blocks_invalid_kubernetes_executor_base_container(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _build_attested_bundle(tmp_path)
    GitOpsAirflowRenderService(ctx=_ctx(tmp_path)).build_view(_render_args())
    pod_template = tmp_path / ".dpone/gitops/airflow/pod_template.yaml"
    pod_template.write_text(
        pod_template.read_text(encoding="utf-8").replace("name: base", "name: worker", 1),
        encoding="utf-8",
    )

    view = GitOpsAirflowDoctorService(ctx=_ctx(tmp_path)).build_view(_doctor_args())
    payload = view.report.to_jsonable()

    assert view.exit_code == 2
    assert any(blocker["code"] == "pod_template_base_container_missing" for blocker in payload["blockers"])
    assert any(check["name"] == "pod_template_base_container" and not check["passed"] for check in payload["checks"])


def test_airflow_image_contract_service_writes_stable_contract(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    view = GitOpsAirflowImageContractService(ctx=_ctx(tmp_path)).build_view(_image_contract_args())
    payload = view.report.to_jsonable()
    contract_file = json.loads((tmp_path / ".dpone/gitops/airflow/image-contract.json").read_text(encoding="utf-8"))

    assert view.exit_code == 0
    assert payload["kind"] == "gitops.airflow_image_contract"
    assert payload["contract"]["image"] == "ghcr.io/acme/dpone:2026.06.16"
    assert payload["contract"]["image_digest"] == "sha256:" + "a" * 64
    assert payload["contract"]["tools"] == ["dpone", "bcp", "clickhouse-client"]
    assert contract_file == payload["contract"]
    assert str(tmp_path) not in json.dumps(payload)


def test_airflow_render_blocks_invalid_repo_relative_output_dir(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _build_attested_bundle(tmp_path)

    view = GitOpsAirflowRenderService(ctx=_ctx(tmp_path)).build_view(_render_args(output_dir="/tmp/dpone-airflow"))
    payload = view.report.to_jsonable()

    assert view.exit_code == 2
    assert payload["blockers"][0]["code"] == "invalid_path"
