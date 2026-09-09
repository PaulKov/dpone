from __future__ import annotations

import shlex
from collections.abc import Mapping

from dpone.gitops.airflow_interval_env import airflow_interval_env_vars
from dpone.gitops.airflow_outcome_gate import AIRFLOW_OUTCOME_XCOM_THEN_GATE


def render_airflow_dag_factory(
    *,
    task_id: str,
    image: str,
    namespace: str,
    service_account: str,
    run_spec_path: str,
    runtime_profile_path: str,
    runtime_evidence_path: str,
    xcom_summary_path: str,
    outcome_mode: str,
    env: tuple[dict[str, str], ...],
    labels: Mapping[str, str],
    annotations: Mapping[str, str],
) -> str:
    """Render the Airflow helper that consumes pod-contract artifacts."""
    env_vars = {item["name"]: item["value"] for item in env if item.get("name")}
    env_vars["DPONE_AIRFLOW_RUNTIME_PROFILE"] = runtime_profile_path
    env_vars["DPONE_AIRFLOW_XCOM_SUMMARY"] = xcom_summary_path
    env_vars["DPONE_AIRFLOW_OUTCOME_MODE"] = outcome_mode
    env_vars.update(airflow_interval_env_vars())
    command = _run_spec_exec_xcom_command(
        run_spec_path=run_spec_path,
        runtime_evidence_path=runtime_evidence_path,
        xcom_summary_path=xcom_summary_path,
        outcome_mode=outcome_mode,
    )
    return (
        '"""Generated dpone Airflow DAG factory for KubernetesPodOperator runners.\n'
        "\n"
        "The default helper loads kpo-kwargs.json and pod-spec.yaml emitted by\n"
        "dpone gitops airflow pod-contract. Direct KPO mode is legacy convenience\n"
        "and is not the sparse git-sync runtime contract.\n"
        '"""\n'
        "\n"
        "from __future__ import annotations\n"
        "\n"
        "import json\n"
        "from collections.abc import Mapping\n"
        "from pathlib import Path\n"
        "from typing import Any\n"
        "\n"
        "from airflow import DAG\n"
        "from airflow.exceptions import AirflowException\n"
        "from airflow.providers.standard.operators.python import PythonOperator\n"
        "from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator\n"
        "from dpone_airflow_pack.step_tasks import build_dpone_gitops_step_tasks_from_artifacts\n"
        "\n"
        "\n"
        f"DEFAULT_ARTIFACT_DIR = {'.dpone/gitops/airflow'!r}\n"
        f"DEFAULT_TASK_ID = {task_id!r}\n"
        f"DEFAULT_RUNTIME_PROFILE_PATH = {runtime_profile_path!r}\n"
        f"DEFAULT_XCOM_SUMMARY_PATH = {xcom_summary_path!r}\n"
        "_KPO_KWARGS_FILE = 'kpo-kwargs.json'\n"
        "_POD_SPEC_FILE = 'pod-spec.yaml'\n"
        "_CONTRACT_OWNED_KPO_FIELDS = frozenset({'pod_template_file', 'do_xcom_push', 'cmds', 'arguments'})\n"
        "\n"
        "\n"
        "def _merge_mapping(\n"
        "    base: Mapping[str, Any] | None, extra: Mapping[str, Any] | None\n"
        ") -> dict[str, Any]:\n"
        "    merged = dict(base or {})\n"
        "    merged.update(dict(extra or {}))\n"
        "    return merged\n"
        "\n"
        "\n"
        "def _operator_overrides(overrides: Mapping[str, Any] | None) -> dict[str, Any]:\n"
        "    resolved = dict(overrides or {})\n"
        "    forbidden = sorted(_CONTRACT_OWNED_KPO_FIELDS.intersection(resolved))\n"
        "    if forbidden:\n"
        "        fields = ', '.join(forbidden)\n"
        "        raise ValueError(f'operator_overrides cannot replace dpone contract-owned KPO fields: {fields}')\n"
        "    return resolved\n"
        "\n"
        "\n"
        "def load_dpone_kpo_kwargs(\n"
        "    artifact_dir: str | Path = DEFAULT_ARTIFACT_DIR,\n"
        "    *,\n"
        "    task_id: str = DEFAULT_TASK_ID,\n"
        "    name: str | None = None,\n"
        "    labels: Mapping[str, Any] | None = None,\n"
        "    annotations: Mapping[str, Any] | None = None,\n"
        "    operator_overrides: Mapping[str, Any] | None = None,\n"
        ") -> dict[str, Any]:\n"
        "    root = Path(artifact_dir)\n"
        "    kwargs = json.loads((root / _KPO_KWARGS_FILE).read_text(encoding='utf-8'))\n"
        "    kwargs['task_id'] = task_id\n"
        "    kwargs['name'] = name or task_id\n"
        "    kwargs['pod_template_file'] = str(root / _POD_SPEC_FILE)\n"
        "    kwargs['do_xcom_push'] = True\n"
        "    kwargs['labels'] = _merge_mapping(kwargs.get('labels'), labels)\n"
        "    kwargs['annotations'] = _merge_mapping(kwargs.get('annotations'), annotations)\n"
        "    kwargs.update(_operator_overrides(operator_overrides))\n"
        "    return kwargs\n"
        "\n"
        "\n"
        "def _dpone_gitops_outcome_gate(*, ti, upstream_task_id: str, required_status: str = 'passed'):\n"
        "    summary = ti.xcom_pull(task_ids=upstream_task_id)\n"
        "    if not isinstance(summary, dict):\n"
        "        raise AirflowException('dpone GitOps XCom summary is missing or is not a JSON object')\n"
        "    status = str(summary.get('status') or 'unknown')\n"
        "    blockers = summary.get('blockers') or []\n"
        "    if status != required_status or blockers:\n"
        "        failed_step = summary.get('failed_step') or ''\n"
        "        raise AirflowException(\n"
        "            f'dpone GitOps outcome blocked: status={status!r}, '\n"
        "            f'failed_step={failed_step!r}, blockers={blockers!r}'\n"
        "        )\n"
        "    return summary\n"
        "\n"
        "\n"
        "def build_dpone_gitops_task_from_artifacts(\n"
        "    *,\n"
        "    dag: DAG,\n"
        "    artifact_dir: str | Path = DEFAULT_ARTIFACT_DIR,\n"
        "    task_id: str = DEFAULT_TASK_ID,\n"
        "    name: str | None = None,\n"
        "    labels: Mapping[str, Any] | None = None,\n"
        "    annotations: Mapping[str, Any] | None = None,\n"
        "    operator_overrides: Mapping[str, Any] | None = None,\n"
        ") -> KubernetesPodOperator:\n"
        "    kwargs = load_dpone_kpo_kwargs(\n"
        "        artifact_dir,\n"
        "        task_id=task_id,\n"
        "        name=name,\n"
        "        labels=labels,\n"
        "        annotations=annotations,\n"
        "        operator_overrides=operator_overrides,\n"
        "    )\n"
        "    return KubernetesPodOperator(dag=dag, **kwargs)\n"
        "\n"
        "\n"
        "def build_dpone_gitops_task(\n"
        "    *, dag: DAG, artifact_dir: str | Path = DEFAULT_ARTIFACT_DIR, task_id: str = DEFAULT_TASK_ID\n"
        ") -> KubernetesPodOperator:\n"
        '    """Compatibility alias for artifact-loading mode."""\n'
        "    return build_dpone_gitops_task_from_artifacts(dag=dag, artifact_dir=artifact_dir, task_id=task_id)\n"
        "\n"
        "\n"
        "def build_dpone_direct_kpo_task(*, dag: DAG, task_id: str = DEFAULT_TASK_ID) -> KubernetesPodOperator:\n"
        '    """Legacy direct KPO mode; not the sparse git-sync runtime contract."""\n'
        "    return KubernetesPodOperator(\n"
        "        dag=dag,\n"
        "        task_id=task_id,\n"
        "        name=task_id,\n"
        f"        namespace={namespace!r},\n"
        f"        image={image!r},\n"
        f"        service_account_name={service_account!r},\n"
        "        cmds=['/bin/sh', '-ec'],\n"
        f"        arguments=[{command!r}],\n"
        f"        env_vars={env_vars!r},\n"
        f"        labels={dict(labels)!r},\n"
        f"        annotations={dict(annotations)!r},\n"
        "        get_logs=True,\n"
        "        is_delete_operator_pod=True,\n"
        "        do_xcom_push=True,\n"
        "    )\n"
        "\n"
        "\n"
        "def build_dpone_gitops_outcome_gate(\n"
        "    *, dag: DAG, upstream_task_id: str = "
        f"{task_id!r}, task_id: str = 'dpone_gitops_outcome_gate'\n"
        ") -> PythonOperator:\n"
        "    return PythonOperator(\n"
        "        dag=dag,\n"
        "        task_id=task_id,\n"
        "        python_callable=_dpone_gitops_outcome_gate,\n"
        "        op_kwargs={'upstream_task_id': upstream_task_id, 'required_status': 'passed'},\n"
        "    )\n"
        "\n"
        "\n"
        "def wire_dpone_gitops_task_with_gate(*, dag: DAG, artifact_dir: str | Path = DEFAULT_ARTIFACT_DIR):\n"
        "    runtime_task = build_dpone_gitops_task_from_artifacts(dag=dag, artifact_dir=artifact_dir)\n"
        "    outcome_gate = build_dpone_gitops_outcome_gate(dag=dag, upstream_task_id=runtime_task.task_id)\n"
        "    runtime_task >> outcome_gate\n"
        "    return runtime_task, outcome_gate\n"
        "\n"
        "\n"
        "def wire_dpone_gitops_task_group(*, dag: DAG, artifact_dir: str | Path = DEFAULT_ARTIFACT_DIR):\n"
        '    """Build visible KPO tasks from run-spec steps and append the outcome gate."""\n'
        "    step_tasks = build_dpone_gitops_step_tasks_from_artifacts(dag=dag, artifact_dir=artifact_dir)\n"
        "    runtime_tasks = [task for name, task in step_tasks.items() if name.startswith('dpone_run_')]\n"
        "    if not runtime_tasks:\n"
        "        raise AirflowException('dpone GitOps run-spec does not contain a dpone_run step')\n"
        "    runtime_task_id = getattr(runtime_tasks[-1], 'task_id', None)\n"
        "    if runtime_task_id is None and hasattr(runtime_tasks[-1], 'kwargs'):\n"
        "        runtime_task_id = runtime_tasks[-1].kwargs.get('task_id')\n"
        "    outcome_gate = build_dpone_gitops_outcome_gate(dag=dag, upstream_task_id=runtime_task_id)\n"
        "    runtime_tasks[-1] >> outcome_gate\n"
        "    return step_tasks, outcome_gate\n"
    )


def _run_spec_exec_xcom_command(
    *,
    run_spec_path: str,
    runtime_evidence_path: str,
    xcom_summary_path: str,
    outcome_mode: str,
) -> str:
    exit_command = "exit 0" if outcome_mode == AIRFLOW_OUTCOME_XCOM_THEN_GATE else "exit $status"
    return (
        "status=0; "
        f"dpone gitops airflow run-spec-exec {shlex.quote(run_spec_path)} "
        f"--evidence-output {shlex.quote(runtime_evidence_path)} "
        f"--xcom-output {shlex.quote(xcom_summary_path)} || status=$?; "
        f"mkdir -p /airflow/xcom && cat {shlex.quote(xcom_summary_path)} > /airflow/xcom/return.json; "
        f"{exit_command}"
    )


__all__ = ["render_airflow_dag_factory"]
