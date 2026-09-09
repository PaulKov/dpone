from __future__ import annotations

from pathlib import Path


def airflow_render_artifact_paths(
    *,
    output_dir: Path,
    image_contract_path: Path,
    run_spec_path: Path,
    evidence_path: Path,
    runtime_profile_path: Path,
    xcom_summary_path: Path,
    dag_factory_path: Path,
    outcome_gate_path: Path,
) -> tuple[tuple[Path, str, str, bool], ...]:
    return (
        (output_dir / "pod_template.yaml", "pod_template", "Airflow KubernetesExecutor pod_template_file", True),
        (
            output_dir / "executor_config.json",
            "executor_config",
            "Airflow KubernetesExecutor executor_config example",
            True,
        ),
        (output_dir / "airflow_task.py", "airflow_task", "Airflow KubernetesPodOperator task snippet", True),
        (output_dir / "entrypoint.sh", "entrypoint", "Container entrypoint for the run-spec executor", True),
        (run_spec_path, "run_spec", "Runtime contract consumed by the custom dpone image", True),
        (
            runtime_profile_path,
            "runtime_profile",
            "Runtime placement profile consumed by Airflow GitOps handoff",
            True,
        ),
        (xcom_summary_path, "xcom_summary", "Planned XCom summary for KubernetesPodOperator handoff", True),
        (dag_factory_path, "dag_factory", "Airflow DAG factory for KubernetesPodOperator handoff", True),
        (outcome_gate_path, "outcome_gate", "Airflow downstream outcome gate helper", True),
        (evidence_path, "runtime_evidence", "Runtime evidence path written by the custom dpone image", False),
        (image_contract_path, "image_contract", "Custom dpone image contract", True),
    )


def run_spec_exec_command(*, run_spec_path: Path, evidence_path: Path, xcom_path: Path) -> str:
    return (
        f"dpone gitops airflow run-spec-exec {run_spec_path.as_posix()} "
        f"--evidence-output {evidence_path.as_posix()} --xcom-output {xcom_path.as_posix()}"
    )


__all__ = [
    "airflow_render_artifact_paths",
    "run_spec_exec_command",
]
