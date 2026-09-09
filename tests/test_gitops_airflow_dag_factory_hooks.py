from __future__ import annotations

from dpone.gitops.airflow_dag_factory_renderer import render_airflow_dag_factory


def test_generated_airflow_factory_exposes_visible_run_spec_step_group() -> None:
    rendered = render_airflow_dag_factory(
        task_id="dpone_gitops_runtime",
        image="dpone:local",
        namespace="airflow",
        service_account="dpone",
        run_spec_path=".dpone/gitops/airflow/run-spec.json",
        runtime_profile_path=".dpone/gitops/airflow/runtime-profile.json",
        runtime_evidence_path=".dpone/gitops/airflow/runtime-evidence.json",
        xcom_summary_path=".dpone/gitops/airflow/xcom-summary.json",
        outcome_mode="xcom_then_gate",
        env=(),
        labels={},
        annotations={},
    )

    assert "from dpone_airflow_pack.step_tasks import build_dpone_gitops_step_tasks_from_artifacts" in rendered
    assert "def wire_dpone_gitops_task_group" in rendered
    assert "source_refresh" not in rendered
    compile(rendered, "airflow_dag_factory.py", "exec")
