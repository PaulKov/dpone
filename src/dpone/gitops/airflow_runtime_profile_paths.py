from __future__ import annotations

from pathlib import Path

from dpone.gitops.models import GitOpsIssue
from dpone.gitops.paths import GitOpsPathValidationError, safe_relative_path


class GitOpsAirflowRuntimeProfilePaths:
    def __init__(
        self,
        *,
        bundle_path: Path,
        bundle_label: str,
        run_spec_path: Path,
        run_spec_label: str,
        runtime_evidence_path: Path,
        runtime_evidence_label: str,
        output_path: Path,
        output_label: str,
        xcom_summary_path: Path,
        xcom_summary_label: str,
        dag_factory_path: Path,
        dag_factory_label: str,
        outcome_gate_path: Path,
        outcome_gate_label: str,
    ) -> None:
        self.bundle_path = bundle_path
        self.bundle_label = bundle_label
        self.run_spec_path = run_spec_path
        self.run_spec_label = run_spec_label
        self.runtime_evidence_path = runtime_evidence_path
        self.runtime_evidence_label = runtime_evidence_label
        self.output_path = output_path
        self.output_label = output_label
        self.xcom_summary_path = xcom_summary_path
        self.xcom_summary_label = xcom_summary_label
        self.dag_factory_path = dag_factory_path
        self.dag_factory_label = dag_factory_label
        self.outcome_gate_path = outcome_gate_path
        self.outcome_gate_label = outcome_gate_label


def resolve_runtime_profile_paths(
    args: object,
) -> tuple[GitOpsAirflowRuntimeProfilePaths, tuple[GitOpsIssue, ...]]:
    bundle_path, bundle_label, bundle_issue = _safe_path(getattr(args, "bundle_path", None), source="BUNDLE")
    run_spec_path, run_spec_label, run_spec_issue = _safe_path(
        getattr(args, "run_spec_path", ".dpone/gitops/airflow/run-spec.json"),
        source="--run-spec-path",
    )
    runtime_evidence_path, runtime_evidence_label, runtime_evidence_issue = _safe_path(
        getattr(args, "runtime_evidence_path", ".dpone/gitops/airflow/runtime-evidence.json"),
        source="--runtime-evidence-path",
    )
    output_path, output_label, output_issue = _safe_path(
        getattr(args, "output_path", ".dpone/gitops/airflow/runtime-profile.json"),
        source="--output-path",
    )
    xcom_summary_path, xcom_summary_label, xcom_issue = _safe_path(
        getattr(args, "xcom_summary_path", ".dpone/gitops/airflow/xcom-summary.json"),
        source="--xcom-summary-path",
    )
    dag_factory_path, dag_factory_label, dag_issue = _safe_path(
        getattr(args, "dag_factory_path", ".dpone/gitops/airflow/airflow_dag_factory.py"),
        source="--dag-factory-path",
    )
    outcome_gate_path, outcome_gate_label, outcome_gate_issue = _safe_path(
        getattr(args, "outcome_gate_path", ".dpone/gitops/airflow/outcome_gate.py"),
        source="--outcome-gate-path",
    )
    paths = GitOpsAirflowRuntimeProfilePaths(
        bundle_path=bundle_path,
        bundle_label=bundle_label,
        run_spec_path=run_spec_path,
        run_spec_label=run_spec_label,
        runtime_evidence_path=runtime_evidence_path,
        runtime_evidence_label=runtime_evidence_label,
        output_path=output_path,
        output_label=output_label,
        xcom_summary_path=xcom_summary_path,
        xcom_summary_label=xcom_summary_label,
        dag_factory_path=dag_factory_path,
        dag_factory_label=dag_factory_label,
        outcome_gate_path=outcome_gate_path,
        outcome_gate_label=outcome_gate_label,
    )
    issues = (
        bundle_issue,
        run_spec_issue,
        runtime_evidence_issue,
        output_issue,
        xcom_issue,
        dag_issue,
        outcome_gate_issue,
    )
    return paths, tuple(issue for issue in issues if issue)


def _safe_path(raw_path: object, *, source: str) -> tuple[Path, str, GitOpsIssue | None]:
    try:
        path = safe_relative_path(raw_path, source=source)
    except GitOpsPathValidationError as exc:
        label = str(raw_path or "")
        return Path("."), label, GitOpsIssue(code="invalid_path", message=str(exc), path=label, source=source)
    return path, "." if path.as_posix() == "." else path.as_posix(), None


__all__ = [
    "GitOpsAirflowRuntimeProfilePaths",
    "resolve_runtime_profile_paths",
]
