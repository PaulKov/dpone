from __future__ import annotations

from typing import Any

from dpone.gitops.airflow_evidence_bundle_models import GitOpsAirflowEvidenceBundleReport
from dpone.gitops.airflow_k8s_smoke_models import GitOpsAirflowK8sSmokeReport
from dpone.gitops.airflow_models import (
    GitOpsAirflowDoctorReport,
    GitOpsAirflowImageContractReport,
    GitOpsAirflowRenderReport,
)
from dpone.gitops.airflow_outcome_gate import GitOpsAirflowOutcomeGateReport
from dpone.gitops.airflow_pod_launch_evidence_models import GitOpsAirflowPodLaunchEvidenceReport
from dpone.gitops.airflow_runtime_models import GitOpsAirflowRunSpec, GitOpsAirflowRuntimeEvidence
from dpone.gitops.airflow_runtime_profile_models import GitOpsAirflowRuntimeProfile


def render_gitops_airflow_render_markdown(report: GitOpsAirflowRenderReport) -> str:
    lines = [
        "# GitOps Airflow render",
        "",
        f"- Status: {'passed' if report.passed else 'blocked'}",
        f"- Bundle: `{report.bundle_path}`",
        f"- Output directory: `{report.output_dir}`",
        f"- Image: `{report.image}`",
        "",
        "## Artifacts",
        "",
    ]
    for artifact in report.artifacts:
        state = "present" if artifact.exists else "missing"
        lines.append(f"- `{artifact.path}` ({artifact.kind}, {state})")
    lines.extend(["", "## Commands", ""])
    for command in report.commands:
        lines.append(f"- `{command}`")
    _append_issues(lines, title="Warnings", issues=report.warnings)
    _append_issues(lines, title="Blockers", issues=report.blockers)
    return "\n".join(lines).rstrip() + "\n"


def render_gitops_airflow_doctor_markdown(report: GitOpsAirflowDoctorReport) -> str:
    lines = [
        "# GitOps Airflow doctor",
        "",
        f"- Status: {'passed' if report.passed else 'blocked'}",
        f"- Bundle: `{report.bundle_path}`",
        f"- Pod template: `{report.pod_template}`",
        f"- Image contract: `{report.image_contract}`",
        f"- Image: `{report.image or ''}`",
        f"- Runner policy: `{report.runner_policy}`",
        "",
        "## Checks",
        "",
    ]
    for check in report.checks:
        state = "passed" if check.passed else "blocked"
        lines.append(f"- `{check.name}` ({state}): {check.message}")
    _append_issues(lines, title="Warnings", issues=report.warnings)
    _append_issues(lines, title="Blockers", issues=report.blockers)
    return "\n".join(lines).rstrip() + "\n"


def render_gitops_airflow_image_contract_markdown(report: GitOpsAirflowImageContractReport) -> str:
    contract = report.contract
    lines = [
        "# GitOps Airflow image contract",
        "",
        f"- Status: {'passed' if report.passed else 'blocked'}",
        f"- Output path: `{report.output_path}`",
        f"- Image: `{contract.image}`",
        f"- Tools: `{', '.join(contract.tools)}`",
    ]
    _append_issues(lines, title="Warnings", issues=report.warnings)
    _append_issues(lines, title="Blockers", issues=report.blockers)
    return "\n".join(lines).rstrip() + "\n"


def render_gitops_airflow_run_spec_markdown(report: GitOpsAirflowRunSpec) -> str:
    lines = [
        "# GitOps Airflow run spec",
        "",
        f"- Status: {'passed' if report.passed else 'blocked'}",
        f"- Bundle: `{report.bundle_path}`",
        f"- Evidence output: `{report.evidence_output}`",
        f"- Image: `{report.image}`",
        "",
        "## Steps",
        "",
    ]
    for step in report.steps:
        lines.append(f"- `{step.name}` ({step.kind}): `{step.command}`")
    _append_issues(lines, title="Warnings", issues=report.warnings)
    _append_issues(lines, title="Blockers", issues=report.blockers)
    return "\n".join(lines).rstrip() + "\n"


def render_gitops_airflow_runtime_profile_markdown(report: GitOpsAirflowRuntimeProfile) -> str:
    lines = [
        "# GitOps Airflow runtime profile",
        "",
        f"- Status: {'passed' if report.passed else 'blocked'}",
        f"- Bundle: `{report.bundle_path}`",
        f"- Run spec: `{report.run_spec_path}`",
        f"- Runtime evidence: `{report.runtime_evidence_path}`",
        f"- Runtime profile: `{report.dag_factory_path}`",
        f"- XCom summary: `{report.xcom_summary_path}`",
        f"- Image: `{report.image}`",
        f"- Runner policy: `{report.runner_policy}`",
        "",
        "## Placement",
        "",
        f"- Namespace: `{report.namespace}`",
        f"- Service account: `{report.service_account}`",
        f"- Artifact sink: `{report.artifact_sink.kind}` `{report.artifact_sink.path or ''}`",
    ]
    _append_issues(lines, title="Warnings", issues=report.warnings)
    _append_issues(lines, title="Blockers", issues=report.blockers)
    return "\n".join(lines).rstrip() + "\n"


def render_gitops_airflow_pod_contract_markdown(report: Any) -> str:
    lines = [
        "# GitOps Airflow pod contract",
        "",
        f"- Status: {'passed' if report.passed else 'blocked'}",
        f"- Bundle: `{report.bundle_path}`",
        f"- Run spec: `{report.run_spec_path}`",
        f"- Runtime profile: `{report.runtime_profile_path}`",
        f"- Pod spec: `{report.pod_spec_path}`",
        f"- KPO kwargs: `{report.kpo_kwargs_path}`",
        f"- XCom return: `{report.xcom.return_path}`",
        "",
        "## Runtime",
        "",
        f"- Namespace: `{report.namespace}`",
        f"- Service account: `{report.service_account}`",
        f"- Image: `{report.image}`",
    ]
    _append_issues(lines, title="Warnings", issues=report.warnings)
    _append_issues(lines, title="Blockers", issues=report.blockers)
    return "\n".join(lines).rstrip() + "\n"


def render_gitops_airflow_pod_doctor_markdown(report: Any) -> str:
    lines = [
        "# GitOps Airflow pod doctor",
        "",
        f"- Status: {'passed' if report.passed else 'blocked'}",
        f"- Artifact dir: `{report.artifact_dir}`",
        f"- Pod contract: `{report.pod_contract_path}`",
        f"- Pod spec: `{report.pod_spec_path}`",
        f"- KPO kwargs: `{report.kpo_kwargs_path}`",
        f"- Runner policy: `{report.runner_policy}`",
        "",
        "## Checks",
        "",
    ]
    for check in report.checks:
        state = "passed" if check.passed else "blocked"
        lines.append(f"- `{check.name}` ({state}): {check.message}")
    _append_issues(lines, title="Warnings", issues=report.warnings)
    _append_issues(lines, title="Blockers", issues=report.blockers)
    return "\n".join(lines).rstrip() + "\n"


def render_gitops_airflow_artifact_index_markdown(report: Any) -> str:
    lines = [
        "# GitOps Airflow artifact index",
        "",
        f"- Status: {'passed' if report.passed else 'blocked'}",
        f"- Artifact dir: `{report.artifact_dir}`",
        f"- Output path: `{report.output_path}`",
        f"- Created at: `{report.created_at}`",
        "",
        "## Entries",
        "",
    ]
    for entry in report.entries:
        state = "passed" if entry.passed else "blocked"
        required = "required" if entry.required else "optional"
        lines.append(f"- `{entry.path}` ({entry.name}, {required}, {state}): `{entry.sha256 or ''}`")
    _append_issues(lines, title="Warnings", issues=report.warnings)
    _append_issues(lines, title="Blockers", issues=report.blockers)
    return "\n".join(lines).rstrip() + "\n"


def render_gitops_airflow_preflight_markdown(report: Any) -> str:
    lines = [
        "# GitOps Airflow preflight",
        "",
        f"- Status: {'passed' if report.passed else 'blocked'}",
        f"- Artifact dir: `{report.artifact_dir}`",
        f"- Artifact index: `{report.artifact_index_path}`",
        f"- Runner policy: `{report.runner_policy}`",
        "",
        "## Checks",
        "",
    ]
    for check in report.checks:
        state = "passed" if check.passed else "blocked"
        lines.append(f"- `{check.name}` ({state}): {check.message}")
    if report.next_actions:
        lines.extend(["", "## Next Actions", ""])
        for action in report.next_actions:
            lines.append(f"- {action}")
    _append_issues(lines, title="Warnings", issues=report.warnings)
    _append_issues(lines, title="Blockers", issues=report.blockers)
    return "\n".join(lines).rstrip() + "\n"


def render_gitops_airflow_outcome_gate_markdown(report: GitOpsAirflowOutcomeGateReport) -> str:
    lines = [
        "# GitOps Airflow outcome gate",
        "",
        f"- Status: {'passed' if report.passed else 'blocked'}",
        f"- XCom summary: `{report.xcom_summary_path}`",
        f"- Required status: `{report.required_status}`",
        f"- Actual status: `{report.status}`",
        f"- Failed step: `{report.failed_step or ''}`",
        f"- Runtime evidence: `{report.runtime_evidence_path}`",
    ]
    _append_issues(lines, title="Warnings", issues=report.warnings)
    _append_issues(lines, title="Blockers", issues=report.blockers)
    return "\n".join(lines).rstrip() + "\n"


def render_gitops_airflow_runtime_evidence_markdown(report: GitOpsAirflowRuntimeEvidence) -> str:
    lines = [
        "# GitOps Airflow runtime evidence",
        "",
        f"- Status: `{report.status}`",
        f"- Run spec: `{report.run_spec_path}`",
        f"- Bundle: `{report.bundle_path}`",
        f"- Image: `{report.image}`",
        "",
        "## Steps",
        "",
    ]
    for step in report.steps:
        lines.append(f"- `{step.name}` ({step.kind}, {step.status}, exit={step.exit_code}): `{step.command}`")
    _append_issues(lines, title="Warnings", issues=report.warnings)
    _append_issues(lines, title="Blockers", issues=report.blockers)
    return "\n".join(lines).rstrip() + "\n"


def render_gitops_airflow_k8s_smoke_markdown(report: GitOpsAirflowK8sSmokeReport) -> str:
    lines = [
        "# GitOps Airflow K8s smoke",
        "",
        f"- Status: {'passed' if report.passed else 'blocked'}",
        f"- Mode: `{report.mode}`",
        f"- Runner kind: `{report.runner_kind}`",
        f"- Runner policy: `{report.runner_policy}`",
        f"- Namespace: `{report.namespace}`",
        f"- Service account: `{report.service_account}`",
        f"- Image ref: `{report.image_ref}`",
        "",
        "## Checks",
        "",
    ]
    for check in report.checks:
        state = "passed" if check.passed else "blocked"
        lines.append(f"- `{check.name}` ({state}): {check.message}")
    lines.extend(["", "## Commands", ""])
    for command in report.commands:
        state = "executed" if command.executed else "planned"
        lines.append(f"- `{command.name}` ({state}, {command.kind}): `{command.command}`")
    if report.results:
        lines.extend(["", "## Results", ""])
        for result in report.results:
            state = "passed" if result.passed else "failed"
            lines.append(f"- `{result.name}` ({state}, exit={result.exit_code})")
    _append_issues(lines, title="Warnings", issues=report.warnings)
    _append_issues(lines, title="Blockers", issues=report.blockers)
    return "\n".join(lines).rstrip() + "\n"


def render_gitops_airflow_pod_launch_evidence_markdown(report: GitOpsAirflowPodLaunchEvidenceReport) -> str:
    lines = [
        "# GitOps Airflow pod launch evidence",
        "",
        f"- Status: {'passed' if report.passed else 'blocked'}",
        f"- Mode: `{report.mode}`",
        f"- Runner policy: `{report.runner_policy}`",
        f"- Pod: `{report.pod_name}`",
        f"- Namespace: `{report.namespace}`",
        f"- Service account: `{report.service_account}`",
        f"- Expected phase: `{report.expected_phase}`",
        f"- Image: `{report.image}`",
        "",
        "## Checks",
        "",
    ]
    for check in report.checks:
        state = "passed" if check.passed else "blocked"
        lines.append(f"- `{check.name}` ({state}): {check.message}")
    lines.extend(["", "## Commands", ""])
    for command in report.commands:
        state = "executed" if command.executed else "planned"
        lines.append(f"- `{command.name}` ({state}, {command.kind}): `{command.command}`")
    if report.observed_pod is not None:
        lines.extend(["", "## Observed Pod", ""])
        lines.append(f"- Phase: `{report.observed_pod.phase}`")
        lines.append(f"- Node: `{report.observed_pod.node_name or ''}`")
        for container in report.observed_pod.containers:
            lines.append(
                f"- Container `{container.name}`: `{container.state}`, "
                f"exit=`{container.exit_code}`, restarts=`{container.restart_count}`"
            )
    if report.results:
        lines.extend(["", "## Results", ""])
        for result in report.results:
            state = "passed" if result.passed else "failed"
            lines.append(f"- `{result.name}` ({state}, exit={result.exit_code})")
    _append_issues(lines, title="Warnings", issues=report.warnings)
    _append_issues(lines, title="Blockers", issues=report.blockers)
    return "\n".join(lines).rstrip() + "\n"


def render_gitops_airflow_evidence_bundle_markdown(report: GitOpsAirflowEvidenceBundleReport) -> str:
    lines = [
        "# GitOps Airflow evidence bundle",
        "",
        f"- Status: {'passed' if report.passed else 'blocked'}",
        f"- Runner policy: `{report.runner_policy}`",
        f"- DAG: `{report.attempt.dag_id}`",
        f"- Task: `{report.attempt.task_id}`",
        f"- Run: `{report.attempt.run_id}`",
        f"- Try: `{report.attempt.try_number}`",
        f"- Map index: `{report.attempt.map_index}`",
        f"- Pod: `{report.pod.pod_name or ''}`",
        f"- Pod UID: `{report.pod.pod_uid or ''}`",
        "",
        "## Artifacts",
        "",
    ]
    for artifact in report.artifacts:
        state = "passed" if artifact.passed else "blocked"
        required = "required" if artifact.required else "optional"
        lines.append(f"- `{artifact.path}` ({artifact.name}, {required}, {state}): `{artifact.sha256 or ''}`")
    _append_issues(lines, title="Warnings", issues=report.warnings)
    _append_issues(lines, title="Blockers", issues=report.blockers)
    return "\n".join(lines).rstrip() + "\n"


def _append_issues(lines: list[str], *, title: str, issues: tuple[object, ...]) -> None:
    if not issues:
        return
    lines.extend(["", f"## {title}", ""])
    for issue in issues:
        lines.append(f"- `{issue.code}` `{issue.path}`: {issue.message}")


__all__ = [
    "render_gitops_airflow_artifact_index_markdown",
    "render_gitops_airflow_doctor_markdown",
    "render_gitops_airflow_evidence_bundle_markdown",
    "render_gitops_airflow_image_contract_markdown",
    "render_gitops_airflow_k8s_smoke_markdown",
    "render_gitops_airflow_outcome_gate_markdown",
    "render_gitops_airflow_pod_launch_evidence_markdown",
    "render_gitops_airflow_pod_contract_markdown",
    "render_gitops_airflow_pod_doctor_markdown",
    "render_gitops_airflow_preflight_markdown",
    "render_gitops_airflow_render_markdown",
    "render_gitops_airflow_runtime_profile_markdown",
    "render_gitops_airflow_run_spec_markdown",
    "render_gitops_airflow_runtime_evidence_markdown",
]
