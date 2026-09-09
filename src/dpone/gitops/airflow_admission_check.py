from __future__ import annotations

import shlex
from typing import Any

from dpone.gitops.airflow_admission_check_models import (
    GitOpsAirflowAdmissionCommand,
    GitOpsAirflowAdmissionReport,
    GitOpsAirflowAdmissionResult,
    airflow_admission_check_issue,
)


class GitOpsAirflowAdmissionPlanner:
    """Build and evaluate Kubernetes admission dry-run checks."""

    def plan(
        self,
        *,
        mode: str,
        runner_policy: str,
        artifact_dir: str,
        manifest_path: str,
        pod_spec_path: str,
        timeout_seconds: int,
        kubectl: str,
    ) -> GitOpsAirflowAdmissionReport:
        return GitOpsAirflowAdmissionReport(
            mode=mode,
            runner_policy=runner_policy,
            artifact_dir=artifact_dir,
            manifest_path=manifest_path,
            pod_spec_path=pod_spec_path,
            timeout_seconds=timeout_seconds,
            commands=(
                _command(
                    "kubectl_apply_airflow_k8s_manifests",
                    "kubernetes_apply_dry_run",
                    kubectl,
                    manifest_path,
                    timeout_seconds,
                    required=True,
                ),
                _command(
                    "kubectl_apply_pod_spec",
                    "kubernetes_pod_admission_dry_run",
                    kubectl,
                    pod_spec_path,
                    timeout_seconds,
                    required=True,
                ),
            ),
        )

    def with_results(
        self,
        report: GitOpsAirflowAdmissionReport,
        *,
        results: tuple[GitOpsAirflowAdmissionResult, ...],
    ) -> GitOpsAirflowAdmissionReport:
        result_by_name = {result.name: result for result in results}
        command_by_name = {command.name: command for command in report.commands}
        commands = tuple(
            command.with_executed() if command.name in result_by_name else command for command in report.commands
        )
        blockers: list[Any] = []
        warnings: list[Any] = []
        for result in results:
            command = command_by_name.get(result.name)
            if command is None or result.exit_code == 0:
                continue
            issue = _issue(
                "airflow_admission_command_failed",
                f"Admission command {result.name} failed with exit code {result.exit_code}: {_diagnostic(result)}",
                command.path,
            )
            (blockers if command.required else warnings).append(issue)
        return report.with_results(
            commands=commands, results=results, blockers=tuple(blockers), warnings=tuple(warnings)
        )


def _command(
    name: str,
    kind: str,
    kubectl: str,
    path: str,
    timeout_seconds: int,
    *,
    required: bool,
) -> GitOpsAirflowAdmissionCommand:
    return GitOpsAirflowAdmissionCommand(
        name=name,
        kind=kind,
        command=f"{kubectl} apply --dry-run=server --filename {shlex.quote(path)}",
        path=path,
        required=required,
        timeout_seconds=timeout_seconds,
    )


def _diagnostic(result: GitOpsAirflowAdmissionResult) -> str:
    return (result.stderr or result.stdout or "no diagnostic output").strip()


def _issue(code: str, message: str, path: str) -> Any:
    return airflow_admission_check_issue(code, message, path)


__all__ = ["GitOpsAirflowAdmissionPlanner"]
