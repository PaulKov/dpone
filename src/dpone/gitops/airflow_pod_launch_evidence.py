from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from dpone.gitops.airflow_pod_launch_evidence_models import (
    GitOpsAirflowPodLaunchEvidenceCheck,
    GitOpsAirflowPodLaunchEvidenceCommand,
    GitOpsAirflowPodLaunchEvidenceCommandResult,
    GitOpsAirflowPodLaunchEvidenceReport,
)
from dpone.gitops.airflow_pod_launch_evidence_parser import (
    GitOpsAirflowObservedPod,
    GitOpsAirflowPodContainerEvidence,
    GitOpsAirflowPodEventEvidence,
    parse_observed_pod,
)
from dpone.gitops.airflow_pod_launch_evidence_policy import (
    append_artifact_checks,
    append_observed_checks,
    append_profile_contract_checks,
    pod_command_blockers,
    pod_evidence_issue,
)
from dpone.gitops.models import GitOpsIssue


class GitOpsAirflowPodLaunchEvidencePlanner:
    """Build and evaluate Airflow Kubernetes pod launch evidence."""

    def plan(
        self,
        *,
        runtime_profile_path: str,
        runtime_profile: Mapping[str, Any],
        pod_contract_path: str,
        pod_contract: Mapping[str, Any],
        image_contract_path: str | None,
        image_contract: Mapping[str, Any] | None,
        runtime_evidence_path: str | None,
        runtime_evidence: Mapping[str, Any] | None,
        xcom_summary_path: str | None,
        xcom_summary: Mapping[str, Any] | None,
        mode: str,
        runner_policy: str,
        pod_name: str | None,
        expected_phase: str,
        timeout_seconds: int,
        log_tail_lines: int,
        kubectl: str = "kubectl",
        warnings: tuple[GitOpsIssue, ...] = (),
        blockers: tuple[GitOpsIssue, ...] = (),
    ) -> GitOpsAirflowPodLaunchEvidenceReport:
        image = _first_text(
            runtime_profile.get("image"), pod_contract.get("image"), (image_contract or {}).get("image")
        )
        image_digest = _first_optional_text(
            runtime_profile.get("image_digest"), (image_contract or {}).get("image_digest")
        )
        namespace = _first_text(runtime_profile.get("namespace"), pod_contract.get("namespace"), "default")
        service_account = _first_text(runtime_profile.get("service_account"), pod_contract.get("service_account"))
        resolved_pod_name = _resolve_pod_name(pod_contract=pod_contract, explicit=pod_name)
        checks: list[GitOpsAirflowPodLaunchEvidenceCheck] = []
        plan_blockers = list(blockers)

        append_artifact_checks(
            checks,
            plan_blockers,
            runtime_profile_path=runtime_profile_path,
            runtime_profile=runtime_profile,
            pod_contract_path=pod_contract_path,
            pod_contract=pod_contract,
            image_contract_path=image_contract_path,
            image_contract=image_contract,
            runtime_evidence_path=runtime_evidence_path,
            runtime_evidence=runtime_evidence,
            xcom_summary_path=xcom_summary_path,
            xcom_summary=xcom_summary,
            runner_policy=runner_policy,
        )
        append_profile_contract_checks(
            checks,
            plan_blockers,
            image=image,
            image_digest=image_digest,
            namespace=namespace,
            service_account=service_account,
            runtime_profile_path=runtime_profile_path,
            pod_contract=pod_contract,
            image_contract=image_contract,
            image_contract_path=image_contract_path,
            runner_policy=runner_policy,
        )
        commands = _build_commands(
            kubectl=kubectl,
            pod_name=resolved_pod_name,
            namespace=namespace,
            timeout_seconds=timeout_seconds,
            log_tail_lines=log_tail_lines,
        )
        return GitOpsAirflowPodLaunchEvidenceReport(
            mode=mode,
            runner_policy=runner_policy,
            runtime_profile_path=runtime_profile_path,
            pod_contract_path=pod_contract_path,
            image_contract_path=image_contract_path,
            runtime_evidence_path=runtime_evidence_path,
            xcom_summary_path=xcom_summary_path,
            pod_name=resolved_pod_name,
            namespace=namespace,
            service_account=service_account,
            image=image,
            image_digest=image_digest,
            expected_phase=expected_phase,
            timeout_seconds=timeout_seconds,
            log_tail_lines=log_tail_lines,
            checks=tuple(checks),
            commands=commands,
            warnings=warnings,
            blockers=tuple(plan_blockers),
        )

    def with_results(
        self,
        report: GitOpsAirflowPodLaunchEvidenceReport,
        *,
        results: tuple[GitOpsAirflowPodLaunchEvidenceCommandResult, ...],
    ) -> GitOpsAirflowPodLaunchEvidenceReport:
        result_by_name = {result.name: result for result in results}
        commands = tuple(
            command.with_executed() if command.name in result_by_name else command for command in report.commands
        )
        checks: list[GitOpsAirflowPodLaunchEvidenceCheck] = []
        warnings: list[GitOpsIssue] = []
        blockers = pod_command_blockers(commands=report.commands, results=results)
        observed_pod, parse_blockers = _observed_pod_from_results(result_by_name)
        blockers.extend(parse_blockers)
        if observed_pod is not None:
            append_observed_checks(checks, blockers, report=report, observed_pod=observed_pod)
        elif results:
            blockers.append(
                pod_evidence_issue(
                    code="airflow_pod_json_missing",
                    message="kubectl get pod JSON evidence is missing",
                    path="kubectl_get_pod",
                )
            )
        return report.with_results(
            commands=commands,
            results=results,
            observed_pod=observed_pod,
            checks=tuple(checks),
            warnings=tuple(warnings),
            blockers=tuple(blockers),
        )


def _observed_pod_from_results(
    result_by_name: Mapping[str, GitOpsAirflowPodLaunchEvidenceCommandResult],
) -> tuple[GitOpsAirflowObservedPod | None, list[GitOpsIssue]]:
    blockers: list[GitOpsIssue] = []
    pod = _json_result(result_by_name.get("kubectl_get_pod"), path="kubectl_get_pod", blockers=blockers)
    if pod is None:
        return None, blockers
    events = _json_result(result_by_name.get("kubectl_get_events"), path="kubectl_get_events", blockers=blockers)
    logs = (result_by_name.get("kubectl_logs_base") or GitOpsAirflowPodLaunchEvidenceCommandResult("", "", 0)).stdout
    return parse_observed_pod(pod=pod, events=events, logs_tail=logs), blockers


def _json_result(
    result: GitOpsAirflowPodLaunchEvidenceCommandResult | None,
    *,
    path: str,
    blockers: list[GitOpsIssue],
) -> Mapping[str, Any] | None:
    if result is None or result.exit_code != 0:
        return None
    try:
        loaded = json.loads(result.stdout or "{}")
    except json.JSONDecodeError as exc:
        blockers.append(
            pod_evidence_issue(
                code="airflow_pod_json_invalid",
                message=f"Kubernetes JSON could not be parsed: {exc.msg}",
                path=path,
            )
        )
        return None
    if not isinstance(loaded, Mapping):
        blockers.append(
            pod_evidence_issue(code="airflow_pod_json_invalid", message="Kubernetes JSON must be an object", path=path)
        )
        return None
    return loaded


def _build_commands(
    *,
    kubectl: str,
    pod_name: str,
    namespace: str,
    timeout_seconds: int,
    log_tail_lines: int,
) -> tuple[GitOpsAirflowPodLaunchEvidenceCommand, ...]:
    return (
        GitOpsAirflowPodLaunchEvidenceCommand(
            name="kubectl_get_pod",
            kind="kubernetes_pod",
            command=f"{kubectl} get pod {pod_name} -n {namespace} -o json",
            required=True,
            timeout_seconds=timeout_seconds,
        ),
        GitOpsAirflowPodLaunchEvidenceCommand(
            name="kubectl_get_events",
            kind="kubernetes_events",
            command=f"{kubectl} get events -n {namespace} --field-selector involvedObject.name={pod_name} -o json",
            required=False,
            timeout_seconds=timeout_seconds,
        ),
        GitOpsAirflowPodLaunchEvidenceCommand(
            name="kubectl_logs_base",
            kind="kubernetes_logs",
            command=f"{kubectl} logs {pod_name} -n {namespace} -c base --tail={log_tail_lines}",
            required=False,
            timeout_seconds=timeout_seconds,
        ),
    )


def _resolve_pod_name(*, pod_contract: Mapping[str, Any], explicit: str | None) -> str:
    return _first_text(
        explicit,
        _mapping(pod_contract.get("kpo_kwargs")).get("name"),
        _mapping(_mapping(pod_contract.get("pod_spec")).get("metadata")).get("name"),
        "dpone-gitops-runtime",
    )


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _text(value: object) -> str:
    return str(value or "").strip()


def _first_text(*values: object) -> str:
    return next((text for text in (_text(value) for value in values) if text), "")


def _first_optional_text(*values: object) -> str | None:
    text = _first_text(*values)
    return text or None


__all__ = [
    "GitOpsAirflowObservedPod",
    "GitOpsAirflowPodContainerEvidence",
    "GitOpsAirflowPodEventEvidence",
    "GitOpsAirflowPodLaunchEvidenceCheck",
    "GitOpsAirflowPodLaunchEvidenceCommand",
    "GitOpsAirflowPodLaunchEvidenceCommandResult",
    "GitOpsAirflowPodLaunchEvidencePlanner",
    "GitOpsAirflowPodLaunchEvidenceReport",
]
