from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.gitops.airflow_k8s_smoke_commands import build_airflow_k8s_smoke_commands
from dpone.gitops.airflow_k8s_smoke_models import (
    AIRFLOW_K8S_SMOKE_SOURCE,
    GitOpsAirflowK8sCommandResult,
    GitOpsAirflowK8sSmokeCheck,
    GitOpsAirflowK8sSmokeCommand,
    GitOpsAirflowK8sSmokeReport,
)
from dpone.gitops.models import GitOpsIssue

_SOURCE = AIRFLOW_K8S_SMOKE_SOURCE
_RUNNER_KINDS = {"kubernetes_pod_operator", "kubernetes_executor"}


class GitOpsAirflowK8sSmokePlanner:
    """Build and evaluate opt-in Airflow Kubernetes smoke contracts."""

    def plan(
        self,
        *,
        run_spec_path: str,
        run_spec: Mapping[str, Any],
        runtime_profile_path: str,
        runtime_profile: Mapping[str, Any],
        pod_contract_path: str,
        pod_contract: Mapping[str, Any],
        image_contract_path: str | None,
        image_contract: Mapping[str, Any] | None,
        xcom_summary_path: str | None,
        xcom_summary: Mapping[str, Any] | None,
        mode: str,
        runner_kind: str,
        runner_policy: str,
        smoke_name: str,
        timeout_seconds: int,
        kubectl: str = "kubectl",
        airflow_cmd: str = "airflow",
    ) -> GitOpsAirflowK8sSmokeReport:
        image = _text(runtime_profile.get("image")) or _text(run_spec.get("image")) or _text(pod_contract.get("image"))
        image_digest = (
            _optional_text(runtime_profile.get("image_digest"))
            or _optional_text(run_spec.get("image_digest"))
            or _optional_text((image_contract or {}).get("image_digest"))
        )
        namespace = _text(runtime_profile.get("namespace")) or _text(pod_contract.get("namespace")) or "default"
        service_account = _text(runtime_profile.get("service_account")) or _text(pod_contract.get("service_account"))
        image_ref = _image_ref(image=image, image_digest=image_digest)
        checks: list[GitOpsAirflowK8sSmokeCheck] = []
        blockers: list[GitOpsIssue] = []
        warnings: list[GitOpsIssue] = []

        _append_check(
            checks,
            blockers,
            name="run_spec_kind",
            passed=run_spec.get("kind") == "gitops.airflow_run_spec",
            path=run_spec_path,
            message="Run-spec artifact has the expected kind",
        )
        _append_check(
            checks,
            blockers,
            name="runtime_profile_kind",
            passed=runtime_profile.get("kind") == "gitops.airflow_runtime_profile",
            path=runtime_profile_path,
            message="Runtime profile artifact has the expected kind",
        )
        _append_check(
            checks,
            blockers,
            name="pod_contract_kind",
            passed=pod_contract.get("kind") == "gitops.airflow_pod_contract",
            path=pod_contract_path,
            message="Pod contract artifact has the expected kind",
        )
        _append_runner_kind_check(checks, blockers, runner_kind=runner_kind)
        _append_image_checks(
            checks,
            blockers,
            image=image,
            image_digest=image_digest,
            pod_contract=pod_contract,
            image_contract=image_contract,
            image_contract_path=image_contract_path,
            runner_policy=runner_policy,
        )
        _append_placement_checks(
            checks,
            blockers,
            namespace=namespace,
            service_account=service_account,
            pod_contract=pod_contract,
            runtime_profile_path=runtime_profile_path,
            runner_policy=runner_policy,
        )
        _append_xcom_checks(
            checks,
            blockers,
            xcom_summary_path=xcom_summary_path,
            xcom_summary=xcom_summary,
            pod_contract=pod_contract,
            pod_contract_path=pod_contract_path,
            runner_policy=runner_policy,
        )
        command_specs = build_airflow_k8s_smoke_commands(
            kubectl=kubectl,
            airflow_cmd=airflow_cmd,
            smoke_name=smoke_name,
            namespace=namespace,
            service_account=service_account,
            image_ref=image_ref,
            run_spec_path=run_spec_path,
            evidence_output=_text(run_spec.get("evidence_output")) or ".dpone/gitops/airflow/runtime-evidence.json",
            xcom_summary_path=xcom_summary_path or ".dpone/gitops/airflow/xcom-summary.json",
            runner_kind=runner_kind,
            timeout_seconds=timeout_seconds,
        )
        commands = tuple(
            GitOpsAirflowK8sSmokeCommand(
                name=spec.name,
                kind=spec.kind,
                command=spec.command,
                required=spec.required,
                timeout_seconds=spec.timeout_seconds,
            )
            for spec in command_specs
        )
        return GitOpsAirflowK8sSmokeReport(
            mode=mode,
            runner_kind=runner_kind,
            runner_policy=runner_policy,
            run_spec_path=run_spec_path,
            runtime_profile_path=runtime_profile_path,
            pod_contract_path=pod_contract_path,
            image_contract_path=image_contract_path,
            xcom_summary_path=xcom_summary_path,
            namespace=namespace,
            service_account=service_account,
            image=image,
            image_digest=image_digest,
            image_ref=image_ref,
            smoke_name=smoke_name,
            timeout_seconds=timeout_seconds,
            checks=tuple(checks),
            commands=commands,
            warnings=tuple(warnings),
            blockers=tuple(blockers),
        )

    def with_results(
        self,
        report: GitOpsAirflowK8sSmokeReport,
        *,
        results: tuple[GitOpsAirflowK8sCommandResult, ...],
    ) -> GitOpsAirflowK8sSmokeReport:
        result_by_name = {result.name: result for result in results}
        commands = tuple(
            command.with_executed() if command.name in result_by_name else command for command in report.commands
        )
        blockers = tuple(
            GitOpsIssue(
                code="airflow_k8s_command_failed",
                message=f"Smoke command {result.name} failed with exit code {result.exit_code}",
                path=result.name,
                source=_SOURCE,
            )
            for result in results
            if result.exit_code != 0 and _command_required(report.commands, result.name)
        )
        return report.with_results(commands=commands, results=results, blockers=blockers)


def _append_runner_kind_check(
    checks: list[GitOpsAirflowK8sSmokeCheck],
    blockers: list[GitOpsIssue],
    *,
    runner_kind: str,
) -> None:
    passed = runner_kind in _RUNNER_KINDS
    _append_check(
        checks,
        blockers,
        name="airflow_runner_kind",
        passed=passed,
        path="--runner-kind",
        message="Runner kind is supported by the Airflow K8s smoke contract",
        code="airflow_k8s_runner_kind_invalid",
    )


def _append_image_checks(
    checks: list[GitOpsAirflowK8sSmokeCheck],
    blockers: list[GitOpsIssue],
    *,
    image: str,
    image_digest: str | None,
    pod_contract: Mapping[str, Any],
    image_contract: Mapping[str, Any] | None,
    image_contract_path: str | None,
    runner_policy: str,
) -> None:
    pod_image = _text(pod_contract.get("image"))
    _append_check(
        checks,
        blockers,
        name="airflow_k8s_image_matches_pod_contract",
        passed=bool(image) and image == pod_image,
        path="pod_contract.image",
        message="Runtime image matches the pod contract image",
        code="airflow_k8s_image_mismatch",
    )
    if image_contract is not None:
        contract_tools = tuple(str(tool) for tool in _list(image_contract.get("tools")))
        _append_check(
            checks,
            blockers,
            name="airflow_k8s_image_contract_has_dpone",
            passed="dpone" in contract_tools,
            path=image_contract_path or "image_contract",
            message="Image contract declares the dpone executable",
            code="airflow_k8s_image_contract_missing_dpone",
        )
    if runner_policy == "release":
        _append_check(
            checks,
            blockers,
            name="airflow_k8s_image_digest",
            passed=bool(image_digest),
            path=image_contract_path or "image_digest",
            message="Release smoke uses an immutable image digest",
            code="airflow_k8s_image_digest_required",
        )


def _append_placement_checks(
    checks: list[GitOpsAirflowK8sSmokeCheck],
    blockers: list[GitOpsIssue],
    *,
    namespace: str,
    service_account: str,
    pod_contract: Mapping[str, Any],
    runtime_profile_path: str,
    runner_policy: str,
) -> None:
    _append_check(
        checks,
        blockers,
        name="airflow_k8s_namespace",
        passed=bool(namespace) and namespace == _text(pod_contract.get("namespace")),
        path=runtime_profile_path,
        message="Runtime profile namespace matches the pod contract",
        code="airflow_k8s_namespace_mismatch",
    )
    service_account_matches = bool(service_account) and service_account == _text(pod_contract.get("service_account"))
    _append_check(
        checks,
        blockers,
        name="airflow_k8s_service_account",
        passed=service_account_matches,
        path=runtime_profile_path,
        message="Runtime profile service account matches the pod contract",
        code="airflow_k8s_service_account_mismatch",
    )
    if runner_policy == "release":
        _append_check(
            checks,
            blockers,
            name="airflow_k8s_release_service_account",
            passed=bool(service_account) and service_account != "default",
            path=runtime_profile_path,
            message="Release smoke uses a non-default Kubernetes service account",
            code="airflow_k8s_service_account_required",
        )


def _append_xcom_checks(
    checks: list[GitOpsAirflowK8sSmokeCheck],
    blockers: list[GitOpsIssue],
    *,
    xcom_summary_path: str | None,
    xcom_summary: Mapping[str, Any] | None,
    pod_contract: Mapping[str, Any],
    pod_contract_path: str,
    runner_policy: str,
) -> None:
    xcom = _mapping(pod_contract.get("xcom"))
    _append_check(
        checks,
        blockers,
        name="airflow_k8s_xcom_enabled",
        passed=xcom.get("enabled") is True and xcom.get("return_path") == "/airflow/xcom/return.json",
        path=pod_contract_path,
        message="Pod contract enables final Airflow XCom return.json",
        code="airflow_k8s_xcom_required",
    )
    if xcom_summary is None:
        if runner_policy == "release":
            blockers.append(
                GitOpsIssue(
                    code="airflow_k8s_xcom_summary_required",
                    message="Release smoke requires final XCom summary evidence",
                    path=xcom_summary_path or "xcom_summary",
                    source=_SOURCE,
                )
            )
        return
    _append_check(
        checks,
        blockers,
        name="airflow_k8s_xcom_status",
        passed=xcom_summary.get("status") == "passed",
        path=xcom_summary_path or "xcom_summary",
        message="Final XCom outcome is passed",
        code="airflow_k8s_xcom_failed",
    )


def _append_check(
    checks: list[GitOpsAirflowK8sSmokeCheck],
    blockers: list[GitOpsIssue],
    *,
    name: str,
    passed: bool,
    path: str,
    message: str,
    severity: str = "blocker",
    code: str | None = None,
) -> None:
    checks.append(GitOpsAirflowK8sSmokeCheck(name=name, passed=passed, severity=severity, message=message, path=path))
    if not passed and severity == "blocker":
        blockers.append(GitOpsIssue(code=code or name, message=message, path=path, source=_SOURCE))


def _command_required(commands: tuple[GitOpsAirflowK8sSmokeCommand, ...], name: str) -> bool:
    return any(command.name == name and command.required for command in commands)


def _image_ref(*, image: str, image_digest: str | None) -> str:
    if not image_digest:
        return image
    if "@" in image:
        return image
    return f"{image}@{image_digest}"


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _list(value: object) -> tuple[object, ...]:
    return tuple(value) if isinstance(value, list) else ()


def _text(value: object) -> str:
    return str(value or "").strip()


def _optional_text(value: object) -> str | None:
    text = _text(value)
    return text or None


__all__ = [
    "GitOpsAirflowK8sCommandResult",
    "GitOpsAirflowK8sSmokeCheck",
    "GitOpsAirflowK8sSmokeCommand",
    "GitOpsAirflowK8sSmokePlanner",
    "GitOpsAirflowK8sSmokeReport",
]
