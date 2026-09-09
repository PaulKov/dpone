from __future__ import annotations

import json
import shlex
from collections.abc import Mapping, Sequence

from dpone.gitops.airflow_cluster_doctor_models import (
    AIRFLOW_CLUSTER_DOCTOR_SOURCE,
    GitOpsAirflowClusterDoctorCommand,
    GitOpsAirflowClusterDoctorResult,
    GitOpsAirflowClusterExternalSecretRef,
    GitOpsAirflowClusterSecretRef,
)
from dpone.gitops.models import GitOpsIssue

_SECRET_KEY_JSONPATH = r"{range $k,$v:=.data}{$k}{'\n'}{end}"


def build_airflow_cluster_doctor_commands(
    *,
    kubectl: str,
    namespace: str,
    service_account: str,
    timeout_seconds: int,
    secret_refs: Sequence[GitOpsAirflowClusterSecretRef],
    external_secret_refs: Sequence[GitOpsAirflowClusterExternalSecretRef],
) -> tuple[GitOpsAirflowClusterDoctorCommand, ...]:
    base = f"{kubectl} --namespace {shlex.quote(namespace)}"
    commands: list[GitOpsAirflowClusterDoctorCommand] = [
        _command(
            "kubectl_namespace",
            "kubernetes_namespace",
            f"{kubectl} get namespace {shlex.quote(namespace)} -o json",
            timeout_seconds,
        ),
        _command(
            "kubectl_service_account",
            "kubernetes_service_account",
            f"{base} get serviceaccount {shlex.quote(service_account)} -o json",
            timeout_seconds,
        ),
        *_rbac_commands(
            base=base, namespace=namespace, service_account=service_account, timeout_seconds=timeout_seconds
        ),
        _command(
            "kubectl_resource_quota",
            "kubernetes_resource_quota",
            f"{base} get resourcequota -o json",
            timeout_seconds,
            required=False,
        ),
        _command(
            "kubectl_limit_range",
            "kubernetes_limit_range",
            f"{base} get limitrange -o json",
            timeout_seconds,
            required=False,
        ),
    ]
    commands.extend(_secret_commands(base=base, refs=secret_refs, timeout_seconds=timeout_seconds))
    commands.extend(_external_secret_commands(base=base, refs=external_secret_refs, timeout_seconds=timeout_seconds))
    return tuple(commands)


def _rbac_commands(
    *,
    base: str,
    namespace: str,
    service_account: str,
    timeout_seconds: int,
) -> tuple[GitOpsAirflowClusterDoctorCommand, ...]:
    subject = f"system:serviceaccount:{namespace}:{service_account}"
    resources = (
        ("kubectl_can_create_pods", "create pods"),
        ("kubectl_can_get_pods", "get pods"),
        ("kubectl_can_get_pod_logs", "get pods/log"),
        ("kubectl_can_get_events", "get events"),
    )
    return tuple(
        _command(
            name,
            "kubernetes_rbac",
            f"{base} auth can-i {verb_resource} --as {shlex.quote(subject)}",
            timeout_seconds,
        )
        for name, verb_resource in resources
    )


def _secret_commands(
    *,
    base: str,
    refs: Sequence[GitOpsAirflowClusterSecretRef],
    timeout_seconds: int,
) -> tuple[GitOpsAirflowClusterDoctorCommand, ...]:
    commands: list[GitOpsAirflowClusterDoctorCommand] = []
    for index, ref in enumerate(refs, start=1):
        name = _safe_name(ref.name, index=index)
        if ref.required_keys:
            commands.append(
                GitOpsAirflowClusterDoctorCommand(
                    name=f"kubectl_secret_keys_{name}",
                    kind="kubernetes_secret_keys",
                    command=(
                        f"{base} get secret {shlex.quote(ref.name)} -o jsonpath={shlex.quote(_SECRET_KEY_JSONPATH)}"
                    ),
                    required=ref.required,
                    timeout_seconds=timeout_seconds,
                    expected_keys=ref.required_keys,
                )
            )
            continue
        commands.append(
            _command(
                f"kubectl_secret_{name}",
                "kubernetes_secret",
                f"{base} get secret {shlex.quote(ref.name)} -o name",
                timeout_seconds,
                required=ref.required,
            )
        )
    return tuple(commands)


def _external_secret_commands(
    *,
    base: str,
    refs: Sequence[GitOpsAirflowClusterExternalSecretRef],
    timeout_seconds: int,
) -> tuple[GitOpsAirflowClusterDoctorCommand, ...]:
    return tuple(
        _command(
            f"kubectl_external_secret_{_safe_name(ref.name, index=index)}",
            "kubernetes_external_secret",
            f"{base} get externalsecret {shlex.quote(ref.name)} -o json",
            timeout_seconds,
            required=ref.required,
        )
        for index, ref in enumerate(refs, start=1)
    )


def _command(
    name: str,
    kind: str,
    command: str,
    timeout_seconds: int,
    *,
    required: bool = True,
) -> GitOpsAirflowClusterDoctorCommand:
    return GitOpsAirflowClusterDoctorCommand(
        name=name,
        kind=kind,
        command=command,
        required=required,
        timeout_seconds=timeout_seconds,
    )


def _safe_name(value: str, *, index: int) -> str:
    clean = "".join(char.lower() if char.isalnum() else "_" for char in value).strip("_")
    return clean or f"secret_{index}"


def evaluate_cluster_doctor_results(
    *,
    commands: Mapping[str, GitOpsAirflowClusterDoctorCommand],
    results: tuple[GitOpsAirflowClusterDoctorResult, ...],
) -> tuple[tuple[GitOpsAirflowClusterDoctorResult, ...], tuple[GitOpsIssue, ...], tuple[GitOpsIssue, ...]]:
    sanitized = tuple(_sanitize_result(result, commands.get(result.name)) for result in results)
    warnings: list[GitOpsIssue] = []
    blockers: list[GitOpsIssue] = []
    for result in sanitized:
        command = commands.get(result.name)
        if command is None:
            continue
        target = blockers if command.required else warnings
        _append_result_issues(target, command=command, result=result)
    return sanitized, tuple(warnings), tuple(blockers)


def _append_result_issues(
    issues: list[GitOpsIssue],
    *,
    command: GitOpsAirflowClusterDoctorCommand,
    result: GitOpsAirflowClusterDoctorResult,
) -> None:
    if result.exit_code != 0:
        issues.append(
            _issue(
                "airflow_cluster_command_failed",
                f"Cluster doctor command {result.name} failed with exit code {result.exit_code}",
                result.name,
            )
        )
        return
    if command.kind == "kubernetes_rbac" and result.stdout.strip().lower().startswith("no"):
        issues.append(_issue("airflow_cluster_rbac_denied", f"RBAC denied: {result.name}", result.name))
    if command.kind == "kubernetes_secret_keys":
        missing = [key for key in command.expected_keys if key not in _stdout_lines(result.stdout)]
        if missing:
            issues.append(
                _issue(
                    "airflow_cluster_secret_key_missing",
                    "Missing required Secret keys: " + ", ".join(missing),
                    result.name,
                )
            )
    if command.kind == "kubernetes_external_secret" and command.required and not _external_secret_ready(result.stdout):
        issues.append(
            _issue("airflow_cluster_external_secret_not_ready", "ExternalSecret is not Ready=True", result.name)
        )


def _sanitize_result(
    result: GitOpsAirflowClusterDoctorResult,
    command: GitOpsAirflowClusterDoctorCommand | None,
) -> GitOpsAirflowClusterDoctorResult:
    if command is None or command.kind != "kubernetes_secret_keys":
        return result
    safe_stdout = "\n".join(key for key in command.expected_keys if key in _stdout_lines(result.stdout))
    return GitOpsAirflowClusterDoctorResult(
        name=result.name,
        command=result.command,
        exit_code=result.exit_code,
        stdout=(safe_stdout + "\n") if safe_stdout else "",
        stderr=result.stderr,
        duration_seconds=result.duration_seconds,
    )


def _external_secret_ready(stdout: str) -> bool:
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError:
        return False
    conditions = _mapping(_mapping(payload).get("status")).get("conditions")
    if not isinstance(conditions, list):
        return False
    return any(
        isinstance(item, Mapping) and item.get("type") == "Ready" and str(item.get("status")).lower() == "true"
        for item in conditions
    )


def _stdout_lines(stdout: str) -> frozenset[str]:
    return frozenset(line.strip() for line in stdout.splitlines() if line.strip())


def _mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _issue(code: str, message: str, path: str) -> GitOpsIssue:
    return GitOpsIssue(code=code, message=message, path=path, source=AIRFLOW_CLUSTER_DOCTOR_SOURCE)


__all__ = ["build_airflow_cluster_doctor_commands", "evaluate_cluster_doctor_results"]
