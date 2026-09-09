from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dpone.gitops.airflow_cluster_doctor_commands import (
    build_airflow_cluster_doctor_commands,
    evaluate_cluster_doctor_results,
)
from dpone.gitops.airflow_cluster_doctor_models import (
    AIRFLOW_CLUSTER_DOCTOR_SOURCE,
    GitOpsAirflowClusterDoctorCheck,
    GitOpsAirflowClusterDoctorReport,
    GitOpsAirflowClusterDoctorResult,
    GitOpsAirflowClusterExternalSecretRef,
    GitOpsAirflowClusterSecretRef,
)
from dpone.gitops.models import GitOpsIssue

_SOURCE = AIRFLOW_CLUSTER_DOCTOR_SOURCE


class GitOpsAirflowClusterDoctorPlanner:
    """Build and evaluate opt-in Airflow/Kubernetes cluster readiness checks."""

    def plan(
        self,
        *,
        artifact_dir: str,
        runtime_profile_path: str,
        runtime_profile: Mapping[str, Any],
        pod_contract_path: str,
        pod_contract: Mapping[str, Any],
        connection_bridge_plan_path: str | None,
        connection_bridge_plan: Mapping[str, Any] | None,
        mode: str,
        runner_policy: str,
        timeout_seconds: int,
        kubectl: str = "kubectl",
        external_secret_names: Sequence[str] = (),
        extra_external_secrets: Sequence[str] = (),
        require_external_secret_ready: bool = False,
        require_external_secret: bool = False,
    ) -> GitOpsAirflowClusterDoctorReport:
        namespace = _text(runtime_profile.get("namespace")) or _text(pod_contract.get("namespace")) or "default"
        service_account = _text(runtime_profile.get("service_account")) or _text(pod_contract.get("service_account"))
        checks, blockers = _static_checks(
            runtime_profile_path=runtime_profile_path,
            runtime_profile=runtime_profile,
            pod_contract_path=pod_contract_path,
            pod_contract=pod_contract,
            connection_bridge_plan_path=connection_bridge_plan_path,
            connection_bridge_plan=connection_bridge_plan,
            namespace=namespace,
            service_account=service_account,
            runner_policy=runner_policy,
        )
        secret_refs = _secret_refs(
            runtime_profile=runtime_profile,
            pod_contract=pod_contract,
            connection_bridge_plan=connection_bridge_plan,
        )
        external_refs = _external_secret_refs(
            connection_bridge_plan=connection_bridge_plan,
            extra_external_secrets=(*external_secret_names, *extra_external_secrets),
            required=require_external_secret_ready or require_external_secret,
        )
        commands = build_airflow_cluster_doctor_commands(
            kubectl=kubectl,
            namespace=namespace,
            service_account=service_account,
            timeout_seconds=timeout_seconds,
            secret_refs=secret_refs,
            external_secret_refs=external_refs,
        )
        return GitOpsAirflowClusterDoctorReport(
            mode=mode,
            runner_policy=runner_policy,
            artifact_dir=artifact_dir,
            runtime_profile_path=runtime_profile_path,
            pod_contract_path=pod_contract_path,
            connection_bridge_plan_path=connection_bridge_plan_path,
            namespace=namespace,
            service_account=service_account,
            timeout_seconds=timeout_seconds,
            secret_refs=secret_refs,
            external_secret_refs=external_refs,
            checks=checks,
            commands=commands,
            blockers=blockers,
        )

    def with_results(
        self,
        report: GitOpsAirflowClusterDoctorReport,
        *,
        results: tuple[GitOpsAirflowClusterDoctorResult, ...],
    ) -> GitOpsAirflowClusterDoctorReport:
        command_by_name = {command.name: command for command in report.commands}
        sanitized, warnings, blockers = evaluate_cluster_doctor_results(commands=command_by_name, results=results)
        result_by_name = {result.name: result for result in sanitized}
        commands = tuple(
            command.with_executed() if command.name in result_by_name else command for command in report.commands
        )
        return report.with_results(
            commands=commands,
            results=sanitized,
            warnings=warnings,
            blockers=blockers,
        )


def _static_checks(
    *,
    runtime_profile_path: str,
    runtime_profile: Mapping[str, Any],
    pod_contract_path: str,
    pod_contract: Mapping[str, Any],
    connection_bridge_plan_path: str | None,
    connection_bridge_plan: Mapping[str, Any] | None,
    namespace: str,
    service_account: str,
    runner_policy: str,
) -> tuple[tuple[GitOpsAirflowClusterDoctorCheck, ...], tuple[GitOpsIssue, ...]]:
    checks: list[GitOpsAirflowClusterDoctorCheck] = []
    blockers: list[GitOpsIssue] = []
    _append_check(
        checks,
        blockers,
        "runtime_profile_kind",
        runtime_profile.get("kind") == "gitops.airflow_runtime_profile",
        runtime_profile_path,
        "Runtime profile artifact has the expected kind",
    )
    _append_check(
        checks,
        blockers,
        "pod_contract_kind",
        pod_contract.get("kind") == "gitops.airflow_pod_contract",
        pod_contract_path,
        "Pod contract artifact has the expected kind",
    )
    _append_check(
        checks,
        blockers,
        "cluster_namespace_matches_contract",
        namespace == _text(pod_contract.get("namespace")),
        runtime_profile_path,
        "Runtime profile namespace matches pod contract",
    )
    _append_check(
        checks,
        blockers,
        "cluster_service_account_matches_contract",
        service_account == _text(pod_contract.get("service_account")),
        runtime_profile_path,
        "Runtime profile service account matches pod contract",
    )
    if runner_policy == "release":
        _append_check(
            checks,
            blockers,
            "cluster_release_service_account",
            bool(service_account) and service_account != "default",
            runtime_profile_path,
            "Release cluster doctor uses a non-default service account",
        )
        _append_check(
            checks,
            blockers,
            "cluster_release_resources",
            _base_container_has_resources(pod_contract),
            pod_contract_path,
            "Base container has CPU and memory requests and limits",
        )
    if _bridge_required(pod_contract) and connection_bridge_plan is None:
        _append_check(
            checks,
            blockers,
            "cluster_connection_bridge_plan",
            False,
            connection_bridge_plan_path or "connection-bridge-plan.json",
            "Runtime-only Airflow connection bridge has a deploy-time plan",
        )
    elif connection_bridge_plan is not None:
        _append_check(
            checks,
            blockers,
            "cluster_connection_bridge_plan",
            connection_bridge_plan.get("kind") == "gitops.airflow_connection_bridge_plan",
            connection_bridge_plan_path or "connection-bridge-plan.json",
            "Connection bridge plan artifact has the expected kind",
        )
    return tuple(checks), tuple(blockers)


def _secret_refs(
    *,
    runtime_profile: Mapping[str, Any],
    pod_contract: Mapping[str, Any],
    connection_bridge_plan: Mapping[str, Any] | None,
) -> tuple[GitOpsAirflowClusterSecretRef, ...]:
    refs: dict[tuple[str, str], GitOpsAirflowClusterSecretRef] = {}
    _merge_secret_refs(refs, _image_pull_secrets(pod_contract))
    _merge_secret_refs(refs, _git_sync_secret_refs(runtime_profile.get("git_sync") or pod_contract.get("git_sync")))
    _merge_secret_refs(refs, _connection_secret_refs(connection_bridge_plan or pod_contract.get("connection_bridge")))
    return tuple(refs.values())


def _image_pull_secrets(pod_contract: Mapping[str, Any]) -> tuple[GitOpsAirflowClusterSecretRef, ...]:
    raw = _mapping(_mapping(pod_contract.get("pod_spec")).get("spec")).get("imagePullSecrets")
    if not isinstance(raw, list):
        return ()
    return tuple(
        GitOpsAirflowClusterSecretRef(
            name=_text(item.get("name")), kind="imagePullSecret", source="pod_spec", required=True
        )
        for item in raw
        if isinstance(item, Mapping) and _text(item.get("name"))
    )


def _git_sync_secret_refs(value: object) -> tuple[GitOpsAirflowClusterSecretRef, ...]:
    git_sync = _mapping(value)
    auth = _mapping(git_sync.get("auth"))
    mode = _text(auth.get("mode"))
    if mode == "ssh_secret":
        secret = _mapping(auth.get("ssh_secret"))
        name = _text(secret.get("name"))
        if not name:
            return ()
        return (
            GitOpsAirflowClusterSecretRef(
                name=name,
                kind="gitSyncSshSecret",
                source="git_sync.auth",
                required=True,
                required_keys=(
                    _text(secret.get("ssh_key")) or "ssh",
                    _text(secret.get("known_hosts_key")) or "known_hosts",
                ),
            ),
        )
    if mode == "https_secret":
        secret = _mapping(auth.get("https_secret"))
        name = _text(secret.get("name"))
        if not name:
            return ()
        return (
            GitOpsAirflowClusterSecretRef(
                name=name,
                kind="gitSyncHttpsSecret",
                source="git_sync.auth",
                required=True,
                required_keys=(
                    _text(secret.get("username_key")) or "username",
                    _text(secret.get("password_key")) or "password",
                ),
            ),
        )
    return ()


def _connection_secret_refs(value: object) -> tuple[GitOpsAirflowClusterSecretRef, ...]:
    payload = _mapping(value)
    raw_env = payload.get("env")
    if not isinstance(raw_env, list):
        return ()
    by_name: dict[str, set[str]] = {}
    for item in raw_env:
        secret = _mapping(_mapping(item).get("secret_ref"))
        name = _text(secret.get("name"))
        key = _text(secret.get("key"))
        if name and key:
            by_name.setdefault(name, set()).add(key)
    return tuple(
        GitOpsAirflowClusterSecretRef(
            name=name,
            kind="airflowConnectionSecret",
            source="connection_bridge",
            required=True,
            required_keys=tuple(sorted(keys)),
        )
        for name, keys in sorted(by_name.items())
    )


def _external_secret_refs(
    *,
    connection_bridge_plan: Mapping[str, Any] | None,
    extra_external_secrets: Sequence[str],
    required: bool,
) -> tuple[GitOpsAirflowClusterExternalSecretRef, ...]:
    names = [(name, "cli") for name in extra_external_secrets if name]
    plan = connection_bridge_plan or {}
    if _has_external_secret_artifact(plan) and _text(plan.get("secret_name")):
        names.append((_text(plan.get("secret_name")), "connection_bridge_plan"))
    deduped = dict.fromkeys(names)
    return tuple(
        GitOpsAirflowClusterExternalSecretRef(name=name, source=source, required=required) for name, source in deduped
    )


def _merge_secret_refs(
    target: dict[tuple[str, str], GitOpsAirflowClusterSecretRef],
    refs: Sequence[GitOpsAirflowClusterSecretRef],
) -> None:
    for ref in refs:
        key = (ref.name, ref.kind)
        existing = target.get(key)
        merged_keys = tuple(sorted({*(existing.required_keys if existing else ()), *ref.required_keys}))
        target[key] = GitOpsAirflowClusterSecretRef(
            name=ref.name,
            kind=ref.kind,
            source=ref.source,
            required=ref.required or bool(existing and existing.required),
            required_keys=merged_keys,
        )


def _append_check(
    checks: list[GitOpsAirflowClusterDoctorCheck],
    blockers: list[GitOpsIssue],
    name: str,
    passed: bool,
    path: str,
    message: str,
) -> None:
    checks.append(
        GitOpsAirflowClusterDoctorCheck(name=name, passed=passed, severity="blocker", message=message, path=path)
    )
    if not passed:
        blockers.append(GitOpsIssue(code=name, message=message, path=path, source=_SOURCE))


def _base_container_has_resources(pod_contract: Mapping[str, Any]) -> bool:
    container = _first_container(_mapping(pod_contract.get("pod_spec")))
    resources = _mapping(container.get("resources"))
    requests = _mapping(resources.get("requests"))
    limits = _mapping(resources.get("limits"))
    return all(_text(block.get(name)) for block in (requests, limits) for name in ("cpu", "memory"))


def _bridge_required(pod_contract: Mapping[str, Any]) -> bool:
    bridge = _mapping(pod_contract.get("connection_bridge"))
    return bool(bridge.get("required_connection_ids")) and bridge.get("runtime_mode") != "airflow_image"


def _has_external_secret_artifact(plan: Mapping[str, Any]) -> bool:
    raw = plan.get("artifacts")
    return isinstance(raw, list) and any(_mapping(item).get("kind") == "kubernetes.ExternalSecret" for item in raw)


def _first_container(pod_spec: Mapping[str, Any]) -> Mapping[str, Any]:
    containers = _mapping(pod_spec.get("spec")).get("containers")
    if isinstance(containers, list) and containers and isinstance(containers[0], Mapping):
        return containers[0]
    return {}


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _text(value: object) -> str:
    return str(value or "").strip()


__all__ = ["GitOpsAirflowClusterDoctorPlanner"]
