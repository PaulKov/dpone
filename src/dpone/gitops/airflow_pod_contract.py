from __future__ import annotations

import shlex
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.gitops.airflow_connection_bridge_models import (
    GitOpsAirflowConnectionBridge,
    connection_bridge_from_mapping,
)
from dpone.gitops.airflow_git_sync import GitOpsAirflowGitSyncPodPatchBuilder
from dpone.gitops.airflow_git_sync_models import GitOpsAirflowGitSyncContract, git_sync_contract_from_mapping
from dpone.gitops.airflow_interval_env import airflow_interval_env_vars
from dpone.gitops.airflow_outcome_gate import AIRFLOW_OUTCOME_XCOM_THEN_GATE, normalize_airflow_outcome_mode
from dpone.gitops.airflow_pod_contract_models import (
    GitOpsAirflowPodContract,
    GitOpsAirflowXComContract,
)

AIRFLOW_XCOM_RETURN_PATH = "/airflow/xcom/return.json"


@dataclass(frozen=True, slots=True)
class GitOpsAirflowEnvSecret:
    name: str
    secret_name: str
    secret_key: str


@dataclass(frozen=True, slots=True)
class GitOpsAirflowVolume:
    name: str
    repo_path: str


@dataclass(frozen=True, slots=True)
class GitOpsAirflowVolumeMount:
    name: str
    mount_path: str
    read_only: bool


@dataclass(frozen=True, slots=True)
class GitOpsAirflowToleration:
    key: str
    value: str
    effect: str


@dataclass(frozen=True, slots=True)
class GitOpsAirflowPodContractInput:
    bundle_path: str
    run_spec_path: str
    runtime_profile_path: str
    pod_spec_path: str
    kpo_kwargs_path: str
    bundle: Mapping[str, Any]
    run_spec: Mapping[str, Any]
    runtime_profile: Mapping[str, Any]
    image_pull_secrets: tuple[str, ...] = ()
    volumes: tuple[GitOpsAirflowVolume, ...] = ()
    volume_mounts: tuple[GitOpsAirflowVolumeMount, ...] = ()
    env_from_configmaps: tuple[str, ...] = ()
    env_secrets: tuple[GitOpsAirflowEnvSecret, ...] = ()
    node_selector: Mapping[str, str] | None = None
    tolerations: tuple[GitOpsAirflowToleration, ...] = ()
    labels: Mapping[str, str] | None = None
    annotations: Mapping[str, str] | None = None
    on_finish_action: str = "delete_pod"
    get_logs: bool = True
    deferrable: bool = False
    outcome_mode: str = "strict_fail"
    warnings: tuple[Any, ...] = ()
    blockers: tuple[Any, ...] = ()


class GitOpsAirflowPodContractBuilder:
    """Build static Airflow/Kubernetes pod handoff artifacts."""

    def build(self, data: GitOpsAirflowPodContractInput) -> GitOpsAirflowPodContract:
        profile = data.runtime_profile
        run_spec = data.run_spec
        image = _text(profile.get("image"))
        namespace = _text(profile.get("namespace")) or "default"
        service_account = _text(profile.get("service_account")) or "default"
        runtime_evidence_path = _text(profile.get("runtime_evidence_path")) or _text(run_spec.get("evidence_output"))
        xcom_summary_path = _text(profile.get("xcom_summary_path")) or ".dpone/gitops/airflow/xcom-summary.json"
        outcome_mode = normalize_airflow_outcome_mode(data.outcome_mode or profile.get("outcome_mode"))
        labels = _merge_labels(_mapping(profile.get("labels")), data.labels)
        annotations = _merge_labels(_mapping(profile.get("annotations")), data.annotations)
        git_sync = git_sync_contract_from_mapping(profile.get("git_sync"))
        connection_bridge = connection_bridge_from_mapping(profile.get("connection_bridge"))
        command = _runner_command(
            run_spec_path=data.run_spec_path,
            runtime_evidence_path=runtime_evidence_path,
            xcom_summary_path=xcom_summary_path,
            outcome_mode=outcome_mode,
        )
        pod_spec = _pod_spec(
            image=image,
            namespace=namespace,
            service_account=service_account,
            command=command,
            profile=profile,
            runtime_profile_path=data.runtime_profile_path,
            labels=labels,
            annotations=annotations,
            xcom_summary_path=xcom_summary_path,
            outcome_mode=outcome_mode,
            image_pull_secrets=data.image_pull_secrets,
            volumes=data.volumes,
            volume_mounts=data.volume_mounts,
            env_from_configmaps=data.env_from_configmaps,
            env_secrets=data.env_secrets,
            connection_bridge=connection_bridge,
            node_selector=data.node_selector or {},
            tolerations=data.tolerations,
            git_sync=git_sync,
        )
        kpo_kwargs = _kpo_kwargs(
            image=image,
            namespace=namespace,
            service_account=service_account,
            pod_spec_path=data.pod_spec_path,
            labels=labels,
            annotations=annotations,
            command=command,
            on_finish_action=data.on_finish_action,
            get_logs=data.get_logs,
            deferrable=data.deferrable,
        )
        return GitOpsAirflowPodContract(
            bundle_path=data.bundle_path,
            run_spec_path=data.run_spec_path,
            runtime_profile_path=data.runtime_profile_path,
            pod_spec_path=data.pod_spec_path,
            kpo_kwargs_path=data.kpo_kwargs_path,
            image=image,
            namespace=namespace,
            service_account=service_account,
            xcom=GitOpsAirflowXComContract(
                enabled=True,
                return_path=AIRFLOW_XCOM_RETURN_PATH,
                summary_path=xcom_summary_path,
                outcome_mode=outcome_mode,
            ),
            pod_spec=pod_spec,
            kpo_kwargs=kpo_kwargs,
            git_sync=git_sync,
            connection_bridge=connection_bridge,
            warnings=data.warnings,
            blockers=data.blockers,
        )


def _pod_spec(
    *,
    image: str,
    namespace: str,
    service_account: str,
    command: str,
    profile: Mapping[str, Any],
    runtime_profile_path: str,
    labels: Mapping[str, str],
    annotations: Mapping[str, str],
    xcom_summary_path: str,
    outcome_mode: str,
    image_pull_secrets: tuple[str, ...],
    volumes: tuple[GitOpsAirflowVolume, ...],
    volume_mounts: tuple[GitOpsAirflowVolumeMount, ...],
    env_from_configmaps: tuple[str, ...],
    env_secrets: tuple[GitOpsAirflowEnvSecret, ...],
    connection_bridge: GitOpsAirflowConnectionBridge | None,
    node_selector: Mapping[str, str],
    tolerations: tuple[GitOpsAirflowToleration, ...],
    git_sync: GitOpsAirflowGitSyncContract | None,
) -> dict[str, Any]:
    spec: dict[str, Any] = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "dpone-gitops-runtime",
            "namespace": namespace,
            "labels": dict(labels),
            "annotations": dict(annotations),
        },
        "spec": {
            "restartPolicy": "Never",
            "serviceAccountName": service_account,
            "containers": [
                {
                    "name": "base",
                    "image": image,
                    "imagePullPolicy": "IfNotPresent",
                    "command": ["/bin/sh", "-ec"],
                    "args": [command],
                    "env": _env(
                        profile=profile,
                        runtime_profile_path=runtime_profile_path,
                        xcom_summary_path=xcom_summary_path,
                        outcome_mode=outcome_mode,
                        env_secrets=env_secrets,
                        connection_bridge=connection_bridge,
                    ),
                    "resources": _mapping(profile.get("resources")),
                }
            ],
        },
    }
    container = spec["spec"]["containers"][0]
    if env_from_configmaps:
        container["envFrom"] = [{"configMapRef": {"name": name}} for name in env_from_configmaps]
    if image_pull_secrets:
        spec["spec"]["imagePullSecrets"] = [{"name": name} for name in image_pull_secrets]
    if volumes:
        spec["spec"]["volumes"] = [{"name": volume.name, "emptyDir": {}} for volume in volumes]
    if volume_mounts:
        container["volumeMounts"] = [
            {"name": mount.name, "mountPath": mount.mount_path, "readOnly": mount.read_only} for mount in volume_mounts
        ]
    if node_selector:
        spec["spec"]["nodeSelector"] = dict(node_selector)
    if tolerations:
        spec["spec"]["tolerations"] = [
            {"key": item.key, "operator": "Equal", "value": item.value, "effect": item.effect} for item in tolerations
        ]
    if git_sync is not None:
        _apply_git_sync_patch(spec, git_sync=git_sync, runtime_image=image)
    return spec


def _apply_git_sync_patch(spec: dict[str, Any], *, git_sync: GitOpsAirflowGitSyncContract, runtime_image: str) -> None:
    patch = GitOpsAirflowGitSyncPodPatchBuilder().build(contract=git_sync, runtime_image=runtime_image)
    pod_spec = spec["spec"]
    pod_spec.setdefault("volumes", [])
    pod_spec["volumes"].extend(dict(volume) for volume in patch.volumes)
    pod_spec["initContainers"] = [dict(container) for container in patch.init_containers] + list(
        pod_spec.get("initContainers", [])
    )
    container = pod_spec["containers"][0]
    container.setdefault("volumeMounts", [])
    container["volumeMounts"].extend(dict(mount) for mount in patch.base_volume_mounts)
    container["workingDir"] = patch.working_dir


def _env(
    *,
    profile: Mapping[str, Any],
    runtime_profile_path: str,
    xcom_summary_path: str,
    outcome_mode: str,
    env_secrets: tuple[GitOpsAirflowEnvSecret, ...],
    connection_bridge: GitOpsAirflowConnectionBridge | None,
) -> list[dict[str, Any]]:
    env: list[dict[str, Any]] = [
        {"name": "DPONE_AIRFLOW_RUN_SPEC", "value": _text(profile.get("run_spec_path"))},
        {"name": "DPONE_AIRFLOW_RUNTIME_PROFILE", "value": runtime_profile_path},
        {"name": "DPONE_AIRFLOW_RUNTIME_EVIDENCE", "value": _text(profile.get("runtime_evidence_path"))},
        {"name": "DPONE_AIRFLOW_XCOM_SUMMARY", "value": xcom_summary_path},
        {"name": "DPONE_AIRFLOW_XCOM_RETURN", "value": AIRFLOW_XCOM_RETURN_PATH},
        {"name": "DPONE_AIRFLOW_OUTCOME_MODE", "value": outcome_mode},
    ]
    env.extend(item for item in _profile_env(profile) if item.get("name"))
    env.extend(_connection_bridge_env(connection_bridge))
    env.extend(
        {
            "name": secret.name,
            "valueFrom": {"secretKeyRef": {"name": secret.secret_name, "key": secret.secret_key}},
        }
        for secret in env_secrets
    )
    return env


def _connection_bridge_env(connection_bridge: GitOpsAirflowConnectionBridge | None) -> list[dict[str, Any]]:
    if connection_bridge is None or connection_bridge.mode != "k8s_secret":
        return []
    entries: list[dict[str, Any]] = []
    for ref in connection_bridge.env:
        if ref.secret_name and ref.env_name:
            entries.append(
                {
                    "name": ref.env_name,
                    "valueFrom": {
                        "secretKeyRef": {
                            "name": ref.secret_name,
                            "key": ref.secret_key or ref.env_name,
                        }
                    },
                }
            )
    return entries


def _profile_env(profile: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw_env = profile.get("env")
    if not isinstance(raw_env, list):
        return []
    return [dict(item) for item in raw_env if isinstance(item, Mapping)]


def _kpo_kwargs(
    *,
    image: str,
    namespace: str,
    service_account: str,
    pod_spec_path: str,
    labels: Mapping[str, str],
    annotations: Mapping[str, str],
    command: str,
    on_finish_action: str,
    get_logs: bool,
    deferrable: bool,
) -> dict[str, Any]:
    return {
        "task_id": "dpone_gitops_runtime",
        "name": "dpone-gitops-runtime",
        "namespace": namespace,
        "image": image,
        "service_account_name": service_account,
        "pod_template_file": pod_spec_path,
        "cmds": ["/bin/sh", "-ec"],
        "arguments": [command],
        "labels": dict(labels),
        "annotations": dict(annotations),
        # Airflow renders env_vars per task instance, injecting the DAG-run
        # interval into the pod (see dpone.contracts.run_interval).
        "env_vars": airflow_interval_env_vars(),
        "get_logs": get_logs,
        "do_xcom_push": True,
        "on_finish_action": on_finish_action,
        "deferrable": deferrable,
    }


def _runner_command(
    *, run_spec_path: str, runtime_evidence_path: str, xcom_summary_path: str, outcome_mode: str
) -> str:
    run_spec_exec = (
        f"dpone gitops airflow run-spec-exec {shlex.quote(run_spec_path)} "
        f"--evidence-output {shlex.quote(runtime_evidence_path)} "
        f"--xcom-output {shlex.quote(xcom_summary_path)}"
    )
    exit_command = "exit 0" if outcome_mode == AIRFLOW_OUTCOME_XCOM_THEN_GATE else "exit $status"
    return (
        f"status=0; {run_spec_exec} || status=$?; "
        "mkdir -p /airflow/xcom; "
        f"cat {shlex.quote(xcom_summary_path)} > {shlex.quote(AIRFLOW_XCOM_RETURN_PATH)}; "
        f"{exit_command}"
    )


def _merge_labels(left: Mapping[str, str], right: Mapping[str, str] | None) -> dict[str, str]:
    merged = dict(left)
    merged.update(dict(right or {}))
    return merged


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _text(value: object) -> str:
    return str(value or "").strip()


__all__ = [
    "AIRFLOW_XCOM_RETURN_PATH",
    "GitOpsAirflowEnvSecret",
    "GitOpsAirflowPodContractBuilder",
    "GitOpsAirflowPodContractInput",
    "GitOpsAirflowToleration",
    "GitOpsAirflowVolume",
    "GitOpsAirflowVolumeMount",
]
