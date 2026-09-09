from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from dpone.gitops.airflow_git_sync import reserved_git_sync_volume_names
from dpone.gitops.airflow_pod_contract import (
    GitOpsAirflowEnvSecret,
    GitOpsAirflowToleration,
    GitOpsAirflowVolume,
    GitOpsAirflowVolumeMount,
)
from dpone.gitops.models import GitOpsIssue
from dpone.gitops.paths import GitOpsPathValidationError, safe_relative_path


class GitOpsAirflowPodContractOptions:
    def __init__(
        self,
        *,
        image_pull_secrets: tuple[str, ...],
        volumes: tuple[GitOpsAirflowVolume, ...],
        volume_mounts: tuple[GitOpsAirflowVolumeMount, ...],
        env_from_configmaps: tuple[str, ...],
        env_secrets: tuple[GitOpsAirflowEnvSecret, ...],
        node_selector: dict[str, str],
        tolerations: tuple[GitOpsAirflowToleration, ...],
        labels: dict[str, str],
        annotations: dict[str, str],
        on_finish_action: str,
        get_logs: bool,
        deferrable: bool,
        outcome_mode: str,
    ) -> None:
        self.image_pull_secrets = image_pull_secrets
        self.volumes = volumes
        self.volume_mounts = volume_mounts
        self.env_from_configmaps = env_from_configmaps
        self.env_secrets = env_secrets
        self.node_selector = node_selector
        self.tolerations = tolerations
        self.labels = labels
        self.annotations = annotations
        self.on_finish_action = on_finish_action
        self.get_logs = get_logs
        self.deferrable = deferrable
        self.outcome_mode = outcome_mode


def parse_pod_contract_options(args: object) -> tuple[GitOpsAirflowPodContractOptions, tuple[GitOpsIssue, ...]]:
    labels, label_blockers = _parse_key_values(getattr(args, "label", ()), source="--label")
    annotations, annotation_blockers = _parse_key_values(getattr(args, "annotation", ()), source="--annotation")
    node_selector, selector_blockers = _parse_key_values(
        getattr(args, "node_selector", ()),
        source="--node-selector",
    )
    volumes, volume_blockers = _parse_volumes(getattr(args, "volume", ()))
    mounts, mount_blockers = _parse_volume_mounts(getattr(args, "volume_mount", ()))
    env_secrets, secret_blockers = _parse_env_secrets(getattr(args, "env_secret", ()))
    tolerations, toleration_blockers = _parse_tolerations(getattr(args, "toleration", ()))
    options = GitOpsAirflowPodContractOptions(
        image_pull_secrets=_text_tuple(getattr(args, "image_pull_secret", ())),
        volumes=volumes,
        volume_mounts=mounts,
        env_from_configmaps=_text_tuple(getattr(args, "env_from_configmap", ())),
        env_secrets=env_secrets,
        node_selector=node_selector,
        tolerations=tolerations,
        labels=labels,
        annotations=annotations,
        on_finish_action=_text(getattr(args, "on_finish_action", None)) or "delete_pod",
        get_logs=bool(getattr(args, "get_logs", True)),
        deferrable=bool(getattr(args, "deferrable", False)),
        outcome_mode=_text(getattr(args, "outcome_mode", None)),
    )
    blockers = (
        *label_blockers,
        *annotation_blockers,
        *selector_blockers,
        *volume_blockers,
        *mount_blockers,
        *secret_blockers,
        *toleration_blockers,
    )
    return options, blockers


def git_sync_option_blockers(
    *, runtime_profile: Mapping[str, Any], options: GitOpsAirflowPodContractOptions
) -> tuple[GitOpsIssue, ...]:
    git_sync = _mapping(runtime_profile.get("git_sync"))
    if not git_sync.get("enabled"):
        return ()
    reserved = reserved_git_sync_volume_names()
    blockers: list[GitOpsIssue] = []
    for volume in options.volumes:
        if volume.name in reserved:
            blockers.append(_reserved_name_issue(volume.name, "--volume"))
    for mount in options.volume_mounts:
        if mount.name in reserved:
            blockers.append(_reserved_name_issue(mount.name, "--volume-mount"))
    return tuple(blockers)


def _parse_volumes(raw_values: object) -> tuple[tuple[GitOpsAirflowVolume, ...], tuple[GitOpsIssue, ...]]:
    volumes: list[GitOpsAirflowVolume] = []
    blockers: list[GitOpsIssue] = []
    for text in _text_iter(raw_values):
        if "=" not in text:
            blockers.append(_kv_issue(source="--volume", value=text))
            continue
        name, raw_path = text.split("=", 1)
        name = name.strip()
        try:
            repo_path = safe_relative_path(raw_path.strip(), source="--volume")
        except GitOpsPathValidationError as exc:
            blockers.append(GitOpsIssue(code="invalid_path", message=str(exc), path=raw_path, source="--volume"))
            continue
        if not name:
            blockers.append(_kv_issue(source="--volume", value=text))
            continue
        volumes.append(GitOpsAirflowVolume(name=name, repo_path=repo_path.as_posix()))
    return tuple(volumes), tuple(blockers)


def _parse_volume_mounts(raw_values: object) -> tuple[tuple[GitOpsAirflowVolumeMount, ...], tuple[GitOpsIssue, ...]]:
    mounts: list[GitOpsAirflowVolumeMount] = []
    blockers: list[GitOpsIssue] = []
    for text in _text_iter(raw_values):
        if "=" not in text:
            blockers.append(_kv_issue(source="--volume-mount", value=text))
            continue
        name, raw_mount = text.split("=", 1)
        mount_path, mode = _split_mount_mode(raw_mount)
        if not name.strip() or not _safe_container_path(mount_path):
            blockers.append(_kv_issue(source="--volume-mount", value=text))
            continue
        mounts.append(GitOpsAirflowVolumeMount(name=name.strip(), mount_path=mount_path, read_only=mode != "rw"))
    return tuple(mounts), tuple(blockers)


def _parse_env_secrets(raw_values: object) -> tuple[tuple[GitOpsAirflowEnvSecret, ...], tuple[GitOpsIssue, ...]]:
    secrets: list[GitOpsAirflowEnvSecret] = []
    blockers: list[GitOpsIssue] = []
    for text in _text_iter(raw_values):
        if "=" not in text or ":" not in text:
            blockers.append(_kv_issue(source="--env-secret", value=text))
            continue
        name, raw_secret = text.split("=", 1)
        secret_name, secret_key = raw_secret.split(":", 1)
        if not name.strip() or not secret_name.strip() or not secret_key.strip():
            blockers.append(_kv_issue(source="--env-secret", value=text))
            continue
        secrets.append(
            GitOpsAirflowEnvSecret(name=name.strip(), secret_name=secret_name.strip(), secret_key=secret_key.strip())
        )
    return tuple(secrets), tuple(blockers)


def _parse_tolerations(raw_values: object) -> tuple[tuple[GitOpsAirflowToleration, ...], tuple[GitOpsIssue, ...]]:
    tolerations: list[GitOpsAirflowToleration] = []
    blockers: list[GitOpsIssue] = []
    for text in _text_iter(raw_values):
        if "=" not in text or ":" not in text:
            blockers.append(_kv_issue(source="--toleration", value=text))
            continue
        key, raw_value = text.split("=", 1)
        value, effect = raw_value.split(":", 1)
        if not key.strip() or not value.strip() or not effect.strip():
            blockers.append(_kv_issue(source="--toleration", value=text))
            continue
        tolerations.append(GitOpsAirflowToleration(key=key.strip(), value=value.strip(), effect=effect.strip()))
    return tuple(tolerations), tuple(blockers)


def _split_mount_mode(raw_value: str) -> tuple[str, str]:
    path, separator, mode = raw_value.rpartition(":")
    if separator and mode in {"ro", "rw"}:
        return path.strip(), mode
    return raw_value.strip(), "ro"


def _safe_container_path(path: str) -> bool:
    return path.startswith("/") and all(part not in {"", ".."} for part in path.split("/") if part)


def _parse_key_values(raw_values: object, *, source: str) -> tuple[dict[str, str], tuple[GitOpsIssue, ...]]:
    values: dict[str, str] = {}
    blockers: list[GitOpsIssue] = []
    for text in _text_iter(raw_values):
        if "=" not in text:
            blockers.append(_kv_issue(source=source, value=text))
            continue
        key, value = text.split("=", 1)
        key = key.strip()
        if not key:
            blockers.append(_kv_issue(source=source, value=text))
            continue
        values[key] = value.strip()
    return values, tuple(blockers)


def _text_tuple(raw_values: object) -> tuple[str, ...]:
    return tuple(_text_iter(raw_values))


def _text_iter(value: object) -> tuple[str, ...]:
    if isinstance(value, str) or value is None:
        raw_items: Iterable[object] = (value,) if value else ()
    elif isinstance(value, Iterable):
        raw_items = value
    else:
        raw_items = (value,)
    return tuple(text for item in raw_items if (text := _text(item)))


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _text(value: object) -> str:
    return str(value or "").strip()


def _kv_issue(*, source: str, value: str) -> GitOpsIssue:
    return GitOpsIssue(
        code="pod_contract_key_value_invalid",
        message=f"{source} values must use the documented syntax",
        path=value,
        source="dpone gitops airflow pod-contract",
    )


def _reserved_name_issue(name: str, source: str) -> GitOpsIssue:
    return GitOpsIssue(
        code="git_sync_reserved_name",
        message=f"{source} name {name!r} is reserved for dpone git-sync pod wiring",
        path=name,
        source="dpone gitops airflow pod-contract",
    )


__all__ = [
    "GitOpsAirflowPodContractOptions",
    "git_sync_option_blockers",
    "parse_pod_contract_options",
]
