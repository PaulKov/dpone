from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.gitops.airflow_git_sync_capabilities import (
    evaluate_git_sync_filter_request,
    should_render_git_sync_filter,
)
from dpone.gitops.airflow_git_sync_names import (
    GIT_SYNC_INIT_CONTAINER_NAME,
    SPARSE_INIT_CONTAINER_NAME,
    WORKTREE_VOLUME_NAME,
)
from dpone.gitops.airflow_models import GitOpsAirflowCheck
from dpone.gitops.models import GitOpsIssue

SOURCE = "dpone gitops airflow pod-doctor"


def git_sync_pod_doctor_checks(
    *,
    contract: Mapping[str, Any],
    pod_spec_path: str,
    pod_spec: Mapping[str, Any],
    runner_policy: str,
) -> tuple[tuple[GitOpsAirflowCheck, ...], tuple[GitOpsIssue, ...], tuple[GitOpsIssue, ...]]:
    git_sync = _mapping(contract.get("git_sync"))
    if not git_sync.get("enabled"):
        return (), (), ()

    checks: list[GitOpsAirflowCheck] = []
    warnings: list[GitOpsIssue] = []
    blockers: list[GitOpsIssue] = []
    _append_init_checks(checks, blockers, pod_spec_path=pod_spec_path, pod_spec=pod_spec)
    _append_worktree_checks(checks, blockers, git_sync=git_sync, pod_spec_path=pod_spec_path, pod_spec=pod_spec)
    _append_arg_checks(
        checks,
        warnings,
        blockers,
        git_sync=git_sync,
        pod_spec_path=pod_spec_path,
        pod_spec=pod_spec,
        runner_policy=runner_policy,
    )
    return tuple(checks), tuple(warnings), tuple(blockers)


def _append_init_checks(
    checks: list[GitOpsAirflowCheck],
    blockers: list[GitOpsIssue],
    *,
    pod_spec_path: str,
    pod_spec: Mapping[str, Any],
) -> None:
    expected_names = [SPARSE_INIT_CONTAINER_NAME, GIT_SYNC_INIT_CONTAINER_NAME]
    init_names = [_text(item.get("name")) for item in _init_containers(pod_spec)[:2]]
    passed = init_names == expected_names
    checks.append(_check("pod_contract_git_sync_init_containers", passed, "blocker", pod_spec_path))
    if not passed:
        blockers.append(
            _issue(
                "pod_contract_git_sync_init_containers_required",
                "Pod contract requires dpone-sparse-checkout and dpone-git-sync initContainers",
                pod_spec_path,
            )
        )


def _append_worktree_checks(
    checks: list[GitOpsAirflowCheck],
    blockers: list[GitOpsIssue],
    *,
    git_sync: Mapping[str, Any],
    pod_spec_path: str,
    pod_spec: Mapping[str, Any],
) -> None:
    working_dir = _text(_first_container(pod_spec).get("workingDir"))
    expected_worktree = _text(git_sync.get("worktree_path"))
    working_dir_passed = bool(expected_worktree) and working_dir == expected_worktree
    checks.append(_check("pod_contract_git_sync_working_dir", working_dir_passed, "blocker", pod_spec_path))
    if not working_dir_passed:
        blockers.append(
            _issue(
                "pod_contract_git_sync_working_dir_mismatch",
                "Base container must use git_sync.worktree_path",
                pod_spec_path,
            )
        )

    mount_passed = _has_worktree_mount(_first_container(pod_spec))
    volume_passed = _has_worktree_volume(pod_spec)
    checks.append(
        _check("pod_contract_git_sync_worktree_mount", mount_passed and volume_passed, "blocker", pod_spec_path)
    )
    if not mount_passed or not volume_passed:
        blockers.append(
            _issue(
                "pod_contract_git_sync_worktree_mount_required",
                "Pod contract requires dpone-worktree volume and base container mount",
                pod_spec_path,
            )
        )


def _append_arg_checks(
    checks: list[GitOpsAirflowCheck],
    warnings: list[GitOpsIssue],
    blockers: list[GitOpsIssue],
    *,
    git_sync: Mapping[str, Any],
    pod_spec_path: str,
    pod_spec: Mapping[str, Any],
    runner_policy: str,
) -> None:
    args = _git_sync_args(pod_spec)
    sparse_file = _text(git_sync.get("sparse_checkout_file"))
    _append_required_arg_check(
        checks,
        blockers,
        name="pod_contract_git_sync_sparse_checkout",
        code="pod_contract_git_sync_sparse_checkout_required",
        expected=f"--sparse-checkout-file={sparse_file}",
        args=args,
        path=pod_spec_path,
    )

    clone = _mapping(git_sync.get("clone"))
    depth = _text(clone.get("depth")) or "1"
    _append_required_arg_check(
        checks,
        blockers,
        name="pod_contract_git_sync_depth",
        code="pod_contract_git_sync_depth_required",
        expected=f"--depth={depth}",
        args=args,
        path=pod_spec_path,
    )
    _append_filter_checks(
        checks, warnings, blockers, git_sync=git_sync, args=args, path=pod_spec_path, runner_policy=runner_policy
    )


def _append_filter_checks(
    checks: list[GitOpsAirflowCheck],
    warnings: list[GitOpsIssue],
    blockers: list[GitOpsIssue],
    *,
    git_sync: Mapping[str, Any],
    args: tuple[str, ...],
    path: str,
    runner_policy: str,
) -> None:
    clone = _mapping(git_sync.get("clone"))
    filter_value = _text(clone.get("filter"))
    if not filter_value:
        checks.append(_check("pod_contract_git_sync_partial_clone", True, "warning", path))
        return

    policy_warnings, policy_blockers = evaluate_git_sync_filter_request(
        image=_text(git_sync.get("image")),
        filter_value=filter_value,
        runner_policy=runner_policy,
        path="git_sync.clone.filter",
        source=SOURCE,
    )
    warnings.extend(policy_warnings)
    blockers.extend(policy_blockers)
    expected = f"--filter={filter_value}"
    passed = expected in args and should_render_git_sync_filter(
        image=_text(git_sync.get("image")), filter_value=filter_value
    )
    checks.append(_check("pod_contract_git_sync_partial_clone", passed, "blocker", path))
    if not passed:
        blockers.append(
            _issue(
                "pod_contract_git_sync_filter_missing",
                "Pod contract requires the requested git-sync --filter arg",
                path,
            )
        )


def _append_required_arg_check(
    checks: list[GitOpsAirflowCheck],
    blockers: list[GitOpsIssue],
    *,
    name: str,
    code: str,
    expected: str,
    args: tuple[str, ...],
    path: str,
) -> None:
    passed = expected in args
    checks.append(_check(name, passed, "blocker", path))
    if not passed:
        blockers.append(_issue(code, f"Pod contract requires git-sync arg {expected}", path))


def _git_sync_args(payload: Mapping[str, Any]) -> tuple[str, ...]:
    container = _named_init_container(payload, GIT_SYNC_INIT_CONTAINER_NAME)
    args = container.get("args")
    if not isinstance(args, list):
        return ()
    return tuple(str(item) for item in args)


def _named_init_container(payload: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    for item in _init_containers(payload):
        if item.get("name") == name:
            return item
    return {}


def _first_container(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    spec = _mapping(payload.get("spec"))
    containers = spec.get("containers")
    if isinstance(containers, list) and containers and isinstance(containers[0], Mapping):
        return containers[0]
    return {}


def _init_containers(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    spec = _mapping(payload.get("spec"))
    containers = spec.get("initContainers")
    if not isinstance(containers, list):
        return ()
    return tuple(item for item in containers if isinstance(item, Mapping))


def _has_worktree_volume(payload: Mapping[str, Any]) -> bool:
    spec = _mapping(payload.get("spec"))
    volumes = spec.get("volumes")
    if not isinstance(volumes, list):
        return False
    return any(isinstance(item, Mapping) and item.get("name") == WORKTREE_VOLUME_NAME for item in volumes)


def _has_worktree_mount(container: Mapping[str, Any]) -> bool:
    mounts = container.get("volumeMounts")
    if not isinstance(mounts, list):
        return False
    return any(
        isinstance(item, Mapping) and item.get("name") == WORKTREE_VOLUME_NAME and item.get("mountPath") == "/workspace"
        for item in mounts
    )


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _text(value: object) -> str:
    return str(value or "").strip()


def _check(name: str, passed: bool, severity: str, path: str) -> GitOpsAirflowCheck:
    messages = {
        "pod_contract_git_sync_init_containers": "Pod spec includes sparse checkout and git-sync initContainers",
        "pod_contract_git_sync_working_dir": "Base container workingDir points at the git-sync worktree",
        "pod_contract_git_sync_worktree_mount": "Base container mounts the git-sync worktree volume",
        "pod_contract_git_sync_sparse_checkout": "git-sync uses the generated sparse checkout file",
        "pod_contract_git_sync_depth": "git-sync uses the configured shallow clone depth",
        "pod_contract_git_sync_partial_clone": "git-sync partial clone filter matches the contract",
    }
    return GitOpsAirflowCheck(
        name=name, passed=passed, severity=severity, message=messages[name], path=path, source=SOURCE
    )


def _issue(code: str, message: str, path: str) -> GitOpsIssue:
    return GitOpsIssue(code=code, message=message, path=path, source=SOURCE)


__all__ = ["git_sync_pod_doctor_checks"]
