from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any, Protocol

from dpone.ports.filesystem import FileSystem

CheckFactory = Callable[[str, bool, str, str, str], Any]
IssueFactory = Callable[[str, str, str], Any]


class GitOpsAirflowConnectionBridgePlanPreflightContext(Protocol):
    fs: FileSystem


@dataclass(frozen=True, slots=True)
class GitOpsAirflowConnectionBridgePlanPreflightResult:
    checks: tuple[Any, ...]
    warnings: tuple[Any, ...] = ()
    blockers: tuple[Any, ...] = ()


def evaluate_connection_bridge_plan_preflight(
    *,
    ctx: GitOpsAirflowConnectionBridgePlanPreflightContext,
    repo_root: Path,
    artifact_dir: str,
    runner_policy: str,
    check_factory: CheckFactory,
    issue_factory: IssueFactory,
) -> GitOpsAirflowConnectionBridgePlanPreflightResult:
    try:
        artifact_dir_path = _safe_path(artifact_dir, source="--artifact-dir")
    except _path_validation_error() as exc:
        issue = issue_factory("invalid_path", str(exc), str(artifact_dir))
        return _result_for_issue(
            issue,
            runner_policy=runner_policy,
            check_factory=check_factory,
            path=str(artifact_dir),
        )
    pod_contract = _load_json(ctx=ctx, repo_root=repo_root, path=artifact_dir_path / "pod-contract.json")
    runtime_profile = _load_json(ctx=ctx, repo_root=repo_root, path=artifact_dir_path / "runtime-profile.json")
    bridge = _bridge_from_payloads(pod_contract, runtime_profile)
    required_ids = tuple(str(item) for item in bridge.get("required_connection_ids", ()) if str(item))
    plan_path = artifact_dir_path / "connection-bridge-plan.json"
    plan_label = _label(plan_path)
    if not required_ids or str(bridge.get("runtime_mode") or "") == "airflow_image":
        return GitOpsAirflowConnectionBridgePlanPreflightResult(
            checks=(
                check_factory(
                    "airflow_connection_bridge_plan_not_required",
                    True,
                    "warning",
                    "connection-bridge-plan.json is not required for this artifact directory",
                    plan_label,
                ),
            )
        )
    plan = _load_json(ctx=ctx, repo_root=repo_root, path=plan_path)
    if plan is None:
        issue = issue_factory(
            "airflow_connection_bridge_plan_missing",
            "connection-bridge-plan.json is required when connection_type=airflow ids are bridged",
            plan_label,
        )
        return _result_for_issue(
            issue,
            runner_policy=runner_policy,
            check_factory=check_factory,
            path=plan_label,
        )
    issues = (
        *_shape_issues(
            plan=plan, bridge=bridge, required_ids=required_ids, issue_factory=issue_factory, path=plan_label
        ),
        *_artifact_issues(ctx=ctx, repo_root=repo_root, plan=plan, issue_factory=issue_factory),
    )
    passed = not issues
    severity = "blocker" if runner_policy == "release" else "warning"
    return GitOpsAirflowConnectionBridgePlanPreflightResult(
        checks=(
            check_factory(
                "airflow_connection_bridge_plan",
                passed or runner_policy != "release",
                severity,
                "connection-bridge-plan.json matches the runtime connection bridge contract",
                plan_label,
            ),
        ),
        warnings=() if runner_policy == "release" else issues,
        blockers=issues if runner_policy == "release" else (),
    )


def _result_for_issue(
    issue: Any,
    *,
    runner_policy: str,
    check_factory: CheckFactory,
    path: str,
) -> GitOpsAirflowConnectionBridgePlanPreflightResult:
    severity = "blocker" if runner_policy == "release" else "warning"
    return GitOpsAirflowConnectionBridgePlanPreflightResult(
        checks=(
            check_factory(
                "airflow_connection_bridge_plan",
                runner_policy != "release",
                severity,
                "connection-bridge-plan.json exists when Airflow connection bridge is required",
                path,
            ),
        ),
        warnings=() if runner_policy == "release" else (issue,),
        blockers=(issue,) if runner_policy == "release" else (),
    )


def _shape_issues(
    *,
    plan: Mapping[str, Any],
    bridge: Mapping[str, Any],
    required_ids: tuple[str, ...],
    issue_factory: IssueFactory,
    path: str,
) -> tuple[Any, ...]:
    issues: list[Any] = []
    if plan.get("kind") != "gitops.airflow_connection_bridge_plan":
        issues.append(issue_factory("airflow_connection_bridge_plan_kind_mismatch", "Unexpected plan kind", path))
    if tuple(plan.get("required_connection_ids", ())) != required_ids:
        issues.append(issue_factory("airflow_connection_bridge_plan_stale", "Required connection ids differ", path))
    if _env_signature(plan.get("env")) != _env_signature(bridge.get("env")):
        issues.append(issue_factory("airflow_connection_bridge_plan_stale", "AIRFLOW_CONN_* refs differ", path))
    return tuple(issues)


def _artifact_issues(
    *,
    ctx: GitOpsAirflowConnectionBridgePlanPreflightContext,
    repo_root: Path,
    plan: Mapping[str, Any],
    issue_factory: IssueFactory,
) -> tuple[Any, ...]:
    issues: list[Any] = []
    for artifact in _required_artifacts(plan):
        raw_path = str(artifact.get("path") or "")
        try:
            path = _safe_path(raw_path, source="connection_bridge_plan.artifacts[].path")
        except _path_validation_error() as exc:
            issues.append(issue_factory("airflow_connection_bridge_plan_artifact_path_invalid", str(exc), raw_path))
            continue
        if not ctx.fs.exists(repo_root / path):
            issues.append(
                issue_factory(
                    "airflow_connection_bridge_plan_artifact_missing",
                    "Required connection bridge skeleton artifact is missing",
                    _label(path),
                )
            )
    return tuple(issues)


def _required_artifacts(plan: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    raw = plan.get("artifacts")
    if not isinstance(raw, list):
        return ()
    return tuple(item for item in raw if isinstance(item, Mapping) and item.get("required") is True)


def _bridge_from_payloads(
    pod_contract: Mapping[str, Any] | None,
    runtime_profile: Mapping[str, Any] | None,
) -> Mapping[str, Any]:
    for payload in (pod_contract, runtime_profile):
        bridge = payload.get("connection_bridge") if isinstance(payload, Mapping) else None
        if isinstance(bridge, Mapping):
            return bridge
    return {}


def _load_json(
    *,
    ctx: GitOpsAirflowConnectionBridgePlanPreflightContext,
    repo_root: Path,
    path: Path,
) -> Mapping[str, Any] | None:
    full_path = repo_root / path
    if not ctx.fs.exists(full_path):
        return None
    try:
        payload = json.loads(ctx.fs.read_text(full_path, encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, Mapping) else None


def _env_signature(value: object) -> tuple[tuple[str, str, str, str], ...]:
    if not isinstance(value, list):
        return ()
    signature: list[tuple[str, str, str, str]] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        secret_ref = item.get("secret_ref")
        secret = secret_ref if isinstance(secret_ref, Mapping) else {}
        signature.append(
            (
                str(item.get("connection_id") or ""),
                str(item.get("env_name") or ""),
                str(secret.get("name") or ""),
                str(secret.get("key") or ""),
            )
        )
    return tuple(signature)


def _safe_path(raw_path: object, *, source: str) -> Path:
    return _paths_domain().safe_relative_path(raw_path, source=source)


def _path_validation_error() -> type[Exception]:
    return _paths_domain().GitOpsPathValidationError


def _label(path: Path) -> str:
    return "." if path.as_posix() == "." else path.as_posix()


def _paths_domain() -> Any:
    return import_module("dpone.gitops.paths")


__all__ = [
    "GitOpsAirflowConnectionBridgePlanPreflightResult",
    "evaluate_connection_bridge_plan_preflight",
]
