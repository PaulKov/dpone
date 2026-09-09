from __future__ import annotations

import json
from argparse import Namespace
from collections.abc import Mapping
from importlib import import_module
from pathlib import Path
from typing import Any, Protocol

from dpone.ports.filesystem import FileSystem
from dpone.ports.yaml_codec import YamlCodec
from dpone.services.gitops.airflow_artifact_index_service import build_airflow_artifact_index
from dpone.services.gitops.airflow_pod_doctor_service import GitOpsAirflowPodDoctorService
from dpone.services.gitops.views import GitOpsView, build_gitops_meta


class _GitOpsSettings(Protocol):
    repo_root: Path


class GitOpsAirflowPreflightContext(Protocol):
    settings: _GitOpsSettings
    fs: FileSystem
    yaml: YamlCodec


class GitOpsAirflowPreflightService:
    """Run offline Airflow artifact-dir preflight checks."""

    def __init__(self, *, ctx: GitOpsAirflowPreflightContext) -> None:
        self._ctx = ctx
        self._schema_validator = _schema_validator()

    def build_view(self, args: object) -> GitOpsView:
        runner_policy = _normalize_runner_policy(getattr(args, "runner_policy", "advisory"))
        index_result = build_airflow_artifact_index(ctx=self._ctx, args=_index_args(args))
        artifact_index = index_result.report
        repo_root = self._ctx.settings.repo_root.resolve(strict=False)
        stored_index, stored_warnings, stored_blockers = _load_stored_index(
            ctx=self._ctx,
            repo_root=repo_root,
            artifact_index_path=artifact_index.output_path,
            runner_policy=runner_policy,
        )
        freshness_blockers = _freshness_blockers(current=artifact_index, stored=stored_index)
        schema_blockers = self._schema_blockers(repo_root=repo_root, artifact_index=artifact_index)
        pod_doctor_view = GitOpsAirflowPodDoctorService(ctx=self._ctx).build_view(_pod_doctor_args(args, runner_policy))
        pod_doctor_payload = pod_doctor_view.report.to_jsonable()
        pod_doctor_blockers = tuple(pod_doctor_view.report.blockers)
        pod_doctor_warnings = tuple(pod_doctor_view.report.warnings)
        bridge_plan = _bridge_plan_preflight().evaluate_connection_bridge_plan_preflight(
            ctx=self._ctx,
            repo_root=repo_root,
            artifact_dir=artifact_index.artifact_dir,
            runner_policy=runner_policy,
            check_factory=_check,
            issue_factory=_issue,
        )
        blockers = (
            *artifact_index.blockers,
            *stored_blockers,
            *freshness_blockers,
            *schema_blockers,
            *pod_doctor_blockers,
            *bridge_plan.blockers,
        )
        warnings = (*artifact_index.warnings, *stored_warnings, *pod_doctor_warnings, *bridge_plan.warnings)
        checks = _checks(
            artifact_index_path=artifact_index.output_path,
            runner_policy=runner_policy,
            stored_index=stored_index,
            freshness_blockers=freshness_blockers,
            schema_blockers=schema_blockers,
            pod_doctor_passed=pod_doctor_view.report.passed,
            bridge_plan_checks=bridge_plan.checks,
        )
        report = _preflight_report_type()(
            artifact_dir=artifact_index.artifact_dir,
            artifact_index_path=artifact_index.output_path,
            runner_policy=runner_policy,
            artifact_index=artifact_index,
            pod_doctor=pod_doctor_payload,
            checks=checks,
            next_actions=_next_actions(blockers=blockers, warnings=warnings, artifact_dir=artifact_index.artifact_dir),
            warnings=warnings,
            blockers=blockers,
        )
        return GitOpsView(
            meta=build_gitops_meta(
                "gitops.airflow_preflight",
                path=artifact_index.output_path,
                options={
                    "artifact_dir": report.artifact_dir,
                    "format": getattr(args, "format", "json"),
                    "runner_policy": runner_policy,
                },
            ),
            report=report,
        )

    def _schema_blockers(
        self,
        *,
        repo_root: Path,
        artifact_index: Any,
    ) -> tuple[Any, ...]:
        blockers: list[Any] = []
        for entry in artifact_index.entries:
            if not entry.exists or entry.format != "json" or not entry.expected_kind.startswith("gitops."):
                continue
            path = _safe_relative_path(entry.path, source="artifact-index.entries[].path")
            full_path = repo_root / path
            try:
                payload = json.loads(self._ctx.fs.read_text(full_path, encoding="utf-8"))
            except json.JSONDecodeError as exc:
                blockers.append(
                    _issue("airflow_preflight_json_invalid", f"JSON artifact is invalid: {exc.msg}", entry.path)
                )
                continue
            blockers.extend(
                _with_path(issue, entry.path)
                for issue in self._schema_validator.validate(payload, expected_kind=entry.expected_kind)
            )
        return tuple(blockers)


def _index_args(args: object) -> Namespace:
    return Namespace(
        artifact_dir=getattr(args, "artifact_dir", ".dpone/gitops/airflow"),
        output_path=getattr(args, "artifact_index_path", None),
        format=getattr(args, "format", "json"),
    )


def _pod_doctor_args(args: object, runner_policy: str) -> Namespace:
    return Namespace(
        artifact_dir=getattr(args, "artifact_dir", ".dpone/gitops/airflow"),
        pod_contract_path=None,
        pod_spec_path=None,
        kpo_kwargs_path=None,
        runner_policy=runner_policy,
        format="json",
        output=None,
    )


def _load_stored_index(
    *,
    ctx: GitOpsAirflowPreflightContext,
    repo_root: Path,
    artifact_index_path: str,
    runner_policy: str,
) -> tuple[Mapping[str, Any] | None, tuple[Any, ...], tuple[Any, ...]]:
    try:
        rel_path = _safe_relative_path(artifact_index_path, source="--artifact-index-path")
    except _path_validation_error() as exc:
        issue = _issue("invalid_path", str(exc), artifact_index_path)
        return None, (), (issue,)
    full_path = repo_root / rel_path
    if not ctx.fs.exists(full_path):
        issue = _issue(
            "airflow_artifact_index_missing",
            "artifact-index.json is missing; run dpone gitops airflow artifact-index",
            artifact_index_path,
        )
        return None, () if runner_policy == "release" else (issue,), (issue,) if runner_policy == "release" else ()
    try:
        payload = json.loads(ctx.fs.read_text(full_path, encoding="utf-8"))
    except json.JSONDecodeError as exc:
        issue = _issue(
            "airflow_artifact_index_json_invalid",
            f"artifact-index.json is invalid: {exc.msg}",
            artifact_index_path,
        )
        return None, (), (issue,)
    if not isinstance(payload, Mapping) or payload.get("kind") != "gitops.airflow_artifact_index":
        issue = _issue(
            "airflow_artifact_index_kind_mismatch",
            "artifact-index.json must have kind gitops.airflow_artifact_index",
            artifact_index_path,
        )
        return None, (), (issue,)
    return payload, (), ()


def _freshness_blockers(*, current: Any, stored: Mapping[str, Any] | None) -> tuple[Any, ...]:
    if stored is None:
        return ()
    stored_entries = {
        str(entry.get("name")): entry
        for entry in stored.get("entries", [])
        if isinstance(entry, Mapping) and entry.get("name")
    }
    blockers: list[Any] = []
    for entry in current.entries:
        if not entry.exists:
            continue
        stored_entry = stored_entries.get(entry.name)
        if stored_entry is None:
            blockers.append(
                _issue("airflow_artifact_index_entry_missing", "artifact-index.json is missing an entry", entry.path)
            )
            continue
        if stored_entry.get("sha256") != entry.sha256:
            blockers.append(
                _issue(
                    "airflow_artifact_index_digest_mismatch",
                    "artifact-index.json sha256 does not match the current artifact",
                    entry.path,
                )
            )
    return tuple(blockers)


def _checks(
    *,
    artifact_index_path: str,
    runner_policy: str,
    stored_index: Mapping[str, Any] | None,
    freshness_blockers: tuple[Any, ...],
    schema_blockers: tuple[Any, ...],
    pod_doctor_passed: bool,
    bridge_plan_checks: tuple[Any, ...],
) -> tuple[Any, ...]:
    index_required = runner_policy == "release"
    index_present = stored_index is not None
    return (
        _check(
            "airflow_artifact_index_present",
            index_present or not index_required,
            "blocker" if index_required else "warning",
            "artifact-index.json is present for release freshness checks",
            artifact_index_path,
        ),
        _check(
            "airflow_artifact_index_fresh",
            not freshness_blockers,
            "blocker",
            "artifact-index.json matches current Airflow artifacts",
            artifact_index_path,
        ),
        _check(
            "airflow_artifact_schema_contracts",
            not schema_blockers,
            "blocker",
            "GitOps JSON artifacts satisfy registered schema contracts",
            artifact_index_path,
        ),
        _check(
            "airflow_pod_doctor",
            pod_doctor_passed,
            "blocker",
            "pod-doctor accepts pod contract, PodSpec, KPO kwargs, git-sync, and connection bridge",
            artifact_index_path,
        ),
        *bridge_plan_checks,
    )


def _next_actions(*, blockers: tuple[Any, ...], warnings: tuple[Any, ...], artifact_dir: str) -> tuple[str, ...]:
    codes = {issue.code for issue in (*blockers, *warnings)}
    actions: list[str] = []
    if "airflow_artifact_index_missing" in codes:
        actions.append(f"Run dpone gitops airflow artifact-index --artifact-dir {artifact_dir}")
    if "airflow_artifact_index_digest_mismatch" in codes or "airflow_artifact_index_entry_missing" in codes:
        actions.append("Regenerate artifact-index.json after changing Airflow artifacts")
    if any(code.startswith("schema_") or code == "airflow_preflight_json_invalid" for code in codes):
        actions.append("Regenerate the Airflow artifact pack and rerun schema validation")
    if any(code.startswith("pod_contract_") for code in codes):
        actions.append("Regenerate pod-contract, pod-spec.yaml, and kpo-kwargs.json")
    if any(code.startswith("airflow_connection_bridge_plan") for code in codes):
        actions.append(f"Run dpone gitops airflow connection-bridge-plan --artifact-dir {artifact_dir}")
    if not actions:
        actions.append("Artifact directory is ready for Airflow deployment")
    return tuple(dict.fromkeys(actions))


def _check(name: str, passed: bool, severity: str, message: str, path: str) -> Any:
    return _airflow_models().GitOpsAirflowCheck(
        name=name,
        passed=passed,
        severity=severity,
        message=message,
        path=path,
        source=_preflight_source(),
    )


def _issue(code: str, message: str, path: str) -> Any:
    return _models_domain().GitOpsIssue(code=code, message=message, path=path, source=_preflight_source())


def _with_path(issue: Any, path: str) -> Any:
    return _models_domain().GitOpsIssue(code=issue.code, message=issue.message, path=path, source=issue.source)


def _safe_relative_path(raw_path: object, *, source: str) -> Path:
    return _paths_domain().safe_relative_path(raw_path, source=source)


def _path_validation_error() -> type[Exception]:
    return _paths_domain().GitOpsPathValidationError


def _normalize_runner_policy(raw_policy: object) -> str:
    return _airflow_policy().normalize_airflow_runner_policy(raw_policy)


def _schema_validator() -> Any:
    return _schema_validation().GitOpsSchemaValidator()


def _preflight_report_type() -> Any:
    return _preflight_models().GitOpsAirflowPreflightReport


def _preflight_source() -> str:
    return _preflight_models().AIRFLOW_PREFLIGHT_SOURCE


def _airflow_models() -> Any:
    return import_module("dpone.gitops.airflow_models")


def _airflow_policy() -> Any:
    return import_module("dpone.gitops.airflow_policy")


def _models_domain() -> Any:
    return import_module("dpone.gitops.models")


def _paths_domain() -> Any:
    return import_module("dpone.gitops.paths")


def _preflight_models() -> Any:
    return import_module("dpone.gitops.airflow_preflight_models")


def _schema_validation() -> Any:
    return import_module("dpone.gitops.schema_validation")


def _bridge_plan_preflight() -> Any:
    return import_module("dpone.services.gitops.airflow_connection_bridge_plan_preflight")


__all__ = ["GitOpsAirflowPreflightContext", "GitOpsAirflowPreflightService"]
