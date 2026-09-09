"""Pure rendering and project-path helpers for the dbt CLI adapter."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
from typing import Any
from uuid import uuid4

from dpone.commands.dbt_publish_project_inputs import (
    DbtProjectRootError,
)
from dpone.commands.dbt_publish_project_inputs import (
    bundle_inputs as _bundle_inputs,
)
from dpone.contracts.dbt_publish_models import DbtCompileReport, DbtPublishIssue


class DbtCliUsageError(ValueError):
    """Safe public CLI-argument error."""

    code = "DPONE_DBT_PROJECT_ARGUMENT_CONFLICT"


def emit_report(report: DbtCompileReport, output_format: str) -> None:
    if output_format == "json":
        print(json.dumps(report.to_jsonable(), allow_nan=False, ensure_ascii=False, indent=2))
        return
    icon = "PASS" if report.passed else "BLOCKED"
    print(f"dbt -> dpone publish: {icon}")
    print(f"manifest schema: v{report.manifest_schema_version or 'unknown'}")
    print(f"published models: {len(report.models)}; workflows: {len(report.workflows)}")
    if report.models:
        print("models:")
        for item in report.models:
            print(f"- {item.model.unique_id}")
    for workflow in report.workflows:
        print(f"- {workflow.dag_id}: {len(workflow.models)} model(s)")
    for issue in (*report.blockers, *report.warnings):
        print(f"- {issue.severity.upper()} {public_error_code(issue.code)}: {issue.message} [{issue.path}]")
    if report.artifacts:
        print("artifacts:")
        for name, path in sorted(report.artifacts.items()):
            print(f"- {name}: {path}")


def emit_failure(
    issues: tuple[DbtPublishIssue, ...],
    output_format: str,
    *,
    stage: str,
    trace_id: str | None = None,
) -> None:
    if output_format != "json":
        print("dbt -> dpone publish: BLOCKED")
        for issue in issues:
            print(f"- ERROR {public_error_code(issue.code)}: {issue.message} [{issue.path}]")
            if issue.remediation:
                print(f"  Next: {issue.remediation}")
        return
    primary = issues[0] if issues else _internal_issue()
    payload: dict[str, Any] = {
        "schema": "dpone.error.v1",
        "code": public_error_code(primary.code),
        "stage": stage,
        "severity": "error",
        "message": primary.message,
        "fixes": _issue_fixes(primary),
    }
    if trace_id is not None:
        payload["trace_id"] = trace_id
    if primary.path:
        payload["path"] = primary.path
    if len(issues) > 1:
        payload["issues"] = [
            {
                "code": public_error_code(issue.code),
                "message": issue.message,
                "path": issue.path,
                "fixes": _issue_fixes(issue),
            }
            for issue in issues
        ]
    print(json.dumps(payload, allow_nan=False, ensure_ascii=False, indent=2))


def emit_internal_failure(output_format: str, *, stage: str) -> None:
    emit_failure(
        (_internal_issue(),),
        output_format,
        stage=stage,
        trace_id=uuid4().hex,
    )


def emit_model(model: dict[str, Any]) -> None:
    semantic = model.get("semantic_refresh")
    if isinstance(semantic, Mapping):
        print(f"Asset: {model['model']}")
        print(f"Model: {semantic['model_archetype']}")
        print(f"Workflow mode: {semantic['workflow_mode']}")
        print(f"Scope: {semantic['scope']}")
        print(
            "Static policy: dependency closure "
            f"{semantic['static_dependency_closure']}; adapter lifecycle "
            f"{semantic['adapter_lifecycle_policy']}"
        )
        print(
            "Runtime proof: dependency closure "
            f"{semantic['runtime_dependency_closure']}; adapter lifecycle "
            f"{semantic['runtime_adapter_lifecycle']}"
        )
        print(f"Toolchain: dbt Core {semantic['dbt_core']}; dbt-sqlserver {semantic['dbt_sqlserver']}")
        print(
            f"Assurance: writer {semantic['writer_assurance']}; source-side pruning {semantic['source_side_pruning']}"
        )
        print(
            f"Key policy: event time {semantic['event_time_policy']}; effective key {semantic['effective_key_policy']}"
        )
        print(f"UTC assurance: {semantic['utc_assurance']}")
        print(f"Publication: {semantic['publication']}; recovery {semantic['recovery']}")
        print(
            f"Recovery: replay is {str(semantic['replay_mutation']).replace('_', '-')}; "
            f"row removal {semantic['row_removal']}"
        )
        print(f"Profile: {semantic['profile_sha256']}")
        return
    print(f"model: {model['model']}")
    print(f"source: {model['source_relation']}")
    print(f"workload: {model['workload_id']}")
    print(f"strategy: {model['resolved_strategy']}")
    print(f"physical design: {model['resolved_physical_design']}")


def relative_pack_path(raw: str) -> str:
    path = PurePosixPath(raw)
    if (
        not raw
        or "\\" in raw
        or path.is_absolute()
        or not path.parts
        or path.as_posix() != raw
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise argparse.ArgumentTypeError("RELATIVE_PACK_PATH must be a normalized relative POSIX path")
    return raw


def manifest_path(args: argparse.Namespace) -> Path:
    explicit = getattr(args, "manifest", None)
    return Path(explicit) if explicit else project_root(args) / "target" / "manifest.json"


def stale_default_manifest(
    args: argparse.Namespace,
    candidate: Path,
) -> DbtCompileReport | None:
    if getattr(args, "manifest", None) or not candidate.is_file():
        return None
    manifest_mtime = candidate.stat().st_mtime_ns
    root = project_root(args)
    inspected = 0
    try:
        inputs = _bundle_inputs(root)
        for path in inputs:
            inspected += 1
            if inspected > 20_000:
                return _stale_report(
                    candidate,
                    "dbt source discovery exceeded the 20,000-path limit",
                )
            if path.is_symlink():
                return _stale_report(
                    candidate,
                    "dbt source discovery found a symbolic link",
                )
            if path.is_file() and path.stat().st_mtime_ns > manifest_mtime:
                return _stale_report(
                    candidate,
                    "dbt project inputs changed after target/manifest.json",
                )
    except OSError:
        return _stale_report(
            candidate,
            "dbt project inputs could not be inspected safely",
        )
    return None


def project_root(args: argparse.Namespace) -> Path:
    positional = _project_argument(args)
    configured = str(getattr(args, "project_dir", "") or "").strip()
    if positional and configured:
        positional_path = Path(positional).absolute()
        configured_path = Path(configured).absolute()
        if positional_path != configured_path:
            raise DbtCliUsageError("PROJECT and --project-dir must resolve to the same directory")
    if configured or positional:
        selected = Path(configured or positional).absolute()
        return _validated_project_root(selected)
    explicit_manifest = str(getattr(args, "manifest", "") or "").strip()
    if explicit_manifest:
        current = Path(explicit_manifest).absolute().parent
        for candidate in (current, *current.parents):
            project_file = candidate / "dbt_project.yml"
            if project_file.is_file() and not project_file.is_symlink():
                return candidate
        return Path(".").absolute()
    return _validated_project_root(Path(".").absolute())


def project_argument_issue() -> DbtPublishIssue:
    return DbtPublishIssue(
        code="DPONE_DBT_PROJECT_ARGUMENT_CONFLICT",
        message="PROJECT and --project-dir select different dbt project roots",
        path="command",
        remediation=("Use PROJECT or --project-dir once, or make both resolve to the same directory."),
    )


def project_root_issue() -> DbtPublishIssue:
    return DbtPublishIssue(
        code="DPONE_DBT_PROJECT_INVALID",
        message="The selected dbt project root is missing, unsafe, or does not contain a regular dbt_project.yml",
        path="command",
        remediation=("Change to a dbt project or pass its directory explicitly, then run `dbt parse` and retry."),
    )


def explain_selector(args: argparse.Namespace) -> str | None:
    project_model = str(getattr(args, "project_model", "") or "").strip()
    positional = project_model or str(getattr(args, "model", "") or "").strip()
    legacy = str(getattr(args, "legacy_model", "") or "").strip()
    if positional and legacy and positional != legacy:
        return None
    return positional or legacy or None


def _project_argument(args: argparse.Namespace) -> str:
    if str(getattr(args, "project_model", "") or "").strip():
        return str(getattr(args, "model", "") or "").strip()
    return str(getattr(args, "project", "") or "").strip()


def _validated_project_root(selected: Path) -> Path:
    project_file = selected / "dbt_project.yml"
    if selected.is_symlink() or not selected.is_dir() or project_file.is_symlink() or not project_file.is_file():
        raise DbtProjectRootError("selected dbt project root is missing or invalid")
    return selected


def public_error_code(code: str) -> str:
    normalized = "".join(character if character.isalnum() else "_" for character in code.upper()).strip("_")
    if normalized == "COMMIT_UNKNOWN":
        return normalized
    if normalized.startswith("DPONE_DBT_"):
        return normalized
    if normalized.startswith("DBT_PUBLISH_"):
        normalized = normalized.removeprefix("DBT_PUBLISH_")
    elif normalized.startswith("DBT_"):
        normalized = normalized.removeprefix("DBT_")
    return f"DPONE_DBT_{normalized or 'INTERNAL'}"


def _stale_report(path: Path, message: str) -> DbtCompileReport:
    return DbtCompileReport(
        manifest_path=path.as_posix(),
        manifest_schema_version=None,
        blockers=(
            DbtPublishIssue(
                code="DPONE_DBT_MANIFEST_STALE",
                message=message,
                path=path.as_posix(),
                remediation="Run `dbt parse`, then retry.",
            ),
        ),
    )


def _issue_fixes(issue: DbtPublishIssue) -> list[dict[str, str]]:
    if not issue.remediation:
        return []
    suffix = public_error_code(issue.code).removeprefix("DPONE_DBT_").lower()
    return [
        {
            "id": f"resolve_{suffix}",
            "safety": "manual",
            "description": issue.remediation,
        }
    ]


def _internal_issue() -> DbtPublishIssue:
    return DbtPublishIssue(
        code="DPONE_DBT_INTERNAL",
        message="The dbt publishing command failed unexpectedly",
        path="manifest.json",
        remediation=("Retry once. If the error persists, provide the redacted trace ID to the dpone platform owner."),
    )


__all__ = [
    "DbtCliUsageError",
    "DbtProjectRootError",
    "emit_failure",
    "emit_internal_failure",
    "emit_model",
    "emit_report",
    "explain_selector",
    "manifest_path",
    "project_argument_issue",
    "project_root",
    "project_root_issue",
    "public_error_code",
    "relative_pack_path",
    "stale_default_manifest",
]
