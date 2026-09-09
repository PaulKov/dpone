"""Thin authoring CLI over one complete-workspace check/publication service."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from contextlib import redirect_stdout
from pathlib import Path

from dpone.app.dbt_workspace_composition import build_dbt_workspace_service
from dpone.contracts.dbt_workspace import (
    DbtWorkspaceCheckReport,
    DbtWorkspaceCompileReport,
    DbtWorkspaceDiscoveryReport,
)

from .dbt_publish_cli_support import emit_internal_failure
from .func_command import FuncCommand


def workspace_authoring_commands() -> list[FuncCommand]:
    return [
        FuncCommand("discover", _discover_parser, _run, _requires_app_context=False),
        FuncCommand("check", _check_parser, _run, _requires_app_context=False),
        FuncCommand("compile", _compile_parser, _run, _requires_app_context=False),
    ]


def _discover_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _parser(subparsers, "discover", "List all dbt projects and local publishing policy; no dbt execution")


def _check_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _parser(
        subparsers, "check", "Check every configured publishing project and global identities without writing artifacts"
    )


def _compile_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = _parser(
        subparsers, "compile", "Compile all publishing projects into one verified immutable release; no SQL execution"
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Immutable release directory; identical retry is a no-op, different content is a conflict",
    )
    parser.add_argument(
        "--dbt-profiles-dir",
        type=_profile_directory,
        help="Shared parse-only profiles.yml directory (relative to current working directory); default: each project root",
    )
    return parser


def _profile_directory(value: str) -> Path:
    if not value.strip() or "\x00" in value:
        raise argparse.ArgumentTypeError("dbt profiles directory must be nonblank")
    return Path(value).absolute()


def _parser(subparsers: argparse._SubParsersAction, name: str, help_text: str) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(name, help=help_text, description=help_text)
    parser.add_argument("--root", default=".", help="Complete workspace directory (default: current directory)")
    parser.add_argument("--format", choices=["text", "json"], default="text")
    parser.set_defaults(workspace_operation=name)
    return parser


def _run(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    try:
        profiles_dir = getattr(args, "dbt_profiles_dir", None)
        service = (
            build_dbt_workspace_service(dbt_profiles_dir=profiles_dir)
            if profiles_dir is not None
            else build_dbt_workspace_service()
        )
        report: DbtWorkspaceDiscoveryReport | DbtWorkspaceCheckReport | DbtWorkspaceCompileReport
        if args.workspace_operation == "discover":
            report = service.discover(Path(args.root))
        elif args.workspace_operation == "compile":
            report = service.compile(Path(args.root), output_dir=Path(args.output_dir))
        else:
            report = service.check(Path(args.root))
    except Exception:
        with redirect_stdout(sys.stdout if args.format == "json" else sys.stderr):
            emit_internal_failure(args.format, stage="dbt_workspace_authoring")
        return 5
    if args.format == "json":
        print(json.dumps(report.to_dict(), allow_nan=False, ensure_ascii=False, indent=2))
    else:
        _text(report)
    if isinstance(report, DbtWorkspaceCompileReport):
        return report.exit_code
    return 0 if report.passed else 2


def _text(report: DbtWorkspaceDiscoveryReport | DbtWorkspaceCheckReport | DbtWorkspaceCompileReport) -> None:
    compile_report = report if isinstance(report, DbtWorkspaceCompileReport) else None
    authoring = report.check if isinstance(report, DbtWorkspaceCompileReport) else report
    discovery = authoring if isinstance(authoring, DbtWorkspaceDiscoveryReport) else authoring.discovery
    passed = compile_report.passed if compile_report is not None else report.passed
    print(f"dbt workspace: {'PASS' if passed else 'FAIL'}")
    for project in discovery.projects:
        print(f"- {project.project_path}: {project.reason}")
    issues = list(discovery.blockers)
    if isinstance(authoring, DbtWorkspaceCheckReport):
        issues.extend(authoring.blockers)
        for checked in authoring.projects:
            print(
                f"- {'PASS' if checked.report.passed else 'FAIL'} {checked.project.project_path}: {len(checked.report.models)} publish models"
            )
            issues.extend((*checked.report.blockers, *checked.report.warnings))
    if compile_report is not None:
        issues.extend(compile_report.blockers)
        print(f"Output: {compile_report.output_dir}")
        if compile_report.passed:
            print(f"Release: {compile_report.release_id}")
            print(f"Integrity subject: {compile_report.subject_sha256}")
    for issue in issues:
        print(f"{issue.severity.upper()} {issue.code}: {issue.message} [{issue.path}]", file=sys.stderr)
        if issue.remediation:
            print(f"  Next: {issue.remediation}", file=sys.stderr)
