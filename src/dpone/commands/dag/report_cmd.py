from __future__ import annotations

import argparse
import logging
import sys

from dpone.cli_render.dag.report import emit_report
from dpone.dag.dag_report import build_dag_report
from dpone.manifest.validation import get_profile
from dpone.services.dag.load_context import load_dag_context
from dpone.services.dag.views import build_dag_report_view


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser(
        "report",
        help="Generate a unified DAG report (edges + reasons + anomalies + lint)",
    )
    p.add_argument(
        "root",
        help=("Root manifest (same entrypoint you pass to DAG builder). If no extension, .yaml is assumed."),
    )
    p.add_argument(
        "--base-path",
        help="Manifest directory (defaults to MANIFEST_DIR). Used for resolving relative root and refs.",
    )
    p.add_argument(
        "--preset",
        choices=["default", "ci", "debug", "debug-lite"],
        default="default",
        help=(
            "Report preset. 'ci' uses safe defaults for CI artifacts (e.g., max-edges=500, no evidence). "
            "'debug' includes evidence and disables truncation by default (max-edges=0, md-max-edges=0). "
            "'debug-lite' includes evidence but keeps safe truncation defaults (max-edges=500, md-max-edges=200). "
            "Explicit CLI args override preset values."
        ),
    )
    p.add_argument(
        "--format",
        choices=["md", "json", "both"],
        default="md",
        help="Output format. 'both' emits Markdown + embedded JSON (or writes to --out-* files).",
    )
    p.add_argument("--out-json", help="Write JSON report to this file (optional).")
    p.add_argument("--out-md", help="Write Markdown report to this file (optional).")
    p.add_argument(
        "--max-edges",
        type=int,
        default=None,
        help="Safety limit: max edges to include in the report (defaults: preset/2000).",
    )
    p.add_argument(
        "--md-max-edges",
        type=int,
        default=None,
        help="Max edges to print in Markdown output (defaults: preset/200).",
    )

    ev_group = p.add_mutually_exclusive_group()
    ev_group.set_defaults(include_evidence=None)
    ev_group.add_argument(
        "--include-evidence",
        dest="include_evidence",
        action="store_true",
        help="Include full reasons evidence for edges (report can be large).",
    )
    ev_group.add_argument(
        "--no-evidence",
        dest="include_evidence",
        action="store_false",
        help="Do not include full reasons evidence for edges (overrides preset).",
    )

    p.add_argument(
        "--max-triggers",
        type=int,
        default=None,
        help="Max trigger tasks to include for group-to-group reasons evidence (defaults: preset/10).",
    )
    p.add_argument(
        "--registry-require-field",
        action="append",
        default=None,
        help=(
            "Registry required field for dag report lint (can be repeated). "
            "Default: host,type. Use 'none' to disable required-field checks."
        ),
    )
    p.add_argument(
        "--profile",
        help="Validation profile name for manifest validation (overrides per-manifest validation blocks).",
    )
    p.add_argument(
        "--no-lint",
        action="store_true",
        help="Skip lint section (manifest validation / registry lint / dependency checks).",
    )
    p.add_argument(
        "--fail-on",
        choices=["none", "lint_errors", "anomalies", "both"],
        default="none",
        help=(
            "Exit with code 1 if the generated report contains issues at or above --fail-severity. "
            "lint_errors: any lint issue; "
            "anomalies: any anomaly; "
            "both: either lint_errors or anomalies; "
            "none: always exit 0 (default)."
        ),
    )
    p.add_argument(
        "--fail-severity",
        type=lambda s: str(s).strip().upper(),
        choices=["ERROR", "WARNING"],
        default="ERROR",
        help=(
            "Minimum severity threshold for --fail-on. "
            "ERROR (default) fails only on ERROR; "
            "WARNING fails on WARNING or ERROR."
        ),
    )
    p.add_argument(
        "--registry",
        action="append",
        default=[],
        help=(
            "Path to a sources registry YAML (can be repeated). "
            "Registry provides default vars like host/type for naming/metadata conventions."
        ),
    )
    return p


def cmd_dag_report(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    dag = load_dag_context(args, ctx=ctx)

    dep_file_errors = dag.dep_manager.validate_dependencies()

    profile = None
    if getattr(args, "profile", None):
        profile = get_profile(str(args.profile))

    preset = str(getattr(args, "preset", "default") or "default").strip().lower()
    if preset not in ("default", "ci", "debug", "debug-lite"):
        preset = "default"

    preset_defaults = {
        "default": {"max_edges": 2000, "md_max_edges": 200, "max_triggers": 10, "include_evidence": False},
        "ci": {"max_edges": 500, "md_max_edges": 200, "max_triggers": 10, "include_evidence": False},
        "debug-lite": {"max_edges": 500, "md_max_edges": 200, "max_triggers": 20, "include_evidence": True},
        "debug": {"max_edges": 0, "md_max_edges": 0, "max_triggers": 50, "include_evidence": True},
    }
    pd = preset_defaults[preset]

    max_edges = int(args.max_edges) if getattr(args, "max_edges", None) is not None else int(pd["max_edges"])
    md_max_edges = (
        int(args.md_max_edges) if getattr(args, "md_max_edges", None) is not None else int(pd["md_max_edges"])
    )
    max_triggers = (
        int(args.max_triggers) if getattr(args, "max_triggers", None) is not None else int(pd["max_triggers"])
    )

    include_evidence = (
        bool(args.include_evidence)
        if getattr(args, "include_evidence", None) is not None
        else bool(pd["include_evidence"])
    )

    reg_require_fields: tuple[str, ...] = ("host", "type")
    raw_req = getattr(args, "registry_require_field", None)
    if raw_req is not None:
        fields: list[str] = []
        for item in raw_req:
            if item is None:
                continue
            s = str(item).strip()
            if not s:
                continue
            if s.lower() == "none":
                fields = []
                break
            for part in s.split(","):
                p = part.strip()
                if p:
                    fields.append(p)
        reg_require_fields = tuple(fields)

    lint_enabled = not bool(getattr(args, "no_lint", False))
    report = build_dag_report(
        ctx=dag.edge_ctx,
        nodes=dag.nodes,
        root=dag.root_path,
        base_path=dag.base_path,
        cfg_loader=dag.cfg_loader,
        registry_paths=dag.registry_paths,
        registry_require_fields=reg_require_fields,
        validation_profile=profile,
        include_edge_evidence=include_evidence,
        max_edges=max_edges,
        max_triggers=max_triggers,
        include_lint=lint_enabled,
        dependency_file_errors=dep_file_errors,
    )

    fmt = str(getattr(args, "format", "md") or "md").strip().lower()
    fail_on = str(getattr(args, "fail_on", "none") or "none").strip().lower()
    if fail_on not in ("none", "lint_errors", "anomalies", "both"):
        fail_on = "none"

    fail_sev = str(getattr(args, "fail_severity", "ERROR") or "ERROR").strip().upper()
    if fail_sev not in ("ERROR", "WARNING"):
        fail_sev = "ERROR"

    view = build_dag_report_view(
        dag=dag,
        report=report,
        preset=preset,
        max_edges=max_edges,
        md_max_edges=md_max_edges,
        max_triggers=max_triggers,
        include_evidence=include_evidence,
        registry_require_fields=reg_require_fields,
        lint_enabled=lint_enabled,
        profile_name=str(getattr(args, "profile", "") or "") or None,
        fail_on=fail_on,
        fail_severity=fail_sev,
    )

    emit_report(
        view,
        fmt=fmt,
        out_md=getattr(args, "out_md", None),
        out_json=getattr(args, "out_json", None),
    )

    lint_cnt = report.lint_issue_count(min_severity=fail_sev)
    anom_cnt = report.anomaly_issue_count(min_severity=fail_sev)

    should_fail = False
    if fail_on == "lint_errors":
        should_fail = lint_cnt > 0
    elif fail_on == "anomalies":
        should_fail = anom_cnt > 0
    elif fail_on == "both":
        should_fail = (lint_cnt > 0) or (anom_cnt > 0)

    if should_fail:
        print(
            f"FAIL (--fail-on {fail_on} --fail-severity {fail_sev}): "
            f"lint>={fail_sev}={lint_cnt}, anomalies>={fail_sev}={anom_cnt}",
            file=sys.stderr,
        )
        return 1

    return 0
