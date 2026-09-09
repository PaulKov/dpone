#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

DEFAULT_REGISTRY = "examples/registry/sources.yaml"
DEFAULT_SINGLE_MANIFEST = "examples/batch/landing_postgres_to_bq.batch.yaml"
DEFAULT_SINGLE_SELECTOR = "public.core_city"


@dataclass(frozen=True, slots=True)
class SmokeCase:
    name: str
    manifest_rel: str
    selector: str
    registry_rel: str = DEFAULT_REGISTRY
    include_dag_report: bool = True


DEFAULT_SMOKE_CASES: tuple[SmokeCase, ...] = (
    SmokeCase(
        name="postgres-batch",
        manifest_rel="examples/batch/landing_postgres_to_bq.batch.yaml",
        selector="public.core_city",
    ),
    SmokeCase(
        name="appsflyer-batch",
        manifest_rel="examples/batch/landing_appsflyer_api.batch.yaml",
        selector="app.installs_report",
        include_dag_report=False,
    ),
    SmokeCase(
        name="cbr-batch",
        manifest_rel="examples/batch/landing_cbr_api.batch.yaml",
        selector="app.xml_daily_asp",
        include_dag_report=False,
    ),
    SmokeCase(
        name="mindbox-batch",
        manifest_rel="examples/batch/landing_mindbox_api.batch.yaml",
        selector="app.getclients",
        include_dag_report=False,
    ),
    SmokeCase(
        name="fasttrack-batch",
        manifest_rel="examples/batch/landing_fasttrack_api.batch.yaml",
        selector="default.cascade_transactions",
    ),
    SmokeCase(
        name="similarweb-batch",
        manifest_rel="examples/batch/landing_similarweb_api.batch.yaml",
        selector="default.keywords",
    ),
    SmokeCase(
        name="openexchangerates-batch",
        manifest_rel="examples/batch/landing_openexchangerates_api.batch.yaml",
        selector="default.historical_rates_daily",
        include_dag_report=False,
    ),
    SmokeCase(
        name="google-ads-batch",
        manifest_rel="examples/batch/landing_google_ads_api.batch.yaml",
        selector="app.ads_stats",
        include_dag_report=False,
    ),
    SmokeCase(
        name="google-sheets-batch",
        manifest_rel="examples/batch/landing_google_sheets_api.batch.yaml",
        selector="app.worksheet_rows",
        include_dag_report=False,
    ),
    SmokeCase(
        name="yandex-webmaster-batch",
        manifest_rel="examples/batch/landing_yandex_webmaster_api.batch.yaml",
        selector="app.host_metrics_daily",
        include_dag_report=False,
    ),
    SmokeCase(
        name="yandex-webmaster-search-queries-batch",
        manifest_rel="examples/batch/landing_yandex_webmaster_api.batch.yaml",
        selector="app.search_queries_history_daily",
        include_dag_report=False,
    ),
    SmokeCase(
        name="yandex-webmaster-query-regions-batch",
        manifest_rel="examples/batch/landing_yandex_webmaster_api.batch.yaml",
        selector="app.query_analytics_by_region_daily",
        include_dag_report=False,
    ),
)


def _command_prefix(raw: str | None) -> list[str]:
    value = raw or os.environ.get("DPONE_CMD", "dpone")
    return shlex.split(value)


def build_smoke_commands(
    *,
    project_root: Path,
    manifest_rel: str,
    registry_rel: str,
    selector: str,
    dpone_cmd: str | None = None,
) -> list[list[str]]:
    prefix = _command_prefix(dpone_cmd)
    manifest = str((project_root / manifest_rel).resolve())
    manifest_base_path = str((project_root / manifest_rel).resolve().parent)
    registry = str((project_root / registry_rel).resolve())
    return [
        prefix + ["--help"],
        prefix + ["manifest", "--help"],
        prefix + ["dag", "--help"],
        prefix + ["manifest", "validate", manifest, "--registry", registry],
        prefix + ["manifest", "render", manifest, "--selector", selector, "--registry", registry],
        prefix
        + [
            "dag",
            "report",
            manifest,
            "--base-path",
            manifest_base_path,
            "--format",
            "json",
            "--preset",
            "ci",
            "--registry",
            registry,
        ],
    ]


def build_smoke_plan(
    *,
    project_root: Path,
    cases: Sequence[SmokeCase],
    dpone_cmd: str | None = None,
) -> list[list[str]]:
    prefix = _command_prefix(dpone_cmd)
    commands: list[list[str]] = [
        prefix + ["--help"],
        prefix + ["manifest", "--help"],
        prefix + ["dag", "--help"],
    ]
    for case in cases:
        manifest_path = (project_root / case.manifest_rel).resolve()
        manifest = str(manifest_path)
        manifest_base_path = str(manifest_path.parent)
        registry = str((project_root / case.registry_rel).resolve())
        commands.extend(
            [
                prefix + ["manifest", "validate", manifest, "--registry", registry],
                prefix + ["manifest", "render", manifest, "--selector", case.selector, "--registry", registry],
            ]
        )
        if case.include_dag_report:
            commands.append(
                prefix
                + [
                    "dag",
                    "report",
                    manifest,
                    "--base-path",
                    manifest_base_path,
                    "--format",
                    "json",
                    "--preset",
                    "ci",
                    "--registry",
                    registry,
                ]
            )
    return commands


def _assert_semantics(cmd: Sequence[str], stdout: str) -> None:
    rendered = " ".join(cmd)
    if len(cmd) >= 3 and cmd[1:3] == ["manifest", "validate"]:
        if "OK" not in stdout and "no issues" not in stdout.lower():
            raise RuntimeError(f"Unexpected manifest validate output for: {rendered}\n{stdout}")
    elif len(cmd) >= 3 and cmd[1:3] == ["manifest", "render"]:
        if "sink:" not in stdout or "source:" not in stdout:
            raise RuntimeError(f"Unexpected manifest render output for: {rendered}\n{stdout}")
    elif len(cmd) >= 3 and cmd[1:3] == ["dag", "report"]:
        payload = json.loads(stdout)
        if int(payload.get("summary", {}).get("task_count", 0)) <= 0:
            raise RuntimeError(f"Unexpected dag report payload for: {rendered}\n{stdout}")


def run_smoke_commands(commands: Sequence[Sequence[str]], *, project_root: Path) -> None:
    env = os.environ.copy()
    env.setdefault("DPONE_PROJECT_DIR", str(project_root.resolve()))
    for cmd in commands:
        print("[smoke]", " ".join(cmd))
        proc = subprocess.run(
            list(cmd),
            cwd=str(project_root),
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            sys.stderr.write(proc.stdout)
            sys.stderr.write(proc.stderr)
            raise SystemExit(proc.returncode)
        _assert_semantics(cmd, proc.stdout)


def _single_case_from_args(args: argparse.Namespace) -> SmokeCase:
    manifest = args.manifest or DEFAULT_SINGLE_MANIFEST
    selector = args.selector or DEFAULT_SINGLE_SELECTOR
    registry = args.registry or DEFAULT_REGISTRY
    return SmokeCase(name="custom", manifest_rel=manifest, selector=selector, registry_rel=registry)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run installed-wheel smoke checks against dpone CLI.")
    parser.add_argument("--project-root", default=".", help="Repository checkout root with examples/ directory")
    parser.add_argument(
        "--manifest",
        default=None,
        help=(
            "Optional relative manifest path used for a single-case smoke check. "
            "If omitted, dpone validates/renders/reports all default example manifests."
        ),
    )
    parser.add_argument("--registry", default=DEFAULT_REGISTRY, help="Relative registry path used for smoke checks")
    parser.add_argument("--selector", default=None, help="Selector used for manifest render smoke check")
    parser.add_argument(
        "--dpone-cmd",
        default=None,
        help="Override dpone command, for example: 'python -m dpone.cli.main'. Defaults to env DPONE_CMD or 'dpone'.",
    )
    args = parser.parse_args()

    project_root = Path(args.project_root).resolve()
    if args.manifest:
        cases = (_single_case_from_args(args),)
    else:
        cases = DEFAULT_SMOKE_CASES
    commands = build_smoke_plan(project_root=project_root, cases=cases, dpone_cmd=args.dpone_cmd)
    run_smoke_commands(commands, project_root=project_root)
    print(f"[smoke] package smoke passed ({len(cases)} case(s))")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
