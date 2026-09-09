"""Command-line entrypoint implementation for the OSS benchmark."""

from __future__ import annotations

import argparse
import getpass
import json
import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from tools.oss_benchmark.certification_runtime_catalog import SCENARIO_CHOICES
from tools.oss_benchmark.collectors import collect_all_projects, is_dirty_repo
from tools.oss_benchmark.config import DATA_PATH, DOC_PATH, ROOT
from tools.oss_benchmark.contexts import resolve_release_context
from tools.oss_benchmark.outputs import write_benchmark_outputs
from tools.oss_benchmark.pr_summary import PR_SUMMARY_PATH
from tools.oss_benchmark.state import RunContext


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse benchmark CLI options."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", default=str(ROOT / ".cache" / "oss-code-quality-benchmark"))
    parser.add_argument("--top", type=int, default=15)
    parser.add_argument("--include-external", action="store_true")
    parser.add_argument("--previous-data", default=str(ROOT / DATA_PATH))
    parser.add_argument("--baseline-data", default="")
    parser.add_argument("--allow-stale", action="store_true")
    parser.add_argument(
        "--project",
        choices=("all", "dpone", "airbyte", "dlt", "pentaho-kettle", "apache-hop", "sling"),
        default="all",
    )
    parser.add_argument("--runner-name", default=os.environ.get("GITHUB_ACTOR") or getpass.getuser())
    parser.add_argument("--workflow-name", default=os.environ.get("GITHUB_WORKFLOW") or "local")
    parser.add_argument("--run-id", default=os.environ.get("GITHUB_RUN_ID") or "local")
    parser.add_argument("--run-url", default="")
    parser.add_argument("--git-sha", default=os.environ.get("GITHUB_SHA") or _git_value("rev-parse", "HEAD"))
    parser.add_argument("--branch", default=os.environ.get("GITHUB_REF_NAME") or _git_value("branch", "--show-current"))
    parser.add_argument("--release-version", default="")
    parser.add_argument("--release-tag", default="")
    parser.add_argument("--release-sha", default="")
    parser.add_argument("--max-stale-days", type=int, default=30)
    parser.add_argument("--external-analyzer-timeout", type=int, default=60)
    parser.add_argument("--verify-source-urls", action="store_true")
    parser.add_argument("--pr-summary", default=str(ROOT / PR_SUMMARY_PATH))
    parser.add_argument("--enforce-quality-gates", action="store_true")
    parser.add_argument("--run-certification", action="store_true")
    parser.add_argument("--certification-mode", choices=("reuse", "local", "skip"), default="reuse")
    parser.add_argument("--scenario", choices=SCENARIO_CHOICES, default="all")
    parser.add_argument("--max-scenario-seconds", type=int, default=120)
    parser.add_argument("--fail-on-certification", action="store_true")
    return parser.parse_args(argv)


def main() -> int:
    """Run the benchmark refresh CLI."""

    args = parse_args()
    generated_at = datetime.now(UTC).replace(microsecond=0).isoformat()
    metrics, failures = collect_all_projects(
        Path(args.workspace),
        top_n=args.top,
        include_external=bool(args.include_external),
        project=args.project,
        local_branch=str(args.branch),
        local_commit=str(args.git_sha),
    )
    requested_slugs = {_metric.spec.slug for _metric in metrics} | set(failures)
    if args.project != "all" and not requested_slugs:
        requested_slugs = {args.project}
    previous_payload = load_previous_payload(Path(args.previous_data))
    baseline_payload = load_previous_payload(Path(args.baseline_data)) if args.baseline_data else previous_payload
    run_context = RunContext(
        generated_at=generated_at,
        updated_by=str(args.runner_name),
        workflow_name=str(args.workflow_name),
        run_id=str(args.run_id),
        run_url=str(args.run_url),
        git_sha=str(args.git_sha),
        branch=str(args.branch),
    )
    release_context = resolve_release_context(
        repo_root=ROOT,
        branch=str(args.branch),
        git_sha=str(args.git_sha),
        dirty=is_dirty_repo(ROOT),
        explicit_version=str(args.release_version),
        explicit_tag=str(args.release_tag),
        explicit_sha=str(args.release_sha),
    )
    payload = write_benchmark_outputs(
        metrics,
        generated_at=generated_at,
        run_context=run_context,
        release_context=release_context,
        previous_payload=previous_payload,
        requested_slugs=requested_slugs,
        failures=failures,
        allow_stale=bool(args.allow_stale),
        baseline_payload=baseline_payload,
        pr_summary_path=_resolve_root_path(str(args.pr_summary)),
        external_analyzer_timeout=int(args.external_analyzer_timeout),
        verify_source_urls=bool(args.verify_source_urls),
        max_stale_days=int(args.max_stale_days),
        run_certification=bool(args.run_certification),
        certification_mode=str(args.certification_mode),
        certification_scenario=str(args.scenario),
        max_scenario_seconds=int(args.max_scenario_seconds),
    )
    print(f"Wrote {DOC_PATH.as_posix()}, {DATA_PATH.as_posix()} and SVG assets.")
    if _should_fail_for_freshness(payload, failures, allow_stale=bool(args.allow_stale)):
        print(f"Benchmark refresh had unavailable metrics: {failures}")
        return 1
    if payload.get("freshness_summary", {}).get("stale", 0):
        print(f"Benchmark refresh retained {payload['freshness_summary']['stale']} stale project metric bundle(s).")
    gates = payload.get("quality_gates") or {}
    if args.enforce_quality_gates and gates.get("status") == "failed":
        failed_labels = ", ".join(check.get("label", "unknown") for check in gates.get("failed_checks", []))
        print(f"Benchmark quality gates failed: {failed_labels}")
        return 1
    certification_gates = (payload.get("runtime_certification_v2") or {}).get("gates") or {}
    if args.fail_on_certification and certification_gates.get("status") == "failed":
        failed_labels = ", ".join(
            check.get("label", "unknown") for check in certification_gates.get("failed_checks", [])
        )
        print(f"Benchmark executable certification failed: {failed_labels}")
        return 1
    return 0


def load_previous_payload(path: Path) -> dict[str, Any] | None:
    """Load prior benchmark payload when it exists."""

    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def _should_fail_for_freshness(payload: dict[str, Any], failures: dict[str, str], *, allow_stale: bool) -> bool:
    unavailable = payload.get("freshness_summary", {}).get("unavailable", 0)
    return bool(failures and (not allow_stale or unavailable))


def _git_value(*args: str) -> str:
    result = subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True, check=False)
    return result.stdout.strip() if result.returncode == 0 else ""


def _resolve_root_path(value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else ROOT / path
