#!/usr/bin/env python3
"""Run the closed semantic-refresh V2 local Docker evidence campaign."""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime  # type: ignore[attr-defined]
from pathlib import Path

from dpone.adapters.semantic_refresh_local_campaign_evidence import (
    CreateOnlyLocalCampaignReportWriter,
    LocalCampaignEvidenceWriteError,
)
from dpone.ops.semantic_refresh_local_campaign import (
    LocalCampaignCheck,
    LocalCommandObservation,
    SemanticRefreshLocalCampaign,
)
from dpone.security_redaction import redact_absolute_paths, redact_text

_SKIPPED = re.compile(r"(?:^|\s)\d+ skipped(?:,|\s|$)")


def _checks() -> tuple[LocalCampaignCheck, ...]:
    return (
        LocalCampaignCheck(
            "contracts",
            (
                "uv",
                "run",
                "pytest",
                "tests/test_semantic_refresh_contracts.py",
                "tests/test_semantic_refresh_contracts_evidence.py",
                "tests/test_semantic_refresh_contracts_assurance.py",
                "tests/test_dbt_semantic_refresh_baseline_issuance.py",
                "tests/test_semantic_refresh_mssql_baseline.py",
                "tests/test_semantic_refresh_baseline_composition.py",
                "-q",
            ),
            {},
        ),
        LocalCampaignCheck(
            "dbt_catalog_mssql",
            ("uv", "run", "pytest", "tests/test_dbt_semantic_refresh_catalog_proof_live.py", "-q"),
            {"DPONE_RUN_SEMANTIC_REFRESH_MSSQL_LIVE": "1"},
        ),
        LocalCampaignCheck(
            "dbt_runtime_overlay",
            ("uv", "run", "pytest", "tests/test_dbt_semantic_refresh_execution.py", "-q"),
            {},
        ),
        LocalCampaignCheck(
            "mssql_runtime",
            ("uv", "run", "pytest", "tests/test_semantic_refresh_mssql_live.py", "-q"),
            {"DPONE_RUN_SEMANTIC_REFRESH_MSSQL_LIVE": "1"},
        ),
        LocalCampaignCheck(
            "clickhouse_minio_kes",
            (
                "uv",
                "run",
                "--extra",
                "s3",
                "pytest",
                "tests/test_semantic_refresh_clickhouse_local_integration.py",
                "-q",
            ),
            {
                "DPONE_RUN_INTEGRATION": "1",
                "DPONE_RUN_INTEGRATION_LIVE": "1",
                "DPONE_IT_CH_HTTP_URL": "http://127.0.0.1:58124/",
                "DPONE_IT_CH_DATABASE": "dpone_semref_v2",
            },
        ),
        LocalCampaignCheck(
            # Bounded fixture-produced authority: local publication-stage proof,
            # deliberately not a full dbt-to-publication production route claim.
            "local_publication_stage_cross_provider_proof",
            (
                "uv",
                "run",
                "--extra",
                "s3",
                "pytest",
                "tests/test_semantic_refresh_end_to_end_local_integration.py",
                "-q",
            ),
            {
                "DPONE_RUN_SEMANTIC_REFRESH_E2E_LIVE": "1",
                "DPONE_RUN_SEMANTIC_REFRESH_MSSQL_LIVE": "1",
                "DPONE_RUN_INTEGRATION": "1",
                "DPONE_RUN_INTEGRATION_LIVE": "1",
                "DPONE_IT_CH_HTTP_URL": "http://127.0.0.1:58124/",
                "DPONE_IT_CH_DATABASE": "dpone_semref_v2",
            },
        ),
        LocalCampaignCheck(
            "airflow_3_3",
            (
                "uv",
                "run",
                "pytest",
                (
                    "tests/test_semantic_refresh_airflow_materializer.py::"
                    "test_real_airflow_33_dag_serializes_with_assets_only_on_terminal_task"
                ),
                "-q",
            ),
            {"DPONE_RUN_INTEGRATION_LIVE": "1"},
        ),
        LocalCampaignCheck(
            "kubernetes_termination",
            (
                "uv",
                "run",
                "--extra",
                "kubernetes",
                "pytest",
                "tests/test_semantic_refresh_kubernetes_local_integration.py",
                "-q",
            ),
            {
                "DPONE_RUN_SEMANTIC_REFRESH_K8S_LIVE": "1",
                "DPONE_IT_KUBECONFIG_CONTEXT": "k3d-dpone-semref-v2",
            },
        ),
        LocalCampaignCheck(
            "vault_kubernetes_auth",
            (
                "uv",
                "run",
                "--extra",
                "vault",
                "pytest",
                "tests/test_semantic_refresh_vault_local_integration.py",
                "-q",
            ),
            {
                "DPONE_RUN_SEMANTIC_REFRESH_VAULT_LIVE": "1",
                "DPONE_IT_KUBECONFIG_CONTEXT": "k3d-dpone-semref-v2",
            },
        ),
    )


class _SubprocessRunner:
    def __call__(
        self,
        *,
        argv: Sequence[str],
        environment: Mapping[str, str],
        cwd: Path,
    ) -> LocalCommandObservation:
        started = time.monotonic()
        completed = subprocess.run(
            tuple(argv),
            cwd=cwd,
            env={**os.environ, **environment},
            capture_output=True,
            text=True,
            check=False,
        )
        duration_ms = round((time.monotonic() - started) * 1000)
        combined = f"{completed.stdout}\n{completed.stderr}"
        if completed.returncode != 0:
            safe = redact_absolute_paths(redact_text(combined))
            print(safe[-4000:], file=sys.stderr)
        return LocalCommandObservation(
            return_code=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
            duration_ms=duration_ms,
            skipped=bool(_SKIPPED.search(combined)),
        )


def _source_snapshot(repo_root: Path) -> tuple[str, str, bool]:
    head = _git(repo_root, "rev-parse", "HEAD").strip()
    status = _git(repo_root, "status", "--porcelain=v1", "--untracked-files=all")
    tracked_diff = subprocess.run(
        ("git", "diff", "--binary", "HEAD", "--", "src", "packages", "tests", "tools", "docs"),
        cwd=repo_root,
        check=True,
        capture_output=True,
    ).stdout
    untracked = _git(
        repo_root,
        "ls-files",
        "--others",
        "--exclude-standard",
        "--",
        "src",
        "packages",
        "tests",
        "tools",
        "docs",
    ).splitlines()
    hasher = hashlib.sha256()
    hasher.update(head.encode())
    hasher.update(tracked_diff)
    for relative in sorted(untracked):
        path = repo_root / relative
        if not path.is_file() or "/target/" in f"/{relative}" or relative.endswith(".user.yml"):
            continue
        hasher.update(relative.encode())
        hasher.update(b"\0")
        hasher.update(path.read_bytes())
        hasher.update(b"\0")
    return head, "sha256:" + hasher.hexdigest(), bool(status.strip())


def _git(repo_root: Path, *args: str) -> str:
    return subprocess.run(
        ("git", *args),
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path)
    parser.add_argument("--check", action="append", dest="check_ids")
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args(argv)

    available = _checks()
    by_id = {check.check_id: check for check in available}
    if args.list:
        print("\n".join(by_id))
        return 0
    if args.output is None:
        parser.error("--output is required unless --list is used")
    requested = tuple(args.check_ids or by_id)
    unknown = sorted(set(requested) - set(by_id))
    if unknown:
        parser.error(f"unknown checks: {', '.join(unknown)}")

    repo_root = args.repo_root.resolve()
    output = args.output if args.output.is_absolute() else repo_root / args.output
    git_head, snapshot, dirty = _source_snapshot(repo_root)
    try:
        report = SemanticRefreshLocalCampaign(
            command_runner=_SubprocessRunner(),
            now=lambda: datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        ).run(
            repo_root=repo_root,
            output_path=output,
            campaign_id=f"semantic-refresh-v2-local-{git_head[:12]}",
            git_head_sha=git_head,
            source_snapshot_sha256=snapshot,
            worktree_dirty=dirty,
            checks=tuple(by_id[check_id] for check_id in requested),
            expected_check_ids=frozenset(by_id),
            report_writer=CreateOnlyLocalCampaignReportWriter(),
        )
    except LocalCampaignEvidenceWriteError as exc:
        print(f"DPONE_SEMANTIC_REFRESH_LOCAL_EVIDENCE_WRITE_FAILED: {exc}", file=sys.stderr)
        return 1
    print(report.to_json(), end="")
    return 0 if report.status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
