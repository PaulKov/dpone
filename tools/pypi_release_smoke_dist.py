#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Callable, Sequence
from dataclasses import replace
from pathlib import Path
from typing import Literal

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pypi_candidate_inventory as candidate_inventory  # noqa: E402
from pypi_candidate_contract import (  # noqa: E402
    CandidateArtifact,
    CandidateInventoryArtifact,
    CandidateInventoryLimits,
    CandidateInventoryReport,
    DistributionRelease,
)
from pypi_release_smoke import PyPIClient, PyPIReleaseSmokeReport, wait_for_release  # noqa: E402

__all__ = [
    "CandidateArtifact",
    "CandidateInventoryArtifact",
    "CandidateInventoryReport",
    "DistributionRelease",
    "discover_distribution_releases",
    "evaluate_candidate_inventory",
    "revalidate_candidate_inventory",
    "validate_distribution_inventory",
    "verify_distribution_releases",
]

ReportFormat = Literal["json", "md"]
ReleaseWaiter = Callable[..., PyPIReleaseSmokeReport]

# Compatibility exports for existing automation and tests. The inventory policy
# itself lives in pypi_candidate_inventory.
EXPECTED_DISTRIBUTIONS = candidate_inventory.EXPECTED_DISTRIBUTIONS
MAX_CANDIDATE_ENTRIES = candidate_inventory.DEFAULT_MAX_ENTRIES
MAX_CANDIDATE_FILE_BYTES = candidate_inventory.DEFAULT_MAX_FILE_BYTES
MAX_CANDIDATE_TOTAL_BYTES = candidate_inventory.DEFAULT_MAX_TOTAL_BYTES
MAX_RELEASE_TIMEOUT_SECONDS = 3600
MAX_RELEASE_POLL_INTERVAL_SECONDS = 300


def _inventory_limits() -> CandidateInventoryLimits:
    return CandidateInventoryLimits(
        max_entries=MAX_CANDIDATE_ENTRIES,
        max_file_bytes=MAX_CANDIDATE_FILE_BYTES,
        max_total_bytes=MAX_CANDIDATE_TOTAL_BYTES,
    )


def discover_distribution_releases(dist_dir: Path) -> tuple[DistributionRelease, ...]:
    """Discover only a fully inspected directory; never ignore stray entries."""

    return candidate_inventory.discover_distribution_releases(
        dist_dir,
        limits=_inventory_limits(),
        hash_factory=hashlib.sha256,
    )


def validate_distribution_inventory(
    releases: Sequence[DistributionRelease],
    *,
    expected_version: str,
) -> tuple[str, ...]:
    return candidate_inventory.validate_distribution_inventory(
        releases,
        expected_version=expected_version,
    )


def evaluate_candidate_inventory(dist_dir: Path, *, expected_version: str) -> CandidateInventoryReport:
    """Evaluate a closed local candidate directory without network access."""

    return candidate_inventory.evaluate_candidate_inventory(
        dist_dir,
        expected_version=expected_version,
        limits=_inventory_limits(),
        hash_factory=hashlib.sha256,
    )


def revalidate_candidate_inventory(
    dist_dir: Path,
    *,
    initial: CandidateInventoryReport,
) -> CandidateInventoryReport:
    """Repeat the full safe scan and compare it with the pre-network handoff."""

    return candidate_inventory.revalidate_candidate_inventory(
        dist_dir,
        initial=initial,
        limits=_inventory_limits(),
        hash_factory=hashlib.sha256,
    )


def _polling_blockers(timeout_seconds: int, poll_interval_seconds: int) -> tuple[str, ...]:
    blockers: list[str] = []
    settings = (
        ("TIMEOUT", timeout_seconds, 0, MAX_RELEASE_TIMEOUT_SECONDS),
        ("POLL_INTERVAL", poll_interval_seconds, 1, MAX_RELEASE_POLL_INTERVAL_SECONDS),
    )
    for name, value, minimum, maximum in settings:
        if value < minimum:
            blockers.append(f"PYPI_RELEASE_{name}_INVALID: minimum_seconds={minimum} actual_seconds={value}")
        elif value > maximum:
            blockers.append(f"PYPI_RELEASE_{name}_LIMIT_EXCEEDED: maximum_seconds={maximum} actual_seconds={value}")
    return tuple(blockers)


def verify_distribution_releases(
    releases: Sequence[DistributionRelease],
    *,
    timeout_seconds: int,
    poll_interval_seconds: int,
    install_smoke: bool,
    dpone_install_extra: str | None,
    waiter: ReleaseWaiter = wait_for_release,
) -> tuple[PyPIReleaseSmokeReport, ...]:
    if blockers := _polling_blockers(timeout_seconds, poll_interval_seconds):
        raise ValueError("; ".join(blockers))
    reports: list[PyPIReleaseSmokeReport] = []
    for release in releases:
        install_extra = dpone_install_extra if release.package == "dpone" else None
        reports.append(
            waiter(
                package=release.package,
                version=release.version,
                timeout_seconds=timeout_seconds,
                poll_interval_seconds=poll_interval_seconds,
                install_smoke=install_smoke,
                install_extra=install_extra,
                client=PyPIClient(),
                candidate_artifacts=release.candidate_artifacts,
                emit_progress=False,
            )
        )
    return tuple(reports)


def _render_reports(reports: Sequence[PyPIReleaseSmokeReport], report_format: ReportFormat) -> str:
    if report_format == "json":
        return json.dumps([report.to_dict() for report in reports], indent=2, sort_keys=True) + "\n"
    return "\n".join(report.to_markdown().rstrip() for report in reports) + "\n"


def _render_inventory_report(report: CandidateInventoryReport, report_format: ReportFormat) -> str:
    if report_format == "json":
        return json.dumps(report.to_payload(), indent=2, sort_keys=True) + "\n"
    lines = [
        "# PyPI candidate inventory",
        "",
        f"- Status: `{report.status}`",
        f"- Decision: `{report.decision}`",
        f"- Expected version: `{report.expected_version}`",
        f"- Artifacts: `{len(report.artifacts)}`",
    ]
    if report.blockers:
        lines.extend(("", "## Blockers", "", *(f"- {blocker}" for blocker in report.blockers)))
    return "\n".join(lines) + "\n"


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify PyPI visibility for built distribution versions.")
    parser.add_argument("--dist-dir", default="dist", type=Path)
    parser.add_argument("--timeout-seconds", default=900, type=int)
    parser.add_argument("--poll-interval-seconds", default=30, type=int)
    parser.add_argument("--install-smoke", action="store_true")
    parser.add_argument("--dpone-install-extra", default="accel")
    parser.add_argument("--expected-version", required=True)
    parser.add_argument("--inventory-only", action="store_true")
    parser.add_argument("--format", choices=("json", "md"), default="md")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    inventory = evaluate_candidate_inventory(args.dist_dir, expected_version=args.expected_version)
    polling_blockers = _polling_blockers(args.timeout_seconds, args.poll_interval_seconds)
    if polling_blockers:
        inventory = replace(inventory, blockers=inventory.blockers + polling_blockers)
    if inventory.blockers:
        print(_render_inventory_report(inventory, args.format), end="")
        return 2
    if args.inventory_only:
        final_inventory = revalidate_candidate_inventory(args.dist_dir, initial=inventory)
        print(_render_inventory_report(final_inventory, args.format), end="")
        return 0 if not final_inventory.blockers else 2

    reports = verify_distribution_releases(
        inventory.releases,
        timeout_seconds=args.timeout_seconds,
        poll_interval_seconds=args.poll_interval_seconds,
        install_smoke=args.install_smoke,
        dpone_install_extra=args.dpone_install_extra,
    )
    if not all(report.passed for report in reports):
        print(_render_reports(reports, args.format), end="")
        return 1

    final_inventory = revalidate_candidate_inventory(args.dist_dir, initial=inventory)
    if final_inventory.blockers:
        print(_render_inventory_report(final_inventory, args.format), end="")
        return 2

    print(_render_reports(reports, args.format), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
