"""Public CI producers for domain-first workload index and change impact."""

from __future__ import annotations

import argparse
import logging
from typing import Any

from dpone.commands.output_json import write_json
from dpone.readiness.airflow_self_service_models import dpone_error, manual_fix
from dpone.readiness.error_contract import error_docs_url
from dpone.readiness.workload_index_promotion_composition import (
    WorkloadIndexPromotionError,
    build_workload_index_promotion_service,
)
from dpone.services.workload_discovery_projection import (
    WorkloadDiscoveryFailed,
    build_change_impact_report_from_files,
    build_workload_index,
)


def register_index_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("index", help="Emit the ephemeral domain-first workload index as JSON")
    parser._dpone_io_contract = (
        "`0`: UTF-8 `dpone.workload-index.v1` JSON on stdout; stderr is empty.",
        "`1`: structured JSON with `dpone.error.v1` entries on stdout; stderr is empty.",
        "`2`: argparse usage error on stderr.",
    )
    parser.add_argument("--root", default=".", help="Project root (default: current directory)")
    return parser


def register_impact_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("impact", help="Compare domain-first discovery with a prior workload index")
    parser._dpone_io_contract = (
        "`0`: UTF-8 `dpone.workload-change-impact.v1` JSON on stdout; stderr is empty.",
        "`1`: failed discovery or invalid baseline/current JSON on stdout; stderr is empty.",
        "`2`: argparse usage error on stderr.",
    )
    parser.add_argument("--root", default=".", help="Project root (default: current directory)")
    parser.add_argument(
        "--baseline",
        required=True,
        help="Project-confined prior workload-index JSON/YAML",
    )
    parser.add_argument(
        "--current",
        help="Project-confined candidate workload index; avoids rediscovery races in CI",
    )
    return parser


def register_promote_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "promote",
        help="CAS-promote an approved workload-index candidate",
        usage=(
            "%(prog)s --candidate PATH --baseline PATH "
            "--approved-candidate-sha256 SHA256 "
            "--approved-current-fingerprint FINGERPRINT "
            "(--expect-baseline-absent | "
            "--expected-baseline-sha256 SHA256 "
            "--expected-baseline-fingerprint FINGERPRINT) [--root ROOT]"
        ),
    )
    parser._dpone_io_contract = (
        "`0`: UTF-8 `dpone.workload-index-promotion.v1` receipt on stdout.",
        "`1`: invalid workload-index input on stdout; stderr is empty.",
        "`2`: malformed promotion request as structured JSON on stdout, or argparse usage error on stderr.",
        "`4`: approval, confinement, lock, or baseline-CAS violation on stdout.",
    )
    parser.add_argument("--root", default=".", help="Project root (default: current directory)")
    parser.add_argument("--candidate", required=True, help="Project-confined approved candidate index")
    parser.add_argument("--baseline", required=True, help="Project-confined accepted baseline path")
    parser.add_argument("--approved-candidate-sha256", required=True, help="Raw candidate digest from approval")
    parser.add_argument(
        "--approved-current-fingerprint",
        required=True,
        help="Candidate semantic fingerprint from approval",
    )
    baseline = parser.add_mutually_exclusive_group(required=True)
    baseline.add_argument("--expected-baseline-sha256", help="Approved raw digest of the existing baseline")
    baseline.add_argument(
        "--expect-baseline-absent",
        action="store_true",
        help="Create the first baseline only if it is still absent",
    )
    parser.add_argument(
        "--expected-baseline-fingerprint",
        help="Approved existing-baseline fingerprint; required with --expected-baseline-sha256",
    )
    return parser


def cmd_workload_index(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    try:
        payload = build_workload_index(args.root)
    except WorkloadDiscoveryFailed as exc:
        write_json(_discovery_error_payload(exc))
        return 1
    except (OSError, ValueError) as exc:
        write_json(
            _projection_error_payload(
                str(exc),
                fix_id="repair_project_authoring",
            )
        )
        return 1
    write_json(payload)
    return 0


def cmd_workload_impact(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    try:
        payload = build_change_impact_report_from_files(
            args.root,
            baseline_path=args.baseline,
            current_path=getattr(args, "current", None),
        )
    except WorkloadDiscoveryFailed as exc:
        write_json(_discovery_error_payload(exc))
        return 1
    except (OSError, ValueError) as exc:
        write_json(
            _projection_error_payload(
                str(exc),
                fix_id="restore_or_regenerate_approved_baseline",
            )
        )
        return 1
    write_json(payload)
    return 0 if payload["discovery_status"] != "failed" else 1


def cmd_workload_promote(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    try:
        receipt = build_workload_index_promotion_service(args.root).promote(
            candidate_path=args.candidate,
            baseline_path=args.baseline,
            approved_candidate_sha256=args.approved_candidate_sha256,
            approved_current_fingerprint=args.approved_current_fingerprint,
            expected_baseline_sha256=args.expected_baseline_sha256,
            expected_baseline_fingerprint=args.expected_baseline_fingerprint,
            expect_baseline_absent=bool(args.expect_baseline_absent),
        )
    except WorkloadIndexPromotionError as exc:
        write_json(
            _promotion_error_payload(
                exc.code,
                str(exc),
                recovery_path=exc.recovery_path,
                recovery_artifacts=exc.recovery_artifacts,
            )
        )
        if exc.code == "DPONE_WORKLOAD_INDEX_INVALID":
            return 1
        if exc.code == "DPONE_WORKLOAD_INDEX_PROMOTION_REQUEST_INVALID":
            return 2
        return 4
    except (OSError, ValueError) as exc:
        write_json(
            _projection_error_payload(
                str(exc),
                fix_id="inspect_promotion_inputs",
            )
        )
        return 1
    write_json(receipt.to_dict())
    return 0


def _discovery_error_payload(error: WorkloadDiscoveryFailed) -> dict[str, Any]:
    return {
        "passed": False,
        "errors": [
            dpone_error(
                issue.code,
                issue.message,
                stage="workload_discovery",
                path=issue.path,
                entity=(
                    {"kind": "pipeline", "id": issue.pipeline_id}
                    if issue.pipeline_id is not None
                    else {"kind": "domain", "id": issue.domain}
                    if issue.domain is not None
                    else None
                ),
                docs_url=error_docs_url(issue.code),
            )
            for issue in error.issues
        ],
    }


def _projection_error_payload(
    message: str,
    *,
    fix_id: str,
) -> dict[str, Any]:
    code = "DPONE_WORKLOAD_INDEX_INVALID"
    return {
        "passed": False,
        "errors": [
            dpone_error(
                code,
                message,
                stage="workload_discovery",
                fixes=[manual_fix(fix_id)],
                docs_url=error_docs_url(code),
            )
        ],
    }


def _promotion_error_payload(
    code: str,
    message: str,
    *,
    recovery_path: str | None = None,
    recovery_artifacts: tuple[str, ...] = (),
) -> dict[str, Any]:
    fix_id = {
        "DPONE_WORKLOAD_INDEX_APPROVAL_MISMATCH": "repeat_protected_approval",
        "DPONE_WORKLOAD_INDEX_INVALID": "regenerate_valid_candidate",
        "DPONE_WORKLOAD_INDEX_PROMOTION_CONFLICT": "regenerate_impact_and_reapprove",
        "DPONE_WORKLOAD_INDEX_PROMOTION_RECOVERY_REQUIRED": "follow_workload_index_recovery_runbook",
        "DPONE_WORKLOAD_INDEX_PROMOTION_REQUEST_INVALID": "use_exactly_one_baseline_guard",
    }.get(code, "inspect_promotion_inputs")
    return {
        "passed": False,
        "errors": [
            dpone_error(
                code,
                message,
                stage="workload_index_promotion",
                path=recovery_path,
                fixes=[manual_fix(fix_id)],
                docs_url=error_docs_url(code),
                extra=({"recovery_artifacts": list(recovery_artifacts)} if recovery_artifacts else None),
            )
        ],
    }


__all__ = [
    "cmd_workload_impact",
    "cmd_workload_index",
    "cmd_workload_promote",
    "register_impact_parser",
    "register_index_parser",
    "register_promote_parser",
]
