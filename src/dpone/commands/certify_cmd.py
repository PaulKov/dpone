from __future__ import annotations

import argparse
import logging
from pathlib import Path

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.commands.readiness_facade import build_readiness_service
from dpone.services.ops.facades import (
    RouteCapabilityCertificationRequest,
    RouteCertificationMatrixError,
    RouteCertificationMatrixRequest,
    RouteCertificationMatrixService,
    SelfServiceCertificationError,
    SelfServiceCertificationRequest,
    SelfServiceCertificationService,
    build_route_capability_certification_service,
)


def cmd_certify(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    if getattr(args, "certify_command", None) == "routes":
        return _cmd_route_matrix(args)
    if getattr(args, "certify_command", None) == "self-service":
        return _cmd_self_service(args)
    if getattr(args, "certify_command", None) == "route-capabilities":
        report = build_route_capability_certification_service().certify(
            RouteCapabilityCertificationRequest(
                manifest_path=Path(args.manifest),
                scenario=args.scenario,
                output_dir=Path(args.output),
                run_id=args.run_id,
                routes=tuple(args.route or ()) or None,
                keep_artifacts=args.keep_artifacts,
                object_prefix=args.object_prefix,
                selector=args.selector,
            )
        )
        if args.format == "json":
            write_json(report.to_dict())
        else:
            write_text(report.to_markdown())
        return 0 if report.passed else 1
    code, payload, markdown = build_readiness_service().certification()
    if args.format == "json":
        write_json(payload)
    else:
        write_text(markdown)
    return code


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("certify", help="Show connector or route certification evidence")
    parser.add_argument("--format", choices=["markdown", "json"], default="markdown")
    nested = parser.add_subparsers(dest="certify_command")
    routes = nested.add_parser(
        "routes",
        help="Publish the six-dimensional route certification matrix from immutable evidence",
    )
    routes.add_argument("--commit-sha", required=True, help="Exact source commit expected in release provenance")
    routes.add_argument(
        "--evidence-dir",
        action="append",
        default=[],
        help="Explicit bounded evidence set directory; repeat for independent deployments",
    )
    routes.add_argument("--output-dir", required=True, help="Create-only matrix artifact directory")
    routes.add_argument("--max-age-hours", type=int, default=168, help="Maximum vendor-live bundle age")
    routes.add_argument(
        "--allow-unverified",
        action="store_true",
        help="Exit 0 after publishing when status is UNVERIFIED (still exits non-zero on FAIL)",
    )
    routes.add_argument("--format", choices=["md", "json"], default="md")
    self_service = nested.add_parser(
        "self-service",
        help="Publish five-user and production-reference evidence without external I/O",
    )
    self_service.add_argument("--commit-sha", required=True, help="Exact source commit under study")
    self_service.add_argument("--usability-study", help="Privacy-safe usability study JSON")
    self_service.add_argument(
        "--reference-deployment",
        action="append",
        default=[],
        help="Explicit bounded production proof directory; repeat for independent deployments",
    )
    self_service.add_argument("--output-dir", required=True, help="Create-only certification artifact directory")
    self_service.add_argument("--max-age-hours", type=int, default=168, help="Maximum route proof age")
    self_service.add_argument(
        "--allow-unverified",
        action="store_true",
        help="Exit 0 after publishing when overall_status is UNVERIFIED (still exits non-zero on FAIL)",
    )
    self_service.add_argument("--format", choices=["md", "json"], default="md")
    route = nested.add_parser(
        "route-capabilities",
        help="Certify route capability planning and benchmark selected runtime routes",
    )
    route.add_argument("--manifest", required=True, help="Path to a YAML manifest")
    route.add_argument(
        "--scenario",
        required=True,
        choices=["preflight_only", "small_live", "work-item_account_sales_benchmark"],
    )
    route.add_argument("--output", required=True, help="Output directory for certification artifacts")
    route.add_argument("--run-id", default="route-capability-certification")
    route.add_argument("--selector")
    route.add_argument("--route", action="append", default=[], help="Route id to benchmark; repeat for a matrix")
    route.add_argument("--object-prefix", help="Object-storage prefix template with {run_id} and {route_id}")
    route.add_argument("--keep-artifacts", action="store_true")
    route.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def _cmd_route_matrix(args: argparse.Namespace) -> int:
    try:
        report = RouteCertificationMatrixService().publish(
            RouteCertificationMatrixRequest(
                expected_commit=args.commit_sha,
                evidence_dirs=tuple(Path(value) for value in args.evidence_dir),
                output_dir=Path(args.output_dir),
                max_age_hours=args.max_age_hours,
            )
        )
    except RouteCertificationMatrixError as exc:
        error = {
            "schema": "dpone.error.v1",
            "code": exc.code,
            "stage": "route_certification_matrix",
            "severity": "error",
            "message": " ".join(str(exc).split())[:500],
            "fixes": [],
        }
        if args.format == "json":
            write_json(error)
        else:
            write_text(f"# dpone route certification matrix\n\n- error: `{exc.code}`\n- message: {error['message']}\n")
        return 2
    if args.format == "json":
        write_json(report.to_dict())
    else:
        write_text(report.to_markdown())
    certified = {"route-certified", "production-certified", "enterprise-certified"}
    return _certify_exit_code(
        has_input_failures=report.has_input_failures,
        overall_pass=not report.has_input_failures
        and bool(report.rows)
        and all(row.status in certified for row in report.rows),
        allow_unverified=bool(getattr(args, "allow_unverified", False)),
    )


def _cmd_self_service(args: argparse.Namespace) -> int:
    try:
        report = SelfServiceCertificationService().publish(
            SelfServiceCertificationRequest(
                expected_commit=args.commit_sha,
                usability_study=Path(args.usability_study) if args.usability_study else None,
                reference_deployments=tuple(Path(value) for value in args.reference_deployment),
                output_dir=Path(args.output_dir),
                max_age_hours=args.max_age_hours,
            )
        )
    except SelfServiceCertificationError as exc:
        error = {
            "schema": "dpone.error.v1",
            "code": exc.code,
            "stage": "self_service_certification",
            "severity": "error",
            "message": " ".join(str(exc).split())[:500],
            "fixes": [],
        }
        if args.format == "json":
            write_json(error)
        else:
            write_text(
                f"# dpone Airflow self-service certification\n\n- error: `{exc.code}`\n- message: {error['message']}\n"
            )
        return 2
    if args.format == "json":
        write_json(report.to_dict())
    else:
        write_text(report.to_markdown())
    return _certify_exit_code(
        has_input_failures=report.has_input_failures,
        overall_pass=report.overall_status == "PASS",
        allow_unverified=bool(getattr(args, "allow_unverified", False)),
    )


def _certify_exit_code(*, has_input_failures: bool, overall_pass: bool, allow_unverified: bool) -> int:
    """Exit 0 only for PASS, or for publish-only UNVERIFIED when explicitly allowed."""

    if has_input_failures:
        return 2
    if overall_pass:
        return 0
    return 0 if allow_unverified else 1
