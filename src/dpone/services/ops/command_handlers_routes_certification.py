"""Route release certification and release-candidate command handlers."""

from __future__ import annotations

import argparse
import logging

from .command_handlers_routes_common import ops_catalog
from .command_helpers import _emit, _parse_artifacts
from .facades import RouteCertificationMatrixError


def cmd_route_release_gate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        ops_catalog(ctx)
        .route_release_gate()
        .evaluate(
            output_dir=args.output_dir,
            release=args.release,
            source=args.source,
            sink=args.sink,
            strategy=args.strategy,
            artifacts=_parse_artifacts(args.artifact or []),
            required_evidence=tuple(args.require) if args.require else tuple(),
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_route_live_certification(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        ops_catalog(ctx)
        .route_live_certification()
        .build(
            output_dir=args.output_dir,
            release=args.release,
            source=args.source,
            sink=args.sink,
            strategy=args.strategy,
            profile=args.profile,
            row_count=args.row_count,
            artifacts=_parse_artifacts(args.artifact or []),
            required_evidence=tuple(args.require) if args.require else tuple(),
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_route_certify(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    try:
        report = (
            ops_catalog(ctx)
            .route_certify()
            .certify(
                output_dir=args.output_dir,
                release=args.release,
                source=args.source,
                sink=args.sink,
                strategy=args.strategy,
                profile=args.profile,
                artifacts=_parse_artifacts(args.artifact or []),
                required_evidence=tuple(args.require) if args.require else tuple(),
                release_set=args.release_set,
            )
        )
    except RouteCertificationMatrixError as exc:
        payload = {
            "schema": "dpone.error.v1",
            "code": exc.code,
            "stage": "route_certification_bundle",
            "severity": "error",
            "message": " ".join(str(exc).split())[:500],
            "fixes": [],
        }
        _emit(
            payload,
            f"# Route certify\n\n- error: `{exc.code}`\n- message: {payload['message']}\n",
            args.format,
        )
        return 2
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_route_certify_release(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        ops_catalog(ctx)
        .route_certify_release()
        .evaluate(
            output_dir=args.output_dir,
            release=args.release,
            profile=args.profile,
            route_bundles=_parse_artifacts(args.route_bundle or []),
            required_routes=tuple(args.route) if args.route else tuple(),
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_route_rc_orchestrator(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        ops_catalog(ctx)
        .route_rc_orchestrator()
        .run(
            output_dir=args.output_dir,
            release=args.release,
            source=args.source,
            sink=args.sink,
            strategy=args.strategy,
            profile=args.profile,
            row_count=args.row_count,
            artifacts=_parse_artifacts(args.artifact or []),
            required_evidence=tuple(args.require) if args.require else tuple(),
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_route_rc_execute(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        ops_catalog(ctx)
        .route_rc_executor()
        .execute(
            orchestration_json=args.orchestration_json,
            output_dir=args.output_dir,
            execute=args.execute,
            timeout_seconds=args.timeout_seconds,
            max_attempts=args.max_attempts,
            extra_redactions=tuple(args.redact or ()),
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_route_certification_pack(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        ops_catalog(ctx)
        .route_certification_pack()
        .build(
            output_dir=args.output_dir,
            source=args.source,
            sink=args.sink,
            strategy=args.strategy,
            artifacts=_parse_artifacts(args.artifact or []),
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


__all__ = [
    "cmd_route_certification_pack",
    "cmd_route_certify",
    "cmd_route_certify_release",
    "cmd_route_live_certification",
    "cmd_route_rc_execute",
    "cmd_route_rc_orchestrator",
    "cmd_route_release_gate",
]
