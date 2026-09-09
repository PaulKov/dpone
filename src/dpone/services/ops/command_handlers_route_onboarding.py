from __future__ import annotations

import argparse
import logging

from .command_handlers_release_context import ReleaseOpsCatalog, release_ops
from .command_helpers import _emit, _parse_artifacts


def _ops(ctx: object) -> ReleaseOpsCatalog:
    return release_ops(ctx)


def cmd_connection_doctor(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .onboarding.connection_doctor()
        .check(
            output_dir=args.output_dir,
            source=args.source,
            sink=args.sink,
            strategy=args.strategy,
            required_tools=tuple(args.tool or ()),
            optional_tools=tuple(args.optional_tool or ()),
            required_env=tuple(args.env or ()),
            optional_env=tuple(args.optional_env or ()),
            python_imports=tuple(args.python_import or ()),
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_source_discover(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .onboarding.source_discovery()
        .discover(
            output_dir=args.output_dir,
            source=args.source,
            schema_json=args.schema_json,
            dataset=args.dataset,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_route_bootstrap(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .onboarding.route_bootstrap()
        .bootstrap(
            output_dir=args.output_dir,
            source=args.source,
            sink=args.sink,
            strategy=args.strategy,
            dataset=args.dataset,
            source_discovery_json=args.source_discovery_json,
            manifest_id=args.manifest_id,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_route_doctor(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .onboarding.route_doctor()
        .diagnose(
            output_dir=args.output_dir,
            source=args.source,
            sink=args.sink,
            strategy=args.strategy,
            artifacts=_parse_artifacts(args.artifact or []),
            required_artifacts=tuple(args.require)
            if args.require
            else (
                "connection_doctor",
                "source_discovery",
                "route_bootstrap",
            ),
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


__all__ = [
    "cmd_connection_doctor",
    "cmd_route_bootstrap",
    "cmd_route_doctor",
    "cmd_source_discover",
]
