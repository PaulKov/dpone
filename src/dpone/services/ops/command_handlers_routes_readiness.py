"""Route readiness, schema, repair, and data quality command handlers."""

from __future__ import annotations

import argparse
import logging

from .command_handlers_routes_common import ops_catalog, read_rows
from .command_helpers import _emit, _parse_artifacts


def cmd_route_readiness(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        ops_catalog(ctx)
        .route_readiness()
        .evaluate(
            output_dir=args.output_dir,
            source=args.source,
            sink=args.sink,
            strategy=args.strategy,
            artifacts=_parse_artifacts(args.artifact or []),
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_route_schema_evolution(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        ops_catalog(ctx)
        .route_schema_evolution()
        .evaluate(
            output_dir=args.output_dir,
            source=args.source,
            sink=args.sink,
            strategy=args.strategy,
            schema_evolution_json=args.schema_evolution_json,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_route_reconciliation_repair(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        ops_catalog(ctx)
        .route_reconciliation_repair()
        .evaluate(
            output_dir=args.output_dir,
            source=args.source,
            sink=args.sink,
            strategy=args.strategy,
            source_rows=read_rows(args.source_rows_json),
            target_rows=read_rows(args.target_rows_json),
            key_columns=tuple(args.key),
            compare_columns=tuple(args.compare_column) if args.compare_column else None,
            delete_column=args.delete_column,
            source_boundary=args.source_boundary,
            target_boundary=args.target_boundary,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_route_data_quality(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        ops_catalog(ctx)
        .route_data_quality()
        .evaluate(
            output_dir=args.output_dir,
            source=args.source,
            sink=args.sink,
            strategy=args.strategy,
            artifacts=_parse_artifacts(args.artifact or []),
            required_evidence=tuple(args.require) if args.require else tuple(),
            min_score=args.min_score,
            warning_score=args.warning_score,
            max_exception_ratio=args.max_exception_ratio,
            max_quarantine_rows=args.max_quarantine_rows,
            max_exception_age_hours=args.max_exception_age_hours,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


__all__ = [
    "cmd_route_data_quality",
    "cmd_route_readiness",
    "cmd_route_reconciliation_repair",
    "cmd_route_schema_evolution",
]
