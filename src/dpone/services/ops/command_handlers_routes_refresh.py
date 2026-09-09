"""Route refresh planning, execution, and verification command handlers."""

from __future__ import annotations

import argparse
import logging

from .command_handlers_routes_common import ops_catalog
from .command_helpers import _emit, _parse_artifacts, _parse_type_hints


def cmd_route_refresh_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        ops_catalog(ctx)
        .route_refresh_plan()
        .plan(
            output_dir=args.output_dir,
            source=args.source,
            sink=args.sink,
            strategy=args.strategy,
            dataset=args.dataset,
            reason=args.reason,
            window_kind=args.window_kind,
            start=args.start,
            end=args.end,
            chunk_size=args.chunk_size,
            max_chunks=args.max_chunks,
            partition=args.partition,
            current_state=args.current_state,
            target_state=args.target_state,
            destructive=args.destructive,
            require_approval=args.require_approval,
            approval_granted=args.approval_granted,
            artifacts=_parse_artifacts(args.artifact or []),
            required_evidence=tuple(args.require) if args.require else tuple(),
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_route_refresh_execute(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        ops_catalog(ctx)
        .route_refresh_execute(
            executor_backend=args.executor,
            executor_config_json=args.executor_config_json,
        )
        .execute(
            route_refresh_plan_json=args.route_refresh_plan_json,
            output_dir=args.output_dir,
            runner_id=args.runner_id,
            execute=args.execute,
            source=args.source,
            sink=args.sink,
            strategy=args.strategy,
            max_chunks=args.max_chunks,
            artifacts=_parse_artifacts(args.artifact or []),
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_route_refresh_capture_snapshots(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        ops_catalog(ctx)
        .route_refresh_capture_snapshots(
            source_rows_json=args.source_rows_json,
            sink_rows_json=args.sink_rows_json,
            executor_backend=args.executor,
            executor_config_json=args.executor_config_json,
        )
        .capture(
            route_refresh_execution_json=args.route_refresh_execution_json,
            output_dir=args.output_dir,
            runner_id=args.runner_id,
            key_columns=tuple(args.key),
            boundary_column=args.boundary_column,
            columns=tuple(args.column),
            type_hints=_parse_type_hints(args.type or []),
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_route_refresh_verify(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        ops_catalog(ctx)
        .route_refresh_verify(
            source_snapshot_json=args.source_snapshot_json,
            sink_snapshot_json=args.sink_snapshot_json,
        )
        .verify(
            route_refresh_execution_json=args.route_refresh_execution_json,
            output_dir=args.output_dir,
            runner_id=args.runner_id,
            key_columns=tuple(args.key),
            boundary_column=args.boundary_column,
            columns=tuple(args.column),
            type_hints=_parse_type_hints(args.type or []),
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


__all__ = [
    "cmd_route_refresh_capture_snapshots",
    "cmd_route_refresh_execute",
    "cmd_route_refresh_plan",
    "cmd_route_refresh_verify",
]
