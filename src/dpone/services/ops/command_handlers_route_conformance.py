from __future__ import annotations

import argparse
import logging
from typing import Any

from dpone.ops.routes.conformance_live_models import RouteConformanceLiveConfig
from dpone.ops.routes.conformance_models import RouteConformanceDatasetProfile

from .command_handlers_release_context import ReleaseOpsCatalog, release_ops
from .command_helpers import _emit, _parse_artifacts


def _ops(ctx: object) -> ReleaseOpsCatalog:
    return release_ops(ctx)


def cmd_route_conformance(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    command = args.route_conformance_command
    service = _ops(ctx).conformance.route_conformance()
    report: Any
    if command == "run":
        report = service.run(
            output_dir=args.output_dir,
            source=args.source,
            sink=args.sink,
            strategy=args.strategy,
            dataset_profile=_dataset_profile(args),
            min_rows=args.min_rows,
            min_columns=args.min_columns,
            require_schema_evolution=args.require_schema_evolution,
        )
    elif command == "live-run":
        live_report = (
            _ops(ctx)
            .conformance.route_conformance_live()
            .run(
                output_dir=args.output_dir,
                source=args.source,
                sink=args.sink,
                strategy=args.strategy,
                config=RouteConformanceLiveConfig(
                    adapter=args.adapter,
                    dataset=_dataset_profile(args),
                    min_rows=args.min_rows,
                    min_columns=args.min_columns,
                    require_schema_evolution=args.require_schema_evolution,
                    drift_mode=args.drift,
                ),
            )
        )
        _emit(live_report.to_dict(), live_report.to_markdown(), args.format)
        return 0 if live_report.passed else 1
    elif command == "summarize":
        report = service.summarize(
            output_dir=args.output_dir,
            artifacts=_parse_artifacts(args.artifact or []),
        )
    elif command == "release-gate":
        report = service.release_gate(
            output_dir=args.output_dir,
            release=args.release,
            artifacts=_parse_artifacts(args.artifact or []),
        )
    else:
        raise ValueError(f"Unsupported route-conformance command: {command}")
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def _dataset_profile(args: argparse.Namespace) -> RouteConformanceDatasetProfile:
    return RouteConformanceDatasetProfile(
        name=args.dataset_profile,
        row_count=args.rows,
        column_count=args.columns,
        chunk_size=args.chunk_size,
        include_nested=args.include_nested,
        include_schema_evolution=not args.no_schema_evolution,
        seed=args.seed,
    )


__all__ = ["cmd_route_conformance"]
