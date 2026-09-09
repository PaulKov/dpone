from __future__ import annotations

import argparse
import logging

from .command_handlers_release_context import ReleaseOpsCatalog, release_ops
from .command_helpers import _emit, _parse_artifacts


def _ops(ctx: object) -> ReleaseOpsCatalog:
    return release_ops(ctx)


def cmd_route_release_finalize(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .route_release_finalize()
        .finalize(
            output_dir=args.output_dir,
            release=args.release,
            profile=args.profile,
            bundle_roots=tuple(args.bundle_root or ()),
            route_bundles=_parse_artifacts(args.route_bundle or []),
            required_routes=tuple(args.route) if args.route else tuple(),
            history_dir=args.history_dir,
            baseline_json=args.baseline_json,
            max_age_hours=args.max_age_hours,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


__all__ = ["cmd_route_release_finalize"]
