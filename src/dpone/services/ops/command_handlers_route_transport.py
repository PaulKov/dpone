from __future__ import annotations

import argparse
import logging

from dpone.ops.route_transport_certification import RouteTransportCertificationService

from .command_helpers import _emit


def cmd_route_transport_certification(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    service = getattr(ctx, "route_transport_certification_service", None) or RouteTransportCertificationService()
    report = service.certify(
        manifest=args.manifest,
        artifact_dir=args.artifact_dir,
        profile=args.profile,
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


__all__ = ["cmd_route_transport_certification"]
