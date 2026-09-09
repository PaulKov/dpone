"""Route execution ledger, state promotion, and supervisor command handlers."""

from __future__ import annotations

import argparse
import logging

from .command_handlers_routes_common import ops_catalog
from .command_helpers import _emit, _parse_artifacts


def cmd_route_run_supervisor(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        ops_catalog(ctx)
        .route_run_supervisor()
        .evaluate(
            output_dir=args.output_dir,
            source=args.source,
            sink=args.sink,
            strategy=args.strategy,
            run_id=args.run_id,
            dataset=args.dataset,
            manifest=args.manifest,
            run_mode=args.run_mode,
            artifacts=_parse_artifacts(args.artifact or []),
            required_evidence=tuple(args.require) if args.require else tuple(),
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_route_execution_ledger(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        ops_catalog(ctx)
        .route_execution_ledger(store_backend=args.store_backend, store_uri=args.store_uri)
        .record_step(
            output_dir=args.output_dir,
            source=args.source,
            sink=args.sink,
            strategy=args.strategy,
            dataset=args.dataset,
            run_id=args.run_id,
            stage=args.stage,
            status=args.status,
            runner_id=args.runner_id,
            source_boundary=args.source_boundary,
            sink_boundary=args.sink_boundary,
            artifact_paths=_parse_artifacts(args.artifact or []),
            idempotency_key=args.idempotency_key,
            lease_ttl_seconds=args.lease_ttl_seconds,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_route_state_promote(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        ops_catalog(ctx)
        .route_state_promotion(state_backend=args.state_backend, state_uri=args.state_uri)
        .promote(
            output_dir=args.output_dir,
            source=args.source,
            sink=args.sink,
            strategy=args.strategy,
            dataset=args.dataset,
            run_id=args.run_id,
            ledger_json=args.ledger_json,
            proposed_state=args.proposed_state,
            source_boundary=args.source_boundary,
            sink_boundary=args.sink_boundary,
            idempotency_key=args.idempotency_key,
            commit_token=args.commit_token,
            target=args.target,
            fencing_token=args.fencing_token,
            rows_applied=args.rows_applied,
            events_applied=args.events_applied,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


__all__ = [
    "cmd_route_execution_ledger",
    "cmd_route_run_supervisor",
    "cmd_route_state_promote",
]
