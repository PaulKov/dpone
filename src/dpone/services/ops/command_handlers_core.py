from __future__ import annotations

import argparse
import json
import logging
from importlib import import_module
from pathlib import Path

from dpone.ops.service_catalog import CoreOpsCatalog, OpsServiceCatalog

from .command_helpers import _emit, _markdown_table


def _ops(ctx: object) -> CoreOpsCatalog:
    injected = getattr(ctx, "ops_services", None)
    if isinstance(injected, CoreOpsCatalog):
        return injected
    if isinstance(injected, OpsServiceCatalog):
        return injected.core
    return OpsServiceCatalog.default().core


def cmd_certification_run(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .certification_harness()
        .run_mock_contract(
            artifact_dir=args.artifact_dir,
            source=args.source,
            sink=args.sink,
            strategy=args.strategy,
            row_count=args.row_count,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_artifact_index(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .artifact_index()
        .build(
            output_dir=args.output_dir,
            roots=args.root or [".dpone", "test_artifacts"],
            release=args.release,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_certification_history(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .certification_history()
        .record(
            history_dir=args.history_dir,
            release=args.release,
            current_report_path=args.current_report,
            previous_report_path=args.previous_report,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.status != "regression" else 1


def cmd_connector_badges(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .connector_badges()
        .generate(
            output_dir=args.output_dir,
            history_index_path=args.history_index,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_contract_check(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    rows = json.loads(args.rows_json)
    contract = json.loads(args.contract_json)
    report = _ops(ctx).data_contracts().evaluate(rows, contract, mode=args.mode)
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_quarantine_export(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    exported = _ops(ctx).quarantine(args.dir).export(run_id=args.run_id)
    payload = exported.to_dict()
    _emit(payload, _markdown_table("quarantine export", payload), args.format)
    return 0


def cmd_quarantine_replay(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    result = _ops(ctx).quarantine(args.dir).replay(run_id=args.run_id, yes=args.yes)
    errors = []
    if result.error_code:
        error_contract = import_module("dpone.readiness.error_contract")
        errors.append(
            error_contract.dpone_error(
                result.error_code,
                "Generic DLQ replay cannot apply records without a route-specific resolver and sink.",
                stage="dlq_replay_plan",
                fixes=[error_contract.manual_fix("configure_route_specific_dlq_replay_executor")],
                docs_url=error_contract.error_docs_url(result.error_code),
            )
        )
    payload = {
        "applied": result.applied,
        "replayed_rows": result.replayed_rows,
        "planned_rows": len(result.entries),
        "error_code": result.error_code,
        "errors": errors,
    }
    _emit(payload, _markdown_table("quarantine replay", payload), args.format)
    return 4 if result.error_code else 0


def cmd_package_start(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    package = _ops(ctx).load_packages(args.dir).start(run_id=args.run_id, target=args.target, chunk_id=args.chunk_id)
    _emit(package.to_dict(), _markdown_table("load package start", package.to_dict()), args.format)
    return 0


def cmd_package_commit(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    package = (
        _ops(ctx)
        .load_packages(args.dir)
        .mark_committed(
            args.load_id,
            rows_loaded=args.rows_loaded,
            state_after=json.loads(args.state_after_json),
        )
    )
    _emit(package.to_dict(), _markdown_table("load package commit", package.to_dict()), args.format)
    return 0


def cmd_rollback_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    plan = (
        _ops(ctx)
        .rollback_plan_service()
        .plan(sink=args.sink, target=args.target, load_id=args.load_id, strategy=args.strategy)
    )
    _emit(plan.to_dict(), plan.to_markdown(), args.format)
    return 0


def cmd_rollback_apply(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    payload = json.loads(Path(args.plan_json).read_text(encoding="utf-8"))
    plan = _ops(ctx).rollback_plan_from_payload(payload)
    result = _ops(ctx).rollback_plan_service().apply(plan, yes=args.yes)
    data = {"applied": result.applied, "planned_actions": result.planned_actions, "message": result.message}
    _emit(data, _markdown_table("rollback apply", data), args.format)
    return 0 if result.applied else 1


def cmd_rollback_execute(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .rollback_execution()
        .execute(
            sink=args.sink,
            target=args.target,
            load_id=args.load_id,
            strategy=args.strategy,
            yes=args.yes,
            require_backup=args.require_backup,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.applied or report.dry_run else 1


def cmd_marketplace(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    catalog = _ops(ctx).connector_marketplace().catalog()
    _emit(catalog.to_dict(), catalog.to_markdown(), args.format)
    return 0 if catalog.passed else 1


def cmd_evidence_bundle(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    bundle = (
        _ops(ctx)
        .evidence_bundle()
        .build(
            artifact_dir=args.artifact_dir,
            run_id=args.run_id,
            source=args.source,
            sink=args.sink,
            strategy=args.strategy,
            rows=json.loads(args.rows_json),
            contract=json.loads(args.contract_json),
            row_count=args.row_count,
        )
    )
    _emit(bundle.to_dict(), bundle.to_markdown(), args.format)
    return 0 if bundle.passed else 1


def cmd_evidence_chain(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    entry = (
        _ops(ctx)
        .evidence_chain()
        .append(
            chain_dir=args.chain_dir,
            release=args.release,
            artifact_index_path=args.artifact_index,
            previous_entry_path=args.previous_entry,
        )
    )
    _emit(entry.to_dict(), entry.to_markdown(), args.format)
    return 0 if entry.verified else 1
