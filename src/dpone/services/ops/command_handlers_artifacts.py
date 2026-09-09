from __future__ import annotations

import argparse
import json
import logging

from dpone.ops.service_catalog import ArtifactOpsCatalog, OpsServiceCatalog

from .command_helpers import _emit, _parse_artifacts, _parse_bool_checks, _parse_dataset_refs, _parse_thresholds


def _ops(ctx: object) -> ArtifactOpsCatalog:
    injected = getattr(ctx, "ops_services", None)
    if isinstance(injected, ArtifactOpsCatalog):
        return injected
    if isinstance(injected, OpsServiceCatalog):
        return injected.artifacts
    return OpsServiceCatalog.default().artifacts


def cmd_docs_publish_pack(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .docs_publish_pack()
        .build(
            output_dir=args.output_dir,
            release=args.release,
            artifacts=_parse_artifacts(args.artifact),
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_manifest_bundle(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .manifest_bundle()
        .build(
            output_dir=args.output_dir,
            bundle_id=args.bundle_id,
            files=_parse_artifacts(args.file),
            redact=args.redact,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_runbook_pack(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .runbook_pack()
        .build(
            output_dir=args.output_dir,
            runbook_id=args.runbook_id,
            title=args.title,
            artifacts=_parse_artifacts(args.artifact),
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_run_registry(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .run_registry()
        .record(
            output_dir=args.output_dir,
            run_result_path=args.run_result,
            artifacts=_parse_artifacts(args.artifact),
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_lineage_export(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .openlineage_export()
        .export(
            output_dir=args.output_dir,
            run_registry_entry_path=args.run_registry_entry,
            namespace=args.namespace,
            input_datasets=_parse_dataset_refs(args.input or []),
            output_datasets=_parse_dataset_refs(args.output or []),
            event_type=args.event_type,
            airflow_evidence_bundle_path=getattr(args, "airflow_evidence_bundle", None),
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_benchmark_baseline(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .benchmark_baseline()
        .evaluate(
            output_dir=args.output_dir,
            metrics=json.loads(args.metrics_json),
            baseline=json.loads(args.baseline_json),
            allowed_regression_ratio=args.allowed_regression_ratio,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_dbt_lineage(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .dbt_lineage()
        .export(
            output_dir=args.output_dir,
            manifest_path=args.manifest,
            run_results_path=args.run_results,
            run_registry_entry_path=args.run_registry_entry,
            namespace=args.namespace,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_certification_pack(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .connector_certification_pack()
        .build(
            output_dir=args.output_dir,
            pack_id=args.pack_id,
            artifacts=_parse_artifacts(args.artifact or []),
            required=tuple(args.require or ["certification_report"]),
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_recovery_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .runtime_recovery()
        .plan(
            state_dir=args.state_dir,
            lock_dir=args.lock_dir,
            load_package_dir=args.load_package_dir,
            output_dir=args.output_dir,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_reconcile(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .reconciliation()
        .reconcile(
            output_dir=args.output_dir,
            source_rows=json.loads(args.source_rows_json),
            target_rows=json.loads(args.target_rows_json),
            key_columns=tuple(item.strip() for item in args.key.split(",") if item.strip()),
            compare_columns=tuple(item.strip() for item in args.compare_columns.split(",") if item.strip())
            if args.compare_columns
            else None,
            delete_column=args.delete_column,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_observability_pack(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .observability_pack()
        .build(
            output_dir=args.output_dir,
            service_name=args.service_name,
            dashboard_title=args.dashboard_title,
            alert_thresholds=_parse_thresholds(args.alert or []),
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_deploy_render(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .deployment_profiles()
        .render(
            output_dir=args.output_dir,
            target=args.target,
            manifest_path=args.manifest,
            selector=args.selector,
            image=args.image,
            schedule=args.schedule,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_staging_evidence(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .staging_evidence()
        .build(
            output_dir=args.output_dir,
            manifest_path=args.manifest,
            sink=args.sink,
            target_table=args.target_table,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_catalog_publish(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .catalog_publication()
        .publish(
            output_dir=args.output_dir,
            run_registry_entry_path=args.run_registry_entry,
            input_datasets=_parse_dataset_refs(args.input or []),
            output_datasets=_parse_dataset_refs(args.output or []),
            namespace=args.namespace,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_live_certification_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .live_certification()
        .build(
            output_dir=args.output_dir,
            profile=args.profile,
            row_count=args.row_count,
            include_vendor_live=args.include_vendor_live,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_managed_credentials_readiness(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .managed_credentials_readiness()
        .check(
            output_dir=args.output_dir,
            profile=args.profile,
            required_env=tuple(args.required_env or ()),
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_benchmark_slo_gate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .benchmark_slo_gate()
        .evaluate(
            output_dir=args.output_dir,
            metrics=json.loads(args.metrics_json),
            baseline=json.loads(args.baseline_json),
            objectives=json.loads(args.objectives_json),
            allowed_regression_ratio=args.allowed_regression_ratio,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_performance_certification(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .performance_certification()
        .certify(
            output_dir=args.output_dir,
            profile=args.profile,
            row_count=args.row_count,
            metrics=json.loads(args.metrics_json),
            minimums=json.loads(args.minimum_json or "{}"),
            maximums=json.loads(args.maximum_json or "{}"),
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_live_state_reconciliation(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .live_state_reconciliation()
        .certify(
            output_dir=args.output_dir,
            profile=args.profile,
            artifacts=_parse_artifacts(args.artifact or []),
            required=tuple(args.require or ["state", "reconciliation"]),
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_release_evidence_pack(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .release_evidence_pack()
        .build(
            output_dir=args.output_dir,
            release=args.release,
            profile=args.profile,
            artifacts=_parse_artifacts(args.artifact or []),
            required=tuple(args.require) if args.require is not None else None,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_pre_release_checklist(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .pre_release_checklist()
        .build(
            output_dir=args.output_dir,
            release=args.release,
            release_type=args.release_type,
            checks=_parse_bool_checks(args.check or []),
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1
