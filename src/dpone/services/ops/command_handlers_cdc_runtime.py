from __future__ import annotations

import argparse
import logging

from .command_handlers_cdc_context import ops_catalog
from .command_helpers import _emit


def _ops(ctx: object):
    return ops_catalog(ctx)


def cmd_cdc_resync_execute(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .cdc()
        .resync_execution()
        .execute(
            output_dir=args.output_dir,
            resync_plan_json=args.resync_plan_json,
            source=args.source,
            sink=args.sink,
            backend=args.backend,
            pipeline_name=args.pipeline_name,
            source_schema=args.source_schema,
            source_table=args.source_table,
            target_dataset=args.target_dataset,
            unique_key=tuple(args.unique_key or ()),
            mode=args.mode,
            sink_connection_id=args.sink_connection_id,
            credentials_source=args.credentials_source,
            credentials_mount_point=args.credentials_mount_point,
            credentials_path=args.credentials_path,
            max_actions=args.max_actions,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_cdc_schema_evolution_evidence(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .cdc()
        .schema_evolution_evidence()
        .evaluate(
            output_dir=args.output_dir,
            handoff_json=args.handoff_json,
            apply_certification_json=args.apply_certification_json,
            observability_json=args.observability_json,
            recovery_json=args.recovery_json,
            schema_change_json=args.schema_change_json,
            policy_json=args.policy_json,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_cdc_schema_apply(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .cdc()
        .schema_apply()
        .apply(
            output_dir=args.output_dir,
            schema_change_json=args.schema_change_json,
            sink=args.sink,
            target_dataset=args.target_dataset,
            mode=args.mode,
            sink_connection_id=args.sink_connection_id,
            credentials_source=args.credentials_source,
            credentials_mount_point=args.credentials_mount_point,
            credentials_path=args.credentials_path,
            cdc_dataset=args.cdc_dataset,
            unique_key=tuple(args.unique_key or ()),
            columns=tuple(args.column or ()),
            typed_refresh=args.typed_refresh,
            require_approval=args.require_approval,
            fail_on_parse_errors=args.fail_on_parse_errors,
            max_parse_error_ratio=args.max_parse_error_ratio,
            quarantine_dataset=args.quarantine_dataset,
            schema_drift_mode=args.schema_drift_mode,
            quality_sample_limit=args.quality_sample_limit,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_cdc_promotion_gate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .cdc()
        .promotion_gate()
        .evaluate(
            output_dir=args.output_dir,
            apply_certification_json=args.apply_certification_json,
            handoff_json=args.handoff_json,
            observability_json=args.observability_json,
            recovery_json=args.recovery_json,
            schema_evolution_json=args.schema_evolution_json,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_cdc_runtime_run(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .cdc()
        .runtime_run()
        .run(
            output_dir=args.output_dir,
            source=args.source,
            sink=args.sink,
            backend=args.backend,
            pipeline_name=args.pipeline_name,
            source_schema=args.source_schema,
            source_table=args.source_table,
            target_dataset=args.target_dataset,
            unique_key=tuple(args.unique_key or ()),
            mode=args.mode,
            events_json=args.events_json,
            checkpoint_json=args.checkpoint_json,
            source_connection_id=args.source_connection_id,
            sink_connection_id=args.sink_connection_id,
            credentials_source=args.credentials_source,
            credentials_mount_point=args.credentials_mount_point,
            credentials_path=args.credentials_path,
            state_schema=args.state_schema,
            state_table=args.state_table,
            capture_instance=args.capture_instance,
            change_tracking_keys=tuple(args.change_tracking_key or ()),
            max_changes=args.max_changes,
            poison_mode=args.poison_mode,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1
