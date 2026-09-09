from __future__ import annotations

import argparse
import logging

from dpone.ops.service_catalog import ReleaseOpsCatalog

from .command_handlers_cdc_context import OpsServiceCatalog as OpsServiceCatalog
from .command_handlers_cdc_context import ops_catalog
from .command_handlers_cdc_materialization import (
    cmd_cdc_materialize_clickhouse as cmd_cdc_materialize_clickhouse,
)
from .command_handlers_cdc_materialization import (
    cmd_cdc_materialize_clickhouse_typed as cmd_cdc_materialize_clickhouse_typed,
)
from .command_handlers_cdc_runtime import (
    cmd_cdc_promotion_gate as cmd_cdc_promotion_gate,
)
from .command_handlers_cdc_runtime import (
    cmd_cdc_resync_execute as cmd_cdc_resync_execute,
)
from .command_handlers_cdc_runtime import (
    cmd_cdc_runtime_run as cmd_cdc_runtime_run,
)
from .command_handlers_cdc_runtime import (
    cmd_cdc_schema_apply as cmd_cdc_schema_apply,
)
from .command_handlers_cdc_runtime import (
    cmd_cdc_schema_evolution_evidence as cmd_cdc_schema_evolution_evidence,
)
from .command_helpers import _emit, _parse_artifacts


def _ops(ctx: object) -> ReleaseOpsCatalog:
    return ops_catalog(ctx)


def cmd_cdc_handoff(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .cdc()
        .handoff()
        .evaluate(
            output_dir=args.output_dir,
            source=args.source,
            sink=args.sink,
            strategy=args.strategy,
            source_dataset=args.source_dataset,
            target_dataset=args.target_dataset,
            artifacts=_parse_artifacts(args.artifact or []),
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_cdc_apply_certification(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .cdc()
        .apply_certification()
        .certify(
            output_dir=args.output_dir,
            source=args.source,
            sink=args.sink,
            strategy=args.strategy,
            source_dataset=args.source_dataset,
            target_dataset=args.target_dataset,
            fixture_json=args.fixture_json,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_cdc_observability_evidence(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .cdc()
        .observability_evidence()
        .evaluate(
            output_dir=args.output_dir,
            handoff_json=args.handoff_json,
            apply_certification_json=args.apply_certification_json,
            metrics_json=args.metrics_json,
            slo_json=args.slo_json,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_cdc_recovery_evidence(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .cdc()
        .recovery_evidence()
        .evaluate(
            output_dir=args.output_dir,
            handoff_json=args.handoff_json,
            apply_certification_json=args.apply_certification_json,
            observability_json=args.observability_json,
            scenario_json=args.scenario_json,
            policy_json=args.policy_json,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_cdc_quarantine_inspect(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .cdc()
        .quarantine_inspection()
        .inspect(
            output_dir=args.output_dir,
            quarantine_json=args.quarantine_json,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_cdc_replay_execute(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .cdc()
        .replay_execution()
        .execute(
            output_dir=args.output_dir,
            quarantine_json=args.quarantine_json,
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
            max_events=args.max_events,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_cdc_compare_repair(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .cdc()
        .compare_repair()
        .compare(
            output_dir=args.output_dir,
            source=args.source,
            sink=args.sink,
            backend=args.backend,
            pipeline_name=args.pipeline_name,
            source_schema=args.source_schema,
            source_table=args.source_table,
            target_dataset=args.target_dataset,
            unique_key=tuple(args.unique_key or ()),
            columns=tuple(args.column or ()),
            mode=args.mode,
            source_rows_json=args.source_rows_json,
            target_rows_json=args.target_rows_json,
            source_connection_id=args.source_connection_id,
            sink_connection_id=args.sink_connection_id,
            credentials_source=args.credentials_source,
            credentials_mount_point=args.credentials_mount_point,
            credentials_path=args.credentials_path,
            max_rows=args.max_rows,
            max_diffs=args.max_diffs,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_cdc_repair_execute(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .cdc()
        .repair_execution()
        .execute(
            output_dir=args.output_dir,
            repair_plan_json=args.repair_plan_json,
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


def cmd_cdc_retention_check(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .cdc()
        .retention_check()
        .evaluate(
            output_dir=args.output_dir,
            source=args.source,
            sink=args.sink,
            backend=args.backend,
            pipeline_name=args.pipeline_name,
            source_schema=args.source_schema,
            source_table=args.source_table,
            target_dataset=args.target_dataset,
            unique_key=tuple(args.unique_key or ()),
            committed_offset=args.committed_offset,
            mode=args.mode,
            min_available_offset=args.min_available_offset,
            high_watermark=args.high_watermark,
            current_offset=args.current_offset,
            retention_seconds=args.retention_seconds,
            source_connection_id=args.source_connection_id,
            credentials_source=args.credentials_source,
            credentials_mount_point=args.credentials_mount_point,
            credentials_path=args.credentials_path,
            capture_instance=args.capture_instance,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_cdc_resync_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .cdc()
        .resync_plan()
        .plan(
            output_dir=args.output_dir,
            retention_report_json=args.retention_report_json,
            rows_json=args.rows_json,
            source=args.source,
            sink=args.sink,
            backend=args.backend,
            pipeline_name=args.pipeline_name,
            source_schema=args.source_schema,
            source_table=args.source_table,
            target_dataset=args.target_dataset,
            unique_key=tuple(args.unique_key or ()),
            max_rows=args.max_rows,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1
