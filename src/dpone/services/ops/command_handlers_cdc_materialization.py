"""CDC materialization command handlers."""

from __future__ import annotations

import argparse
import logging

from .command_handlers_cdc_context import ops_catalog
from .command_helpers import _emit


def cmd_cdc_materialize_clickhouse(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        ops_catalog(ctx)
        .cdc()
        .materialization()
        .materialize(
            output_dir=args.output_dir,
            cdc_dataset=args.cdc_dataset,
            target_dataset=args.target_dataset,
            unique_key=tuple(args.unique_key or ()),
            sink_connection_id=args.sink_connection_id,
            credentials_source=args.credentials_source,
            credentials_mount_point=args.credentials_mount_point,
            credentials_path=args.credentials_path,
            delete_mode=args.delete_mode,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_cdc_materialize_clickhouse_typed(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        ops_catalog(ctx)
        .cdc()
        .typed_materialization()
        .materialize(
            output_dir=args.output_dir,
            cdc_dataset=args.cdc_dataset,
            target_dataset=args.target_dataset,
            unique_key=tuple(args.unique_key or ()),
            columns=tuple(args.column or ()),
            sink_connection_id=args.sink_connection_id,
            credentials_source=args.credentials_source,
            credentials_mount_point=args.credentials_mount_point,
            credentials_path=args.credentials_path,
            delete_mode=args.delete_mode,
            fail_on_parse_errors=args.fail_on_parse_errors,
            max_parse_error_ratio=args.max_parse_error_ratio,
            quarantine_dataset=args.quarantine_dataset,
            schema_drift_mode=args.schema_drift_mode,
            quality_sample_limit=args.quality_sample_limit,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1
