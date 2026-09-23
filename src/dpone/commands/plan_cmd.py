from __future__ import annotations

import argparse
import logging

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.commands.plan_output_render import _render_md, _render_text
from dpone.manifest.errors import ManifestConfigurationError
from dpone.readiness.managed import ExecutionPlanService


def cmd_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    try:
        payload = ExecutionPlanService().plan_manifest(
            args.path,
            selector=args.selector,
            apply_safe_schema=bool(args.apply_safe_schema),
            explain_strategy=bool(args.explain_strategy),
        )
    except ValueError as error:
        code = str(error)
        if (
            code.startswith(("mssql_native.transport_invalid:", "mssql_native.transport_required:"))
            or code == "mssql_native.transport_requires_native_route"
        ):
            raise ManifestConfigurationError(
                f"{code}: source.options.native_transfer.execution.native_chunks.transport "
                "requires backend: mssql_python|mssql_sqlclient, input: rows|arrow, and integer "
                "max_worker_address_space_bytes in 67108864..17179869184 for mssql_python "
                "or 8589934592..17179869184 for mssql_sqlclient; "
                "max_input_batch_bytes is SqlClient-only, in 1048576..268435456; "
                "use documented finite bounds and operation_timeout_seconds >= startup_timeout_seconds. "
                "Use typed_binary/mssql_native with bounded_stream on ClickHouse -> MSSQL; "
                "omit transport to keep BCP."
            ) from error
        if code == "mssql_native.source_read_invalid":
            raise ManifestConfigurationError(
                f"{code}: source.options.native_transfer.source_read must contain only "
                "mode: raw_single_query; omit source_read to preserve legacy admission."
            ) from error
        if code not in (
            "mssql_native.invalid_limit:encoding_parallelism",
            "mssql_native.invalid_limit:import_parallelism",
        ):
            raise
        field = code.split(":", 1)[1]
        raise ManifestConfigurationError(
            f"{code}: source.options.native_transfer.execution.native_chunks.{field} "
            "must be an integer in 1..64; omit the field to use chunking.parallelism."
        ) from error
    if args.format == "json":
        write_json(payload)
    elif args.format == "md":
        write_text(_render_md(payload))
    else:
        write_text(_render_text(payload))
    publication = payload.get("publication") if isinstance(payload, dict) else None
    blocked = (
        isinstance(publication, dict) and publication.get("requested") is True and bool(publication.get("blockers"))
    )
    return 1 if blocked else 0


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("plan", help="Render dry-run execution plan for a manifest process")
    parser.add_argument("path")
    parser.add_argument("--selector")
    parser.add_argument("--apply-safe-schema", action="store_true")
    parser.add_argument("--explain-strategy", action="store_true")
    parser.add_argument("--format", choices=["text", "json", "md"], default="text")
    return parser
