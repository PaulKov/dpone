from __future__ import annotations

import argparse
import logging

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.contracts.mssql_object_name import safe_mssql_identifier
from dpone.readiness.managed import ManagedRenderer, StateInspectorService
from dpone.readiness.mssql_transaction_catalog_ddl import (
    MssqlTransactionCatalogDdlService,
)


def _emit(payload: dict, fmt: str) -> None:
    if fmt == "json":
        write_json(payload)
    elif fmt == "md":
        write_text(ManagedRenderer.render_markdown("dpone state", payload))
    else:
        write_text(ManagedRenderer.render_text("dpone state", payload))


def cmd_state_inspect(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    _emit(StateInspectorService().inspect(args.backend, args.state_type, args.identity), args.format)
    return 0


def cmd_state_reset(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    _emit(StateInspectorService().reset(args.backend, args.state_type, args.identity, yes=bool(args.yes)), args.format)
    return 0


def cmd_state_export(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    _emit(StateInspectorService().export(args.backend, args.state_type, args.identity), args.format)
    return 0


def cmd_state_replay_from(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    _emit(
        StateInspectorService().replay_from(
            args.backend, args.state_type, args.identity, args.offset, yes=bool(args.yes)
        ),
        args.format,
    )
    return 0


def cmd_state_rewind(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = StateInspectorService().rewind(
        args.backend,
        args.state_type,
        args.identity,
        to=args.to,
        reason=args.reason or "",
        yes=bool(args.yes),
        approved_by=args.approved_by or "",
    )
    if args.evidence_output:
        _write_evidence(args.evidence_output, payload)
    _emit(payload, args.format)
    return 0 if payload["mode"] == "execute" or not args.strict else 1


def _write_evidence(path: str, payload: dict) -> None:
    import json
    from pathlib import Path

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def cmd_state_compare(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    _emit(StateInspectorService().compare(args.backend, args.state_type, args.left, args.right), args.format)
    return 0


def cmd_state_render_mssql_transaction_ddl(
    args: argparse.Namespace,
    *,
    ctx: object,
    logger: logging.Logger,
) -> int:
    """Render the exact external four-object governance catalog to stdout."""

    del ctx, logger
    write_text(
        MssqlTransactionCatalogDdlService.render(
            database=args.database,
            schema=args.schema,
            upgrade_from=args.upgrade_from,
        )
    )
    return 0


def _common(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument("--backend", required=True, choices=["bigquery", "postgres", "mssql"])
    parser.add_argument("--state-type", required=True, choices=["xmin", "kafka_offsets", "cdc_offsets", "run_state"])
    parser.add_argument("--format", choices=["text", "json", "md"], default="text")
    return parser


def register_inspect_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = _common(subparsers.add_parser("inspect", help="Inspect state without mutating it"))
    parser.add_argument("identity")
    return parser


def register_reset_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = _common(subparsers.add_parser("reset", help="Preview or execute a state reset"))
    parser.add_argument("identity")
    parser.add_argument("--yes", action="store_true")
    return parser


def register_export_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = _common(subparsers.add_parser("export", help="Export state records"))
    parser.add_argument("identity")
    return parser


def register_replay_from_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = _common(subparsers.add_parser("replay-from", help="Preview or execute replay from an offset"))
    parser.add_argument("identity")
    parser.add_argument("--offset", required=True)
    parser.add_argument("--yes", action="store_true")
    return parser


def register_rewind_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = _common(subparsers.add_parser("rewind", help="Preview or execute an approved watermark rewind"))
    parser.add_argument("identity")
    parser.add_argument("--to", required=True, help="Target watermark value to rewind to")
    parser.add_argument("--reason", help="Business reason recorded in the rewind evidence")
    parser.add_argument("--approved-by", dest="approved_by", help="Approver identity required for execution")
    parser.add_argument("--yes", action="store_true", help="Confirm the destructive rewind")
    parser.add_argument("--evidence-output", dest="evidence_output", help="Write rewind evidence JSON to this path")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero while the rewind stays in preview mode (for CI gating)",
    )
    return parser


def register_compare_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = _common(subparsers.add_parser("compare", help="Compare two state identities"))
    parser.add_argument("left")
    parser.add_argument("right")
    return parser


def register_render_mssql_transaction_ddl_parser(
    subparsers: argparse._SubParsersAction,
) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "render-mssql-transaction-ddl",
        help="Render the exact external four-object MSSQL governance catalog",
    )
    parser.add_argument(
        "--database",
        required=True,
        type=_safe_mssql_identifier,
        help="State catalog database",
    )
    parser.add_argument(
        "--schema",
        required=True,
        type=_safe_mssql_identifier,
        help="State catalog schema",
    )
    parser.add_argument(
        "--upgrade-from",
        type=int,
        choices=(1,),
        default=None,
        help="Render the locked, evidence-preserving v1-to-v2 migration instead of a fresh catalog",
    )
    return parser


def _safe_mssql_identifier(value: str) -> str:
    if not safe_mssql_identifier(value):
        raise argparse.ArgumentTypeError("must be a safe unquoted SQL Server identifier")
    return value
