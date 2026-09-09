"""``dpone backfill`` command group.

Self-service workflow:

1. ``dpone backfill plan manifest.yml``            — review the chunk plan
2. ``dpone backfill run manifest.yml``             — dry-run (same as plan)
3. ``dpone backfill run manifest.yml --execute``   — load pending chunks
4. ``dpone backfill resume manifest.yml``          — continue after a failure
5. ``dpone backfill status manifest.yml``          — inspect ledger progress
6. ``dpone backfill retry-failed manifest.yml``    — retry failed chunks only

Window overrides (``--from/--to/--step``) let Airflow DAGs map
``data_interval_start``/``data_interval_end`` onto one chunked campaign.
"""

from __future__ import annotations

import argparse
import logging
from typing import Any

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.services.backfill_service import BackfillCommandService


def cmd_backfill_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = BackfillCommandService().plan(
        path=args.path,
        registry=args.registry,
        selector=args.selector,
        overrides=_overrides(args),
        advisor=bool(getattr(args, "advisor", False)),
        advisor_evidence_paths=tuple(getattr(args, "advisor_evidence", ()) or ()),
    )
    _emit(payload, args.format)
    return 0


def cmd_backfill_run(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = BackfillCommandService().run(
        path=args.path,
        registry=args.registry,
        selector=args.selector,
        overrides=_overrides(args),
        execute=bool(getattr(args, "execute", False)),
        run_id=args.run_id,
        dag_id=args.dag_id,
        execution_date=args.execution_date,
    )
    _emit(payload, args.format)
    if payload.get("executed"):
        return 0 if payload.get("passed") else 1
    return 0


def cmd_backfill_resume(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    args.execute = True
    return cmd_backfill_run(args, ctx=ctx, logger=logger)


def cmd_backfill_status(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = BackfillCommandService().status(
        path=args.path,
        registry=args.registry,
        selector=args.selector,
        overrides=_overrides(args),
    )
    _emit(payload, args.format)
    return 0


def cmd_backfill_retry_failed(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = BackfillCommandService().retry_failed(
        path=args.path,
        registry=args.registry,
        selector=args.selector,
        overrides=_overrides(args),
        run_id=args.run_id,
        dag_id=args.dag_id,
        execution_date=args.execution_date,
    )
    _emit(payload, args.format)
    return 0 if payload.get("operation_passed", payload.get("passed")) else 1


def cmd_backfill_cancel(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = BackfillCommandService().cancel(
        path=args.path,
        registry=args.registry,
        selector=args.selector,
        overrides=_overrides(args),
        reason=args.reason,
        requested_by=args.requested_by,
    )
    _emit(payload, args.format)
    return 0


def cmd_backfill_doctor(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = BackfillCommandService().doctor(
        path=args.path,
        registry=args.registry,
        selector=args.selector,
        overrides=_overrides(args),
    )
    _emit(payload, args.format)
    return 0 if payload.get("status") != "blocked" else 1


def _overrides(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "column": getattr(args, "column", None),
        "from": getattr(args, "window_from", None),
        "to": getattr(args, "window_to", None),
        "step": getattr(args, "step", None),
        "kind": getattr(args, "kind", None),
        "inner_mode": getattr(args, "inner_mode", None),
        "parallel_workers": getattr(args, "parallel_workers", None),
        "state_dir": getattr(args, "state_dir", None),
        "backfill_id": getattr(args, "backfill_id", None),
        "max_chunks": getattr(args, "max_chunks", None),
        "predicate_dialect": getattr(args, "predicate_dialect", None),
    }


def _emit(payload: dict[str, Any], fmt: str) -> None:
    if fmt == "json":
        write_json(payload)
        return
    write_text(_render_text(payload, markdown=fmt == "md"))


def _render_text(payload: dict[str, Any], *, markdown: bool) -> str:
    title = str(payload.get("kind") or "dpone backfill")
    lines = [f"# {title}" if markdown else title, ""]
    for key in (
        "manifest",
        "dataset",
        "run_key",
        "inner_mode",
        "parallel_workers",
        "state_path",
        "chunks_total",
        "operation_status",
        "operation_passed",
    ):
        if key in payload:
            lines.append(f"- {key}: {payload[key]}")
    counts = payload.get("counts") or {}
    if counts:
        lines.append("- counts: " + ", ".join(f"{status}={count}" for status, count in sorted(counts.items())))
    for action in payload.get("next_actions") or []:
        lines.append(f"- next: {action}")
    chunks = payload.get("chunks") or []
    if chunks:
        lines.append("")
        lines.append("| # | window | status |" if markdown else "chunks:")
        if markdown:
            lines.append("|---|---|---|")
        for chunk in chunks:
            row = f"{chunk['index']} | {chunk['start']} .. {chunk['end']} | {chunk.get('status', 'pending')}"
            lines.append(f"| {row} |" if markdown else f"  {row}")
    if "passed" in payload:
        lines.append("")
        lines.append(f"- passed: {payload['passed']}")
    lines.append("")
    return "\n".join(lines)


def _common(parser: argparse.ArgumentParser, *, with_run_args: bool = False) -> argparse.ArgumentParser:
    parser.add_argument("path", help="Path to a YAML manifest with sink.strategy.mode: backfill")
    parser.add_argument("--selector", help="Process selector inside a batch manifest")
    parser.add_argument("--column", help="Override backfill.chunk.column")
    parser.add_argument("--from", dest="window_from", help="Override backfill.chunk.from")
    parser.add_argument("--to", dest="window_to", help="Override backfill.chunk.to")
    parser.add_argument("--step", help="Override backfill.chunk.step (Nh/Nd/Nw/Nmo/Ny or integer)")
    parser.add_argument("--kind", choices=["date", "timestamp", "integer"], help="Override chunk boundary kind")
    parser.add_argument(
        "--inner-mode",
        dest="inner_mode",
        choices=["partition_replace", "replace", "incremental_merge", "full_refresh"],
        help="Override backfill.inner_mode",
    )
    parser.add_argument("--parallel-workers", dest="parallel_workers", type=int, help="Override parallel chunk workers")
    parser.add_argument("--max-chunks", dest="max_chunks", type=int, help="Override the chunk count guard")
    parser.add_argument("--state-dir", dest="state_dir", help="Ledger directory (default .dpone/backfill)")
    parser.add_argument("--backfill-id", dest="backfill_id", help="Pin an explicit campaign id / ledger key")
    parser.add_argument(
        "--predicate-dialect",
        dest="predicate_dialect",
        choices=["generic", "clickhouse", "mssql", "postgres"],
        help="Render generated chunk predicates for a SQL dialect",
    )
    parser.add_argument(
        "--registry",
        action="append",
        default=[],
        help="Path to a sources registry YAML (can be repeated)",
    )
    parser.add_argument("--format", choices=["text", "json", "md"], default="text")
    if with_run_args:
        parser.add_argument("--run-id", help="Explicit run id; defaults to process name")
        parser.add_argument("--dag-id")
        parser.add_argument("--execution-date")
    return parser


def register_plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = _common(subparsers.add_parser("plan", help="Build the deterministic chunk plan (no data movement)"))
    parser.add_argument("--advisor", action="store_true", help="Attach a conservative performance recommendation")
    parser.add_argument(
        "--advisor-evidence",
        action="append",
        default=[],
        help="JSON evidence artifact with load_steps/chunks/matrix_report; can be repeated",
    )
    return parser


def register_run_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = _common(
        subparsers.add_parser("run", help="Execute a chunked backfill (dry-run by default)"),
        with_run_args=True,
    )
    parser.add_argument("--execute", action="store_true", help="Actually load pending chunks (default: dry-run plan)")
    return parser


def register_resume_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _common(
        subparsers.add_parser("resume", help="Resume an interrupted campaign from the first non-committed chunk"),
        with_run_args=True,
    )


def register_status_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _common(subparsers.add_parser("status", help="Show ledger progress for a campaign"))


def register_retry_failed_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _common(
        subparsers.add_parser("retry-failed", help="Retry failed chunks only"),
        with_run_args=True,
    )


def register_cancel_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = _common(subparsers.add_parser("cancel", help="Request cancellation for an existing campaign"))
    parser.add_argument("--reason", required=True, help="Human-readable cancellation reason")
    parser.add_argument("--requested-by", required=True, help="Operator or automation requesting cancellation")
    return parser


def register_doctor_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _common(subparsers.add_parser("doctor", help="Inspect campaign state and next recommended action"))
