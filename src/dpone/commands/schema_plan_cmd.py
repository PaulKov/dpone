from __future__ import annotations

import argparse
import json
import logging
from typing import Literal, cast

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.commands.readiness_facade import build_readiness_service
from dpone.commands.schema_plan_rendering import (
    render_infer_md,
    render_infer_text,
    render_physical_diff_md,
    render_physical_diff_text,
    render_physical_md,
    render_physical_text,
    render_schema_explain_md,
    render_schema_explain_text,
    render_type_matrix_md,
    render_type_matrix_text,
)
from dpone.readiness.schema_evolution import SchemaEvolutionPolicy
from dpone.readiness.schema_workflows import ExpandContractService, SchemaApprovalService, load_columns
from dpone.services.schema_evolution_explain import SchemaEvolutionExplainService
from dpone.services.schema_type_matrix import PairTypeMatrixService


def cmd_schema_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload, ddl, has_breaking_changes = build_readiness_service().schema_plan(
        source_path=args.source,
        target_path=args.target,
        table=args.table,
        dialect=args.dialect,
        mode=args.mode,
        allow_drop=args.allow_drop,
        on_type_change=args.on_type_change,
        new_column_prefix=args.new_column_prefix,
        ddl_mode=args.ddl_mode,
        lock_timeout_seconds=args.lock_timeout_seconds,
        statement_timeout_seconds=args.statement_timeout_seconds,
        max_table_size_for_inline_ddl=args.max_table_size_for_inline_ddl,
    )
    if args.format == "json":
        write_json(payload)
    else:
        write_text("\n".join(ddl) + "\n")
    return 2 if has_breaking_changes and args.fail_on_breaking else 0


def cmd_schema_explain(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    policy = SchemaEvolutionPolicy(
        mode=cast(Literal["strict", "additive", "widening"], args.mode),
        allow_drop=bool(args.allow_drop),
        on_type_change=cast(Literal["fail", "new_column"], args.on_type_change),
    )
    payload = SchemaEvolutionExplainService().explain(
        source_system=args.source_system,
        sink_system=args.sink_system,
        source=load_columns(args.source),
        target=load_columns(args.target),
        policy=policy,
    )
    if args.format == "json":
        write_json(payload)
    elif args.format == "md":
        write_text(_render_schema_explain_md(payload))
    else:
        write_text(_render_schema_explain_text(payload))
    return 2 if payload["has_breaking_changes"] else 0


def cmd_schema_approve(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    record = SchemaApprovalService().approve(
        ledger_path=args.ledger,
        approver=args.approver,
        output_dir=args.output_dir,
    )
    payload = record.to_dict()
    if args.format == "json":
        write_json(payload)
    else:
        write_text(f"approved {payload['approval_id']} -> {payload['artifact_path']}\n")
    return 0


def cmd_schema_expand_contract(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    plan = ExpandContractService().build(
        source_columns=load_columns(args.source),
        target_columns=load_columns(args.target),
        table=args.table,
        dialect=args.dialect,
        output_dir=args.output_dir,
    )
    payload = plan.to_dict()
    if args.format == "json":
        write_json(payload)
    else:
        actions = payload.get("actions", [])
        if not isinstance(actions, list):
            actions = []
        write_text("\n".join(str(item) for item in actions) + "\n")
    return 0


def cmd_schema_infer(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = build_readiness_service().schema_infer(
        manifest_path=args.manifest,
        rows_path=args.rows,
        source_path=args.source,
    )
    if args.format == "json":
        write_json(payload)
    elif args.format == "md":
        write_text(_render_infer_md(payload))
    else:
        write_text(_render_infer_text(payload))
    return 0


def cmd_schema_physical_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = build_readiness_service().physical_plan(
        manifest_path=args.manifest,
        source_path=args.source,
        table=args.table,
        sink_type=args.sink,
    )
    if args.format == "json":
        write_json(payload)
    elif args.format == "md":
        write_text(_render_physical_md(payload))
    else:
        write_text(_render_physical_text(payload))
    return 0


def cmd_schema_physical_diff(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = build_readiness_service().physical_diff(
        manifest_path=args.manifest,
        source_path=args.source,
        table=args.table,
        sink_type=args.sink,
        actual_path=args.actual,
    )
    if args.format == "json":
        write_json(payload)
    elif args.format == "md":
        write_text(_render_physical_diff_md(payload))
    else:
        write_text(_render_physical_diff_text(payload))
    return 2 if payload.get("blockers") else 0


def cmd_schema_type_matrix(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    try:
        payload = PairTypeMatrixService().build(
            source=args.source,
            sink=args.sink,
            source_types=tuple(args.source_type or ()),
            type_fidelity=_load_type_fidelity(args.type_fidelity_json),
        )
    except ValueError as exc:
        write_text(f"ERROR: {exc}\n")
        return 2
    if args.format == "json":
        write_json(payload)
    elif args.format == "md":
        write_text(_render_type_matrix_md(payload))
    else:
        write_text(_render_type_matrix_text(payload))
    return 0


def _build_parser(subparsers: argparse._SubParsersAction, name: str) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(name, help="Plan safe schema evolution from source/target column JSON")
    parser.add_argument("--source", required=True, help="Path to source columns JSON")
    parser.add_argument("--target", required=True, help="Path to target columns JSON")
    parser.add_argument("--table", required=True, help="Qualified target table, e.g. dbo.orders")
    parser.add_argument("--dialect", default="mssql", choices=["mssql", "postgres", "clickhouse", "bigquery"])
    parser.add_argument("--mode", default="widening", choices=["strict", "additive", "widening"])
    parser.add_argument("--on-type-change", default="fail", choices=["fail", "new_column"])
    parser.add_argument("--new-column-prefix", default="__dpone__nc__")
    parser.add_argument(
        "--ddl-mode", default="online", choices=["online", "safe_window", "plan_only", "manual_approval"]
    )
    parser.add_argument("--lock-timeout-seconds", type=int)
    parser.add_argument("--statement-timeout-seconds", type=int)
    parser.add_argument("--max-table-size-for-inline-ddl", type=int)
    parser.add_argument(
        "--apply-safe", action="store_true", help="Alias for documenting runtime parity; CLI remains plan-only"
    )
    parser.add_argument(
        "--plan-only", action="store_true", help="Render plan without applying DDL (default CLI behavior)"
    )
    parser.add_argument("--allow-drop", action="store_true")
    parser.add_argument("--fail-on-breaking", action="store_true")
    parser.add_argument("--format", choices=["text", "json"], default="text")
    return parser


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _build_parser(subparsers, "schema-plan")


def register_plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _build_parser(subparsers, "plan")


def register_explain_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "explain",
        help="Explain schema evolution with source -> sink type-matrix diagnostics",
    )
    parser.add_argument("--source", required=True, help="Path to source columns JSON")
    parser.add_argument("--target", required=True, help="Path to target columns JSON")
    parser.add_argument("--source-system", required=True, choices=["mssql", "postgres", "clickhouse"])
    parser.add_argument("--sink-system", required=True, choices=["mssql", "postgres", "clickhouse"])
    parser.add_argument("--mode", default="widening", choices=["strict", "additive", "widening"])
    parser.add_argument("--on-type-change", default="fail", choices=["fail", "new_column"])
    parser.add_argument("--allow-drop", action="store_true")
    parser.add_argument("--format", choices=["text", "json", "md"], default="text")
    return parser


def register_approve_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("approve", help="Approve a schema change ledger artifact")
    parser.add_argument("--ledger", required=True, help="Path to schema change ledger JSON")
    parser.add_argument("--approver", required=True, help="Approver identifier")
    parser.add_argument("--output-dir", default=".dpone/schema-approvals/latest")
    parser.add_argument("--format", choices=["text", "json"], default="text")
    return parser


def register_expand_contract_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("expand-contract", help="Build an expand/backfill/contract schema migration plan")
    parser.add_argument("--source", required=True, help="Path to source columns JSON")
    parser.add_argument("--target", required=True, help="Path to target columns JSON")
    parser.add_argument("--table", required=True, help="Qualified target table")
    parser.add_argument("--dialect", default="postgres", choices=["mssql", "postgres", "clickhouse", "bigquery"])
    parser.add_argument("--output-dir", default=".dpone/schema-expand-contract/latest")
    parser.add_argument("--format", choices=["text", "json"], default="text")
    return parser


def register_infer_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("infer", help="Infer logical source schema from manifest, source JSON, or rows")
    parser.add_argument("--manifest", help="Manifest path with source.options.columns and sink options")
    parser.add_argument("--source", help="Optional source columns JSON")
    parser.add_argument("--rows", help="Optional JSON array or JSONL row sample file")
    parser.add_argument("--format", choices=["text", "json", "md"], default="text")
    return parser


def register_physical_plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("physical-plan", help="Plan target physical DDL from manifest schema contracts")
    parser.add_argument("--manifest", help="Manifest path with sink.options.physical_design")
    parser.add_argument("--source", help="Optional source columns JSON")
    parser.add_argument("--table", help="Qualified target table override")
    parser.add_argument(
        "--sink", choices=["mssql", "postgres", "clickhouse", "bigquery", "kafka"], help="Sink override"
    )
    parser.add_argument("--format", choices=["text", "json", "md"], default="text")
    return parser


def register_physical_diff_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("physical-diff", help="Compare desired physical design with actual target state")
    parser.add_argument("--actual", required=True, help="Actual physical design JSON")
    parser.add_argument("--manifest", help="Manifest path with sink.options.physical_design")
    parser.add_argument("--source", help="Optional source columns JSON")
    parser.add_argument("--table", help="Qualified target table override")
    parser.add_argument("--sink", choices=["clickhouse"], help="Sink override")
    parser.add_argument("--format", choices=["text", "json", "md"], default="text")
    return parser


def register_type_matrix_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    pairs = PairTypeMatrixService().available_pairs()
    source_choices = sorted({source for source, _ in pairs})
    sink_choices = sorted({sink for _, sink in pairs})
    parser = subparsers.add_parser(
        "type-matrix", help="Explain source -> sink type mapping and schema-evolution compatibility"
    )
    parser.add_argument("--source", required=True, choices=source_choices)
    parser.add_argument("--sink", required=True, choices=sink_choices)
    parser.add_argument(
        "--source-type",
        action="append",
        help="Source type to explain. Can be repeated. Defaults to the pair-specific production matrix.",
    )
    parser.add_argument(
        "--type-fidelity-json",
        help="Optional inline JSON policy for pair-specific fidelity controls, for example temporal settings.",
    )
    parser.add_argument("--format", choices=["text", "json", "md"], default="text")
    return parser


def _render_infer_text(payload: dict) -> str:
    return render_infer_text(payload)


def _render_infer_md(payload: dict) -> str:
    return render_infer_md(payload)


def _render_physical_text(payload: dict) -> str:
    return render_physical_text(payload)


def _render_physical_md(payload: dict) -> str:
    return render_physical_md(payload)


def _render_physical_diff_text(payload: dict) -> str:
    return render_physical_diff_text(payload)


def _render_physical_diff_md(payload: dict) -> str:
    return render_physical_diff_md(payload)


def _render_schema_explain_text(payload: dict) -> str:
    return render_schema_explain_text(payload)


def _render_schema_explain_md(payload: dict) -> str:
    return render_schema_explain_md(payload)


def _render_type_matrix_text(payload: dict) -> str:
    return render_type_matrix_text(payload)


def _render_type_matrix_md(payload: dict) -> str:
    return render_type_matrix_md(payload)


def _load_type_fidelity(raw: str | None) -> dict[str, object] | None:
    if not raw:
        return None
    loaded = json.loads(raw)
    if not isinstance(loaded, dict):
        raise ValueError("--type-fidelity-json must be a JSON object")
    return loaded
