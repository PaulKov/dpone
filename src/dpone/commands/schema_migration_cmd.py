from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.commands.base import Command


import argparse
import json
import logging
from collections.abc import Callable
from importlib import import_module
from pathlib import Path
from typing import Any, TypeVar

from dpone.commands import (
    schema_migration_backup_cmd,
    schema_migration_bundle_cmd,
    schema_migration_post_apply_cmd,
    schema_migration_registry_cmd,
    schema_migration_rehearse_cmd,
    schema_migration_remediation_cmd,
    schema_migration_watch_cmd,
)
from dpone.commands.func_command import CommandGroup, FuncCommand
from dpone.commands.output_json import dumps_json
from dpone.commands.output_text import write_text
from dpone.schema.errors import SchemaMigrationConfigurationError

DEFAULT_LEDGER = ".dpone/schema-migration/ledger.json"
T = TypeVar("T")


def migration_group() -> Command:
    subcommands = [
        FuncCommand("plan", register_plan_parser, cmd_schema_migration_plan),
        FuncCommand("baseline", register_baseline_parser, cmd_schema_migration_baseline),
        FuncCommand("apply", register_apply_parser, cmd_schema_migration_apply),
        FuncCommand("history", register_history_parser, cmd_schema_migration_history),
        FuncCommand("rollback", register_rollback_parser, cmd_schema_migration_rollback),
        FuncCommand("verify-env", register_verify_env_parser, cmd_schema_migration_verify_env),
        FuncCommand("certify", register_certify_parser, cmd_schema_migration_certify),
        FuncCommand("promote", register_promote_parser, cmd_schema_migration_promote),
        schema_migration_rehearse_cmd.rehearse_group(),
        schema_migration_post_apply_cmd.post_apply_group(),
        schema_migration_watch_cmd.watch_group(),
        schema_migration_remediation_cmd.remediation_group(),
        schema_migration_backup_cmd.backup_group(),
        _recovery_group(),
        schema_migration_bundle_cmd.bundle_group(),
        schema_migration_registry_cmd.registry_group(),
        schema_migration_bundle_cmd.review_group(),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("migration", help="Schema/DDL migration lifecycle commands")

    return CommandGroup(
        name="migration",
        help="Schema/DDL migration lifecycle commands",
        build_parser=build,
        subcommands=subcommands,
        subdest="schema_migration_cmd",
    )


def _recovery_group() -> Command:
    return import_module("dpone.commands.schema_migration_recovery_cmd").recovery_group()


def cmd_schema_migration_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _guard(
        lambda: _migration_facade().plan(
            manifest_path=args.manifest,
            actual_path=args.actual,
            source_path=args.source,
            table=args.table,
            sink_type=args.sink,
            strategy=args.strategy,
        )
    )
    _emit(payload, args.format, args.output)
    return 0


def cmd_schema_migration_baseline(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _guard(
        lambda: _migration_facade().baseline(
            manifest_path=args.manifest,
            actual_path=args.actual,
            ledger_path=args.ledger,
            source_path=args.source,
            table=args.table,
            sink_type=args.sink,
        )
    )
    _emit(payload, args.format, args.output)
    return 0


def cmd_schema_migration_apply(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    if args.execute and not args.target_connection and not (args.environment and args.environment_contract):
        _emit(_target_connection_required_payload(), args.format, args.output)
        return 2
    code, payload = _guard(
        lambda: _migration_facade().apply(
            plan_path=args.plan,
            ledger_path=args.ledger,
            actual_path=args.actual,
            approval_path=args.approval,
            phase=args.phase,
            execute=args.execute,
            target_connection_path=args.target_connection,
            environment=args.environment,
            environment_contract_path=args.environment_contract,
            promotion_path=args.promotion,
            bundle_gate_path=args.bundle_gate,
        )
    )
    _emit(payload, args.format, args.output)
    return code


def cmd_schema_migration_history(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _guard(lambda: _migration_facade().history(ledger_path=args.ledger))
    _emit(payload, args.format, args.output)
    return 0


def cmd_schema_migration_rollback(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    code, payload = _guard(
        lambda: _migration_facade().rollback(
            plan_path=args.plan,
            pack_id=args.pack_id,
            ledger_path=args.ledger,
            approval_path=args.approval,
        )
    )
    _emit(payload, args.format, args.output)
    return code


def cmd_schema_migration_verify_env(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _guard(
        lambda: _promotion_facade().verify_env(
            pack_path=args.pack,
            environment=args.environment,
            environment_contract_path=args.environment_contract,
            actual_path=args.actual,
        )
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("blockers") else 0


def cmd_schema_migration_certify(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    code, payload = _guard(
        lambda: _promotion_facade().certify(
            pack_path=args.pack,
            environment=args.environment,
            environment_contract_path=args.environment_contract,
            actual_path=args.actual,
        )
    )
    _emit(payload, args.format, args.output)
    return code


def cmd_schema_migration_promote(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    code, payload = _guard(
        lambda: _promotion_facade().promote(
            pack_path=args.pack,
            from_environment=args.from_environment,
            to_environment=args.to_environment,
            certificate_path=args.certificate,
            environment_contract_path=args.environment_contract,
            approval_path=args.approval,
        )
    )
    _emit(payload, args.format, args.output)
    return code


def register_plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("plan", help="Build an immutable schema/DDL migration pack")
    _add_manifest_args(parser)
    parser.add_argument("--actual", help="Optional actual physical target state JSON")
    parser.add_argument(
        "--strategy",
        choices=["block", "online_safe", "shadow"],
        help="Migration planning strategy; overrides physical_design.migration.strategy",
    )
    _add_output_args(parser, formats=("text", "json", "md"))
    return parser


def register_baseline_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("baseline", help="Adopt existing target state into the migration ledger")
    _add_manifest_args(parser)
    parser.add_argument("--actual", required=True, help="Actual physical target state JSON")
    parser.add_argument("--ledger", default=DEFAULT_LEDGER, help="Artifact ledger JSON path")
    _add_output_args(parser, formats=("text", "json", "md"))
    return parser


def register_apply_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("apply", help="Apply an approved migration pack into the migration ledger")
    parser.add_argument("--plan", required=True, help="Migration pack JSON path")
    parser.add_argument("--actual", help="Optional current actual state JSON for fingerprint verification")
    parser.add_argument("--approval", help="Optional approval artifact path")
    parser.add_argument("--phase", help="Apply one phase from a phased migration pack")
    parser.add_argument("--execute", action="store_true", help="Execute target DDL/DML instead of ledger-only apply")
    parser.add_argument("--target-connection", help="Target connection JSON for --execute")
    parser.add_argument("--environment", help="Promotion environment name for environment-bound apply")
    parser.add_argument("--environment-contract", help="Migration environments contract YAML/JSON")
    parser.add_argument("--promotion", help="Promotion receipt JSON/YAML required by guarded environments")
    parser.add_argument("--bundle-gate", help="Optional bundle gate receipt required by protected CI/CD flows")
    parser.add_argument("--ledger", default=DEFAULT_LEDGER, help="Artifact ledger JSON path")
    _add_output_args(parser, formats=("text", "json", "md"))
    return parser


def register_history_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("history", help="Read the schema migration ledger")
    parser.add_argument("--ledger", default=DEFAULT_LEDGER, help="Artifact ledger JSON path")
    _add_output_args(parser, formats=("text", "json", "md", "table"), default="table")
    return parser


def register_rollback_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("rollback", help="Rollback a reversible migration pack")
    selector = parser.add_mutually_exclusive_group(required=True)
    selector.add_argument("--plan", help="Migration pack JSON path")
    selector.add_argument("--pack-id", help="Applied pack id; requires a rollback plan in later target-backed mode")
    parser.add_argument("--approval", help="Optional approval artifact path")
    parser.add_argument("--ledger", default=DEFAULT_LEDGER, help="Artifact ledger JSON path")
    _add_output_args(parser, formats=("text", "json", "md"))
    return parser


def register_verify_env_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("verify-env", help="Verify a migration pack against one environment")
    _add_promotion_pack_args(parser)
    parser.add_argument("--actual", help="Override actual physical state JSON for this verification")
    _add_output_args(parser, formats=("text", "json", "md"))
    return parser


def register_certify_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("certify", help="Create an immutable environment certification receipt")
    _add_promotion_pack_args(parser)
    parser.add_argument("--actual", help="Override actual physical state JSON for this certification")
    _add_output_args(parser, formats=("text", "json", "md"))
    return parser


def register_promote_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("promote", help="Promote a certified migration pack to the next environment")
    parser.add_argument("--pack", required=True, help="Migration pack JSON path")
    parser.add_argument("--from", dest="from_environment", required=True, help="Certified source environment")
    parser.add_argument("--to", dest="to_environment", required=True, help="Target environment")
    parser.add_argument("--certificate", required=True, help="Environment certification receipt JSON/YAML")
    parser.add_argument("--environment-contract", required=True, help="Migration environments contract YAML/JSON")
    parser.add_argument("--approval", help="Optional promotion approval artifact JSON/YAML")
    _add_output_args(parser, formats=("text", "json", "md"))
    return parser


def _add_manifest_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--manifest", required=True, help="Manifest path")
    parser.add_argument("--source", help="Optional source columns JSON")
    parser.add_argument("--table", help="Qualified target table override")
    parser.add_argument(
        "--sink", choices=["mssql", "postgres", "clickhouse", "bigquery", "kafka"], help="Sink override"
    )


def _add_promotion_pack_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--pack", required=True, help="Migration pack JSON path")
    parser.add_argument("--environment", required=True, help="Environment name")
    parser.add_argument("--environment-contract", required=True, help="Migration environments contract YAML/JSON")


def _add_output_args(
    parser: argparse.ArgumentParser,
    *,
    formats: tuple[str, ...],
    default: str = "text",
) -> None:
    parser.add_argument("--format", choices=list(formats), default=default)
    parser.add_argument("--output", help="Optional output artifact path")


def _emit(payload: dict[str, Any], output_format: str, output_path: str | None) -> None:
    rendered = _render(payload, output_format)
    if output_path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(rendered, encoding="utf-8")
    write_text(rendered)


def _render(payload: dict[str, Any], output_format: str) -> str:
    if output_format == "json":
        return dumps_json(payload)
    if output_format == "md":
        return _render_md(payload)
    if output_format == "table":
        return _render_history_table(payload)
    return _render_text(payload)


def _render_text(payload: dict[str, Any]) -> str:
    command = str(payload.get("command") or "migration")
    status = str(payload.get("status") or ("blocked" if payload.get("blockers") else "planned"))
    lines = [f"dpone schema migration {command}", f"- status: {status}"]
    if payload.get("pack_id"):
        lines.append(f"- pack_id: {payload.get('pack_id')}")
    if payload.get("strategy"):
        lines.append(f"- strategy: {payload.get('strategy')}")
    if payload.get("environment"):
        lines.append(f"- environment: {payload.get('environment')}")
    if payload.get("certification_id"):
        lines.append(f"- certification_id: {payload.get('certification_id')}")
    if payload.get("promotion_id"):
        lines.append(f"- promotion_id: {payload.get('promotion_id')}")
    target = payload.get("target", {})
    if isinstance(target, dict):
        lines.append(f"- target: {target.get('sink_type')}.{target.get('table')}")
    phases = payload.get("phases", [])
    if isinstance(phases, list) and phases:
        lines.append("- phases:")
        lines.extend(f"  - {phase.get('name')}" for phase in phases if isinstance(phase, dict))
    for key in ("blockers", "warnings", "ddl"):
        values = payload.get(key, [])
        if isinstance(values, list) and values:
            lines.append(f"- {key}:")
            lines.extend(f"  - {item}" for item in values)
    return "\n".join(lines) + "\n"


def _render_md(payload: dict[str, Any]) -> str:
    title = str(payload.get("command") or "migration").replace("_", " ").title()
    return f"# dpone schema migration {title}\n\n```json\n{json.dumps(payload, indent=2, ensure_ascii=False)}\n```\n"


def _render_history_table(payload: dict[str, Any]) -> str:
    records = payload.get("records", [])
    lines = [
        "schema migration history",
        "pack_id | status | phase | target | applied_at",
        "--- | --- | --- | --- | ---",
    ]
    if isinstance(records, list):
        for record in records:
            if not isinstance(record, dict):
                continue
            target = record.get("target", {})
            target_text = ""
            if isinstance(target, dict):
                target_text = f"{target.get('sink_type')}.{target.get('table')}"
            lines.append(
                f"{record.get('pack_id')} | {record.get('status')} | {record.get('phase', '')} | "
                f"{target_text} | {record.get('applied_at')}"
            )
    return "\n".join(lines) + "\n"


def _guard(action: Callable[[], T]) -> T:
    try:
        return action()
    except (KeyError, ValueError) as exc:
        raise SchemaMigrationConfigurationError(str(exc)) from exc


def _target_connection_required_payload() -> dict[str, Any]:
    return {
        "schema_version": "dpone.schema_migration_result.v1",
        "command": "apply",
        "status": "blocked",
        "pack_id": None,
        "blockers": ["migration.target_connection_required"],
        "warnings": [],
    }


def _migration_facade() -> Any:
    module = import_module("dpone.services.schema_migration")
    return module.MigrationControlFacade()


def _promotion_facade() -> Any:
    module = import_module("dpone.services.schema_migration_promotion")
    return module.MigrationPromotionFacade()


__all__ = [
    "cmd_schema_migration_apply",
    "cmd_schema_migration_baseline",
    "cmd_schema_migration_certify",
    "cmd_schema_migration_history",
    "cmd_schema_migration_plan",
    "cmd_schema_migration_promote",
    "cmd_schema_migration_rollback",
    "cmd_schema_migration_verify_env",
    "migration_group",
]
