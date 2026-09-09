from __future__ import annotations

import argparse
import logging
from importlib import import_module
from typing import Any

from dpone.commands.func_command import CommandGroup, FuncCommand
from dpone.commands.schema_contract_output import emit_schema_contract_payload


def contract_group() -> object:
    subcommands: list[Any] = [
        FuncCommand("publish", register_publish_parser, cmd_publish),
        FuncCommand("check", register_check_parser, cmd_check),
        FuncCommand("gate", register_gate_parser, cmd_gate),
        FuncCommand("history", register_history_parser, cmd_history),
        FuncCommand("latest", register_latest_parser, cmd_latest),
        ConsumersCommand(),
        ViewsCommand(),
        AdoptionCommand(),
        FuncCommand("deprecate", register_deprecate_parser, cmd_deprecate),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("contract", help="Version and gate data schema contracts")

    return CommandGroup(
        name="contract",
        help="Version and gate data schema contracts",
        build_parser=build,
        subcommands=subcommands,
        subdest="schema_contract_cmd",
    )


def cmd_publish(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().publish(manifest_path=args.manifest, store_backend=args.store_backend, store_uri=args.store_uri)
    emit_schema_contract_payload(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_check(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().check(manifest_path=args.manifest, against=args.against, compatibility=args.compatibility)
    emit_schema_contract_payload(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_gate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().gate(manifest_path=args.manifest, pack_path=args.pack)
    emit_schema_contract_payload(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_history(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().history(contract_id=args.contract, store_backend=args.store_backend, store_uri=args.store_uri)
    emit_schema_contract_payload(payload, args.format, args.output)
    return 0


def cmd_latest(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().latest(contract_id=args.contract, store_backend=args.store_backend, store_uri=args.store_uri)
    emit_schema_contract_payload(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


class ConsumersCommand:
    @property
    def name(self) -> str:
        return "consumers"

    def register(self, subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        parser = register_consumers_parser(subparsers)
        parser.set_defaults(_command=self)
        sub = parser.add_subparsers(dest="schema_contract_consumers_cmd")
        lineage = sub.add_parser("lineage", help="Build column-level consumer lineage from local artifacts")
        lineage.add_argument("--manifest", required=True, help="Manifest path")
        _add_output_args(lineage, formats=("text", "json", "md", "table"))
        discover = sub.add_parser("discover", help="Discover consumers from configured local evidence")
        discover.add_argument("--manifest", required=True, help="Manifest path")
        discover.add_argument("--lineage", help="Optional consumer lineage artifact")
        _add_output_args(discover, formats=("text", "json", "md", "table"))
        matrix = sub.add_parser("matrix", help="Build consumer compatibility matrix")
        matrix.add_argument("--manifest", required=True, help="Manifest path")
        matrix.add_argument("--against", required=True, help="Base contract ref or artifact path")
        matrix.add_argument("--consumers", required=True, help="Consumer inventory artifact")
        matrix.add_argument("--lineage", help="Optional consumer lineage artifact")
        _add_output_args(matrix, formats=("text", "json", "md", "table"))
        gate = sub.add_parser("gate", help="Gate a migration pack with a consumer matrix")
        gate.add_argument("--manifest", required=True, help="Manifest path")
        gate.add_argument("--pack", required=True, help="Migration pack JSON path")
        gate.add_argument("--matrix", required=True, help="Consumer matrix artifact")
        _add_output_args(gate, formats=("text", "json", "md", "table"))
        test_kit = sub.add_parser("test-kit", help="Plan, render and certify consumer contract tests")
        test_kit.set_defaults(schema_contract_consumers_cmd="test-kit")
        test_kit_sub = test_kit.add_subparsers(dest="schema_contract_consumer_test_kit_cmd")
        kit_plan = test_kit_sub.add_parser("plan", help="Build a consumer contract test kit")
        kit_plan.add_argument("--manifest", required=True, help="Manifest path")
        kit_plan.add_argument("--matrix", required=True, help="Consumer matrix artifact")
        kit_plan.add_argument("--compatibility-view-plan", help="Optional compatibility view plan artifact")
        _add_output_args(kit_plan, formats=("text", "json", "md", "table"))
        kit_render = test_kit_sub.add_parser("render", help="Render consumer owner test artifacts")
        kit_render.add_argument("--kit", required=True, help="Consumer test kit JSON/YAML")
        _add_output_args(kit_render, formats=("text", "json", "md", "table", "pytest"), default="md")
        kit_certify = test_kit_sub.add_parser("certify", help="Certify consumer test execution")
        kit_certify.add_argument("--kit", required=True, help="Consumer test kit JSON/YAML")
        kit_certify.add_argument("--result", choices=["passed", "failed"], required=True, help="Execution status")
        kit_certify.add_argument("--result-artifact", help="Optional detailed result JSON/YAML")
        _add_output_args(kit_certify, formats=("text", "json", "md", "table"))
        return parser

    def run(self, args: argparse.Namespace, ctx: object) -> int:
        del ctx
        subcommand = getattr(args, "schema_contract_consumers_cmd", None)
        if subcommand == "lineage":
            payload = _consumer_facade().lineage(manifest_path=args.manifest)
        elif subcommand == "discover":
            payload = _consumer_facade().discover(manifest_path=args.manifest, lineage_path=args.lineage)
        elif subcommand == "matrix":
            payload = _consumer_facade().matrix(
                manifest_path=args.manifest,
                against=args.against,
                consumers_path=args.consumers,
                lineage_path=args.lineage,
            )
        elif subcommand == "gate":
            payload = _consumer_facade().gate(
                manifest_path=args.manifest,
                pack_path=args.pack,
                matrix_path=args.matrix,
            )
        elif subcommand == "test-kit":
            payload = _run_consumer_test_kit(args)
        else:
            if not args.contract:
                raise SystemExit("schema contract consumers requires --contract or a nested subcommand")
            payload = _facade().consumers(
                contract_id=args.contract,
                version=args.version,
                store_backend=args.store_backend,
                store_uri=args.store_uri,
            )
        emit_schema_contract_payload(payload, args.format, args.output)
        return 2 if payload.get("status") == "blocked" else 0


class ViewsCommand:
    @property
    def name(self) -> str:
        return "views"

    def register(self, subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        parser = subparsers.add_parser("views", help="Plan and gate contract compatibility views")
        parser.set_defaults(_command=self)
        sub = parser.add_subparsers(dest="schema_contract_views_cmd")
        plan = sub.add_parser("plan", help="Plan versioned compatibility views")
        plan.add_argument("--manifest", required=True, help="Manifest path")
        plan.add_argument("--against", required=True, help="Base contract ref or artifact path")
        plan.add_argument("--consumer-matrix", help="Optional consumer matrix artifact")
        _add_output_args(plan, formats=("text", "json", "md", "table"))
        gate = sub.add_parser("gate", help="Gate compatibility view coverage")
        gate.add_argument("--plan", required=True, help="Compatibility view plan artifact")
        gate.add_argument("--consumer-gate", help="Optional consumer gate artifact")
        _add_output_args(gate, formats=("text", "json", "md", "table"))
        report = sub.add_parser("report", help="Render compatibility view report")
        report.add_argument("--plan", required=True, help="Compatibility view plan artifact")
        _add_output_args(report, formats=("text", "json", "md", "table"), default="md")
        return parser

    def run(self, args: argparse.Namespace, ctx: object) -> int:
        del ctx
        subcommand = getattr(args, "schema_contract_views_cmd", None)
        if subcommand == "plan":
            payload = _views_facade().plan(
                manifest_path=args.manifest,
                against=args.against,
                consumer_matrix_path=args.consumer_matrix,
            )
        elif subcommand == "gate":
            payload = _views_facade().gate(plan_path=args.plan, consumer_gate_path=args.consumer_gate)
        elif subcommand == "report":
            payload = _views_facade().report(plan_path=args.plan)
        else:
            raise SystemExit("schema contract views requires a nested subcommand")
        emit_schema_contract_payload(payload, args.format, args.output)
        return 2 if payload.get("status") == "blocked" else 0


class AdoptionCommand:
    @property
    def name(self) -> str:
        return "adoption"

    def register(self, subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        parser = subparsers.add_parser("adoption", help="Plan and gate schema contract consumer adoption")
        parser.set_defaults(_command=self)
        sub = parser.add_subparsers(dest="schema_contract_adoption_cmd")
        plan = sub.add_parser("plan", help="Build a consumer adoption plan")
        plan.add_argument("--manifest", required=True, help="Manifest path")
        plan.add_argument("--consumer-matrix", required=True, help="Consumer matrix artifact")
        plan.add_argument("--compatibility-view-plan", help="Optional compatibility view plan artifact")
        _add_output_args(plan, formats=("text", "json", "md", "table"))
        status = sub.add_parser("status", help="Build current adoption status evidence")
        status.add_argument("--plan", required=True, help="Adoption plan artifact")
        status.add_argument("--registry", help="Optional evidence registry JSON")
        status.add_argument("--consumer-certification", help="Optional consumer certification artifact")
        _add_output_args(status, formats=("text", "json", "md", "table"), default="table")
        gate = sub.add_parser("gate", help="Gate old contract or compatibility view retirement")
        gate.add_argument("--status", required=True, help="Adoption status artifact")
        gate.add_argument("--profile", choices=["advisory", "prod_strict", "regulated"], default="prod_strict")
        _add_output_args(gate, formats=("text", "json", "md", "table"))
        retire = sub.add_parser("retire", help="Build a deterministic retirement plan")
        retire.add_argument("--gate", required=True, help="Contract retirement gate artifact")
        retire.add_argument("--compatibility-view-plan", help="Optional compatibility view plan artifact")
        _add_output_args(retire, formats=("text", "json", "md", "table"))
        return parser

    def run(self, args: argparse.Namespace, ctx: object) -> int:
        del ctx
        subcommand = getattr(args, "schema_contract_adoption_cmd", None)
        if subcommand == "plan":
            payload = _adoption_facade().plan(
                manifest_path=args.manifest,
                consumer_matrix_path=args.consumer_matrix,
                compatibility_view_plan_path=args.compatibility_view_plan,
            )
        elif subcommand == "status":
            payload = _adoption_facade().status(
                plan_path=args.plan,
                registry_path=args.registry,
                consumer_certification_path=args.consumer_certification,
            )
        elif subcommand == "gate":
            payload = _adoption_facade().gate(status_path=args.status, profile=args.profile)
        elif subcommand == "retire":
            payload = _adoption_facade().retire(
                gate_path=args.gate,
                compatibility_view_plan_path=args.compatibility_view_plan,
            )
        else:
            raise SystemExit("schema contract adoption requires a nested subcommand")
        emit_schema_contract_payload(payload, args.format, args.output)
        return 2 if payload.get("status") == "blocked" else 0


def cmd_deprecate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().deprecate(
        contract_id=args.contract,
        column=args.column,
        remove_after=args.remove_after,
        store_backend=args.store_backend,
        store_uri=args.store_uri,
    )
    emit_schema_contract_payload(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def register_publish_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("publish", help="Publish manifest schema contract version")
    parser.add_argument("--manifest", required=True, help="Manifest path")
    _add_store_args(parser, optional=True)
    _add_output_args(parser)
    return parser


def register_check_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("check", help="Check manifest contract against a prior version")
    parser.add_argument("--manifest", required=True, help="Manifest path")
    parser.add_argument("--against", required=True, help="Contract ref like analytics.orders@1.4.0 or artifact path")
    parser.add_argument("--compatibility", choices=["backward", "forward", "full", "none"], default="backward")
    _add_output_args(parser, formats=("text", "json", "md"))
    return parser


def register_gate_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("gate", help="Build consumer compatibility gate for a migration pack")
    parser.add_argument("--manifest", required=True, help="Manifest path")
    parser.add_argument("--pack", help="Optional migration pack JSON path")
    _add_output_args(parser)
    return parser


def register_history_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("history", help="List schema contract versions")
    parser.add_argument("--contract", required=True, help="Contract id")
    _add_store_args(parser)
    _add_output_args(parser, formats=("text", "json", "md", "table"), default="table")
    return parser


def register_latest_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("latest", help="Read latest schema contract version")
    parser.add_argument("--contract", required=True, help="Contract id")
    _add_store_args(parser)
    _add_output_args(parser, formats=("text", "json", "md", "table"))
    return parser


def register_consumers_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("consumers", help="List consumers bound to a contract")
    parser.add_argument("--contract", help="Contract id")
    parser.add_argument("--version", default="latest", help="Version or range selector")
    _add_store_args(parser)
    _add_output_args(parser, formats=("text", "json", "md", "table"), default="table")
    return parser


def register_deprecate_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("deprecate", help="Emit a deprecation evidence receipt for one column")
    parser.add_argument("--contract", required=True, help="Contract id")
    parser.add_argument("--column", required=True, help="Column or alias name")
    parser.add_argument("--remove-after", required=True, help="Removal date YYYY-MM-DD")
    _add_store_args(parser)
    _add_output_args(parser)
    return parser


def _add_store_args(parser: argparse.ArgumentParser, *, optional: bool = False) -> None:
    parser.add_argument("--store-backend", choices=["local_json", "sqlite"], default=None if optional else "local_json")
    parser.add_argument("--store-uri", help="Contract registry path or SQLite database path")


def _add_output_args(
    parser: argparse.ArgumentParser,
    *,
    formats: tuple[str, ...] = ("text", "json", "md"),
    default: str = "text",
) -> None:
    parser.add_argument("--format", choices=list(formats), default=default)
    parser.add_argument("--output", help="Optional output artifact path")


def _facade() -> Any:
    return import_module("dpone.services.schema_contract_registry").SchemaContractFacade()


def _consumer_facade() -> Any:
    return import_module("dpone.services.schema_contract_consumers").SchemaConsumerDiscoveryFacade()


def _consumer_test_kit_facade() -> Any:
    return import_module("dpone.services.schema_contract_consumer_test_kit").SchemaConsumerTestKitFacade()


def _views_facade() -> Any:
    return import_module("dpone.services.schema_contract_compatibility_views").SchemaContractCompatibilityViewFacade()


def _adoption_facade() -> Any:
    return import_module("dpone.services.schema_contract_adoption").SchemaContractAdoptionFacade()


def _run_consumer_test_kit(args: argparse.Namespace) -> dict[str, Any]:
    subcommand = getattr(args, "schema_contract_consumer_test_kit_cmd", None)
    if subcommand == "plan":
        return _consumer_test_kit_facade().plan(
            manifest_path=args.manifest,
            matrix_path=args.matrix,
            compatibility_view_plan_path=args.compatibility_view_plan,
        )
    if subcommand == "render":
        return _consumer_test_kit_facade().render(kit_path=args.kit, output_format=args.format)
    if subcommand == "certify":
        return _consumer_test_kit_facade().certify(
            kit_path=args.kit,
            result=args.result,
            result_artifact_path=args.result_artifact,
        )
    raise SystemExit("schema contract consumers test-kit requires a nested subcommand")


__all__ = ["contract_group"]
