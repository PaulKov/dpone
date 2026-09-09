from __future__ import annotations

import argparse
import json
import logging
from importlib import import_module
from pathlib import Path
from typing import Any

from dpone.commands.base import Command
from dpone.commands.func_command import CommandGroup, FuncCommand
from dpone.commands.output_json import dumps_json
from dpone.commands.output_text import write_text


def authority_group() -> Command:
    subcommands = [
        registry_group(),
        FuncCommand("check", register_check_parser, cmd_check),
        quorum_group(),
        signature_group(),
        FuncCommand("gate", register_gate_parser, cmd_gate),
        FuncCommand("report", register_report_parser, cmd_report),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("authority", help="Build and verify data product approval authority")

    return CommandGroup(
        name="authority",
        help="Build and verify data product approval authority",
        build_parser=build,
        subcommands=subcommands,
        subdest="data_product_authority_cmd",
    )


def registry_group() -> Command:
    subcommands = [FuncCommand("build", register_registry_build_parser, cmd_registry_build)]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("registry", help="Build authority registries")

    return CommandGroup(
        name="registry",
        help="Build authority registries",
        build_parser=build,
        subcommands=subcommands,
        subdest="data_product_authority_registry_cmd",
    )


def quorum_group() -> Command:
    subcommands = [FuncCommand("verify", register_quorum_verify_parser, cmd_quorum_verify)]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("quorum", help="Verify approval quorum evidence")

    return CommandGroup(
        name="quorum",
        help="Verify approval quorum evidence",
        build_parser=build,
        subcommands=subcommands,
        subdest="data_product_authority_quorum_cmd",
    )


def signature_group() -> Command:
    subcommands = [
        FuncCommand("sign", register_signature_sign_parser, cmd_signature_sign),
        FuncCommand("verify", register_signature_verify_parser, cmd_signature_verify),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("signature", help="Sign and verify evidence artifacts")

    return CommandGroup(
        name="signature",
        help="Sign and verify evidence artifacts",
        build_parser=build,
        subcommands=subcommands,
        subdest="data_product_authority_signature_cmd",
    )


def cmd_registry_build(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().registry_build(manifest_path=args.manifest)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_check(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().check(
        registry_path=args.registry, actor=args.actor, action=args.action, subject_path=args.subject
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_quorum_verify(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().quorum_verify(
        registry_path=args.registry,
        request_path=args.request,
        approval_paths=tuple(args.approval or ()),
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_signature_sign(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().signature_sign(registry_path=args.registry, artifact_path=args.artifact, actor=args.actor)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_signature_verify(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().signature_verify(
        registry_path=args.registry,
        artifact_path=args.artifact,
        signature_path=args.signature,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_gate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().gate(
        authority_check_path=args.authority_check,
        approval_quorum_path=args.approval_quorum,
        signature_paths=tuple(args.signature or ()),
        profile=args.profile,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_report(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().report(gate_path=args.gate)
    _emit(payload, args.format, args.output)
    return 0


def register_registry_build_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("build", help="Build a data product authority registry")
    parser.add_argument("--manifest", required=True, help="Manifest path")
    _add_output_args(parser)
    return parser


def register_check_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("check", help="Check actor authority for an action")
    parser.add_argument("--registry", required=True, help="Authority registry artifact")
    parser.add_argument("--actor", required=True, help="Actor id")
    parser.add_argument("--action", required=True, help="Action grant, such as policy_waiver.approve")
    parser.add_argument("--subject", required=True, help="Subject evidence artifact")
    _add_output_args(parser)
    return parser


def register_quorum_verify_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("verify", help="Verify approval quorum")
    parser.add_argument("--registry", required=True, help="Authority registry artifact")
    parser.add_argument("--request", required=True, help="Waiver or release request artifact")
    parser.add_argument("--approval", action="append", required=True, help="Approval evidence JSON/YAML")
    _add_output_args(parser)
    return parser


def register_signature_sign_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("sign", help="Create a detached evidence signature")
    parser.add_argument("--registry", required=True, help="Authority registry artifact")
    parser.add_argument("--artifact", required=True, help="Evidence artifact to sign")
    parser.add_argument("--actor", required=True, help="Signing actor")
    _add_output_args(parser)
    return parser


def register_signature_verify_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("verify", help="Verify a detached evidence signature")
    parser.add_argument("--registry", required=True, help="Authority registry artifact")
    parser.add_argument("--artifact", required=True, help="Evidence artifact to verify")
    parser.add_argument("--signature", required=True, help="Detached evidence signature artifact")
    _add_output_args(parser)
    return parser


def register_gate_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("gate", help="Gate authority, quorum and signature evidence")
    parser.add_argument("--authority-check", help="Authority check artifact")
    parser.add_argument("--approval-quorum", help="Approval quorum artifact")
    parser.add_argument("--signature", action="append", help="Evidence signature artifact")
    parser.add_argument(
        "--profile",
        choices=["advisory", "stage", "prod_strict", "regulated"],
        default="prod_strict",
        help="Authority gate profile",
    )
    _add_output_args(parser)
    return parser


def register_report_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("report", help="Render an authority gate report")
    parser.add_argument("--gate", required=True, help="Authority gate artifact")
    _add_output_args(parser, default="md")
    return parser


def _add_output_args(
    parser: argparse.ArgumentParser,
    *,
    formats: tuple[str, ...] = ("text", "json", "md", "table"),
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
        return str(payload.get("markdown") or _render_md(payload))
    if output_format == "table":
        return _render_table(payload)
    return _render_text(payload)


def _render_text(payload: dict[str, Any]) -> str:
    lines = [str(payload.get("schema_version", "dpone.data_product_authority"))]
    for key in (
        "status",
        "authority_registry_id",
        "authority_check_id",
        "approval_quorum_id",
        "evidence_signature_id",
        "authority_gate_id",
    ):
        if payload.get(key) is not None:
            lines.append(f"- {key}: {payload.get(key)}")
    return "\n".join(lines) + "\n"


def _render_md(payload: dict[str, Any]) -> str:
    return "# Data Product Authority\n\n```json\n" + json.dumps(payload, ensure_ascii=False, indent=2) + "\n```\n"


def _render_table(payload: dict[str, Any]) -> str:
    lines = ["data product authority", "field | value", "--- | ---"]
    for key in ("status", "actor", "profile", "authority_gate_id"):
        if payload.get(key) is not None:
            lines.append(f"{key} | {payload.get(key)}")
    return "\n".join(lines) + "\n"


def _facade() -> Any:
    return import_module("dpone.services.data_product_authority").DataProductAuthorityFacade()


__all__ = ["authority_group"]
