from __future__ import annotations

import argparse
from typing import Any

from dpone.commands.base import Command
from dpone.commands.func_command import CommandGroup, FuncCommand
from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.services.ops.facades import ObjectStorageOpsService


def object_storage_group() -> Command:
    lifecycle = CommandGroup(
        name="lifecycle",
        help="Verify or render object-storage lifecycle safety-net rules",
        build_parser=lambda sub: sub.add_parser("lifecycle", help="Object-storage lifecycle utilities"),
        subcommands=(
            FuncCommand("render", _lifecycle_render_parser, cmd_lifecycle_render),
            FuncCommand("verify", _lifecycle_verify_parser, cmd_lifecycle_verify),
        ),
        subdest="object_storage_lifecycle_cmd",
    )
    return CommandGroup(
        name="object-storage",
        help="Object-storage staging budget, cleanup and lifecycle governance",
        build_parser=lambda sub: sub.add_parser("object-storage", help="Object-storage staging governance"),
        subcommands=(
            FuncCommand("budget", _budget_parser, cmd_budget),
            FuncCommand("cleanup", _cleanup_parser, cmd_cleanup),
            lifecycle,
        ),
        subdest="object_storage_cmd",
    )


def cmd_budget(args: argparse.Namespace, *, ctx: object, logger: Any) -> int:
    del ctx, logger
    result = ObjectStorageOpsService().budget(**vars(args))
    _emit(result.payload, args.format)
    return result.code


def cmd_cleanup(args: argparse.Namespace, *, ctx: object, logger: Any) -> int:
    del ctx, logger
    result = ObjectStorageOpsService().cleanup(**vars(args))
    _emit(result.payload, args.format)
    return result.code


def cmd_lifecycle_render(args: argparse.Namespace, *, ctx: object, logger: Any) -> int:
    del ctx, logger
    result = ObjectStorageOpsService().lifecycle_render(**vars(args))
    _emit(result.payload, args.format)
    return result.code


def cmd_lifecycle_verify(args: argparse.Namespace, *, ctx: object, logger: Any) -> int:
    del ctx, logger
    result = ObjectStorageOpsService().lifecycle_verify(**vars(args))
    _emit(result.payload, args.format)
    return result.code


def _budget_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("budget", help="Check object-storage staging budget")
    _common_storage_args(parser)
    parser.add_argument("--limit", dest="bucket_limit_bytes", default="200GiB")
    parser.add_argument("--expected-run-bytes", default="0")
    parser.add_argument("--format", choices=["json", "md"], default="json")
    return parser


def _cleanup_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("cleanup", help="Dry-run or execute stale staging prefix cleanup")
    _common_storage_args(parser)
    parser.add_argument("--mode", choices=["dry-run", "execute"], default="dry-run")
    parser.add_argument("--now")
    parser.add_argument("--format", choices=["json", "md"], default="json")
    return parser


def _lifecycle_render_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("render", help="Render a prefix-scoped lifecycle rule")
    _common_policy_args(parser)
    parser.add_argument("--uri-prefix", required=True)
    parser.add_argument("--format", choices=["json", "md"], default="json")
    return parser


def _lifecycle_verify_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("verify", help="Verify lifecycle readiness")
    _common_policy_args(parser)
    parser.add_argument("--uri-prefix", required=True)
    parser.add_argument("--format", choices=["json", "md"], default="json")
    return parser


def _common_storage_args(parser: argparse.ArgumentParser) -> None:
    _common_policy_args(parser)
    parser.add_argument("--uri-prefix", required=True)
    parser.add_argument("--local-root-dir")
    parser.add_argument("--connection-type", default="env")
    parser.add_argument("--connection-id")


def _common_policy_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--warn-usage-pct", type=int, default=70)
    parser.add_argument("--block-usage-pct", type=int, default=85)
    parser.add_argument("--orphan-ttl-hours", type=int, default=12)
    parser.add_argument("--failed-run-ttl-hours", type=int, default=24)
    parser.add_argument("--lifecycle-expiration-days", type=int, default=2)
    parser.add_argument("--abort-incomplete-multipart-days", type=int, default=1)
    parser.add_argument("--require-lifecycle-rule", choices=["off", "warn", "required"], default="warn")


def _emit(payload: dict[str, object], output_format: str) -> None:
    if output_format == "json":
        write_json(payload)
        return
    write_text("\n".join(f"- {key}: `{value}`" for key, value in payload.items()) + "\n")


__all__ = ["object_storage_group"]
