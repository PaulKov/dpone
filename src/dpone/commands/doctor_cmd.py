from __future__ import annotations

import argparse
import logging
from typing import cast

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.commands.readiness_facade import build_readiness_service


def cmd_doctor(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = build_readiness_service().doctor(profile=args.profile)
    checks = cast(list[dict[str, object]], payload["checks"])
    if args.format == "json":
        write_json(payload)
    elif args.format == "md":
        lines = [
            "# dpone doctor",
            "",
            f"- profile: `{payload['profile']}`",
            f"- passed: `{payload['passed']}`",
            "",
            "| check | status | message | required |",
            "|---|---|---|---|",
        ]
        for check in checks:
            lines.append(f"| {check['name']} | {check['status']} | {check['message']} | {check['required']} |")
        fixes = payload.get("fixes") or []
        if fixes:
            lines.extend(["", "## Fix hints", ""])
            lines.extend(f"- {fix}" for fix in fixes)
        write_text("\n".join(lines) + "\n")
    else:
        lines = ["dpone doctor"]
        lines.append(f"profile: {payload['profile']}")
        for check in checks:
            lines.append(f"- {check['name']}: {check['status']} ({check['message']})")
        for fix in payload.get("fixes") or []:
            lines.append(f"- fix: {fix}")
        write_text("\n".join(lines) + "\n")
    return 0 if payload["passed"] else 1


def register_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("doctor", help="Check local dpone runtime readiness")
    parser.add_argument(
        "--profile",
        choices=["local", "ci", "production", "mssql"],
        default="local",
    )
    parser.add_argument("--format", choices=["text", "json", "md"], default="text")
    return parser
