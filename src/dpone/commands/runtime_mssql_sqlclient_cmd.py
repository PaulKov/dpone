from __future__ import annotations

import argparse
import logging

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.readiness.mssql_sqlclient import MssqlSqlClientReadinessService


def cmd_runtime_mssql_sqlclient_doctor(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = MssqlSqlClientReadinessService().doctor()
    if args.format == "json":
        write_json(payload)
    else:
        write_text(
            "\n".join(
                [
                    "# dpone runtime mssql-sqlclient doctor",
                    "",
                    f"- ready: `{payload['ready']}`",
                    f"- package_version: `{payload.get('package_version')}`",
                    f"- protocol: `{payload.get('protocol')}`",
                    f"- runtime_major: `{payload.get('runtime_major')}`",
                    f"- blockers: `{', '.join(payload['blocker_codes'])}`",
                ]
            )
            + "\n"
        )
    return 0 if payload["ready"] else 2


def register_doctor_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("doctor", help="Verify the optional SqlClient package and .NET runtime")
    parser.add_argument("--format", choices=["json", "md"], default="json")
    return parser


__all__ = ["cmd_runtime_mssql_sqlclient_doctor", "register_doctor_parser"]
