"""CLI adapter for bounded Airflow cache status publication."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.readiness.airflow_cache_status_publication import publish_airflow_cache_status


def register_cache_status_publish_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "cache-status-publish",
        help="Validate and atomically publish one Airflow cache status file",
    )
    parser.add_argument("--status-root", required=True, help="Private directory containing cache status files")
    parser.add_argument("--source", required=True, help="Temporary status file name under status-root")
    parser.add_argument("--target", required=True, help="Last-known-good status file name under status-root")
    parser.add_argument("--failure-marker", required=True, help="Failure marker file name under status-root")
    parser.add_argument("--expected-schema", required=True, help="Registered dpone evidence schema expected in source")
    parser.add_argument("--format", choices=("text", "json"), default="text")
    setattr(
        parser,
        "_dpone_io_contract",
        (
            "stdout contains text or dpone.airflow-cache-status-publication.v1 JSON; argparse errors use stderr.",
            "Exit 0 is committed, 1 is rejected/attention, 2 is an unknown schema, and 4 is unsafe or "
            "commit-unknown state.",
            "A schema-valid source atomically replaces last-known-good; failure preserves it and publishes a "
            "separate bounded marker when possible.",
        ),
    )
    return parser


def cmd_airflow_cache_status_publish(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload, exit_code = publish_airflow_cache_status(
        status_root=Path(args.status_root),
        source_name=args.source,
        target_name=args.target,
        failure_marker_name=args.failure_marker,
        expected_schema=args.expected_schema,
    )
    _emit(payload, args.format)
    return exit_code


def _emit(payload: dict[str, object], output_format: str) -> None:
    if output_format == "json":
        write_json(payload)
        return
    outcome = str(payload["status"]).upper()
    lines = [
        f"dpone Airflow cache status: {outcome}",
        f"- target: {payload['target']}",
        f"- expected schema: {payload['expected_schema']}",
    ]
    if payload.get("error_code"):
        lines.append(f"- error: {payload['error_code']}")
    write_text("\n".join(lines) + "\n")


__all__ = ["cmd_airflow_cache_status_publish", "register_cache_status_publish_parser"]
