from __future__ import annotations

import argparse
import logging

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.readiness.native_acceleration import NativeAccelerationReadinessService


def cmd_runtime_native_accel_doctor(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = NativeAccelerationReadinessService().doctor()
    if args.format == "json":
        write_json(payload)
    else:
        lines = [
            "# dpone runtime native-accel doctor",
            "",
            f"- selected_backend: `{payload.get('selected_backend')}`",
            f"- requested_mode: `{payload.get('requested_mode')}`",
            f"- importable: `{payload.get('importable')}`",
            f"- accelerator_version: `{payload.get('accelerator_version')}`",
            f"- fallback_reason: `{payload.get('fallback_reason')}`",
            f"- direct_ingest_backend: `{(payload.get('direct_ingest') or {}).get('selected_backend')}`",
            f"- direct_ingest_fallback: `{(payload.get('direct_ingest') or {}).get('fallback_reason')}`",
            f"- direct_ingest_provider_version: `{(payload.get('direct_ingest') or {}).get('provider_version')}`",
            f"- direct_ingest_protocol_revision: `{(payload.get('direct_ingest') or {}).get('protocol_revision')}`",
            "- direct_ingest_compression: "
            f"`{', '.join(str(item) for item in (payload.get('direct_ingest') or {}).get('supported_compression') or [])}`",
            f"- blockers: `{', '.join(str(item) for item in payload.get('blocker_codes') or [])}`",
            f"- warnings: `{', '.join(str(item) for item in payload.get('warning_codes') or [])}`",
        ]
        write_text("\n".join(lines) + "\n")
    return 0


def cmd_runtime_native_accel_benchmark(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = NativeAccelerationReadinessService().benchmark_plan(
        manifest=args.manifest,
        rows=args.rows,
        output=args.output,
    )
    if args.format == "json":
        write_json(payload)
    else:
        write_text(
            "\n".join(
                [
                    "# dpone runtime native-accel benchmark",
                    "",
                    f"- manifest: `{args.manifest}`",
                    f"- rows: `{args.rows}`",
                    f"- output: `{args.output}`",
                    "- status: `planned`",
                    f"- selected_backend: `{payload['doctor'].get('selected_backend')}`",
                ]
            )
            + "\n"
        )
    return 0


def register_doctor_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("doctor", help="Inspect optional native acceleration backend availability")
    parser.add_argument("--format", choices=["json", "md", "table"], default="json")
    return parser


def register_benchmark_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("benchmark", help="Render a native acceleration benchmark plan")
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--rows", type=int, default=10_000)
    parser.add_argument("--output", required=True)
    parser.add_argument("--format", choices=["json", "md", "table"], default="json")
    return parser


__all__ = [
    "cmd_runtime_native_accel_benchmark",
    "cmd_runtime_native_accel_doctor",
    "register_benchmark_parser",
    "register_doctor_parser",
]
