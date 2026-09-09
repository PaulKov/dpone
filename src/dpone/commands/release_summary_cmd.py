from __future__ import annotations

import argparse
import logging
from pathlib import Path

from dpone.commands.output_json import dumps_json, write_json
from dpone.commands.output_text import write_text
from dpone.services.ops.facades import ReleaseSummaryService, ReleaseVerificationService


def cmd_release_summary(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    report = ReleaseSummaryService().evaluate(
        output_dir=args.output_dir,
        release_id=args.release_id,
        replay_chain_dir=args.replay_chain_dir,
        matrix_suite_path=args.matrix_suite,
        matrix_chain_dir=args.matrix_chain_dir,
        connector_suite_path=args.connector_suite,
        connector_chain_dir=args.connector_chain_dir,
    )
    if args.format == "json":
        write_json(report.to_dict())
    else:
        write_text(report.to_markdown())
    return 0 if report.passed else 1


def cmd_release_verify(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    service = getattr(ctx, "release_verify_service", None) or ReleaseVerificationService.default()
    report = service.verify(
        release=args.release,
        package=args.package,
        github_repository=args.github_repository,
        runtime_image=args.runtime_image,
        check_github_release=not args.skip_github_release,
        check_pypi=not args.skip_pypi,
        check_runtime_image=not args.skip_runtime_image,
        install_smoke=args.install_smoke,
        install_extra=args.install_extra,
        install_python=args.install_python,
        timeout_seconds=args.timeout_seconds,
        poll_interval_seconds=args.poll_interval_seconds,
        command_timeout_seconds=args.command_timeout_seconds,
    )
    payload = report.to_dict()
    markdown = report.to_markdown()
    if args.output:
        _write_output(Path(args.output), payload, markdown, args.format)
    if args.format == "json":
        write_json(payload)
    else:
        write_text(markdown)
    return 0 if report.passed else 1


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "release-summary",
        help="Combine replay, matrix, and connector certification evidence into one final go/no-go report",
    )
    parser.add_argument("--output-dir", default=".dpone/certification-release-summary/latest")
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--replay-chain-dir", required=True)
    parser.add_argument("--matrix-suite", required=True)
    parser.add_argument("--matrix-chain-dir", required=True)
    parser.add_argument("--connector-suite", required=True)
    parser.add_argument("--connector-chain-dir", required=True)
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_release_verify_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "release-verify",
        help="Verify GitHub, PyPI and runtime image artifacts for a released dpone version",
    )
    parser.add_argument("--release", required=True, help="Release tag or version, for example v0.24.0")
    parser.add_argument("--package", default="dpone")
    parser.add_argument("--github-repository", default="PaulKov/dpone")
    parser.add_argument("--runtime-image", default="ghcr.io/paulkov/dpone-runtime")
    parser.add_argument("--skip-github-release", action="store_true")
    parser.add_argument("--skip-pypi", action="store_true")
    parser.add_argument("--skip-runtime-image", action="store_true")
    parser.add_argument("--install-smoke", action="store_true", help="Run `uvx --from PACKAGE dpone --version`")
    parser.add_argument("--install-extra", default=None, help="Optional package extra used by --install-smoke")
    parser.add_argument("--install-python", help="Optional Python version passed to uvx, for example 3.11")
    parser.add_argument("--timeout-seconds", type=int, default=0, help="Total wait time; 0 means a single attempt")
    parser.add_argument("--poll-interval-seconds", type=int, default=30)
    parser.add_argument("--command-timeout-seconds", type=int, default=300)
    parser.add_argument("--output", help="Optional JSON/Markdown evidence output path")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def _write_output(path: Path, payload: dict[str, object], markdown: str, fmt: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fmt == "json":
        path.write_text(dumps_json(payload), encoding="utf-8")
    else:
        path.write_text(markdown, encoding="utf-8")
