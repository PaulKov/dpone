"""Thin CLI adapter for compiled dbt release integrity."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from dpone.app.dbt_promotion_composition import (
    DbtReleaseIntegrityError,
    build_dbt_release_integrity_service,
)
from dpone.contracts.dbt_publish_models import DbtPublishIssue

from .dbt_publish_cli_support import emit_failure, emit_internal_failure


def register_write_release_checksums_parser(
    subparsers: argparse._SubParsersAction,
) -> argparse.ArgumentParser:
    """Register deterministic release checksum subject creation."""

    parser = subparsers.add_parser(
        "write-release-checksums",
        help="Create the deterministic checksum subject for a compiled dbt release",
    )
    parser.add_argument("--compiled-root", required=True)
    parser.add_argument("--format", choices=["text", "json"], default="text")
    return parser


def register_verify_release_checksums_parser(
    subparsers: argparse._SubParsersAction,
) -> argparse.ArgumentParser:
    """Register fail-closed verification of an attested release subject."""

    parser = subparsers.add_parser(
        "verify-release-checksums",
        help="Verify all compiled release bytes against the checksum subject",
    )
    parser.add_argument("--compiled-root", required=True)
    parser.add_argument("--format", choices=["text", "json"], default="text")
    return parser


def cmd_write_release_checksums(
    args: argparse.Namespace,
    *,
    ctx: object,
    logger: logging.Logger,
) -> int:
    """Write the single subject later signed by the dev release workflow."""

    del ctx, logger
    return _run_release_integrity(args, verify=False)


def cmd_verify_release_checksums(
    args: argparse.Namespace,
    *,
    ctx: object,
    logger: logging.Logger,
) -> int:
    """Verify the attested checksum subject before release materialization."""

    del ctx, logger
    return _run_release_integrity(args, verify=True)


def _run_release_integrity(args: argparse.Namespace, *, verify: bool) -> int:
    service = build_dbt_release_integrity_service()
    try:
        report = service.verify(Path(args.compiled_root)) if verify else service.write(Path(args.compiled_root))
    except DbtReleaseIntegrityError:
        emit_failure(
            (
                DbtPublishIssue(
                    code="DPONE_DBT_RELEASE_INTEGRITY_INVALID",
                    message="Compiled dbt release bytes failed integrity verification",
                    path=Path(args.compiled_root).as_posix(),
                    remediation=(
                        "Fetch the attested dev release again; do not edit, rebuild, or partially upload release files."
                    ),
                ),
            ),
            args.format,
            stage="dbt_release_integrity",
        )
        return 1
    except Exception:
        emit_internal_failure(args.format, stage="dbt_release_integrity")
        return 5
    payload = report.to_dict()
    if args.format == "json":
        print(json.dumps(payload, allow_nan=False, ensure_ascii=False, indent=2))
    else:
        action = "verified" if verify else "created"
        print(f"dbt release checksum subject {action}: {report.file_count} file(s), {report.subject_sha256}")
    return 0


__all__ = [
    "cmd_verify_release_checksums",
    "cmd_write_release_checksums",
    "register_verify_release_checksums_parser",
    "register_write_release_checksums_parser",
]
