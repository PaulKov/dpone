"""Thin workspace promotion CLI; all source and ownership decisions live in services."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from contextlib import redirect_stdout
from pathlib import Path

from dpone.app.dbt_promotion_composition import (
    build_dbt_workspace_mirror_service,
    build_dbt_workspace_promotion_verification_service,
)
from dpone.contracts.dbt_promotion import (
    DbtProdMirrorError,
    DbtPromotionTrustDescriptor,
    dbt_json_object,
    require_dbt_digest,
)
from dpone.contracts.dbt_publish_models import DbtPublishIssue
from dpone.manifest.confined_files import read_confined_file

from .dbt_publish_cli_support import emit_failure, emit_internal_failure
from .dbt_workspace_authoring_cmd import workspace_authoring_commands
from .func_command import CommandGroup, FuncCommand


def workspace_group() -> CommandGroup:
    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("workspace", help="Discover, check, compile and promote a complete dbt workspace")

    return CommandGroup(
        name="workspace",
        help="Complete dbt workspace sources",
        build_parser=build,
        subcommands=[
            *workspace_authoring_commands(),
            FuncCommand("prepare-prod-mirror", _prepare_parser, _prepare, _requires_app_context=False),
            FuncCommand("verify-promotion", _verify_parser, _verify, _requires_app_context=False),
        ],
        subdest="dbt_workspace_cmd",
    )


def _prepare_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "prepare-prod-mirror", help="Prepare bot-owned review files; does not authorize or activate PROD"
    )
    _common(parser)
    for name in (
        "compiled-root",
        "mirror-root",
        "source-snapshot-path",
        "descriptor-path",
        "expected-release-id",
        "dev-deployment-id",
        "dev-evidence-ref",
        "dev-evidence-subject-sha256",
        "dev-evidence-artifact-name",
        "dev-evidence-producer-workflow",
        "dev-evidence-source-commit",
        "dev-evidence-set-id",
        "dev-evidence-campaign-request-sha256",
    ):
        parser.add_argument(f"--{name}", required=True)
    return parser


def _verify_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "verify-promotion", help="Read-only comparison of every project; not cryptographic promotion approval"
    )
    _common(parser)
    parser.add_argument("--descriptor", required=True, help="V3 descriptor path relative to --root")
    parser.add_argument(
        "--release-set", required=True, help="Pinned compiled release-set.json with its complete artifact tree"
    )
    return parser


def _common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--root", default=".", help="Audit Git checkout (default: current directory)")
    parser.add_argument("--format", choices=["text", "json"], default="text")


def _prepare(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    try:
        trust = DbtPromotionTrustDescriptor.validated(
            subject_sha256=args.dev_evidence_subject_sha256,
            artifact_name=args.dev_evidence_artifact_name,
            producer_workflow=args.dev_evidence_producer_workflow,
            source_commit=args.dev_evidence_source_commit,
            evidence_set_id=args.dev_evidence_set_id,
            campaign_request_sha256=args.dev_evidence_campaign_request_sha256,
        )
        report = build_dbt_workspace_mirror_service().prepare(
            compiled_root=Path(args.compiled_root),
            repository_root=Path(args.root),
            mirror_root=args.mirror_root,
            source_snapshot_path=args.source_snapshot_path,
            descriptor_path=args.descriptor_path,
            expected_release_id=args.expected_release_id,
            dev_deployment_id=args.dev_deployment_id,
            dev_evidence_ref=args.dev_evidence_ref,
            trust=trust,
        )
    except (OSError, ValueError):
        return _failure(args)
    except Exception:
        emit_internal_failure(args.format, stage="dbt_workspace_promotion")
        return 5
    if args.format == "json":
        print(json.dumps(report.to_dict(), allow_nan=False, indent=2))
    else:
        print(f"Workspace mirror {'unchanged' if report.no_op else 'prepared'}: {report.release_id}")
        for project in report.projects:
            print(f"- {project}")
    return 0


def _verify(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    try:
        path = Path(args.release_set).absolute()
        if path.name != "release-set.json":
            raise DbtProdMirrorError("canonical release-set.json is required")
        release = dbt_json_object(read_confined_file(path.parent, path.name, max_bytes=8 * 1024 * 1024))
        report = build_dbt_workspace_promotion_verification_service().verify(
            compiled_root=path.parent,
            repository_root=Path(args.root),
            descriptor_path=args.descriptor,
            expected_release_id=require_dbt_digest(release.get("release_id"), "release identity"),
        )
    except (OSError, ValueError, RecursionError):
        return _failure(args)
    except Exception:
        emit_internal_failure(args.format, stage="dbt_workspace_promotion")
        return 5
    if args.format == "json":
        print(json.dumps(report.to_dict(), allow_nan=False, indent=2))
    else:
        print(f"Workspace source verification: {'PASS' if report.passed else 'FAIL'}")
        for project in report.projects:
            print(f"- {'PASS' if project.passed else 'FAIL'} {project.project_path}")
    return 0 if report.passed else 2


def _failure(args: argparse.Namespace) -> int:
    issue = DbtPublishIssue(
        code="DPONE_DBT_PROMOTION_SOURCE_DRIFT",
        message="Workspace promotion sources or ownership could not be verified",
        path=str(args.root),
        remediation="Use the complete attested release and valid v3 bot-owned metadata. Recover pending transactions with the installer; do not delete their journal or adopt author-owned paths.",
    )
    if args.format == "json":
        emit_failure((issue,), "json", stage="dbt_workspace_promotion")
    else:
        with redirect_stdout(sys.stderr):
            emit_failure((issue,), "text", stage="dbt_workspace_promotion")
    return 2


__all__ = ["workspace_group"]
