"""Thin CLI adapter for trusted dbt dev-evidence bundles."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from dpone.app.dbt_promotion_composition import (
    DbtDevEvidenceBundleError,
    DbtDevEvidenceRequest,
    DbtDevEvidenceRequestError,
    build_dbt_dev_evidence_bundle_service,
    build_dbt_expected_release_loader,
)
from dpone.contracts.dbt_publish_models import DbtPublishIssue

from .dbt_dev_evidence_request_file import (
    DevEvidenceRequestFileError,
    read_dev_evidence_request,
)
from .dbt_publish_cli_support import emit_failure, emit_internal_failure


def register_finalize_dev_evidence_parser(
    subparsers: argparse._SubParsersAction,
) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "finalize-dev-evidence",
        help="Validate and atomically create an attestation-ready dev evidence bundle",
    )
    _add_evidence_identity_arguments(parser)
    parser.add_argument("--source-evidence-root", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument(
        "--campaign-request",
        help="Canonical request that authorized this provider evidence set",
    )
    parser.add_argument("--producer-repository", required=True)
    parser.add_argument("--producer-workflow", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--format", choices=["text", "json"], default="text")
    return parser


def register_prepare_dev_evidence_request_parser(
    subparsers: argparse._SubParsersAction,
) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "prepare-dev-evidence-request",
        help="Derive one immutable Airflow evidence campaign from a release and deployment",
    )
    _add_release_deployment_identity_arguments(parser)
    parser.add_argument("--producer-repository", required=True)
    parser.add_argument("--producer-workflow", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--orchestration-run-id", required=True)
    parser.add_argument(
        "--orchestration-run-attempt",
        required=True,
        type=int,
    )
    parser.add_argument("--format", choices=["text", "json"], default="text")
    return parser


def register_verify_dev_evidence_integrity_parser(
    subparsers: argparse._SubParsersAction,
) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "verify-dev-evidence-integrity",
        help="Verify attested bytes and release-bound semantics of dev evidence",
    )
    _add_evidence_identity_arguments(parser)
    parser.add_argument("--evidence-root", required=True)
    parser.add_argument("--format", choices=["text", "json"], default="text")
    return parser


def cmd_prepare_dev_evidence_request(
    args: argparse.Namespace,
    *,
    ctx: object,
    logger: logging.Logger,
) -> int:
    del ctx, logger
    try:
        request = DbtDevEvidenceRequest.build(
            compiled_root=Path(args.compiled_root),
            release_id=args.expected_release_id,
            deployment_id=args.expected_deployment_id,
            producer_repository=args.producer_repository,
            producer_workflow=args.producer_workflow,
            source_commit=args.source_commit,
            orchestration_run_id=args.orchestration_run_id,
            orchestration_run_attempt=args.orchestration_run_attempt,
            release_loader=build_dbt_expected_release_loader(),
        )
    except DbtDevEvidenceRequestError as exc:
        emit_failure(
            (
                DbtPublishIssue(
                    code=exc.code,
                    message="Dev evidence campaign request is invalid",
                    path=Path(args.compiled_root).as_posix(),
                    remediation=(
                        "Use the exact compiled release, deployment and trusted "
                        "workflow-run identity, then prepare the request again."
                    ),
                ),
            ),
            args.format,
            stage="dbt_dev_evidence",
        )
        return 1
    except Exception:
        emit_internal_failure(args.format, stage="dbt_dev_evidence")
        return 5
    payload = request.to_dict()
    if args.format == "json":
        print(
            json.dumps(
                payload,
                allow_nan=False,
                ensure_ascii=False,
                indent=2,
            )
        )
    else:
        print(f"dbt dev evidence request: {request.evidence_set_id} ({len(request.workflows)} workflow(s))")
    return 0


def cmd_finalize_dev_evidence(
    args: argparse.Namespace,
    *,
    ctx: object,
    logger: logging.Logger,
) -> int:
    del ctx, logger
    try:
        evidence_set_id = _expected_evidence_set_id(args)
        campaign_request = (
            DbtDevEvidenceRequest.from_mapping(read_dev_evidence_request(Path(args.campaign_request)))
            if isinstance(args.campaign_request, str)
            else None
        )
        service = build_dbt_dev_evidence_bundle_service()
        if evidence_set_id is None and campaign_request is None:
            report = service.finalize(
                compiled_root=Path(args.compiled_root),
                source_evidence_root=Path(args.source_evidence_root),
                output_root=Path(args.output_root),
                expected_release_id=args.expected_release_id,
                expected_deployment_id=args.expected_deployment_id,
                expected_activation_id=_expected_activation_id(args),
                producer_repository=args.producer_repository,
                producer_workflow=args.producer_workflow,
                source_commit=args.source_commit,
            )
        else:
            report = service.finalize(
                compiled_root=Path(args.compiled_root),
                source_evidence_root=Path(args.source_evidence_root),
                output_root=Path(args.output_root),
                expected_release_id=args.expected_release_id,
                expected_deployment_id=args.expected_deployment_id,
                expected_evidence_set_id=evidence_set_id,
                expected_activation_id=_expected_activation_id(args),
                campaign_request=campaign_request,
                producer_repository=args.producer_repository,
                producer_workflow=args.producer_workflow,
                source_commit=args.source_commit,
            )
    except (
        DbtDevEvidenceBundleError,
        DevEvidenceRequestFileError,
        OSError,
    ):
        return _emit_failure(args)
    except Exception:
        emit_internal_failure(args.format, stage="dbt_dev_evidence")
        return 5
    return _emit_success(args, report.to_dict())


def cmd_verify_dev_evidence_integrity(
    args: argparse.Namespace,
    *,
    ctx: object,
    logger: logging.Logger,
) -> int:
    del ctx, logger
    try:
        report = build_dbt_dev_evidence_bundle_service().verify(
            compiled_root=Path(args.compiled_root),
            evidence_root=Path(args.evidence_root),
            expected_release_id=args.expected_release_id,
            expected_deployment_id=args.expected_deployment_id,
            expected_evidence_set_id=_expected_evidence_set_id(args),
            expected_activation_id=_expected_activation_id(args),
        )
    except DbtDevEvidenceBundleError:
        return _emit_failure(args)
    except Exception:
        emit_internal_failure(args.format, stage="dbt_dev_evidence")
        return 5
    return _emit_success(args, report.to_dict())


def _add_evidence_identity_arguments(parser: argparse.ArgumentParser) -> None:
    _add_release_deployment_identity_arguments(parser)
    parser.add_argument(
        "--expected-evidence-set-id",
        help="Campaign identity required for provider-produced evidence sets",
    )
    parser.add_argument(
        "--expected-activation-id",
        help="Exact cache activation UUID expected in every runtime evidence record",
    )


def _add_release_deployment_identity_arguments(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument("--compiled-root", required=True)
    parser.add_argument("--expected-release-id", required=True)
    parser.add_argument("--expected-deployment-id", required=True)


def _expected_evidence_set_id(args: argparse.Namespace) -> str | None:
    value = getattr(args, "expected_evidence_set_id", None)
    return value if isinstance(value, str) else None


def _expected_activation_id(args: argparse.Namespace) -> str | None:
    value = getattr(args, "expected_activation_id", None)
    return value if isinstance(value, str) else None


def _emit_failure(args: argparse.Namespace) -> int:
    root = str(
        getattr(args, "evidence_root", None)
        or getattr(
            args,
            "source_evidence_root",
            "",
        )
    )
    emit_failure(
        (
            DbtPublishIssue(
                code="DPONE_DBT_DEV_EVIDENCE_INTEGRITY_INVALID",
                message="Dev runtime evidence is incomplete, unsafe, or not release-bound",
                path=Path(root).as_posix(),
                remediation=(
                    "Export final evidence from the exact dev deployment, then let "
                    "the trusted evidence workflow finalize and attest it again."
                ),
            ),
        ),
        args.format,
        stage="dbt_dev_evidence",
    )
    return 1


def _emit_success(args: argparse.Namespace, payload: dict[str, object]) -> int:
    if args.format == "json":
        print(json.dumps(payload, allow_nan=False, ensure_ascii=False, indent=2))
    else:
        print(f"dbt dev evidence verified: {payload['file_count']} file(s), {payload['subject_sha256']}")
    return 0


__all__ = [
    "cmd_finalize_dev_evidence",
    "cmd_prepare_dev_evidence_request",
    "cmd_verify_dev_evidence_integrity",
    "register_finalize_dev_evidence_parser",
    "register_prepare_dev_evidence_request_parser",
    "register_verify_dev_evidence_integrity_parser",
]
