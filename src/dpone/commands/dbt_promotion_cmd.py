"""CLI adapter for deterministic dbt dev-to-prod source verification."""

from __future__ import annotations

import argparse
import json
import logging
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.app.dbt_promotion_composition import (
    DbtProdMirrorError,
    build_dbt_dev_evidence_verification_service,
    build_dbt_prod_promotion_metadata_verifier,
    build_dbt_promotion_verification_service,
    render_dbt_promotion_ci_report,
)
from dpone.contracts.dbt_publish_models import DbtPublishIssue
from dpone.contracts.strict_json import StrictJsonError, strict_json_object
from dpone.manifest.confined_files import read_confined_file
from dpone.readiness.dbt_publish_release_materializer import (
    DbtReleaseMaterializationError,
    DbtReleaseMaterializer,
)

from .dbt_publish_cli_support import emit_failure, emit_internal_failure

_MAX_RELEASE_BYTES = 8 * 1024 * 1024
_MAX_SNAPSHOT_BYTES = 1024 * 1024


def register_verify_promotion_parser(
    subparsers: argparse._SubParsersAction,
) -> argparse.ArgumentParser:
    """Register the CI-oriented source-mirror verification command."""

    parser = subparsers.add_parser(
        "verify-promotion",
        help="Verify a prod dbt source mirror against one compiled release",
    )
    parser.add_argument("project", help="Mirrored dbt project root")
    parser.add_argument(
        "--compiled-root",
        required=True,
        help="Downloaded immutable compiled release tree",
    )
    optional_metadata_help = {
        "--repository-root": "Prod repository root used to confine and verify the promotion descriptor",
        "--descriptor-path": "Promotion descriptor path relative to --repository-root",
        "--expected-dev-deployment-id": "Pinned dev deployment identity expected in the promotion descriptor",
        "--expected-dev-evidence-ref": "Immutable dev evidence reference expected in the promotion descriptor",
        "--expected-dev-evidence-subject-sha256": "Expected SHA-256 of the attested dev evidence subject",
        "--expected-dev-evidence-artifact-name": "Expected immutable dev evidence artifact name",
        "--expected-dev-evidence-producer-workflow": "Expected trusted workflow that produced dev evidence",
        "--expected-dev-evidence-source-commit": "Expected source commit recorded by the dev evidence producer",
        "--expected-dev-evidence-set-id": "Expected finalized dev evidence-set identity",
        "--expected-dev-evidence-campaign-request-sha256": ("Expected SHA-256 of the dev evidence campaign request"),
    }
    for flag, help_text in optional_metadata_help.items():
        parser.add_argument(flag, help=help_text)
    parser.add_argument("--format", choices=["text", "json"], default="text")
    return parser


def register_materialize_release_parser(
    subparsers: argparse._SubParsersAction,
) -> argparse.ArgumentParser:
    """Register installation of downloaded immutable release bytes."""

    parser = subparsers.add_parser(
        "materialize-release",
        help="Validate and install a downloaded dbt release without rebuilding",
    )
    parser.add_argument("--compiled-root", required=True, help="Downloaded immutable release tree to install")
    parser.add_argument("--cache-root", required=True, help="Content-addressed local cache root")
    parser.add_argument(
        "--expected-release-id", required=True, help="Release identity that the downloaded bytes must reproduce"
    )
    parser.add_argument("--format", choices=["text", "json"], default="text")
    return parser


def register_verify_dev_evidence_parser(
    subparsers: argparse._SubParsersAction,
) -> argparse.ArgumentParser:
    """Register the fail-closed dev execution evidence promotion gate."""

    parser = subparsers.add_parser(
        "verify-dev-evidence",
        help="Verify dev dbt and Airflow evidence for every release workload",
    )
    parser.add_argument(
        "--compiled-root", required=True, help="Downloaded release tree whose workloads must be covered"
    )
    parser.add_argument(
        "--evidence-root",
        required=True,
        help="Root containing exact-depth airflow/ and dbt/ JSON evidence",
    )
    parser.add_argument(
        "--expected-release-id", required=True, help="Pinned release identity expected in every evidence record"
    )
    parser.add_argument(
        "--expected-deployment-id", required=True, help="Pinned dev deployment expected in every evidence record"
    )
    parser.add_argument(
        "--expected-activation-id",
        required=True,
        help="Pinned dev cache activation expected in every evidence record",
    )
    parser.add_argument("--format", choices=["text", "json"], default="text")
    return parser


def register_render_ci_report_parser(
    subparsers: argparse._SubParsersAction,
) -> argparse.ArgumentParser:
    """Register deterministic Markdown projection for MR summaries."""

    parser = subparsers.add_parser(
        "render-ci-report",
        help="Render the compiled dbt evidence as a Markdown MR summary",
    )
    parser.add_argument("--compiled-root", required=True)
    parser.add_argument("--airflow-base-url")
    return parser


def cmd_verify_promotion(
    args: argparse.Namespace,
    *,
    ctx: object,
    logger: logging.Logger,
) -> int:
    """Return success only when local source reproduces the pinned project bundle."""

    del ctx, logger
    compiled_root = Path(args.compiled_root).absolute()
    release_set = _read_json_object(
        compiled_root,
        "release-set.json",
        max_bytes=_MAX_RELEASE_BYTES,
    )
    source_snapshot = _read_json_object(
        compiled_root,
        "_dbt/dbt-source-snapshot.json",
        max_bytes=_MAX_SNAPSHOT_BYTES,
    )
    report = build_dbt_promotion_verification_service().verify(
        project_root=Path(args.project),
        release_set=release_set,
        source_snapshot=source_snapshot,
    )
    metadata_values = (
        args.repository_root,
        args.descriptor_path,
        args.expected_dev_deployment_id,
        args.expected_dev_evidence_ref,
        args.expected_dev_evidence_subject_sha256,
        args.expected_dev_evidence_artifact_name,
        args.expected_dev_evidence_producer_workflow,
        args.expected_dev_evidence_source_commit,
    )
    expected_evidence_set_id = getattr(
        args,
        "expected_dev_evidence_set_id",
        None,
    )
    expected_campaign_request_sha256 = getattr(
        args,
        "expected_dev_evidence_campaign_request_sha256",
        None,
    )
    metadata_requested = (
        any(value is not None for value in metadata_values)
        or expected_evidence_set_id is not None
        or expected_campaign_request_sha256 is not None
    )
    metadata_complete = all(isinstance(value, str) and bool(value) for value in metadata_values)
    campaign_values = (
        expected_evidence_set_id,
        expected_campaign_request_sha256,
    )
    campaign_complete = all(isinstance(value, str) and bool(value) for value in campaign_values)
    campaign_omitted = all(value is None for value in campaign_values)
    if metadata_requested and (not metadata_complete or not (campaign_complete or campaign_omitted)):
        emit_failure(
            (
                DbtPublishIssue(
                    code="DPONE_DBT_PROJECT_ARGUMENT_CONFLICT",
                    message="Prod promotion metadata verification arguments are incomplete",
                    path="command",
                    remediation=(
                        "Provide repository root, descriptor path, dev deployment "
                        "identity, and the complete dev evidence trust descriptor together."
                    ),
                ),
            ),
            args.format,
            stage="dbt_promotion",
        )
        return 2
    if report.passed and metadata_complete:
        try:
            evidence_set_kwargs = (
                {
                    "expected_dev_evidence_set_id": (expected_evidence_set_id),
                    "expected_dev_evidence_campaign_request_sha256": (expected_campaign_request_sha256),
                }
                if isinstance(expected_evidence_set_id, str)
                and isinstance(
                    expected_campaign_request_sha256,
                    str,
                )
                else {}
            )
            build_dbt_prod_promotion_metadata_verifier().verify(
                compiled_root=compiled_root,
                repository_root=Path(args.repository_root),
                descriptor_path=args.descriptor_path,
                expected_release_id=str(report.release_id),
                expected_dev_deployment_id=args.expected_dev_deployment_id,
                expected_dev_evidence_ref=args.expected_dev_evidence_ref,
                expected_dev_evidence_subject_sha256=args.expected_dev_evidence_subject_sha256,
                expected_dev_evidence_artifact_name=args.expected_dev_evidence_artifact_name,
                expected_dev_evidence_producer_workflow=args.expected_dev_evidence_producer_workflow,
                expected_dev_evidence_source_commit=args.expected_dev_evidence_source_commit,
                **evidence_set_kwargs,
            )
        except DbtProdMirrorError:
            report = type(report)(code="DPONE_DBT_PROMOTION_SOURCE_DRIFT")
    if not report.passed:
        emit_failure(
            (
                DbtPublishIssue(
                    code=report.code,
                    message="The dbt source mirror does not match the pinned release",
                    path=Path(args.project).as_posix(),
                    remediation=(
                        "Replace the prod mirror from the dev release source snapshot; "
                        "do not rebuild or edit generated artifacts."
                    ),
                ),
            ),
            args.format,
            stage="dbt_promotion",
        )
        return 1
    payload = report.to_dict()
    if args.format == "json":
        print(json.dumps(payload, allow_nan=False, ensure_ascii=False, indent=2))
    else:
        print(f"dbt source mirror verified: {payload['release_id']}")
    return 0


def cmd_materialize_release(
    args: argparse.Namespace,
    *,
    ctx: object,
    logger: logging.Logger,
) -> int:
    """Install exactly the release requested by promotion metadata."""

    del ctx, logger
    try:
        materialized = DbtReleaseMaterializer().materialize(
            compiled_root=Path(args.compiled_root),
            cache_root=Path(args.cache_root),
            expected_release_id=args.expected_release_id,
        )
    except DbtReleaseMaterializationError as exc:
        _emit_materialization_failure(args, exc)
        return 1
    except Exception:
        emit_internal_failure(args.format, stage="dbt_promotion")
        return 5
    if materialized.release_id != args.expected_release_id:
        _emit_materialization_failure(args)
        return 1
    payload = {
        "schema": "dpone.dbt-release-materialization.v1",
        "passed": True,
        "release_id": materialized.release_id,
        "release_dir": materialized.release_dir.as_posix(),
        "no_op": materialized.no_op,
    }
    if args.format == "json":
        print(json.dumps(payload, allow_nan=False, ensure_ascii=False, indent=2))
    else:
        print(f"dbt release materialized: {materialized.release_id}")
    return 0


def cmd_verify_dev_evidence(
    args: argparse.Namespace,
    *,
    ctx: object,
    logger: logging.Logger,
) -> int:
    """Block promotion unless every pinned workload passed in dev."""

    del ctx, logger
    report = build_dbt_dev_evidence_verification_service().verify(
        compiled_root=Path(args.compiled_root),
        evidence_root=Path(args.evidence_root),
        expected_release_id=args.expected_release_id,
        expected_deployment_id=args.expected_deployment_id,
        expected_activation_id=args.expected_activation_id,
        require_exact_activation=True,
    )
    if not report.passed:
        emit_failure(
            (
                DbtPublishIssue(
                    code=report.code,
                    message=("Current dev runtime evidence does not prove every workload in the pinned release"),
                    path=Path(args.evidence_root).as_posix(),
                    remediation=(
                        "Run the exact dev deployment, collect final dbt and Airflow evidence, then retry promotion."
                    ),
                ),
            ),
            args.format,
            stage="dbt_promotion",
        )
        return 1
    payload = report.to_dict()
    if args.format == "json":
        print(json.dumps(payload, allow_nan=False, ensure_ascii=False, indent=2))
    else:
        print(f"dbt dev evidence verified: {len(report.verified_workloads)} workload(s)")
    return 0


def cmd_render_ci_report(
    args: argparse.Namespace,
    *,
    ctx: object,
    logger: logging.Logger,
) -> int:
    """Print a bounded Markdown projection from the compiled evidence."""

    del ctx, logger
    report = _read_json_object(
        Path(args.compiled_root).absolute(),
        "_dbt/dbt-publish-evidence.json",
        max_bytes=_MAX_RELEASE_BYTES,
    )
    if not report:
        emit_failure(
            (
                DbtPublishIssue(
                    code="DPONE_DBT_PROMOTION_SOURCE_DRIFT",
                    message="Compiled dbt evidence is missing or invalid",
                    path=Path(args.compiled_root).as_posix(),
                    remediation="Re-run the dev compile and upload the complete compiled tree.",
                ),
            ),
            "text",
            stage="dbt_promotion",
        )
        return 1
    print(
        render_dbt_promotion_ci_report(
            report,
            airflow_base_url=args.airflow_base_url,
        ),
        end="",
    )
    return 0


def _emit_materialization_failure(args: argparse.Namespace, exc: DbtReleaseMaterializationError | None = None) -> None:
    cache_lock_failed = exc is not None and exc.code == "DPONE_DBT_RELEASE_CACHE_LOCK_FAILED"
    error_code = exc.code if exc is not None and cache_lock_failed else "DPONE_DBT_RELEASE_INTEGRITY_INVALID"
    emit_failure(
        (
            DbtPublishIssue(
                code=error_code,
                message=(
                    "The local dpone cache writer lease could not be initialized"
                    if cache_lock_failed
                    else "Downloaded release bytes do not match the requested release"
                ),
                path=(Path(args.cache_root).as_posix() if cache_lock_failed else Path(args.compiled_root).as_posix()),
                remediation=(
                    "Check cache-root permissions and require .promotion.lock to be a regular writable file."
                    if cache_lock_failed
                    else "Fetch the attested dev release again; do not rebuild it in prod."
                ),
            ),
        ),
        args.format,
        stage="dbt_promotion",
    )


def _read_json_object(
    root: Path,
    relative_path: str,
    *,
    max_bytes: int,
) -> Mapping[str, Any]:
    try:
        raw = read_confined_file(
            root,
            relative_path,
            max_bytes=max_bytes,
        )
        payload = strict_json_object(raw)
    except (OSError, ValueError, StrictJsonError):
        return {}
    return payload


__all__ = [
    "cmd_materialize_release",
    "cmd_render_ci_report",
    "cmd_verify_dev_evidence",
    "cmd_verify_promotion",
    "register_materialize_release_parser",
    "register_render_ci_report_parser",
    "register_verify_dev_evidence_parser",
    "register_verify_promotion_parser",
]
