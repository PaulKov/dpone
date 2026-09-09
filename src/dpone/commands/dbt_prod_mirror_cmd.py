"""CLI adapter for bot-owned prod dbt source mirrors."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from dpone.app.dbt_promotion_composition import (
    DbtProdMirrorError,
    build_dbt_prod_mirror_service,
)
from dpone.contracts.dbt_publish_models import DbtPublishIssue

from .dbt_publish_cli_support import emit_failure, emit_internal_failure


def register_prepare_prod_mirror_parser(
    subparsers: argparse._SubParsersAction,
) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "prepare-prod-mirror",
        help="Prepare a byte-identical dbt mirror for a bot-owned prod PR",
    )
    parser.add_argument("--compiled-root", required=True)
    parser.add_argument("--repository-root", required=True)
    parser.add_argument("--mirror-path", required=True)
    parser.add_argument("--source-snapshot-path", required=True)
    parser.add_argument("--descriptor-path", required=True)
    parser.add_argument("--expected-release-id", required=True)
    parser.add_argument("--dev-deployment-id", required=True)
    parser.add_argument("--dev-evidence-ref", required=True)
    parser.add_argument("--dev-evidence-subject-sha256", required=True)
    parser.add_argument("--dev-evidence-artifact-name", required=True)
    parser.add_argument("--dev-evidence-producer-workflow", required=True)
    parser.add_argument("--dev-evidence-source-commit", required=True)
    parser.add_argument("--dev-evidence-set-id")
    parser.add_argument("--dev-evidence-campaign-request-sha256")
    parser.add_argument("--format", choices=["text", "json"], default="text")
    return parser


def cmd_prepare_prod_mirror(
    args: argparse.Namespace,
    *,
    ctx: object,
    logger: logging.Logger,
) -> int:
    del ctx, logger
    try:
        evidence_set_kwargs: dict[str, str] = {}
        if isinstance(args.dev_evidence_set_id, str):
            evidence_set_kwargs["dev_evidence_set_id"] = args.dev_evidence_set_id
        if isinstance(
            getattr(
                args,
                "dev_evidence_campaign_request_sha256",
                None,
            ),
            str,
        ):
            evidence_set_kwargs["dev_evidence_campaign_request_sha256"] = args.dev_evidence_campaign_request_sha256
        report = build_dbt_prod_mirror_service().prepare(
            compiled_root=Path(args.compiled_root),
            repository_root=Path(args.repository_root),
            mirror_path=args.mirror_path,
            source_snapshot_path=args.source_snapshot_path,
            descriptor_path=args.descriptor_path,
            expected_release_id=args.expected_release_id,
            dev_deployment_id=args.dev_deployment_id,
            dev_evidence_ref=args.dev_evidence_ref,
            dev_evidence_subject_sha256=args.dev_evidence_subject_sha256,
            dev_evidence_artifact_name=args.dev_evidence_artifact_name,
            dev_evidence_producer_workflow=args.dev_evidence_producer_workflow,
            dev_evidence_source_commit=args.dev_evidence_source_commit,
            **evidence_set_kwargs,
        )
    except DbtProdMirrorError:
        emit_failure(
            (
                DbtPublishIssue(
                    code="DPONE_DBT_PROMOTION_SOURCE_DRIFT",
                    message="The prod dbt source mirror could not be prepared safely",
                    path=Path(args.repository_root).as_posix(),
                    remediation=(
                        "Use an attested dev release and bot-owned paths in a "
                        "clean prod checkout; do not edit generated promotion metadata."
                    ),
                ),
            ),
            args.format,
            stage="dbt_prod_mirror",
        )
        return 1
    except Exception:
        emit_internal_failure(args.format, stage="dbt_prod_mirror")
        return 5
    payload = report.to_dict()
    if args.format == "json":
        print(json.dumps(payload, allow_nan=False, ensure_ascii=False, indent=2))
    else:
        print(f"dbt prod mirror prepared: {report.release_id} -> {report.mirror_path}")
    return 0


__all__ = [
    "cmd_prepare_prod_mirror",
    "register_prepare_prod_mirror_parser",
]
