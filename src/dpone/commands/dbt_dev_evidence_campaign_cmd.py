"""CLI adapter for one trusted Airflow dbt evidence campaign."""

from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path

from dpone.adapters.dbt_dev_evidence_campaign_journal import (
    DbtDevEvidenceCampaignJournalError,
)
from dpone.app.dbt_promotion_composition import (
    DbtDevEvidenceCampaignError,
    DbtDevEvidenceRequest,
    DbtDevEvidenceRequestError,
    build_dbt_dev_evidence_campaign_service,
)
from dpone.contracts.dbt_publish_models import DbtPublishIssue
from dpone.ports.dbt_airflow_evidence import (
    DbtAirflowEvidenceConfigurationError,
)

from .dbt_dev_evidence_request_file import (
    DevEvidenceRequestFileError,
    read_dev_evidence_request,
)
from .dbt_publish_cli_support import emit_failure, emit_internal_failure

_CAMPAIGN_TIMEOUT_RANGE = (30, 7_200)
_POLL_INTERVAL_RANGE = (1, 60)
_REQUEST_TIMEOUT_RANGE = (1, 60)


def register_run_dev_evidence_campaign_parser(
    subparsers: argparse._SubParsersAction,
) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "run-dev-evidence-campaign",
        help="Trigger and wait for every Airflow workflow in a trusted evidence request",
    )
    parser.add_argument("--request", required=True)
    parser.add_argument(
        "--evidence-root",
        required=True,
        help="Protected shared root for create-only campaign authority and closure",
    )
    parser.add_argument("--airflow-api-url", required=True)
    parser.add_argument(
        "--airflow-api-version",
        choices=["v1", "v2"],
        required=True,
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=1_800,
        help="Whole-campaign timeout in seconds (default: 1800; range: 30..7200)",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=int,
        default=5,
        help="Airflow polling interval in seconds (default: 5; range: 1..60 and not above timeout)",
    )
    parser.add_argument(
        "--request-timeout-seconds",
        type=int,
        default=10,
        help="Per-request HTTP timeout in seconds (default: 10; range: 1..60)",
    )
    parser.add_argument("--format", choices=["text", "json"], default="text")
    return parser


def cmd_run_dev_evidence_campaign(
    args: argparse.Namespace,
    *,
    ctx: object,
    logger: logging.Logger,
) -> int:
    del ctx, logger
    limit_issue = _campaign_limit_issue(args)
    if limit_issue is not None:
        emit_failure((limit_issue,), args.format, stage="dbt_dev_evidence")
        return 2
    try:
        request = DbtDevEvidenceRequest.from_mapping(read_dev_evidence_request(Path(args.request)))
    except (
        DbtDevEvidenceRequestError,
        DevEvidenceRequestFileError,
        OSError,
    ) as exc:
        emit_failure(
            (
                DbtPublishIssue(
                    code=getattr(exc, "code", "DPONE_DBT_DEV_EVIDENCE_REQUEST_INVALID"),
                    message="Dev evidence campaign request is missing or invalid",
                    path=Path(args.request).as_posix(),
                    remediation=(
                        "Generate the request from the exact compiled release "
                        "with `dpone dbt prepare-dev-evidence-request`."
                    ),
                ),
            ),
            args.format,
            stage="dbt_dev_evidence",
        )
        return 2
    return _run_campaign(args, request)


def _run_campaign(
    args: argparse.Namespace,
    request: DbtDevEvidenceRequest,
) -> int:
    try:
        report = build_dbt_dev_evidence_campaign_service(
            airflow_api_url=args.airflow_api_url,
            airflow_api_version=args.airflow_api_version,
            bearer_token=os.environ.get("DPONE_AIRFLOW_API_TOKEN", ""),
            evidence_root=Path(args.evidence_root),
            request_timeout_seconds=args.request_timeout_seconds,
        ).run(
            request,
            timeout_seconds=args.timeout_seconds,
            poll_interval_seconds=args.poll_interval_seconds,
        )
    except (
        DbtAirflowEvidenceConfigurationError,
        DbtDevEvidenceCampaignJournalError,
    ) as exc:
        emit_failure(
            (
                DbtPublishIssue(
                    code=getattr(exc, "code", "DPONE_DBT_DEV_EVIDENCE_INTEGRITY_INVALID"),
                    message="Dev evidence campaign configuration is unsafe",
                    path=Path(args.request).as_posix(),
                    remediation="Use the protected Airflow origin/token and a confined platform-owned evidence root.",
                ),
            ),
            args.format,
            stage="dbt_dev_evidence",
        )
        return 4
    except DbtDevEvidenceCampaignError as exc:
        emit_failure(
            (
                DbtPublishIssue(
                    code=exc.code,
                    message="Airflow did not produce the complete dev evidence campaign",
                    path=Path(args.request).as_posix(),
                    remediation=(
                        "Check the trusted Airflow service and exact dev deployment, then rerun this workflow attempt."
                    ),
                ),
            ),
            args.format,
            stage="dbt_dev_evidence",
        )
        return 3
    except Exception:
        emit_internal_failure(args.format, stage="dbt_dev_evidence")
        return 5
    return _emit_success(args, report.to_dict())


def _campaign_limit_issue(args: argparse.Namespace) -> DbtPublishIssue | None:
    values = (
        (
            "timeout_seconds",
            args.timeout_seconds,
            _CAMPAIGN_TIMEOUT_RANGE,
            "Set --timeout-seconds to an integer from 30 through 7200.",
        ),
        (
            "poll_interval_seconds",
            args.poll_interval_seconds,
            _POLL_INTERVAL_RANGE,
            "Set --poll-interval-seconds to an integer from 1 through 60.",
        ),
        (
            "request_timeout_seconds",
            args.request_timeout_seconds,
            _REQUEST_TIMEOUT_RANGE,
            "Set --request-timeout-seconds to an integer from 1 through 60.",
        ),
    )
    for path, value, bounds, remediation in values:
        minimum, maximum = bounds
        if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
            return DbtPublishIssue(
                code="DPONE_DBT_DEV_EVIDENCE_LIMIT_INVALID",
                message=f"{path} must be between {minimum} and {maximum} seconds",
                path=path,
                remediation=remediation,
            )
    if args.poll_interval_seconds > args.timeout_seconds:
        return DbtPublishIssue(
            code="DPONE_DBT_DEV_EVIDENCE_LIMIT_INVALID",
            message="poll_interval_seconds cannot exceed timeout_seconds",
            path="poll_interval_seconds",
            remediation="Reduce --poll-interval-seconds or increase --timeout-seconds, then rerun the command.",
        )
    return None


def _emit_success(
    args: argparse.Namespace,
    payload: dict[str, object],
) -> int:
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
        print(f"dbt dev evidence campaign passed: {payload['evidence_set_id']}")
    return 0


__all__ = [
    "cmd_run_dev_evidence_campaign",
    "register_run_dev_evidence_campaign_parser",
]
