"""CLI composition for retry-safe Airflow desired-state publication."""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from dpone.adapters.airflow_desired_state_publish_intent import (
    FileDesiredStatePublishIntentStore,
    FileDesiredStatePublishPreparationStore,
    commit_desired_state_publish_output,
)
from dpone.adapters.gitlab_protected_source import GitLabProtectedSourceAuthorizer
from dpone.contracts.airflow_desired_state import DesiredStateRevision
from dpone.contracts.airflow_desired_state_publish import DesiredStatePublishRequest
from dpone.readiness.airflow_desired_state_authority import (
    AUTHORITY_FILE_ENV,
    AirflowDesiredStateAuthority,
    load_airflow_desired_state_authority,
)
from dpone.readiness.airflow_desired_state_store import (
    DesiredStateStoreOptions,
    PromotionInput,
    load_promotion_input,
)
from dpone.services.airflow_desired_state import (
    AirflowDesiredStatePublisher,
    DesiredStatePublishError,
)
from dpone.services.airflow_desired_state_preparation import (
    AirflowDesiredStatePreparationService,
)


def register_prepare_parser(
    subparsers: argparse._SubParsersAction,
) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "prepare",
        help="Prepare one immutable cross-job desired-state publication",
    )
    parser.add_argument("--promotion-evidence", required=True)
    parser.add_argument("--expected-revision", required=True, help="Opaque revision or 'absent'")
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--status-output",
        help="Optional failure evidence JSON distinct from all inputs/output",
    )
    return parser


def register_publish_parser(
    subparsers: argparse._SubParsersAction,
) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "publish",
        help="Conditionally select one verified immutable deployment",
    )
    _add_store_arguments(parser)
    parser.add_argument("--promotion-evidence", required=True)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--preparation", help="Canonical cross-job preparation artifact")
    source.add_argument("--intent", help="Legacy same-job durable publish intent")
    parser.add_argument("--expected-revision", help="Required with legacy --intent")
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--status-output",
        help="Failure evidence JSON; required with --preparation and distinct from all inputs/output",
    )
    return parser


def cmd_prepare(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx
    output = Path(args.output)
    promotion_path = Path(args.promotion_evidence)
    status_output: Path | None = None
    try:
        operation_paths = {
            "output": output,
            "promotion_evidence": promotion_path,
            **_authority_path(),
        }
        requested_status = _optional_path(args.status_output)
        status_output = _validate_status_output(
            status_output=requested_status,
            operation_paths=operation_paths,
        )
        if status_output is not None:
            _require_output_file_path(status_output, field="status output")
        _require_distinct_operation_paths(operation_paths)
        _require_output_file_path(output, field="output")
        authority = load_airflow_desired_state_authority()
        promotion = load_promotion_input(promotion_path)
        service = _preparation_service()
        candidate = service.candidate(
            authority=authority,
            promotion=promotion,
            pipeline_id=_identity("CI_PIPELINE_ID"),
            preparation_job_id=_identity("CI_JOB_ID"),
            expected_revision=_revision(args.expected_revision),
        )
        FileDesiredStatePublishPreparationStore(output).resolve(
            candidate=candidate,
            preparation_factory=lambda: service.preparation(candidate),
        )
    except (OSError, ValueError) as exc:
        logger.warning("Airflow desired-state preparation failed: %s", type(exc).__name__)
        _write_failure(
            status_output,
            "DPONE_AIRFLOW_DESIRED_STATE_INPUT_INVALID",
            stage="airflow_desired_state_prepare",
            logger=logger,
        )
        return 2
    return 0


def cmd_publish(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx
    output = Path(args.output)
    status_output: Path | None = None
    failure_output: Path | None = None
    try:
        requested_status = _optional_path(args.status_output)
        if args.preparation is not None and requested_status is None:
            raise ValueError("--status-output is required with --preparation")
        control_path = _control_path(args)
        operation_paths = {
            "output": output,
            "promotion_evidence": Path(args.promotion_evidence),
            "control": control_path,
            **_authority_path(),
        }
        status_output = _validate_status_output(
            status_output=requested_status,
            operation_paths=operation_paths,
        )
        if status_output is not None:
            _require_output_file_path(status_output, field="status output")
        failure_output = status_output
        _require_distinct_operation_paths(operation_paths)
        _require_output_file_path(output, field="output")
        failure_output = status_output or (output if args.intent is not None else None)
        authority = load_airflow_desired_state_authority()
        promotion = load_promotion_input(Path(args.promotion_evidence))
        request, legacy_evidence = _resolve_input(
            args=args,
            authority=authority,
            promotion=promotion,
            control_path=control_path,
        )
        store = _store_options(args, authority).build()
        evidence = AirflowDesiredStatePublisher(
            reader=store,
            writer=store,
            source_authorizer=_source_authorizer(authority),
        ).publish(request)
        payload = evidence.to_v1_dict() if legacy_evidence else evidence.to_dict()
    except DesiredStatePublishError as exc:
        logger.warning("Airflow desired-state publish failed: %s", type(exc).__name__)
        _write_failure(
            failure_output,
            exc.code,
            stage="airflow_desired_state_publish",
            logger=logger,
            state_may_have_changed=exc.state_may_have_changed,
            legacy_compat=args.intent is not None,
        )
        return 4
    except (OSError, ValueError) as exc:
        logger.warning("Airflow desired-state publish failed: %s", type(exc).__name__)
        _write_failure(
            failure_output,
            "DPONE_AIRFLOW_DESIRED_STATE_INPUT_INVALID",
            stage="airflow_desired_state_publish",
            logger=logger,
            legacy_compat=args.intent is not None,
        )
        return 2
    except Exception as exc:  # noqa: BLE001 - public boundary redacts dependencies.
        logger.error("Airflow desired-state publish failed: %s", type(exc).__name__)
        _write_failure(
            failure_output,
            "DPONE_INTERNAL_AIRFLOW_DESIRED_STATE_PUBLISH_FAILED",
            stage="airflow_desired_state_publish",
            logger=logger,
            legacy_compat=args.intent is not None,
        )
        return 5
    try:
        _write_json(output, payload)
    except (OSError, ValueError) as exc:
        logger.error(
            "Airflow desired-state publish evidence commit failed: %s",
            type(exc).__name__,
        )
        _write_failure(
            failure_output,
            "DPONE_INTERNAL_AIRFLOW_DESIRED_STATE_PUBLISH_EVIDENCE_FAILED",
            stage="airflow_desired_state_publish",
            logger=logger,
            state_may_have_changed=True,
            legacy_compat=args.intent is not None,
        )
        return 5
    return 0


def _resolve_input(
    *,
    args: argparse.Namespace,
    authority: AirflowDesiredStateAuthority,
    promotion: PromotionInput,
    control_path: Path,
) -> tuple[DesiredStatePublishRequest, bool]:
    service = _preparation_service()
    pipeline_id = _identity("CI_PIPELINE_ID")
    publisher_job_id = _identity("CI_JOB_ID")
    if args.preparation is not None:
        if args.expected_revision is not None:
            raise ValueError("--expected-revision cannot be combined with --preparation")
        preparation = FileDesiredStatePublishPreparationStore(control_path).read()
        request = service.validate(
            preparation=preparation,
            authority=authority,
            promotion=promotion,
            pipeline_id=pipeline_id,
            publisher_job_id=publisher_job_id,
        )
        return request, False
    if args.intent is None or args.expected_revision is None:
        raise ValueError("legacy --intent requires --expected-revision")
    candidate = service.legacy_candidate(
        authority=authority,
        promotion=promotion,
        pipeline_id=pipeline_id,
        job_id=publisher_job_id,
        expected_revision=_revision(args.expected_revision),
    )
    intent = FileDesiredStatePublishIntentStore(control_path).resolve(
        candidate_sha256=candidate.sha256,
        clock=lambda: datetime.now(timezone.utc),  # noqa: UP017
        occurrence_id_factory=uuid4,
    )
    return DesiredStatePublishRequest(candidate=candidate, intent=intent), True


def _add_store_arguments(parser: argparse.ArgumentParser) -> None:
    access = parser.add_mutually_exclusive_group(required=True)
    access.add_argument("--identity-mode", choices=["workload_identity"])
    access.add_argument("--connection-id")
    parser.add_argument("--connection-type", choices=["airflow", "env", "vault"])


def _store_options(
    args: argparse.Namespace,
    authority: AirflowDesiredStateAuthority,
) -> DesiredStateStoreOptions:
    return DesiredStateStoreOptions(
        desired_uri=authority.desired_state_uri,
        certified_endpoint_url=authority.certified_s3_endpoint_url,
        identity_mode=args.identity_mode,
        connection_type=args.connection_type,
        connection_id=args.connection_id,
    )


def _source_authorizer(
    authority: AirflowDesiredStateAuthority,
) -> GitLabProtectedSourceAuthorizer:
    return GitLabProtectedSourceAuthorizer(
        api_url=_identity("CI_API_V4_URL"),
        project=authority.source_project,
        protected_ref=authority.source_ref,
        job_token=_identity("CI_JOB_TOKEN"),
    )


def _identity(environment_name: str) -> str:
    value = os.environ.get(environment_name)
    if not value:
        raise ValueError(f"{environment_name} is required")
    return value


def _preparation_service() -> AirflowDesiredStatePreparationService:
    return AirflowDesiredStatePreparationService(
        clock=lambda: datetime.now(timezone.utc),  # noqa: UP017
        occurrence_id_factory=uuid4,
    )


def _control_path(args: argparse.Namespace) -> Path:
    selected = args.preparation if args.preparation is not None else args.intent
    if selected is None:
        raise ValueError("one desired-state publication control input is required")
    return Path(selected)


def _optional_path(value: str | None) -> Path | None:
    return None if value is None else Path(value)


def _authority_path() -> dict[str, Path]:
    configured = os.environ.get(AUTHORITY_FILE_ENV)
    return {} if configured is None else {"authority": Path(configured)}


def _validate_status_output(
    *,
    status_output: Path | None,
    operation_paths: dict[str, Path],
) -> Path | None:
    if status_output is not None and any(_same_path(status_output, path) for path in operation_paths.values()):
        raise ValueError("Airflow desired-state status output aliases an operation path")
    return status_output


def _require_output_file_path(path: Path, *, field: str) -> None:
    if not path.name:
        raise ValueError(f"Airflow desired-state {field} path must name a file")
    if path.exists() and not path.is_file():
        raise ValueError(f"Airflow desired-state {field} path must be a file")
    parent = path.parent
    if parent.exists() and not parent.is_dir():
        raise ValueError(f"Airflow desired-state {field} parent must be a directory")


def _require_distinct_operation_paths(operation_paths: dict[str, Path]) -> None:
    names = tuple(operation_paths)
    for index, first_name in enumerate(names):
        for second_name in names[index + 1 :]:
            if _same_path(operation_paths[first_name], operation_paths[second_name]):
                raise ValueError("Airflow desired-state operation paths must be pairwise distinct")


def _same_path(first: Path, second: Path) -> bool:
    if first.resolve(strict=False) == second.resolve(strict=False):
        return True
    try:
        return first.samefile(second)
    except OSError:
        return False


def _write_failure(
    path: Path | None,
    code: str,
    *,
    stage: str,
    logger: logging.Logger,
    state_may_have_changed: bool | None = None,
    legacy_compat: bool = False,
) -> None:
    if path is None:
        return
    message = "Airflow desired-state operation failed."
    payload: dict[str, object] = {
        "schema": "dpone.error.v1",
        "code": code,
        "stage": stage,
        "severity": "error",
        "message": message,
    }
    if legacy_compat:
        payload["passed"] = False
        payload["errors"] = [{"code": code, "message": message}]
    if state_may_have_changed is not None:
        payload["state_may_have_changed"] = state_may_have_changed
    try:
        _write_json(path, payload)
    except (OSError, ValueError) as exc:
        logger.error(
            "Airflow desired-state failure evidence commit failed: %s",
            type(exc).__name__,
        )


def _revision(value: str) -> DesiredStateRevision | None:
    return None if value == "absent" else DesiredStateRevision(value)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    body = (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode()
    commit_desired_state_publish_output(path, body)


__all__ = [
    "cmd_prepare",
    "cmd_publish",
    "register_prepare_parser",
    "register_publish_parser",
]
