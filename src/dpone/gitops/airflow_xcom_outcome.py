from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.gitops.airflow_runtime_models import GitOpsAirflowRuntimeEvidence, GitOpsAirflowRuntimeStep


from collections.abc import Mapping
from typing import Any

from dpone.contracts.airflow_run_identity import (
    AIRFLOW_RUN_IDENTITY_ENV,
    AirflowDeploymentIdentityError,
    AirflowRunIdentityError,
    parse_airflow_deployment_identity_json,
    parse_airflow_run_identity_json,
)
from dpone.gitops.airflow_interval_env import run_interval_from_env
from dpone.gitops.airflow_runtime_profile_models import GitOpsAirflowXComSummary
from dpone.gitops.airflow_xcom_inline_evidence import build_inline_runtime_evidence
from dpone.gitops.models import GitOpsIssue
from dpone.security_redaction import public_path_label, redact_absolute_paths, redact_text

_BACKFILL_SUMMARY_FIELDS = (
    "run_key",
    "dataset",
    "inner_mode",
    "operation_status",
    "retry_policy",
    "state_authority",
    "state_cache_status",
    "state_path",
    "chunks_total",
    "chunks_selected",
    "chunks_committed",
    "chunks_failed",
    "chunks_skipped_resume",
    "verification",
    "mapping",
)
_COMMIT_UNKNOWN_RECOVERY_FIELDS = (
    "failure_boundary",
    "target_state",
    "checkpoint_state",
    "source_state",
    "safe_to_retry",
    "operator_verification_required",
    "recovery_action",
)


def parse_optional_airflow_run_identity(raw: object) -> dict[str, Any] | None:
    """Validate the optional provider-owned runtime identity environment value."""

    if raw is None or raw == "":
        return None
    return parse_airflow_run_identity_json(str(raw)).to_dict()


def parse_optional_airflow_deployment_identity(raw: object) -> dict[str, str] | None:
    """Validate the exact activation identity injected beside run identity."""

    if raw is None or raw == "":
        return None
    return parse_airflow_deployment_identity_json(str(raw)).to_dict()


class GitOpsAirflowXComOutcomeBuilder:
    """Build the final XCom payload emitted by the custom dpone Airflow image."""

    def build(
        self,
        *,
        evidence: GitOpsAirflowRuntimeEvidence,
        runtime_evidence_path: str,
        runtime_evidence_sha256: str,
        runtime_profile_path: str = "",
        inline_payload: dict[str, object] | None = None,
        run_identity: Mapping[str, Any] | None = None,
        deployment_identity: Mapping[str, str] | None = None,
        dbt_execution_evidence_ref: Mapping[str, Any] | None = None,
    ) -> GitOpsAirflowXComSummary:
        _require_matching_deployment_identity(run_identity, deployment_identity)
        failed_step = _first_failed_required_step(evidence.steps)
        blockers = (*(_public_issue(issue) for issue in evidence.blockers), *_runtime_step_blockers(evidence.steps))
        warnings = tuple(_public_issue(issue) for issue in evidence.warnings)
        public_runtime_evidence_path = public_path_label(
            runtime_evidence_path,
            fallback="runtime-evidence.json",
        )
        return GitOpsAirflowXComSummary(
            runtime_profile_path=public_path_label(runtime_profile_path, fallback="runtime-profile.json"),
            run_spec_path=public_path_label(evidence.run_spec_path, fallback="run-spec.json"),
            runtime_evidence_path=public_runtime_evidence_path,
            runtime_evidence_sha256=runtime_evidence_sha256,
            runtime_evidence=build_inline_runtime_evidence(evidence, inline_payload=inline_payload),
            status=evidence.status,
            failed_step=failed_step.name if failed_step is not None else None,
            step_counts=_step_counts(evidence.steps),
            artifact_paths={"runtime_evidence": public_runtime_evidence_path},
            warnings=warnings,
            interval=_interval_section(),
            backfill=_backfill_section(inline_payload),
            recovery=_recovery_section(inline_payload),
            run_identity=dict(run_identity) if run_identity is not None else None,
            deployment_identity=(dict(deployment_identity) if deployment_identity is not None else None),
            dbt_execution_evidence_ref=(
                dict(dbt_execution_evidence_ref) if dbt_execution_evidence_ref is not None else None
            ),
            blockers=blockers,
            producer="dpone gitops airflow run-spec-exec",
        )


def _require_matching_deployment_identity(
    run_identity: Mapping[str, Any] | None,
    deployment_identity: Mapping[str, str] | None,
) -> None:
    if deployment_identity is None:
        return
    if run_identity is None:
        raise AirflowDeploymentIdentityError("deployment identity requires run identity")
    mismatched = [
        field for field in ("release_id", "deployment_id") if deployment_identity.get(field) != run_identity.get(field)
    ]
    if mismatched:
        raise AirflowDeploymentIdentityError(
            "deployment identity does not match run identity: " + ", ".join(mismatched)
        )


def _interval_section() -> dict[str, Any] | None:
    """Data interval of the current DAG run (from the DPONE_* env contract)."""

    interval = run_interval_from_env()
    return None if interval.is_empty else dict(interval.to_jsonable())


def _backfill_section(inline_payload: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """Bounded backfill progress from ``dpone run --format json`` evidence.

    Per-chunk details stay in the runtime evidence file and the backfill
    ledger; XCom carries only campaign-level counters so payloads stay small.
    """

    if not isinstance(inline_payload, Mapping):
        return None
    result = inline_payload.get("result")
    details = result.get("details") if isinstance(result, Mapping) else None
    backfill = details.get("backfill") if isinstance(details, Mapping) else None
    if not isinstance(backfill, Mapping):
        return None
    return {key: backfill[key] for key in _BACKFILL_SUMMARY_FIELDS if key in backfill}


def _recovery_section(inline_payload: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """Preserve the bounded manual-recovery contract for commit uncertainty."""

    if not isinstance(inline_payload, Mapping):
        return None
    result = inline_payload.get("result")
    if not isinstance(result, Mapping) or result.get("error_code") != "COMMIT_UNKNOWN":
        return None
    recovery = {key: result.get(key) for key in _COMMIT_UNKNOWN_RECOVERY_FIELDS}
    if (
        recovery["safe_to_retry"] is not False
        or recovery["operator_verification_required"] is not True
        or recovery["recovery_action"] != "operator_verification_required"
    ):
        return None
    return {"code": "COMMIT_UNKNOWN", **recovery}


def _first_failed_required_step(steps: tuple[GitOpsAirflowRuntimeStep, ...]) -> GitOpsAirflowRuntimeStep | None:
    for step in steps:
        if step.required and not step.passed:
            return step
    return None


def _runtime_step_blockers(steps: tuple[GitOpsAirflowRuntimeStep, ...]) -> tuple[GitOpsIssue, ...]:
    return tuple(
        GitOpsIssue(
            code="runtime_step_failed",
            message=f"Runtime step `{step.name}` failed with exit code {step.exit_code}",
            path=public_path_label(step.manifest or step.name, fallback=step.name or "runtime-step"),
            source="dpone gitops airflow run-spec-exec",
        )
        for step in steps
        if step.required and not step.passed
    )


def _public_issue(issue: GitOpsIssue) -> GitOpsIssue:
    return GitOpsIssue(
        code=issue.code,
        message=redact_absolute_paths(redact_text(issue.message)),
        path=public_path_label(issue.path, fallback="runtime-artifact"),
        source=issue.source,
    )


def _step_counts(steps: tuple[GitOpsAirflowRuntimeStep, ...]) -> dict[str, int]:
    passed = sum(1 for step in steps if step.passed)
    failed = sum(1 for step in steps if not step.passed)
    return {
        "total": len(steps),
        "passed": passed,
        "failed": failed,
    }


__all__ = [
    "AIRFLOW_RUN_IDENTITY_ENV",
    "AirflowRunIdentityError",
    "GitOpsAirflowXComOutcomeBuilder",
    "parse_optional_airflow_deployment_identity",
    "parse_optional_airflow_run_identity",
]
