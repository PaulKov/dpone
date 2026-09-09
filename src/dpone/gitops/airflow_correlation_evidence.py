"""Join verified Airflow evidence observations into one correlation contract."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from importlib import import_module
from typing import Any

from dpone.gitops.models import GitOpsIssue

_SOURCE = "dpone gitops airflow evidence-bundle"


def _symbol(path: str) -> Any:
    module_name, attr = path.split(":", 1)
    return getattr(import_module(module_name), attr)


@dataclass(frozen=True, slots=True)
class AirflowCorrelationEvidenceResult:
    correlation: Any | None
    warnings: tuple[GitOpsIssue, ...] = ()
    blockers: tuple[GitOpsIssue, ...] = ()


def correlate_airflow_evidence(
    *,
    run_identity: Any | None,
    attempt: Mapping[str, object],
    pod: Mapping[str, object],
    payloads: Mapping[str, Mapping[str, Any]],
    artifact_sha256: Mapping[str, str],
    runner_policy: str,
) -> AirflowCorrelationEvidenceResult:
    """Build correlation and classify incomplete versus contradictory evidence."""

    if run_identity is None:
        return AirflowCorrelationEvidenceResult(correlation=None)
    xcom = payloads.get("xcom_summary") or {}
    inline = _mapping(xcom.get("runtime_evidence"))
    dpone_run = _mapping(inline.get("dpone_run"))
    runtime_evidence_sha256 = _optional_string(xcom.get("runtime_evidence_sha256"))
    observed_evidence_sha256 = _prefixed_digest(artifact_sha256.get("runtime_evidence"))
    if (
        runtime_evidence_sha256 is not None
        and observed_evidence_sha256 is not None
        and runtime_evidence_sha256 != observed_evidence_sha256
    ):
        return AirflowCorrelationEvidenceResult(
            correlation=None,
            blockers=(
                _issue(
                    code="DPONE_AIRFLOW_CORRELATION_MISMATCH",
                    message="XCom runtime evidence digest does not match the collected runtime evidence artifact",
                    path="xcom_summary.runtime_evidence_sha256",
                ),
            ),
        )
    try:
        correlation = _symbol("dpone.contracts.airflow_correlation:build_airflow_correlation")(
            run_identity=run_identity,
            attempt=attempt,
            dpone_run_id=_optional_string(dpone_run.get("run_id")),
            dpone_process=_optional_string(dpone_run.get("process")),
            runtime_evidence_sha256=runtime_evidence_sha256,
            pod=pod,
        )
    except ValueError as exc:
        return AirflowCorrelationEvidenceResult(
            correlation=None,
            blockers=(
                _issue(
                    code=str(getattr(exc, "code", "DPONE_AIRFLOW_CORRELATION_INVALID")),
                    message="Airflow evidence observations cannot form a valid correlation",
                    path="correlation",
                ),
            ),
        )
    if correlation.complete:
        return AirflowCorrelationEvidenceResult(correlation=correlation)
    issue = _issue(
        code="DPONE_AIRFLOW_CORRELATION_INCOMPLETE",
        message="Airflow attempt correlation is missing mandatory runtime or pod observations",
        path=",".join(correlation.missing_fields),
    )
    if runner_policy == "release":
        return AirflowCorrelationEvidenceResult(correlation=correlation, blockers=(issue,))
    return AirflowCorrelationEvidenceResult(correlation=correlation, warnings=(issue,))


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _prefixed_digest(value: str | None) -> str | None:
    if value is None:
        return None
    return value if value.startswith("sha256:") else f"sha256:{value}"


def _issue(*, code: str, message: str, path: str) -> GitOpsIssue:
    return GitOpsIssue(code=code, message=message, path=path, source=_SOURCE)


__all__ = ["AirflowCorrelationEvidenceResult", "correlate_airflow_evidence"]
