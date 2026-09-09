from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class AirflowRuntimeEvidenceFinding:
    code: str
    message: str
    path: str


class GitOpsAirflowRuntimeEvidenceVerifier:
    """Evaluate runtime evidence against a run-spec without executing commands."""

    def verify(
        self,
        *,
        run_spec_path: str,
        run_spec: Mapping[str, Any],
        evidence: Any,
        require_all_steps: bool,
    ) -> tuple[AirflowRuntimeEvidenceFinding, ...]:
        findings: list[AirflowRuntimeEvidenceFinding] = []
        if getattr(evidence, "kind", "") != "gitops.airflow_runtime_evidence":
            findings.append(
                AirflowRuntimeEvidenceFinding(
                    code="runtime_evidence_kind_invalid",
                    message="Runtime evidence kind must be gitops.airflow_runtime_evidence",
                    path=run_spec_path,
                )
            )
        if getattr(evidence, "run_spec_path", "") != run_spec_path:
            findings.append(
                AirflowRuntimeEvidenceFinding(
                    code="runtime_run_spec_path_mismatch",
                    message="Runtime evidence was produced for a different run-spec path",
                    path=str(getattr(evidence, "run_spec_path", "")),
                )
            )
        findings.extend(_failed_step_findings(evidence))
        if require_all_steps:
            findings.extend(_missing_step_findings(run_spec=run_spec, evidence=evidence))
        return tuple(_dedupe_findings(findings))


def _failed_step_findings(evidence: Any) -> tuple[AirflowRuntimeEvidenceFinding, ...]:
    return tuple(
        AirflowRuntimeEvidenceFinding(
            code="runtime_step_failed",
            message=f"Runtime step `{step.name}` failed with exit code {step.exit_code}",
            path=step.manifest or step.name,
        )
        for step in tuple(getattr(evidence, "steps", ()))
        if bool(getattr(step, "required", True)) and not bool(getattr(step, "passed", False))
    )


def _missing_step_findings(
    *,
    run_spec: Mapping[str, Any],
    evidence: Any,
) -> tuple[AirflowRuntimeEvidenceFinding, ...]:
    evidence_names = {step.name for step in tuple(getattr(evidence, "steps", ()))}
    raw_steps = run_spec.get("steps")
    if not isinstance(raw_steps, list):
        return (
            AirflowRuntimeEvidenceFinding(
                code="run_spec_steps_missing",
                message="Run-spec does not contain a steps list",
                path=str(run_spec.get("run_spec_path") or "steps"),
            ),
        )
    findings: list[AirflowRuntimeEvidenceFinding] = []
    for raw_step in raw_steps:
        if not isinstance(raw_step, Mapping):
            continue
        name = str(raw_step.get("name") or "")
        required = bool(raw_step.get("required", True))
        if required and name and name not in evidence_names:
            findings.append(
                AirflowRuntimeEvidenceFinding(
                    code="runtime_step_missing",
                    message=f"Runtime evidence is missing required step `{name}`",
                    path=name,
                )
            )
    return tuple(findings)


def _dedupe_findings(
    findings: list[AirflowRuntimeEvidenceFinding],
) -> tuple[AirflowRuntimeEvidenceFinding, ...]:
    seen: set[tuple[str, str]] = set()
    deduped: list[AirflowRuntimeEvidenceFinding] = []
    for finding in findings:
        key = (finding.code, finding.path)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(finding)
    return tuple(deduped)


__all__ = ["AirflowRuntimeEvidenceFinding", "GitOpsAirflowRuntimeEvidenceVerifier"]
