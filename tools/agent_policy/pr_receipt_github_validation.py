"""Validation helpers for live GitHub PR receipt evidence."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any


def load_sibling(module_name: str, filename: str) -> Any:
    """Load a sibling policy module when this file is executed by path."""

    if module_name in sys.modules:
        return sys.modules[module_name]
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


github_metadata = load_sibling("dpone_agent_pr_receipt_github_metadata", "pr_receipt_github_metadata.py")
github_attestation = load_sibling("dpone_agent_pr_receipt_github_attestation", "pr_receipt_github_attestation.py")
governance_content = load_sibling("dpone_agent_governance_artifact_content", "governance_artifact_content.py")


def validate_required_check_evidence(evidence: Any, self_check_name: str, errors: list[str]) -> None:
    """Append errors for missing or unsuccessful live required checks."""

    required_checks = sorted({check for check in evidence.required_checks if check != self_check_name})
    if not required_checks:
        errors.append("No live required GitHub checks were discovered for receipt validation.")
        return

    observed = _observed_checks_by_name(evidence)
    for check in required_checks:
        check_evidence = observed.get(check)
        if check_evidence is None:
            errors.append(f"Required GitHub check '{check}' is missing on head {evidence.head_sha}.")
        elif not check_evidence.successful:
            errors.append(
                f"Required GitHub check '{check}' is not successful on head {evidence.head_sha} "
                f"(observed {check_evidence.source}: {check_evidence.observed_state})."
            )


def validate_governance_artifact_evidence(
    evidence: Any,
    governance_artifact_name: str,
    errors: list[str],
    *,
    expected_changed_paths: list[str],
    control_surface_changed: bool,
    require_attestation: bool = False,
    expected_attestation_repository: str | None = None,
    expected_attestation_source_ref: str | None = None,
) -> Any | None:
    """Append errors and return the exact governance artifact that validated."""

    candidates = sorted(
        (
            artifact
            for artifact in evidence.artifacts
            if artifact.name == governance_artifact_name and artifact.workflow_run_head_sha == evidence.head_sha
        ),
        key=lambda artifact: (
            getattr(artifact, "created_at", None) or "",
            getattr(artifact, "artifact_id", None) or 0,
        ),
        reverse=True,
    )
    if not candidates:
        errors.append(f"Required GitHub artifact '{governance_artifact_name}' is missing for head {evidence.head_sha}.")
        return None
    artifact = candidates[0]
    artifact_errors: list[str] = []
    if artifact.expired:
        artifact_errors.append(
            f"Required GitHub artifact '{governance_artifact_name}' for head {evidence.head_sha} is expired."
        )
    artifact_errors.extend(
        github_metadata.governance_artifact_identity_errors(
            artifact=artifact,
            governance_artifact_name=governance_artifact_name,
            head_sha=evidence.head_sha,
        )
    )
    artifact_errors.extend(
        governance_content.validate_content_for_receipt(
            content=artifact.content,
            expected_changed_paths=expected_changed_paths,
            control_surface_changed=control_surface_changed,
            governance_artifact_name=governance_artifact_name,
            head_sha=evidence.head_sha,
        )
    )
    if require_attestation:
        if expected_attestation_repository is None or expected_attestation_source_ref is None:
            artifact_errors.append(
                "Required GitHub artifact attestation validation requires canonical repository and pull-request ref."
            )
        else:
            artifact_errors.extend(
                github_attestation.governance_attestation.validate_attestation_for_receipt(
                    attestation=getattr(artifact, "attestation", None),
                    governance_artifact_name=governance_artifact_name,
                    head_sha=evidence.head_sha,
                    expected_subject_sha256=getattr(artifact.content, "subject_sha256", None),
                    expected_repository=expected_attestation_repository,
                    expected_source_ref=expected_attestation_source_ref,
                )
            )
    if artifact_errors:
        errors.extend(dict.fromkeys(artifact_errors))
        return None
    return artifact


def _observed_checks_by_name(evidence: Any) -> dict[str, Any]:
    observed: dict[str, Any] = {}
    for item in [*evidence.check_runs, *evidence.statuses]:
        observed.setdefault(item.name, item)
    return observed
