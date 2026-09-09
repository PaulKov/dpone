"""Build compact evidence chains for agent pull-request receipts."""

from __future__ import annotations

from typing import Any


def evidence_chain_payload(
    evidence: Any | None,
    *,
    self_check_name: str,
    selected_governance_artifact: Any | None,
) -> dict[str, Any] | None:
    """Convert raw GitHub evidence into a compact PR audit chain."""

    if evidence is None:
        return None
    observed_checks = _observed_checks_by_name(evidence)
    return {
        "head_sha": evidence.head_sha,
        "required_checks": [
            _check_payload(check, observed_checks.get(check))
            for check in sorted({item for item in evidence.required_checks if item != self_check_name})
        ],
        "governance_artifact": _artifact_payload(selected_governance_artifact),
    }


def _observed_checks_by_name(evidence: Any) -> dict[str, Any]:
    observed: dict[str, Any] = {}
    for item in [*evidence.check_runs, *evidence.statuses]:
        observed.setdefault(item.name, item)
    return observed


def _check_payload(name: str, evidence: Any | None) -> dict[str, Any]:
    if evidence is None:
        return {
            "name": name,
            "source": None,
            "id": None,
            "workflow_run_id": None,
            "status": None,
            "conclusion": None,
            "state": None,
            "url": None,
            "completed_at": None,
        }
    return {
        "name": name,
        "source": evidence.source,
        "id": evidence.id,
        "workflow_run_id": evidence.workflow_run_id,
        "status": evidence.status,
        "conclusion": evidence.conclusion,
        "state": evidence.state,
        "url": evidence.url,
        "completed_at": evidence.completed_at,
    }


def _artifact_payload(artifact: Any | None) -> dict[str, Any] | None:
    if artifact is None:
        return None
    content = getattr(artifact, "content", None)
    return {
        "name": artifact.name,
        "artifact_id": artifact.artifact_id,
        "workflow_run_id": artifact.workflow_run_id,
        "workflow_run_head_sha": artifact.workflow_run_head_sha,
        "digest": artifact.digest,
        "archive_sha256": artifact.archive_sha256,
        "archive_size_bytes": artifact.archive_size_bytes,
        "expired": artifact.expired,
        "url": artifact.url,
        "content_status": getattr(content, "status", None),
        "content_control_surface_changed": getattr(content, "control_surface_changed", None),
        "content_changed_paths": list(getattr(content, "changed_paths", [])),
        "content_checks": _content_checks(content),
        "attestation": _attestation_payload(getattr(artifact, "attestation", None)),
    }


def _content_checks(content: Any | None) -> list[dict[str, str | None]]:
    checks = getattr(content, "checks", None)
    if not isinstance(checks, list):
        return []
    return [
        {
            "name": check.get("name"),
            "status": check.get("status"),
        }
        for check in checks
        if isinstance(check, dict)
    ]


def _attestation_payload(attestation: Any | None) -> dict[str, Any] | None:
    if attestation is None:
        return None
    return {
        "status": getattr(attestation, "status", None),
        "predicate_type": getattr(attestation, "predicate_type", None),
        "subject_sha256": getattr(attestation, "subject_sha256", None),
        "source_repository": getattr(attestation, "source_repository", None),
        "source_ref": getattr(attestation, "source_ref", None),
        "source_digest": getattr(attestation, "source_digest", None),
        "signer_workflow": getattr(attestation, "signer_workflow", None),
        "issuer": getattr(attestation, "issuer", None),
        "verified_timestamps_count": getattr(attestation, "verified_timestamps_count", None),
        "runner_environment": getattr(attestation, "runner_environment", None),
        "errors": list(getattr(attestation, "errors", [])),
    }
