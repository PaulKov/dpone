"""Text output for Airflow deployment-attestation lifecycle commands."""

from __future__ import annotations

from collections.abc import Mapping


def airflow_attestation_prepare_text(payload: Mapping[str, object]) -> str:
    if payload.get("passed") is not True:
        return _failure_text(payload, "prepare")
    return "\n".join(
        (
            "dpone airflow artifact-attestation prepare: PREPARED",
            f"- attestation id: {payload.get('attestation_id', 'unknown')}",
            f"- statement sha256: {payload.get('statement_sha256', 'unknown')}",
            f"- statement: {payload.get('output_path', 'unknown')}",
            "- next: sign the exact statement with protected cosign key material",
            "",
        )
    )


def airflow_attestation_publish_text(payload: Mapping[str, object]) -> str:
    if payload.get("passed") is not True:
        return _failure_text(payload, "publish")
    verification = payload.get("verification")
    decision = verification.get("decision") if isinstance(verification, Mapping) else "unknown"
    return "\n".join(
        (
            "dpone airflow artifact-attestation publish: PUBLISHED",
            f"- status: {payload.get('status', 'unknown')}",
            f"- deployment id: {payload.get('deployment_id', 'unknown')}",
            f"- attestation id: {payload.get('attestation_id', 'unknown')}",
            f"- verification: {decision}",
            f"- verified immutable objects: {payload.get('verified_objects', 0)}",
            "",
        )
    )


def airflow_trust_policy_render_text(payload: Mapping[str, object]) -> str:
    if payload.get("passed") is not True:
        return _failure_text(payload, "policy render")
    return "\n".join(
        (
            "dpone airflow artifact-attestation policy render: RENDERED",
            f"- policy fingerprint: {payload.get('policy_fingerprint', 'unknown')}",
            f"- policy sha256: {payload.get('policy_sha256', 'unknown')}",
            f"- canonical policy: {payload.get('output_path', 'unknown')}",
            "- next: mount these exact bytes and pin policy_sha256 in desired state",
            "",
        )
    )


def _failure_text(payload: Mapping[str, object], action: str) -> str:
    lines = [f"dpone airflow artifact-attestation {action}: FAILED"]
    errors = payload.get("errors")
    if isinstance(errors, list):
        for error in errors:
            if isinstance(error, Mapping):
                lines.append(f"- {error.get('code', 'unknown')}: {error.get('message', '')}")
                fixes = error.get("fixes")
                if isinstance(fixes, list):
                    for fix in fixes:
                        if isinstance(fix, Mapping) and fix.get("id"):
                            lines.append(f"  action: {fix['id']}")
                if error.get("docs_url"):
                    lines.append(f"  runbook: {error['docs_url']}")
    return "\n".join(lines) + "\n"


__all__ = [
    "airflow_attestation_prepare_text",
    "airflow_attestation_publish_text",
    "airflow_trust_policy_render_text",
]
