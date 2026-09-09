"""Human-readable rendering for Airflow deployment cache sync."""

from __future__ import annotations

from collections.abc import Mapping

from dpone.commands.docs_url_rendering import public_docs_url


def self_service_cache_sync_text(payload: Mapping[str, object]) -> str:
    """Render current-pointer promotion details without exposing local paths."""

    status = "OK" if bool(payload.get("passed")) else "FAILED"
    lines = [
        f"dpone airflow cache sync: {status}",
        f"- environment: {payload.get('environment') or 'unknown'}",
    ]
    deployment_id = _optional_text(payload.get("deployment_id"))
    if deployment_id:
        lines.append(f"- promoted deployment: {deployment_id}")
    previous_deployment_id = _optional_text(payload.get("previous_deployment_id"))
    if previous_deployment_id:
        lines.append(f"- previous deployment: {previous_deployment_id}")
    release_id = _optional_text(payload.get("release_id"))
    if release_id:
        lines.append(f"- release: {release_id}")
    promoted_by = _optional_text(payload.get("promoted_by"))
    if promoted_by:
        lines.append(f"- promoted by: {promoted_by}")
    source_commit = _optional_text(payload.get("source_commit"))
    if source_commit:
        lines.append(f"- source commit: {source_commit}")
    attestation_ref = _optional_text(payload.get("attestation_ref"))
    if attestation_ref:
        lines.append(f"- attestation: {attestation_ref}")
    if status == "OK":
        lines.append("- current pointer: updated")
        lines.append("- action: run dpone airflow cache-recovery-plan to verify cache health")
    else:
        error = _first_error(payload.get("errors"))
        code = ""
        if error:
            code = _optional_text(error.get("code")) or "DPONE_CACHE_SYNC_FAILED"
            message = _optional_text(error.get("message")) or "cache promotion failed"
            lines.append(f"- issue: {code}: {message}")
            docs_url = _optional_text(error.get("docs_url"))
            if docs_url:
                lines.append(f"- help: {public_docs_url(docs_url)}")
        recovery_required = bool(payload.get("recovery_required")) or (
            code == "DPONE_CACHE_PROMOTION_WRITE_FAILED" and "recovery_required" not in payload
        )
        pointer_state = "recovery required" if recovery_required else "not changed"
        lines.append(f"- current pointer: {pointer_state}")
        lines.append(f"- action: {_failure_action(error)}")
    details = (
        "rerun with --format json for current/pointer paths and audit metadata"
        if status == "OK"
        else "rerun with --format json for the structured error and failing path"
    )
    lines.append(f"- details: {details}")
    return "\n".join(lines) + "\n"


def _optional_text(value: object) -> str:
    return str(value or "").strip()


def _first_error(value: object) -> Mapping[str, object]:
    if not isinstance(value, (list, tuple)):
        return {}
    return next((item for item in value if isinstance(item, Mapping)), {})


def _failure_action(error: Mapping[str, object]) -> str:
    code = _optional_text(error.get("code"))
    if code == "DPONE_DEPLOYMENT_CACHE_SYNC_CONFIRMATION_REQUIRED":
        return "add --confirm-promote after reviewing the candidate deployment"
    if code in {"DPONE_CURRENT_POINTER_PROMOTER_MISSING", "DPONE_CURRENT_POINTER_PROMOTER_UNAUTHORIZED"}:
        return "use an allowed CI/service identity or update the platform allowlist"
    if code in {"DPONE_CURRENT_POINTER_CAS_INVALID", "DPONE_CURRENT_POINTER_CAS_MISMATCH"}:
        return "refresh current state and retry with its deployment id as the CAS guard"
    if code in {
        "DPONE_CACHE_PROMOTION_LOCK_FAILED",
        "DPONE_CACHE_PROMOTION_WRITE_FAILED",
        "DPONE_DEPLOYMENT_CACHE_RECOVERY_REQUIRED",
    }:
        return "run dpone airflow cache-recovery-plan before retrying promotion"
    if code == "DPONE_DEPLOYMENT_INCOMPLETE":
        return "rebuild the complete deployment projection before retrying promotion"
    if code == "DPONE_DEPLOYMENT_ENVIRONMENT_MISMATCH":
        return "select the matching environment and deployment projection"
    if code in {
        "DPONE_CACHE_ARTIFACT_NOT_FOUND",
        "DPONE_CACHE_ARTIFACT_READ_FAILED",
        "DPONE_CACHE_ARTIFACT_SIZE_MISMATCH",
        "DPONE_CACHE_CHECKSUM_MISMATCH",
        "DPONE_RELEASE_READ_FAILED",
    }:
        return "restore the pinned release artifacts, then rerun dpone airflow cache-sync"
    if code == "DPONE_CACHE_ARTIFACT_TOO_LARGE":
        return "review the cache artifact size policy or build a smaller release before retrying"
    if code == "DPONE_RELEASE_NOT_FOUND":
        return "materialize the pinned release or rebuild a projection with a valid release_ref"
    if code == "DPONE_DEPLOYMENT_PATH_OUTSIDE_CACHE_ROOT":
        return "select a deployment projection inside the configured cache root"
    if code == "DPONE_DEPLOYMENT_PATH_INVALID":
        return "use the canonical deployments/<environment>/<deployment_id> cache layout"
    if code in {
        "DPONE_RELEASE_ARTIFACTS_INVALID",
        "DPONE_RELEASE_FINGERPRINT_MISMATCH",
        "DPONE_RELEASE_ID_INVALID",
        "DPONE_RELEASE_INVALID",
        "DPONE_RELEASE_SCHEMA_INVALID",
    }:
        return "rebuild the immutable release-set and deployment projection, then rerun dpone airflow cache-sync"
    if code in {
        "DPONE_AIRFLOW_INDEX_ARTIFACT_INVALID",
        "DPONE_AIRFLOW_INDEX_INVALID",
        "DPONE_AIRFLOW_INDEX_NOT_FOUND",
        "DPONE_AIRFLOW_INDEX_SCHEMA_INVALID",
        "DPONE_CACHE_PATH_ESCAPE",
        "DPONE_CACHE_REFERENCE_INVALID",
        "DPONE_CACHE_UNPINNED_REFERENCE",
        "DPONE_DEPLOYMENT_ID_MISMATCH",
        "DPONE_DEPLOYMENT_ID_INVALID",
        "DPONE_DEPLOYMENT_FINGERPRINT_MISMATCH",
        "DPONE_DEPLOYMENT_INDEX_MIRROR_MISMATCH",
        "DPONE_DEPLOYMENT_INVALID",
        "DPONE_DEPLOYMENT_NOT_FOUND",
        "DPONE_DEPLOYMENT_SCHEMA_INVALID",
        "DPONE_RELEASE_ID_MISMATCH",
        "DPONE_RELEASE_INDEX_ARTIFACT_MISMATCH",
    }:
        return "rebuild the deployment projection from the pinned release, then rerun dpone airflow cache-sync"
    return "rerun with --format json and inspect the structured error"


__all__ = ["self_service_cache_sync_text"]
