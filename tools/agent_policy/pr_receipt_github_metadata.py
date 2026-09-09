"""Metadata normalization helpers for GitHub PR receipt evidence."""

from __future__ import annotations

import re
from typing import Any

SHA256_DIGEST = re.compile(r"^sha256:[0-9a-fA-F]{64}$")
WORKFLOW_RUN_ID = re.compile(r"/actions/runs/(?P<run_id>\d+)")


def optional_int(value: Any) -> int | None:
    """Return an integer for GitHub numeric metadata when one is present."""

    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def workflow_run_id_from_payload(item: dict[str, Any]) -> int | None:
    """Extract a workflow run id from GitHub check-run URLs."""

    for key in ("details_url", "html_url"):
        run_id = workflow_run_id_from_url(item.get(key))
        if run_id is not None:
            return run_id
    return None


def workflow_run_id_from_url(value: Any) -> int | None:
    """Extract an Actions workflow run id from a URL value."""

    if not isinstance(value, str):
        return None
    match = WORKFLOW_RUN_ID.search(value)
    if match is None:
        return None
    return int(match.group("run_id"))


def governance_artifact_identity_errors(
    *,
    artifact: Any,
    governance_artifact_name: str,
    head_sha: str,
) -> list[str]:
    """Return fail-closed identity errors for the required governance artifact."""

    errors: list[str] = []
    if artifact.artifact_id is None or artifact.artifact_id <= 0:
        errors.append(
            f"Required GitHub artifact '{governance_artifact_name}' for head {head_sha} is missing an artifact id."
        )
    if artifact.workflow_run_id <= 0:
        errors.append(
            f"Required GitHub artifact '{governance_artifact_name}' for head {head_sha} is missing a workflow run id."
        )
    if artifact.digest is None or SHA256_DIGEST.fullmatch(artifact.digest) is None:
        errors.append(
            f"Required GitHub artifact '{governance_artifact_name}' for head {head_sha} is missing a SHA-256 digest."
        )
    if artifact.archive_sha256 is None or SHA256_DIGEST.fullmatch(artifact.archive_sha256) is None:
        errors.append(
            f"Required GitHub artifact '{governance_artifact_name}' for head {head_sha} "
            "is missing a downloaded archive SHA-256 digest."
        )
    if (
        artifact.digest is not None
        and artifact.archive_sha256 is not None
        and SHA256_DIGEST.fullmatch(artifact.digest) is not None
        and SHA256_DIGEST.fullmatch(artifact.archive_sha256) is not None
        and artifact.digest.lower() != artifact.archive_sha256.lower()
    ):
        errors.append(
            f"Required GitHub artifact '{governance_artifact_name}' for head {head_sha} metadata digest "
            f"{artifact.digest} does not match downloaded archive SHA-256 {artifact.archive_sha256}."
        )
    if not _positive_int(getattr(artifact, "size_in_bytes", None)):
        errors.append(
            f"Required GitHub artifact '{governance_artifact_name}' for head {head_sha} "
            "is missing a positive metadata size."
        )
    if not _positive_int(getattr(artifact, "archive_size_bytes", None)):
        errors.append(
            f"Required GitHub artifact '{governance_artifact_name}' for head {head_sha} "
            "is missing a positive downloaded archive size."
        )
    if _positive_int(getattr(artifact, "size_in_bytes", None)) and _positive_int(
        getattr(artifact, "archive_size_bytes", None)
    ):
        if artifact.size_in_bytes != artifact.archive_size_bytes:
            errors.append(
                f"Required GitHub artifact '{governance_artifact_name}' for head {head_sha} metadata size does not "
                f"match downloaded archive size (metadata {artifact.size_in_bytes}, "
                f"downloaded {artifact.archive_size_bytes})."
            )
    return errors


def _positive_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0
