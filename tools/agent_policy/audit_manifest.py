"""Build compact audit manifests for agent governance evidence artifacts."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_AUDIT_MANIFEST = "agent_audit_manifest.json"


def build_pr_receipt_manifest(
    *,
    receipt: dict[str, Any],
    event: dict[str, Any],
    repository: str,
    workflow: str,
    run_id: str,
    run_attempt: str,
    artifact_name: str,
    retention_days: int,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Build a compact audit manifest for an agent PR receipt artifact."""

    pull_request = _mapping(event.get("pull_request"))
    head = _mapping(pull_request.get("head"))
    base = _mapping(pull_request.get("base"))
    evidence = _mapping(receipt.get("github_evidence"))
    traceability = _mapping(receipt.get("traceability"))
    evidence_chain = _mapping(receipt.get("evidence_chain"))
    head_sha = _optional_string(evidence.get("head_sha")) or _optional_string(head.get("sha"))

    return {
        "schema_version": 1,
        "kind": "agent_pr_receipt",
        "repository": repository,
        "workflow": workflow,
        "run_id": run_id,
        "run_attempt": run_attempt,
        "artifact_name": artifact_name,
        "artifact_retention_days": retention_days,
        "generated_at": generated_at or _utc_now(),
        "pr_number": _optional_int(pull_request.get("number")),
        "base_ref": _optional_string(base.get("ref")),
        "base_sha": _optional_string(base.get("sha")),
        "head_ref": _optional_string(head.get("ref")),
        "head_sha": head_sha,
        "merge_sha": _optional_string(pull_request.get("merge_commit_sha")),
        "receipt_status": str(receipt.get("status", "")),
        "receipt_errors": _string_list(receipt.get("errors")),
        "receipt_warnings": _string_list(receipt.get("warnings")),
        "traceability_source": _optional_string(traceability.get("approved_source")),
        "traceability_source_kind": _optional_traceability_source_kind(traceability.get("approved_source_kind")),
        "traceability_statuses": _string_list(traceability.get("validation_statuses")),
        "traceability_non_pass_reasons": _non_pass_reason_list(traceability.get("non_pass_reasons")),
        "evidence_chain_head_sha": _optional_string(evidence_chain.get("head_sha")),
        "evidence_chain_required_checks": _check_ref_list(evidence_chain.get("required_checks")),
        "evidence_chain_governance_artifact": _optional_chain_artifact(evidence_chain.get("governance_artifact")),
        "required_checks": _string_list(evidence.get("required_checks")),
        "evidence_artifacts": _artifact_list(evidence.get("artifacts")),
    }


def write_manifest(manifest: dict[str, Any], output: Path) -> None:
    """Write a manifest JSON file."""

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: JSON document must be an object")
    return payload


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _optional_string(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _optional_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    return value if isinstance(value, int) else None


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]


def _optional_traceability_source_kind(value: Any) -> str | None:
    allowed = {"issue", "url", "docs", "adr", "na", "unknown"}
    return value if isinstance(value, str) and value in allowed else None


def _non_pass_reason_list(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    reasons: list[dict[str, str]] = []
    for item in value:
        if isinstance(item, dict):
            reasons.append(
                {
                    "check": str(item.get("check", "")),
                    "status": str(item.get("status", "")),
                    "reason": str(item.get("reason", "")),
                }
            )
    return reasons


def _artifact_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    artifacts: list[dict[str, Any]] = []
    for item in value:
        if isinstance(item, dict):
            artifacts.append(dict(item))
    return artifacts


def _check_ref_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    refs: list[dict[str, Any]] = []
    for item in value:
        if isinstance(item, dict):
            refs.append(
                {
                    "name": str(item.get("name", "")),
                    "source": _optional_string(item.get("source")),
                    "id": _optional_int(item.get("id")),
                    "workflow_run_id": _optional_int(item.get("workflow_run_id")),
                    "status": _optional_string(item.get("status")),
                    "conclusion": _optional_string(item.get("conclusion")),
                    "state": _optional_string(item.get("state")),
                    "url": _optional_string(item.get("url")),
                    "completed_at": _optional_string(item.get("completed_at")),
                }
            )
    return refs


def _optional_chain_artifact(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    return {
        "name": str(value.get("name", "")),
        "artifact_id": _optional_int(value.get("artifact_id")),
        "workflow_run_id": _optional_int(value.get("workflow_run_id")),
        "workflow_run_head_sha": _optional_string(value.get("workflow_run_head_sha")),
        "digest": _optional_string(value.get("digest")),
        "archive_sha256": _optional_string(value.get("archive_sha256")),
        "archive_size_bytes": _optional_int(value.get("archive_size_bytes")),
        "expired": bool(value.get("expired")),
        "url": _optional_string(value.get("url")),
        "content_status": _optional_string(value.get("content_status")),
        "content_control_surface_changed": _optional_bool(value.get("content_control_surface_changed")),
        "content_changed_paths": _string_list(value.get("content_changed_paths")),
        "content_checks": _content_check_list(value.get("content_checks")),
        "attestation": _optional_attestation(value.get("attestation")),
    }


def _content_check_list(value: Any) -> list[dict[str, str | None]]:
    if not isinstance(value, list):
        return []
    checks: list[dict[str, str | None]] = []
    for item in value:
        if isinstance(item, dict):
            checks.append(
                {
                    "name": _optional_string(item.get("name")),
                    "status": _optional_string(item.get("status")),
                }
            )
    return checks


def _optional_attestation(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    return {
        "status": _optional_attestation_status(value.get("status")),
        "predicate_type": _optional_string(value.get("predicate_type")),
        "subject_sha256": _optional_string(value.get("subject_sha256")),
        "source_repository": _optional_string(value.get("source_repository")),
        "source_ref": _optional_string(value.get("source_ref")),
        "source_digest": _optional_string(value.get("source_digest")),
        "signer_workflow": _optional_string(value.get("signer_workflow")),
        "issuer": _optional_string(value.get("issuer")),
        "verified_timestamps_count": _optional_int(value.get("verified_timestamps_count")),
        "runner_environment": _optional_string(value.get("runner_environment")),
        "errors": _string_list(value.get("errors")),
    }


def _optional_attestation_status(value: Any) -> str | None:
    allowed = {"PASS", "FAIL", "UNVERIFIED"}
    return value if isinstance(value, str) and value in allowed else None


def _optional_bool(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")  # noqa: UP017


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    pr_receipt = subparsers.add_parser("pr-receipt", help="Build an agent PR receipt audit manifest.")
    pr_receipt.add_argument("--receipt", required=True, type=Path)
    pr_receipt.add_argument("--event", required=True, type=Path)
    pr_receipt.add_argument("--repository", required=True)
    pr_receipt.add_argument("--workflow", required=True)
    pr_receipt.add_argument("--run-id", required=True)
    pr_receipt.add_argument("--run-attempt", required=True)
    pr_receipt.add_argument("--artifact-name", required=True)
    pr_receipt.add_argument("--retention-days", required=True, type=int)
    pr_receipt.add_argument("--generated-at")
    pr_receipt.add_argument("--output", required=True, type=Path)

    args = parser.parse_args(argv)
    if args.command == "pr-receipt":
        manifest = build_pr_receipt_manifest(
            receipt=_load_json(args.receipt),
            event=_load_json(args.event),
            repository=args.repository,
            workflow=args.workflow,
            run_id=args.run_id,
            run_attempt=args.run_attempt,
            artifact_name=args.artifact_name,
            retention_days=args.retention_days,
            generated_at=args.generated_at,
        )
        write_manifest(manifest, args.output)
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
