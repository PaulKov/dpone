"""Validate immutable Agent PR receipt archive bytes."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

SOURCE_ARTIFACT_NAME = "agent-pr-receipt"
REQUIRED_ARCHIVE_FILES = frozenset(
    {
        "agent_audit_manifest.json",
        "agent_pr_receipt.json",
        "pr-body.md",
        "pr-changed-paths.txt",
        "pr-head-sha.txt",
        "pr-receipt-exit-code.txt",
    }
)
AUTHORITY_FILENAME_MARKERS = (
    "agent_audit_manifest",
    "agent_pr_receipt",
    "current-pr-body",
    "pr-body",
    "pr-changed-paths",
    "pr-head-sha",
    "pr-receipt-exit-code",
)


@dataclass(frozen=True)
class SourceReceipt:
    """Validated semantic content from the immutable source receipt archive."""

    artifact: Any
    status: str
    reviewed_head_sha: str
    pr_number: int
    base_ref: str
    changed_paths: list[str]
    body_sha256: str
    receipt_sha256: str
    audit_manifest_sha256: str
    receipt: dict[str, Any]
    audit_manifest: dict[str, Any]


def validate_source_archive(
    source: Any,
    *,
    repository: str,
    reviewed_head_sha: str,
    pr_number: int,
    base_ref: str,
    changed_paths: list[str],
    body: str,
) -> SourceReceipt:
    """Validate source receipt files and cross-bind them to merge-event identity."""

    files = _archive_files(source.archive_bytes)
    receipt_bytes = files["agent_pr_receipt.json"]
    audit_bytes = files["agent_audit_manifest.json"]
    receipt = _json_object(receipt_bytes, field="agent_pr_receipt.json")
    audit = _json_object(audit_bytes, field="agent_audit_manifest.json")
    _validate_schema(receipt, "pr-receipt.schema.json", field="agent_pr_receipt.json")
    _validate_schema(audit, "audit-manifest.schema.json", field="agent_audit_manifest.json")
    if receipt.get("schema_version") != 2:
        raise ValueError("source agent_pr_receipt.json must have schema_version 2")
    status = receipt.get("status")
    if status not in {"PASS", "N/A"}:
        raise ValueError(f"source receipt status must be PASS or N/A (observed {status or 'missing'})")
    receipt_errors = _validate_receipt_semantics(receipt, status=status)
    normalized_paths = sorted(set(changed_paths))
    if receipt.get("changed_paths") != normalized_paths:
        raise ValueError("source receipt changed_paths do not match exact integration paths")
    path_parser = _load_sibling(
        "dpone_agent_pr_receipt_source_path_evidence",
        "path_evidence.py",
    )
    archived_paths = path_parser.parse_path_evidence(
        files["pr-changed-paths.txt"],
        field="pr-changed-paths.txt",
    )
    if archived_paths != normalized_paths:
        raise ValueError("archived PR changed paths do not match exact integration paths")
    archived_head = _utf8(files, "pr-head-sha.txt").strip()
    if archived_head != reviewed_head_sha:
        raise ValueError("archived PR head SHA does not match reviewed head")
    archived_body = _utf8(files, "pr-body.md")
    expected_body_digest = f"sha256:{hashlib.sha256(body.encode()).hexdigest()}"
    archived_body_digest = f"sha256:{hashlib.sha256(archived_body.encode()).hexdigest()}"
    if archived_body_digest != expected_body_digest:
        raise ValueError("closed-event PR body does not match immutable pre-merge receipt body")
    if _utf8(files, "pr-receipt-exit-code.txt").strip() != "0":
        raise ValueError("source PR receipt exit code must be 0")

    selected_artifact = _validate_source_evidence(
        receipt,
        reviewed_head_sha=reviewed_head_sha,
        status=status,
    )
    _validate_source_audit(
        audit,
        source=source,
        repository=repository,
        reviewed_head_sha=reviewed_head_sha,
        pr_number=pr_number,
        base_ref=base_ref,
        status=status,
        receipt_errors=receipt_errors,
        selected_artifact=selected_artifact,
    )
    if status == "N/A":
        control_surface = _load_sibling(
            "dpone_agent_pr_receipt_source_control_surface",
            "control_surface.py",
        )
        if control_surface.control_surface_changed(normalized_paths):
            raise ValueError("source N/A receipt cannot close agent control-surface changes")

    return SourceReceipt(
        artifact=source,
        status=status,
        reviewed_head_sha=reviewed_head_sha,
        pr_number=pr_number,
        base_ref=base_ref,
        changed_paths=normalized_paths,
        body_sha256=archived_body_digest,
        receipt_sha256=f"sha256:{hashlib.sha256(receipt_bytes).hexdigest()}",
        audit_manifest_sha256=f"sha256:{hashlib.sha256(audit_bytes).hexdigest()}",
        receipt=receipt,
        audit_manifest=audit,
    )


def _validate_source_audit(
    audit: dict[str, Any],
    *,
    source: Any,
    repository: str,
    reviewed_head_sha: str,
    pr_number: int,
    base_ref: str,
    status: str,
    receipt_errors: list[Any],
    selected_artifact: dict[str, Any] | None,
) -> None:
    expected = {
        "schema_version": 1,
        "kind": "agent_pr_receipt",
        "repository": repository,
        "pr_number": pr_number,
        "base_ref": base_ref,
        "head_sha": reviewed_head_sha,
        "receipt_status": status,
        "artifact_name": SOURCE_ARTIFACT_NAME,
    }
    for field, value in expected.items():
        if audit.get(field) != value:
            raise ValueError(f"source audit manifest {field} does not match immutable receipt identity")
    if str(audit.get("run_id")) != str(source.workflow_run_id):
        raise ValueError("source audit manifest run_id does not match selected workflow run")
    if str(audit.get("run_attempt")) != str(source.workflow_run_attempt):
        raise ValueError("source audit manifest run_attempt does not match selected workflow attempt")
    if audit.get("receipt_errors") != receipt_errors:
        raise ValueError("source audit manifest receipt_errors do not match source receipt")
    if status == "PASS":
        if audit.get("evidence_chain_head_sha") != reviewed_head_sha:
            raise ValueError("source audit manifest evidence-chain head does not match reviewed head")
        audit_artifact = audit.get("evidence_chain_governance_artifact")
        if not isinstance(audit_artifact, dict) or selected_artifact is None:
            raise ValueError("source audit manifest lacks selected governance artifact")
        for field in ("artifact_id", "digest"):
            if audit_artifact.get(field) != selected_artifact.get(field):
                raise ValueError(f"source audit manifest governance artifact {field} does not match source receipt")


def _validate_receipt_semantics(receipt: dict[str, Any], *, status: str) -> list[Any]:
    if receipt.get("control_surface_changed") is not (status == "PASS"):
        raise ValueError("source receipt control_surface_changed contradicts its status")
    errors = receipt.get("errors")
    if errors != []:
        raise ValueError("source PASS or N/A receipt must not contain errors")
    return errors


def _validate_source_evidence(
    receipt: dict[str, Any],
    *,
    reviewed_head_sha: str,
    status: str,
) -> dict[str, Any] | None:
    evidence = receipt.get("github_evidence")
    if not isinstance(evidence, dict) or evidence.get("head_sha") != reviewed_head_sha:
        raise ValueError("source receipt GitHub evidence does not match reviewed head")
    if evidence.get("errors") != []:
        raise ValueError("source receipt GitHub evidence must not contain unresolved errors")
    if status == "N/A":
        return None
    chain = receipt.get("evidence_chain")
    if not isinstance(chain, dict) or chain.get("head_sha") != reviewed_head_sha:
        raise ValueError("source receipt evidence chain does not match reviewed head")
    selected = chain.get("governance_artifact")
    if not isinstance(selected, dict):
        raise ValueError("source receipt evidence chain lacks selected governance artifact")
    artifact_id = selected.get("artifact_id")
    digest = selected.get("digest")
    matches = [
        artifact
        for artifact in evidence.get("artifacts", [])
        if isinstance(artifact, dict)
        and artifact.get("artifact_id") == artifact_id
        and artifact.get("digest") == digest
        and artifact.get("workflow_run_head_sha") == reviewed_head_sha
    ]
    if len(matches) != 1:
        raise ValueError("source receipt selected governance artifact does not match raw evidence exactly")
    return selected


def _archive_files(content: bytes) -> dict[str, bytes]:
    members = resource_limits.read_bounded_zip(
        content,
        resource="source Agent PR receipt artifact",
    )
    files: dict[str, bytes] = {}
    for member in members:
        if member.is_dir:
            continue
        name = PurePosixPath(member.filename).name
        if name not in REQUIRED_ARCHIVE_FILES:
            if _is_unexpected_authority_file(name):
                raise ValueError(f"source receipt artifact contains unexpected authority file {member.filename!r}")
            continue
        if name in files:
            raise ValueError(f"source receipt artifact contains duplicate {name}")
        files[name] = member.content
    missing = sorted(REQUIRED_ARCHIVE_FILES - files.keys())
    if missing:
        raise ValueError(f"source receipt artifact is missing required files {missing}")
    return files


def _is_unexpected_authority_file(name: str) -> bool:
    lowered = name.lower().replace("_", "-")
    return any(marker.replace("_", "-") in lowered for marker in AUTHORITY_FILENAME_MARKERS)


def _json_object(content: bytes, *, field: str) -> dict[str, Any]:
    try:
        payload = json.loads(content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{field} must contain a UTF-8 JSON object") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"{field} must contain a JSON object")
    return payload


def _utf8(files: dict[str, bytes], name: str) -> str:
    try:
        return files[name].decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"{name} must contain UTF-8 text") from exc


def _validate_schema(payload: dict[str, Any], filename: str, *, field: str) -> None:
    schema_path = Path(__file__).resolve().parents[2] / "evals" / "agent" / filename
    try:
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        Draft202012Validator(schema).validate(payload)
    except (OSError, json.JSONDecodeError, ValidationError) as exc:
        raise ValueError(f"{field} does not satisfy its closed schema: {exc}") from exc


def _load_sibling(module_name: str, filename: str) -> Any:
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


resource_limits = _load_sibling(
    "dpone_agent_pr_receipt_source_archive_resource_limits",
    "artifact_resource_limits.py",
)
