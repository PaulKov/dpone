"""Derive an exact integration receipt from immutable pre-merge PR evidence."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import yaml

RECEIPT_FILENAME = "agent_pr_merge_receipt.json"
SOURCE_ARCHIVE_FILENAME = "source-agent-pr-receipt.zip"


def _load_sibling(module_name: str, filename: str) -> Any:
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


merge_identity = _load_sibling("dpone_agent_pr_merge_identity", "pr_merge_identity.py")
receipt_source = _load_sibling("dpone_agent_pr_receipt_source", "pr_receipt_source.py")
merge_binding = _load_sibling("dpone_agent_pr_merge_binding", "pr_merge_binding.py")
merge_contract = _load_sibling("dpone_agent_pr_merge_contract", "pr_merge_receipt_contract.py")
compute_binding_id = merge_binding.compute_binding_id


@dataclass(frozen=True)
class Producer:
    """Workflow producer identity for the derived receipt."""

    workflow: str
    run_id: str
    run_attempt: str


@dataclass(frozen=True)
class DerivedMergeReceipt:
    """Derived payload and byte-identical immutable source archive."""

    payload: dict[str, Any]
    source_archive: bytes


def derive_merge_receipt(
    *,
    event: dict[str, Any],
    root: Path,
    repository: str,
    integration_commit_sha: str,
    protected_base_refs: list[str],
    adapter: Any,
    producer: Producer,
    runner: Any = subprocess.run,
) -> DerivedMergeReceipt:
    """Derive one deterministic `H -> C` closure from immutable evidence."""

    merge_event = merge_identity.validate_merge_event(
        event,
        repository=repository,
        protected_base_refs=protected_base_refs,
        integration_commit_sha=integration_commit_sha,
    )
    integration = merge_identity.verify_integration_identity(root, merge_event, runner=runner)
    source_artifact = receipt_source.select_source_receipt(
        repository=repository,
        reviewed_head_sha=merge_event.reviewed_head_sha,
        merged_at=merge_event.merged_at,
        adapter=adapter,
    )
    source = receipt_source.validate_source_archive(
        source_artifact,
        repository=repository,
        reviewed_head_sha=merge_event.reviewed_head_sha,
        pr_number=merge_event.pr_number,
        base_ref=merge_event.protected_base_ref,
        changed_paths=integration.changed_paths,
        body=merge_event.body,
    )
    binding = {
        "repository": repository,
        "protected_base_ref": merge_event.protected_base_ref,
        "pr_number": merge_event.pr_number,
        "merged_at": merge_event.merged_at,
        "integration_method": integration.method,
        "reviewed_head_sha": integration.reviewed_head_sha,
        "reviewed_head_tree": integration.reviewed_head_tree,
        "base_parent_sha": integration.base_parent_sha,
        "integration_commit_sha": integration.integration_commit_sha,
        "integration_tree": integration.integration_tree,
        "changed_paths": integration.changed_paths,
        "pr_body_sha256": source.body_sha256,
        "source_receipt": _source_payload(source),
    }
    payload = {
        "schema_version": 1,
        "status": "PASS",
        "binding_id": compute_binding_id(binding),
        **binding,
        "producer": asdict(producer),
        "errors": [],
        "warnings": (
            ["Source PR receipt was N/A because no agent control-surface path changed."]
            if source.status == "N/A"
            else []
        ),
    }
    _validate_receipt_payload(payload)
    return DerivedMergeReceipt(payload=payload, source_archive=source_artifact.archive_bytes)


def failure_payload(
    *,
    repository: str,
    integration_commit_sha: str | None,
    producer: Producer,
    error: str,
) -> dict[str, Any]:
    """Build a closed failure receipt without manufacturing observations."""

    return {
        "schema_version": 1,
        "status": "FAIL",
        "binding_id": None,
        "repository": repository or "UNKNOWN",
        "protected_base_ref": None,
        "pr_number": None,
        "merged_at": None,
        "integration_method": None,
        "reviewed_head_sha": None,
        "reviewed_head_tree": None,
        "base_parent_sha": None,
        "integration_commit_sha": _safe_sha(integration_commit_sha),
        "integration_tree": None,
        "changed_paths": [],
        "pr_body_sha256": None,
        "source_receipt": None,
        "producer": asdict(producer),
        "errors": [error],
        "warnings": [],
    }


def write_outputs(result: DerivedMergeReceipt, output_dir: Path) -> None:
    """Atomically publish receipt JSON and byte-identical source archive."""

    output_dir.mkdir(parents=True, exist_ok=True)
    _atomic_write(
        output_dir / RECEIPT_FILENAME,
        (json.dumps(result.payload, indent=2, sort_keys=True) + "\n").encode(),
    )
    _atomic_write(output_dir / SOURCE_ARCHIVE_FILENAME, result.source_archive)


def write_failure(payload: dict[str, Any], output_dir: Path) -> None:
    """Atomically publish a failure receipt and no source archive."""

    output_dir.mkdir(parents=True, exist_ok=True)
    source_archive = output_dir / SOURCE_ARCHIVE_FILENAME
    if source_archive.exists():
        source_archive.unlink()
    _atomic_write(
        output_dir / RECEIPT_FILENAME,
        (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode(),
    )


def _source_payload(source: Any) -> dict[str, Any]:
    artifact = source.artifact
    return {
        "check_run_id": artifact.check_run_id,
        "workflow_run_id": artifact.workflow_run_id,
        "workflow_run_attempt": artifact.workflow_run_attempt,
        "artifact_id": artifact.artifact_id,
        "artifact_digest": artifact.artifact_digest,
        "artifact_size_bytes": artifact.artifact_size_bytes,
        "archive_sha256": artifact.archive_sha256,
        "archive_size_bytes": artifact.archive_size_bytes,
        "created_at": artifact.created_at,
        "completed_at": artifact.completed_at,
        "receipt_sha256": source.receipt_sha256,
        "audit_manifest_sha256": source.audit_manifest_sha256,
        "body_sha256": source.body_sha256,
        "status": source.status,
    }


def _protected_base_refs(policy: Path) -> list[str]:
    try:
        payload = yaml.safe_load(policy.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ValueError(f"cannot read frozen branch policy: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("frozen branch policy must be a YAML mapping")
    ruleset = payload.get("ruleset")
    if payload.get("schema_version") == 2:
        branch = payload.get("branch_governance")
        ruleset = branch.get("ruleset") if isinstance(branch, dict) else None
    if not isinstance(ruleset, dict):
        raise ValueError("frozen branch policy must contain ruleset")
    branches = ruleset.get("branches")
    if not isinstance(branches, list) or not branches or not all(isinstance(item, str) and item for item in branches):
        raise ValueError("frozen branch policy must contain protected branch names")
    return list(dict.fromkeys(branches))


def _atomic_write(path: Path, content: bytes) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_bytes(content)
    temporary.replace(path)


def _safe_sha(value: str | None) -> str | None:
    if (
        not isinstance(value, str)
        or len(value) != 40
        or any(character not in "0123456789abcdef" for character in value)
    ):
        return None
    return value


def _validate_receipt_payload(payload: dict[str, Any]) -> None:
    merge_contract.validate_receipt_payload(payload)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        epilog=(
            "Exit 0 writes a PASS receipt and preserved source archive. Exit 1 "
            "atomically overwrites the receipt with FAIL and removes any stale source archive."
        ),
    )
    parser.add_argument("--event", required=True, type=Path, help="Immutable pull_request closed-event JSON.")
    parser.add_argument("--root", type=Path, default=Path("."), help="Checked-out repository root (default: .).")
    parser.add_argument("--repository", required=True, help="Canonical owner/name repository identity.")
    parser.add_argument("--policy", required=True, type=Path, help="Frozen branch-policy YAML from the checkout.")
    parser.add_argument("--workflow", required=True, help="Producer workflow name recorded in the receipt.")
    parser.add_argument("--run-id", required=True, help="Producer GitHub Actions run ID.")
    parser.add_argument("--run-attempt", required=True, help="Producer GitHub Actions run attempt.")
    parser.add_argument(
        "--github-token-env",
        default="GITHUB_TOKEN",
        help="Environment variable holding the read-only GitHub token (default: GITHUB_TOKEN).",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        type=Path,
        help="Directory atomically overwritten with receipt evidence.",
    )
    args = parser.parse_args(argv)

    producer = Producer(
        workflow=args.workflow,
        run_id=args.run_id,
        run_attempt=args.run_attempt,
    )
    try:
        event = json.loads(args.event.read_text(encoding="utf-8"))
        if not isinstance(event, dict):
            raise ValueError("workflow event must be a JSON object")
        integration_sha = os.environ.get("GITHUB_SHA")
        if not integration_sha:
            raise ValueError("GITHUB_SHA workflow identity is required")
        token = os.environ.get(args.github_token_env)
        if not token:
            raise ValueError(f"{args.github_token_env} is required")
        result = derive_merge_receipt(
            event=event,
            root=args.root,
            repository=args.repository,
            integration_commit_sha=integration_sha,
            protected_base_refs=_protected_base_refs(args.policy),
            adapter=receipt_source.GitHubSourceReceiptAdapter(token),
            producer=producer,
        )
        write_outputs(result, args.output_dir)
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as exc:
        payload = failure_payload(
            repository=args.repository,
            integration_commit_sha=os.environ.get("GITHUB_SHA"),
            producer=producer,
            error=str(exc),
        )
        _validate_receipt_payload(payload)
        write_failure(payload, args.output_dir)
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
