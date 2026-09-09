"""Live GitHub evidence helpers for agent pull-request receipts."""

from __future__ import annotations

import argparse
import importlib.util
import os
import sys
import urllib.parse
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import yaml

SELF_CHECK_NAME = "Agent PR receipt"
GOVERNANCE_ARTIFACT_NAME = "agent-governance-gate"


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


github_api = load_sibling("dpone_agent_pr_receipt_github_api", "pr_receipt_github_api.py")
github_metadata = load_sibling("dpone_agent_pr_receipt_github_metadata", "pr_receipt_github_metadata.py")
governance_archive = load_sibling("dpone_agent_governance_artifact_archive", "governance_artifact_archive.py")
artifact_limits = load_sibling("dpone_agent_artifact_resource_limits", "artifact_resource_limits.py")
github_attestation = load_sibling("dpone_agent_pr_receipt_github_attestation", "pr_receipt_github_attestation.py")
github_validation = load_sibling("dpone_agent_pr_receipt_github_validation", "pr_receipt_github_validation.py")
_github_json = github_api.github_json
_github_paginated_items = github_api.github_paginated_items
_github_bytes = github_api.github_bytes
GitHubArtifactAttestationEvidence = github_attestation.GitHubArtifactAttestationEvidence
validate_required_check_evidence = github_validation.validate_required_check_evidence
validate_governance_artifact_evidence = github_validation.validate_governance_artifact_evidence


@dataclass(frozen=True)
class GitHubCheckEvidence:
    """Observed GitHub check-run or commit-status evidence."""

    name: str
    source: str
    id: int | None = None
    status: str | None = None
    conclusion: str | None = None
    state: str | None = None
    url: str | None = None
    details_url: str | None = None
    workflow_run_id: int | None = None
    completed_at: str | None = None

    @property
    def successful(self) -> bool:
        if self.source == "status":
            return self.state == "success"
        return self.status == "completed" and self.conclusion == "success"

    @property
    def observed_state(self) -> str:
        return self.state or self.conclusion or self.status or "missing"


@dataclass(frozen=True)
class GitHubArtifactEvidence:
    """Observed GitHub Actions artifact evidence."""

    name: str
    workflow_run_head_sha: str
    workflow_run_id: int
    expired: bool
    url: str | None = None
    artifact_id: int | None = None
    digest: str | None = None
    size_in_bytes: int | None = None
    archive_sha256: str | None = None
    archive_size_bytes: int | None = None
    created_at: str | None = None
    expires_at: str | None = None
    content: Any | None = None
    attestation: Any | None = None


@dataclass(frozen=True)
class GitHubEvidence:
    """Live GitHub evidence bound to a pull-request head commit."""

    head_sha: str
    required_checks: list[str]
    check_runs: list[GitHubCheckEvidence]
    statuses: list[GitHubCheckEvidence]
    artifacts: list[GitHubArtifactEvidence]
    errors: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class PullRequestSnapshot:
    """Current PR state fetched directly from the GitHub API."""

    head_sha: str
    body: str


def evidence_payload(evidence: GitHubEvidence | None) -> dict[str, Any] | None:
    """Convert GitHub evidence to deterministic JSON-compatible data."""

    if evidence is None:
        return None
    return {
        "head_sha": evidence.head_sha,
        "required_checks": evidence.required_checks,
        "check_runs": [asdict(item) for item in evidence.check_runs],
        "statuses": [asdict(item) for item in evidence.statuses],
        "artifacts": [_artifact_payload(item) for item in evidence.artifacts],
        "errors": evidence.errors,
    }


def fetch_pull_request_snapshot(*, repo: str, pull_request_number: int, token: str) -> PullRequestSnapshot:
    """Read current PR head/body, rather than trusting the stale event snapshot."""

    if pull_request_number <= 0:
        raise ValueError("pull_request_number must be positive")
    payload = _github_json(f"repos/{repo}/pulls/{pull_request_number}", token=token, fresh=True)
    head = payload.get("head")
    head_sha = head.get("sha") if isinstance(head, dict) else None
    if not isinstance(head_sha, str) or not head_sha:
        raise RuntimeError("GitHub pull-request response has no canonical head SHA.")
    body = payload.get("body")
    return PullRequestSnapshot(head_sha=head_sha, body=body if isinstance(body, str) else "")


def _artifact_payload(artifact: GitHubArtifactEvidence) -> dict[str, Any]:
    payload = asdict(artifact)
    content = payload.get("content")
    if isinstance(content, dict):
        content.pop("subject_sha256", None)
    return payload


def fetch_github_evidence(
    *,
    repo: str,
    head_sha: str,
    token: str,
    ruleset_id: int,
    artifact_name: str = GOVERNANCE_ARTIFACT_NAME,
    require_attestation: bool = False,
    signer_workflow: str | None = None,
) -> GitHubEvidence:
    """Fetch live GitHub checks and artifact evidence for a PR head commit."""

    required_checks = _fetch_required_checks(repo=repo, ruleset_id=ruleset_id, token=token)
    return GitHubEvidence(
        head_sha=head_sha,
        required_checks=required_checks,
        check_runs=_fetch_check_runs(repo=repo, head_sha=head_sha, token=token),
        statuses=_fetch_statuses(repo=repo, head_sha=head_sha, token=token),
        artifacts=_fetch_artifacts(
            repo=repo,
            head_sha=head_sha,
            token=token,
            artifact_name=artifact_name,
            require_attestation=require_attestation,
            signer_workflow=signer_workflow or github_attestation.default_signer_workflow(repo),
        ),
    )


def github_evidence_from_args(args: argparse.Namespace) -> GitHubEvidence | None:
    """Build live GitHub evidence from parsed CLI arguments."""

    if not args.repo and not args.head_sha:
        return None
    if not args.repo or not args.head_sha:
        return GitHubEvidence(
            head_sha=args.head_sha or "unknown",
            required_checks=[],
            check_runs=[],
            statuses=[],
            artifacts=[],
            errors=["Both --repo and --head-sha are required to fetch live GitHub evidence."],
        )
    token = os.environ.get(args.github_token_env)
    if not token:
        return GitHubEvidence(
            head_sha=args.head_sha,
            required_checks=[],
            check_runs=[],
            statuses=[],
            artifacts=[],
            errors=[f"{args.github_token_env} is required to fetch live GitHub evidence."],
        )
    ruleset_id = args.ruleset_id or ruleset_id_from_policy(args.policy)
    if ruleset_id is None:
        return GitHubEvidence(
            head_sha=args.head_sha,
            required_checks=[],
            check_runs=[],
            statuses=[],
            artifacts=[],
            errors=["--ruleset-id or --policy with ruleset.id is required to fetch live required checks."],
        )
    try:
        return fetch_github_evidence(
            repo=args.repo,
            head_sha=args.head_sha,
            token=token,
            ruleset_id=ruleset_id,
            artifact_name=args.artifact_name,
            require_attestation=getattr(args, "require_github_attestation", False),
            signer_workflow=getattr(args, "github_attestation_signer_workflow", None),
        )
    except (RuntimeError, ValueError) as exc:
        return GitHubEvidence(
            head_sha=args.head_sha,
            required_checks=[],
            check_runs=[],
            statuses=[],
            artifacts=[],
            errors=[str(exc)],
        )


def ruleset_id_from_policy(policy: Path | None) -> int | None:
    """Read the GitHub ruleset id from v1 or v2 governance policy YAML."""

    if policy is None:
        return None
    try:
        payload = yaml.safe_load(policy.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError):
        return None
    if not isinstance(payload, dict):
        return None
    if payload.get("schema_version") == 2:
        branch = payload.get("branch_governance")
        ruleset = branch.get("ruleset") if isinstance(branch, dict) else None
    else:
        ruleset = payload.get("ruleset")
    if not isinstance(ruleset, dict):
        return None
    ruleset_id = ruleset.get("id")
    if isinstance(ruleset_id, bool) or not isinstance(ruleset_id, int) or ruleset_id <= 0:
        return None
    return ruleset_id


def _fetch_required_checks(*, repo: str, ruleset_id: int, token: str) -> list[str]:
    ruleset = _github_json(f"repos/{repo}/rulesets/{ruleset_id}", token=token)
    checks: set[str] = set()
    for rule in ruleset.get("rules", []):
        if rule.get("type") != "required_status_checks":
            continue
        parameters = rule.get("parameters") or {}
        for check in parameters.get("required_status_checks") or []:
            context = check.get("context")
            if context:
                checks.add(str(context))
    return sorted(checks)


def _fetch_check_runs(*, repo: str, head_sha: str, token: str) -> list[GitHubCheckEvidence]:
    payloads = _github_paginated_items(
        f"repos/{repo}/commits/{head_sha}/check-runs",
        token=token,
        array_key="check_runs",
    )
    return [
        GitHubCheckEvidence(
            name=str(item.get("name") or ""),
            source="check_run",
            id=github_metadata.optional_int(item.get("id")),
            status=item.get("status"),
            conclusion=item.get("conclusion"),
            url=item.get("html_url"),
            details_url=item.get("details_url"),
            workflow_run_id=github_metadata.workflow_run_id_from_payload(item),
            completed_at=item.get("completed_at"),
        )
        for item in payloads
        if item.get("name")
    ]


def _fetch_statuses(*, repo: str, head_sha: str, token: str) -> list[GitHubCheckEvidence]:
    payloads = _github_paginated_items(f"repos/{repo}/commits/{head_sha}/statuses", token=token)
    return [
        GitHubCheckEvidence(
            name=str(item.get("context") or ""),
            source="status",
            id=github_metadata.optional_int(item.get("id")),
            state=item.get("state"),
            url=item.get("target_url"),
            completed_at=item.get("updated_at"),
        )
        for item in payloads
        if item.get("context")
    ]


def _fetch_artifacts(
    *,
    repo: str,
    head_sha: str,
    token: str,
    artifact_name: str,
    require_attestation: bool,
    signer_workflow: str,
) -> list[GitHubArtifactEvidence]:
    query = urllib.parse.urlencode({"name": artifact_name, "per_page": 100})
    payloads = _github_paginated_items(f"repos/{repo}/actions/artifacts?{query}", token=token, array_key="artifacts")
    artifacts: list[GitHubArtifactEvidence] = []
    for item in payloads:
        workflow_run = item.get("workflow_run") or {}
        run_head_sha = str(workflow_run.get("head_sha") or "")
        if run_head_sha != head_sha:
            continue
        provider_size = github_metadata.optional_int(item.get("size_in_bytes"))
        archive, archive_bytes = _fetch_artifact_archive(
            item.get("archive_download_url"),
            token=token,
            provider_size=provider_size,
        )
        artifacts.append(
            GitHubArtifactEvidence(
                name=str(item.get("name") or ""),
                workflow_run_head_sha=run_head_sha,
                workflow_run_id=int(workflow_run.get("id") or 0),
                expired=bool(item.get("expired")),
                url=item.get("archive_download_url"),
                artifact_id=github_metadata.optional_int(item.get("id")),
                digest=item.get("digest"),
                size_in_bytes=provider_size,
                archive_sha256=archive.sha256,
                archive_size_bytes=archive.size_bytes,
                created_at=item.get("created_at"),
                expires_at=item.get("expires_at"),
                content=archive.content,
                attestation=github_attestation.fetch_artifact_attestation(
                    archive_bytes,
                    repo=repo,
                    token=token,
                    signer_workflow=signer_workflow,
                    require_attestation=require_attestation,
                ),
            )
        )
    return artifacts


def _fetch_artifact_archive(
    url: Any,
    *,
    token: str,
    provider_size: int | None,
) -> tuple[Any, bytes | None]:
    if not isinstance(url, str) or not url:
        return governance_archive.error_evidence("Governance artifact archive download URL is missing."), None
    try:
        artifact_limits.validate_provider_size(provider_size, resource="Governance artifact")
    except ValueError as exc:
        return governance_archive.error_evidence(str(exc)), None
    try:
        archive_bytes = _github_bytes(url, token=token)
    except RuntimeError as exc:
        return governance_archive.error_evidence(str(exc)), None
    try:
        artifact_limits.validate_downloaded_size(archive_bytes, resource="Governance artifact")
    except ValueError as exc:
        return governance_archive.error_evidence(str(exc)), None
    return governance_archive.evidence_from_archive_bytes(archive_bytes), archive_bytes
