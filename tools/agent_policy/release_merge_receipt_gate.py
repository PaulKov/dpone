"""Verify the durable merge-closure receipt behind an exact release check."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import re
import sys
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol, TypeVar

CHECK_NAME = "Agent PR receipt"
ARTIFACT_NAME = "agent-pr-receipt"
WORKFLOW_PATH = ".github/workflows/agent-pr-receipt.yml"
WORKFLOW_NAME = "Agent PR receipt"
APP_SLUG = "github-actions"
FULL_SHA = re.compile(r"^[0-9a-f]{40}$")
REPOSITORY = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
EXTERNAL_ID = re.compile(r"^agent-pr-merge-closure:([1-9][0-9]*):([1-9][0-9]*)$")
DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
REQUIRED_FILES = frozenset(
    {
        "agent_pr_merge_receipt.json",
        "pr-receipt-exit-code.txt",
        "source-agent-pr-receipt.zip",
    }
)
T = TypeVar("T")


def _load_sibling(module_name: str, filename: str) -> Any:
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


github_api = _load_sibling("dpone_release_merge_receipt_github_api", "pr_receipt_github_api.py")
resource_limits = _load_sibling(
    "dpone_release_merge_receipt_artifact_resource_limits",
    "artifact_resource_limits.py",
)
merge_binding = _load_sibling("dpone_release_merge_receipt_binding", "pr_merge_binding.py")
merge_contract = _load_sibling("dpone_release_merge_receipt_contract", "pr_merge_receipt_contract.py")
reconcile = _load_sibling(
    "dpone_release_merge_receipt_reconcile",
    "release_merge_receipt_reconcile.py",
)


class MergeReceiptAdapter(Protocol):
    """Read-only GitHub operations needed by the release evidence gate."""

    def list_check_runs(self, repository: str, commit_sha: str) -> list[dict[str, Any]]: ...

    def get_workflow_run(self, repository: str, run_id: int) -> dict[str, Any]: ...

    def list_run_artifacts(self, repository: str, run_id: int) -> list[dict[str, Any]]: ...

    def download_artifact(self, url: str) -> bytes: ...


class GitHubMergeReceiptAdapter:
    """GitHub REST adapter whose credential never enters evidence."""

    def __init__(
        self,
        token: str,
        *,
        attempts: int = 3,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if not token:
            raise ValueError("GitHub token is required")
        if attempts < 1:
            raise ValueError("GitHub retry attempts must be positive")
        self._token = token
        self._attempts = attempts
        self._sleeper = sleeper

    def list_check_runs(self, repository: str, commit_sha: str) -> list[dict[str, Any]]:
        return self._retry(
            lambda: github_api.github_paginated_items(
                f"repos/{repository}/commits/{commit_sha}/check-runs?filter=latest",
                token=self._token,
                array_key="check_runs",
            )
        )

    def get_workflow_run(self, repository: str, run_id: int) -> dict[str, Any]:
        return self._retry(
            lambda: github_api.github_json(f"repos/{repository}/actions/runs/{run_id}", token=self._token)
        )

    def list_run_artifacts(self, repository: str, run_id: int) -> list[dict[str, Any]]:
        return self._retry(
            lambda: github_api.github_paginated_items(
                f"repos/{repository}/actions/runs/{run_id}/artifacts",
                token=self._token,
                array_key="artifacts",
            )
        )

    def download_artifact(self, url: str) -> bytes:
        return self._retry(lambda: github_api.github_bytes(url, token=self._token))

    def _retry(self, operation: Callable[[], T]) -> T:
        for attempt in range(1, self._attempts + 1):
            try:
                return operation()
            except RuntimeError:
                if attempt == self._attempts:
                    raise
                self._sleeper(0.25 * attempt)
        raise AssertionError("positive retry count must execute")


@dataclass(frozen=True)
class MergeReceiptEvidence:
    """Credential-free identity of one verified exact-commit evidence chain."""

    schema_version: int
    status: str
    repository: str
    integration_commit_sha: str
    check_run_id: int
    github_app_id: int
    workflow_run_id: int
    workflow_run_attempt: int
    artifact_id: int
    artifact_digest: str
    artifact_size_bytes: int
    archive_sha256: str
    receipt_sha256: str
    binding_id: str

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


def verify_merge_receipt(
    *,
    repository: str,
    integration_commit_sha: str,
    adapter: MergeReceiptAdapter,
) -> MergeReceiptEvidence:
    """Verify the latest exact-C check, its successful producer, and durable bytes."""

    _validate_identity(repository, integration_commit_sha)
    check = _latest_check(adapter.list_check_runs(repository, integration_commit_sha), integration_commit_sha)
    check_id = _positive_int(check.get("id"), "check_run.id")
    app = _mapping(check.get("app"), "check_run.app")
    if app.get("slug") != APP_SLUG:
        raise ValueError("exact integration check must be owned by GitHub Actions")
    app_id = _positive_int(app.get("id"), "check_run.app.id")
    external = _string(check.get("external_id"), "check_run.external_id")
    match = EXTERNAL_ID.fullmatch(external)
    if match is None:
        raise ValueError("exact integration check external_id does not identify its workflow run")
    run_id = int(match.group(1))
    run_attempt = int(match.group(2))
    details_url = _string(check.get("details_url"), "check_run.details_url")
    accepted_details_urls = {
        f"https://github.com/{repository}/actions/runs/{run_id}",
        f"https://github.com/{repository}/runs/{check_id}",
    }
    if details_url not in accepted_details_urls:
        raise ValueError("exact integration check details_url is not canonical")

    run = adapter.get_workflow_run(repository, run_id)
    _validate_run(
        run,
        repository=repository,
        run_id=run_id,
        run_attempt=run_attempt,
    )
    artifact = _select_artifact(adapter.list_run_artifacts(repository, run_id), run_id=run_id, run=run)
    artifact_id = _positive_int(artifact.get("id"), "artifact.id")
    digest = _digest(artifact.get("digest"), "artifact.digest")
    size = _positive_int(artifact.get("size_in_bytes"), "artifact.size_in_bytes")
    resource_limits.validate_provider_size(
        size,
        resource="merge-closure receipt artifact",
    )
    archive_url = _string(artifact.get("archive_download_url"), "artifact.archive_download_url")
    archive = adapter.download_artifact(archive_url)
    resource_limits.validate_downloaded_size(
        archive,
        resource="merge-closure receipt artifact",
    )
    archive_digest = f"sha256:{hashlib.sha256(archive).hexdigest()}"
    if archive_digest != digest or len(archive) != size:
        raise ValueError("downloaded merge-closure artifact does not match GitHub metadata")
    files = _archive_files(archive)
    if files["pr-receipt-exit-code.txt"].decode("utf-8").strip() != "0":
        raise ValueError("merge-closure producer exit code must be 0")
    receipt_bytes = files["agent_pr_merge_receipt.json"]
    receipt = _json_object(receipt_bytes)
    merge_contract.validate_receipt_payload(receipt)
    _validate_receipt(
        receipt,
        repository=repository,
        integration_commit_sha=integration_commit_sha,
        run_id=run_id,
        run_attempt=run_attempt,
        reviewed_head_sha=_string(run.get("head_sha"), "workflow_run.head_sha"),
        source_archive=files["source-agent-pr-receipt.zip"],
    )
    return MergeReceiptEvidence(
        schema_version=1,
        status="PASS",
        repository=repository,
        integration_commit_sha=integration_commit_sha,
        check_run_id=check_id,
        github_app_id=app_id,
        workflow_run_id=run_id,
        workflow_run_attempt=run_attempt,
        artifact_id=artifact_id,
        artifact_digest=digest,
        artifact_size_bytes=size,
        archive_sha256=archive_digest,
        receipt_sha256=f"sha256:{hashlib.sha256(receipt_bytes).hexdigest()}",
        binding_id=_digest(receipt.get("binding_id"), "receipt.binding_id"),
    )


def _latest_check(checks: list[dict[str, Any]], integration_sha: str) -> dict[str, Any]:
    candidates = [item for item in checks if item.get("name") == CHECK_NAME and item.get("head_sha") == integration_sha]
    if not candidates:
        raise ValueError("exact integration Agent PR receipt check is unavailable")
    latest = max(candidates, key=lambda item: _positive_int(item.get("id"), "check_run.id"))
    if latest.get("status") != "completed" or latest.get("conclusion") != "success":
        raise ValueError("latest exact integration Agent PR receipt check is not successful")
    return latest


def _validate_run(
    run: dict[str, Any],
    *,
    repository: str,
    run_id: int,
    run_attempt: int,
) -> None:
    expected = {
        "id": run_id,
        "name": WORKFLOW_NAME,
        "event": "pull_request",
        "status": "completed",
        "conclusion": "success",
        "run_attempt": run_attempt,
    }
    if any(run.get(field) != value for field, value in expected.items()):
        raise ValueError("merge-closure workflow run identity or result is invalid")
    repo = _mapping(run.get("repository"), "workflow_run.repository")
    if repo.get("full_name") != repository:
        raise ValueError("merge-closure workflow run repository is invalid")
    if _string(run.get("path"), "workflow_run.path").split("@", 1)[0] != WORKFLOW_PATH:
        raise ValueError("merge-closure workflow path is invalid")


def _select_artifact(artifacts: list[dict[str, Any]], *, run_id: int, run: dict[str, Any]) -> dict[str, Any]:
    matches = [item for item in artifacts if item.get("name") == ARTIFACT_NAME]
    if len(matches) != 1:
        raise ValueError("merge-closure workflow must retain exactly one receipt artifact")
    artifact = matches[0]
    if artifact.get("expired") is not False:
        raise ValueError("merge-closure receipt artifact is expired")
    workflow_run = _mapping(artifact.get("workflow_run"), "artifact.workflow_run")
    if workflow_run.get("id") != run_id or workflow_run.get("head_sha") != run.get("head_sha"):
        raise ValueError("merge-closure artifact workflow identity is invalid")
    return artifact


def _archive_files(raw: bytes) -> dict[str, bytes]:
    members = resource_limits.read_bounded_zip(
        raw,
        resource="merge-closure receipt artifact",
    )
    if any(member.is_dir for member in members):
        raise ValueError("merge-closure artifact file set is invalid")
    files = {member.filename: member.content for member in members}
    if frozenset(files) != REQUIRED_FILES:
        raise ValueError("merge-closure artifact file set is invalid")
    return files


def _validate_receipt(
    receipt: dict[str, Any],
    *,
    repository: str,
    integration_commit_sha: str,
    run_id: int,
    run_attempt: int,
    reviewed_head_sha: str,
    source_archive: bytes,
) -> None:
    producer = _mapping(receipt.get("producer"), "receipt.producer")
    expected = {
        "status": "PASS",
        "repository": repository,
        "integration_commit_sha": integration_commit_sha,
        "reviewed_head_sha": reviewed_head_sha,
    }
    if any(receipt.get(field) != value for field, value in expected.items()):
        raise ValueError("merge-closure receipt does not bind the exact release commit")
    if producer != {"workflow": WORKFLOW_NAME, "run_id": str(run_id), "run_attempt": str(run_attempt)}:
        raise ValueError("merge-closure receipt producer identity is invalid")
    source = _mapping(receipt.get("source_receipt"), "receipt.source_receipt")
    source_digest = f"sha256:{hashlib.sha256(source_archive).hexdigest()}"
    if source.get("archive_sha256") != source_digest or source.get("archive_size_bytes") != len(source_archive):
        raise ValueError("preserved source receipt archive does not match merge receipt")
    expected_binding = merge_binding.compute_binding_id(receipt)
    if receipt.get("binding_id") != expected_binding:
        raise ValueError("merge-closure receipt binding_id does not match its canonical content")


def _validate_identity(repository: str, commit_sha: str) -> None:
    if REPOSITORY.fullmatch(repository) is None:
        raise ValueError("repository must use owner/name form")
    if FULL_SHA.fullmatch(commit_sha) is None:
        raise ValueError("integration commit SHA must be full lowercase hexadecimal")


def _mapping(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{field} must be a mapping")
    return value


def _string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a non-empty string")
    return value


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _digest(value: Any, field: str) -> str:
    if not isinstance(value, str) or DIGEST.fullmatch(value) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 digest")
    return value


def _json_object(raw: bytes) -> dict[str, Any]:
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("merge-closure receipt must be a UTF-8 JSON object") from exc
    return _mapping(payload, "merge-closure receipt")


def main(argv: list[str] | None = None) -> int:
    """Emit one credential-free exact-C evidence report."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY"))
    parser.add_argument("--commit-sha", default=os.environ.get("GITHUB_SHA"))
    parser.add_argument("--github-token-env", default="GITHUB_TOKEN")
    parser.add_argument("--required-check-report", required=True, type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        token = os.environ.get(args.github_token_env)
        if not token:
            raise ValueError("approved GitHub token is required")
        evidence = verify_merge_receipt(
            repository=args.repo or "",
            integration_commit_sha=args.commit_sha or "",
            adapter=GitHubMergeReceiptAdapter(token),
        )
        reconcile.reconcile_required_check_report(evidence, args.required_check_report)
        payload = evidence.to_payload()
    except (OSError, RuntimeError, ValueError) as exc:
        payload = {"schema_version": 1, "status": "FAIL", "message": str(exc)}
    output = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output, encoding="utf-8")
    print(output, end="")
    return 0 if payload["status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
