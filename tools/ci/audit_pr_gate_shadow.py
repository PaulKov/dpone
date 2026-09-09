"""Audit a fixture-backed PR Gate shadow producer attempt without executing it."""

from __future__ import annotations

import argparse
import os
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import cast

from dpone.adapters.ci_shadow_audit_github import GitHubAuditApi
from dpone.ports.ci_shadow_audit import CiShadowAuditProvider
from dpone.services.ci.shadow import read_object, write_create_new
from dpone.services.ci.shadow_audit import audit_event
from dpone.services.ci.shadow_bundle import load_bundle_digest, load_bundle_entries


class FixtureAuditProvider(CiShadowAuditProvider):
    """Read-only fixture adapter; production workflow supplies a GitHub adapter."""

    def __init__(self, payload: Mapping[str, object]) -> None:
        self._payload = payload

    def get_workflow_run(self, run_id: int) -> Mapping[str, object]:
        runs = _records(self._payload.get("workflow_runs"), "workflow_runs")
        for record in runs:
            if record.get("run_id") == run_id:
                return record
        raise ValueError("workflow run not found")

    def list_attempt_jobs(self, run_id: int, attempt: int) -> Sequence[Mapping[str, object]]:
        attempts = _mapping(self._payload.get("attempt_jobs"), "attempt_jobs")
        value = attempts.get(f"{run_id}-{attempt}")
        return _records(value, "attempt jobs")

    def get_git_tree(self, commit_sha: str) -> Mapping[str, Mapping[str, object]]:
        trees = _mapping(self._payload.get("git_trees"), "git_trees")
        tree = _mapping(trees.get(commit_sha), "git tree")
        return {path: _mapping(entry, "git tree entry") for path, entry in tree.items() if isinstance(path, str)}

    def get_git_blob(self, blob_sha: str) -> bytes:
        blobs = _mapping(self._payload.get("git_blobs"), "git_blobs")
        value = blobs.get(blob_sha)
        if not isinstance(value, str):
            raise ValueError("git blob not found")
        try:
            return bytes.fromhex(value)
        except ValueError as exc:
            raise ValueError("git blob is malformed") from exc

    def list_open_pull_requests(self) -> Sequence[Mapping[str, object]]:
        return _records(self._payload.get("open_pull_requests"), "open_pull_requests")

    def get_pull_request(self, number: int) -> Mapping[str, object]:
        records = _records(self._payload.get("pull_requests"), "pull_requests")
        for record in records:
            if record.get("number") == number:
                return record
        raise ValueError("pull request not found")

    def resolve_pull_merge_ref(self, number: int) -> str:
        refs = _mapping(self._payload.get("merge_refs"), "merge_refs")
        value = refs.get(str(number))
        if not isinstance(value, str):
            raise ValueError("merge ref not found")
        return value

    def get_commit_parents(self, sha: str) -> Sequence[str]:
        commits = _mapping(self._payload.get("commits"), "commits")
        value = commits.get(sha)
        if not isinstance(value, list) or not all(isinstance(parent, str) for parent in value):
            raise ValueError("commit parents not found")
        return cast(list[str], value)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--event", type=Path, help="Closed audit fixture JSON")
    source.add_argument("--github-event", type=Path, help="Trusted workflow_run event JSON")
    parser.add_argument("--claims", type=Path, help="Downloaded exact producer claims JSON")
    parser.add_argument(
        "--bundle",
        type=Path,
        default=Path(".agents/policy/ci-shadow-bundle-v1.yml"),
        help="Trusted default-branch bundle manifest",
    )
    parser.add_argument("--output", type=Path, required=True, help="Create-only audit receipt path")
    args = parser.parse_args()
    if args.event is not None:
        payload = read_object(args.event)
        event = _mapping(payload.get("event"), "event")
        provider: CiShadowAuditProvider = FixtureAuditProvider(payload)
    else:
        if args.claims is None:
            parser.error("--claims is required with --github-event")
        event_payload = read_object(args.github_event)
        claims = read_object(args.claims)
        event = _live_event(event_payload, claims)
        provider = GitHubAuditApi(
            repository=_string(_mapping(event_payload.get("repository"), "repository").get("full_name"), "full_name"),
            token=os.environ.get("GITHUB_TOKEN", ""),
        )
        if not provider.token:
            parser.error("GITHUB_TOKEN is required with --github-event")
    result = audit_event(
        event,
        provider=provider,
        expected_bundle_digest=load_bundle_digest(args.bundle),
        trusted_bundle=load_bundle_entries(args.bundle),
    )
    if result is None:
        return 0
    write_create_new(args.output, result.receipt)
    return 0 if result.decision == "PASS" else 1


def _records(value: object, name: str) -> Sequence[Mapping[str, object]]:
    if not isinstance(value, list) or not all(isinstance(item, Mapping) for item in value):
        raise ValueError(f"{name} must be a list of objects")
    return cast(list[Mapping[str, object]], value)


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be an object")
    return cast(Mapping[str, object], value)


def _live_event(payload: Mapping[str, object], claims: Mapping[str, object]) -> Mapping[str, object]:
    run = _mapping(payload.get("workflow_run"), "workflow_run")
    repository = _mapping(payload.get("repository"), "repository")
    head_repository = _mapping(run.get("head_repository"), "workflow_run.head_repository")
    auditor = {
        "workflow_id": os.environ.get("GITHUB_WORKFLOW_REF", "unknown"),
        "run_id": _positive_int(os.environ.get("GITHUB_RUN_ID"), "GITHUB_RUN_ID"),
        "run_attempt": _positive_int(os.environ.get("GITHUB_RUN_ATTEMPT"), "GITHUB_RUN_ATTEMPT"),
        "revision": os.environ.get("GITHUB_SHA", ""),
    }
    return {
        "workflow_run": {
            "repository_id": repository.get("id"),
            "run_id": run.get("id"),
            "run_attempt": run.get("run_attempt"),
            "head_repository_id": head_repository.get("id"),
            "head_branch": run.get("head_branch"),
            "head_sha": run.get("head_sha"),
            "status": run.get("status"),
            "conclusion": run.get("conclusion"),
        },
        "claims": {**claims, "run_id": run.get("id"), "run_attempt": run.get("run_attempt")},
        "auditor": auditor,
    }


def _positive_int(value: object, name: str) -> int:
    try:
        parsed = int(str(value))
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if parsed <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return parsed


def _string(value: object, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string")
    return value


if __name__ == "__main__":
    raise SystemExit(main())
